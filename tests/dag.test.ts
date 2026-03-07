/**
 * DAG flow (multi-parent dependency) integration tests.
 * Runs against both standalone (:6379) and cluster (:7000).
 */
import { it, expect, beforeAll, afterAll } from 'vitest';
import { describeEachMode, createCleanupClient, flushQueue, waitFor } from './helpers/fixture';

const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { FlowProducer } = require('../dist/flow-producer') as typeof import('../src/flow-producer');
const { buildKeys, keyPrefix } = require('../dist/utils') as typeof import('../src/utils');
const { dag } = require('../dist/workflows') as typeof import('../src/workflows');

describeEachMode('DAG flows', (CONNECTION) => {
  let cleanupClient: any;
  const Q = 'test-dag-' + Date.now();

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    await flushQueue(cleanupClient, Q + '-cross');
    cleanupClient.close();
  });

  it('creates a diamond DAG: D waits for both B and C', async () => {
    const flow = new FlowProducer({ connection: CONNECTION });

    try {
      const jobs = await flow.addDAG({
        nodes: [
          { name: 'A', queueName: Q, data: { step: 'A' } },
          { name: 'B', queueName: Q, data: { step: 'B' }, deps: ['A'] },
          { name: 'C', queueName: Q, data: { step: 'C' }, deps: ['A'] },
          { name: 'D', queueName: Q, data: { step: 'D' }, deps: ['B', 'C'] },
        ],
      });

      expect(jobs.size).toBe(4);

      const k = buildKeys(Q);

      // A should be in waiting-children (B and C depend on it)
      const stateA = await cleanupClient.hget(k.job(jobs.get('A')!.id), 'state');
      expect(String(stateA)).toBe('waiting-children');

      // B should be in waiting-children (D depends on it)
      const stateB = await cleanupClient.hget(k.job(jobs.get('B')!.id), 'state');
      expect(String(stateB)).toBe('waiting-children');

      // C should be in waiting-children (D depends on it)
      const stateC = await cleanupClient.hget(k.job(jobs.get('C')!.id), 'state');
      expect(String(stateC)).toBe('waiting-children');

      // D should be waiting (it's the terminal node with deps - it has parentId set
      // and is registered as child of B and C)
      // D is a non-parent node with deps, so it's a regular job added via addJob
      // with parent=B's id. Its state depends on whether it has delay/priority.
      const stateD = await cleanupClient.hget(k.job(jobs.get('D')!.id), 'state');
      expect(String(stateD)).toBe('waiting');

      // Verify A's deps contain B and C
      const depsA = await cleanupClient.smembers(k.deps(jobs.get('A')!.id));
      expect(depsA.size).toBe(2);

      // Verify B's deps contain D
      const depsB = await cleanupClient.smembers(k.deps(jobs.get('B')!.id));
      expect(depsB.size).toBe(1);

      // Verify C's deps contain D
      const depsC = await cleanupClient.smembers(k.deps(jobs.get('C')!.id));
      expect(depsC.size).toBe(1);

      // Verify D has a parents SET with B and C
      const parentsD = await cleanupClient.smembers(k.parents(jobs.get('D')!.id));
      // Parents SET only created for 2nd+ parents via registerParent
      // The first parent is handled via the traditional parentId/parentDepsKey path
      expect(parentsD.size).toBe(1); // Only the 2nd parent (C) registered via registerParent
    } finally {
      await flow.close();
    }
  });

  it('diamond DAG processes correctly end-to-end', async () => {
    const qName = Q + '-e2e';
    const processed: string[] = [];

    const flow = new FlowProducer({ connection: CONNECTION });
    const worker = new Worker(
      qName,
      async (job: any) => {
        processed.push(job.name);
        return `result-${job.name}`;
      },
      { connection: CONNECTION, concurrency: 1 },
    );

    try {
      const jobs = await flow.addDAG({
        nodes: [
          { name: 'A', queueName: qName, data: { step: 'A' } },
          { name: 'B', queueName: qName, data: { step: 'B' }, deps: ['A'] },
          { name: 'C', queueName: qName, data: { step: 'C' }, deps: ['A'] },
          { name: 'D', queueName: qName, data: { step: 'D' }, deps: ['B', 'C'] },
        ],
      });

      // Wait for ALL jobs to complete (A is the last to complete in the cascade)
      // Flow: D processes first (child) -> notifies B and C -> B and C process -> notify A -> A processes
      const k = buildKeys(qName);
      await waitFor(async () => {
        const stateA = await cleanupClient.hget(k.job(jobs.get('A')!.id), 'state');
        return String(stateA) === 'completed';
      }, 15000);

      // Verify all completed
      for (const [name, job] of jobs) {
        const state = await cleanupClient.hget(k.job(job.id), 'state');
        expect(String(state)).toBe('completed');
      }

      // D processes first (it's the leaf child in the stream), then B and C,
      // then finally A (root parent)
      expect(processed).toContain('D');
      expect(processed).toContain('A');
      expect(processed.indexOf('D')).toBeLessThan(processed.indexOf('A'));
    } finally {
      await worker.close();
      await flow.close();
      await flushQueue(cleanupClient, qName);
    }
  });

  it('fan-in: 3 parents feeding into one child', async () => {
    const flow = new FlowProducer({ connection: CONNECTION });
    const qName = Q + '-fanin';

    try {
      const jobs = await flow.addDAG({
        nodes: [
          { name: 'P1', queueName: qName, data: {} },
          { name: 'P2', queueName: qName, data: {} },
          { name: 'P3', queueName: qName, data: {} },
          { name: 'child', queueName: qName, data: {}, deps: ['P1', 'P2', 'P3'] },
        ],
      });

      expect(jobs.size).toBe(4);

      const k = buildKeys(qName);

      // All parents should be in waiting-children
      for (const name of ['P1', 'P2', 'P3']) {
        const state = await cleanupClient.hget(k.job(jobs.get(name)!.id), 'state');
        expect(String(state)).toBe('waiting-children');
      }

      // P1's deps should contain child
      const depsP1 = await cleanupClient.smembers(k.deps(jobs.get('P1')!.id));
      expect(depsP1.size).toBe(1);

      // P2's deps should contain child
      const depsP2 = await cleanupClient.smembers(k.deps(jobs.get('P2')!.id));
      expect(depsP2.size).toBe(1);

      // P3's deps should contain child
      const depsP3 = await cleanupClient.smembers(k.deps(jobs.get('P3')!.id));
      expect(depsP3.size).toBe(1);

      // child should have parentIds with all 3 parents
      const parentIdsRaw = await cleanupClient.hget(k.job(jobs.get('child')!.id), 'parentIds');
      const parentIds = JSON.parse(String(parentIdsRaw));
      expect(parentIds).toHaveLength(3);
    } finally {
      await flow.close();
      await flushQueue(cleanupClient, qName);
    }
  });

  it('dag() workflow helper creates a valid diamond', async () => {
    const qName = Q + '-helper';

    const jobs = await dag(
      [
        { name: 'root', queueName: qName, data: { v: 1 } },
        { name: 'left', queueName: qName, data: { v: 2 }, deps: ['root'] },
        { name: 'right', queueName: qName, data: { v: 3 }, deps: ['root'] },
        { name: 'sink', queueName: qName, data: { v: 4 }, deps: ['left', 'right'] },
      ],
      CONNECTION,
    );

    expect(jobs.size).toBe(4);
    expect(jobs.get('root')).toBeDefined();
    expect(jobs.get('left')).toBeDefined();
    expect(jobs.get('right')).toBeDefined();
    expect(jobs.get('sink')).toBeDefined();

    await flushQueue(cleanupClient, qName);
  });

  it('rejects DAG with cycle', async () => {
    const flow = new FlowProducer({ connection: CONNECTION });

    try {
      await expect(
        flow.addDAG({
          nodes: [
            { name: 'A', queueName: Q, data: {}, deps: ['B'] },
            { name: 'B', queueName: Q, data: {}, deps: ['A'] },
          ],
        }),
      ).rejects.toThrow(/cycle/i);
    } finally {
      await flow.close();
    }
  });

  it('rejects empty DAG', async () => {
    await expect(dag([], CONNECTION)).rejects.toThrow(/at least one node/);
  });

  it('handles single node DAG (no deps, no children)', async () => {
    const qName = Q + '-single';
    const flow = new FlowProducer({ connection: CONNECTION });

    try {
      const jobs = await flow.addDAG({
        nodes: [{ name: 'solo', queueName: qName, data: { x: 1 } }],
      });

      expect(jobs.size).toBe(1);
      const k = buildKeys(qName);
      const state = await cleanupClient.hget(k.job(jobs.get('solo')!.id), 'state');
      expect(String(state)).toBe('waiting');
    } finally {
      await flow.close();
      await flushQueue(cleanupClient, qName);
    }
  });

  it('handles linear chain A -> B -> C via DAG API', async () => {
    const qName = Q + '-linear';
    const flow = new FlowProducer({ connection: CONNECTION });

    try {
      const jobs = await flow.addDAG({
        nodes: [
          { name: 'A', queueName: qName, data: {} },
          { name: 'B', queueName: qName, data: {}, deps: ['A'] },
          { name: 'C', queueName: qName, data: {}, deps: ['B'] },
        ],
      });

      expect(jobs.size).toBe(3);
      const k = buildKeys(qName);

      // A is parent of B -> waiting-children
      const stateA = await cleanupClient.hget(k.job(jobs.get('A')!.id), 'state');
      expect(String(stateA)).toBe('waiting-children');

      // B is parent of C -> waiting-children
      const stateB = await cleanupClient.hget(k.job(jobs.get('B')!.id), 'state');
      expect(String(stateB)).toBe('waiting-children');

      // C is terminal -> waiting
      const stateC = await cleanupClient.hget(k.job(jobs.get('C')!.id), 'state');
      expect(String(stateC)).toBe('waiting');
    } finally {
      await flow.close();
      await flushQueue(cleanupClient, qName);
    }
  });
});
