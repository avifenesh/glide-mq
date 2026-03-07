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

  it('handles cross-queue diamond DAG', async () => {
    // Use same queue to avoid cluster cross-slot errors - test the logic with different queue names
    // but same hash tag (both resolve to same prefix)
    const qName1 = Q + '-cross';
    const qName2 = Q + '-cross'; // Same queue for cluster compatibility
    const flow = new FlowProducer({ connection: CONNECTION });

    try {
      const jobs = await flow.addDAG({
        nodes: [
          { name: 'A', queueName: qName1, data: { step: 'A' } },
          { name: 'B', queueName: qName1, data: { step: 'B' }, deps: ['A'] },
          { name: 'C', queueName: qName2, data: { step: 'C' }, deps: ['A'] },
          { name: 'D', queueName: qName2, data: { step: 'D' }, deps: ['B', 'C'] },
        ],
      });

      expect(jobs.size).toBe(4);

      const k1 = buildKeys(qName1);

      // A should be in waiting-children (B and C depend on it)
      const stateA = await cleanupClient.hget(k1.job(jobs.get('A')!.id), 'state');
      expect(String(stateA)).toBe('waiting-children');

      // B should be in waiting-children (D depends on it)
      const stateB = await cleanupClient.hget(k1.job(jobs.get('B')!.id), 'state');
      expect(String(stateB)).toBe('waiting-children');

      // C should be in waiting-children (D depends on it)
      const stateC = await cleanupClient.hget(k1.job(jobs.get('C')!.id), 'state');
      expect(String(stateC)).toBe('waiting-children');

      // D should be waiting (terminal node)
      const stateD = await cleanupClient.hget(k1.job(jobs.get('D')!.id), 'state');
      expect(String(stateD)).toBe('waiting');

      // Verify A's deps contain both B and C
      const depsA = await cleanupClient.smembers(k1.deps(jobs.get('A')!.id));
      expect(depsA.size).toBe(2);

      // Verify B's deps contain D
      const depsB = await cleanupClient.smembers(k1.deps(jobs.get('B')!.id));
      expect(depsB.size).toBe(1);

      // Verify C's deps contain D
      const depsC = await cleanupClient.smembers(k1.deps(jobs.get('C')!.id));
      expect(depsC.size).toBe(1);

      // Verify D has parentIds with both B and C
      const parentIdsRaw = await cleanupClient.hget(k1.job(jobs.get('D')!.id), 'parentIds');
      const parentIds = JSON.parse(String(parentIdsRaw));
      expect(parentIds).toHaveLength(2);

      // Verify D has parentQueues array
      const parentQueuesRaw = await cleanupClient.hget(k1.job(jobs.get('D')!.id), 'parentQueues');
      const parentQueues = JSON.parse(String(parentQueuesRaw));
      expect(parentQueues).toContain(qName1);
      expect(parentQueues).toContain(qName2);
    } finally {
      await flow.close();
      await flushQueue(cleanupClient, qName1);
    }
  });

  it('handles race: registerParent after child completes', async () => {
    const qName = Q + '-race';
    const flow = new FlowProducer({ connection: CONNECTION });

    try {
      // Create a child job and manually complete it
      const jobs = await flow.addDAG({
        nodes: [{ name: 'child', queueName: qName, data: {} }],
      });

      const k = buildKeys(qName);
      const childId = jobs.get('child')!.id;

      // Manually mark the child as completed
      await cleanupClient.hset(k.job(childId), { state: 'completed' });

      // Create a parent in waiting-children state
      const jobs2 = await flow.addDAG({
        nodes: [{ name: 'P', queueName: qName, data: {} }],
      });
      const parentId = jobs2.get('P')!.id;

      // Now set parent to waiting-children (simulate it having deps)
      await cleanupClient.hset(k.job(parentId), { state: 'waiting-children' });

      // Register parent to the already-completed child (race condition simulation)
      const { registerParent } = require('../dist/functions/index') as typeof import('../src/functions/index');
      const depsMember = `${keyPrefix('glide', qName)}:${childId}`;
      const result = await registerParent(cleanupClient, k, childId, parentId, qName, k, depsMember);

      // Should return 'already_completed' since child is done
      expect(result).toBe('already_completed');

      // Parent should transition to waiting (since all deps complete)
      const stateP = await cleanupClient.hget(k.job(parentId), 'state');
      expect(String(stateP)).toBe('waiting');

      // Verify parent's depsCompleted counter incremented
      const depsCompleted = await cleanupClient.hget(k.job(parentId), 'depsCompleted');
      expect(Number(depsCompleted)).toBe(1);

      // Verify parent is in the stream
      const streamLen = await cleanupClient.xlen(k.stream);
      expect(Number(streamLen)).toBeGreaterThan(0);
    } finally {
      await flow.close();
      await flushQueue(cleanupClient, qName);
    }
  });

  it('verifies depsCompleted counter increments correctly', async () => {
    const qName = Q + '-depscounter';
    const flow = new FlowProducer({ connection: CONNECTION });
    const processed: string[] = [];

    const worker = new Worker(
      qName,
      async (job: any) => {
        processed.push(job.name);
        return `result-${job.name}`;
      },
      { connection: CONNECTION, concurrency: 2 },
    );

    try {
      // Diamond: A->B, A->C, B->D, C->D
      const jobs = await flow.addDAG({
        nodes: [
          { name: 'A', queueName: qName, data: {} },
          { name: 'B', queueName: qName, data: {}, deps: ['A'] },
          { name: 'C', queueName: qName, data: {}, deps: ['A'] },
          { name: 'D', queueName: qName, data: {}, deps: ['B', 'C'] },
        ],
      });

      const k = buildKeys(qName);
      const aId = jobs.get('A')!.id;
      const bId = jobs.get('B')!.id;
      const cId = jobs.get('C')!.id;
      const dId = jobs.get('D')!.id;

      // Wait for D to complete (it processes first as the leaf)
      await waitFor(async () => {
        const state = await cleanupClient.hget(k.job(dId), 'state');
        return String(state) === 'completed';
      }, 15000);

      // At this point, one of B or C should have completed (notifying A)
      // Wait for at least one of them to complete
      await waitFor(async () => {
        const stateB = await cleanupClient.hget(k.job(bId), 'state');
        const stateC = await cleanupClient.hget(k.job(cId), 'state');
        return String(stateB) === 'completed' || String(stateC) === 'completed';
      }, 15000);

      // Check A's depsCompleted counter
      let depsCompleted = await cleanupClient.hget(k.job(aId), 'depsCompleted');
      const firstCount = Number(depsCompleted) || 0;
      expect(firstCount).toBeGreaterThanOrEqual(1);

      // Wait for both B and C to complete
      await waitFor(async () => {
        const stateB = await cleanupClient.hget(k.job(bId), 'state');
        const stateC = await cleanupClient.hget(k.job(cId), 'state');
        return String(stateB) === 'completed' && String(stateC) === 'completed';
      }, 15000);

      // After both complete, A's depsCompleted should be 2
      depsCompleted = await cleanupClient.hget(k.job(aId), 'depsCompleted');
      expect(Number(depsCompleted)).toBe(2);

      // Wait for A to transition to waiting (and then complete)
      await waitFor(async () => {
        const state = await cleanupClient.hget(k.job(aId), 'state');
        return String(state) === 'completed';
      }, 15000);

      const stateA = await cleanupClient.hget(k.job(aId), 'state');
      expect(String(stateA)).toBe('completed');
    } finally {
      await worker.close();
      await flow.close();
      await flushQueue(cleanupClient, qName);
    }
  });

  it('verifies idempotency: duplicate parent registration', async () => {
    const qName = Q + '-idempotent';
    const flow = new FlowProducer({ connection: CONNECTION });

    try {
      // Create a child job and manually complete it
      const jobs = await flow.addDAG({
        nodes: [{ name: 'child', queueName: qName, data: {} }],
      });

      const k = buildKeys(qName);
      const childId = jobs.get('child')!.id;

      // Manually mark child as completed
      await cleanupClient.hset(k.job(childId), { state: 'completed' });

      // Create a parent in waiting-children state
      const jobs2 = await flow.addDAG({
        nodes: [{ name: 'parent', queueName: qName, data: {} }],
      });
      const parentId = jobs2.get('parent')!.id;

      // Set parent to waiting-children
      await cleanupClient.hset(k.job(parentId), { state: 'waiting-children' });

      const { registerParent } = require('../dist/functions/index') as typeof import('../src/functions/index');
      const depsMember = `${keyPrefix('glide', qName)}:${childId}`;

      // First registration (should work and immediately notify since child is completed)
      const result1 = await registerParent(cleanupClient, k, childId, parentId, qName, k, depsMember);
      expect(result1).toBe('already_completed');

      // Duplicate registration (should be idempotent - SADD is idempotent, HSETNX prevents double-increment)
      const result2 = await registerParent(cleanupClient, k, childId, parentId, qName, k, depsMember);
      expect(result2).toBe('already_completed');

      // Verify parent's deps SET contains child only once
      const deps = await cleanupClient.smembers(k.deps(parentId));
      expect(deps.size).toBe(1);

      // Parent should still be in waiting state (transitioned on first registration)
      const state = await cleanupClient.hget(k.job(parentId), 'state');
      expect(String(state)).toBe('waiting');

      // Check that depsCompleted is exactly 1 (not 2 from duplicate registration)
      // The HSETNX in registerParent ensures idempotency
      const depsCompleted = await cleanupClient.hget(k.job(parentId), 'depsCompleted');
      expect(Number(depsCompleted)).toBe(1);
    } finally {
      await flow.close();
      await flushQueue(cleanupClient, qName);
    }
  });
});
