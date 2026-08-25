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

      // A has no deps - it runs immediately. state=waiting.
      const stateA = await cleanupClient.hget(k.job(jobs.get('A')!.id), 'state');
      expect(String(stateA)).toBe('waiting');

      // B waits for A (B.deps=[A]). state=waiting-children.
      const stateB = await cleanupClient.hget(k.job(jobs.get('B')!.id), 'state');
      expect(String(stateB)).toBe('waiting-children');

      // C waits for A. state=waiting-children.
      const stateC = await cleanupClient.hget(k.job(jobs.get('C')!.id), 'state');
      expect(String(stateC)).toBe('waiting-children');

      // D waits for B and C. state=waiting-children.
      const stateD = await cleanupClient.hget(k.job(jobs.get('D')!.id), 'state');
      expect(String(stateD)).toBe('waiting-children');

      // A has no deps to wait for - its Valkey deps SET is empty.
      const depsA = await cleanupClient.smembers(k.deps(jobs.get('A')!.id));
      expect(depsA.size).toBe(0);

      // B waits for A.
      const depsB = await cleanupClient.smembers(k.deps(jobs.get('B')!.id));
      expect(depsB.size).toBe(1);

      // C waits for A.
      const depsC = await cleanupClient.smembers(k.deps(jobs.get('C')!.id));
      expect(depsC.size).toBe(1);

      // D waits for B and C.
      const depsD = await cleanupClient.smembers(k.deps(jobs.get('D')!.id));
      expect(depsD.size).toBe(2);

      // A has B and C as dependents. With multiple dependents, none is
      // piggybacked on addJob's parentId path - both are wired via
      // registerParent into the parents SET so that completion notifies
      // both via SMEMBERS atomically (avoids a race where the worker's
      // snapshot of parentIds is stale).
      const parentsA = await cleanupClient.smembers(k.parents(jobs.get('A')!.id));
      expect(parentsA.size).toBe(2);
      const parentIdA = await cleanupClient.hget(k.job(jobs.get('A')!.id), 'parentId');
      expect(parentIdA == null || String(parentIdA) === '').toBe(true);
      const parentIdsA = await cleanupClient.hget(k.job(jobs.get('A')!.id), 'parentIds');
      const idsA = JSON.parse(String(parentIdsA));
      expect(new Set(idsA)).toEqual(new Set([jobs.get('B')!.id, jobs.get('C')!.id]));
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
    await worker.waitUntilReady();

    try {
      const jobs = await flow.addDAG({
        nodes: [
          { name: 'A', queueName: qName, data: { step: 'A' } },
          { name: 'B', queueName: qName, data: { step: 'B' }, deps: ['A'] },
          { name: 'C', queueName: qName, data: { step: 'C' }, deps: ['A'] },
          { name: 'D', queueName: qName, data: { step: 'D' }, deps: ['B', 'C'] },
        ],
      });

      // Wait for D to complete (D is the terminal node - last to run).
      // Flow: A runs (no deps) -> notifies B and C -> B and C run in parallel
      // -> both notify D -> D runs last.
      const k = buildKeys(qName);
      await waitFor(async () => {
        const stateD = await cleanupClient.hget(k.job(jobs.get('D')!.id), 'state');
        return String(stateD) === 'completed';
      }, 40000);

      // Verify all completed
      for (const [_name, job] of jobs) {
        const state = await cleanupClient.hget(k.job(job.id), 'state');
        expect(String(state)).toBe('completed');
      }

      // A runs first (no deps), then B/C in parallel, then D last.
      expect(processed[0]).toBe('A');
      expect(processed[3]).toBe('D');
      expect(['B', 'C']).toContain(processed[1]);
      expect(['B', 'C']).toContain(processed[2]);
    } finally {
      await worker.close();
      await flow.close();
      await flushQueue(cleanupClient, qName);
    }
  }, 50000);

  it('fan-in: 3 sources feeding into one aggregator', async () => {
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

      // P1/P2/P3 have no deps - they run immediately. state=waiting.
      for (const name of ['P1', 'P2', 'P3']) {
        const state = await cleanupClient.hget(k.job(jobs.get(name)!.id), 'state');
        expect(String(state)).toBe('waiting');
      }

      // child waits for all three sources. state=waiting-children.
      const stateChild = await cleanupClient.hget(k.job(jobs.get('child')!.id), 'state');
      expect(String(stateChild)).toBe('waiting-children');

      // child's deps SET contains P1, P2, P3.
      const depsChild = await cleanupClient.smembers(k.deps(jobs.get('child')!.id));
      expect(depsChild.size).toBe(3);

      // P1/P2/P3 each have 'child' as their single dependent. With one
      // dependent, the addJob primary-parent path stores parentId/parentQueue
      // on the leaf and we don't set parentIds (parentIds is reserved for
      // multi-dependent leaves where additional dependents are wired via
      // registerParent into the parents SET).
      for (const name of ['P1', 'P2', 'P3']) {
        const parentId = await cleanupClient.hget(k.job(jobs.get(name)!.id), 'parentId');
        expect(String(parentId)).toBe(jobs.get('child')!.id);
      }
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

      // A has no deps -> runs immediately -> state=waiting
      const stateA = await cleanupClient.hget(k.job(jobs.get('A')!.id), 'state');
      expect(String(stateA)).toBe('waiting');

      // B waits for A -> state=waiting-children
      const stateB = await cleanupClient.hget(k.job(jobs.get('B')!.id), 'state');
      expect(String(stateB)).toBe('waiting-children');

      // C waits for B -> state=waiting-children
      const stateC = await cleanupClient.hget(k.job(jobs.get('C')!.id), 'state');
      expect(String(stateC)).toBe('waiting-children');
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

      // A has no deps -> runs immediately
      const stateA = await cleanupClient.hget(k1.job(jobs.get('A')!.id), 'state');
      expect(String(stateA)).toBe('waiting');

      // B and C wait for A
      const stateB = await cleanupClient.hget(k1.job(jobs.get('B')!.id), 'state');
      expect(String(stateB)).toBe('waiting-children');

      const stateC = await cleanupClient.hget(k1.job(jobs.get('C')!.id), 'state');
      expect(String(stateC)).toBe('waiting-children');

      // D waits for B and C
      const stateD = await cleanupClient.hget(k1.job(jobs.get('D')!.id), 'state');
      expect(String(stateD)).toBe('waiting-children');

      // A has no deps to wait for
      const depsA = await cleanupClient.smembers(k1.deps(jobs.get('A')!.id));
      expect(depsA.size).toBe(0);

      // B and C each wait for A
      const depsB = await cleanupClient.smembers(k1.deps(jobs.get('B')!.id));
      expect(depsB.size).toBe(1);
      const depsC = await cleanupClient.smembers(k1.deps(jobs.get('C')!.id));
      expect(depsC.size).toBe(1);

      // D waits for both B and C
      const depsD = await cleanupClient.smembers(k1.deps(jobs.get('D')!.id));
      expect(depsD.size).toBe(2);

      // A has dependents B and C - parentIds contains both
      const parentIdsRaw = await cleanupClient.hget(k1.job(jobs.get('A')!.id), 'parentIds');
      const parentIds = JSON.parse(String(parentIdsRaw));
      expect(parentIds).toHaveLength(2);

      // parentQueues array contains the queues of A's dependents
      const parentQueuesRaw = await cleanupClient.hget(k1.job(jobs.get('A')!.id), 'parentQueues');
      const parentQueues = JSON.parse(String(parentQueuesRaw));
      expect(parentQueues).toContain(qName1);
    } finally {
      await flow.close();
      await flushQueue(cleanupClient, qName1);
    }
  });

  it('wires a DAG across distinct queue slots', async () => {
    const qA = Q + '-distinct-a';
    const qB = Q + '-distinct-b';
    const qC = Q + '-distinct-c';
    const flow = new FlowProducer({ connection: CONNECTION });

    try {
      const jobs = await flow.addDAG({
        nodes: [
          { name: 'A', queueName: qA, data: {} },
          { name: 'B', queueName: qB, data: {}, deps: ['A'] },
          { name: 'C', queueName: qC, data: {}, deps: ['B'] },
        ],
      });

      expect(String(await cleanupClient.hget(buildKeys(qB).job(jobs.get('B')!.id), 'state'))).toBe('waiting-children');
      expect(String(await cleanupClient.hget(buildKeys(qC).job(jobs.get('C')!.id), 'state'))).toBe('waiting-children');
      expect((await cleanupClient.smembers(buildKeys(qB).deps(jobs.get('B')!.id))).size).toBe(1);
      expect((await cleanupClient.smembers(buildKeys(qC).deps(jobs.get('C')!.id))).size).toBe(1);
      const parentsA = await cleanupClient.smembers(buildKeys(qA).parents(jobs.get('A')!.id));
      expect(parentsA.size).toBe(1);
    } finally {
      await flow.close();
      await flushQueue(cleanupClient, qA);
      await flushQueue(cleanupClient, qB);
      await flushQueue(cleanupClient, qC);
    }
  });

  it('executes a DAG across distinct queue workers', async () => {
    const qA = Q + '-execute-a';
    const qB = Q + '-execute-b';
    const qC = Q + '-execute-c';
    const flow = new FlowProducer({ connection: CONNECTION });
    const processed: string[] = [];
    const workers = [
      new Worker(
        qA,
        async () => {
          processed.push('A');
          return 'A';
        },
        { connection: CONNECTION, blockTimeout: 200, stalledInterval: 60000, promotionInterval: 100 },
      ),
      new Worker(
        qB,
        async () => {
          processed.push('B');
          return 'B';
        },
        { connection: CONNECTION, blockTimeout: 200, stalledInterval: 60000, promotionInterval: 100 },
      ),
      new Worker(
        qC,
        async () => {
          processed.push('C');
          return 'C';
        },
        { connection: CONNECTION, blockTimeout: 200, stalledInterval: 60000, promotionInterval: 100 },
      ),
    ];
    for (const worker of workers) {
      worker.on('error', () => {});
      await worker.waitUntilReady();
    }

    try {
      const jobs = await flow.addDAG({
        nodes: [
          { name: 'A', queueName: qA, data: {} },
          { name: 'B', queueName: qB, data: {}, deps: ['A'] },
          { name: 'C', queueName: qC, data: {}, deps: ['B'] },
        ],
      });
      const cKeys = buildKeys(qC);
      await waitFor(
        async () => String(await cleanupClient.hget(cKeys.job(jobs.get('C')!.id), 'state')) === 'completed',
        10000,
      );
      expect(processed).toEqual(['A', 'B', 'C']);
    } finally {
      for (const worker of workers) await worker.close(true);
      await flow.close();
      await flushQueue(cleanupClient, qA);
      await flushQueue(cleanupClient, qB);
      await flushQueue(cleanupClient, qC);
    }
  }, 20000);

  it('notifies cross-queue DAG parents without waiting for the promotion tick', async () => {
    const qA = Q + '-eager-a';
    const qB = Q + '-eager-b';
    const flow = new FlowProducer({ connection: CONNECTION });
    const processed: string[] = [];
    let releaseA!: () => void;
    const finishA = new Promise<void>((resolve) => {
      releaseA = resolve;
    });
    const workers = [
      new Worker(
        qA,
        async () => {
          processed.push('A');
          await finishA;
          return 'A';
        },
        { connection: CONNECTION, blockTimeout: 100, stalledInterval: 60000, promotionInterval: 10000 },
      ),
      new Worker(
        qB,
        async () => {
          processed.push('B');
          return 'B';
        },
        { connection: CONNECTION, blockTimeout: 100, stalledInterval: 60000, promotionInterval: 10000 },
      ),
    ];
    for (const worker of workers) {
      worker.on('error', () => {});
      await worker.waitUntilReady();
    }
    // Let the startup promotion tick finish before submitting the DAG. The
    // next scheduled tick is deliberately far outside this test's deadline.
    await new Promise((resolve) => setTimeout(resolve, 100));

    try {
      const jobs = await flow.addDAG({
        nodes: [
          { name: 'A', queueName: qA, data: {} },
          { name: 'B', queueName: qB, data: {}, deps: ['A'] },
        ],
      });
      releaseA();
      const bKeys = buildKeys(qB);
      await waitFor(
        async () => String(await cleanupClient.hget(bKeys.job(jobs.get('B')!.id), 'state')) === 'completed',
        1500,
        25,
      );
      expect(processed).toEqual(['A', 'B']);
    } finally {
      releaseA();
      for (const worker of workers) await worker.close(true);
      await flow.close();
      await flushQueue(cleanupClient, qA);
      await flushQueue(cleanupClient, qB);
    }
  }, 5000);

  it('executes a cross-queue DAG with a prefix containing braces', async () => {
    const prefix = 'test-dag:{tenant}';
    const qA = Q + '-prefix-a';
    const qB = Q + '-prefix-b';
    const qC = Q + '-prefix-c';
    const flow = new FlowProducer({ connection: CONNECTION, prefix });
    const processed: string[] = [];
    const workers = [
      new Worker(
        qA,
        async () => {
          processed.push('A');
          return 'A';
        },
        {
          connection: CONNECTION,
          prefix,
          blockTimeout: 200,
          stalledInterval: 60000,
          promotionInterval: 100,
        },
      ),
      new Worker(
        qB,
        async () => {
          processed.push('B');
          return 'B';
        },
        {
          connection: CONNECTION,
          prefix,
          blockTimeout: 200,
          stalledInterval: 60000,
          promotionInterval: 100,
        },
      ),
      new Worker(
        qC,
        async () => {
          processed.push('C');
          return 'C';
        },
        {
          connection: CONNECTION,
          prefix,
          blockTimeout: 200,
          stalledInterval: 60000,
          promotionInterval: 100,
        },
      ),
    ];
    for (const worker of workers) {
      worker.on('error', () => {});
      await worker.waitUntilReady();
    }

    try {
      const jobs = await flow.addDAG({
        nodes: [
          { name: 'A', queueName: qA, data: {} },
          { name: 'B', queueName: qB, data: {}, deps: ['A'] },
          { name: 'C', queueName: qC, data: {}, deps: ['B'] },
        ],
      });
      await waitFor(
        async () =>
          String(await cleanupClient.hget(buildKeys(qC, prefix).job(jobs.get('C')!.id), 'state')) === 'completed',
        10000,
      );
      expect(processed).toEqual(['A', 'B', 'C']);
    } finally {
      for (const worker of workers) await worker.close(true);
      await flow.close();
      await flushQueue(cleanupClient, qA, prefix);
      await flushQueue(cleanupClient, qB, prefix);
      await flushQueue(cleanupClient, qC, prefix);
    }
  }, 20000);

  it('reconciles a DAG child deleted between Phase A and Phase B', async () => {
    const queueName = Q + '-phaseb-race';
    const flow = new FlowProducer({ connection: CONNECTION });
    let worker: any;

    const client = await (flow as any).getClient();
    const originalExists = client.exists.bind(client);
    let deleted = false;
    client.exists = async (keys: string[]) => {
      const result = await originalExists(keys);
      const key = String(keys[0] ?? '');
      if (!deleted && result === 1 && key.includes(`{${queueName}}:job:`)) {
        deleted = true;
        await cleanupClient.del([key]);
        return 0;
      }
      return result;
    };

    try {
      const jobs = await flow.addDAG({
        nodes: [
          { name: 'A', queueName, data: {} },
          { name: 'B', queueName, data: {}, deps: ['A'] },
          { name: 'C', queueName, data: {}, deps: ['A'] },
        ],
      });
      expect(deleted).toBe(true);
      const keys = buildKeys(queueName);
      const streamEntries = (await cleanupClient.xrange(keys.stream, '-', '+')) as Record<string, [string, string][]>;
      for (const [entryId, fields] of Object.entries(streamEntries)) {
        if (fields.some(([field, value]) => String(field) === 'jobId' && String(value) === jobs.get('A')!.id)) {
          await cleanupClient.xdel(keys.stream, [entryId]);
        }
      }
      worker = new Worker(queueName, async () => 'ok', {
        connection: CONNECTION,
        blockTimeout: 200,
        stalledInterval: 60000,
      });
      worker.on('error', () => {});
      await worker.waitUntilReady();
      await waitFor(
        async () =>
          String(await cleanupClient.hget(keys.job(jobs.get('B')!.id), 'state')) === 'completed' &&
          String(await cleanupClient.hget(keys.job(jobs.get('C')!.id), 'state')) === 'completed',
        10000,
      );
      expect(await cleanupClient.exists([keys.job(jobs.get('A')!.id)])).toBe(0);
      expect(await cleanupClient.exists([keys.parents(jobs.get('A')!.id)])).toBe(0);
    } finally {
      client.exists = originalExists;
      await worker?.close(true);
      await flow.close();
      await flushQueue(cleanupClient, queueName);
    }
  }, 20000);

  it('reconciles a child deleted during same-queue parent registration', async () => {
    const queueName = Q + '-register-race';
    const flow = new FlowProducer({ connection: CONNECTION });
    const client = await (flow as any).getClient();
    const originalFcall = client.fcall.bind(client);
    let deleted = false;
    client.fcall = async (name: string, keys: string[], args: string[]) => {
      if (!deleted && name === 'glidemq_registerParent') {
        deleted = true;
        await cleanupClient.del([String(keys[0])]);
      }
      return originalFcall(name, keys, args);
    };

    try {
      const jobs = await flow.addDAG({
        nodes: [
          { name: 'A', queueName, data: {} },
          { name: 'B', queueName, data: {}, deps: ['A'] },
          { name: 'C', queueName, data: {}, deps: ['A'] },
        ],
      });

      expect(deleted).toBe(true);
      const keys = buildKeys(queueName);
      expect(await cleanupClient.exists([keys.job(jobs.get('A')!.id)])).toBe(0);
      expect(String(await cleanupClient.hget(keys.job(jobs.get('B')!.id), 'state'))).toBe('waiting');
      expect(String(await cleanupClient.hget(keys.job(jobs.get('C')!.id), 'state'))).toBe('waiting');
    } finally {
      client.fcall = originalFcall;
      await flow.close();
      await flushQueue(cleanupClient, queueName);
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
    // Wait for the worker to be ready before submitting the DAG. Without
    // this, the worker can still be opening its blocking client when the
    // DAG is added, and the cascading completions race the worker startup.
    await worker.waitUntilReady();

    try {
      // Diamond: A runs first (no deps), B/C wait for A, D waits for B and C.
      // D's depsCompleted increments as B and C complete; D runs once it
      // reaches 2.
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

      // Wait for A to complete (runs first - no deps).
      await waitFor(async () => {
        const state = await cleanupClient.hget(k.job(aId), 'state');
        return String(state) === 'completed';
      }, 15000);

      // After A completes, one of B or C should run.
      await waitFor(async () => {
        const stateB = await cleanupClient.hget(k.job(bId), 'state');
        const stateC = await cleanupClient.hget(k.job(cId), 'state');
        return String(stateB) === 'completed' || String(stateC) === 'completed';
      }, 15000);

      // D's depsCompleted should be at least 1 (one of B/C notified D).
      let depsCompleted = await cleanupClient.hget(k.job(dId), 'depsCompleted');
      const firstCount = Number(depsCompleted) || 0;
      expect(firstCount).toBeGreaterThanOrEqual(1);

      // Wait for both B and C to complete.
      await waitFor(async () => {
        const stateB = await cleanupClient.hget(k.job(bId), 'state');
        const stateC = await cleanupClient.hget(k.job(cId), 'state');
        return String(stateB) === 'completed' && String(stateC) === 'completed';
      }, 15000);

      // Both notified D - depsCompleted should be exactly 2.
      depsCompleted = await cleanupClient.hget(k.job(dId), 'depsCompleted');
      expect(Number(depsCompleted)).toBe(2);

      // D should transition to waiting and run.
      await waitFor(async () => {
        const state = await cleanupClient.hget(k.job(dId), 'state');
        return String(state) === 'completed';
      }, 15000);

      const stateD = await cleanupClient.hget(k.job(dId), 'state');
      expect(String(stateD)).toBe('completed');
    } finally {
      await worker.close();
      await flow.close();
      await flushQueue(cleanupClient, qName);
    }
  }, 60000);

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

  it('addDAG throws on node with lifo+ordering combination', async () => {
    const flow = new FlowProducer({ connection: CONNECTION });

    try {
      await expect(
        flow.addDAG({
          nodes: [
            { name: 'A', queueName: Q, data: {}, opts: { lifo: true, ordering: { key: 'grp1' } } },
            { name: 'B', queueName: Q, data: {}, deps: ['A'] },
          ],
        }),
      ).rejects.toThrow('lifo and ordering.key cannot be combined in a DAG node');
    } finally {
      await flow.close();
    }
  });
});
