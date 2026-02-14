/**
 * Deep tests: Workflow primitives - chain, group, chord.
 * Requires: valkey-server running on localhost:6379
 *
 * Run: npx vitest run tests/deep-workflows.test.ts
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';

const { GlideClient } = require('speedkey') as typeof import('speedkey');
const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { FlowProducer } = require('../dist/flow-producer') as typeof import('../src/flow-producer');
const { chain, group, chord } = require('../dist/workflows') as typeof import('../src/workflows');
const { buildKeys, keyPrefix } = require('../dist/utils') as typeof import('../src/utils');
const { LIBRARY_SOURCE } = require('../dist/functions/index') as typeof import('../src/functions/index');
const { ensureFunctionLibrary } = require('../dist/connection') as typeof import('../src/connection');

const CONNECTION = {
  addresses: [{ host: 'localhost', port: 6379 }],
};

let cleanupClient: InstanceType<typeof GlideClient>;

async function flushQueue(queueName: string) {
  const k = buildKeys(queueName);
  const keysToDelete = [
    k.id, k.stream, k.scheduled, k.completed, k.failed,
    k.events, k.meta, k.dedup, k.rate, k.schedulers,
  ];
  for (const key of keysToDelete) {
    try { await cleanupClient.del([key]); } catch {}
  }
  const prefix = `glide:{${queueName}}:`;
  let cursor = '0';
  do {
    const result = await cleanupClient.scan(cursor, { match: `${prefix}*`, count: 100 });
    cursor = result[0] as string;
    const keys = result[1] as string[];
    if (keys.length > 0) {
      await cleanupClient.del(keys);
    }
  } while (cursor !== '0');
}

/** Helper: create a worker that processes jobs, resolves when targetJobId completes. */
function createTestWorker(
  queueName: string,
  processor: (job: any) => Promise<any>,
  targetJobId: string,
  timeoutMs = 15000,
): { promise: Promise<void>; workerRef: { w: any } } {
  const ref: { w: any } = { w: null };
  const promise = new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error('timeout waiting for ' + targetJobId)), timeoutMs);
    const worker = new Worker(queueName, processor, {
      connection: CONNECTION,
      concurrency: 3,
      blockTimeout: 500,
      promotionInterval: 500,
    });
    ref.w = worker;
    worker.on('completed', (job: any) => {
      if (job.id === targetJobId) {
        clearTimeout(timeout);
        setTimeout(() => worker.close(true).then(resolve), 200);
      }
    });
    worker.on('error', () => {});
  });
  return { promise, workerRef: ref };
}

beforeAll(async () => {
  cleanupClient = await GlideClient.createClient({
    addresses: [{ host: 'localhost', port: 6379 }],
  });
  await (cleanupClient as any).functionLoad(LIBRARY_SOURCE, { replace: true });
});

afterAll(async () => {
  cleanupClient.close();
});

// ---------------------------------------------------------------------------
// chain()
// ---------------------------------------------------------------------------
describe('chain()', () => {
  it('single-job chain returns a leaf node', async () => {
    const Q = 'deep-chain-single-' + Date.now();
    const node = await chain(Q, [{ name: 'only', data: { v: 1 } }], CONNECTION);

    expect(node.job.id).toBeTruthy();
    expect(node.job.name).toBe('only');
    expect(node.children).toBeUndefined();

    await flushQueue(Q);
  });

  it('rejects empty jobs array', async () => {
    await expect(chain('q', [], CONNECTION)).rejects.toThrow('at least one job');
  });

  it('two-job chain creates parent-child relationship', async () => {
    const Q = 'deep-chain-2-' + Date.now();
    const node = await chain(
      Q,
      [
        { name: 'step-A', data: { order: 1 } },
        { name: 'step-B', data: { order: 2 } },
      ],
      CONNECTION,
    );

    // step-A is root (runs last), step-B is child (runs first)
    expect(node.job.name).toBe('step-A');
    expect(node.children).toHaveLength(1);
    expect(node.children![0].job.name).toBe('step-B');

    // Verify parent is waiting-children in Valkey
    const k = buildKeys(Q);
    const parentState = await cleanupClient.hget(k.job(node.job.id), 'state');
    expect(String(parentState)).toBe('waiting-children');

    await flushQueue(Q);
  });

  it('three-job chain nests correctly: A -> B -> C (C runs first)', async () => {
    const Q = 'deep-chain-3-' + Date.now();
    const node = await chain(
      Q,
      [
        { name: 'A', data: {} },
        { name: 'B', data: {} },
        { name: 'C', data: {} },
      ],
      CONNECTION,
    );

    expect(node.job.name).toBe('A');
    expect(node.children).toHaveLength(1);
    expect(node.children![0].job.name).toBe('B');
    expect(node.children![0].children).toHaveLength(1);
    expect(node.children![0].children![0].job.name).toBe('C');

    await flushQueue(Q);
  });

  it('chain executes jobs in order (deepest child first)', async () => {
    const Q = 'deep-chain-exec-' + Date.now();
    const order: string[] = [];

    const node = await chain(
      Q,
      [
        { name: 'last', data: { step: 3 } },
        { name: 'middle', data: { step: 2 } },
        { name: 'first', data: { step: 1 } },
      ],
      CONNECTION,
    );

    const rootId = node.job.id;
    const { promise } = createTestWorker(
      Q,
      async (job: any) => {
        order.push(job.name);
        return { step: job.data.step };
      },
      rootId,
    );

    await promise;

    expect(order.indexOf('first')).toBeLessThan(order.indexOf('middle'));
    expect(order.indexOf('middle')).toBeLessThan(order.indexOf('last'));

    await flushQueue(Q);
  }, 20000);

  it('chain root receives children values via getChildrenValues()', async () => {
    const Q = 'deep-chain-values-' + Date.now();

    const node = await chain(
      Q,
      [
        { name: 'root', data: {} },
        { name: 'child', data: { x: 42 } },
      ],
      CONNECTION,
    );

    const rootId = node.job.id;
    let childValues: any = null;

    const { promise } = createTestWorker(
      Q,
      async (job: any) => {
        if (job.name === 'root') {
          childValues = await job.getChildrenValues();
          return { got: childValues };
        }
        return { doubled: job.data.x * 2 };
      },
      rootId,
    );

    await promise;

    expect(childValues).toBeTruthy();
    const vals = Object.values(childValues) as any[];
    expect(vals).toHaveLength(1);
    expect(vals[0].doubled).toBe(84);

    await flushQueue(Q);
  }, 20000);

  it('chain with job options (delay, priority) preserves opts', async () => {
    const Q = 'deep-chain-opts-' + Date.now();
    const node = await chain(
      Q,
      [
        { name: 'parent', data: {}, opts: { priority: 5 } },
        { name: 'child', data: {}, opts: { priority: 10 } },
      ],
      CONNECTION,
    );

    const k = buildKeys(Q);
    const parentOpts = await cleanupClient.hget(k.job(node.job.id), 'opts');
    const childOpts = await cleanupClient.hget(k.job(node.children![0].job.id), 'opts');

    const pParsed = JSON.parse(String(parentOpts));
    const cParsed = JSON.parse(String(childOpts));
    expect(pParsed.priority).toBe(5);
    expect(cParsed.priority).toBe(10);

    await flushQueue(Q);
  });

  it('long chain (5 jobs) completes end-to-end', async () => {
    const Q = 'deep-chain-5-' + Date.now();
    const jobs = Array.from({ length: 5 }, (_, i) => ({
      name: `step-${i}`,
      data: { idx: i },
    }));

    const node = await chain(Q, jobs, CONNECTION);
    const rootId = node.job.id;
    const processed: string[] = [];

    const { promise } = createTestWorker(
      Q,
      async (job: any) => {
        processed.push(job.name);
        return { idx: job.data.idx };
      },
      rootId,
      25000,
    );

    await promise;

    // step-4 should run first (deepest), step-0 last (root)
    expect(processed.indexOf('step-4')).toBeLessThan(processed.indexOf('step-3'));
    expect(processed.indexOf('step-1')).toBeLessThan(processed.indexOf('step-0'));
    expect(processed).toHaveLength(5);

    await flushQueue(Q);
  }, 30000);
});

// ---------------------------------------------------------------------------
// group()
// ---------------------------------------------------------------------------
describe('group()', () => {
  it('rejects empty jobs array', async () => {
    await expect(group('q', [], CONNECTION)).rejects.toThrow('at least one job');
  });

  it('single-job group creates __group__ parent with one child', async () => {
    const Q = 'deep-group-single-' + Date.now();
    const node = await group(Q, [{ name: 'only', data: { v: 1 } }], CONNECTION);

    expect(node.job.name).toBe('__group__');
    expect(node.children).toHaveLength(1);
    expect(node.children![0].job.name).toBe('only');

    await flushQueue(Q);
  });

  it('group creates parent waiting-children with N children in waiting', async () => {
    const Q = 'deep-group-state-' + Date.now();
    const node = await group(
      Q,
      [
        { name: 'a', data: { v: 1 } },
        { name: 'b', data: { v: 2 } },
        { name: 'c', data: { v: 3 } },
      ],
      CONNECTION,
    );

    const k = buildKeys(Q);
    const parentState = await cleanupClient.hget(k.job(node.job.id), 'state');
    expect(String(parentState)).toBe('waiting-children');

    expect(node.children).toHaveLength(3);
    for (const child of node.children!) {
      const childParentId = await cleanupClient.hget(k.job(child.job.id), 'parentId');
      expect(String(childParentId)).toBe(node.job.id);
    }

    // deps set should have 3 members
    const deps = await cleanupClient.smembers(k.deps(node.job.id));
    expect(deps.size).toBe(3);

    await flushQueue(Q);
  });

  it('group children run in parallel and parent completes after all', async () => {
    const Q = 'deep-group-exec-' + Date.now();
    const startTimes: Record<string, number> = {};

    const node = await group(
      Q,
      [
        { name: 'g1', data: { v: 1 } },
        { name: 'g2', data: { v: 2 } },
        { name: 'g3', data: { v: 3 } },
      ],
      CONNECTION,
    );

    const parentId = node.job.id;
    const completed: string[] = [];

    const { promise } = createTestWorker(
      Q,
      async (job: any) => {
        startTimes[job.name] = Date.now();
        if (job.name === '__group__') {
          return { type: 'group-parent' };
        }
        return { v: job.data.v * 10 };
      },
      parentId,
    );

    await promise;

    // Verify parent is completed
    const k = buildKeys(Q);
    const parentState = await cleanupClient.hget(k.job(parentId), 'state');
    expect(String(parentState)).toBe('completed');

    await flushQueue(Q);
  }, 20000);

  it('group parent getChildrenValues returns all child results', async () => {
    const Q = 'deep-group-values-' + Date.now();

    const node = await group(
      Q,
      [
        { name: 'c1', data: { x: 10 } },
        { name: 'c2', data: { x: 20 } },
      ],
      CONNECTION,
    );

    const parentId = node.job.id;
    let childValues: any = null;

    const { promise } = createTestWorker(
      Q,
      async (job: any) => {
        if (job.name === '__group__') {
          childValues = await job.getChildrenValues();
          return { childValues };
        }
        return { doubled: job.data.x * 2 };
      },
      parentId,
    );

    await promise;

    const vals = Object.values(childValues!) as any[];
    expect(vals).toHaveLength(2);
    const doubled = vals.map((v: any) => v.doubled).sort((a: number, b: number) => a - b);
    expect(doubled).toEqual([20, 40]);

    await flushQueue(Q);
  }, 20000);

  it('group with many children (8) completes', async () => {
    const Q = 'deep-group-many-' + Date.now();
    const jobs = Array.from({ length: 8 }, (_, i) => ({
      name: `item-${i}`,
      data: { idx: i },
    }));

    const node = await group(Q, jobs, CONNECTION);
    const parentId = node.job.id;
    const processed: string[] = [];

    const { promise } = createTestWorker(
      Q,
      async (job: any) => {
        processed.push(job.name);
        return { idx: job.data.idx };
      },
      parentId,
      25000,
    );

    await promise;

    // All 8 children + 1 parent = 9 jobs processed
    expect(processed).toHaveLength(9);
    expect(processed).toContain('__group__');
    for (let i = 0; i < 8; i++) {
      expect(processed).toContain(`item-${i}`);
    }

    await flushQueue(Q);
  }, 30000);
});

// ---------------------------------------------------------------------------
// chord()
// ---------------------------------------------------------------------------
describe('chord()', () => {
  it('rejects empty group jobs array', async () => {
    await expect(chord('q', [], { name: 'cb', data: {} }, CONNECTION)).rejects.toThrow('at least one group job');
  });

  it('chord creates callback parent with group jobs as children', async () => {
    const Q = 'deep-chord-struct-' + Date.now();
    const node = await chord(
      Q,
      [
        { name: 'task-a', data: { v: 1 } },
        { name: 'task-b', data: { v: 2 } },
      ],
      { name: 'summarize', data: {} },
      CONNECTION,
    );

    expect(node.job.name).toBe('summarize');
    expect(node.children).toHaveLength(2);
    expect(node.children![0].job.name).toBe('task-a');
    expect(node.children![1].job.name).toBe('task-b');

    const k = buildKeys(Q);
    const parentState = await cleanupClient.hget(k.job(node.job.id), 'state');
    expect(String(parentState)).toBe('waiting-children');

    await flushQueue(Q);
  });

  it('chord callback runs after all group jobs complete', async () => {
    const Q = 'deep-chord-exec-' + Date.now();
    const order: string[] = [];

    const node = await chord(
      Q,
      [
        { name: 'fetch-a', data: { url: 'a' } },
        { name: 'fetch-b', data: { url: 'b' } },
      ],
      { name: 'aggregate', data: {} },
      CONNECTION,
    );

    const callbackId = node.job.id;

    const { promise } = createTestWorker(
      Q,
      async (job: any) => {
        order.push(job.name);
        if (job.name === 'aggregate') {
          return { aggregated: true };
        }
        return { fetched: job.data.url };
      },
      callbackId,
    );

    await promise;

    // aggregate must come after both fetch-a and fetch-b
    expect(order.indexOf('aggregate')).toBeGreaterThan(order.indexOf('fetch-a'));
    expect(order.indexOf('aggregate')).toBeGreaterThan(order.indexOf('fetch-b'));

    await flushQueue(Q);
  }, 20000);

  it('chord callback receives children results', async () => {
    const Q = 'deep-chord-values-' + Date.now();

    const node = await chord(
      Q,
      [
        { name: 'compute-1', data: { n: 5 } },
        { name: 'compute-2', data: { n: 10 } },
      ],
      { name: 'reduce', data: {} },
      CONNECTION,
    );

    const callbackId = node.job.id;
    let callbackResult: any = null;

    const { promise } = createTestWorker(
      Q,
      async (job: any) => {
        if (job.name === 'reduce') {
          const vals = await job.getChildrenValues();
          const sum = Object.values(vals).reduce((acc: number, v: any) => acc + v.squared, 0);
          callbackResult = { sum };
          return callbackResult;
        }
        return { squared: job.data.n * job.data.n };
      },
      callbackId,
    );

    await promise;

    // 5^2 + 10^2 = 25 + 100 = 125
    expect(callbackResult).toEqual({ sum: 125 });

    await flushQueue(Q);
  }, 20000);

  it('chord with single group job works', async () => {
    const Q = 'deep-chord-single-' + Date.now();

    const node = await chord(
      Q,
      [{ name: 'sole-task', data: { x: 7 } }],
      { name: 'finish', data: {} },
      CONNECTION,
    );

    const callbackId = node.job.id;
    let finishValues: any = null;

    const { promise } = createTestWorker(
      Q,
      async (job: any) => {
        if (job.name === 'finish') {
          finishValues = await job.getChildrenValues();
          return { done: true };
        }
        return { result: job.data.x + 1 };
      },
      callbackId,
    );

    await promise;

    const vals = Object.values(finishValues!) as any[];
    expect(vals).toHaveLength(1);
    expect(vals[0].result).toBe(8);

    await flushQueue(Q);
  }, 20000);

  it('chord callback opts are preserved', async () => {
    const Q = 'deep-chord-opts-' + Date.now();

    const node = await chord(
      Q,
      [{ name: 'w', data: {} }],
      { name: 'cb', data: {}, opts: { priority: 3 } },
      CONNECTION,
    );

    const k = buildKeys(Q);
    const optsRaw = await cleanupClient.hget(k.job(node.job.id), 'opts');
    const parsed = JSON.parse(String(optsRaw));
    expect(parsed.priority).toBe(3);

    await flushQueue(Q);
  });
});

// ---------------------------------------------------------------------------
// FlowProducer.addBulk
// ---------------------------------------------------------------------------
describe('FlowProducer.addBulk()', () => {
  it('creates multiple independent flows', async () => {
    const Q = 'deep-bulk-' + Date.now();
    const flow = new FlowProducer({ connection: CONNECTION });

    const nodes = await flow.addBulk([
      {
        name: 'flow-A',
        queueName: Q,
        data: { f: 'A' },
        children: [
          { name: 'a-child', queueName: Q, data: {} },
        ],
      },
      {
        name: 'flow-B',
        queueName: Q,
        data: { f: 'B' },
        children: [
          { name: 'b-child-1', queueName: Q, data: {} },
          { name: 'b-child-2', queueName: Q, data: {} },
        ],
      },
    ]);

    expect(nodes).toHaveLength(2);
    expect(nodes[0].children).toHaveLength(1);
    expect(nodes[1].children).toHaveLength(2);

    // Verify distinct IDs
    const allIds = [
      nodes[0].job.id,
      nodes[0].children![0].job.id,
      nodes[1].job.id,
      nodes[1].children![0].job.id,
      nodes[1].children![1].job.id,
    ];
    expect(new Set(allIds).size).toBe(5);

    await flow.close();
    await flushQueue(Q);
  });

  it('addBulk flows complete independently via worker', async () => {
    const Q = 'deep-bulk-exec-' + Date.now();
    const flow = new FlowProducer({ connection: CONNECTION });

    const nodes = await flow.addBulk([
      {
        name: 'p1',
        queueName: Q,
        data: {},
        children: [{ name: 'c1', queueName: Q, data: { v: 1 } }],
      },
      {
        name: 'p2',
        queueName: Q,
        data: {},
        children: [{ name: 'c2', queueName: Q, data: { v: 2 } }],
      },
    ]);

    const processed: string[] = [];
    const parentIds = new Set([nodes[0].job.id, nodes[1].job.id]);
    let completedParents = 0;

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 20000);
      const worker = new Worker(
        Q,
        async (job: any) => {
          processed.push(job.name);
          return { ok: true };
        },
        {
          connection: CONNECTION,
          concurrency: 4,
          blockTimeout: 500,
          promotionInterval: 500,
        },
      );

      worker.on('completed', (job: any) => {
        if (parentIds.has(job.id)) {
          completedParents++;
          if (completedParents >= 2) {
            clearTimeout(timeout);
            setTimeout(() => worker.close(true).then(resolve), 200);
          }
        }
      });
      worker.on('error', () => {});
    });

    await done;

    expect(processed).toContain('c1');
    expect(processed).toContain('c2');
    expect(processed).toContain('p1');
    expect(processed).toContain('p2');

    await flow.close();
    await flushQueue(Q);
  }, 25000);
});

// ---------------------------------------------------------------------------
// Mixed / edge cases
// ---------------------------------------------------------------------------
describe('workflow edge cases', () => {
  it('chain where child fails - parent stays in waiting-children', async () => {
    const Q = 'deep-chain-fail-' + Date.now();

    const node = await chain(
      Q,
      [
        { name: 'parent', data: {} },
        { name: 'failing-child', data: {} },
      ],
      CONNECTION,
    );

    const parentId = node.job.id;
    const k = buildKeys(Q);

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker = new Worker(
        Q,
        async (job: any) => {
          if (job.name === 'failing-child') {
            throw new Error('child-error');
          }
          return {};
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, promotionInterval: 500 },
      );

      worker.on('failed', (job: any) => {
        if (job.name === 'failing-child') {
          clearTimeout(timeout);
          setTimeout(() => worker.close(true).then(resolve), 300);
        }
      });
      worker.on('error', () => {});
    });

    await done;

    // Parent should still be waiting-children (child failed, not completed)
    const parentState = await cleanupClient.hget(k.job(parentId), 'state');
    expect(String(parentState)).toBe('waiting-children');

    await flushQueue(Q);
  }, 15000);

  it('FlowProducer leaf job (no children) creates standalone job', async () => {
    const Q = 'deep-flow-leaf-' + Date.now();
    const flow = new FlowProducer({ connection: CONNECTION });

    const node = await flow.add({
      name: 'standalone',
      queueName: Q,
      data: { solo: true },
    });

    expect(node.job.id).toBeTruthy();
    expect(node.children).toBeUndefined();

    // Should be in stream (waiting)
    const k = buildKeys(Q);
    const state = await cleanupClient.hget(k.job(node.job.id), 'state');
    expect(String(state)).toBe('waiting');

    await flow.close();
    await flushQueue(Q);
  });

  it('chord where one group job fails - callback stays waiting-children', async () => {
    const Q = 'deep-chord-fail-' + Date.now();

    const node = await chord(
      Q,
      [
        { name: 'ok-task', data: {} },
        { name: 'fail-task', data: {} },
      ],
      { name: 'callback', data: {} },
      CONNECTION,
    );

    const callbackId = node.job.id;
    const k = buildKeys(Q);
    const processed: string[] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      let failSeen = false;
      let okSeen = false;
      const worker = new Worker(
        Q,
        async (job: any) => {
          processed.push(job.name);
          if (job.name === 'fail-task') {
            throw new Error('deliberate fail');
          }
          return { ok: true };
        },
        { connection: CONNECTION, concurrency: 2, blockTimeout: 500, promotionInterval: 500 },
      );

      worker.on('failed', (job: any) => {
        if (job.name === 'fail-task') failSeen = true;
        maybeResolve();
      });
      worker.on('completed', (job: any) => {
        if (job.name === 'ok-task') okSeen = true;
        maybeResolve();
      });
      function maybeResolve() {
        if (failSeen && okSeen) {
          clearTimeout(timeout);
          setTimeout(() => worker.close(true).then(resolve), 300);
        }
      }
      worker.on('error', () => {});
    });

    await done;

    // callback should remain in waiting-children because fail-task did not complete
    const cbState = await cleanupClient.hget(k.job(callbackId), 'state');
    expect(String(cbState)).toBe('waiting-children');

    await flushQueue(Q);
  }, 15000);
});
