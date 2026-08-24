/**
 * Flow (parent-child job trees) integration tests.
 * Runs against both standalone (:6379) and cluster (:7000).
 */
import { readFileSync } from 'node:fs';
import { it, expect, beforeAll, afterAll, describe, vi } from 'vitest';
import { describeEachMode, createCleanupClient, flushQueue, waitFor } from './helpers/fixture';

const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { FlowProducer } = require('../dist/flow-producer') as typeof import('../src/flow-producer');
const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Scheduler } = require('../dist/scheduler') as typeof import('../src/scheduler');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

describe('DAG wiring source invariants', () => {
  it('does not FCALL registerParent from the cross-queue Phase B batch', () => {
    const source = readFileSync('src/flow-producer.ts', 'utf8');
    expect(source).not.toMatch(/batchB\.fcall\(\s*['"]glidemq_registerParent/);
  });

  it('guards completeChild against recreating deleted parent hashes', () => {
    const source = readFileSync('src/functions/glidemq.lua', 'utf8');
    const start = source.indexOf("redis.register_function('glidemq_completeChild'");
    const end = source.indexOf("redis.register_function('glidemq_registerParent'", start);
    expect(source.slice(start, end)).toMatch(/redis\.call\('EXISTS', parentJobKey\) == 0/);
  });
});

describe('Scheduler cross-queue notification parsing', () => {
  it('handles JSON, legacy, malformed, stale, and failed notifications', async () => {
    const jsonMember = JSON.stringify(['parent', 'p1', 'glide:{child}:c1']);
    const legacyMember = 'parent\tp2\tglide:{child}:c2';
    const malformedMember = '{"not":"a notification"}';
    const failedMember = JSON.stringify(['parent', 'p3', 'glide:{child}:c3']);
    const errors: Error[] = [];
    const client = {
      smembers: vi.fn().mockResolvedValue(new Set([jsonMember, legacyMember, malformedMember, failedMember])),
      fcall: vi.fn(async (_name: string, _keys: string[], args: string[]) => {
        if (args[1] === 'p2') return -1;
        if (args[1] === 'p3') throw new Error('temporary connection failure');
        return 0;
      }),
      srem: vi.fn().mockResolvedValue(1),
    } as any;

    await new Scheduler(client, buildKeys('child'), {
      onError: (err: Error) => errors.push(err),
    }).flushCrossQueueParentNotifies();

    expect(client.fcall).toHaveBeenCalledTimes(3);
    expect(client.srem).toHaveBeenCalledTimes(2);
    expect(errors.map((error) => error.message)).toEqual(['temporary connection failure']);
  });

  it('reports malformed queue key metadata without attempting completion', async () => {
    const client = { smembers: vi.fn().mockResolvedValue(new Set(['ignored'])) } as any;
    const errors: Error[] = [];
    const keys = { ...buildKeys('child'), id: 'glide:child:id' };

    await new Scheduler(client, keys, { onError: (err: Error) => errors.push(err) }).flushCrossQueueParentNotifies();

    expect(errors[0]?.message).toMatch(/Invalid queue id key/);
  });
});

describeEachMode('FlowProducer', (CONNECTION) => {
  let cleanupClient: any;
  const Q = 'test-flow-' + Date.now();

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('creates parent in waiting-children state with children in waiting state', async () => {
    const flow = new FlowProducer({ connection: CONNECTION });

    const node = await flow.add({
      name: 'parent-job',
      queueName: Q,
      data: { type: 'parent' },
      children: [
        { name: 'child-1', queueName: Q, data: { idx: 1 } },
        { name: 'child-2', queueName: Q, data: { idx: 2 } },
      ],
    });

    expect(node.job.id).toBeTruthy();
    expect(node.children).toHaveLength(2);
    expect(node.children![0].job.name).toBe('child-1');
    expect(node.children![1].job.name).toBe('child-2');

    // Verify parent state
    const k = buildKeys(Q);
    const parentState = await cleanupClient.hget(k.job(node.job.id), 'state');
    expect(String(parentState)).toBe('waiting-children');

    // Verify children have parentId set
    for (const childNode of node.children!) {
      const cParentId = await cleanupClient.hget(k.job(childNode.job.id), 'parentId');
      expect(String(cParentId)).toBe(node.job.id);
    }

    // Verify deps set contains both children
    const deps = await cleanupClient.smembers(k.deps(node.job.id));
    expect(deps.size).toBe(2);

    await flow.close();
  });

  it('persists ordering metadata for flow parent and children', async () => {
    const qName = Q + '-ordering';
    const flow = new FlowProducer({ connection: CONNECTION });

    const node = await flow.add({
      name: 'parent-ordered',
      queueName: qName,
      data: { type: 'parent' },
      opts: { ordering: { key: 'parent-key' } },
      children: [
        { name: 'child-ordered', queueName: qName, data: { idx: 1 }, opts: { ordering: { key: 'child-key' } } },
      ],
    });

    const k = buildKeys(qName);
    // Ordering-key jobs use the group path: groupKey + orderingSeq
    const parentGroupKey = await cleanupClient.hget(k.job(node.job.id), 'groupKey');
    const parentOrderingSeq = await cleanupClient.hget(k.job(node.job.id), 'orderingSeq');
    const childGroupKey = await cleanupClient.hget(k.job(node.children![0].job.id), 'groupKey');
    const childOrderingSeq = await cleanupClient.hget(k.job(node.children![0].job.id), 'orderingSeq');

    expect(String(parentGroupKey)).toBe('parent-key');
    expect(Number(parentOrderingSeq)).toBe(1);
    expect(String(childGroupKey)).toBe('child-key');
    expect(Number(childOrderingSeq)).toBe(1);

    await flow.close();
    await flushQueue(cleanupClient, qName);
  });

  it('parent completes after all children are processed', async () => {
    const qName = Q + '-complete';
    const flow = new FlowProducer({ connection: CONNECTION });

    const node = await flow.add({
      name: 'parent',
      queueName: qName,
      data: { type: 'parent' },
      children: [
        { name: 'child-a', queueName: qName, data: { v: 'a' } },
        { name: 'child-b', queueName: qName, data: { v: 'b' } },
      ],
    });

    const parentId = node.job.id;
    const completedJobs: string[] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);
      const worker = new Worker(
        qName,
        async (job: any) => {
          completedJobs.push(job.name);
          return { result: job.data.v || 'parent-done' };
        },
        { connection: CONNECTION, concurrency: 2, blockTimeout: 1000 },
      );

      worker.on('completed', (job: any) => {
        if (job.id === parentId) {
          clearTimeout(timeout);
          setTimeout(() => worker.close(true).then(resolve), 200);
        }
      });
      worker.on('error', () => {});
    });

    await done;

    // Parent should be completed
    const k = buildKeys(qName);
    const parentState = await cleanupClient.hget(k.job(parentId), 'state');
    expect(String(parentState)).toBe('completed');

    // Both children and parent should be completed
    expect(completedJobs).toContain('child-a');
    expect(completedJobs).toContain('child-b');
    expect(completedJobs).toContain('parent');

    await flow.close();
    await flushQueue(cleanupClient, qName);
  }, 20000);

  it('nested flow (grandchild)', async () => {
    const qName = Q + '-nested';
    const flow = new FlowProducer({ connection: CONNECTION });

    const node = await flow.add({
      name: 'grandparent',
      queueName: qName,
      data: { level: 0 },
      children: [
        {
          name: 'parent-child',
          queueName: qName,
          data: { level: 1 },
          children: [{ name: 'grandchild', queueName: qName, data: { level: 2 } }],
        },
      ],
    });

    expect(node.job.name).toBe('grandparent');
    expect(node.children).toHaveLength(1);
    expect(node.children![0].job.name).toBe('parent-child');
    expect(node.children![0].children).toHaveLength(1);
    expect(node.children![0].children![0].job.name).toBe('grandchild');

    const completedNames: string[] = [];
    const grandparentId = node.job.id;

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 20000);
      const worker = new Worker(
        qName,
        async (job: any) => {
          completedNames.push(job.name);
          return { level: job.data.level };
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
      );

      worker.on('completed', (job: any) => {
        if (job.id === grandparentId) {
          clearTimeout(timeout);
          setTimeout(() => worker.close(true).then(resolve), 200);
        }
      });
      worker.on('error', () => {});
    });

    await done;

    // Grandchild should complete first, then parent-child, then grandparent
    expect(completedNames.indexOf('grandchild')).toBeLessThan(completedNames.indexOf('parent-child'));
    expect(completedNames.indexOf('parent-child')).toBeLessThan(completedNames.indexOf('grandparent'));

    await flow.close();
    await flushQueue(cleanupClient, qName);
  }, 25000);

  it('nested flows still complete when workers are already running', async () => {
    const qName = Q + '-nested-live';
    const flow = new FlowProducer({ connection: CONNECTION });
    const worker = new Worker(
      qName,
      async (job: any) => {
        return { level: job.data.level, round: job.data.round ?? -1 };
      },
      { connection: CONNECTION, concurrency: 5, blockTimeout: 1000, stalledInterval: 60000 },
    );
    worker.on('error', () => {});

    await worker.waitUntilReady();

    const k = buildKeys(qName);
    for (let round = 0; round < 8; round++) {
      const node = await flow.add({
        name: `grandparent-${round}`,
        queueName: qName,
        data: { level: 0, round },
        children: [
          {
            name: `parent-child-${round}`,
            queueName: qName,
            data: { level: 1, round },
            children: [{ name: `grandchild-${round}`, queueName: qName, data: { level: 2, round } }],
          },
        ],
      });

      const deadline = Date.now() + 10000;
      let state = '';
      while (Date.now() < deadline) {
        const rawState = await cleanupClient.hget(k.job(node.job.id), 'state');
        state = rawState ? String(rawState) : '';
        if (state === 'completed') break;
        await new Promise((resolve) => setTimeout(resolve, 100));
      }

      expect(state).toBe('completed');
    }

    await worker.close(true);
    await flow.close();
    await flushQueue(cleanupClient, qName);
  }, 60000);

  it('nested cross-queue flow completes when workers are already running', async () => {
    const parentQ = Q + '-xq-parent';
    const childQ = Q + '-xq-child';
    const flow = new FlowProducer({ connection: CONNECTION });
    let middleStarted = false;
    let releaseMiddle = () => {};
    const middleGate = new Promise<void>((resolve) => {
      releaseMiddle = resolve;
    });
    const parentWorker = new Worker(parentQ, async () => ({ ok: true }), {
      connection: CONNECTION,
      concurrency: 4,
      blockTimeout: 200,
      stalledInterval: 60000,
    });
    const childWorker = new Worker(
      childQ,
      async (job: any) => {
        if (job.data.level === 1) {
          middleStarted = true;
          await middleGate;
        }
        return { ok: true };
      },
      {
        connection: CONNECTION,
        concurrency: 4,
        blockTimeout: 200,
        stalledInterval: 60000,
      },
    );
    parentWorker.on('error', () => {});
    childWorker.on('error', () => {});
    await parentWorker.waitUntilReady();
    await childWorker.waitUntilReady();

    // Hold the recursive child flow after its middle job is active. The outer
    // parent is then created and wired while that worker still has a stale job
    // snapshot with no parentId/parentQueue fields.
    const originalAddFlowRecursive = (flow as any).addFlowRecursive;
    let nesting = 0;
    (flow as any).addFlowRecursive = async function (...args: any[]) {
      nesting++;
      try {
        const result = await originalAddFlowRecursive.apply(this, args);
        if (nesting === 2) await waitFor(() => middleStarted, 5000);
        return result;
      } finally {
        nesting--;
      }
    };

    try {
      const node = await flow.add({
        name: 'grandparent',
        queueName: parentQ,
        data: { level: 0 },
        children: [
          {
            name: 'parent-child',
            queueName: childQ,
            data: { level: 1 },
            children: [{ name: 'grandchild', queueName: childQ, data: { level: 2 } }],
          },
        ],
      });

      releaseMiddle();
      const parentKeys = buildKeys(parentQ);
      await waitFor(
        async () => String(await cleanupClient.hget(parentKeys.job(node.job.id), 'state')) === 'completed',
        10000,
      );
    } finally {
      releaseMiddle();
      await parentWorker.close(true);
      await childWorker.close(true);
      await flow.close();
      await flushQueue(cleanupClient, parentQ);
      await flushQueue(cleanupClient, childQ);
    }
  }, 20000);

  it('completeChild does not recreate a removed parent hash', async () => {
    const parentQ = Q + '-removed-parent';
    const flow = new FlowProducer({ connection: CONNECTION });
    try {
      const node = await flow.add({
        name: 'removed-parent',
        queueName: parentQ,
        data: {},
        children: [{ name: 'child', queueName: parentQ, data: {} }],
      });
      const parentKeys = buildKeys(parentQ);
      const parentJobKey = parentKeys.job(node.job.id);
      await cleanupClient.del([parentJobKey]);

      await cleanupClient.fcall(
        'glidemq_completeChild',
        [parentKeys.deps(node.job.id), parentJobKey, parentKeys.stream, parentKeys.events],
        [`${parentKeys.name}:${node.children![0].job.id}`, node.job.id],
      );

      expect(await cleanupClient.exists([parentJobKey])).toBe(0);
    } finally {
      await flow.close();
      await flushQueue(cleanupClient, parentQ);
    }
  });

  it('reconciles a removed nested cross-queue child', async () => {
    const parentQ = Q + '-removed-parent';
    const childQ = Q + '-removed-child';
    const flow = new FlowProducer({ connection: CONNECTION });
    const parentWorker = new Worker(parentQ, async () => ({ ok: true }), {
      connection: CONNECTION,
      blockTimeout: 200,
      stalledInterval: 60000,
    });
    parentWorker.on('error', () => {});
    await parentWorker.waitUntilReady();

    let nesting = 0;
    const originalAddFlowRecursive = (flow as any).addFlowRecursive;
    (flow as any).addFlowRecursive = async function (...args: any[]) {
      nesting++;
      try {
        const result = await originalAddFlowRecursive.apply(this, args);
        if (nesting === 2) {
          const childKeys = buildKeys(args[1].queueName);
          await cleanupClient.del([childKeys.job(result.job.id)]);
        }
        return result;
      } finally {
        nesting--;
      }
    };

    try {
      const node = await flow.add({
        name: 'outer-parent',
        queueName: parentQ,
        data: {},
        children: [
          {
            name: 'nested-parent',
            queueName: childQ,
            data: {},
            children: [{ name: 'nested-child', queueName: childQ, data: {} }],
          },
        ],
      });

      const parentKeys = buildKeys(parentQ);
      await waitFor(
        async () => String(await cleanupClient.hget(parentKeys.job(node.job.id), 'state')) === 'completed',
        10000,
      );
    } finally {
      await parentWorker.close(true);
      await flow.close();
      await flushQueue(cleanupClient, parentQ);
      await flushQueue(cleanupClient, childQ);
    }
  }, 20000);

  it('reconciles a child deleted between the existence check and parent wiring', async () => {
    const queueName = Q + '-toctou';
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
      }
      return result;
    };

    try {
      const node = await flow.add({
        name: 'toctou-parent',
        queueName,
        data: {},
        children: [
          {
            name: 'toctou-nested-parent',
            queueName,
            data: {},
            children: [{ name: 'toctou-leaf', queueName, data: {} }],
          },
        ],
      });

      expect(deleted).toBe(true);
      const nestedId = node.children![0].job.id;
      const keys = buildKeys(queueName);
      expect(await cleanupClient.exists([keys.job(nestedId)])).toBe(0);
      expect(await cleanupClient.exists([keys.parents(nestedId)])).toBe(0);
      worker = new Worker(queueName, async () => ({ ok: true }), {
        connection: CONNECTION,
        blockTimeout: 200,
        stalledInterval: 60000,
        promotionInterval: 100,
      });
      worker.on('error', () => {});
      await worker.waitUntilReady();
      await waitFor(
        async () => String(await cleanupClient.hget(keys.job(node.job.id), 'state')) === 'completed',
        10000,
      );
    } finally {
      client.exists = originalExists;
      if (worker) await worker.close(true);
      await flow.close();
      await flushQueue(cleanupClient, queueName);
    }
  }, 20000);

  it('reconciles a cross-queue child deleted between the existence check and parent wiring', async () => {
    const parentQ = Q + '-toctou-parent';
    const childQ = Q + '-toctou-child';
    const flow = new FlowProducer({ connection: CONNECTION });
    const client = await (flow as any).getClient();
    const originalExists = client.exists.bind(client);
    let deleted = false;
    client.exists = async (keys: string[]) => {
      const result = await originalExists(keys);
      const key = String(keys[0] ?? '');
      if (!deleted && result === 1 && key.includes(`{${childQ}}:job:`)) {
        deleted = true;
        await cleanupClient.del([key]);
      }
      return result;
    };

    let worker: any;
    try {
      const node = await flow.add({
        name: 'cross-toctou-parent',
        queueName: parentQ,
        data: {},
        children: [
          {
            name: 'cross-toctou-nested-parent',
            queueName: childQ,
            data: {},
            children: [{ name: 'cross-toctou-leaf', queueName: childQ, data: {} }],
          },
        ],
      });

      expect(deleted).toBe(true);
      const childKeys = buildKeys(childQ);
      const nestedId = node.children![0].job.id;
      expect(await cleanupClient.exists([childKeys.job(nestedId)])).toBe(0);
      expect(await cleanupClient.exists([childKeys.parents(nestedId)])).toBe(0);

      worker = new Worker(parentQ, async () => ({ ok: true }), {
        connection: CONNECTION,
        blockTimeout: 200,
        stalledInterval: 60000,
        promotionInterval: 100,
      });
      worker.on('error', () => {});
      await worker.waitUntilReady();
      const parentKeys = buildKeys(parentQ);
      await waitFor(
        async () => String(await cleanupClient.hget(parentKeys.job(node.job.id), 'state')) === 'completed',
        10000,
      );
    } finally {
      client.exists = originalExists;
      await worker?.close(true);
      await flow.close();
      await flushQueue(cleanupClient, parentQ);
      await flushQueue(cleanupClient, childQ);
    }
  }, 20000);

  it('flushes JSON pending notifications for queue names containing tabs', async () => {
    const parentQ = Q + '-tab-parent\tqueue';
    const childQ = parentQ;
    const flow = new FlowProducer({ connection: CONNECTION });

    try {
      const node = await flow.add({
        name: 'tab-parent',
        queueName: parentQ,
        data: {},
        children: [{ name: 'tab-child', queueName: childQ, data: {} }],
      });
      const childId = node.children![0].job.id;
      const childKeys = buildKeys(childQ);
      const pending = JSON.stringify([parentQ, node.job.id, `glide:{${childQ}}:${childId}`]);
      await cleanupClient.sadd(childKeys.xqPending, [pending]);

      await new Scheduler(cleanupClient, childKeys).flushCrossQueueParentNotifies();

      expect(String(await cleanupClient.hget(buildKeys(parentQ).job(node.job.id), 'state'))).toBe('waiting');
      expect(await cleanupClient.sismember(childKeys.xqPending, pending)).toBe(false);
    } finally {
      await flow.close();
      await flushQueue(cleanupClient, parentQ);
      await flushQueue(cleanupClient, childQ);
    }
  }, 20000);

  it('flushes pending notifications with a custom prefix containing braces', async () => {
    const prefix = 'test-flow:{tenant}';
    const parentQ = Q + '-prefix-parent';
    const childQ = Q + '-prefix-child';
    const flow = new FlowProducer({ connection: CONNECTION, prefix });

    try {
      const node = await flow.add({
        name: 'prefix-parent',
        queueName: parentQ,
        data: {},
        children: [{ name: 'prefix-child', queueName: childQ, data: {} }],
      });
      const childId = node.children![0].job.id;
      const childKeys = buildKeys(childQ, prefix);
      const pending = JSON.stringify([parentQ, node.job.id, `${prefix}:{${childQ}}:${childId}`]);
      await cleanupClient.sadd(childKeys.xqPending, [pending]);

      await new Scheduler(cleanupClient, childKeys).flushCrossQueueParentNotifies();

      expect(String(await cleanupClient.hget(buildKeys(parentQ, prefix).job(node.job.id), 'state'))).toBe('waiting');
      expect(await cleanupClient.sismember(childKeys.xqPending, pending)).toBe(false);
    } finally {
      await flow.close();
      await flushQueue(cleanupClient, parentQ, prefix);
      await flushQueue(cleanupClient, childQ, prefix);
    }
  });

  it('obliterate removes cross-queue pending notifications', async () => {
    const queueName = Q + '-obliterate-xq';
    const queue = new Queue(queueName, { connection: CONNECTION });
    const keys = buildKeys(queueName);
    try {
      await cleanupClient.sadd(keys.xqPending, ['pending']);
      await queue.obliterate({ force: true });
      expect(await cleanupClient.exists([keys.xqPending])).toBe(0);
    } finally {
      await queue.close();
    }
  });

  it('parent getChildrenValues returns all child results', async () => {
    const qName = Q + '-values';
    const flow = new FlowProducer({ connection: CONNECTION });

    const node = await flow.add({
      name: 'parent',
      queueName: qName,
      data: {},
      children: [
        { name: 'c1', queueName: qName, data: { x: 10 } },
        { name: 'c2', queueName: qName, data: { x: 20 } },
      ],
    });

    const parentId = node.job.id;
    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);
      const worker = new Worker(
        qName,
        async (job: any) => {
          if (job.name === 'parent') {
            const childValues = await job.getChildrenValues();
            return { childValues };
          }
          return { doubled: job.data.x * 2 };
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
      );

      worker.on('completed', (job: any) => {
        if (job.id === parentId) {
          clearTimeout(timeout);
          setTimeout(() => worker.close(true).then(resolve), 200);
        }
      });
      worker.on('error', () => {});
    });

    await done;

    // Verify parent has completed with child values
    const k = buildKeys(qName);
    const returnval = await cleanupClient.hget(k.job(parentId), 'returnvalue');
    const parsed = JSON.parse(String(returnval));
    // childValues is a Record<depsMember, result>
    const values = Object.values(parsed.childValues) as any[];
    expect(values).toHaveLength(2);
    const doubled = values.map((v: any) => v.doubled).sort((a: number, b: number) => a - b);
    expect(doubled).toEqual([20, 40]);

    await flow.close();
    await flushQueue(cleanupClient, qName);
  }, 20000);

  it('addBulk creates multiple flows', async () => {
    const qName = Q + '-bulk';
    const flow = new FlowProducer({ connection: CONNECTION });

    const nodes = await flow.addBulk([
      {
        name: 'flow-1',
        queueName: qName,
        data: { f: 1 },
        children: [{ name: 'f1-child', queueName: qName, data: { c: 1 } }],
      },
      {
        name: 'flow-2',
        queueName: qName,
        data: { f: 2 },
        children: [{ name: 'f2-child', queueName: qName, data: { c: 2 } }],
      },
    ]);

    expect(nodes).toHaveLength(2);
    expect(nodes[0].job.name).toBe('flow-1');
    expect(nodes[1].job.name).toBe('flow-2');
    expect(nodes[0].children).toHaveLength(1);
    expect(nodes[1].children).toHaveLength(1);

    await flow.close();
    await flushQueue(cleanupClient, qName);
  });
});
