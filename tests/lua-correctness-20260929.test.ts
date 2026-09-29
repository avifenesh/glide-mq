/**
 * Server-function correctness regressions (2026-09-29 audit).
 *
 * Run: npx vitest run tests/lua-correctness-20260929.test.ts
 */
import { afterAll, beforeAll, expect, it } from 'vitest';
import { createCleanupClient, describeEachMode, flushQueue, waitFor } from './helpers/fixture';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { FlowProducer } = require('../dist/flow-producer') as typeof import('../src/flow-producer');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
const { completeAndFetchNext, reclaimStalled, CONSUMER_GROUP } =
  require('../dist/functions') as typeof import('../src/functions');

describeEachMode('Lua correctness 2026-09-29', (CONNECTION) => {
  let cleanupClient: any;
  const queues: string[] = [];

  function uniqueQueue(prefix: string): string {
    const name = `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    queues.push(name);
    return name;
  }

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await Promise.all(queues.map((q) => flushQueue(cleanupClient, q).catch(() => {})));
    cleanupClient.close();
  });

  async function hget(key: string, field: string): Promise<string | null> {
    const v = await cleanupClient.hget(key, field);
    return v == null ? null : String(v);
  }

  it('removing every same-queue child releases the parent', async () => {
    const Q = uniqueQueue('lc-rm-child');
    const flow = new FlowProducer({ connection: CONNECTION });
    const queue = new Queue(Q, { connection: CONNECTION });
    try {
      const node = await flow.add({
        name: 'parent',
        queueName: Q,
        data: {},
        children: [
          { name: 'c1', queueName: Q, data: {} },
          { name: 'c2', queueName: Q, data: {} },
        ],
      });
      const k = buildKeys(Q);
      for (const child of node.children!) {
        const job = await queue.getJob(child.job.id);
        await job!.remove();
      }
      expect(await hget(k.job(node.job.id), 'state')).toBe('waiting');
    } finally {
      await flow.close();
      await queue.close();
    }
  });

  it('removing a child after a sibling completed releases the parent once', async () => {
    const Q = uniqueQueue('lc-rm-child-mix');
    const flow = new FlowProducer({ connection: CONNECTION });
    const queue = new Queue(Q, { connection: CONNECTION });
    try {
      const node = await flow.add({
        name: 'parent',
        queueName: Q,
        data: {},
        children: [
          { name: 'c1', queueName: Q, data: {}, opts: { delay: 60_000 } },
          { name: 'c2', queueName: Q, data: {} },
        ],
      });
      const k = buildKeys(Q);
      const processed: string[] = [];
      const worker = new Worker(
        Q,
        async (job) => {
          processed.push(job.name);
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 50, stalledInterval: 60_000 },
      );
      worker.on('error', () => {});
      try {
        await waitFor(() => processed.includes('c2'), 5000, 25);
        const delayed = await queue.getJob(node.children![0].job.id);
        await delayed!.remove();
        await waitFor(() => processed.includes('parent'), 5000, 25);
        expect(await hget(k.job(node.job.id), 'depsCompleted')).toBe('2');
        expect(processed.filter((n) => n === 'parent')).toHaveLength(1);
      } finally {
        await worker.close(true);
      }
    } finally {
      await flow.close();
      await queue.close();
    }
  });

  it('removing a cross-queue child releases the parent', async () => {
    const PQ = uniqueQueue('lc-rm-xq-parent');
    const CQ = uniqueQueue('lc-rm-xq-child');
    const flow = new FlowProducer({ connection: CONNECTION });
    const childQueue = new Queue(CQ, { connection: CONNECTION });
    try {
      const node = await flow.add({
        name: 'parent',
        queueName: PQ,
        data: {},
        // A nested child keeps the flow slot-safe in cluster mode.
        children: [{ name: 'c1', queueName: CQ, data: {}, children: [{ name: 'gc', queueName: CQ, data: {} }] }],
      });
      const job = await childQueue.getJob(node.children![0].job.id);
      await job!.remove();
      expect(await hget(buildKeys(PQ).job(node.job.id), 'state')).toBe('waiting');
      expect((await cleanupClient.smembers(buildKeys(CQ).xqPending)).size).toBe(0);
    } finally {
      await flow.close();
      await childQueue.close();
    }
  });

  it('removing a parent leaves no deps set and later child completion is harmless', async () => {
    const Q = uniqueQueue('lc-rm-parent');
    const flow = new FlowProducer({ connection: CONNECTION });
    const queue = new Queue(Q, { connection: CONNECTION });
    try {
      const node = await flow.add({
        name: 'parent',
        queueName: Q,
        data: {},
        children: [{ name: 'c1', queueName: Q, data: {} }],
      });
      const k = buildKeys(Q);
      const parent = await queue.getJob(node.job.id);
      await parent!.remove();
      expect(await cleanupClient.exists([k.deps(node.job.id)])).toBe(0);
      const worker = new Worker(Q, async () => 'ok', {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 50,
        stalledInterval: 60_000,
      });
      worker.on('error', () => {});
      try {
        await waitFor(async () => (await hget(k.job(node.children![0].job.id), 'state')) === 'completed', 5000, 25);
        expect(await cleanupClient.exists([k.job(node.job.id)])).toBe(0);
      } finally {
        await worker.close(true);
      }
    } finally {
      await flow.close();
      await queue.close();
    }
  });
  async function processedBy(Q: string, names: string[], timeoutMs = 5000): Promise<string[]> {
    const processed: string[] = [];
    const worker = new Worker(
      Q,
      async (job) => {
        processed.push(job.name);
        return 'ok';
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 50, stalledInterval: 60_000 },
    );
    worker.on('error', () => {});
    try {
      await waitFor(() => names.every((n) => processed.includes(n)), timeoutMs, 25).catch(() => {});
    } finally {
      await worker.close(true);
    }
    return processed;
  }

  it('drain closes ordering holes so later ordered jobs still run', async () => {
    const Q = uniqueQueue('lc-drain-ord');
    const queue = new Queue(Q, { connection: CONNECTION });
    try {
      for (let i = 0; i < 3; i++) await queue.add(`old-${i}`, {}, { ordering: { key: 'k' } });
      await queue.add('old-delayed', {}, { ordering: { key: 'k' }, delay: 60_000 });
      await queue.drain(true);
      await queue.add('fresh', {}, { ordering: { key: 'k' } });
      expect(await processedBy(Q, ['fresh'])).toEqual(['fresh']);
    } finally {
      await queue.close();
    }
  });
  it('CAF priority-list cost overflow advances a repeat-after-complete scheduler', async () => {
    const Q = uniqueQueue('lc-caf-pri-cost');
    const k = buildKeys(Q);
    await cleanupClient.xgroupCreate(k.stream, CONSUMER_GROUP, '0', { mkStream: true });
    await cleanupClient.hset(k.schedulers, {
      rac: JSON.stringify({ repeatAfterComplete: 1000, nextRun: 0, template: { name: 'rac' } }),
    });
    await cleanupClient.hset(k.job('pri'), {
      id: 'pri',
      name: 'rac',
      state: 'waiting',
      priority: '1',
      groupKey: 'g',
      cost: '5000',
      schedulerName: 'rac',
    });
    await cleanupClient.hset(k.group('g'), {
      maxConcurrency: '0',
      active: '0',
      tbCapacity: '1000',
      tbRefillRate: '1000',
      tbTokens: '1000',
      tbLastRefill: String(Date.now()),
    });
    await cleanupClient.lpush(k.priority, ['pri']);
    await cleanupClient.hset(k.job('cur'), { id: 'cur', name: 'cur', state: 'active' });
    const now = Date.now();
    await completeAndFetchNext(cleanupClient, k, 'cur', '', 'null', now, CONSUMER_GROUP, 'c1');
    expect(await hget(k.job('pri'), 'state')).toBe('failed');
    const config = JSON.parse((await hget(k.schedulers, 'rac'))!);
    expect(config.nextRun).toBe(now + 1000);
  });
  function gate(): { wait: Promise<void>; open: () => void } {
    let open!: () => void;
    const wait = new Promise<void>((resolve) => {
      open = resolve;
    });
    return { wait, open };
  }

  it('completing a removed active job leaves no ghost and keeps group accounting', async () => {
    const Q = uniqueQueue('lc-rm-active');
    const k = buildKeys(Q);
    const queue = new Queue(Q, { connection: CONNECTION });
    const gates = new Map<string, ReturnType<typeof gate>>();
    const started: string[] = [];
    const worker = new Worker(
      Q,
      async (job) => {
        started.push(job.name);
        const g = gate();
        gates.set(job.name, g);
        await g.wait;
        return 'ok';
      },
      { connection: CONNECTION, concurrency: 2, blockTimeout: 50, stalledInterval: 60_000 },
    );
    worker.on('error', () => {});
    try {
      const a = await queue.add('a', {}, { ordering: { key: 'g', concurrency: 2 } });
      await queue.add('b', {}, { ordering: { key: 'g', concurrency: 2 } });
      await waitFor(() => started.length === 2, 5000, 25);
      expect(await hget(k.group('g'), 'active')).toBe('2');
      await (await queue.getJob(a.id!))!.remove();
      expect(await hget(k.group('g'), 'active')).toBe('1');
      gates.get('a')!.open();
      await new Promise((r) => setTimeout(r, 300));
      expect(await cleanupClient.exists([k.job(a.id!)])).toBe(0);
      expect(await cleanupClient.zscore(k.completed, a.id!)).toBeNull();
      expect(await hget(k.group('g'), 'active')).toBe('1');
      gates.get('b')!.open();
    } finally {
      for (const g of gates.values()) g.open();
      await worker.close(true);
      await queue.close();
    }
  });

  it('failing a removed active job with attempts left does not schedule a ghost retry', async () => {
    const Q = uniqueQueue('lc-rm-active-fail');
    const k = buildKeys(Q);
    const queue = new Queue(Q, { connection: CONNECTION });
    const g = gate();
    let started = false;
    const worker = new Worker(
      Q,
      async () => {
        started = true;
        await g.wait;
        throw new Error('boom');
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 50, stalledInterval: 60_000 },
    );
    worker.on('error', () => {});
    worker.on('failed', () => {});
    try {
      const job = await queue.add('x', {}, { attempts: 3 });
      await waitFor(() => started, 5000, 25);
      await (await queue.getJob(job.id!))!.remove();
      g.open();
      await new Promise((r) => setTimeout(r, 300));
      expect(await cleanupClient.exists([k.job(job.id!)])).toBe(0);
      expect(await cleanupClient.zscore(k.scheduled, job.id!)).toBeNull();
      expect(await cleanupClient.xlen(k.stream)).toBe(0);
    } finally {
      g.open();
      await worker.close(true);
      await queue.close();
    }
  });

  it('stalled recovery does not resurrect a removed active job', async () => {
    const Q = uniqueQueue('lc-rm-active-stall');
    const k = buildKeys(Q);
    const queue = new Queue(Q, { connection: CONNECTION });
    try {
      const job = await queue.add('x', {});
      await cleanupClient.xgroupCreate(k.stream, CONSUMER_GROUP, '0', { mkStream: true }).catch(() => {});
      await cleanupClient.xreadgroup(CONSUMER_GROUP, 'dead', { [k.stream]: '>' });
      await cleanupClient.hset(k.job(job.id!), { state: 'active', lastActive: '1' });
      await (await queue.getJob(job.id!))!.remove();
      await reclaimStalled(cleanupClient, k, 'rescuer', 0, 5, Date.now(), CONSUMER_GROUP);
      expect(await cleanupClient.exists([k.job(job.id!)])).toBe(0);
      expect(await cleanupClient.xlen(k.stream)).toBe(0);
    } finally {
      await queue.close();
    }
  });
});
