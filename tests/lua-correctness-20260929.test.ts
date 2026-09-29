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
const { completeAndFetchNext, healListActive, moveToActive, reclaimStalled, CONSUMER_GROUP } =
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
  it('bulk-retried failed ordered job respects group concurrency and balances active', async () => {
    const Q = uniqueQueue('lc-retry-ord');
    const k = buildKeys(Q);
    const queue = new Queue(Q, { connection: CONNECTION });
    const bGate = gate();
    const order: string[] = [];
    let running = 0;
    let maxRunning = 0;
    let failA = true;
    const worker = new Worker(
      Q,
      async (job) => {
        running++;
        maxRunning = Math.max(maxRunning, running);
        try {
          if (job.name === 'a' && failA) {
            failA = false;
            throw new Error('first run fails');
          }
          if (job.name === 'b') await bGate.wait;
          order.push(job.name);
          return 'ok';
        } finally {
          running--;
        }
      },
      { connection: CONNECTION, concurrency: 4, blockTimeout: 50, stalledInterval: 60_000, promotionInterval: 50 },
    );
    worker.on('error', () => {});
    worker.on('failed', () => {});
    try {
      const a = await queue.add('a', {}, { ordering: { key: 'k', concurrency: 1 } });
      await waitFor(async () => (await hget(k.job(a.id!), 'state')) === 'failed', 5000, 25);
      await queue.add('b', {}, { ordering: { key: 'k', concurrency: 1 } });
      await waitFor(() => running === 1, 5000, 25);
      expect(await queue.retryJobs()).toBe(1);
      await new Promise((r) => setTimeout(r, 500));
      bGate.open();
      await waitFor(() => order.length === 2, 5000, 25);
      expect(order).toEqual(['b', 'a']);
      expect(maxRunning).toBe(1);
      expect(await hget(k.group('k'), 'active')).toBe('0');
    } finally {
      bGate.open();
      await worker.close(true);
      await queue.close();
    }
  });
  it('a priority job promoted from its group into the stream is not treated as list-sourced', async () => {
    const Q = uniqueQueue('lc-pri-stream');
    const queue = new Queue(Q, { connection: CONNECTION });
    const gates = new Map<string, ReturnType<typeof gate>>();
    const started: string[] = [];
    const worker = new Worker(
      Q,
      async (job) => {
        const g = gate();
        gates.set(job.name, g);
        started.push(job.name);
        await g.wait;
        return 'ok';
      },
      { connection: CONNECTION, concurrency: 2, blockTimeout: 50, stalledInterval: 60_000, promotionInterval: 50 },
    );
    worker.on('error', () => {});
    try {
      await queue.add('a', {}, { ordering: { key: 'k', concurrency: 1 } });
      await waitFor(() => started.includes('a'), 5000, 25);
      await queue.add('b', {}, { ordering: { key: 'k', concurrency: 1 }, priority: 1 });
      await new Promise((r) => setTimeout(r, 300));
      gates.get('a')!.open();
      await waitFor(() => started.includes('b'), 5000, 25);
      const active = await queue.getJobs('active');
      expect(active.map((j) => j.name)).toEqual(['b']);
    } finally {
      for (const g of gates.values()) g.open();
      await worker.close(true);
      await queue.close();
    }
  });
  it('healListActive still corrects drift after a completed scan', async () => {
    const Q = uniqueQueue('lc-heal');
    const k = buildKeys(Q);
    await cleanupClient.hset(k.job('1'), { id: '1', name: 'x', state: 'active', listSourced: '1' });
    await cleanupClient.set(k.listActive, '3');
    expect(await healListActive(cleanupClient, k)).toBe(2);
    expect(String(await cleanupClient.get(k.listActive))).toBe('1');
  });
  it('moveToActive rejects a stale entry for a job reclaimed and re-activated elsewhere', async () => {
    const Q = uniqueQueue('lc-stale-activate');
    const k = buildKeys(Q);
    const queue = new Queue(Q, { connection: CONNECTION });
    try {
      const job = await queue.add('x', {});
      await cleanupClient.xgroupCreate(k.stream, CONSUMER_GROUP, '0', { mkStream: true }).catch(() => {});
      const readEntry = async (consumer: string): Promise<string> => {
        const res = await cleanupClient.xreadgroup(CONSUMER_GROUP, consumer, { [k.stream]: '>' });
        return String(Object.keys(res[0].value)[0]);
      };
      const oldEntry = await readEntry('w1');
      // w1 stalls before activating; recovery redispatches the job.
      await reclaimStalled(cleanupClient, k, 'rescuer', 0, 5, Date.now(), CONSUMER_GROUP);
      const newEntry = await readEntry('w2');
      expect(newEntry).not.toBe(oldEntry);
      const first = await moveToActive(cleanupClient, k, job.id!, Date.now(), k.stream, newEntry, CONSUMER_GROUP);
      expect(typeof first).toBe('object');
      const second = await moveToActive(cleanupClient, k, job.id!, Date.now(), k.stream, oldEntry, CONSUMER_GROUP);
      expect(second).toBe('STALE');

      await completeAndFetchNext(cleanupClient, k, job.id!, newEntry, 'null', Date.now(), CONSUMER_GROUP, 'w2');
      const third = await moveToActive(cleanupClient, k, job.id!, Date.now(), k.stream, oldEntry, CONSUMER_GROUP);
      expect(third).toBe('STALE');
      expect(await hget(k.job(job.id!), 'state')).toBe('completed');
    } finally {
      await queue.close();
    }
  });
  it('Job.retry only retries failed jobs', async () => {
    const Q = uniqueQueue('lc-job-retry-state');
    const k = buildKeys(Q);
    const queue = new Queue(Q, { connection: CONNECTION });
    const g = gate();
    let started = false;
    const worker = new Worker(
      Q,
      async () => {
        started = true;
        await g.wait;
        return 'ok';
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 50, stalledInterval: 60_000 },
    );
    worker.on('error', () => {});
    try {
      const job = await queue.add('x', {});
      await waitFor(() => started, 5000, 25);
      const live = (await queue.getJob(job.id!))!;
      await expect(live.retry()).rejects.toThrow(/not_failed/);
      expect(await cleanupClient.zscore(k.scheduled, job.id!)).toBeNull();
      expect(await hget(k.job(job.id!), 'state')).toBe('active');
      g.open();
      await waitFor(async () => (await hget(k.job(job.id!), 'state')) === 'completed', 5000, 25);
      await expect(live.retry()).rejects.toThrow(/not_failed/);
      expect(await cleanupClient.zscore(k.scheduled, job.id!)).toBeNull();
      expect(await cleanupClient.zscore(k.completed, job.id!)).not.toBeNull();
    } finally {
      g.open();
      await worker.close(true);
      await queue.close();
    }
  });

  it('Job.retry re-arms the TTL of an expired job', async () => {
    const Q = uniqueQueue('lc-job-retry-ttl');
    const k = buildKeys(Q);
    const queue = new Queue(Q, { connection: CONNECTION });
    try {
      const job = await queue.add('x', {}, { ttl: 60_000 });
      await cleanupClient.hset(k.job(job.id!), { expireAt: String(Date.now() - 1000) });
      const processed = await processedBy(Q, ['never'], 500);
      expect(processed).toEqual([]);
      expect(await hget(k.job(job.id!), 'state')).toBe('failed');
      await (await queue.getJob(job.id!))!.retry();
      expect(Number(await hget(k.job(job.id!), 'expireAt'))).toBeGreaterThan(Date.now());
      expect(await processedBy(Q, ['x'])).toEqual(['x']);
    } finally {
      await queue.close();
    }
  });
});
