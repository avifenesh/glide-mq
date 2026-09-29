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
const { buildKeys, keyPrefix } = require('../dist/utils') as typeof import('../src/utils');
const { completeAndFetchNext, healListActive, moveToActive, promote, reclaimStalled, registerParent, CONSUMER_GROUP } =
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
      { connection: CONNECTION, concurrency: 1, blockTimeout: 50, stalledInterval: 60_000, promotionInterval: 50 },
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
    const job = await queue.add('x', {}, { ttl: 60_000 });
    await cleanupClient.hset(k.job(job.id!), { expireAt: String(Date.now() - 1000) });
    const processed: string[] = [];
    const worker = new Worker(
      Q,
      async (active) => {
        processed.push(active.name);
        return 'ok';
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 50, stalledInterval: 60_000, promotionInterval: 50 },
    );
    worker.on('error', () => {});
    try {
      await waitFor(async () => (await hget(k.job(job.id!), 'state')) === 'failed', 5000, 25);
      expect(processed).toEqual([]);
      await (await queue.getJob(job.id!))!.retry();
      expect(Number(await hget(k.job(job.id!), 'expireAt'))).toBeGreaterThan(Date.now());
      await waitFor(() => processed.length === 1, 5000, 25);
      expect(await hget(k.job(job.id!), 'state')).toBe('completed');
    } finally {
      await worker.close(true);
      await queue.close();
    }
  });

  it('changePriority and changeDelay handle waiting jobs held in the priority and LIFO lists', async () => {
    const Q = uniqueQueue('lc-change-list');
    const k = buildKeys(Q);
    const queue = new Queue(Q, { connection: CONNECTION });
    const PRIORITY_SHIFT = 2 ** 42;
    const listHas = async (key: string, id: string) =>
      (await cleanupClient.lrange(key, 0, -1)).map(String).includes(id);
    try {
      const a = await queue.add('a', {}, { priority: 1 });
      const b = await queue.add('b', {}, { priority: 1 });
      const c = await queue.add('c', {}, { lifo: true });
      await promote(cleanupClient, k, Date.now());
      expect(await listHas(k.priority, a.id!)).toBe(true);
      expect(await hget(k.job(a.id!), 'state')).toBe('waiting');

      const ja = (await queue.getJob(a.id!))!;
      await ja.changePriority(5);
      expect(await hget(k.job(a.id!), 'state')).toBe('prioritized');
      expect(await listHas(k.priority, a.id!)).toBe(false);
      expect(Number(await cleanupClient.zscore(k.scheduled, a.id!))).toBe(5 * PRIORITY_SHIFT);

      const jb = (await queue.getJob(b.id!))!;
      await jb.changePriority(0);
      expect(await hget(k.job(b.id!), 'priority')).toBe('0');
      expect(await listHas(k.priority, b.id!)).toBe(false);
      expect(await cleanupClient.xlen(k.stream)).toBe(1);

      const jc = (await queue.getJob(c.id!))!;
      await jc.changeDelay(60_000);
      expect(await hget(k.job(c.id!), 'state')).toBe('delayed');
      expect(await listHas(k.lifo, c.id!)).toBe(false);
      expect(await cleanupClient.zscore(k.scheduled, c.id!)).not.toBeNull();
    } finally {
      await queue.close();
    }
  });
  it('debounce replacing a flow child does not leave the parent waiting on the old child', async () => {
    const Q = uniqueQueue('lc-debounce-parent');
    const flow = new FlowProducer({ connection: CONNECTION });
    const queue = new Queue(Q, { connection: CONNECTION });
    try {
      const node = await flow.add({
        name: 'parent',
        queueName: Q,
        data: {},
        children: [{ name: 'c1', queueName: Q, data: {} }],
      });
      const parent = { queue: Q, id: node.job.id };
      const dedupOpts = { parent, delay: 60_000, deduplication: { id: 'd', mode: 'debounce' as const } };
      await queue.add('d', { v: 1 }, dedupOpts);
      const second = await queue.add('d', { v: 2 }, dedupOpts);
      await (await queue.getJob(second!.id!))!.promote();
      const processed = await processedBy(Q, ['c1', 'd', 'parent']);
      expect(processed).toContain('parent');
    } finally {
      await flow.close();
      await queue.close();
    }
  });
  it('debounce replacing a flow child under the same jobId keeps the parent waiting for the replacement', async () => {
    const Q = uniqueQueue('lc-debounce-same-id');
    const k = buildKeys(Q);
    const flow = new FlowProducer({ connection: CONNECTION });
    const queue = new Queue(Q, { connection: CONNECTION });
    try {
      const node = await flow.add({
        name: 'parent',
        queueName: Q,
        data: {},
        children: [{ name: 'c1', queueName: Q, data: {} }],
      });
      const parent = { queue: Q, id: node.job.id };
      const dedupOpts = {
        parent,
        jobId: 'dj',
        delay: 60_000,
        deduplication: { id: 'dsame', mode: 'debounce' as const },
      };
      await queue.add('d', { v: 1 }, dedupOpts);
      const second = await queue.add('d', { v: 2 }, dedupOpts);
      expect(second!.id).toBe('dj');
      // The replacement carries the same deps member, so nothing may count as done yet.
      expect(await hget(k.job(node.job.id), 'depsCompleted')).toBeNull();
      await (await queue.getJob('dj'))!.promote();
      const processed = await processedBy(Q, ['c1', 'd', 'parent']);
      expect(processed.indexOf('parent')).toBeGreaterThan(processed.indexOf('d'));
    } finally {
      await flow.close();
      await queue.close();
    }
  });
  it('debounce replacing a DAG child under the same jobId keeps its DAG parent waiting for the replacement', async () => {
    const Q = uniqueQueue('lc-debounce-same-id-dag');
    const k = buildKeys(Q);
    const flow = new FlowProducer({ connection: CONNECTION });
    const queue = new Queue(Q, { connection: CONNECTION });
    try {
      const dedupOpts = { jobId: 'dagA', delay: 60_000, deduplication: { id: 'ddag', mode: 'debounce' as const } };
      await queue.add('A', { v: 1 }, dedupOpts);
      const node = await flow.add({
        name: 'B',
        queueName: Q,
        data: {},
        children: [{ name: 'c1', queueName: Q, data: {} }],
      });
      // Second parent edge, wired the way addDAG wires multi-parent children.
      const member = `${keyPrefix('glide', Q)}:dagA`;
      expect(await registerParent(cleanupClient, k, 'dagA', node.job.id, keyPrefix('glide', Q), k, member)).toBe('ok');
      const replaced = await queue.add('A', { v: 2 }, dedupOpts);
      expect(replaced!.id).toBe('dagA');
      expect(await hget(k.job(node.job.id), 'depsCompleted')).toBeNull();
      await (await queue.getJob('dagA'))!.promote();
      const processed = await processedBy(Q, ['c1', 'A', 'B']);
      expect(processed.indexOf('B')).toBeGreaterThan(processed.indexOf('A'));
    } finally {
      await flow.close();
      await queue.close();
    }
  });
  it('job field mutators do not recreate a removed job', async () => {
    const Q = uniqueQueue('lc-ghost-mutators');
    const k = buildKeys(Q);
    const queue = new Queue(Q, { connection: CONNECTION });
    try {
      const added = await queue.add('x', { v: 1 });
      const job = (await queue.getJob(added.id!))!;
      await job.updateProgress(10);
      expect(await hget(k.job(job.id), 'progress')).toBe('10');
      await job.storeVector('vec', [1, 2]);
      expect(await cleanupClient.hstrlen(k.job(job.id), 'vec')).toBe(8);
      await job.remove();
      await expect(job.updateProgress(50)).rejects.toThrow(/not found/);
      await expect(job.updateData({ v: 2 })).rejects.toThrow(/not found/);
      await expect(job.reportTokens(5)).rejects.toThrow(/not found/);
      await expect(job.storeVector('vec', [1, 2])).rejects.toThrow(/not found/);
      await expect(job.reportUsage({ model: 'm', tokens: { input: 1 } })).rejects.toThrow(/not found/);
      expect(await cleanupClient.exists([k.job(job.id)])).toBe(0);
      expect(await queue.getJob(job.id)).toBeNull();
    } finally {
      await queue.close();
    }
  });
});
