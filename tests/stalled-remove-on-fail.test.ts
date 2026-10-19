/**
 * Terminal stalled failure applies the job's removeOnFail retention, the same
 * as an ordinary terminal failure.
 *
 * A worker claims jobs and is force-closed (a crash: no heartbeat, entries
 * stay pending). The reclaim FCALL then runs with a timestamp past the lock,
 * so every claimed job is stalled.
 *
 * Run: npx vitest run tests/stalled-remove-on-fail.test.ts
 */
import { afterAll, beforeAll, expect, it } from 'vitest';
import { createCleanupClient, describeEachMode, flushQueue, waitFor } from './helpers/fixture';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { Broadcast } = require('../dist/broadcast') as typeof import('../src/broadcast');
const { BroadcastWorker } = require('../dist/broadcast-worker') as typeof import('../src/broadcast-worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
const { reclaimStalled, reclaimStalledListJobs } =
  require('../dist/functions/index') as typeof import('../src/functions/index');

const STALL_REASON = 'job stalled more than maxStalledCount';
const workerOpts = { blockTimeout: 200, promotionInterval: 200, stalledInterval: 60_000 };

describeEachMode('Terminal stalled failure honors removeOnFail', (CONNECTION) => {
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

  /** Claim `count` jobs with a worker that never finishes them, then crash it. */
  async function crashAfterClaim(Q: string, count: number, concurrency = count): Promise<string[]> {
    const claimed: string[] = [];
    const worker = new Worker(
      Q,
      async (job: any) => {
        claimed.push(job.id);
        await new Promise(() => {});
      },
      { connection: CONNECTION, concurrency, ...workerOpts },
    );
    worker.on('error', () => {});
    try {
      await waitFor(() => claimed.length >= count, 10_000, 20);
    } finally {
      await worker.close(true);
    }
    return claimed;
  }

  function reclaim(Q: string, maxStalledCount: number): Promise<number> {
    return reclaimStalled(cleanupClient, buildKeys(Q), 'reclaimer', 0, maxStalledCount, Date.now() + 120_000);
  }

  async function events(Q: string, jobId: string): Promise<Record<string, string>[]> {
    const entries = (await cleanupClient.xrange(buildKeys(Q).events, '-', '+')) as Record<string, [string, string][]>;
    const out: Record<string, string>[] = [];
    for (const fields of Object.values(entries ?? {})) {
      const e: Record<string, string> = {};
      for (const [f, v] of fields) e[String(f)] = String(v);
      if (e.jobId === jobId) out.push(e);
    }
    return out;
  }

  async function expectRemoved(Q: string, jobId: string): Promise<void> {
    const k = buildKeys(Q);
    expect(await cleanupClient.exists([k.job(jobId)])).toBe(0);
    expect(await cleanupClient.zscore(k.failed, jobId)).toBeNull();
  }

  it('removes the job hash and data of a removeOnFail: true stream job, still emitting failed', async () => {
    const Q = uniqueQueue('srof-true');
    const k = buildKeys(Q);
    const queue = new Queue(Q, { connection: CONNECTION });
    try {
      const job = await queue.add('secret', { token: 'sensitive' }, { removeOnFail: true });
      await crashAfterClaim(Q, 1);

      expect(await reclaim(Q, 0)).toBe(1);

      await expectRemoved(Q, job!.id);
      expect(await queue.getJob(job!.id)).toBeNull();
      const failed = (await events(Q, job!.id)).filter((e) => e.event === 'failed');
      expect(failed).toEqual([expect.objectContaining({ failedReason: STALL_REASON })]);
      expect(await cleanupClient.hlen(k.metricsFailed)).toBeGreaterThan(0);
      expect((await cleanupClient.xpending(k.stream, 'workers'))[0]).toBe(0);
    } finally {
      await queue.close();
    }
  }, 20_000);

  it('keeps the failed job when removeOnFail is unset', async () => {
    const Q = uniqueQueue('srof-keep');
    const k = buildKeys(Q);
    const queue = new Queue(Q, { connection: CONNECTION });
    try {
      const job = await queue.add('kept', { v: 1 });
      await crashAfterClaim(Q, 1);

      expect(await reclaim(Q, 0)).toBe(1);

      const stored = await queue.getJob(job!.id);
      expect(stored?.data).toEqual({ v: 1 });
      expect(stored?.failedReason).toBe(STALL_REASON);
      expect(await cleanupClient.hget(k.job(job!.id), 'state')).toBe('failed');
      expect(await cleanupClient.zscore(k.failed, job!.id)).not.toBeNull();
    } finally {
      await queue.close();
    }
  }, 20_000);

  it.each([
    ['count', 1],
    ['age and count', { age: 3600, count: 1 }],
    // No count limit: the older failure goes only because it is past the age.
    ['age', { age: 60, count: 0 }],
    // A fractional count is floored, not passed to ZRANGE.
    ['fractional count', 1.5],
  ] as const)(
    'keeps only the newest failed jobs with removeOnFail as %s',
    async (_label, removeOnFail) => {
      const Q = uniqueQueue('srof-count');
      const k = buildKeys(Q);
      const queue = new Queue(Q, { connection: CONNECTION });
      try {
        const older = await queue.add('a', { v: 1 }, { removeOnFail });
        await crashAfterClaim(Q, 1);
        expect(await reclaim(Q, 0)).toBe(1);
        expect(await cleanupClient.zscore(k.failed, older!.id)).not.toBeNull();

        const newer = await queue.add('b', { v: 2 }, { removeOnFail });
        await crashAfterClaim(Q, 1);
        // 180s after the first failure: it ranks after it and the first is
        // past a 60s age.
        expect(await reclaimStalled(cleanupClient, k, 'reclaimer', 0, 0, Date.now() + 300_000)).toBe(1);

        await expectRemoved(Q, older!.id);
        expect(await cleanupClient.zrange(k.failed, { start: 0, end: -1 })).toEqual([newer!.id]);
        expect(await cleanupClient.hget(k.job(newer!.id), 'state')).toBe('failed');
      } finally {
        await queue.close();
      }
    },
    30_000,
  );

  it('retries a stalled job under maxStalledCount and removes it on the terminal stall', async () => {
    const Q = uniqueQueue('srof-retry');
    const k = buildKeys(Q);
    const queue = new Queue(Q, { connection: CONNECTION });
    try {
      const job = await queue.add('retry', { v: 1 }, { removeOnFail: true });
      await crashAfterClaim(Q, 1);

      expect(await reclaim(Q, 1)).toBe(1);
      expect(await cleanupClient.hget(k.job(job!.id), 'state')).toBe('waiting');
      expect((await events(Q, job!.id)).map((e) => e.event)).toContain('stalled');

      await crashAfterClaim(Q, 1);
      expect(await reclaim(Q, 1)).toBe(1);

      await expectRemoved(Q, job!.id);
      expect((await events(Q, job!.id)).filter((e) => e.event === 'failed')).toHaveLength(1);
    } finally {
      await queue.close();
    }
  }, 30_000);

  it('removes a list-sourced (priority) job on terminal stall', async () => {
    const Q = uniqueQueue('srof-prio');
    const k = buildKeys(Q);
    const queue = new Queue(Q, { connection: CONNECTION });
    try {
      const job = await queue.add('prio', { v: 1 }, { priority: 3, removeOnFail: true });
      await crashAfterClaim(Q, 1);

      expect(await reclaimStalledListJobs(cleanupClient, k, 0, 0, Date.now() + 120_000)).toBe(1);

      await expectRemoved(Q, job!.id);
      expect(Number(await cleanupClient.get(k.listActive))).toBe(0);
      expect((await events(Q, job!.id)).filter((e) => e.event === 'failed')).toHaveLength(1);
    } finally {
      await queue.close();
    }
  }, 20_000);

  it('still removes a removeOnFail: true job that fails in the processor', async () => {
    const Q = uniqueQueue('srof-ordinary');
    const queue = new Queue(Q, { connection: CONNECTION });
    let failedId = '';
    const worker = new Worker(
      Q,
      async () => {
        throw new Error('boom');
      },
      { connection: CONNECTION, ...workerOpts },
    );
    worker.on('error', () => {});
    worker.on('failed', (job) => {
      failedId = job.id;
    });
    try {
      const job = await queue.add('boom', { v: 1 }, { removeOnFail: true });
      await waitFor(() => failedId === job!.id, 10_000, 20);
      await expectRemoved(Q, job!.id);
    } finally {
      await worker.close();
      await queue.close();
    }
  }, 20_000);

  it('floors a fractional count on processor completion and failure', async () => {
    const Q = uniqueQueue('srof-fraction');
    const k = buildKeys(Q);
    const queue = new Queue(Q, { connection: CONNECTION });
    const finished: string[] = [];
    const errors: Error[] = [];
    const worker = new Worker(
      Q,
      async (job: any) => {
        if (job.data.fail) throw new Error('boom');
        return 'ok';
      },
      { connection: CONNECTION, ...workerOpts },
    );
    worker.on('error', (err: Error) => errors.push(err));
    worker.on('completed', (job: any) => finished.push(job.id));
    worker.on('failed', (job: any) => finished.push(job.id));
    try {
      for (const fail of [false, false, true, true]) {
        await queue.add('j', { fail }, { removeOnComplete: 1.5, removeOnFail: 1.5 });
      }
      await waitFor(() => finished.length === 4, 10_000, 20);
      expect(errors).toEqual([]);
      expect(await cleanupClient.zcard(k.completed)).toBe(1);
      expect(await cleanupClient.zcard(k.failed)).toBe(1);
    } finally {
      await worker.close();
      await queue.close();
    }
  }, 20_000);

  it('releases the ordering slot so the next job in the group runs', async () => {
    const Q = uniqueQueue('srof-order');
    const queue = new Queue(Q, { connection: CONNECTION });
    const completed: string[] = [];
    let worker: any = null;
    try {
      const first = await queue.add('o1', { v: 1 }, { ordering: { key: 'g' }, removeOnFail: true });
      const second = await queue.add('o2', { v: 2 }, { ordering: { key: 'g' }, removeOnFail: true });
      expect(await crashAfterClaim(Q, 1, 2)).toEqual([first!.id]);

      expect(await reclaim(Q, 0)).toBe(1);
      await expectRemoved(Q, first!.id);

      worker = new Worker(
        Q,
        async (job: any) => {
          completed.push(job.id);
          return 'ok';
        },
        { connection: CONNECTION, ...workerOpts },
      );
      worker.on('error', () => {});
      await waitFor(() => completed.length === 1, 10_000, 20);
      expect(completed).toEqual([second!.id]);
    } finally {
      await worker?.close();
      await queue.close();
    }
  }, 30_000);

  it('advances a repeatAfterComplete scheduler', async () => {
    const Q = uniqueQueue('srof-repeat');
    const k = buildKeys(Q);
    const queue = new Queue(Q, { connection: CONNECTION });
    const repeatMs = 60_000;
    try {
      await queue.upsertJobScheduler(
        'rep',
        { repeatAfterComplete: repeatMs },
        { name: 'rep', data: { v: 1 }, opts: { removeOnFail: true } },
      );
      const [jobId] = await crashAfterClaim(Q, 1);

      const timestamp = Date.now() + 120_000;
      expect(await reclaimStalled(cleanupClient, k, 'reclaimer', 0, 0, timestamp)).toBe(1);

      await expectRemoved(Q, jobId);
      const entry = JSON.parse(String(await cleanupClient.hget(k.schedulers, 'rep')));
      expect(entry.nextRun).toBe(timestamp + repeatMs);
    } finally {
      await queue.close();
    }
  }, 20_000);

  it('keeps a broadcast job for the other subscriptions', async () => {
    const Q = uniqueQueue('srof-bcast');
    const k = buildKeys(Q);
    const broadcast = new Broadcast(Q, { connection: CONNECTION });
    const claimed: string[] = [];
    const received: unknown[] = [];
    const subOpts = { connection: CONNECTION, ...workerOpts };
    const a = new BroadcastWorker(
      Q,
      async (job: any) => {
        claimed.push(job.id);
        await new Promise(() => {});
      },
      { ...subOpts, subscription: 'a' },
    );
    a.on('error', () => {});
    // Create subscription b's group before publishing; it reads later.
    const bIdle = new BroadcastWorker(Q, async () => 'unused', { ...subOpts, subscription: 'b' });
    bIdle.on('error', () => {});
    let b: any = null;
    try {
      await Promise.all([a.waitUntilReady(), bIdle.waitUntilReady()]);
      await bIdle.close();
      const id = await broadcast.publish('evt', { n: 1 }, { removeOnFail: true });
      await waitFor(() => claimed.length === 1, 10_000, 20);
      await a.close(true);

      expect(await reclaimStalled(cleanupClient, k, 'reclaimer', 0, 0, Date.now() + 120_000, 'a', true)).toBe(1);
      expect(await cleanupClient.exists([k.job(id!)])).toBe(1);

      b = new BroadcastWorker(
        Q,
        async (job: any) => {
          received.push(job.data);
          return 'b';
        },
        { ...subOpts, subscription: 'b' },
      );
      b.on('error', () => {});
      await waitFor(() => received.length === 1, 10_000, 20);
      expect(received).toEqual([{ n: 1 }]);
    } finally {
      // close() is idempotent; these run on every path.
      await a.close(true).catch(() => {});
      await bIdle.close().catch(() => {});
      await b?.close();
      await broadcast.close();
    }
  }, 30_000);
});
