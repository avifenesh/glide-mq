/**
 * Deep heartbeat / lock renewal tests.
 * Covers: heartbeat writes lastActive, stalled reclaim skipping,
 * heartbeat lifecycle (start/stop), concurrent jobs, edge cases.
 *
 * Requires: valkey-server on localhost:6379 and cluster on :7000-7005
 * Run: npx vitest run tests/deep-heartbeat.test.ts
 */
import { it, expect, beforeAll, afterAll, afterEach } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
const { CONSUMER_GROUP } = require('../dist/functions/index') as typeof import('../src/functions/index');
const { reclaimStalled } = require('../dist/functions/index') as typeof import('../src/functions/index');

import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

describeEachMode('Deep heartbeat / lock renewal', (CONNECTION) => {
  let cleanupClient: any;

  const queues: InstanceType<typeof Queue>[] = [];
  const workers: InstanceType<typeof Worker>[] = [];
  const queueNames: string[] = [];

  function uniqueName(tag: string) {
    const name = `hb-${tag}-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    queueNames.push(name);
    return name;
  }

  /** Helper: wait for a condition with polling */
  async function waitFor(fn: () => Promise<boolean> | boolean, timeoutMs = 10000, pollMs = 50): Promise<void> {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      if (await fn()) return;
      await new Promise((r) => setTimeout(r, pollMs));
    }
    throw new Error(`waitFor timed out after ${timeoutMs}ms`);
  }

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterEach(async () => {
    for (const w of workers) {
      try {
        await w.close(true);
      } catch {}
    }
    workers.length = 0;
    for (const q of queues) {
      try {
        await q.close();
      } catch {}
    }
    queues.length = 0;
  });

  afterAll(async () => {
    for (const name of queueNames) {
      try {
        await flushQueue(cleanupClient, name);
      } catch {}
    }
    cleanupClient.close();
  });

  // 1. Long-running job with lockDuration=2s - NOT reclaimed due to heartbeat
  it('long-running job with heartbeat is NOT reclaimed as stalled', async () => {
    const Q = uniqueName('long-hb');
    const queue = new Queue(Q, { connection: CONNECTION });
    queues.push(queue);

    const completed: string[] = [];
    const stalled: string[] = [];

    const worker = new Worker(
      Q,
      async (job: any) => {
        await new Promise((r) => setTimeout(r, 3000));
        return 'done';
      },
      {
        connection: CONNECTION,
        lockDuration: 2000,
        stalledInterval: 2000,
        maxStalledCount: 1,
      },
    );
    workers.push(worker);

    worker.on('completed', (job: any) => completed.push(job.id));
    worker.on('stalled', (job: any) => stalled.push(typeof job === 'string' ? job : job.id));

    await worker.waitUntilReady();
    await queue.add('task', { x: 1 });

    await waitFor(() => completed.length > 0, 8000);

    expect(completed.length).toBe(1);
    expect(stalled.length).toBe(0);
  }, 15000);

  // 2. Job without heartbeat gets stalledCount incremented and eventually fails
  it('job without heartbeat gets stalledCount incremented by reclaimStalled', async () => {
    const Q = uniqueName('no-hb');
    const queue = new Queue(Q, { connection: CONNECTION });
    queues.push(queue);
    const k = buildKeys(Q);

    const jobStarted = new Promise<string>((resolve) => {
      const w1 = new Worker(
        Q,
        async (job: any) => {
          resolve(job.id);
          await new Promise(() => {}); // hang forever
          return 'never';
        },
        {
          connection: CONNECTION,
          lockDuration: 60000,
          stalledInterval: 60000,
          maxStalledCount: 2,
        },
      );
      workers.push(w1);
    });

    await queue.add('task', { x: 1 });
    const jobId = await jobStarted;

    await workers[0].close(true);

    await cleanupClient.hdel(k.job(jobId), ['lastActive']);

    await new Promise((r) => setTimeout(r, 600));

    const reclaimClient = await createCleanupClient(CONNECTION);
    try {
      await reclaimStalled(reclaimClient, k, 'test-consumer', 500, 2, Date.now(), CONSUMER_GROUP);
      const sc1 = await cleanupClient.hget(k.job(jobId), 'stalledCount');
      expect(Number(sc1)).toBe(1);

      await new Promise((r) => setTimeout(r, 600));
      await reclaimStalled(reclaimClient, k, 'test-consumer', 500, 2, Date.now(), CONSUMER_GROUP);
      const sc2 = await cleanupClient.hget(k.job(jobId), 'stalledCount');
      expect(Number(sc2)).toBe(2);

      await new Promise((r) => setTimeout(r, 600));
      await reclaimStalled(reclaimClient, k, 'test-consumer', 500, 2, Date.now(), CONSUMER_GROUP);

      const state = await cleanupClient.hget(k.job(jobId), 'state');
      expect(state).toBe('failed');
      const reason = await cleanupClient.hget(k.job(jobId), 'failedReason');
      expect(String(reason)).toContain('stalled');
    } finally {
      reclaimClient.close();
    }
  }, 15000);

  // 3. Heartbeat writes lastActive to job hash every lockDuration/2
  it('heartbeat writes lastActive to job hash at lockDuration/2 interval', async () => {
    const Q = uniqueName('lastactive');
    const queue = new Queue(Q, { connection: CONNECTION });
    queues.push(queue);
    const k = buildKeys(Q);

    let jobId: string | undefined;
    const timestamps: number[] = [];

    const worker = new Worker(
      Q,
      async (job: any) => {
        jobId = job.id;
        for (let i = 0; i < 8; i++) {
          await new Promise((r) => setTimeout(r, 200));
          const val = await cleanupClient.hget(k.job(job.id), 'lastActive');
          if (val) timestamps.push(Number(val));
        }
        return 'ok';
      },
      {
        connection: CONNECTION,
        lockDuration: 1000,
      },
    );
    workers.push(worker);

    const completed = new Promise<void>((resolve) => {
      worker.on('completed', () => resolve());
    });

    await worker.waitUntilReady();
    await queue.add('task', { data: 1 });
    await completed;

    expect(timestamps.length).toBeGreaterThan(2);
    const uniqueTs = new Set(timestamps);
    expect(uniqueTs.size).toBeGreaterThanOrEqual(2);
  }, 15000);

  // 4. lastActive timestamp is recent (within lockDuration) during processing
  it('lastActive is recent during processing', async () => {
    const Q = uniqueName('recent-la');
    const queue = new Queue(Q, { connection: CONNECTION });
    queues.push(queue);
    const k = buildKeys(Q);

    let recentCheck = false;

    const worker = new Worker(
      Q,
      async (job: any) => {
        await new Promise((r) => setTimeout(r, 600));
        const val = await cleanupClient.hget(k.job(job.id), 'lastActive');
        const lastActive = Number(val);
        const now = Date.now();
        recentCheck = now - lastActive < 1000;
        return 'ok';
      },
      {
        connection: CONNECTION,
        lockDuration: 1000,
      },
    );
    workers.push(worker);

    const completed = new Promise<void>((resolve) => {
      worker.on('completed', () => resolve());
    });

    await worker.waitUntilReady();
    await queue.add('task', { data: 1 });
    await completed;

    expect(recentCheck).toBe(true);
  }, 10000);

  // 5. Heartbeat stops after job completes
  it('heartbeat stops after job completes', async () => {
    const Q = uniqueName('stop-complete');
    const queue = new Queue(Q, { connection: CONNECTION });
    queues.push(queue);
    const k = buildKeys(Q);

    let jobId = '';

    const worker = new Worker(
      Q,
      async (job: any) => {
        jobId = job.id;
        await new Promise((r) => setTimeout(r, 300));
        return 'done';
      },
      {
        connection: CONNECTION,
        lockDuration: 500,
      },
    );
    workers.push(worker);

    const completed = new Promise<void>((resolve) => {
      worker.on('completed', () => resolve());
    });

    await worker.waitUntilReady();
    await queue.add('task', { data: 1 });
    await completed;

    const afterComplete = await cleanupClient.hget(k.job(jobId), 'lastActive');
    const ts1 = Number(afterComplete);

    await new Promise((r) => setTimeout(r, 600));

    const afterWait = await cleanupClient.hget(k.job(jobId), 'lastActive');
    const ts2 = Number(afterWait);

    expect(ts2).toBe(ts1);
  }, 10000);

  // 6. Heartbeat stops after job fails
  it('heartbeat stops after job fails', async () => {
    const Q = uniqueName('stop-fail');
    const queue = new Queue(Q, { connection: CONNECTION });
    queues.push(queue);
    const k = buildKeys(Q);

    let jobId = '';

    const worker = new Worker(
      Q,
      async (job: any) => {
        jobId = job.id;
        await new Promise((r) => setTimeout(r, 300));
        throw new Error('intentional failure');
      },
      {
        connection: CONNECTION,
        lockDuration: 500,
      },
    );
    workers.push(worker);

    const failed = new Promise<void>((resolve) => {
      worker.on('failed', () => resolve());
    });

    await worker.waitUntilReady();
    await queue.add('task', { data: 1 });
    await failed;

    const afterFail = await cleanupClient.hget(k.job(jobId), 'lastActive');
    const ts1 = Number(afterFail);

    await new Promise((r) => setTimeout(r, 600));

    const afterWait = await cleanupClient.hget(k.job(jobId), 'lastActive');
    const ts2 = Number(afterWait);

    expect(ts2).toBe(ts1);
  }, 10000);

  // 7. Heartbeat stops after worker.close(true)
  it('heartbeat stops after worker.close(true)', async () => {
    const Q = uniqueName('stop-close');
    const queue = new Queue(Q, { connection: CONNECTION });
    queues.push(queue);
    const k = buildKeys(Q);

    let jobId = '';
    const jobStarted = new Promise<void>((resolve) => {
      const worker = new Worker(
        Q,
        async (job: any) => {
          jobId = job.id;
          resolve();
          await new Promise(() => {});
          return 'never';
        },
        {
          connection: CONNECTION,
          lockDuration: 500,
        },
      );
      workers.push(worker);
    });

    await queue.add('task', { data: 1 });
    await jobStarted;

    await new Promise((r) => setTimeout(r, 350));

    const beforeClose = await cleanupClient.hget(k.job(jobId), 'lastActive');
    expect(beforeClose).not.toBeNull();

    await workers[0].close(true);

    const ts1 = Number(await cleanupClient.hget(k.job(jobId), 'lastActive'));

    await new Promise((r) => setTimeout(r, 600));

    const ts2 = Number(await cleanupClient.hget(k.job(jobId), 'lastActive'));
    expect(ts2).toBe(ts1);
  }, 10000);

  // 8. Multiple concurrent jobs each get their own heartbeat
  it('multiple concurrent jobs each get their own heartbeat', async () => {
    const Q = uniqueName('concurrent-hb');
    const queue = new Queue(Q, { connection: CONNECTION });
    queues.push(queue);
    const k = buildKeys(Q);

    const jobIds: string[] = [];
    let resolveAll: () => void;
    const allProcessing = new Promise<void>((r) => {
      resolveAll = r;
    });
    let count = 0;

    const worker = new Worker(
      Q,
      async (job: any) => {
        jobIds.push(job.id);
        count++;
        if (count >= 3) resolveAll!();
        await new Promise((r) => setTimeout(r, 1500));
        return 'done';
      },
      {
        connection: CONNECTION,
        concurrency: 3,
        lockDuration: 500,
      },
    );
    workers.push(worker);

    await worker.waitUntilReady();

    await Promise.all([queue.add('task', { i: 1 }), queue.add('task', { i: 2 }), queue.add('task', { i: 3 })]);

    await allProcessing;

    await new Promise((r) => setTimeout(r, 400));

    for (const id of jobIds) {
      const val = await cleanupClient.hget(k.job(id), 'lastActive');
      expect(val).not.toBeNull();
      const ts = Number(val);
      expect(ts).toBeGreaterThan(0);
    }

    const unique = new Set(jobIds);
    expect(unique.size).toBe(3);
  }, 15000);

  // 9. lockDuration=1000 - heartbeat fires every 500ms
  it('lockDuration=1000 fires heartbeat at ~500ms intervals', async () => {
    const Q = uniqueName('interval-1s');
    const queue = new Queue(Q, { connection: CONNECTION });
    queues.push(queue);
    const k = buildKeys(Q);

    const snapshots: { time: number; lastActive: number }[] = [];

    const worker = new Worker(
      Q,
      async (job: any) => {
        const start = Date.now();
        for (let i = 0; i < 20; i++) {
          await new Promise((r) => setTimeout(r, 100));
          const val = await cleanupClient.hget(k.job(job.id), 'lastActive');
          if (val) {
            snapshots.push({ time: Date.now() - start, lastActive: Number(val) });
          }
        }
        return 'ok';
      },
      {
        connection: CONNECTION,
        lockDuration: 1000,
      },
    );
    workers.push(worker);

    const completed = new Promise<void>((resolve) => {
      worker.on('completed', () => resolve());
    });

    await worker.waitUntilReady();
    await queue.add('task', { data: 1 });
    await completed;

    const distinctTs = [...new Set(snapshots.map((s) => s.lastActive))].sort();

    expect(distinctTs.length).toBeGreaterThanOrEqual(3);

    for (let i = 1; i < distinctTs.length; i++) {
      const gap = distinctTs[i] - distinctTs[i - 1];
      expect(gap).toBeGreaterThan(200);
      expect(gap).toBeLessThan(900);
    }
  }, 15000);

  // 10. lockDuration=60000 - heartbeat fires every 30s (verify first tick only)
  it('lockDuration=60000 writes initial lastActive immediately', async () => {
    const Q = uniqueName('interval-60s');
    const queue = new Queue(Q, { connection: CONNECTION });
    queues.push(queue);
    const k = buildKeys(Q);

    let jobId = '';

    const worker = new Worker(
      Q,
      async (job: any) => {
        jobId = job.id;
        await new Promise((r) => setTimeout(r, 200));
        return 'ok';
      },
      {
        connection: CONNECTION,
        lockDuration: 60000,
      },
    );
    workers.push(worker);

    const completed = new Promise<void>((resolve) => {
      worker.on('completed', () => resolve());
    });

    await worker.waitUntilReady();
    await queue.add('task', { data: 1 });
    await completed;

    const val = await cleanupClient.hget(k.job(jobId), 'lastActive');
    expect(val).not.toBeNull();
    const ts = Number(val);
    const now = Date.now();
    expect(now - ts).toBeLessThan(5000);
  }, 10000);

  // 11. Stalled reclaim skips job with recent lastActive, reclaims without
  it('stalled reclaim skips job with recent lastActive', async () => {
    const Q = uniqueName('reclaim-skip');
    const queue = new Queue(Q, { connection: CONNECTION });
    queues.push(queue);
    const k = buildKeys(Q);

    const jobStarted = new Promise<string>((resolve) => {
      const w = new Worker(
        Q,
        async (job: any) => {
          resolve(job.id);
          await new Promise(() => {});
          return 'never';
        },
        {
          connection: CONNECTION,
          lockDuration: 60000,
          stalledInterval: 60000,
        },
      );
      workers.push(w);
    });

    const job = await queue.add('task', { data: 1 });
    const jobId = await jobStarted;

    await cleanupClient.hset(k.job(jobId), { lastActive: Date.now().toString() });

    await workers[0].close(true);

    const reclaimClient = await createCleanupClient(CONNECTION);
    try {
      const reclaimed = await reclaimStalled(reclaimClient, k, 'test-consumer', 1000, 1, Date.now(), CONSUMER_GROUP);

      const failScore = await cleanupClient.zscore(k.failed, jobId);
      expect(failScore).toBeNull();

      const stalledCount = await cleanupClient.hget(k.job(jobId), 'stalledCount');
      expect(stalledCount).toBeNull();
    } finally {
      reclaimClient.close();
    }
  }, 15000);

  // 12. Heartbeat-protected job NOT reclaimed, unprotected job IS reclaimed
  it('job with heartbeat not reclaimed vs job without heartbeat gets reclaimed', async () => {
    const Q = uniqueName('two-jobs');
    const queue = new Queue(Q, { connection: CONNECTION });
    queues.push(queue);
    const k = buildKeys(Q);

    const jobIds: string[] = [];
    let resolveAll: () => void;
    const allStarted = new Promise<void>((r) => {
      resolveAll = r;
    });

    const w = new Worker(
      Q,
      async (job: any) => {
        jobIds.push(job.id);
        if (jobIds.length >= 2) resolveAll!();
        await new Promise(() => {});
        return 'never';
      },
      {
        connection: CONNECTION,
        concurrency: 2,
        lockDuration: 60000,
        stalledInterval: 60000,
      },
    );
    workers.push(w);

    await queue.add('task', { tag: 'protected' });
    await queue.add('task', { tag: 'unprotected' });
    await allStarted;

    await w.close(true);

    const protectedId = jobIds[0];
    const unprotectedId = jobIds[1];

    await cleanupClient.hdel(k.job(unprotectedId), ['lastActive']);

    await new Promise((r) => setTimeout(r, 600));

    await cleanupClient.hset(k.job(protectedId), { lastActive: Date.now().toString() });

    const reclaimClient = await createCleanupClient(CONNECTION);
    try {
      await reclaimStalled(reclaimClient, k, 'test-consumer', 500, 2, Date.now(), CONSUMER_GROUP);

      const protSc = await cleanupClient.hget(k.job(protectedId), 'stalledCount');
      expect(protSc).toBeNull();

      const unprotSc = await cleanupClient.hget(k.job(unprotectedId), 'stalledCount');
      expect(Number(unprotSc)).toBe(1);
    } finally {
      reclaimClient.close();
    }
  }, 15000);

  // 13. Heartbeat initial write is immediate (not delayed by interval)
  it('heartbeat writes lastActive immediately on job start', async () => {
    const Q = uniqueName('immediate');
    const queue = new Queue(Q, { connection: CONNECTION });
    queues.push(queue);
    const k = buildKeys(Q);

    let earlyLastActive: string | null = null;

    const worker = new Worker(
      Q,
      async (job: any) => {
        await new Promise((r) => setTimeout(r, 50));
        earlyLastActive = (await cleanupClient.hget(k.job(job.id), 'lastActive')) as string | null;
        return 'ok';
      },
      {
        connection: CONNECTION,
        lockDuration: 10000,
      },
    );
    workers.push(worker);

    const completed = new Promise<void>((resolve) => {
      worker.on('completed', () => resolve());
    });

    await worker.waitUntilReady();
    await queue.add('task', { data: 1 });
    await completed;

    expect(earlyLastActive).not.toBeNull();
  }, 10000);

  // 14. Heartbeat interval cleared from heartbeatIntervals map after job done
  it('heartbeatIntervals map is empty after job completes', async () => {
    const Q = uniqueName('map-clear');
    const queue = new Queue(Q, { connection: CONNECTION });
    queues.push(queue);

    const worker = new Worker(
      Q,
      async () => {
        await new Promise((r) => setTimeout(r, 300));
        return 'ok';
      },
      {
        connection: CONNECTION,
        lockDuration: 500,
      },
    );
    workers.push(worker);

    const completed = new Promise<void>((resolve) => {
      worker.on('completed', () => resolve());
    });

    await worker.waitUntilReady();
    await queue.add('task', { data: 1 });
    await completed;

    await new Promise((r) => setTimeout(r, 50));

    const map = (worker as any).heartbeatIntervals as Map<string, any>;
    expect(map.size).toBe(0);
  }, 10000);

  // 15. Worker close clears all active heartbeats (map is empty)
  it('worker close clears all active heartbeats', async () => {
    const Q = uniqueName('close-clear');
    const queue = new Queue(Q, { connection: CONNECTION });
    queues.push(queue);

    let started = 0;
    const allStarted = new Promise<void>((resolve) => {
      const worker = new Worker(
        Q,
        async () => {
          started++;
          if (started >= 2) resolve();
          await new Promise(() => {});
          return 'never';
        },
        {
          connection: CONNECTION,
          concurrency: 3,
          lockDuration: 500,
        },
      );
      workers.push(worker);
    });

    await queue.add('task', { i: 1 });
    await queue.add('task', { i: 2 });
    await allStarted;

    const map = (workers[0] as any).heartbeatIntervals as Map<string, any>;
    expect(map.size).toBe(2);

    await workers[0].close(true);

    expect(map.size).toBe(0);
  }, 10000);

  // 16. Heartbeat keeps job alive across multiple stalled check cycles
  it('heartbeat keeps job alive across multiple stalled check cycles', async () => {
    const Q = uniqueName('multi-cycle');
    const queue = new Queue(Q, { connection: CONNECTION });
    queues.push(queue);

    const completed: string[] = [];
    const stalled: string[] = [];

    const worker = new Worker(
      Q,
      async () => {
        await new Promise((r) => setTimeout(r, 2000));
        return 'alive';
      },
      {
        connection: CONNECTION,
        lockDuration: 800,
        stalledInterval: 500,
        maxStalledCount: 1,
      },
    );
    workers.push(worker);

    worker.on('completed', (job: any) => completed.push(job.id));
    worker.on('stalled', (job: any) => stalled.push(typeof job === 'string' ? job : job.id));

    await worker.waitUntilReady();
    await queue.add('task', { data: 1 });

    await waitFor(() => completed.length > 0, 8000);

    expect(completed.length).toBe(1);
    expect(stalled.length).toBe(0);
  }, 15000);

  // 17. Job completing during heartbeat tick does not error
  it('job completing right at heartbeat tick does not throw', async () => {
    const Q = uniqueName('race-complete');
    const queue = new Queue(Q, { connection: CONNECTION });
    queues.push(queue);

    const errors: Error[] = [];

    const worker = new Worker(
      Q,
      async () => {
        await new Promise((r) => setTimeout(r, 150));
        return 'ok';
      },
      {
        connection: CONNECTION,
        lockDuration: 200,
      },
    );
    workers.push(worker);

    worker.on('error', (err: Error) => errors.push(err));

    const completed = new Promise<void>((resolve) => {
      worker.on('completed', () => resolve());
    });

    await worker.waitUntilReady();
    await queue.add('task', { data: 1 });
    await completed;

    await new Promise((r) => setTimeout(r, 300));

    expect(errors.length).toBe(0);
  }, 10000);

  // 18. lastActive not set on job hash before processing starts
  it('lastActive is not present on job hash before worker picks it up', async () => {
    const Q = uniqueName('pre-process');
    const queue = new Queue(Q, { connection: CONNECTION });
    queues.push(queue);
    const k = buildKeys(Q);

    const job = await queue.add('task', { data: 1 });

    const val = await cleanupClient.hget(k.job(job!.id), 'lastActive');
    expect(val).toBeNull();
  }, 10000);

  // 19. reclaimStalled increments stalledCount when no lastActive
  it('reclaimStalled increments stalledCount when job has no lastActive', async () => {
    const Q = uniqueName('stalled-cnt');
    const queue = new Queue(Q, { connection: CONNECTION });
    queues.push(queue);
    const k = buildKeys(Q);

    const jobStarted = new Promise<string>((resolve) => {
      const w = new Worker(
        Q,
        async (job: any) => {
          resolve(job.id);
          await new Promise(() => {});
          return 'never';
        },
        {
          connection: CONNECTION,
          lockDuration: 60000,
          stalledInterval: 60000,
        },
      );
      workers.push(w);
    });

    await queue.add('task', { data: 1 });
    const jobId = await jobStarted;

    await cleanupClient.hdel(k.job(jobId), ['lastActive']);

    await workers[0].close(true);

    await new Promise((r) => setTimeout(r, 600));

    const reclaimClient = await createCleanupClient(CONNECTION);
    try {
      await reclaimStalled(reclaimClient, k, 'test-consumer', 500, 5, Date.now(), CONSUMER_GROUP);

      const stalledCount = await cleanupClient.hget(k.job(jobId), 'stalledCount');
      expect(Number(stalledCount)).toBe(1);
    } finally {
      reclaimClient.close();
    }
  }, 15000);

  // 20. Heartbeat with very short lockDuration (200ms) still works
  it('very short lockDuration (200ms) heartbeat keeps job alive', async () => {
    const Q = uniqueName('short-lock');
    const queue = new Queue(Q, { connection: CONNECTION });
    queues.push(queue);

    const completed: string[] = [];

    const worker = new Worker(
      Q,
      async () => {
        await new Promise((r) => setTimeout(r, 1000));
        return 'survived';
      },
      {
        connection: CONNECTION,
        lockDuration: 200,
        stalledInterval: 200,
        maxStalledCount: 1,
      },
    );
    workers.push(worker);

    worker.on('completed', (job: any) => completed.push(job.id));

    await worker.waitUntilReady();
    await queue.add('task', { data: 1 });

    await waitFor(() => completed.length > 0, 5000);

    expect(completed.length).toBe(1);

    const jobData = await queue.getJob(completed[0]);
    expect(await jobData!.getState()).toBe('completed');
  }, 10000);
});
