/**
 * Per-job lockDuration tests.
 * Verifies that jobs can override the worker-level lockDuration for individual
 * heartbeat frequency and stall detection thresholds.
 *
 * Requires: valkey-server running on localhost:6379 and cluster on :7000-7005
 *
 * Run: npx vitest run tests/per-job-lock.test.ts
 */
import { it, expect, beforeAll, afterAll } from 'vitest';
import { describeEachMode, createCleanupClient, flushQueue, waitFor } from './helpers/fixture';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

const TS = Date.now();

describeEachMode('Per-job lockDuration', (CONNECTION) => {
  const Q = `pjl-${TS}`;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('job with lockDuration: 120000 is not reclaimed within stalledInterval', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    let completedId: string | null = null;
    const stalledEvents: string[] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 20000);

      const worker = new Worker(
        Q,
        async () => {
          // Job runs for 4s - longer than stalledInterval of 2s
          await new Promise((r) => setTimeout(r, 4000));
          return 'long-lock-done';
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 500,
          stalledInterval: 2000,
          lockDuration: 1000, // worker default: heartbeat every 500ms
          maxStalledCount: 1,
          promotionInterval: 1000,
        },
      );

      worker.on('completed', (job: any) => {
        completedId = job.id;
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });
      worker.on('stalled', (jobId: string) => {
        stalledEvents.push(jobId);
      });
      worker.on('error', () => {});
    });

    await new Promise((r) => setTimeout(r, 500));
    // Per-job lockDuration of 120s - Lua will use this as threshold instead of stalledInterval
    const job = await queue.add('long-lock', { v: 1 }, { lockDuration: 120000 });

    await done;

    // Job must complete normally - not be reclaimed as stalled
    expect(completedId).toBe(job!.id);
    expect(stalledEvents).not.toContain(job!.id);

    await queue.close();
  }, 25000);

  it('job with lockDuration: 5000 is reclaimed after inactivity', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    // Worker 1: picks up the job but we force-close it (simulates crash)
    let w1Started = false;
    const worker1 = new Worker(
      Q,
      async () => {
        w1Started = true;
        await new Promise((r) => setTimeout(r, 60000));
        return 'never';
      },
      {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 500,
        stalledInterval: 60000, // very long - would never reclaim without per-job override
        lockDuration: 60000,
        maxStalledCount: 1,
      },
    );
    worker1.on('error', () => {});
    await worker1.waitUntilReady();

    // Add job with short per-job lockDuration
    const job = await queue.add('short-lock', { v: 2 }, { lockDuration: 2000 });
    while (!w1Started) {
      await new Promise((r) => setTimeout(r, 50));
    }
    // Force-close worker1 to simulate crash (no heartbeat)
    await worker1.close(true);

    // Worker 2: with short stalledInterval to run reclaim cycles
    const stalledIds: string[] = [];
    const worker2 = new Worker(Q, async () => 'recovered', {
      connection: CONNECTION,
      concurrency: 1,
      blockTimeout: 500,
      stalledInterval: 1000, // frequent reclaim checks
      maxStalledCount: 1,
    });
    worker2.on('error', () => {});
    worker2.on('stalled', (jobId: string) => stalledIds.push(jobId));
    await worker2.waitUntilReady();

    // Wait for stalled recovery to detect the job (per-job lockDuration: 2s)
    await waitFor(async () => {
      const state = await cleanupClient.hget(k.job(job!.id), 'state');
      return String(state) === 'failed';
    }, 8000);

    await worker2.close(true);

    // Job should be failed via stalled recovery
    const state = String(await cleanupClient.hget(k.job(job!.id), 'state'));
    expect(state).toBe('failed');
    const failedReason = String(await cleanupClient.hget(k.job(job!.id), 'failedReason'));
    expect(failedReason).toContain('stalled');

    await queue.close();
  }, 20000);

  it('mixed lock durations in same worker work correctly', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const completedIds: string[] = [];
    const stalledEvents: string[] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 20000);
      let count = 0;

      const worker = new Worker(
        Q,
        async (job: any) => {
          // Both jobs run for 3s - longer than stalledInterval
          await new Promise((r) => setTimeout(r, 3000));
          return `done-${job.name}`;
        },
        {
          connection: CONNECTION,
          concurrency: 2,
          blockTimeout: 500,
          stalledInterval: 2000,
          lockDuration: 1000, // worker default: heartbeat every 500ms
          maxStalledCount: 1,
          promotionInterval: 1000,
        },
      );

      worker.on('completed', (job: any) => {
        completedIds.push(job.id);
        count++;
        if (count >= 2) {
          clearTimeout(timeout);
          worker.close(true).then(resolve);
        }
      });
      worker.on('stalled', (jobId: string) => stalledEvents.push(jobId));
      worker.on('error', () => {});
    });

    await new Promise((r) => setTimeout(r, 500));

    // One job with long per-job lock (Lua threshold = 120s), one with default (worker lock = 1s, heartbeat 500ms)
    const jobLong = await queue.add('long-lock', { v: 1 }, { lockDuration: 120000 });
    const jobDefault = await queue.add('default-lock', { v: 2 });

    await done;

    // Both should complete without stalling
    expect(completedIds).toContain(jobLong!.id);
    expect(completedIds).toContain(jobDefault!.id);
    expect(stalledEvents).toHaveLength(0);

    await queue.close();
  }, 25000);

  it('default lock duration used when job does not specify (backward compat)', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    let completedId: string | null = null;
    const stalledEvents: string[] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);

      const worker = new Worker(
        Q,
        async () => {
          // Runs for 3s - longer than stalledInterval but within heartbeat protection
          await new Promise((r) => setTimeout(r, 3000));
          return 'compat-done';
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 500,
          stalledInterval: 2000,
          lockDuration: 1000, // heartbeat every 500ms
          maxStalledCount: 1,
          promotionInterval: 1000,
        },
      );

      worker.on('completed', (job: any) => {
        completedId = job.id;
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });
      worker.on('stalled', (jobId: string) => stalledEvents.push(jobId));
      worker.on('error', () => {});
    });

    await new Promise((r) => setTimeout(r, 500));
    // No per-job lockDuration - uses worker default
    const job = await queue.add('no-override', { v: 3 });

    await done;

    expect(completedId).toBe(job!.id);
    expect(stalledEvents).toHaveLength(0);

    await queue.close();
  }, 18000);

  it('per-job lock duration works with completeAndFetchNext chain', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const completedIds: string[] = [];
    const stalledEvents: string[] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 20000);
      let count = 0;

      const worker = new Worker(
        Q,
        async () => {
          // Each job runs 2.5s - longer than stalledInterval
          await new Promise((r) => setTimeout(r, 2500));
          return 'chain-done';
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 500,
          stalledInterval: 2000,
          lockDuration: 1000,
          maxStalledCount: 1,
          promotionInterval: 1000,
        },
      );

      worker.on('completed', (job: any) => {
        completedIds.push(job.id);
        count++;
        if (count >= 3) {
          clearTimeout(timeout);
          worker.close(true).then(resolve);
        }
      });
      worker.on('stalled', (jobId: string) => stalledEvents.push(jobId));
      worker.on('error', () => {});
    });

    await new Promise((r) => setTimeout(r, 500));

    // Add 3 jobs with per-job lockDuration - these go through completeAndFetchNext chain
    const j1 = await queue.add('chain-1', { i: 1 }, { lockDuration: 60000 });
    const j2 = await queue.add('chain-2', { i: 2 }, { lockDuration: 60000 });
    const j3 = await queue.add('chain-3', { i: 3 }, { lockDuration: 60000 });

    await done;

    // All three must complete without stalling
    expect(completedIds).toContain(j1!.id);
    expect(completedIds).toContain(j2!.id);
    expect(completedIds).toContain(j3!.id);
    expect(stalledEvents).toHaveLength(0);

    await queue.close();
  }, 25000);
});
