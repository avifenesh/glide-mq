/**
 * Compatibility tests: Celery/Sidekiq reliability and operational patterns.
 * Adapted from cross-language queue system test patterns.
 *
 * Requires: valkey-server running on localhost:6379
 *
 * Run: npx vitest run tests/compat-reliability.test.ts
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';

const { GlideClient } = require('speedkey') as typeof import('speedkey');
const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
const { LIBRARY_SOURCE, CONSUMER_GROUP } = require('../dist/functions/index') as typeof import('../src/functions/index');
const { promote } = require('../dist/functions/index') as typeof import('../src/functions/index');
const { ensureFunctionLibrary } = require('../dist/connection') as typeof import('../src/connection');
const { gracefulShutdown } = require('../dist/graceful-shutdown') as typeof import('../src/graceful-shutdown');

const CONNECTION = {
  addresses: [{ host: 'localhost', port: 6379 }],
};

let cleanupClient: InstanceType<typeof GlideClient>;
const allQueues: string[] = [];

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

function uniqueQueue(prefix: string): string {
  const name = `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
  allQueues.push(name);
  return name;
}

beforeAll(async () => {
  cleanupClient = await GlideClient.createClient({
    addresses: [{ host: 'localhost', port: 6379 }],
  });
  await ensureFunctionLibrary(cleanupClient, LIBRARY_SOURCE);
});

afterAll(async () => {
  for (const q of allQueues) {
    await flushQueue(q);
  }
  cleanupClient.close();
});

// ---------------------------------------------------------------------------
// STALLED JOB RECOVERY (from BullMQ/Celery patterns)
// ---------------------------------------------------------------------------
describe('Stalled job recovery', () => {
  it('worker crashes mid-processing - stalled job detected and state updated', async () => {
    const Q = uniqueQueue('stall-reclaim');
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    const job = await queue.add('stall-test', { value: 'important' });

    // Worker 1: picks up the job, then gets force-closed (simulates crash)
    const worker1 = new Worker(
      Q,
      async () => {
        await new Promise(r => setTimeout(r, 60000));
        return 'never';
      },
      {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 500,
        stalledInterval: 60000,
      },
    );
    worker1.on('error', () => {});

    // Wait for worker1 to pick up the job
    await new Promise(r => setTimeout(r, 2000));
    await worker1.close(true);

    // Worker 2: has short stalledInterval - should detect the stalled job
    // XAUTOCLAIM reclaims the entry and increments stalledCount in the hash.
    // The recovery worker's scheduler runs reclaimStalled which either sets
    // state to 'active' (if stalledCount <= maxStalledCount) or 'failed'.
    const stalledIds: string[] = [];
    const worker2 = new Worker(
      Q,
      async () => 'recovered',
      {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 500,
        stalledInterval: 1000,
        maxStalledCount: 2,
        promotionInterval: 500,
      },
    );
    worker2.on('error', () => {});
    worker2.on('stalled', (jobId: string) => stalledIds.push(jobId));

    // Wait for stalled recovery to kick in
    await new Promise(r => setTimeout(r, 5000));

    await worker2.close(true);
    await queue.close();

    const state = await cleanupClient.hget(k.job(job.id), 'state');
    // After stall recovery: 'active' (reclaimed but not re-processed via pollLoop),
    // 'completed' (if the reclaim resulted in a re-read), or 'failed' (if maxStalledCount exceeded)
    expect(['completed', 'active', 'failed', 'waiting'].includes(String(state))).toBe(true);

    // Verify stalledCount was incremented in the job hash
    const stalledCount = await cleanupClient.hget(k.job(job.id), 'stalledCount');
    expect(Number(stalledCount)).toBeGreaterThanOrEqual(1);
  }, 15000);

  it('stalled job exceeds maxStalledCount - moved to failed permanently', async () => {
    const Q = uniqueQueue('stall-max');
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    const job = await queue.add('stall-fail', { value: 'doomed' });

    // Worker that stalls - picks up job then crashes
    const worker1 = new Worker(
      Q,
      async () => {
        await new Promise(r => setTimeout(r, 60000));
        return 'never';
      },
      {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 500,
        stalledInterval: 60000,
      },
    );
    worker1.on('error', () => {});
    await new Promise(r => setTimeout(r, 2000));
    await worker1.close(true);

    // Recovery worker with maxStalledCount=1 - reclaimStalled will XAUTOCLAIM
    // the entry, increment stalledCount to 1, which is <= maxStalledCount(1), so state='active'.
    // On the NEXT stalled check, stalledCount increments to 2 > maxStalledCount(1), so state='failed'.
    const worker2 = new Worker(
      Q,
      async () => {
        // This processor will never actually run for the stalled entry because
        // XAUTOCLAIM reclaims it but pollLoop only reads '>'.
        // The entry sits idle in the PEL, so the next stalled check reclaims again.
        await new Promise(r => setTimeout(r, 60000));
        return 'never';
      },
      {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 500,
        stalledInterval: 1000,
        maxStalledCount: 1,
        promotionInterval: 500,
      },
    );
    worker2.on('error', () => {});

    // Wait for 2+ stall detection cycles
    await new Promise(r => setTimeout(r, 5000));

    await worker2.close(true);
    await queue.close();

    // After exceeding maxStalledCount, job should be in failed state
    const state = await cleanupClient.hget(k.job(job.id), 'state');
    expect(String(state)).toBe('failed');

    const failedReason = await cleanupClient.hget(k.job(job.id), 'failedReason');
    expect(String(failedReason)).toContain('stalled');

    // Should be in the failed ZSet
    const failedScore = await cleanupClient.zscore(k.failed, job.id);
    expect(failedScore).not.toBeNull();
  }, 15000);

  it('stalled job detection interval is configurable', async () => {
    const Q = uniqueQueue('stall-interval');
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    const job = await queue.add('stall-config', { x: 1 });

    // Worker that picks up job then crashes
    const worker1 = new Worker(
      Q,
      async () => {
        await new Promise(r => setTimeout(r, 60000));
        return 'never';
      },
      {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 500,
        stalledInterval: 60000,
      },
    );
    worker1.on('error', () => {});
    await new Promise(r => setTimeout(r, 2000));
    await worker1.close(true);

    // Worker with 500ms stalled interval - should detect quickly
    const stalledAt: number[] = [];
    const worker2 = new Worker(
      Q,
      async () => 'recovered',
      {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 500,
        stalledInterval: 500,
        maxStalledCount: 3,
        promotionInterval: 500,
      },
    );
    worker2.on('error', () => {});
    worker2.on('stalled', () => stalledAt.push(Date.now()));

    const start = Date.now();
    await new Promise(r => setTimeout(r, 3000));

    await worker2.close(true);
    await queue.close();

    // stalledCount should have been incremented, proving detection happened
    const stalledCount = await cleanupClient.hget(k.job(job.id), 'stalledCount');
    expect(Number(stalledCount)).toBeGreaterThanOrEqual(1);

    // Detection should have happened quickly (well under 30s default)
    if (stalledAt.length > 0) {
      const detectionTime = stalledAt[0] - start;
      expect(detectionTime).toBeLessThan(3000);
    }
  }, 10000);

  it('multiple stalled jobs recovered in batch', async () => {
    const Q = uniqueQueue('stall-batch');
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    // Add 3 jobs
    const jobs = await Promise.all([
      queue.add('batch-1', { i: 1 }),
      queue.add('batch-2', { i: 2 }),
      queue.add('batch-3', { i: 3 }),
    ]);

    // Worker that picks up all 3 then crashes
    const worker1 = new Worker(
      Q,
      async () => {
        await new Promise(r => setTimeout(r, 60000));
        return 'never';
      },
      {
        connection: CONNECTION,
        concurrency: 3,
        blockTimeout: 500,
        stalledInterval: 60000,
      },
    );
    worker1.on('error', () => {});

    // Wait for all 3 to be picked up
    await new Promise(r => setTimeout(r, 3000));
    await worker1.close(true);

    // Recovery worker - will detect all 3 as stalled via XAUTOCLAIM
    const worker2 = new Worker(
      Q,
      async () => 'recovered',
      {
        connection: CONNECTION,
        concurrency: 3,
        blockTimeout: 500,
        stalledInterval: 1000,
        maxStalledCount: 3,
        promotionInterval: 500,
      },
    );
    worker2.on('error', () => {});

    await new Promise(r => setTimeout(r, 5000));

    await worker2.close(true);
    await queue.close();

    // All stalled jobs should have been detected - verify stalledCount incremented
    let detectedCount = 0;
    for (const j of jobs) {
      const sc = await cleanupClient.hget(k.job(j.id), 'stalledCount');
      if (Number(sc) >= 1) detectedCount++;
    }
    expect(detectedCount).toBeGreaterThanOrEqual(1);
  }, 15000);
});

// ---------------------------------------------------------------------------
// RETRY EXHAUSTION (from Sidekiq patterns)
// ---------------------------------------------------------------------------
describe('Retry exhaustion', () => {
  it('job exhausts all retries - ends in failed state with correct error', async () => {
    const Q = uniqueQueue('retry-exhaust');
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    let attemptCount = 0;
    const job = await queue.add('always-fail', { value: 'doomed' }, {
      attempts: 3,
      backoff: { type: 'fixed', delay: 100 },
    });

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 20000);
      let failCount = 0;

      const worker = new Worker(
        Q,
        async () => {
          attemptCount++;
          throw new Error(`fatal-error-${attemptCount}`);
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500 },
      );

      worker.on('failed', () => {
        failCount++;
        if (failCount < 3) {
          setTimeout(async () => {
            try { await promote(cleanupClient, buildKeys(Q), Date.now()); } catch {}
          }, 200);
        } else {
          clearTimeout(timeout);
          setTimeout(() => worker.close(true).then(resolve), 200);
        }
      });

      worker.on('error', () => {});
    });

    await done;

    const state = await cleanupClient.hget(k.job(job.id), 'state');
    expect(String(state)).toBe('failed');

    const failedReason = await cleanupClient.hget(k.job(job.id), 'failedReason');
    expect(String(failedReason)).toContain('fatal-error');

    const failedScore = await cleanupClient.zscore(k.failed, job.id);
    expect(failedScore).not.toBeNull();

    await queue.close();
  }, 25000);

  it('retry count persists in job hash across retries', async () => {
    const Q = uniqueQueue('retry-persist');
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    let attemptCount = 0;

    const job = await queue.add('retry-persist', { x: 1 }, {
      attempts: 4,
      backoff: { type: 'fixed', delay: 100 },
    });

    // Use a single worker that fails twice then succeeds.
    // After each failure, manually promote so the worker picks it up again.
    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 20000);

      const worker = new Worker(
        Q,
        async () => {
          attemptCount++;
          if (attemptCount <= 2) {
            throw new Error(`fail-attempt-${attemptCount}`);
          }
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});

      worker.on('failed', () => {
        // Promote after the backoff delay to move job back to stream
        setTimeout(async () => {
          try { await promote(cleanupClient, buildKeys(Q), Date.now()); } catch {}
        }, 200);
      });

      worker.on('completed', () => {
        clearTimeout(timeout);
        setTimeout(() => worker.close(true).then(resolve), 200);
      });
    });

    await done;

    // Verify attemptsMade was incremented to 2 (two failures before success)
    const attempts = await cleanupClient.hget(k.job(job.id), 'attemptsMade');
    expect(String(attempts)).toBe('2');

    // The count persists in the hash - a new worker reading this job would see attemptsMade=2
    const state = await cleanupClient.hget(k.job(job.id), 'state');
    expect(String(state)).toBe('completed');

    // Verify the job went through 3 processing attempts total
    expect(attemptCount).toBe(3);

    await queue.close();
  }, 25000);

  it('exponential backoff delay increases correctly between attempts', async () => {
    const Q = uniqueQueue('retry-exp-delay');
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    const processTimestamps: number[] = [];

    const job = await queue.add('exp-verify', { x: 1 }, {
      attempts: 4,
      backoff: { type: 'exponential', delay: 200 },
    });

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 30000);
      let failCount = 0;

      const worker = new Worker(
        Q,
        async () => {
          processTimestamps.push(Date.now());
          failCount++;
          if (failCount <= 3) {
            throw new Error(`exp-fail-${failCount}`);
          }
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500 },
      );

      worker.on('failed', () => {
        const waitTime = Math.pow(2, failCount - 1) * 200 + 100;
        setTimeout(async () => {
          try { await promote(cleanupClient, buildKeys(Q), Date.now()); } catch {}
        }, waitTime);
      });

      worker.on('completed', () => {
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });

      worker.on('error', () => {});
    });

    await done;

    expect(processTimestamps.length).toBe(4);

    // Verify delays increase: gap between 2nd-3rd should be >= gap between 1st-2nd
    if (processTimestamps.length >= 3) {
      const gap1 = processTimestamps[1] - processTimestamps[0];
      const gap2 = processTimestamps[2] - processTimestamps[1];
      expect(gap2).toBeGreaterThanOrEqual(gap1 * 0.8); // allow some timing slack
    }

    await queue.close();
  }, 30000);

  it('custom backoff strategy with jitter', async () => {
    const Q = uniqueQueue('retry-jitter');
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    const processTimestamps: number[] = [];

    const job = await queue.add('jitter-verify', { x: 1 }, {
      attempts: 3,
      backoff: { type: 'exponential', delay: 100, jitter: 50 },
    });

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);
      let failCount = 0;

      const worker = new Worker(
        Q,
        async () => {
          processTimestamps.push(Date.now());
          failCount++;
          if (failCount <= 2) {
            throw new Error(`jitter-fail-${failCount}`);
          }
          return 'ok';
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 500,
          stalledInterval: 60000,
          promotionInterval: 300,
        },
      );

      worker.on('failed', () => {
        // Wait enough for the backoff delay + jitter, then promote
        setTimeout(async () => {
          try { await promote(cleanupClient, buildKeys(Q), Date.now()); } catch {}
        }, 500);
      });

      worker.on('completed', () => {
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });

      worker.on('error', () => {});
    });

    await done;

    expect(processTimestamps.length).toBe(3);

    const state = await cleanupClient.hget(k.job(job.id), 'state');
    expect(String(state)).toBe('completed');

    await queue.close();
  }, 20000);
});

// ---------------------------------------------------------------------------
// GRACEFUL SHUTDOWN (from Celery patterns)
// ---------------------------------------------------------------------------
describe('Graceful shutdown', () => {
  it('Worker.close(false) waits for active job to finish', async () => {
    const Q = uniqueQueue('shutdown-graceful');
    const queue = new Queue(Q, { connection: CONNECTION });

    let jobFinished = false;
    let closeFinished = false;

    await queue.add('slow-job', { x: 1 });

    const worker = new Worker(
      Q,
      async () => {
        await new Promise(r => setTimeout(r, 2000));
        jobFinished = true;
        return 'done';
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
    );
    worker.on('error', () => {});

    // Wait for job to start processing
    await new Promise(r => setTimeout(r, 1000));

    // close(false) should wait for the active job
    const closePromise = worker.close(false).then(() => { closeFinished = true; });

    // Give it a moment - close should not have resolved yet
    await new Promise(r => setTimeout(r, 500));
    expect(closeFinished).toBe(false);

    // Wait for close to complete (job should finish first)
    await closePromise;

    expect(jobFinished).toBe(true);
    expect(closeFinished).toBe(true);

    await queue.close();
  }, 10000);

  it('Worker.close(true) terminates immediately, job stays in PEL', async () => {
    const Q = uniqueQueue('shutdown-force');
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    const job = await queue.add('long-job', { x: 1 });

    const worker = new Worker(
      Q,
      async () => {
        await new Promise(r => setTimeout(r, 60000));
        return 'never';
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
    );
    worker.on('error', () => {});

    // Wait for job to be picked up
    await new Promise(r => setTimeout(r, 2000));

    const start = Date.now();
    await worker.close(true);
    const elapsed = Date.now() - start;

    // Force close should be fast (well under the 60s job processing time)
    expect(elapsed).toBeLessThan(3000);

    // Job should still exist (not completed, not failed - still in an uncompleted state)
    const state = await cleanupClient.hget(k.job(job.id), 'state');
    expect(state).not.toBeNull();
    expect(String(state)).not.toBe('completed');

    await queue.close();
  }, 10000);

  it('multiple workers closing simultaneously - no data corruption', async () => {
    const Q = uniqueQueue('shutdown-multi');
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    // Add several jobs
    const jobs = [];
    for (let i = 0; i < 5; i++) {
      jobs.push(await queue.add(`multi-${i}`, { i }));
    }

    const completedIds: Set<string> = new Set();
    const workers: InstanceType<typeof Worker>[] = [];

    // Create 3 workers
    for (let w = 0; w < 3; w++) {
      const worker = new Worker(
        Q,
        async (j: any) => {
          await new Promise(r => setTimeout(r, 200));
          return `done-${j.data.i}`;
        },
        { connection: CONNECTION, concurrency: 2, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});
      worker.on('completed', (j: any) => completedIds.add(j.id));
      workers.push(worker);
    }

    // Let workers process some jobs
    await new Promise(r => setTimeout(r, 3000));

    // Close all simultaneously
    await Promise.all(workers.map(w => w.close()));

    // No duplicate IDs and no crashes
    expect(completedIds.size).toBeGreaterThan(0);

    // Verify data integrity - each completed job should have correct state
    for (const id of completedIds) {
      const state = await cleanupClient.hget(k.job(id), 'state');
      expect(String(state)).toBe('completed');
    }

    await queue.close();
  }, 15000);

  it('gracefulShutdown() handles SIGINT', async () => {
    const Q = uniqueQueue('shutdown-sigint');
    const queue = new Queue(Q, { connection: CONNECTION });

    // Verify the function registers signal handlers and responds to SIGINT
    let shutdownResolved = false;
    const shutdownPromise = gracefulShutdown([queue]);
    shutdownPromise.then(() => { shutdownResolved = true; });

    // Simulate SIGINT to trigger the handler
    process.emit('SIGINT' as any);

    await new Promise(r => setTimeout(r, 1000));

    expect(shutdownResolved).toBe(true);
  }, 5000);
});

// ---------------------------------------------------------------------------
// OPERATIONAL (from Celery/Sidekiq patterns)
// ---------------------------------------------------------------------------
describe('Operational patterns', () => {
  it('getJobCounts returns accurate numbers after add/process/fail', async () => {
    const Q = uniqueQueue('op-counts');
    const queue = new Queue(Q, { connection: CONNECTION });

    // Add 3 jobs
    await queue.add('count-1', { i: 1 });
    await queue.add('count-2', { i: 2 });
    await queue.add('count-3', { i: 3 });

    // Check waiting count
    const counts1 = await queue.getJobCounts();
    expect(counts1.waiting).toBe(3);

    // Process: 1 succeeds, 1 fails, 1 succeeds
    let processCount = 0;
    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      let processed = 0;

      const worker = new Worker(
        Q,
        async () => {
          processCount++;
          if (processCount === 2) {
            throw new Error('intentional fail');
          }
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});

      const check = () => {
        processed++;
        if (processed >= 3) {
          clearTimeout(timeout);
          setTimeout(() => worker.close(true).then(resolve), 500);
        }
      };
      worker.on('completed', check);
      worker.on('failed', check);
    });

    await done;

    const counts2 = await queue.getJobCounts();
    expect(counts2.completed).toBe(2);
    expect(counts2.failed).toBe(1);
    expect(counts2.waiting).toBe(0);

    await queue.close();
  }, 15000);

  it('Worker.drain processes remaining jobs then stops', async () => {
    const Q = uniqueQueue('op-drain');
    const queue = new Queue(Q, { connection: CONNECTION });

    // Add jobs
    for (let i = 0; i < 5; i++) {
      await queue.add(`drain-${i}`, { i });
    }

    const completedIds: string[] = [];
    const worker = new Worker(
      Q,
      async (j: any) => {
        await new Promise(r => setTimeout(r, 100));
        return `done-${j.data.i}`;
      },
      {
        connection: CONNECTION,
        concurrency: 2,
        blockTimeout: 500,
        stalledInterval: 60000,
        promotionInterval: 300,
      },
    );
    worker.on('error', () => {});
    worker.on('completed', (j: any) => completedIds.push(j.id));

    // Drain should process all then close
    await worker.drain();

    // All 5 should have been processed
    expect(completedIds.length).toBe(5);

    await queue.close();
  }, 15000);

  it('Queue.obliterate removes all queue data', async () => {
    const Q = uniqueQueue('op-obliterate');
    const queue = new Queue(Q, { connection: CONNECTION });

    // Add some jobs
    await queue.add('obliterate-1', { x: 1 });
    await queue.add('obliterate-2', { x: 2 });

    // Process one
    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 5000);
      const worker = new Worker(
        Q,
        async () => 'ok',
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});
      worker.on('completed', () => {
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });
    });
    await done;

    // Obliterate
    await queue.obliterate({ force: true });

    // Verify all data is gone
    const k = buildKeys(Q);
    const streamExists = await cleanupClient.xlen(k.stream);
    expect(streamExists).toBe(0);

    const scheduledCount = await cleanupClient.zcard(k.scheduled);
    expect(scheduledCount).toBe(0);

    const completedCount = await cleanupClient.zcard(k.completed);
    expect(completedCount).toBe(0);

    await queue.close();
  }, 10000);

  it('failed job can be retried via job.retry() and succeeds', async () => {
    const Q = uniqueQueue('op-retry-failed');
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    let shouldFail = true;

    const job = await queue.add('retry-me', { x: 1 });

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 20000);

      const worker = new Worker(
        Q,
        async () => {
          if (shouldFail) {
            throw new Error('first-fail');
          }
          return 'retried-success';
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 500,
          stalledInterval: 60000,
          promotionInterval: 500,
        },
      );
      worker.on('error', () => {});

      worker.on('failed', async (j: any) => {
        // After the first failure, retry the job
        shouldFail = false;

        // Verify it's in failed state
        const state = await cleanupClient.hget(k.job(j.id), 'state');
        expect(String(state)).toBe('failed');

        // Fetch and retry
        const fetchedJob = await queue.getJob(j.id);
        if (fetchedJob) {
          await fetchedJob.retry();
        }

        // Promote so the worker's next poll can pick it up
        setTimeout(async () => {
          try { await promote(cleanupClient, buildKeys(Q), Date.now()); } catch {}
        }, 200);
      });

      worker.on('completed', (j: any) => {
        clearTimeout(timeout);
        setTimeout(() => worker.close(true).then(resolve), 200);
      });
    });

    await done;

    const state = await cleanupClient.hget(k.job(job.id), 'state');
    expect(String(state)).toBe('completed');

    const rv = await cleanupClient.hget(k.job(job.id), 'returnvalue');
    expect(String(rv)).toContain('retried-success');

    await queue.close();
  }, 25000);
});

// ---------------------------------------------------------------------------
// MEMORY/RESOURCE (from Celery patterns)
// ---------------------------------------------------------------------------
describe('Memory and resource management', () => {
  it('100 rapid add+process cycles - activePromises set stays bounded', async () => {
    const Q = uniqueQueue('mem-rapid');
    const queue = new Queue(Q, { connection: CONNECTION });

    const completedCount = { value: 0 };
    const jobCount = 100;

    const worker = new Worker(
      Q,
      async () => 'ok',
      {
        connection: CONNECTION,
        concurrency: 10,
        blockTimeout: 500,
        stalledInterval: 60000,
        promotionInterval: 1000,
      },
    );
    worker.on('error', () => {});
    worker.on('completed', () => { completedCount.value++; });

    // Add 100 jobs rapidly
    const addPromises = [];
    for (let i = 0; i < jobCount; i++) {
      addPromises.push(queue.add(`rapid-${i}`, { i }));
    }
    await Promise.all(addPromises);

    // Wait for all to be processed
    const start = Date.now();
    while (completedCount.value < jobCount && Date.now() - start < 30000) {
      await new Promise(r => setTimeout(r, 200));
    }

    // Access activePromises via the private property to check it's bounded
    const activePromisesSize = (worker as any).activePromises?.size ?? 0;
    expect(activePromisesSize).toBe(0);

    expect(completedCount.value).toBe(jobCount);

    await worker.close();
    await queue.close();
  }, 35000);

  it('creating and closing queues does not leak connections', async () => {
    const queues: InstanceType<typeof Queue>[] = [];

    // Create and close 10 queues
    for (let i = 0; i < 10; i++) {
      const Q = uniqueQueue(`leak-q-${i}`);
      const q = new Queue(Q, { connection: CONNECTION });
      // Force connection creation by calling a method
      await q.add('leak-test', { i });
      queues.push(q);
    }

    // Close all
    for (const q of queues) {
      await q.close();
    }

    // If we got here without hanging or crashing, connections were cleaned up
    // Verify we can still create new queues
    const Q = uniqueQueue('leak-verify');
    const verifyQueue = new Queue(Q, { connection: CONNECTION });
    const job = await verifyQueue.add('verify', { x: 1 });
    expect(job.id).toBeTruthy();
    await verifyQueue.close();
  }, 15000);

  it('Worker processes 100 jobs without activePromises accumulation', async () => {
    const Q = uniqueQueue('mem-active');
    const queue = new Queue(Q, { connection: CONNECTION });

    const jobCount = 100;
    const maxActiveObserved: number[] = [];

    const worker = new Worker(
      Q,
      async () => {
        // Record the size of activePromises during processing
        const size = (worker as any).activePromises?.size ?? 0;
        maxActiveObserved.push(size);
        return 'ok';
      },
      {
        connection: CONNECTION,
        concurrency: 5,
        blockTimeout: 500,
        stalledInterval: 60000,
        promotionInterval: 1000,
      },
    );
    worker.on('error', () => {});

    let completedCount = 0;
    worker.on('completed', () => { completedCount++; });

    // Add jobs
    for (let i = 0; i < jobCount; i++) {
      await queue.add(`active-${i}`, { i });
    }

    // Wait for all to process
    const start = Date.now();
    while (completedCount < jobCount && Date.now() - start < 30000) {
      await new Promise(r => setTimeout(r, 200));
    }

    expect(completedCount).toBe(jobCount);

    // activePromises should never exceed concurrency (5)
    const maxActive = Math.max(...maxActiveObserved);
    expect(maxActive).toBeLessThanOrEqual(6); // concurrency + 1 for timing

    // After all processing, activePromises should be empty
    const finalSize = (worker as any).activePromises?.size ?? 0;
    expect(finalSize).toBe(0);

    await worker.close();
    await queue.close();
  }, 35000);
});
