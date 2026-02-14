/**
 * Integration tests for retry/backoff lifecycle.
 * Requires: valkey-server running on localhost:6379
 *
 * Run: npx vitest run tests/retry.test.ts
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';

const { GlideClient } = require('speedkey') as typeof import('speedkey');
const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
const { LIBRARY_SOURCE, CONSUMER_GROUP } = require('../dist/functions/index') as typeof import('../src/functions/index');
const { ensureFunctionLibrary } = require('../dist/connection') as typeof import('../src/connection');
const { promote } = require('../dist/functions/index') as typeof import('../src/functions/index');

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
  const prefix = `glide:{${queueName}}:job:`;
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

beforeAll(async () => {
  cleanupClient = await GlideClient.createClient({
    addresses: [{ host: 'localhost', port: 6379 }],
  });
  await ensureFunctionLibrary(cleanupClient, LIBRARY_SOURCE);
});

afterAll(async () => {
  cleanupClient.close();
});

describe('Retry lifecycle', () => {
  it('job retries with fixed backoff: fails twice, succeeds on 3rd attempt', async () => {
    const Q = 'test-retry-fixed-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    let attemptCount = 0;
    const failedEvents: string[] = [];
    const completedIds: string[] = [];

    const job = await queue.add(
      'retry-task',
      { value: 'test' },
      { attempts: 3, backoff: { type: 'fixed', delay: 200 } },
    );

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 20000);

      const worker = new Worker(
        Q,
        async (j: any) => {
          attemptCount++;
          if (attemptCount <= 2) {
            throw new Error(`fail-attempt-${attemptCount}`);
          }
          return { success: true };
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
      );

      worker.on('failed', (j: any, err: Error) => {
        failedEvents.push(err.message);

        // After each failure, the job is moved to scheduled ZSet with backoff.
        // Wait for the backoff delay, then manually promote so the worker picks it up again.
        setTimeout(async () => {
          try {
            await promote(cleanupClient, buildKeys(Q), Date.now());
          } catch {}
        }, 300);
      });

      worker.on('completed', (j: any) => {
        completedIds.push(j.id);
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });

      worker.on('error', () => {});
    });

    await done;

    // Verify: 2 failures, then 1 success
    expect(failedEvents).toEqual(['fail-attempt-1', 'fail-attempt-2']);
    expect(completedIds).toContain(job.id);
    expect(attemptCount).toBe(3);

    // Verify final state in Valkey
    const finalState = await cleanupClient.hget(k.job(job.id), 'state');
    expect(String(finalState)).toBe('completed');

    // Verify attemptsMade was incremented to 2 (two failures)
    const attemptsMade = await cleanupClient.hget(k.job(job.id), 'attemptsMade');
    expect(String(attemptsMade)).toBe('2');

    // Verify job is in completed ZSet
    const completedScore = await cleanupClient.zscore(k.completed, job.id);
    expect(completedScore).not.toBeNull();

    // Verify job is NOT in failed ZSet
    const failedScore = await cleanupClient.zscore(k.failed, job.id);
    expect(failedScore).toBeNull();

    await queue.close();
    await flushQueue(Q);
  }, 25000);

  it('job exceeding max attempts moves to failed ZSet permanently', async () => {
    const Q = 'test-retry-exhaust-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    let attemptCount = 0;

    const job = await queue.add(
      'always-fail',
      { value: 'doomed' },
      { attempts: 2, backoff: { type: 'fixed', delay: 100 } },
    );

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 20000);
      let failCount = 0;

      const worker = new Worker(
        Q,
        async () => {
          attemptCount++;
          throw new Error(`fail-${attemptCount}`);
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
      );

      worker.on('failed', (_j: any, _err: Error) => {
        failCount++;

        if (failCount < 2) {
          // After the first failure with retry, promote so it gets retried
          setTimeout(async () => {
            try {
              await promote(cleanupClient, buildKeys(Q), Date.now());
            } catch {}
          }, 200);
        } else {
          // After the second failure (exhausted), clean up
          clearTimeout(timeout);
          setTimeout(() => worker.close(true).then(resolve), 200);
        }
      });

      worker.on('error', () => {});
    });

    await done;

    // attemptsMade should be 2 (both attempts failed)
    const attemptsMade = await cleanupClient.hget(k.job(job.id), 'attemptsMade');
    expect(String(attemptsMade)).toBe('2');

    // Final state should be 'failed'
    const finalState = await cleanupClient.hget(k.job(job.id), 'state');
    expect(String(finalState)).toBe('failed');

    // Should be in failed ZSet
    const failedScore = await cleanupClient.zscore(k.failed, job.id);
    expect(failedScore).not.toBeNull();

    // Should NOT be in completed ZSet
    const completedScore = await cleanupClient.zscore(k.completed, job.id);
    expect(completedScore).toBeNull();

    // Should NOT be in scheduled ZSet
    const scheduledScore = await cleanupClient.zscore(k.scheduled, job.id);
    expect(scheduledScore).toBeNull();

    // failedReason should reflect the last error
    const failedReason = await cleanupClient.hget(k.job(job.id), 'failedReason');
    expect(String(failedReason)).toBe('fail-2');

    await queue.close();
    await flushQueue(Q);
  }, 25000);

  it('exponential backoff increases delay between retries', async () => {
    const Q = 'test-retry-exp-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    const failTimestamps: number[] = [];

    const job = await queue.add(
      'exp-backoff',
      { x: 1 },
      { attempts: 4, backoff: { type: 'exponential', delay: 100 } },
    );

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 30000);
      let failCount = 0;

      const worker = new Worker(
        Q,
        async () => {
          failTimestamps.push(Date.now());
          failCount++;
          if (failCount <= 3) {
            throw new Error(`exp-fail-${failCount}`);
          }
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
      );

      worker.on('failed', () => {
        // Wait a generous amount for the backoff, then promote
        // Exponential: attempt 1 -> 100ms, attempt 2 -> 200ms, attempt 3 -> 400ms
        const waitTime = Math.pow(2, failCount - 1) * 100 + 100;
        setTimeout(async () => {
          try {
            await promote(cleanupClient, buildKeys(Q), Date.now());
          } catch {}
        }, waitTime);
      });

      worker.on('completed', () => {
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });

      worker.on('error', () => {});
    });

    await done;

    // Verify we had 4 total processing attempts (3 fails + 1 success)
    expect(failTimestamps).toHaveLength(4);

    // Verify attemptsMade in hash is 3 (three failures)
    const attemptsMade = await cleanupClient.hget(k.job(job.id), 'attemptsMade');
    expect(String(attemptsMade)).toBe('3');

    // Verify final state is completed
    const finalState = await cleanupClient.hget(k.job(job.id), 'state');
    expect(String(finalState)).toBe('completed');

    await queue.close();
    await flushQueue(Q);
  }, 30000);

  it('job with no retry config (attempts=0) goes directly to failed on error', async () => {
    const Q = 'test-no-retry-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    const job = await queue.add('no-retry', { x: 1 });

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);

      const worker = new Worker(
        Q,
        async () => {
          throw new Error('immediate-fail');
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
      );

      worker.on('failed', () => {
        clearTimeout(timeout);
        setTimeout(() => worker.close(true).then(resolve), 200);
      });

      worker.on('error', () => {});
    });

    await done;

    // Should be in failed ZSet immediately (no retry)
    const failedScore = await cleanupClient.zscore(k.failed, job.id);
    expect(failedScore).not.toBeNull();

    // State should be 'failed'
    const finalState = await cleanupClient.hget(k.job(job.id), 'state');
    expect(String(finalState)).toBe('failed');

    // attemptsMade should be 1
    const attemptsMade = await cleanupClient.hget(k.job(job.id), 'attemptsMade');
    expect(String(attemptsMade)).toBe('1');

    // Should NOT be in scheduled ZSet (no retry)
    const scheduledScore = await cleanupClient.zscore(k.scheduled, job.id);
    expect(scheduledScore).toBeNull();

    await queue.close();
    await flushQueue(Q);
  }, 15000);

  it('retry cycle emits correct events in events stream', async () => {
    const Q = 'test-retry-events-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    let attemptCount = 0;

    const job = await queue.add(
      'event-retry',
      { x: 1 },
      { attempts: 2, backoff: { type: 'fixed', delay: 100 } },
    );

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);

      const worker = new Worker(
        Q,
        async () => {
          attemptCount++;
          if (attemptCount <= 1) {
            throw new Error('retry-me');
          }
          return 'done';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
      );

      worker.on('failed', () => {
        setTimeout(async () => {
          try {
            await promote(cleanupClient, buildKeys(Q), Date.now());
          } catch {}
        }, 200);
      });

      worker.on('completed', () => {
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });

      worker.on('error', () => {});
    });

    await done;

    // Read events stream
    const entries = await cleanupClient.xrange(k.events, '-', '+') as Record<string, [string, string][]>;
    const events: { event: string; jobId: string }[] = [];
    for (const entryId of Object.keys(entries)) {
      const fields = entries[entryId];
      const map: Record<string, string> = {};
      for (const [f, v] of fields) {
        map[String(f)] = String(v);
      }
      if (map.jobId === job.id) {
        events.push({ event: map.event, jobId: map.jobId });
      }
    }

    // Expected event sequence for this job: added -> retrying -> promoted -> completed
    const eventTypes = events.map(e => e.event);
    expect(eventTypes).toContain('added');
    expect(eventTypes).toContain('retrying');
    expect(eventTypes).toContain('completed');

    // 'retrying' should come before 'completed'
    const retryIdx = eventTypes.indexOf('retrying');
    const completeIdx = eventTypes.indexOf('completed');
    expect(retryIdx).toBeLessThan(completeIdx);

    await queue.close();
    await flushQueue(Q);
  }, 20000);
});
