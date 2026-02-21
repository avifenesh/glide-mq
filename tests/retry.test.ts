/**
 * Integration tests for retry/backoff lifecycle.
 * Requires: valkey-server running on localhost:6379 and cluster on :7000-7005
 *
 * Run: npx vitest run tests/retry.test.ts
 */
import { it, expect, beforeAll, afterAll } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
const { promote } = require('../dist/functions/index') as typeof import('../src/functions/index');

import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

describeEachMode('Retry lifecycle', (CONNECTION) => {
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    cleanupClient.close();
  });

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

    expect(failedEvents).toEqual(['fail-attempt-1', 'fail-attempt-2']);
    expect(completedIds).toContain(job.id);
    expect(attemptCount).toBe(3);

    const finalState = await cleanupClient.hget(k.job(job.id), 'state');
    expect(String(finalState)).toBe('completed');

    const attemptsMade = await cleanupClient.hget(k.job(job.id), 'attemptsMade');
    expect(String(attemptsMade)).toBe('2');

    const completedScore = await cleanupClient.zscore(k.completed, job.id);
    expect(completedScore).not.toBeNull();

    const failedScore = await cleanupClient.zscore(k.failed, job.id);
    expect(failedScore).toBeNull();

    await queue.close();
    await flushQueue(cleanupClient, Q);
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
          setTimeout(async () => {
            try {
              await promote(cleanupClient, buildKeys(Q), Date.now());
            } catch {}
          }, 200);
        } else {
          clearTimeout(timeout);
          setTimeout(() => worker.close(true).then(resolve), 200);
        }
      });

      worker.on('error', () => {});
    });

    await done;

    const attemptsMade = await cleanupClient.hget(k.job(job.id), 'attemptsMade');
    expect(String(attemptsMade)).toBe('2');

    const finalState = await cleanupClient.hget(k.job(job.id), 'state');
    expect(String(finalState)).toBe('failed');

    const failedScore = await cleanupClient.zscore(k.failed, job.id);
    expect(failedScore).not.toBeNull();

    const completedScore = await cleanupClient.zscore(k.completed, job.id);
    expect(completedScore).toBeNull();

    const scheduledScore = await cleanupClient.zscore(k.scheduled, job.id);
    expect(scheduledScore).toBeNull();

    const failedReason = await cleanupClient.hget(k.job(job.id), 'failedReason');
    expect(String(failedReason)).toBe('fail-2');

    await queue.close();
    await flushQueue(cleanupClient, Q);
  }, 25000);

  it('exponential backoff increases delay between retries', async () => {
    const Q = 'test-retry-exp-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    const failTimestamps: number[] = [];

    const job = await queue.add('exp-backoff', { x: 1 }, { attempts: 4, backoff: { type: 'exponential', delay: 100 } });

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

    expect(failTimestamps).toHaveLength(4);

    const attemptsMade = await cleanupClient.hget(k.job(job.id), 'attemptsMade');
    expect(String(attemptsMade)).toBe('3');

    const finalState = await cleanupClient.hget(k.job(job.id), 'state');
    expect(String(finalState)).toBe('completed');

    await queue.close();
    await flushQueue(cleanupClient, Q);
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

    const failedScore = await cleanupClient.zscore(k.failed, job.id);
    expect(failedScore).not.toBeNull();

    const finalState = await cleanupClient.hget(k.job(job.id), 'state');
    expect(String(finalState)).toBe('failed');

    const attemptsMade = await cleanupClient.hget(k.job(job.id), 'attemptsMade');
    expect(String(attemptsMade)).toBe('1');

    const scheduledScore = await cleanupClient.zscore(k.scheduled, job.id);
    expect(scheduledScore).toBeNull();

    await queue.close();
    await flushQueue(cleanupClient, Q);
  }, 15000);

  it('retry cycle emits correct events in events stream', async () => {
    const Q = 'test-retry-events-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    let attemptCount = 0;

    const job = await queue.add('event-retry', { x: 1 }, { attempts: 2, backoff: { type: 'fixed', delay: 100 } });

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

    const entries = (await cleanupClient.xrange(k.events, '-', '+')) as Record<string, [string, string][]>;
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

    const eventTypes = events.map((e) => e.event);
    expect(eventTypes).toContain('added');
    expect(eventTypes).toContain('retrying');
    expect(eventTypes).toContain('completed');

    const retryIdx = eventTypes.indexOf('retrying');
    const completeIdx = eventTypes.indexOf('completed');
    expect(retryIdx).toBeLessThan(completeIdx);

    await queue.close();
    await flushQueue(cleanupClient, Q);
  }, 20000);
});
