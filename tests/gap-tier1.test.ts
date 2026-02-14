/**
 * Tier 1 gap tests: per-job timeout, job log, convenience methods,
 * worker state getters, custom backoff.
 * Requires: valkey-server running on localhost:6379
 *
 * Run: npx vitest run tests/gap-tier1.test.ts
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';

const { GlideClient } = require('speedkey') as typeof import('speedkey');
const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { Job } = require('../dist/job') as typeof import('../src/job');
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
  // Clean job hashes and log keys
  for (const suffix of ['job:', 'log:', 'deps:']) {
    const prefix = `glide:{${queueName}}:${suffix}`;
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

// ---- Gap #2: Per-job timeout ----

describe('Per-job timeout', () => {
  it('fails a job that exceeds its timeout', async () => {
    const Q = 'test-timeout-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    await queue.add('slow-task', { x: 1 }, { timeout: 500, attempts: 0 });

    const failedJobs: { job: any; error: Error }[] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('test timeout')), 15000);

      const worker = new Worker(
        Q,
        async () => {
          // Simulate a slow processor that takes longer than timeout
          await new Promise((r) => setTimeout(r, 3000));
          return 'should-not-reach';
        },
        { connection: CONNECTION, stalledInterval: 60000 },
      );

      worker.on('failed', (job: any, err: Error) => {
        failedJobs.push({ job, error: err });
        clearTimeout(timeout);
        worker.close(true).then(() => resolve());
      });

      worker.on('completed', () => {
        clearTimeout(timeout);
        worker.close(true).then(() => reject(new Error('Job should have timed out')));
      });
    });

    await done;

    expect(failedJobs.length).toBe(1);
    expect(failedJobs[0].error.message).toBe('Job timeout exceeded');

    await queue.close();
    await flushQueue(Q);
  });

  it('completes a job that finishes before its timeout', async () => {
    const Q = 'test-timeout-ok-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    await queue.add('fast-task', { x: 2 }, { timeout: 5000 });

    const completedJobs: any[] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('test timeout')), 15000);

      const worker = new Worker(
        Q,
        async () => {
          return 'fast-result';
        },
        { connection: CONNECTION, stalledInterval: 60000 },
      );

      worker.on('completed', (job: any) => {
        completedJobs.push(job);
        clearTimeout(timeout);
        worker.close(true).then(() => resolve());
      });

      worker.on('failed', (_job: any, err: Error) => {
        clearTimeout(timeout);
        worker.close(true).then(() => reject(err));
      });
    });

    await done;

    expect(completedJobs.length).toBe(1);
    expect(completedJobs[0].returnvalue).toBe('fast-result');

    await queue.close();
    await flushQueue(Q);
  });

  it('retries a timed-out job when attempts are configured', async () => {
    const Q = 'test-timeout-retry-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    await queue.add('timeout-retry', { x: 3 }, {
      timeout: 300,
      attempts: 2,
      backoff: { type: 'fixed', delay: 100 },
    });

    let attemptCount = 0;
    const completedJobs: any[] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('test timeout')), 20000);

      const worker = new Worker(
        Q,
        async () => {
          attemptCount++;
          if (attemptCount === 1) {
            // First attempt: exceed timeout
            await new Promise((r) => setTimeout(r, 2000));
            return 'should-not-reach';
          }
          // Second attempt: succeed quickly
          return 'retry-success';
        },
        { connection: CONNECTION, stalledInterval: 60000, promotionInterval: 200 },
      );

      worker.on('completed', (job: any) => {
        completedJobs.push(job);
        clearTimeout(timeout);
        worker.close(true).then(() => resolve());
      });
    });

    await done;

    expect(attemptCount).toBe(2);
    expect(completedJobs.length).toBe(1);
    expect(completedJobs[0].returnvalue).toBe('retry-success');

    await queue.close();
    await flushQueue(Q);
  });
});

// ---- Gap #12: Job log ----

describe('Job log', () => {
  it('adds log entries and retrieves them via Queue.getJobLogs', async () => {
    const Q = 'test-joblog-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    const completedJobs: any[] = [];

    const job = await queue.add('log-task', { x: 1 });

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('test timeout')), 15000);

      const worker = new Worker(
        Q,
        async (j: any) => {
          await j.log('Step 1: starting');
          await j.log('Step 2: processing');
          await j.log('Step 3: done');
          return 'logged';
        },
        { connection: CONNECTION, stalledInterval: 60000 },
      );

      worker.on('completed', (j: any) => {
        completedJobs.push(j);
        clearTimeout(timeout);
        worker.close(true).then(() => resolve());
      });
    });

    await done;

    expect(completedJobs.length).toBe(1);

    const { logs, count } = await queue.getJobLogs(job!.id);
    expect(count).toBe(3);
    expect(logs).toEqual(['Step 1: starting', 'Step 2: processing', 'Step 3: done']);

    // Test partial range
    const partial = await queue.getJobLogs(job!.id, 1, 1);
    expect(partial.logs).toEqual(['Step 2: processing']);

    await queue.close();
    await flushQueue(Q);
  });

  it('returns empty logs for a job with no logs', async () => {
    const Q = 'test-joblog-empty-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    const job = await queue.add('no-log-task', { x: 1 });

    const { logs, count } = await queue.getJobLogs(job!.id);
    expect(count).toBe(0);
    expect(logs).toEqual([]);

    await queue.close();
    await flushQueue(Q);
  });

  it('log key is removed when job is removed', async () => {
    const Q = 'test-joblog-remove-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    const job = await queue.add('log-remove-task', { x: 1 });

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('test timeout')), 15000);

      const worker = new Worker(
        Q,
        async (j: any) => {
          await j.log('some log');
          return 'ok';
        },
        { connection: CONNECTION, stalledInterval: 60000 },
      );

      worker.on('completed', () => {
        clearTimeout(timeout);
        worker.close(true).then(() => resolve());
      });
    });

    await done;

    // Verify log exists
    const beforeRemove = await queue.getJobLogs(job!.id);
    expect(beforeRemove.count).toBe(1);

    // Remove the job
    const fetched = await queue.getJob(job!.id);
    await fetched!.remove();

    // Log key should be gone
    const afterRemove = await queue.getJobLogs(job!.id);
    expect(afterRemove.count).toBe(0);

    await queue.close();
    await flushQueue(Q);
  });
});

// ---- Gap #14: Convenience methods ----

describe('Queue convenience methods', () => {
  it('isPaused() returns correct state', async () => {
    const Q = 'test-ispaused-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    expect(await queue.isPaused()).toBe(false);

    await queue.pause();
    expect(await queue.isPaused()).toBe(true);

    await queue.resume();
    expect(await queue.isPaused()).toBe(false);

    await queue.close();
    await flushQueue(Q);
  });

  it('count() returns waiting job count', async () => {
    const Q = 'test-count-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    expect(await queue.count()).toBe(0);

    await queue.add('task-1', { x: 1 });
    await queue.add('task-2', { x: 2 });
    await queue.add('task-3', { x: 3 });

    expect(await queue.count()).toBe(3);

    await queue.close();
    await flushQueue(Q);
  });

  it('getRepeatableJobs() returns registered schedulers', async () => {
    const Q = 'test-repeatables-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    // Initially empty
    const empty = await queue.getRepeatableJobs();
    expect(empty).toEqual([]);

    // Add schedulers
    await queue.upsertJobScheduler('daily-cleanup', { pattern: '0 0 * * *' });
    await queue.upsertJobScheduler('frequent-check', { every: 5000 });

    const jobs = await queue.getRepeatableJobs();
    expect(jobs.length).toBe(2);

    const names = jobs.map((j: any) => j.name).sort();
    expect(names).toEqual(['daily-cleanup', 'frequent-check']);

    const dailyJob = jobs.find((j: any) => j.name === 'daily-cleanup');
    expect(dailyJob!.entry.pattern).toBe('0 0 * * *');

    const frequentJob = jobs.find((j: any) => j.name === 'frequent-check');
    expect(frequentJob!.entry.every).toBe(5000);

    await queue.close();
    await flushQueue(Q);
  });
});

// ---- Gap #15: Worker state getters ----

describe('Worker state getters', () => {
  it('isRunning() and isPaused() reflect correct state', async () => {
    const Q = 'test-worker-state-' + Date.now();

    const worker = new Worker(
      Q,
      async () => 'result',
      { connection: CONNECTION, stalledInterval: 60000 },
    );
    await worker.waitUntilReady();

    // After init: running, not paused
    expect(worker.isRunning()).toBe(true);
    expect(worker.isPaused()).toBe(false);

    // After pause
    await worker.pause(true);
    expect(worker.isRunning()).toBe(false);
    expect(worker.isPaused()).toBe(true);

    // After resume
    await worker.resume();
    expect(worker.isRunning()).toBe(true);
    expect(worker.isPaused()).toBe(false);

    // After close
    await worker.close(true);
    expect(worker.isRunning()).toBe(false);

    await flushQueue(Q);
  });
});

// ---- Gap #16: Custom backoff function ----

describe('Custom backoff function', () => {
  it('uses registered custom backoff strategy', async () => {
    const Q = 'test-custom-backoff-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    const backoffDelays: number[] = [];

    await queue.add('custom-backoff-task', { x: 1 }, {
      attempts: 3,
      backoff: { type: 'linear', delay: 100 },
    });

    let attemptCount = 0;
    const completedJobs: any[] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('test timeout')), 20000);

      const worker = new Worker(
        Q,
        async () => {
          attemptCount++;
          if (attemptCount < 3) {
            throw new Error(`fail-${attemptCount}`);
          }
          return 'custom-backoff-done';
        },
        {
          connection: CONNECTION,
          stalledInterval: 60000,
          promotionInterval: 200,
          backoffStrategies: {
            linear: (attemptsMade: number, _err: Error) => {
              const delay = attemptsMade * 200;
              backoffDelays.push(delay);
              return delay;
            },
          },
        },
      );

      worker.on('completed', (job: any) => {
        completedJobs.push(job);
        clearTimeout(timeout);
        worker.close(true).then(() => resolve());
      });
    });

    await done;

    expect(attemptCount).toBe(3);
    expect(completedJobs.length).toBe(1);
    expect(completedJobs[0].returnvalue).toBe('custom-backoff-done');

    // Verify custom backoff was called with correct attempts
    expect(backoffDelays.length).toBe(2);
    expect(backoffDelays[0]).toBe(200); // attemptsMade=1
    expect(backoffDelays[1]).toBe(400); // attemptsMade=2

    await queue.close();
    await flushQueue(Q);
  });

  it('falls back to built-in backoff when no custom strategy matches', async () => {
    const Q = 'test-builtin-fallback-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    await queue.add('fixed-backoff-task', { x: 1 }, {
      attempts: 2,
      backoff: { type: 'fixed', delay: 200 },
    });

    let attemptCount = 0;
    const completedJobs: any[] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('test timeout')), 20000);

      const worker = new Worker(
        Q,
        async () => {
          attemptCount++;
          if (attemptCount < 2) {
            throw new Error('fail-once');
          }
          return 'built-in-backoff-done';
        },
        {
          connection: CONNECTION,
          stalledInterval: 60000,
          promotionInterval: 200,
          backoffStrategies: {
            custom: () => 999, // registered but not used by this job
          },
        },
      );

      worker.on('completed', (job: any) => {
        completedJobs.push(job);
        clearTimeout(timeout);
        worker.close(true).then(() => resolve());
      });
    });

    await done;

    expect(attemptCount).toBe(2);
    expect(completedJobs.length).toBe(1);

    await queue.close();
    await flushQueue(Q);
  });
});
