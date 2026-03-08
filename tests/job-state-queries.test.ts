/**
 * Comprehensive tests for Job state query methods (audit H10).
 * Covers: getState(), isCompleted(), isFailed(), isDelayed(), isActive(), isWaiting(), isRevoked()
 * Tests state transitions across the full job lifecycle.
 *
 * Requires: valkey-server on localhost:6379 and cluster on :7000-7005
 * Run: npx vitest run tests/job-state-queries.test.ts
 */
import { it, expect, beforeAll, afterAll, afterEach } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
const { promote } = require('../dist/functions/index') as typeof import('../src/functions/index');

import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

function uid() {
  return `job-state-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
}

describeEachMode('Job state queries', (CONNECTION) => {
  let cleanupClient: any;
  let queueName: string;
  let queue: InstanceType<typeof Queue>;
  let worker: InstanceType<typeof Worker> | undefined;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterEach(async () => {
    if (worker) {
      try {
        await worker.close(true);
      } catch {}
      worker = undefined;
    }
    if (queue) {
      try {
        await queue.close();
      } catch {}
    }
    if (queueName) await flushQueue(cleanupClient, queueName);
  });

  afterAll(async () => {
    cleanupClient.close();
  });

  // --- getState basic ---

  it('getState returns "waiting" for a newly added job', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });
    const job = await queue.add('test', { x: 1 });

    expect(await job!.getState()).toBe('waiting');
  });

  it('getState returns "delayed" for a job added with delay', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });
    const job = await queue.add('test', { x: 1 }, { delay: 60000 });

    expect(await job!.getState()).toBe('delayed');
  });

  it('getState returns "active" while job is being processed', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });

    let stateWhileActive: string | undefined;
    const processing = new Promise<void>((resolve) => {
      worker = new Worker(
        queueName,
        async (job) => {
          stateWhileActive = await job.getState();
          resolve();
          return 'done';
        },
        { connection: CONNECTION, stalledInterval: 60000 },
      );
    });

    await queue.add('test', { x: 1 });
    await processing;

    expect(stateWhileActive).toBe('active');
  });

  it('getState returns "completed" after successful processing', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });

    const completed = new Promise<void>((resolve) => {
      worker = new Worker(
        queueName,
        async () => 'done',
        { connection: CONNECTION, stalledInterval: 60000 },
      );
      worker.on('completed', () => resolve());
    });

    const job = await queue.add('test', { x: 1 });
    await completed;

    expect(await job!.getState()).toBe('completed');
  });

  it('getState returns "failed" after processor throws', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });

    const failed = new Promise<void>((resolve) => {
      worker = new Worker(
        queueName,
        async () => {
          throw new Error('boom');
        },
        { connection: CONNECTION, stalledInterval: 60000 },
      );
      worker.on('failed', () => resolve());
    });

    const job = await queue.add('test', { x: 1 });
    await failed;

    expect(await job!.getState()).toBe('failed');
  });

  it('getState returns "unknown" for a removed job', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });
    const job = await queue.add('test', { x: 1 });

    const fetched = await queue.getJob(job!.id);
    await fetched!.remove();

    expect(await job!.getState()).toBe('unknown');
  });

  // --- Boolean helpers ---

  it('isWaiting returns true for a newly added job', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });
    const job = await queue.add('test', { x: 1 });

    expect(await job!.isWaiting()).toBe(true);
    expect(await job!.isCompleted()).toBe(false);
    expect(await job!.isFailed()).toBe(false);
    expect(await job!.isDelayed()).toBe(false);
    expect(await job!.isActive()).toBe(false);
  });

  it('isDelayed returns true for a delayed job', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });
    const job = await queue.add('test', { x: 1 }, { delay: 60000 });

    expect(await job!.isDelayed()).toBe(true);
    expect(await job!.isWaiting()).toBe(false);
    expect(await job!.isCompleted()).toBe(false);
    expect(await job!.isFailed()).toBe(false);
    expect(await job!.isActive()).toBe(false);
  });

  it('isActive returns true while job is being processed', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });

    let activeCheck = false;
    let waitingCheck = true;
    const processing = new Promise<void>((resolve) => {
      worker = new Worker(
        queueName,
        async (job) => {
          activeCheck = await job.isActive();
          waitingCheck = await job.isWaiting();
          resolve();
          return 'ok';
        },
        { connection: CONNECTION, stalledInterval: 60000 },
      );
    });

    await queue.add('test', { x: 1 });
    await processing;

    expect(activeCheck).toBe(true);
    expect(waitingCheck).toBe(false);
  });

  it('isCompleted returns true after successful processing', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });

    const completed = new Promise<void>((resolve) => {
      worker = new Worker(
        queueName,
        async () => 'result',
        { connection: CONNECTION, stalledInterval: 60000 },
      );
      worker.on('completed', () => resolve());
    });

    const job = await queue.add('test', { x: 1 });
    await completed;

    expect(await job!.isCompleted()).toBe(true);
    expect(await job!.isWaiting()).toBe(false);
    expect(await job!.isActive()).toBe(false);
    expect(await job!.isFailed()).toBe(false);
  });

  it('isFailed returns true after processor throws', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });

    const failed = new Promise<void>((resolve) => {
      worker = new Worker(
        queueName,
        async () => {
          throw new Error('fail');
        },
        { connection: CONNECTION, stalledInterval: 60000 },
      );
      worker.on('failed', () => resolve());
    });

    const job = await queue.add('test', { x: 1 });
    await failed;

    expect(await job!.isFailed()).toBe(true);
    expect(await job!.isCompleted()).toBe(false);
    expect(await job!.isWaiting()).toBe(false);
    expect(await job!.isActive()).toBe(false);
  });

  // --- Full lifecycle transitions ---

  it('transition: waiting -> active -> completed', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });

    const job = await queue.add('lifecycle', { step: 'start' });
    expect(await job!.getState()).toBe('waiting');

    const completed = new Promise<void>((resolve) => {
      worker = new Worker(
        queueName,
        async () => 'done',
        { connection: CONNECTION, stalledInterval: 60000 },
      );
      worker.on('completed', () => resolve());
    });

    await completed;
    expect(await job!.getState()).toBe('completed');
  });

  it('transition: waiting -> active -> failed', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });

    const job = await queue.add('lifecycle', { step: 'start' });
    expect(await job!.getState()).toBe('waiting');

    const failed = new Promise<void>((resolve) => {
      worker = new Worker(
        queueName,
        async () => {
          throw new Error('intentional');
        },
        { connection: CONNECTION, stalledInterval: 60000 },
      );
      worker.on('failed', () => resolve());
    });

    await failed;
    expect(await job!.getState()).toBe('failed');
  });

  it('transition: delayed -> waiting (after promote) -> active -> completed', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });

    const job = await queue.add('delayed-lifecycle', { x: 1 }, { delay: 200 });
    expect(await job!.getState()).toBe('delayed');
    expect(await job!.isDelayed()).toBe(true);

    // Wait for delay to elapse, then promote
    await new Promise((r) => setTimeout(r, 300));
    const k = buildKeys(queueName);
    const promoted = await promote(cleanupClient, k, Date.now());
    expect(promoted).toBeGreaterThanOrEqual(1);

    expect(await job!.getState()).toBe('waiting');
    expect(await job!.isWaiting()).toBe(true);

    const completed = new Promise<void>((resolve) => {
      worker = new Worker(
        queueName,
        async () => 'processed',
        { connection: CONNECTION, stalledInterval: 60000 },
      );
      worker.on('completed', () => resolve());
    });

    await completed;
    expect(await job!.getState()).toBe('completed');
    expect(await job!.isCompleted()).toBe(true);
  });

  // --- Deleted job behavior ---

  it('all boolean helpers return false for a removed job', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });
    const job = await queue.add('test', { x: 1 });

    const fetched = await queue.getJob(job!.id);
    await fetched!.remove();

    expect(await job!.getState()).toBe('unknown');
    expect(await job!.isWaiting()).toBe(false);
    expect(await job!.isActive()).toBe(false);
    expect(await job!.isCompleted()).toBe(false);
    expect(await job!.isFailed()).toBe(false);
    expect(await job!.isDelayed()).toBe(false);
  });

  it('getJob returns null for a non-existent job ID', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });

    const fetched = await queue.getJob('does-not-exist-999');
    expect(fetched).toBeNull();
  });

  // --- Revocation state ---

  it('revoked waiting job state is "failed"', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });
    const job = await queue.add('revoke-test', { x: 1 });

    expect(await job!.getState()).toBe('waiting');
    await queue.revoke(job!.id);

    expect(await job!.getState()).toBe('failed');
    expect(await job!.isFailed()).toBe(true);
    expect(await job!.isWaiting()).toBe(false);
  });

  it('isRevoked returns true after revocation', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });
    const job = await queue.add('revoke-check', { x: 1 });

    expect(await job!.isRevoked()).toBe(false);
    await queue.revoke(job!.id);

    const fetched = await queue.getJob(job!.id);
    expect(await fetched!.isRevoked()).toBe(true);
  });

  it('revoked delayed job state is "failed"', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });
    const job = await queue.add('revoke-delayed', { x: 1 }, { delay: 60000 });

    expect(await job!.getState()).toBe('delayed');
    await queue.revoke(job!.id);

    expect(await job!.getState()).toBe('failed');
    expect(await job!.isFailed()).toBe(true);
    expect(await job!.isDelayed()).toBe(false);
  });

  it('isRevoked returns false for non-revoked completed job', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });

    const completed = new Promise<void>((resolve) => {
      worker = new Worker(
        queueName,
        async () => 'ok',
        { connection: CONNECTION, stalledInterval: 60000 },
      );
      worker.on('completed', () => resolve());
    });

    const job = await queue.add('no-revoke', { x: 1 });
    await completed;

    const fetched = await queue.getJob(job!.id);
    expect(await fetched!.isRevoked()).toBe(false);
    expect(await fetched!.isCompleted()).toBe(true);
  });

  // --- State queried via queue.getJob (refetch from Valkey) ---

  it('getJob returns job with correct state after completion', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });

    const completed = new Promise<void>((resolve) => {
      worker = new Worker(
        queueName,
        async () => 'result-value',
        { connection: CONNECTION, stalledInterval: 60000 },
      );
      worker.on('completed', () => resolve());
    });

    const job = await queue.add('fetch-state', { x: 1 });
    await completed;

    const fetched = await queue.getJob(job!.id);
    expect(fetched).not.toBeNull();
    expect(await fetched!.getState()).toBe('completed');
    expect(await fetched!.isCompleted()).toBe(true);
  });

  it('getJob returns job with correct state after failure', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });

    const failed = new Promise<void>((resolve) => {
      worker = new Worker(
        queueName,
        async () => {
          throw new Error('kaboom');
        },
        { connection: CONNECTION, stalledInterval: 60000 },
      );
      worker.on('failed', () => resolve());
    });

    const job = await queue.add('fetch-failed', { x: 1 });
    await failed;

    const fetched = await queue.getJob(job!.id);
    expect(fetched).not.toBeNull();
    expect(await fetched!.getState()).toBe('failed');
    expect(await fetched!.isFailed()).toBe(true);
    expect(fetched!.failedReason).toBe('kaboom');
  });

  // --- Prioritized job state ---

  it('getState returns "prioritized" for a job added with priority > 0', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });
    const job = await queue.add('prio', { x: 1 }, { priority: 5 });

    expect(await job!.getState()).toBe('prioritized');
  });

  // --- waitUntilFinished ---

  it('waitUntilFinished resolves with "completed" after processing', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });

    worker = new Worker(
      queueName,
      async () => 'ok',
      { connection: CONNECTION, stalledInterval: 60000 },
    );

    const job = await queue.add('wait-finish', { x: 1 });
    const finalState = await job!.waitUntilFinished(100, 10000);

    expect(finalState).toBe('completed');
  });

  it('waitUntilFinished resolves with "failed" after failure', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });

    worker = new Worker(
      queueName,
      async () => {
        throw new Error('nope');
      },
      { connection: CONNECTION, stalledInterval: 60000 },
    );

    const job = await queue.add('wait-fail', { x: 1 });
    const finalState = await job!.waitUntilFinished(100, 10000);

    expect(finalState).toBe('failed');
  });

  it('waitUntilFinished times out if job never finishes', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });
    // No worker - job stays in waiting forever
    const job = await queue.add('wait-timeout', { x: 1 });

    await expect(job!.waitUntilFinished(50, 200)).rejects.toThrow('did not finish within');
  });

  // --- Multiple state queries on same job ---

  it('state queries are consistent across multiple calls', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });
    const job = await queue.add('consistent', { x: 1 });

    // All queries should agree
    const state1 = await job!.getState();
    const state2 = await job!.getState();
    expect(state1).toBe(state2);
    expect(state1).toBe('waiting');

    const [w, a, c, f, d] = await Promise.all([
      job!.isWaiting(),
      job!.isActive(),
      job!.isCompleted(),
      job!.isFailed(),
      job!.isDelayed(),
    ]);
    expect(w).toBe(true);
    expect(a).toBe(false);
    expect(c).toBe(false);
    expect(f).toBe(false);
    expect(d).toBe(false);
  });
});
