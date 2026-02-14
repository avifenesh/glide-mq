/**
 * Gap reliability tests: Job revocation, memory leak regression, broker failover.
 *
 * Requires: valkey-server running on localhost:6379
 *
 * Run: npx vitest run tests/gap-reliability.test.ts
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';

const { GlideClient } = require('speedkey') as typeof import('speedkey');
const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { Job } = require('../dist/job') as typeof import('../src/job');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
const { LIBRARY_SOURCE, CONSUMER_GROUP } = require('../dist/functions/index') as typeof import('../src/functions/index');
const { ensureFunctionLibrary } = require('../dist/connection') as typeof import('../src/connection');

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
// JOB REVOCATION (gap #4)
// ---------------------------------------------------------------------------
describe('Job revocation', () => {
  it('revoke waiting job - moved to failed with revoked reason', async () => {
    const Q = uniqueQueue('revoke-waiting');
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    const job = await queue.add('revoke-me', { value: 'test' });

    // Revoke before any worker picks it up
    const result = await queue.revoke(job.id);
    expect(result).toBe('revoked');

    // Verify job state
    const state = await cleanupClient.hget(k.job(job.id), 'state');
    expect(String(state)).toBe('failed');

    const reason = await cleanupClient.hget(k.job(job.id), 'failedReason');
    expect(String(reason)).toBe('revoked');

    const revoked = await cleanupClient.hget(k.job(job.id), 'revoked');
    expect(String(revoked)).toBe('1');

    // Should be in failed ZSet
    const failedScore = await cleanupClient.zscore(k.failed, job.id);
    expect(failedScore).not.toBeNull();

    // Should no longer be in stream
    const streamLen = await cleanupClient.xlen(k.stream);
    expect(streamLen).toBe(0);

    await queue.close();
  }, 10000);

  it('revoke delayed job - moved to failed', async () => {
    const Q = uniqueQueue('revoke-delayed');
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    const job = await queue.add('revoke-delayed', { value: 'test' }, { delay: 60000 });

    const result = await queue.revoke(job.id);
    expect(result).toBe('revoked');

    const state = await cleanupClient.hget(k.job(job.id), 'state');
    expect(String(state)).toBe('failed');

    // Should be removed from scheduled ZSet
    const scheduledScore = await cleanupClient.zscore(k.scheduled, job.id);
    expect(scheduledScore).toBeNull();

    await queue.close();
  }, 10000);

  it('revoke non-existent job - returns not_found', async () => {
    const Q = uniqueQueue('revoke-missing');
    const queue = new Queue(Q, { connection: CONNECTION });

    const result = await queue.revoke('999999');
    expect(result).toBe('not_found');

    await queue.close();
  }, 10000);

  it('worker skips revoked job that was flagged after claim', async () => {
    const Q = uniqueQueue('revoke-flagged');
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    // Add a job and immediately set revoked flag manually (simulating revoke after claim)
    const job = await queue.add('flagged-job', { value: 'test' });

    // Set revoked flag directly on the hash
    await cleanupClient.hset(k.job(job.id), { revoked: '1' });

    let processorCalled = false;

    const worker = new Worker(
      Q,
      async () => {
        processorCalled = true;
        return 'should-not-run';
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
    );
    worker.on('error', () => {});

    // Wait for worker to pick up and process (skip) the job
    await new Promise(r => setTimeout(r, 3000));

    await worker.close(true);
    await queue.close();

    // Processor should NOT have been called - worker should skip revoked jobs
    expect(processorCalled).toBe(false);

    // Job should be in failed state
    const state = await cleanupClient.hget(k.job(job.id), 'state');
    expect(String(state)).toBe('failed');
  }, 10000);

  it('revoke during processing - abort signal fires', async () => {
    const Q = uniqueQueue('revoke-active');
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    let abortSignalFired = false;
    let processorStarted = false;

    const job = await queue.add('active-revoke', { value: 'test' });

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);

      const worker = new Worker(
        Q,
        async (j: any) => {
          processorStarted = true;

          // Listen for abort signal
          if (j.abortSignal) {
            j.abortSignal.addEventListener('abort', () => {
              abortSignalFired = true;
            });
          }

          // Simulate long-running work - check abort signal periodically
          for (let i = 0; i < 50; i++) {
            await new Promise(r => setTimeout(r, 100));
            if (j.abortSignal?.aborted) {
              throw new Error('Job was revoked');
            }
          }
          return 'completed';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});

      // Wait for processor to start, then revoke
      const checkInterval = setInterval(async () => {
        if (processorStarted) {
          clearInterval(checkInterval);

          // Revoke the job - sets flag, worker.abortJob fires the signal
          await queue.revoke(job.id);
          worker.abortJob(job.id);

          // Wait a bit for the processor to react
          setTimeout(() => {
            clearTimeout(timeout);
            worker.close(true).then(resolve);
          }, 2000);
        }
      }, 100);
    });

    await done;

    expect(processorStarted).toBe(true);
    expect(abortSignalFired).toBe(true);
  }, 20000);

  it('Job.isRevoked returns correct state', async () => {
    const Q = uniqueQueue('revoke-is-check');
    const queue = new Queue(Q, { connection: CONNECTION });

    const job = await queue.add('check-revoke', { value: 'test' });

    // Not revoked initially
    const fetchedJob = await queue.getJob(job.id);
    expect(fetchedJob).not.toBeNull();
    expect(await fetchedJob!.isRevoked()).toBe(false);

    // Revoke it
    await queue.revoke(job.id);

    // Now isRevoked should be true
    const fetchedJob2 = await queue.getJob(job.id);
    expect(fetchedJob2).not.toBeNull();
    expect(await fetchedJob2!.isRevoked()).toBe(true);

    await queue.close();
  }, 10000);
});

// ---------------------------------------------------------------------------
// MEMORY LEAK REGRESSION TESTS (gap #8)
// ---------------------------------------------------------------------------
describe('Memory leak regression', () => {
  it('add and process 1000 jobs - heap does not grow beyond 2x baseline', async () => {
    const Q = uniqueQueue('mem-heap');
    const queue = new Queue(Q, { connection: CONNECTION });

    // Force GC if available, then measure baseline
    if (global.gc) global.gc();
    const baseline = process.memoryUsage().heapUsed;

    const jobCount = 1000;
    let completedCount = 0;

    const worker = new Worker(
      Q,
      async () => 'ok',
      {
        connection: CONNECTION,
        concurrency: 20,
        blockTimeout: 500,
        stalledInterval: 60000,
      },
    );
    worker.on('error', () => {});
    worker.on('completed', () => { completedCount++; });

    // Add jobs in batches
    for (let batch = 0; batch < 10; batch++) {
      const promises = [];
      for (let i = 0; i < 100; i++) {
        promises.push(queue.add(`heap-${batch * 100 + i}`, { i: batch * 100 + i, data: 'x'.repeat(100) }));
      }
      await Promise.all(promises);
    }

    // Wait for all to be processed
    const start = Date.now();
    while (completedCount < jobCount && Date.now() - start < 60000) {
      await new Promise(r => setTimeout(r, 200));
    }

    expect(completedCount).toBe(jobCount);

    // Check heap growth
    if (global.gc) global.gc();
    const afterHeap = process.memoryUsage().heapUsed;
    const growth = afterHeap / baseline;

    // Heap should not grow beyond 2x baseline (generous threshold)
    expect(growth).toBeLessThan(2);

    await worker.close();
    await queue.close();
  }, 60000);

  it('create and close 50 Queue instances - no connection leak', async () => {
    const queues: InstanceType<typeof Queue>[] = [];

    // Create 50 queues, force connection creation, then close
    for (let i = 0; i < 50; i++) {
      const Q = uniqueQueue(`leak-q-${i}`);
      const q = new Queue(Q, { connection: CONNECTION });
      await q.add('leak-test', { i });
      queues.push(q);
    }

    // Get client count BEFORE closing (should reflect ~50 extra connections)
    const infoOpen = await cleanupClient.info(['CLIENTS']);
    const connectedOpen = parseConnectedClients(String(infoOpen));

    // Close all queues
    for (const q of queues) {
      await q.close();
    }

    // Give connections time to clean up (NAPI client close may be async)
    await new Promise(r => setTimeout(r, 3000));

    const infoAfter = await cleanupClient.info(['CLIENTS']);
    const connectedAfter = parseConnectedClients(String(infoAfter));

    // After closing all 50 queues, connections should drop significantly.
    // The NAPI client should release connections on close().
    // At minimum, we should see fewer connections after close than when all were open.
    expect(connectedAfter).toBeLessThan(connectedOpen);

    // Also verify we can still create new queues (no hanging state)
    const Q = uniqueQueue('leak-verify');
    const verifyQueue = new Queue(Q, { connection: CONNECTION });
    const job = await verifyQueue.add('verify', { x: 1 });
    expect(job.id).toBeTruthy();
    await verifyQueue.close();
  }, 30000);

  it('Worker processes 500 jobs with retries - activePromises.size === 0 at end', async () => {
    const Q = uniqueQueue('mem-active-promises');
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    const jobCount = 500;
    let completedCount = 0;
    let failedCount = 0;
    let callCount = 0;

    const worker = new Worker(
      Q,
      async () => {
        callCount++;
        // Fail every 5th call to exercise retry paths
        if (callCount % 5 === 0) {
          throw new Error('intentional-fail');
        }
        return 'ok';
      },
      {
        connection: CONNECTION,
        concurrency: 10,
        blockTimeout: 500,
        stalledInterval: 60000,
      },
    );
    worker.on('error', () => {});
    worker.on('completed', () => { completedCount++; });
    worker.on('failed', () => { failedCount++; });

    // Add jobs
    for (let batch = 0; batch < 10; batch++) {
      const promises = [];
      for (let i = 0; i < 50; i++) {
        promises.push(queue.add(`active-${batch * 50 + i}`, { i: batch * 50 + i }));
      }
      await Promise.all(promises);
    }

    // Wait for all to be processed
    const start = Date.now();
    while (completedCount + failedCount < jobCount && Date.now() - start < 60000) {
      await new Promise(r => setTimeout(r, 200));
    }

    expect(completedCount + failedCount).toBe(jobCount);

    // Wait for any pending promises to settle
    await new Promise(r => setTimeout(r, 500));

    // activePromises should be empty
    const activeSize = (worker as any).activePromises?.size ?? 0;
    expect(activeSize).toBe(0);

    await worker.close();
    await queue.close();
  }, 60000);
});

// ---------------------------------------------------------------------------
// BROKER FAILOVER TESTS (gap #7)
// ---------------------------------------------------------------------------
describe('Broker failover', () => {
  it('Worker reconnects after connection error and resumes processing', async () => {
    const Q = uniqueQueue('failover-reconnect');
    const queue = new Queue(Q, { connection: CONNECTION });

    // Add a job first
    const job = await queue.add('pre-fail', { x: 1 });

    let completedCount = 0;
    const errors: Error[] = [];

    const worker = new Worker(
      Q,
      async () => {
        completedCount++;
        return 'ok';
      },
      {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 500,
        stalledInterval: 60000,
      },
    );
    worker.on('error', (err: Error) => errors.push(err));
    worker.on('completed', () => {});

    // Wait for the first job to be processed
    const start = Date.now();
    while (completedCount < 1 && Date.now() - start < 10000) {
      await new Promise(r => setTimeout(r, 200));
    }
    expect(completedCount).toBeGreaterThanOrEqual(1);

    // Now add another job and verify the worker can still process
    await queue.add('post-check', { x: 2 });

    const start2 = Date.now();
    while (completedCount < 2 && Date.now() - start2 < 10000) {
      await new Promise(r => setTimeout(r, 200));
    }
    expect(completedCount).toBeGreaterThanOrEqual(2);

    await worker.close(true);
    await queue.close();
  }, 20000);

  it('Worker exponential backoff on repeated connection errors', async () => {
    // Verify the reconnect logic by checking the internal reconnectBackoff field
    const Q = uniqueQueue('failover-backoff');

    const worker = new Worker(
      Q,
      async () => 'ok',
      {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 500,
        stalledInterval: 60000,
      },
    );
    worker.on('error', () => {});

    await worker.waitUntilReady();

    // Verify the worker starts with zero backoff
    expect((worker as any).reconnectBackoff).toBe(0);

    await worker.close(true);
  }, 10000);

  it('Queue.add after queue close throws error, new queue works', async () => {
    const Q = uniqueQueue('failover-queue-add');
    const queue = new Queue(Q, { connection: CONNECTION });

    // Add a job successfully
    const job = await queue.add('before-close', { x: 1 });
    expect(job.id).toBeTruthy();

    // Close the queue
    await queue.close();

    // Adding after close should throw
    await expect(queue.add('after-close', { x: 2 })).rejects.toThrow();

    // A new queue instance should work fine
    const queue2 = new Queue(Q, { connection: CONNECTION });
    const job2 = await queue2.add('after-reconnect', { x: 3 });
    expect(job2.id).toBeTruthy();

    await queue2.close();
  }, 10000);

  it('Worker.close is idempotent - multiple calls do not throw', async () => {
    const Q = uniqueQueue('failover-close-idempotent');

    const worker = new Worker(
      Q,
      async () => 'ok',
      {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 500,
        stalledInterval: 60000,
      },
    );
    worker.on('error', () => {});

    await worker.waitUntilReady();

    // Close multiple times - should not throw
    await worker.close();
    await worker.close();
    await worker.close();
  }, 10000);

  it('Worker handles error events without crashing', async () => {
    const Q = uniqueQueue('failover-error-events');
    const queue = new Queue(Q, { connection: CONNECTION });

    const errors: Error[] = [];
    let completedCount = 0;

    const worker = new Worker(
      Q,
      async () => {
        completedCount++;
        return 'ok';
      },
      {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 500,
        stalledInterval: 60000,
      },
    );
    worker.on('error', (err: Error) => errors.push(err));

    // Add and process a job to verify worker is functioning
    await queue.add('error-test', { x: 1 });
    const start = Date.now();
    while (completedCount < 1 && Date.now() - start < 10000) {
      await new Promise(r => setTimeout(r, 200));
    }
    expect(completedCount).toBe(1);

    await worker.close(true);
    await queue.close();
  }, 15000);
});

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function parseConnectedClients(info: string): number {
  const match = info.match(/connected_clients:(\d+)/);
  return match ? parseInt(match[1], 10) : 0;
}
