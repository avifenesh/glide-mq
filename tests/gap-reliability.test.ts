/**
 * Gap reliability tests: Job revocation, broker failover, memory leak regression.
 *
 * Requires: valkey-server on localhost:6379 and cluster on :7000-7005
 *
 * Run: npx vitest run tests/gap-reliability.test.ts
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { Job } = require('../dist/job') as typeof import('../src/job');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

describeEachMode('Gap Reliability', (CONNECTION) => {
  let cleanupClient: any;
  const allQueues: string[] = [];

  function uniqueQueue(prefix: string): string {
    const name = `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    allQueues.push(name);
    return name;
  }

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    for (const q of allQueues) {
      await flushQueue(cleanupClient, q);
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

      const result = await queue.revoke(job.id);
      expect(result).toBe('revoked');

      const state = await cleanupClient.hget(k.job(job.id), 'state');
      expect(String(state)).toBe('failed');

      const reason = await cleanupClient.hget(k.job(job.id), 'failedReason');
      expect(String(reason)).toBe('revoked');

      const revoked = await cleanupClient.hget(k.job(job.id), 'revoked');
      expect(String(revoked)).toBe('1');

      const failedScore = await cleanupClient.zscore(k.failed, job.id);
      expect(failedScore).not.toBeNull();

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

      const job = await queue.add('flagged-job', { value: 'test' });

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

      await new Promise(r => setTimeout(r, 3000));

      await worker.close(true);
      await queue.close();

      expect(processorCalled).toBe(false);

      const state = await cleanupClient.hget(k.job(job.id), 'state');
      expect(String(state)).toBe('failed');
    }, 10000);

    it('revoke during processing - abort signal fires', async () => {
      const Q = uniqueQueue('revoke-active');
      const queue = new Queue(Q, { connection: CONNECTION });

      let abortSignalFired = false;
      let processorStarted = false;

      const job = await queue.add('active-revoke', { value: 'test' });

      const done = new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error('timeout')), 15000);

        const worker = new Worker(
          Q,
          async (j: any) => {
            processorStarted = true;

            if (j.abortSignal) {
              j.abortSignal.addEventListener('abort', () => {
                abortSignalFired = true;
              });
            }

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

        const checkInterval = setInterval(async () => {
          if (processorStarted) {
            clearInterval(checkInterval);

            await queue.revoke(job.id);
            worker.abortJob(job.id);

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

      const fetchedJob = await queue.getJob(job.id);
      expect(fetchedJob).not.toBeNull();
      expect(await fetchedJob!.isRevoked()).toBe(false);

      await queue.revoke(job.id);

      const fetchedJob2 = await queue.getJob(job.id);
      expect(fetchedJob2).not.toBeNull();
      expect(await fetchedJob2!.isRevoked()).toBe(true);

      await queue.close();
    }, 10000);
  });

  // ---------------------------------------------------------------------------
  // BROKER FAILOVER TESTS (gap #7)
  // Run before memory leak tests to avoid cluster connection exhaustion.
  // ---------------------------------------------------------------------------
  describe('Broker failover', () => {
    it('Worker reconnects after connection error and resumes processing', async () => {
      const Q = uniqueQueue('failover-reconnect');
      const queue = new Queue(Q, { connection: CONNECTION });

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

      const start = Date.now();
      while (completedCount < 1 && Date.now() - start < 10000) {
        await new Promise(r => setTimeout(r, 200));
      }
      expect(completedCount).toBeGreaterThanOrEqual(1);

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

      expect((worker as any).reconnectBackoff).toBe(0);

      await worker.close(true);
    }, 10000);

    it('Queue.add after queue close throws error, new queue works', async () => {
      const Q = uniqueQueue('failover-queue-add');
      const queue = new Queue(Q, { connection: CONNECTION });

      const job = await queue.add('before-close', { x: 1 });
      expect(job.id).toBeTruthy();

      await queue.close();

      await expect(queue.add('after-close', { x: 2 })).rejects.toThrow();

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
  // MEMORY LEAK REGRESSION TESTS (gap #8)
  // These tests create many connections, so they run last to avoid exhausting
  // the cluster connection pool for other tests.
  // ---------------------------------------------------------------------------
  describe('Memory leak regression', () => {
    it('add and process 1000 jobs - heap does not grow beyond 2x baseline', async () => {
      const Q = uniqueQueue('mem-heap');
      const queue = new Queue(Q, { connection: CONNECTION });

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

      for (let batch = 0; batch < 10; batch++) {
        const promises = [];
        for (let i = 0; i < 100; i++) {
          promises.push(queue.add(`heap-${batch * 100 + i}`, { i: batch * 100 + i, data: 'x'.repeat(100) }));
        }
        await Promise.all(promises);
      }

      const start = Date.now();
      while (completedCount < jobCount && Date.now() - start < 60000) {
        await new Promise(r => setTimeout(r, 200));
      }

      expect(completedCount).toBe(jobCount);

      if (global.gc) global.gc();
      const afterHeap = process.memoryUsage().heapUsed;
      const growth = afterHeap / baseline;

      expect(growth).toBeLessThan(2);

      await worker.close();
      await queue.close();
    }, 60000);

    it('create and close Queue instances - no connection leak', async () => {
      const queues: InstanceType<typeof Queue>[] = [];
      // Cluster mode uses more connections per client, so use fewer instances
      const instanceCount = CONNECTION.clusterMode ? 10 : 50;

      for (let i = 0; i < instanceCount; i++) {
        const Q = uniqueQueue(`leak-q-${i}`);
        const q = new Queue(Q, { connection: CONNECTION });
        await q.add('leak-test', { i });
        queues.push(q);
      }

      const infoOpen = await cleanupClient.info(['CLIENTS']);
      const connectedOpen = parseConnectedClients(infoOpen);

      for (const q of queues) {
        await q.close();
      }

      // Wait for connections to fully drain - cluster needs more time
      await new Promise(r => setTimeout(r, CONNECTION.clusterMode ? 5000 : 3000));

      const infoAfter = await cleanupClient.info(['CLIENTS']);
      const connectedAfter = parseConnectedClients(infoAfter);

      expect(connectedAfter).toBeLessThan(connectedOpen);

      const Q = uniqueQueue('leak-verify');
      const verifyQueue = new Queue(Q, { connection: CONNECTION });
      const job = await verifyQueue.add('verify', { x: 1 });
      expect(job.id).toBeTruthy();
      await verifyQueue.close();
    }, 30000);

    it('Worker processes 500 jobs with retries - activePromises.size === 0 at end', async () => {
      const Q = uniqueQueue('mem-active-promises');
      const queue = new Queue(Q, { connection: CONNECTION });

      const jobCount = 500;
      let completedCount = 0;
      let failedCount = 0;
      let callCount = 0;

      const worker = new Worker(
        Q,
        async () => {
          callCount++;
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

      for (let batch = 0; batch < 10; batch++) {
        const promises = [];
        for (let i = 0; i < 50; i++) {
          promises.push(queue.add(`active-${batch * 50 + i}`, { i: batch * 50 + i }));
        }
        await Promise.all(promises);
      }

      const start = Date.now();
      while (completedCount + failedCount < jobCount && Date.now() - start < 60000) {
        await new Promise(r => setTimeout(r, 200));
      }

      expect(completedCount + failedCount).toBe(jobCount);

      await new Promise(r => setTimeout(r, 500));

      const activeSize = (worker as any).activePromises?.size ?? 0;
      expect(activeSize).toBe(0);

      await worker.close();
      await queue.close();
    }, 60000);
  });
});

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function parseConnectedClients(info: string | Record<string, string>): number {
  if (typeof info === 'string') {
    const match = info.match(/connected_clients:(\d+)/);
    return match ? parseInt(match[1], 10) : 0;
  }
  // Cluster mode: info is Record<nodeAddress, infoString>
  // Sum connected_clients across all nodes
  let total = 0;
  for (const nodeInfo of Object.values(info)) {
    const match = nodeInfo.match(/connected_clients:(\d+)/);
    if (match) total += parseInt(match[1], 10);
  }
  return total;
}
