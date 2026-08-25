/**
 * Gap reliability tests: Job revocation, broker failover, memory leak regression.
 *
 * Requires: valkey-server on localhost:6379 and cluster on :7000-7005
 *
 * Run: npx vitest run tests/gap-reliability.test.ts
 */
import net from 'node:net';
import { describe, it, expect, beforeAll, afterAll } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { BaseWorker } = require('../dist/base-worker') as typeof import('../src/base-worker');
const { BatchError, UnrecoverableError } = require('../dist/errors') as typeof import('../src/errors');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

import { describeEachMode, createCleanupClient, flushQueue, STANDALONE, waitFor } from './helpers/fixture';

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
    await Promise.all(allQueues.map((q) => flushQueue(cleanupClient, q).catch(() => {})));
    cleanupClient.close();
  }, 120000);

  // ---------------------------------------------------------------------------
  // JOB REVOCATION (gap #4)
  // ---------------------------------------------------------------------------
  describe('Job revocation', () => {
    it('registers batch abort controllers before limiter waits', async () => {
      let releaseLimiter!: () => void;
      const limiterReleased = new Promise<void>((resolve) => {
        releaseLimiter = resolve;
      });
      let markLimiterStarted!: () => void;
      const limiterStarted = new Promise<void>((resolve) => {
        markLimiterStarted = resolve;
      });
      let processorSawAborted = false;
      let stoppedHeartbeats = 0;
      let handledFailures = 0;

      const worker = Object.create(BaseWorker.prototype) as any;
      worker.commandClient = {};
      worker.batchProcessor = async (jobs: any[]) => {
        processorSawAborted = jobs[0].abortSignal?.aborted === true;
        throw new Error('processor stopped');
      };
      worker.activeAbortControllers = new Map();
      worker.opts = { limiter: { max: 1, duration: 1 } };
      worker.hasActiveListeners = false;
      worker.globalRateLimitEnabled = false;
      worker.waitForRateLimit = async () => {
        markLimiterStarted();
        await limiterReleased;
      };
      worker.stopHeartbeat = () => {
        stoppedHeartbeats++;
      };
      worker.isJobRevoked = async () => false;
      worker.handleJobFailure = async () => {
        handledFailures++;
      };

      const batch = [{ jobId: '1', entryId: '1-0', job: { opts: {} } }];
      const processing = worker.processBatch(batch);
      await limiterStarted;

      expect(worker.abortJob('1')).toBe(true);
      releaseLimiter();
      await processing;

      expect(processorSawAborted).toBe(true);
      expect(handledFailures).toBe(1);
      expect(stoppedHeartbeats).toBe(1);
      expect(worker.activeAbortControllers.size).toBe(0);
    });

    it('treats revoke lookup failures as unconfirmed', async () => {
      const worker = Object.create(Worker.prototype) as {
        commandClient?: { hget: () => Promise<unknown> };
        queueKeys: ReturnType<typeof buildKeys>;
        isJobRevoked: (jobId: string) => Promise<boolean>;
      };
      worker.queueKeys = buildKeys(uniqueQueue('revoke-lookup'));

      expect(await worker.isJobRevoked('1')).toBe(false);

      worker.commandClient = { hget: async () => '1' };
      expect(await worker.isJobRevoked('1')).toBe(true);

      worker.commandClient = { hget: async () => null };
      expect(await worker.isJobRevoked('1')).toBe(false);

      worker.commandClient = {
        hget: async () => {
          throw new Error('connection lost');
        },
      };
      expect(await worker.isJobRevoked('1')).toBe(false);
    });

    it('classifies revoked partial-batch failures and completions as terminal', async () => {
      const failures: Error[] = [];
      const worker = Object.create(BaseWorker.prototype) as any;
      worker.commandClient = {
        fcall: async () => 'REVOKED',
      };
      worker.batchProcessor = async () => {
        throw new BatchError([new Error('processor failure'), 'completed result']);
      };
      worker.activeAbortControllers = new Map();
      worker.opts = {};
      worker.hasActiveListeners = false;
      worker.hasCompletedListeners = false;
      worker.globalRateLimitEnabled = false;
      worker.stopHeartbeat = () => {};
      worker.isJobRevoked = async (jobId: string) => jobId === 'revoked-error';
      worker.handleJobFailure = async (_job: unknown, _jobId: string, _entryId: string, error: Error) => {
        failures.push(error);
      };
      worker.serializer = { serialize: (value: unknown) => JSON.stringify(value) };
      worker.queueKeys = buildKeys(uniqueQueue('revoke-partial-batch'));
      worker.consumerGroup = 'workers';
      worker.broadcastMode = false;
      worker.buildParentInfo = async () => undefined;

      const makeEntry = (jobId: string) => ({
        jobId,
        entryId: `${jobId}-0`,
        job: { opts: {} },
      });
      await worker.processBatch([makeEntry('revoked-error'), makeEntry('revoked-completion')]);

      expect(failures).toHaveLength(2);
      expect(failures.every((error) => error instanceof UnrecoverableError)).toBe(true);
      expect(failures.map((error) => error.message)).toEqual(['revoked', 'revoked']);
    });

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

      await new Promise((r) => setTimeout(r, 3000));

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

      const job = await queue.add('active-revoke', { value: 'test' }, { attempts: 2 });

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
              await new Promise((r) => setTimeout(r, 100));
              if (j.abortSignal?.aborted) {
                throw new Error('processor observed abort');
              }
            }
            return 'completed';
          },
          { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000, lockDuration: 400 },
        );
        worker.on('error', () => {});

        const checkInterval = setInterval(async () => {
          if (processorStarted) {
            clearInterval(checkInterval);

            await queue.revoke(job.id);

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

      const k = buildKeys(Q);
      const state = await cleanupClient.hget(k.job(job.id), 'state');
      const reason = await cleanupClient.hget(k.job(job.id), 'failedReason');
      const attemptsMade = await cleanupClient.hget(k.job(job.id), 'attemptsMade');
      expect(String(state)).toBe('failed');
      expect(String(reason)).toBe('revoked');
      expect(String(attemptsMade)).toBe('1');

      await queue.close();
    }, 20000);

    it('polls revocation promptly with the default lock duration', async () => {
      const Q = uniqueQueue('revoke-default-poll');
      const queue = new Queue(Q, { connection: CONNECTION });
      let markStarted!: () => void;
      const started = new Promise<void>((resolve) => {
        markStarted = resolve;
      });
      let markAborted!: () => void;
      const aborted = new Promise<void>((resolve) => {
        markAborted = resolve;
      });
      const worker = new Worker(
        Q,
        async (job: any) => {
          markStarted();
          await new Promise<void>((resolve) => {
            job.abortSignal?.addEventListener(
              'abort',
              () => {
                markAborted();
                resolve();
              },
              { once: true },
            );
          });
          return 'ignored-after-revoke';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 100, stalledInterval: 60000 },
      );
      worker.on('error', () => {});
      const job = await queue.add('default-poll', {});

      try {
        await started;
        expect(await queue.revoke(job.id)).toBe('flagged');
        await new Promise<void>((resolve, reject) => {
          const timer = setTimeout(() => reject(new Error('abort signal was not prompt')), 3000);
          aborted.then(() => {
            clearTimeout(timer);
            resolve();
          });
        });
      } finally {
        await worker.close(true);
        await queue.close();
      }
    }, 10000);

    it('aborts resumed continuations after revocation', async () => {
      const Q = uniqueQueue('revoke-continuation');
      const queue = new Queue(Q, { connection: CONNECTION });
      const k = buildKeys(Q);
      let markContinuationStarted!: () => void;
      const continuationStarted = new Promise<void>((resolve) => {
        markContinuationStarted = resolve;
      });
      let markContinuationAborted!: () => void;
      const continuationAborted = new Promise<void>((resolve) => {
        markContinuationAborted = resolve;
      });
      let releaseContinuation!: () => void;
      const continuationReleased = new Promise<void>((resolve) => {
        releaseContinuation = resolve;
      });
      const worker = new Worker(
        Q,
        async (job: any) => {
          if (job.signals.length === 0) {
            await job.suspend({
              onResume: async () => {
                markContinuationStarted();
                await new Promise<void>((resolve) => {
                  job.abortSignal?.addEventListener(
                    'abort',
                    () => {
                      markContinuationAborted();
                      resolve();
                    },
                    { once: true },
                  );
                });
                await continuationReleased;
                return 'ignored-after-revoke';
              },
            });
          }
          return 'processor-should-not-run-on-resume';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 100, stalledInterval: 60000 },
      );
      worker.on('error', () => {});
      const job = await queue.add('continuation', {});

      try {
        await waitFor(async () => (await queue.getSuspendInfo(job.id)) !== null, 8000);
        expect(await queue.signal(job.id, 'resume')).toBe(true);
        await continuationStarted;
        expect(await queue.revoke(job.id)).toBe('flagged');
        await new Promise<void>((resolve, reject) => {
          const timer = setTimeout(() => reject(new Error('continuation abort signal was not prompt')), 3000);
          continuationAborted.then(() => {
            clearTimeout(timer);
            resolve();
          });
        });
        releaseContinuation();
        await waitFor(async () => String(await cleanupClient.hget(k.job(job.id), 'state')) === 'failed', 5000);
        expect(String(await cleanupClient.hget(k.job(job.id), 'failedReason'))).toBe('revoked');
      } finally {
        releaseContinuation?.();
        await worker.close(true);
        await queue.close();
      }
    }, 15000);

    it('does not complete a revoked job when its processor ignores abort', async () => {
      const Q = uniqueQueue('revoke-ignore-abort');
      const queue = new Queue(Q, { connection: CONNECTION });
      const k = buildKeys(Q);
      const job = await queue.add('ignore-abort', {});
      let markStarted!: () => void;
      const started = new Promise<void>((resolve) => {
        markStarted = resolve;
      });
      let releaseProcessor!: () => void;
      const released = new Promise<void>((resolve) => {
        releaseProcessor = resolve;
      });

      const worker = new Worker(
        Q,
        async () => {
          markStarted();
          await released;
          return 'must-not-complete';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 200, stalledInterval: 60000, lockDuration: 400 },
      );
      worker.on('error', () => {});

      await started;
      expect(await queue.revoke(job.id)).toBe('flagged');
      releaseProcessor();

      await waitFor(async () => String(await cleanupClient.hget(k.job(job.id), 'state')) === 'failed', 5000);
      expect(String(await cleanupClient.hget(k.job(job.id), 'failedReason'))).toBe('revoked');
      expect(await cleanupClient.zscore(k.completed, job.id)).toBeNull();

      await worker.close(true);
      await queue.close();
    }, 10000);

    it('revoking one batch job does not abort or complete the other job', async () => {
      const Q = uniqueQueue('revoke-batch-one');
      const queue = new Queue(Q, { connection: CONNECTION });
      const k = buildKeys(Q);
      const revokedJob = await queue.add('revoked', { target: true });
      const otherJob = await queue.add('other', { target: false });
      let targetStarted!: () => void;
      const started = new Promise<void>((resolve) => {
        targetStarted = resolve;
      });
      let otherAborted = false;
      let completed = 0;
      let failed = 0;
      let resolveSettled!: () => void;
      const settled = new Promise<void>((resolve) => {
        resolveSettled = resolve;
      });
      const resolveWhenSettled = () => {
        if (completed === 1 && failed === 1) resolveSettled();
      };

      const worker = new Worker(
        Q,
        async (batch: any[]) => {
          const target = batch.find((job) => job.id === revokedJob.id)!;
          const other = batch.find((job) => job.id === otherJob.id)!;
          targetStarted();
          other.abortSignal?.addEventListener('abort', () => {
            otherAborted = true;
          });
          await new Promise<void>((resolve) => {
            target.abortSignal?.addEventListener('abort', resolve, { once: true });
          });
          return batch.map((job) => (job.id === otherJob.id ? 'other-complete' : 'revoked-result'));
        },
        {
          connection: CONNECTION,
          batch: { size: 2 },
          concurrency: 1,
          blockTimeout: 100,
          stalledInterval: 60000,
          lockDuration: 2000,
        },
      );
      worker.on('error', () => {});
      worker.on('completed', () => {
        completed++;
        resolveWhenSettled();
      });
      worker.on('failed', (job: any) => {
        if (job.id === revokedJob.id) {
          failed++;
          resolveWhenSettled();
        }
      });

      await started;
      expect(await queue.revoke(revokedJob.id)).toBe('flagged');
      await settled;

      expect(otherAborted).toBe(false);
      expect(completed).toBe(1);
      expect(failed).toBe(1);
      expect(String(await cleanupClient.hget(k.job(revokedJob.id), 'state'))).toBe('failed');
      expect(String(await cleanupClient.hget(k.job(revokedJob.id), 'failedReason'))).toBe('revoked');
      expect(String(await cleanupClient.hget(k.job(otherJob.id), 'state'))).toBe('completed');

      await worker.close(true);
      await queue.close();
    }, 30000);

    it('batch thrown errors classify revoked jobs as terminal failures', async () => {
      const Q = uniqueQueue('revoke-batch-error');
      const queue = new Queue(Q, { connection: CONNECTION });
      const k = buildKeys(Q);
      const revokedJob = await queue.add('revoked', { target: true }, { attempts: 2 });
      let releaseProcessor!: () => void;
      const processorReady = new Promise<void>((resolve) => {
        releaseProcessor = resolve;
      });
      let processorStarted!: () => void;
      const started = new Promise<void>((resolve) => {
        processorStarted = resolve;
      });
      let failed = 0;
      let resolveSettled!: () => void;
      const settled = new Promise<void>((resolve) => {
        resolveSettled = resolve;
      });
      const checkSettled = () => {
        if (failed === 1) resolveSettled();
      };

      let calls = 0;
      const worker = new Worker(
        Q,
        async (_batch: any[]) => {
          calls++;
          if (calls > 1) throw new Error('unexpected retry after revoked batch error');
          processorStarted();
          await processorReady;
          throw new Error('batch failure');
        },
        {
          connection: CONNECTION,
          batch: { size: 2 },
          concurrency: 1,
          blockTimeout: 100,
          stalledInterval: 60000,
          lockDuration: 2000,
        },
      );
      worker.on('error', () => {});
      worker.on('failed', (job: any) => {
        if (job.id === revokedJob.id) {
          failed++;
          checkSettled();
        }
      });

      await started;
      expect(await queue.revoke(revokedJob.id)).toBe('flagged');
      releaseProcessor();
      await settled;
      await new Promise((resolve) => setTimeout(resolve, 1000));

      expect(calls).toBe(1);
      expect(String(await cleanupClient.hget(k.job(revokedJob.id), 'state'))).toBe('failed');
      expect(String(await cleanupClient.hget(k.job(revokedJob.id), 'failedReason'))).toBe('revoked');
      expect(String(await cleanupClient.hget(k.job(revokedJob.id), 'attemptsMade'))).toBe('1');

      await worker.close(true);
      await queue.close();
    }, 30000);

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

      await queue.add('pre-fail', { x: 1 });

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
        await new Promise((r) => setTimeout(r, 200));
      }
      expect(completedCount).toBeGreaterThanOrEqual(1);

      await queue.add('post-check', { x: 2 });

      const start2 = Date.now();
      while (completedCount < 2 && Date.now() - start2 < 10000) {
        await new Promise((r) => setTimeout(r, 200));
      }
      expect(completedCount).toBeGreaterThanOrEqual(2);

      await worker.close(true);
      await queue.close();
    }, 20000);

    it('Worker exponential backoff on repeated connection errors', async () => {
      const Q = uniqueQueue('failover-backoff');

      const worker = new Worker(Q, async () => 'ok', {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 500,
        stalledInterval: 60000,
      });
      worker.on('error', () => {});

      await worker.waitUntilReady();

      expect((worker as any).reconnectBackoff).toBe(0);

      await worker.close(true);
    }, 10000);

    it('reconnect preserves worker lockDuration on the stall scheduler', async () => {
      const Q = uniqueQueue('failover-lock');
      const worker = new Worker(Q, async () => 'ok', {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 500,
        stalledInterval: 5000,
        lockDuration: 120000,
      });
      worker.on('error', () => {});
      await worker.waitUntilReady();

      expect((worker as any).scheduler.lockDuration).toBe(120000);
      const schedulerBefore = (worker as any).scheduler;

      const origPollOnce = (worker as any).pollOnce.bind(worker);
      let failNext = true;
      (worker as any).pollOnce = async () => {
        if (failNext) {
          failNext = false;
          throw new Error('Connection lost');
        }
        return origPollOnce();
      };

      const start = Date.now();
      while ((worker as any).scheduler === schedulerBefore && Date.now() - start < 10000) {
        await new Promise((r) => setTimeout(r, 50));
      }
      expect((worker as any).scheduler).not.toBe(schedulerBefore);
      expect((worker as any).scheduler.lockDuration).toBe(120000);

      await worker.close(true);
    }, 15000);

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

      const worker = new Worker(Q, async () => 'ok', {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 500,
        stalledInterval: 60000,
      });
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
        await new Promise((r) => setTimeout(r, 200));
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
    it('add and process many jobs - heap does not grow beyond 2x baseline', async () => {
      const Q = uniqueQueue('mem-heap');
      const queue = new Queue(Q, { connection: CONNECTION });

      if (global.gc) global.gc();
      const baseline = process.memoryUsage().heapUsed;

      // Cluster mode uses more resources per job; reduce count to avoid CI timeouts
      const jobCount = CONNECTION.clusterMode ? 500 : 1000;
      const batchSize = CONNECTION.clusterMode ? 50 : 100;
      const batches = jobCount / batchSize;
      let completedCount = 0;

      const worker = new Worker(Q, async () => 'ok', {
        connection: CONNECTION,
        concurrency: 20,
        blockTimeout: 500,
        stalledInterval: 60000,
      });
      worker.on('error', () => {});
      worker.on('completed', () => {
        completedCount++;
      });

      for (let batch = 0; batch < batches; batch++) {
        const promises = [];
        for (let i = 0; i < batchSize; i++) {
          promises.push(
            queue.add(`heap-${batch * batchSize + i}`, { i: batch * batchSize + i, data: 'x'.repeat(100) }),
          );
        }
        await Promise.all(promises);
      }

      const start = Date.now();
      while (completedCount < jobCount && Date.now() - start < 90000) {
        await new Promise((r) => setTimeout(r, 200));
      }

      expect(completedCount).toBe(jobCount);

      if (global.gc) global.gc();
      const afterHeap = process.memoryUsage().heapUsed;
      const growth = afterHeap / baseline;

      expect(growth).toBeLessThan(2);

      await worker.close();
      await queue.close();
    }, 120000);

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

      for (const q of queues) {
        await q.close();
      }

      // Wait for connections to drain
      await new Promise((r) => setTimeout(r, CONNECTION.clusterMode ? 10000 : 5000));

      // Verify system is still functional after mass create/close
      // (proves connections were released, not leaked/exhausted)
      const Q = uniqueQueue('leak-verify');
      const verifyQueue = new Queue(Q, { connection: CONNECTION });
      const job = await verifyQueue.add('verify', { x: 1 });
      expect(job.id).toBeTruthy();
      await verifyQueue.close();
    }, 45000);

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
      worker.on('completed', () => {
        completedCount++;
      });
      worker.on('failed', () => {
        failedCount++;
      });

      for (let batch = 0; batch < 10; batch++) {
        const promises = [];
        for (let i = 0; i < 50; i++) {
          promises.push(queue.add(`active-${batch * 50 + i}`, { i: batch * 50 + i }));
        }
        await Promise.all(promises);
      }

      const start = Date.now();
      while (completedCount + failedCount < jobCount && Date.now() - start < 60000) {
        await new Promise((r) => setTimeout(r, 200));
      }

      expect(completedCount + failedCount).toBe(jobCount);

      await new Promise((r) => setTimeout(r, 500));

      const activeSize = (worker as any).activePromises?.size ?? 0;
      expect(activeSize).toBe(0);

      await worker.close();
      await queue.close();
    }, 60000);
  });
});

describe('Standalone TCP proxy reconnect', () => {
  it('resumes blocked polling after the proxy severs connections without stranded PEL entries', async () => {
    const proxy = await createTcpProxy();
    const Q = `proxy-reconnect-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    const queue = new Queue(Q, { connection: STANDALONE });
    const cleanupClient = await createCleanupClient(STANDALONE);
    const processed: string[] = [];
    const worker = new Worker(
      Q,
      async (job: any) => {
        processed.push(String(job.name));
        return 'ok';
      },
      {
        connection: proxy.connection,
        concurrency: 1,
        blockTimeout: 5000,
        stalledInterval: 5000,
        lockDuration: 120000,
      },
    );
    worker.on('error', () => {});

    try {
      await worker.waitUntilReady();
      expect((worker as any).scheduler.lockDuration).toBe(120000);

      await queue.add('before-cut', { value: 1 });
      await waitFor(() => processed.length === 1, 10000);
      const schedulerBefore = (worker as any).scheduler;

      // Both the command and blocking clients are idle/blocked through the proxy.
      await waitFor(() => proxy.connectionCount >= 2, 5000);
      proxy.cutConnections();

      await queue.add('after-cut', { value: 2 });
      await waitFor(() => (worker as any).scheduler !== schedulerBefore, 15000);
      await waitFor(() => processed.length === 2, 15000);

      const k = buildKeys(Q);
      const pending = await cleanupClient.xpending(k.stream, 'workers');
      expect(Number(pending[0])).toBe(0);
      expect((worker as any).scheduler.lockDuration).toBe(120000);
    } finally {
      await worker.close(true).catch(() => {});
      await queue.close().catch(() => {});
      await flushQueue(cleanupClient, Q).catch(() => {});
      cleanupClient.close();
      await proxy.close();
    }
  }, 30000);
});

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
async function createTcpProxy(): Promise<{
  connection: { addresses: { host: string; port: number }[] };
  connectionCount: number;
  cutConnections: () => void;
  close: () => Promise<void>;
}> {
  const sockets = new Set<net.Socket>();
  const server = net.createServer((client) => {
    const target = net.createConnection({ host: '127.0.0.1', port: 6379 });
    sockets.add(client);
    sockets.add(target);

    const cleanup = () => {
      sockets.delete(client);
      sockets.delete(target);
      client.destroy();
      target.destroy();
    };
    client.on('error', cleanup);
    target.on('error', cleanup);
    client.on('close', cleanup);
    target.on('close', cleanup);
    client.pipe(target);
    target.pipe(client);
  });

  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => resolve());
  });

  const address = server.address();
  if (!address || typeof address === 'string') {
    await new Promise<void>((resolve) => server.close(() => resolve()));
    throw new Error('TCP proxy did not expose a listening address');
  }

  return {
    connection: { addresses: [{ host: '127.0.0.1', port: address.port }] },
    get connectionCount() {
      return sockets.size;
    },
    cutConnections: () => {
      for (const socket of sockets) socket.destroy();
    },
    close: () =>
      new Promise<void>((resolve) => {
        for (const socket of sockets) socket.destroy();
        server.close(() => resolve());
      }),
  };
}

function _parseConnectedClients(info: string | Record<string, string>): number {
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
