/**
 * Tests for job.discard() and UnrecoverableError.
 * Requires: valkey-server running on localhost:6379 and cluster on :7000-7005
 *
 * Run: npx vitest run tests/discard.test.ts
 */
import { it, expect, describe, beforeAll, afterAll } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { UnrecoverableError } = require('../dist/errors') as typeof import('../src/errors');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
const { promote } = require('../dist/functions/index') as typeof import('../src/functions/index');

import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

// ---- Integration tests (real Valkey) ----

describeEachMode('job.discard() and UnrecoverableError', (CONNECTION) => {
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    cleanupClient.close();
  });

  it('job.discard() skips retry and goes straight to failed', async () => {
    const Q = 'test-discard-skip-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });
    let attemptCount = 0;

    const job = await queue.add('task', { v: 1 }, { attempts: 5, backoff: { type: 'fixed', delay: 100 } });

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);

      const worker = new Worker(
        Q,
        async (j: any) => {
          attemptCount++;
          j.discard();
          throw new Error('discarded-fail');
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
      );

      worker.on('failed', async (j: any, err: Error) => {
        clearTimeout(timeout);
        try {
          expect(attemptCount).toBe(1);
          expect(err.message).toBe('discarded-fail');
          await worker.close(true);
          resolve();
        } catch (e) {
          await worker.close(true);
          reject(e);
        }
      });

      worker.on('error', () => {});
    });

    await done;
    await flushQueue(cleanupClient, Q);
    await queue.close();
  });

  it('UnrecoverableError skips retry and goes straight to failed', async () => {
    const Q = 'test-unrecoverable-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });
    let attemptCount = 0;

    await queue.add('task', { v: 1 }, { attempts: 5, backoff: { type: 'fixed', delay: 100 } });

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);

      const worker = new Worker(
        Q,
        async () => {
          attemptCount++;
          throw new UnrecoverableError('fatal-error');
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
      );

      worker.on('failed', async (_j: any, err: Error) => {
        clearTimeout(timeout);
        try {
          expect(attemptCount).toBe(1);
          expect(err.message).toBe('fatal-error');
          await worker.close(true);
          resolve();
        } catch (e) {
          await worker.close(true);
          reject(e);
        }
      });

      worker.on('error', () => {});
    });

    await done;
    await flushQueue(cleanupClient, Q);
    await queue.close();
  });

  it('normal errors still retry as expected (regression)', async () => {
    const Q = 'test-discard-regression-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });
    let attemptCount = 0;

    await queue.add('task', { v: 1 }, { attempts: 3, backoff: { type: 'fixed', delay: 100 } });

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 20000);

      const worker = new Worker(
        Q,
        async () => {
          attemptCount++;
          if (attemptCount < 3) throw new Error('transient');
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
      );

      worker.on('failed', () => {
        // Promote delayed jobs so retries happen quickly
        setTimeout(async () => {
          try {
            await promote(cleanupClient, buildKeys(Q), Date.now());
          } catch {}
        }, 200);
      });

      worker.on('completed', async () => {
        clearTimeout(timeout);
        try {
          expect(attemptCount).toBe(3);
          await worker.close(true);
          resolve();
        } catch (e) {
          await worker.close(true);
          reject(e);
        }
      });

      worker.on('error', () => {});
    });

    await done;
    await flushQueue(cleanupClient, Q);
    await queue.close();
  });
});

// ---- Testing mode (no Valkey needed) ----

describe('discard in testing mode', () => {
  // Use require to load from dist like other tests
  const { TestQueue, TestWorker } = require('../dist/testing') as typeof import('../src/testing');
  const { UnrecoverableError: UE } = require('../dist/errors') as typeof import('../src/errors');

  it('job.discard() prevents retry in TestWorker', async () => {
    const queue = new TestQueue('test-discard-mem');
    let attemptCount = 0;

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 5000);

      const worker = new TestWorker(queue, async (job: any) => {
        attemptCount++;
        job.discard();
        throw new Error('nope');
      });

      worker.on('failed', async () => {
        clearTimeout(timeout);
        try {
          expect(attemptCount).toBe(1);
          await worker.close();
          resolve();
        } catch (e) {
          await worker.close();
          reject(e);
        }
      });
    });

    await queue.add('task', { v: 1 }, { attempts: 5 });
    await done;
    await queue.close();
  });

  it('UnrecoverableError prevents retry in TestWorker', async () => {
    const queue = new TestQueue('test-unrecoverable-mem');
    let attemptCount = 0;

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 5000);

      const worker = new TestWorker(queue, async () => {
        attemptCount++;
        throw new UE('fatal');
      });

      worker.on('failed', async () => {
        clearTimeout(timeout);
        try {
          expect(attemptCount).toBe(1);
          await worker.close();
          resolve();
        } catch (e) {
          await worker.close();
          reject(e);
        }
      });
    });

    await queue.add('task', { v: 1 }, { attempts: 5 });
    await done;
    await queue.close();
  });
});
