/**
 * Integration tests for Queue.retryJobs() - bulk retry failed jobs.
 * Requires: valkey-server running on localhost:6379 and cluster on :7000-7005
 *
 * Run: npx vitest run tests/retry-jobs.test.ts
 */
import { it, expect, beforeAll, afterAll } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
const { promote } = require('../dist/functions/index') as typeof import('../src/functions/index');

import { describeEachMode, createCleanupClient, flushQueue, waitFor } from './helpers/fixture';

describeEachMode('Queue.retryJobs()', (CONNECTION) => {
  const Q = 'test-retryjobs-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    if (!cleanupClient) return;
    const suffixes = ['', '-count', '-prio', '-lifecycle', '-empty', '-over'];
    for (const s of suffixes) {
      await flushQueue(cleanupClient, Q + s);
    }
    cleanupClient.close();
  });

  /**
   * Helper: add N jobs, process them all with a failing worker, wait until all fail.
   */
  async function addAndFail(queueName: string, count: number, opts?: { priority?: number }): Promise<void> {
    const q = new Queue(queueName, { connection: CONNECTION });
    for (let i = 0; i < count; i++) {
      await q.add('task', { i }, opts);
    }

    const failedIds = new Set<string>();
    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout waiting for failures')), 15000);
      const worker = new Worker(
        queueName,
        async () => {
          throw new Error('forced failure');
        },
        { connection: CONNECTION, concurrency: 5, blockTimeout: 1000 },
      );
      worker.on('error', () => {});
      worker.on('failed', async (job: any) => {
        failedIds.add(job.id);
        if (failedIds.size >= count) {
          clearTimeout(timeout);
          // Use non-force close to ensure all operations complete
          await worker.close(false);
          resolve();
        }
      });
    });
    await done;
    await q.close();
    // Ensure all async Valkey operations complete before returning
    await new Promise((r) => setTimeout(r, 50));
  }

  it('retries all failed jobs', async () => {
    const qName = Q;
    await addAndFail(qName, 3);

    const queue = new Queue(qName, { connection: CONNECTION });
    const before = await queue.getJobCounts();
    expect(before.failed).toBe(3);
    expect(before.waiting).toBe(0);

    const retried = await queue.retryJobs();
    expect(retried).toBe(3);

    const after = await queue.getJobCounts();
    expect(after.failed).toBe(0);
    expect(after.delayed).toBe(3);

    // Verify job hashes were reset
    const k = buildKeys(qName);
    for (let id = 1; id <= 3; id++) {
      const state = String(await cleanupClient.hget(k.job(String(id)), 'state'));
      expect(state).toBe('delayed');
      const attempts = String(await cleanupClient.hget(k.job(String(id)), 'attemptsMade'));
      expect(attempts).toBe('0');
      const failedReason = String(await cleanupClient.hget(k.job(String(id)), 'failedReason'));
      expect(failedReason).toBe('');
    }

    await queue.close();
  }, 20000);

  it('respects count limit', async () => {
    const qName = Q + '-count';
    await addAndFail(qName, 5);

    const queue = new Queue(qName, { connection: CONNECTION });
    const before = await queue.getJobCounts();
    expect(before.failed).toBe(5);

    const retried = await queue.retryJobs({ count: 2 });
    expect(retried).toBe(2);

    const after = await queue.getJobCounts();
    expect(after.failed).toBe(3);
    expect(after.delayed).toBe(2);

    await queue.close();
  }, 20000);

  it('routes prioritized jobs to scheduled ZSet', async () => {
    const qName = Q + '-prio';
    await addAndFail(qName, 2, { priority: 3 });

    const queue = new Queue(qName, { connection: CONNECTION });
    const before = await queue.getJobCounts();
    expect(before.failed).toBe(2);

    const retried = await queue.retryJobs();
    expect(retried).toBe(2);

    const after = await queue.getJobCounts();
    expect(after.failed).toBe(0);
    // Priority > 0 goes to scheduled (delayed count)
    expect(after.delayed).toBe(2);

    // Verify state is 'delayed' on the hash
    const k = buildKeys(qName);
    for (let id = 1; id <= 2; id++) {
      const state = String(await cleanupClient.hget(k.job(String(id)), 'state'));
      expect(state).toBe('delayed');
    }

    await queue.close();
  }, 20000);

  it('retried job lands in scheduled ZSet and gets promoted', async () => {
    const qName = Q + '-lifecycle';
    await addAndFail(qName, 1);

    const queue = new Queue(qName, { connection: CONNECTION });
    const retried = await queue.retryJobs();
    expect(retried).toBe(1);

    // Verify the job is in the scheduled ZSet
    const after = await queue.getJobCounts();
    expect(after.failed).toBe(0);
    expect(after.delayed).toBe(1);

    // Explicitly promote the retried job from scheduled ZSet to stream
    const k = buildKeys(qName);
    const promoted = await promote(cleanupClient, k, Date.now());
    expect(promoted).toBe(1);

    // After promotion, the job should be in the stream (waiting state)
    const postPromote = await queue.getJobCounts();
    expect(postPromote.delayed).toBe(0);
    expect(postPromote.waiting).toBe(1);

    const state = String(await cleanupClient.hget(k.job('1'), 'state'));
    expect(state).toBe('waiting');

    await queue.close();
  }, 20000);

  it('count greater than total failed retries all available', async () => {
    const qName = Q + '-over';
    await addAndFail(qName, 3);

    const queue = new Queue(qName, { connection: CONNECTION });
    const retried = await queue.retryJobs({ count: 100 });
    expect(retried).toBe(3);

    const after = await queue.getJobCounts();
    expect(after.failed).toBe(0);
    expect(after.delayed).toBe(3);

    await queue.close();
  }, 20000);

  it('returns 0 when no failed jobs', async () => {
    const qName = Q + '-empty';
    const queue = new Queue(qName, { connection: CONNECTION });

    const retried = await queue.retryJobs();
    expect(retried).toBe(0);

    await queue.close();
  }, 10000);

  it('throws on invalid count', async () => {
    const qName = Q + '-empty';
    const queue = new Queue(qName, { connection: CONNECTION });

    await expect(queue.retryJobs({ count: -1 })).rejects.toThrow('count must be a non-negative integer');
    await expect(queue.retryJobs({ count: 1.5 })).rejects.toThrow('count must be a non-negative integer');

    await queue.close();
  }, 10000);
});
