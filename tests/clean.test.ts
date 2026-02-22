/**
 * Tests for Queue.clean() - bulk removal of old completed/failed jobs by age.
 * Requires: valkey-server running on localhost:6379 and cluster on :7000-7005
 */
import { it, expect, describe, beforeAll, afterAll } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { TestQueue, TestWorker } = require('../dist/testing') as typeof import('../src/testing');

import { describeEachMode, createCleanupClient, flushQueue, waitFor } from './helpers/fixture';

// ---- Integration tests (standalone + cluster) ----

describeEachMode('Queue.clean()', (CONNECTION) => {
  const Q = 'test-clean-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('cleans completed jobs older than grace period', async () => {
    const queue = new Queue(Q + '-comp', { connection: CONNECTION });

    // Add and process jobs
    const worker = new Worker(Q + '-comp', async () => 'done', {
      connection: CONNECTION,
      concurrency: 5,
      blockTimeout: 1000,
    });
    worker.on('error', () => {});

    await new Promise((r) => setTimeout(r, 500));

    await queue.add('old-job-1', { v: 1 });
    await queue.add('old-job-2', { v: 2 });

    // Wait for both to complete
    await waitFor(async () => {
      const counts = await queue.getJobCounts();
      return counts.completed >= 2;
    }, 10000);

    // Small delay so jobs are "old" relative to a 0ms grace
    await new Promise((r) => setTimeout(r, 50));

    const removed = await queue.clean(0, 100, 'completed');
    expect(removed).toHaveLength(2);

    const counts = await queue.getJobCounts();
    expect(counts.completed).toBe(0);

    await worker.close(true);
    await queue.close();
    await flushQueue(cleanupClient, Q + '-comp');
  }, 15000);

  it('cleans failed jobs older than grace period', async () => {
    const queue = new Queue(Q + '-fail', { connection: CONNECTION });

    const worker = new Worker(
      Q + '-fail',
      async () => {
        throw new Error('boom');
      },
      { connection: CONNECTION, concurrency: 5, blockTimeout: 1000 },
    );
    worker.on('error', () => {});

    await new Promise((r) => setTimeout(r, 500));

    await queue.add('fail-1', { v: 1 });
    await queue.add('fail-2', { v: 2 });

    await waitFor(async () => {
      const counts = await queue.getJobCounts();
      return counts.failed >= 2;
    }, 10000);

    await new Promise((r) => setTimeout(r, 50));

    const removed = await queue.clean(0, 100, 'failed');
    expect(removed).toHaveLength(2);

    const counts = await queue.getJobCounts();
    expect(counts.failed).toBe(0);

    await worker.close(true);
    await queue.close();
    await flushQueue(cleanupClient, Q + '-fail');
  }, 15000);

  it('respects limit parameter', async () => {
    const queue = new Queue(Q + '-limit', { connection: CONNECTION });

    const worker = new Worker(Q + '-limit', async () => 'ok', {
      connection: CONNECTION,
      concurrency: 5,
      blockTimeout: 1000,
    });
    worker.on('error', () => {});

    await new Promise((r) => setTimeout(r, 500));

    for (let i = 0; i < 10; i++) {
      await queue.add('job', { i });
    }

    await waitFor(async () => {
      const counts = await queue.getJobCounts();
      return counts.completed >= 10;
    }, 15000);

    await new Promise((r) => setTimeout(r, 50));

    const removed = await queue.clean(0, 3, 'completed');
    expect(removed).toHaveLength(3);

    const counts = await queue.getJobCounts();
    expect(counts.completed).toBe(7);

    await worker.close(true);
    await queue.close();
    await flushQueue(cleanupClient, Q + '-limit');
  }, 20000);

  it('does not remove recent jobs within grace period', async () => {
    const queue = new Queue(Q + '-grace', { connection: CONNECTION });

    const worker = new Worker(Q + '-grace', async () => 'ok', {
      connection: CONNECTION,
      concurrency: 5,
      blockTimeout: 1000,
    });
    worker.on('error', () => {});

    await new Promise((r) => setTimeout(r, 500));

    await queue.add('job', { v: 1 });

    await waitFor(async () => {
      const counts = await queue.getJobCounts();
      return counts.completed >= 1;
    }, 10000);

    // Use a very large grace period so the job is "recent"
    const removed = await queue.clean(60_000, 100, 'completed');
    expect(removed).toHaveLength(0);

    const counts = await queue.getJobCounts();
    expect(counts.completed).toBe(1);

    await worker.close(true);
    await queue.close();
    await flushQueue(cleanupClient, Q + '-grace');
  }, 15000);

  it('returns empty array when no matching jobs', async () => {
    const queue = new Queue(Q + '-empty', { connection: CONNECTION });
    const removed = await queue.clean(0, 100, 'completed');
    expect(removed).toEqual([]);
    await queue.close();
  }, 10000);
});

// ---- TestQueue (in-memory) tests ----

describe('TestQueue.clean()', () => {
  it('cleans completed jobs older than grace period', async () => {
    const queue = new TestQueue('test-clean-mem');
    const worker = new TestWorker(queue, async () => 'done');

    await queue.add('job-1', { v: 1 });
    await queue.add('job-2', { v: 2 });

    await waitFor(async () => {
      const counts = await queue.getJobCounts();
      return counts.completed >= 2;
    }, 5000);

    await new Promise((r) => setTimeout(r, 50));

    const removed = await queue.clean(0, 100, 'completed');
    expect(removed).toHaveLength(2);

    const counts = await queue.getJobCounts();
    expect(counts.completed).toBe(0);

    await worker.close();
    await queue.close();
  });

  it('cleans failed jobs older than grace period', async () => {
    const queue = new TestQueue('test-clean-mem-fail');
    const worker = new TestWorker(queue, async () => {
      throw new Error('boom');
    });

    await queue.add('job-1', { v: 1 });
    await queue.add('job-2', { v: 2 });

    await waitFor(async () => {
      const counts = await queue.getJobCounts();
      return counts.failed >= 2;
    }, 5000);

    await new Promise((r) => setTimeout(r, 50));

    const removed = await queue.clean(0, 100, 'failed');
    expect(removed).toHaveLength(2);

    const counts = await queue.getJobCounts();
    expect(counts.failed).toBe(0);

    await worker.close();
    await queue.close();
  });

  it('respects limit', async () => {
    const queue = new TestQueue('test-clean-mem-limit');
    const worker = new TestWorker(queue, async () => 'ok');

    for (let i = 0; i < 10; i++) {
      await queue.add('job', { i });
    }

    await waitFor(async () => {
      const counts = await queue.getJobCounts();
      return counts.completed >= 10;
    }, 5000);

    await new Promise((r) => setTimeout(r, 50));

    const removed = await queue.clean(0, 3, 'completed');
    expect(removed).toHaveLength(3);

    const counts = await queue.getJobCounts();
    expect(counts.completed).toBe(7);

    await worker.close();
    await queue.close();
  });

  it('does not remove recent jobs within grace period', async () => {
    const queue = new TestQueue('test-clean-mem-grace');
    const worker = new TestWorker(queue, async () => 'ok');

    await queue.add('job', { v: 1 });

    await waitFor(async () => {
      const counts = await queue.getJobCounts();
      return counts.completed >= 1;
    }, 5000);

    const removed = await queue.clean(60_000, 100, 'completed');
    expect(removed).toHaveLength(0);

    await worker.close();
    await queue.close();
  });

  it('returns empty array when no matching jobs', async () => {
    const queue = new TestQueue('test-clean-mem-empty');
    const removed = await queue.clean(0, 100, 'completed');
    expect(removed).toEqual([]);
    await queue.close();
  });
});
