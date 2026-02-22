/**
 * Tests for Queue.drain() - remove waiting/delayed jobs without touching active ones.
 * Requires: valkey-server running on localhost:6379 and cluster on :7000-7005
 */
import { it, expect, describe, beforeAll, afterAll } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { TestQueue, TestWorker } = require('../dist/testing') as typeof import('../src/testing');

import { describeEachMode, createCleanupClient, flushQueue, waitFor } from './helpers/fixture';

// ---- Integration tests (standalone + cluster) ----

describeEachMode('Queue.drain()', (CONNECTION) => {
  const Q = 'test-drain-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    if (!cleanupClient) return;
    const suffixes = ['-wait', '-active', '-delayed', '-both', '-empty', '-hash'];
    for (const s of suffixes) {
      await flushQueue(cleanupClient, Q + s);
    }
    cleanupClient.close();
  });

  it('removes waiting jobs', async () => {
    const queue = new Queue(Q + '-wait', { connection: CONNECTION });

    await queue.add('job-1', { v: 1 });
    await queue.add('job-2', { v: 2 });
    await queue.add('job-3', { v: 3 });

    const before = await queue.getJobCounts();
    expect(before.waiting).toBe(3);

    await queue.drain();

    const after = await queue.getJobCounts();
    expect(after.waiting).toBe(0);

    await queue.close();
  }, 15000);

  it('does not remove active jobs', async () => {
    const queue = new Queue(Q + '-active', { connection: CONNECTION });
    let releaseProcessor: (() => void) | undefined;
    const gate = new Promise<void>((resolve) => {
      releaseProcessor = resolve;
    });

    const worker = new Worker(
      Q + '-active',
      async () => {
        await gate;
        return 'done';
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
    );
    worker.on('error', () => {});

    await new Promise((r) => setTimeout(r, 500));

    // Add 3 jobs - one will become active, two will be waiting
    await queue.add('active-job', { v: 1 });
    await queue.add('waiting-job-1', { v: 2 });
    await queue.add('waiting-job-2', { v: 3 });

    // Wait for one job to become active
    await waitFor(async () => {
      const counts = await queue.getJobCounts();
      return counts.active >= 1;
    }, 10000);

    await queue.drain();

    const counts = await queue.getJobCounts();
    expect(counts.active).toBe(1);
    expect(counts.waiting).toBe(0);

    // Release the active job so worker can shut down cleanly
    releaseProcessor!();
    await waitFor(async () => {
      const c = await queue.getJobCounts();
      return c.active === 0;
    }, 10000);

    await worker.close(true);
    await queue.close();
  }, 20000);

  it('does not remove delayed jobs by default', async () => {
    const queue = new Queue(Q + '-delayed', { connection: CONNECTION });

    await queue.add('delayed-1', { v: 1 }, { delay: 60000 });
    await queue.add('delayed-2', { v: 2 }, { delay: 60000 });

    const before = await queue.getJobCounts();
    expect(before.delayed).toBe(2);

    await queue.drain();

    const after = await queue.getJobCounts();
    expect(after.delayed).toBe(2);

    await queue.close();
  }, 15000);

  it('removes delayed jobs when delayed=true', async () => {
    const queue = new Queue(Q + '-both', { connection: CONNECTION });

    await queue.add('waiting-1', { v: 1 });
    await queue.add('delayed-1', { v: 2 }, { delay: 60000 });

    const before = await queue.getJobCounts();
    expect(before.waiting).toBe(1);
    expect(before.delayed).toBe(1);

    await queue.drain(true);

    const after = await queue.getJobCounts();
    expect(after.waiting).toBe(0);
    expect(after.delayed).toBe(0);

    await queue.close();
  }, 15000);

  it('is a no-op on empty queue', async () => {
    const queue = new Queue(Q + '-empty', { connection: CONNECTION });
    await queue.drain();
    const counts = await queue.getJobCounts();
    expect(counts.waiting).toBe(0);
    expect(counts.delayed).toBe(0);
    await queue.close();
  }, 10000);

  it('deletes job hashes after drain', async () => {
    const queue = new Queue(Q + '-hash', { connection: CONNECTION });

    const job = await queue.add('to-drain', { v: 1 });
    expect(job).not.toBeNull();

    await queue.drain();

    const fetched = await queue.getJob(job!.id);
    expect(fetched).toBeNull();

    await queue.close();
  }, 15000);
});

// ---- TestQueue (in-memory) tests ----

describe('TestQueue.drain()', () => {
  it('removes waiting jobs', async () => {
    const queue = new TestQueue('test-drain-mem');

    await queue.add('job-1', { v: 1 });
    await queue.add('job-2', { v: 2 });

    const before = await queue.getJobCounts();
    expect(before.waiting).toBe(2);

    await queue.drain();

    const after = await queue.getJobCounts();
    expect(after.waiting).toBe(0);

    await queue.close();
  });

  it('does not remove active jobs', async () => {
    const queue = new TestQueue('test-drain-mem-active');
    let releaseProcessor: (() => void) | undefined;
    const gate = new Promise<void>((resolve) => {
      releaseProcessor = resolve;
    });

    const worker = new TestWorker(queue, async () => {
      await gate;
      return 'done';
    });

    await queue.add('active-job', { v: 1 });
    await queue.add('waiting-job', { v: 2 });

    // Wait for one to become active
    await waitFor(async () => {
      const counts = await queue.getJobCounts();
      return counts.active >= 1;
    }, 5000);

    await queue.drain();

    const counts = await queue.getJobCounts();
    expect(counts.active).toBe(1);
    expect(counts.waiting).toBe(0);

    releaseProcessor!();
    await waitFor(async () => {
      const c = await queue.getJobCounts();
      return c.active === 0;
    }, 5000);

    await worker.close();
    await queue.close();
  });

  it('removes delayed jobs when delayed=true', async () => {
    const queue = new TestQueue('test-drain-mem-delayed');

    await queue.add('waiting', { v: 1 });
    await queue.add('delayed', { v: 2 }, { delay: 60000 });

    await queue.drain(true);

    const counts = await queue.getJobCounts();
    expect(counts.waiting).toBe(0);
    expect(counts.delayed).toBe(0);

    await queue.close();
  });

  it('is a no-op on empty queue', async () => {
    const queue = new TestQueue('test-drain-mem-empty');
    await queue.drain();
    const counts = await queue.getJobCounts();
    expect(counts.waiting).toBe(0);
    await queue.close();
  });
});
