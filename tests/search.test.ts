/**
 * Integration tests for Queue.searchJobs.
 * Runs against both standalone (:6379) and cluster (:7000).
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { describeEachMode, createCleanupClient, flushQueue, ConnectionConfig } from './helpers/fixture';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');

describeEachMode('Queue.searchJobs', (CONNECTION) => {
  let cleanupClient: any;
  const Q = 'test-search-' + Date.now();
  let queue: InstanceType<typeof Queue>;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
    queue = new Queue(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('search by name returns exact matches', async () => {
    const qName = Q + '-byname';
    const q = new Queue(qName, { connection: CONNECTION });

    await q.add('email', { to: 'a@b.com' });
    await q.add('sms', { to: '555-1234' });
    await q.add('email', { to: 'c@d.com' });

    const results = await q.searchJobs({ state: 'waiting', name: 'email' });
    expect(results).toHaveLength(2);
    for (const job of results) {
      expect(job.name).toBe('email');
    }

    await q.close();
    await flushQueue(cleanupClient, qName);
  });

  it('search by data field matches shallow key-value pairs', async () => {
    const qName = Q + '-bydata';
    const q = new Queue(qName, { connection: CONNECTION });

    await q.add('task', { userId: '123', type: 'report' });
    await q.add('task', { userId: '456', type: 'report' });
    await q.add('task', { userId: '123', type: 'alert' });

    const results = await q.searchJobs({ state: 'waiting', data: { userId: '123' } });
    expect(results).toHaveLength(2);
    for (const job of results) {
      expect((job.data as any).userId).toBe('123');
    }

    await q.close();
    await flushQueue(cleanupClient, qName);
  });

  it('search by state + name combo', async () => {
    const qName = Q + '-combo';
    const q = new Queue(qName, { connection: CONNECTION });

    // Add some waiting jobs
    await q.add('process', { val: 1 });
    await q.add('notify', { val: 2 });
    await q.add('process', { val: 3 });

    // Add a delayed job with name 'process'
    await q.add('process', { val: 4 }, { delay: 60000 });

    // Search waiting + process
    const waitingResults = await q.searchJobs({ state: 'waiting', name: 'process' });
    expect(waitingResults).toHaveLength(2);
    for (const job of waitingResults) {
      expect(job.name).toBe('process');
    }

    // Search delayed + process
    const delayedResults = await q.searchJobs({ state: 'delayed', name: 'process' });
    expect(delayedResults).toHaveLength(1);
    expect(delayedResults[0].name).toBe('process');

    await q.close();
    await flushQueue(cleanupClient, qName);
  });

  it('search with limit caps results', async () => {
    const qName = Q + '-limit';
    const q = new Queue(qName, { connection: CONNECTION });

    for (let i = 0; i < 10; i++) {
      await q.add('bulk', { index: i });
    }

    const results = await q.searchJobs({ state: 'waiting', name: 'bulk', limit: 3 });
    expect(results).toHaveLength(3);

    await q.close();
    await flushQueue(cleanupClient, qName);
  });

  it('search returns empty array when nothing matches', async () => {
    const qName = Q + '-empty';
    const q = new Queue(qName, { connection: CONNECTION });

    await q.add('real-job', { val: 1 });

    const results = await q.searchJobs({ state: 'waiting', name: 'nonexistent' });
    expect(results).toHaveLength(0);

    await q.close();
    await flushQueue(cleanupClient, qName);
  });

  it('search across all states (no state filter)', async () => {
    const qName = Q + '-allstates';
    const q = new Queue(qName, { connection: CONNECTION });

    // Add a waiting job
    await q.add('target', { src: 'waiting' });
    // Add a delayed job
    await q.add('target', { src: 'delayed' }, { delay: 60000 });

    // Process one job to get a completed state
    await q.add('target', { src: 'complete' });

    let processedCount = 0;
    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker = new Worker(
        qName,
        async () => {
          processedCount++;
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 3, blockTimeout: 1000, stalledInterval: 60000 },
      );
      worker.on('completed', () => {
        // Wait until at least the 'complete' job is done
        if (processedCount >= 1) {
          clearTimeout(timeout);
          setTimeout(() => worker.close(true).then(resolve), 300);
        }
      });
      worker.on('error', () => {});
    });

    await done;

    // Search without state filter for name 'target'
    const results = await q.searchJobs({ name: 'target' });
    // Should find at least the delayed job + completed jobs (waiting ones may have been consumed)
    expect(results.length).toBeGreaterThanOrEqual(1);
    for (const job of results) {
      expect(job.name).toBe('target');
    }

    await q.close();
    await flushQueue(cleanupClient, qName);
  }, 15000);

  it('search in completed state', async () => {
    const qName = Q + '-completed';
    const q = new Queue(qName, { connection: CONNECTION });

    await q.add('done-job', { val: 1 });
    await q.add('done-job', { val: 2 });
    await q.add('other-job', { val: 3 });

    let processedCount = 0;
    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker = new Worker(
        qName,
        async () => {
          processedCount++;
          return 'done';
        },
        { connection: CONNECTION, concurrency: 3, blockTimeout: 1000, stalledInterval: 60000 },
      );
      worker.on('completed', () => {
        if (processedCount >= 3) {
          clearTimeout(timeout);
          setTimeout(() => worker.close(true).then(resolve), 300);
        }
      });
      worker.on('error', () => {});
    });

    await done;

    const results = await q.searchJobs({ state: 'completed', name: 'done-job' });
    expect(results).toHaveLength(2);
    for (const job of results) {
      expect(job.name).toBe('done-job');
    }

    await q.close();
    await flushQueue(cleanupClient, qName);
  }, 15000);

  it('search in failed state', async () => {
    const qName = Q + '-failed';
    const q = new Queue(qName, { connection: CONNECTION });

    await q.add('fail-me', { val: 1 });
    await q.add('succeed', { val: 2 });

    let processedCount = 0;
    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker = new Worker(
        qName,
        async (job) => {
          processedCount++;
          if (job.name === 'fail-me') throw new Error('intentional');
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000, stalledInterval: 60000 },
      );
      const resolved = { failed: false, completed: false };
      worker.on('failed', () => {
        resolved.failed = true;
        if (resolved.completed) {
          clearTimeout(timeout);
          setTimeout(() => worker.close(true).then(resolve), 300);
        }
      });
      worker.on('completed', () => {
        resolved.completed = true;
        if (resolved.failed) {
          clearTimeout(timeout);
          setTimeout(() => worker.close(true).then(resolve), 300);
        }
      });
      worker.on('error', () => {});
    });

    await done;

    const results = await q.searchJobs({ state: 'failed', name: 'fail-me' });
    expect(results).toHaveLength(1);
    expect(results[0].name).toBe('fail-me');

    await q.close();
    await flushQueue(cleanupClient, qName);
  }, 15000);
});
