/**
 * Integration tests for Worker batch processing.
 * Tests the batch option on Worker that collects N jobs and passes them
 * as an array to the processor.
 *
 * Requires: valkey-server running on localhost:6379 and cluster on :7000-7005
 *
 * Run: npx vitest run tests/batch-processing.test.ts
 */
import { it, expect, beforeAll, afterAll } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { BatchError } = require('../dist/errors') as typeof import('../src/errors');
const { GlideMQError } = require('../dist/errors') as typeof import('../src/errors');

import { describeEachMode, createCleanupClient, flushQueue, waitFor } from './helpers/fixture';

describeEachMode('Worker batch processing', (CONNECTION) => {
  const Q = 'test-batch-proc-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('processes jobs in batches', async () => {
    const qName = Q + '-basic';
    const queue = new Queue(qName, { connection: CONNECTION });
    const batchSizes: number[] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);
      let totalProcessed = 0;

      const worker = new Worker(
        qName,
        async (jobs: any[]) => {
          batchSizes.push(jobs.length);
          totalProcessed += jobs.length;
          if (totalProcessed >= 10) {
            clearTimeout(timeout);
            setTimeout(() => worker.close(true).then(resolve), 200);
          }
          return jobs.map((j: any) => ({ processed: j.data.i }));
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 1000,
          batch: { size: 5 },
        },
      );
      worker.on('error', () => {});
    });

    // Wait for worker to connect
    await new Promise((r) => setTimeout(r, 500));

    // Add 10 jobs
    for (let i = 0; i < 10; i++) {
      await queue.add('batch-job', { i });
    }

    await done;

    // All 10 jobs should have been processed
    const total = batchSizes.reduce((a, b) => a + b, 0);
    expect(total).toBe(10);
    // Each batch should be at most 5
    for (const size of batchSizes) {
      expect(size).toBeLessThanOrEqual(5);
      expect(size).toBeGreaterThan(0);
    }

    await queue.close();
    await flushQueue(cleanupClient, qName);
  }, 20000);

  it('processes partial batch without timeout', async () => {
    const qName = Q + '-partial';
    const queue = new Queue(qName, { connection: CONNECTION });
    const batchSizes: number[] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);

      const worker = new Worker(
        qName,
        async (jobs: any[]) => {
          batchSizes.push(jobs.length);
          clearTimeout(timeout);
          setTimeout(() => worker.close(true).then(resolve), 200);
          return jobs.map(() => 'ok');
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 1000,
          batch: { size: 10 },
        },
      );
      worker.on('error', () => {});
    });

    await new Promise((r) => setTimeout(r, 500));

    // Add only 3 jobs (less than batch size of 10)
    await queue.addBulk([
      { name: 'p1', data: { i: 1 } },
      { name: 'p2', data: { i: 2 } },
      { name: 'p3', data: { i: 3 } },
    ]);

    await done;

    // Without timeout, should process immediately with 3 jobs
    expect(batchSizes.length).toBeGreaterThanOrEqual(1);
    const total = batchSizes.reduce((a, b) => a + b, 0);
    expect(total).toBe(3);

    await queue.close();
    await flushQueue(cleanupClient, qName);
  }, 20000);

  it('each job gets correct return value', async () => {
    const qName = Q + '-retval';
    const queue = new Queue(qName, { connection: CONNECTION });
    const completedResults: { jobId: string; returnvalue: any }[] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);
      let count = 0;

      const worker = new Worker(
        qName,
        async (jobs: any[]) => {
          return jobs.map((j: any) => j.data.i * 10);
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 1000,
          batch: { size: 5 },
        },
      );

      worker.on('completed', (job: any, result: any) => {
        completedResults.push({ jobId: job.id, returnvalue: result });
        count++;
        if (count >= 5) {
          clearTimeout(timeout);
          setTimeout(() => worker.close(true).then(resolve), 200);
        }
      });
      worker.on('error', () => {});
    });

    await new Promise((r) => setTimeout(r, 500));

    const jobs = await queue.addBulk([
      { name: 'r1', data: { i: 1 } },
      { name: 'r2', data: { i: 2 } },
      { name: 'r3', data: { i: 3 } },
      { name: 'r4', data: { i: 4 } },
      { name: 'r5', data: { i: 5 } },
    ]);

    await done;

    expect(completedResults).toHaveLength(5);
    // Check return values match input * 10
    for (const job of jobs) {
      const result = completedResults.find((r) => r.jobId === job.id);
      expect(result).toBeDefined();
      expect(result!.returnvalue).toBe(job.data.i * 10);
    }

    await queue.close();
    await flushQueue(cleanupClient, qName);
  }, 20000);

  it('processor throw fails all jobs in batch', async () => {
    const qName = Q + '-throw';
    const queue = new Queue(qName, { connection: CONNECTION });
    const failedJobs: string[] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);

      const worker = new Worker(
        qName,
        async (_jobs: any[]) => {
          throw new Error('batch exploded');
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 1000,
          batch: { size: 3 },
        },
      );

      worker.on('failed', (job: any) => {
        failedJobs.push(job.id);
        if (failedJobs.length >= 3) {
          clearTimeout(timeout);
          setTimeout(() => worker.close(true).then(resolve), 200);
        }
      });
      worker.on('error', () => {});
    });

    await new Promise((r) => setTimeout(r, 500));

    await queue.addBulk([
      { name: 'f1', data: {} },
      { name: 'f2', data: {} },
      { name: 'f3', data: {} },
    ]);

    await done;

    expect(failedJobs).toHaveLength(3);

    await queue.close();
    await flushQueue(cleanupClient, qName);
  }, 20000);

  it('BatchError allows partial failure', async () => {
    const qName = Q + '-partial-fail';
    const queue = new Queue(qName, { connection: CONNECTION });
    const completed: string[] = [];
    const failed: string[] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);
      let total = 0;

      const worker = new Worker(
        qName,
        async (jobs: any[]) => {
          // First job succeeds, second fails, third succeeds
          throw new BatchError([
            'success-1',
            new Error('job 2 failed'),
            'success-3',
          ]);
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 1000,
          batch: { size: 3 },
        },
      );

      worker.on('completed', (job: any) => {
        completed.push(job.id);
        total++;
        if (total >= 3) {
          clearTimeout(timeout);
          setTimeout(() => worker.close(true).then(resolve), 200);
        }
      });
      worker.on('failed', (job: any) => {
        failed.push(job.id);
        total++;
        if (total >= 3) {
          clearTimeout(timeout);
          setTimeout(() => worker.close(true).then(resolve), 200);
        }
      });
      worker.on('error', () => {});
    });

    await new Promise((r) => setTimeout(r, 500));

    await queue.addBulk([
      { name: 'pf1', data: {} },
      { name: 'pf2', data: {} },
      { name: 'pf3', data: {} },
    ]);

    await done;

    expect(completed).toHaveLength(2);
    expect(failed).toHaveLength(1);

    await queue.close();
    await flushQueue(cleanupClient, qName);
  }, 20000);

  it('emits active event for each job in batch', async () => {
    const qName = Q + '-events';
    const queue = new Queue(qName, { connection: CONNECTION });
    const activeJobs: string[] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);
      let completedCount = 0;

      const worker = new Worker(
        qName,
        async (jobs: any[]) => {
          return jobs.map(() => 'done');
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 1000,
          batch: { size: 3 },
        },
      );

      worker.on('active', (_job: any, jobId: string) => {
        activeJobs.push(jobId);
      });
      worker.on('completed', () => {
        completedCount++;
        if (completedCount >= 3) {
          clearTimeout(timeout);
          setTimeout(() => worker.close(true).then(resolve), 200);
        }
      });
      worker.on('error', () => {});
    });

    await new Promise((r) => setTimeout(r, 500));

    const jobs = await queue.addBulk([
      { name: 'e1', data: {} },
      { name: 'e2', data: {} },
      { name: 'e3', data: {} },
    ]);

    await done;

    // Each job should have emitted an 'active' event
    for (const job of jobs) {
      expect(activeJobs).toContain(job.id);
    }

    await queue.close();
    await flushQueue(cleanupClient, qName);
  }, 20000);
});

describeEachMode('Worker batch validation', (CONNECTION) => {
  it('throws on batch.size < 1', () => {
    expect(() => {
      new Worker(
        'test-batch-val-1',
        async () => [],
        { connection: CONNECTION, batch: { size: 0 } },
      );
    }).toThrow('batch.size must be an integer between 1 and 1000');
  });

  it('throws on batch.size > 1000', () => {
    expect(() => {
      new Worker(
        'test-batch-val-2',
        async () => [],
        { connection: CONNECTION, batch: { size: 1001 } },
      );
    }).toThrow('batch.size must be an integer between 1 and 1000');
  });

  it('throws on non-integer batch.size', () => {
    expect(() => {
      new Worker(
        'test-batch-val-3',
        async () => [],
        { connection: CONNECTION, batch: { size: 2.5 } },
      );
    }).toThrow('batch.size must be an integer between 1 and 1000');
  });

  it('throws on batch + sandbox processor', () => {
    expect(() => {
      new Worker(
        'test-batch-val-4',
        '/some/path.js',
        { connection: CONNECTION, batch: { size: 5 } },
      );
    }).toThrow('Batch mode does not support sandbox (file path) processors');
  });

  it('throws on negative batch.timeout', () => {
    expect(() => {
      new Worker(
        'test-batch-val-5',
        async () => [],
        { connection: CONNECTION, batch: { size: 5, timeout: -1 } },
      );
    }).toThrow('batch.timeout must be a non-negative finite number');
  });
});
