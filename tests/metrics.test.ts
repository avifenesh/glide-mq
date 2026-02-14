/**
 * Integration tests for getMetrics and getJobCounts.
 * Requires: valkey-server running on localhost:6379
 *
 * Run: npx vitest run tests/metrics.test.ts
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';

const { GlideClient } = require('speedkey') as typeof import('speedkey');
const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
const { LIBRARY_SOURCE } = require('../dist/functions/index') as typeof import('../src/functions/index');
const { ensureFunctionLibrary } = require('../dist/connection') as typeof import('../src/connection');

const CONNECTION = {
  addresses: [{ host: 'localhost', port: 6379 }],
};

let cleanupClient: InstanceType<typeof GlideClient>;

async function flushQueue(queueName: string) {
  const k = buildKeys(queueName);
  const keysToDelete = [
    k.id, k.stream, k.scheduled, k.completed, k.failed,
    k.events, k.meta, k.dedup, k.rate, k.schedulers,
  ];
  for (const key of keysToDelete) {
    try { await cleanupClient.del([key]); } catch {}
  }
  const prefix = `glide:{${queueName}}:job:`;
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

beforeAll(async () => {
  cleanupClient = await GlideClient.createClient({
    addresses: [{ host: 'localhost', port: 6379 }],
  });
  await ensureFunctionLibrary(cleanupClient, LIBRARY_SOURCE);
});

afterAll(async () => {
  cleanupClient.close();
});

describe('Queue metrics', () => {
  const Q = 'test-metrics-' + Date.now();
  let queue: InstanceType<typeof Queue>;

  beforeAll(() => {
    queue = new Queue(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(Q);
  });

  it('getMetrics returns zero counts on empty queue', async () => {
    const completed = await queue.getMetrics('completed');
    const failed = await queue.getMetrics('failed');
    expect(completed.count).toBe(0);
    expect(failed.count).toBe(0);
  });

  it('getJobCounts returns zero counts on empty queue', async () => {
    const counts = await queue.getJobCounts();
    expect(counts.completed).toBe(0);
    expect(counts.failed).toBe(0);
    expect(counts.delayed).toBe(0);
    // waiting might be 0 even though stream doesn't exist yet
    expect(counts.waiting).toBe(0);
  });

  it('getJobCounts reflects waiting jobs after add', async () => {
    // Add some waiting jobs
    await queue.add('job-a', { val: 1 });
    await queue.add('job-b', { val: 2 });
    await queue.add('job-c', { val: 3 });

    const counts = await queue.getJobCounts();
    // These are in the stream but not claimed yet => waiting
    expect(counts.waiting).toBeGreaterThanOrEqual(3);
    expect(counts.active).toBe(0);
  });

  it('getJobCounts reflects delayed jobs', async () => {
    await queue.add('delayed-job', { val: 99 }, { delay: 60000 });

    const counts = await queue.getJobCounts();
    expect(counts.delayed).toBeGreaterThanOrEqual(1);
  });

  it('getMetrics and getJobCounts reflect completed jobs after worker processes them', async () => {
    const qName = Q + '-complete';
    const localQueue = new Queue(qName, { connection: CONNECTION });

    // Add 3 jobs
    await localQueue.add('m1', { x: 1 });
    await localQueue.add('m2', { x: 2 });
    await localQueue.add('m3', { x: 3 });

    // Process all 3
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
          setTimeout(() => worker.close(true).then(resolve), 200);
        }
      });
      worker.on('error', () => {});
    });

    await done;

    // Check completed metrics
    const metrics = await localQueue.getMetrics('completed');
    expect(metrics.count).toBe(3);

    const counts = await localQueue.getJobCounts();
    expect(counts.completed).toBe(3);
    expect(counts.waiting).toBe(0);

    await localQueue.close();
    await flushQueue(qName);
  }, 15000);

  it('getMetrics reflects failed jobs', async () => {
    const qName = Q + '-fail';
    const localQueue = new Queue(qName, { connection: CONNECTION });

    await localQueue.add('fail-job', { x: 1 });

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker = new Worker(
        qName,
        async () => {
          throw new Error('intentional failure');
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000, stalledInterval: 60000 },
      );
      worker.on('failed', () => {
        clearTimeout(timeout);
        setTimeout(() => worker.close(true).then(resolve), 200);
      });
      worker.on('error', () => {});
    });

    await done;

    const metrics = await localQueue.getMetrics('failed');
    expect(metrics.count).toBe(1);

    const counts = await localQueue.getJobCounts();
    expect(counts.failed).toBe(1);

    await localQueue.close();
    await flushQueue(qName);
  }, 15000);
});
