/**
 * Integration tests for delayed job lifecycle.
 * Requires: valkey-server running on localhost:6379
 *
 * Run: npx vitest run tests/delayed.test.ts
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';

const { GlideClient } = require('speedkey') as typeof import('speedkey');
const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
const { LIBRARY_SOURCE, CONSUMER_GROUP } = require('../dist/functions/index') as typeof import('../src/functions/index');
const { ensureFunctionLibrary } = require('../dist/connection') as typeof import('../src/connection');
const { promote } = require('../dist/functions/index') as typeof import('../src/functions/index');

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

describe('Delayed jobs', () => {
  const Q = 'test-delayed-' + Date.now();
  let queue: InstanceType<typeof Queue>;

  beforeAll(() => {
    queue = new Queue(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(Q);
  });

  it('delayed job lands in scheduled ZSet, not in stream', async () => {
    const job = await queue.add('delayed-task', { x: 1 }, { delay: 60000 });
    const k = buildKeys(Q);

    // Should be in the scheduled ZSet
    const score = await cleanupClient.zscore(k.scheduled, job.id);
    expect(score).not.toBeNull();

    // Should NOT be in the stream
    const streamEntries = await cleanupClient.xrange(k.stream, '-', '+') as Record<string, [string, string][]>;
    const streamJobIds: string[] = [];
    for (const entryId of Object.keys(streamEntries)) {
      const fields = streamEntries[entryId];
      for (const [f, v] of fields) {
        if (String(f) === 'jobId' && String(v) === job.id) {
          streamJobIds.push(String(v));
        }
      }
    }
    expect(streamJobIds).toHaveLength(0);

    // Job state should be 'delayed'
    const state = await cleanupClient.hget(k.job(job.id), 'state');
    expect(String(state)).toBe('delayed');
  });

  it('delayed job is promoted after delay elapses and processed by worker', async () => {
    const k = buildKeys(Q);
    const delayMs = 500;
    const job = await queue.add('short-delay', { val: 42 }, { delay: delayMs });

    // Verify job is in scheduled ZSet
    const scoreBefore = await cleanupClient.zscore(k.scheduled, job.id);
    expect(scoreBefore).not.toBeNull();

    // Wait for the delay to elapse
    await new Promise(r => setTimeout(r, delayMs + 100));

    // Manually trigger promotion via fcall
    const promoted = await promote(cleanupClient, buildKeys(Q), Date.now());
    expect(promoted).toBeGreaterThanOrEqual(1);

    // Job should no longer be in scheduled ZSet
    const scoreAfter = await cleanupClient.zscore(k.scheduled, job.id);
    expect(scoreAfter).toBeNull();

    // Job state should be 'waiting' after promotion
    const state = await cleanupClient.hget(k.job(job.id), 'state');
    expect(String(state)).toBe('waiting');

    // Now start a worker to process it
    const processed: string[] = [];
    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker = new Worker(
        Q,
        async (j: any) => {
          processed.push(j.id);
          return { result: 'done' };
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
      );
      worker.on('completed', () => {
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });
      worker.on('error', () => {});
    });

    await done;
    expect(processed).toContain(job.id);

    // Verify completed state
    const completedScore = await cleanupClient.zscore(k.completed, job.id);
    expect(completedScore).not.toBeNull();
    const finalState = await cleanupClient.hget(k.job(job.id), 'state');
    expect(String(finalState)).toBe('completed');
  }, 15000);

  it('multiple delayed jobs with different delays promote in correct order', async () => {
    const qName = Q + '-multi';
    const localQueue = new Queue(qName, { connection: CONNECTION });
    const k = buildKeys(qName);

    const now = Date.now();
    // Add jobs with increasing delays
    const job1 = await localQueue.add('d1', { order: 1 }, { delay: 200 });
    const job2 = await localQueue.add('d2', { order: 2 }, { delay: 400 });
    const job3 = await localQueue.add('d3', { order: 3 }, { delay: 800 });

    // All three should be in scheduled ZSet
    expect(await cleanupClient.zscore(k.scheduled, job1.id)).not.toBeNull();
    expect(await cleanupClient.zscore(k.scheduled, job2.id)).not.toBeNull();
    expect(await cleanupClient.zscore(k.scheduled, job3.id)).not.toBeNull();

    // After 300ms, only job1 should be promotable
    await new Promise(r => setTimeout(r, 300));
    const promoted1 = await promote(cleanupClient, buildKeys(qName), Date.now());
    expect(promoted1).toBe(1);
    expect(await cleanupClient.zscore(k.scheduled, job1.id)).toBeNull();
    expect(await cleanupClient.zscore(k.scheduled, job2.id)).not.toBeNull();
    expect(await cleanupClient.zscore(k.scheduled, job3.id)).not.toBeNull();

    // After another 200ms (total ~500ms), job2 should also be promotable
    await new Promise(r => setTimeout(r, 200));
    const promoted2 = await promote(cleanupClient, buildKeys(qName), Date.now());
    expect(promoted2).toBe(1);
    expect(await cleanupClient.zscore(k.scheduled, job2.id)).toBeNull();
    expect(await cleanupClient.zscore(k.scheduled, job3.id)).not.toBeNull();

    // After another 400ms (total ~900ms), job3 should be promotable
    await new Promise(r => setTimeout(r, 400));
    const promoted3 = await promote(cleanupClient, buildKeys(qName), Date.now());
    expect(promoted3).toBe(1);
    expect(await cleanupClient.zscore(k.scheduled, job3.id)).toBeNull();

    // Process all three jobs and verify order
    const processedOrder: number[] = [];
    const allDone = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      let count = 0;
      const worker = new Worker(
        qName,
        async (j: any) => {
          processedOrder.push(j.data.order);
          count++;
          if (count >= 3) {
            clearTimeout(timeout);
            setTimeout(() => worker.close(true).then(resolve), 200);
          }
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
      );
      worker.on('error', () => {});
    });

    await allDone;
    expect(processedOrder).toHaveLength(3);
    // They were promoted in order 1,2,3 so stream preserves FIFO
    expect(processedOrder).toEqual([1, 2, 3]);

    await localQueue.close();
    await flushQueue(qName);
  }, 20000);
});
