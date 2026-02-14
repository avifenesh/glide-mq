/**
 * Integration tests for priority job lifecycle.
 * Requires: valkey-server running on localhost:6379
 *
 * Run: npx vitest run tests/priority.test.ts
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

describe('Priority jobs', () => {
  const Q = 'test-priority-' + Date.now();
  let queue: InstanceType<typeof Queue>;

  beforeAll(() => {
    queue = new Queue(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(Q);
  });

  it('prioritized job goes to scheduled ZSet with correct score encoding', async () => {
    const k = buildKeys(Q);
    const job = await queue.add('prio-task', { x: 1 }, { priority: 3 });

    const score = await cleanupClient.zscore(k.scheduled, job.id);
    expect(score).not.toBeNull();

    // Score should be priority * 2^42 + 0 (no delay, timestamp component is 0)
    const PRIORITY_SHIFT = 2 ** 42;
    const expectedScore = 3 * PRIORITY_SHIFT;
    expect(Number(score)).toBe(expectedScore);

    // State should be 'prioritized'
    const state = await cleanupClient.hget(k.job(job.id), 'state');
    expect(String(state)).toBe('prioritized');
  });

  it('prioritized jobs are processed in priority order (lower number first)', async () => {
    const qName = Q + '-order';
    const localQueue = new Queue(qName, { connection: CONNECTION });
    const k = buildKeys(qName);

    // Add jobs with priorities 3, 1, 2
    const jobP3 = await localQueue.add('p3', { prio: 3 }, { priority: 3 });
    const jobP1 = await localQueue.add('p1', { prio: 1 }, { priority: 1 });
    const jobP2 = await localQueue.add('p2', { prio: 2 }, { priority: 2 });

    // All should be in scheduled ZSet
    expect(await cleanupClient.zscore(k.scheduled, jobP3.id)).not.toBeNull();
    expect(await cleanupClient.zscore(k.scheduled, jobP1.id)).not.toBeNull();
    expect(await cleanupClient.zscore(k.scheduled, jobP2.id)).not.toBeNull();

    // Start worker first so the consumer group and poll loop are ready
    const processedPrios: number[] = [];
    const allDone = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      let count = 0;
      const worker = new Worker(
        qName,
        async (j: any) => {
          processedPrios.push(j.data.prio);
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

    // Wait for worker to initialize and start polling
    await new Promise(r => setTimeout(r, 500));

    // Now promote - priority scores exceed Date.now(), so use a large timestamp
    const promoted = await promote(cleanupClient, buildKeys(qName), Number.MAX_SAFE_INTEGER);
    expect(promoted).toBe(3);

    await allDone;

    // ZRANGEBYSCORE returns in ascending score order: prio 1, prio 2, prio 3
    // They enter the stream in that order, worker processes FIFO
    expect(processedPrios).toEqual([1, 2, 3]);

    await localQueue.close();
    await flushQueue(qName);
  }, 15000);

  it('priority + delay: combined score respects both priority and delay', async () => {
    const qName = Q + '-prio-delay';
    const localQueue = new Queue(qName, { connection: CONNECTION });
    const k = buildKeys(qName);

    // Same priority, different delays: within the same priority band,
    // the job with the shorter delay has a lower score and promotes first.
    const jobShort = await localQueue.add('short', { label: 'short' }, { priority: 2, delay: 200 });
    const jobLong = await localQueue.add('long', { label: 'long' }, { priority: 2, delay: 600 });

    // Both in scheduled ZSet
    expect(await cleanupClient.zscore(k.scheduled, jobShort.id)).not.toBeNull();
    expect(await cleanupClient.zscore(k.scheduled, jobLong.id)).not.toBeNull();

    // Score for short delay should be less than score for long delay (same priority)
    const scoreShort = Number(await cleanupClient.zscore(k.scheduled, jobShort.id));
    const scoreLong = Number(await cleanupClient.zscore(k.scheduled, jobLong.id));
    expect(scoreShort).toBeLessThan(scoreLong);

    // Start worker first
    const processedLabels: string[] = [];
    const allDone = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      let count = 0;
      const worker = new Worker(
        qName,
        async (j: any) => {
          processedLabels.push(j.data.label);
          count++;
          if (count >= 2) {
            clearTimeout(timeout);
            setTimeout(() => worker.close(true).then(resolve), 200);
          }
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
      );
      worker.on('error', () => {});
    });

    // Wait for worker to be ready
    await new Promise(r => setTimeout(r, 500));

    // Force-promote all
    const promoted = await promote(cleanupClient, buildKeys(qName), Number.MAX_SAFE_INTEGER);
    expect(promoted).toBe(2);

    await allDone;
    // Short delay has lower score -> promoted first -> processed first
    expect(processedLabels).toEqual(['short', 'long']);

    await localQueue.close();
    await flushQueue(qName);
  }, 15000);

  it('non-prioritized job (priority=0) goes directly to stream, bypassing scheduled ZSet', async () => {
    const qName = Q + '-noprio';
    const localQueue = new Queue(qName, { connection: CONNECTION });
    const k = buildKeys(qName);

    const job = await localQueue.add('normal', { x: 1 });

    // Should NOT be in scheduled ZSet
    const score = await cleanupClient.zscore(k.scheduled, job.id);
    expect(score).toBeNull();

    // State should be 'waiting' (directly in stream)
    const state = await cleanupClient.hget(k.job(job.id), 'state');
    expect(String(state)).toBe('waiting');

    await localQueue.close();
    await flushQueue(qName);
  });
});
