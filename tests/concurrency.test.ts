/**
 * Integration tests for global concurrency control.
 * Requires: valkey-server running on localhost:6379
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
  // Force reload the library to pick up new functions (checkConcurrency)
  await cleanupClient.functionLoad(LIBRARY_SOURCE, { replace: true });
});

afterAll(async () => {
  cleanupClient.close();
});

describe('Queue.setGlobalConcurrency', () => {
  const Q = 'test-gc-set-' + Date.now();
  let queue: InstanceType<typeof Queue>;

  beforeAll(() => {
    queue = new Queue(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(Q);
  });

  it('stores globalConcurrency in meta hash', async () => {
    const k = buildKeys(Q);
    await queue.setGlobalConcurrency(5);
    const val = await cleanupClient.hget(k.meta, 'globalConcurrency');
    expect(String(val)).toBe('5');
  });

  it('setting 0 effectively removes the limit', async () => {
    const k = buildKeys(Q);
    await queue.setGlobalConcurrency(0);
    const val = await cleanupClient.hget(k.meta, 'globalConcurrency');
    expect(String(val)).toBe('0');
  });
});

describe('Global concurrency enforcement', () => {
  const Q = 'test-gc-enforce-' + Date.now();

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('limits active jobs across workers to globalConcurrency', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    await queue.setGlobalConcurrency(2);

    let maxConcurrent = 0;
    let current = 0;
    let processed = 0;
    const TOTAL = 6;

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 20000);
      const worker = new Worker(
        Q,
        async () => {
          current++;
          if (current > maxConcurrent) maxConcurrent = current;
          // Hold the job active for a bit so concurrency can be observed
          await new Promise(r => setTimeout(r, 200));
          current--;
          return 'ok';
        },
        {
          connection: CONNECTION,
          concurrency: 5, // worker willing to run 5, but global limit is 2
          blockTimeout: 500,
        },
      );
      worker.on('completed', () => {
        processed++;
        if (processed >= TOTAL) {
          clearTimeout(timeout);
          worker.close(true).then(resolve);
        }
      });
      worker.on('error', () => {});
    });

    await new Promise(r => setTimeout(r, 500));
    for (let i = 0; i < TOTAL; i++) {
      await queue.add(`gc-${i}`, { i });
    }

    await done;

    expect(processed).toBe(TOTAL);
    // The global concurrency limit is 2, so max concurrent should be at most 2
    expect(maxConcurrent).toBeLessThanOrEqual(2);

    await queue.close();
  }, 25000);
});

describe('Global concurrency: no limit when not set', () => {
  const Q = 'test-gc-nolimit-' + Date.now();

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('processes jobs without blocking when globalConcurrency is not set', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });

    let maxConcurrent = 0;
    let current = 0;
    let processed = 0;
    const TOTAL = 4;

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);
      const worker = new Worker(
        Q,
        async () => {
          current++;
          if (current > maxConcurrent) maxConcurrent = current;
          await new Promise(r => setTimeout(r, 100));
          current--;
          return 'ok';
        },
        {
          connection: CONNECTION,
          concurrency: 4,
          blockTimeout: 500,
        },
      );
      worker.on('completed', () => {
        processed++;
        if (processed >= TOTAL) {
          clearTimeout(timeout);
          worker.close(true).then(resolve);
        }
      });
      worker.on('error', () => {});
    });

    await new Promise(r => setTimeout(r, 500));
    for (let i = 0; i < TOTAL; i++) {
      await queue.add(`nc-${i}`, { i });
    }

    await done;

    expect(processed).toBe(TOTAL);
    // Without global concurrency, should run up to the worker's own concurrency limit
    expect(maxConcurrent).toBeGreaterThanOrEqual(2);

    await queue.close();
  }, 20000);
});
