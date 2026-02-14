/**
 * Integration tests for job retention (removeOnComplete / removeOnFail).
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
  // Force reload the library to pick up updated complete/fail functions with retention
  await cleanupClient.functionLoad(LIBRARY_SOURCE, { replace: true });
});

afterAll(async () => {
  cleanupClient.close();
});

describe('removeOnComplete: true', () => {
  const Q = 'test-ret-true-' + Date.now();

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('removes completed job hash and ZSet entry immediately', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker = new Worker(
        Q,
        async () => 'done',
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
      );
      worker.on('completed', () => {
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });
      worker.on('error', () => {});
    });

    await new Promise(r => setTimeout(r, 500));
    const job = await queue.add('work', { v: 1 }, { removeOnComplete: true });

    await done;

    // Job hash should be deleted
    const exists = await cleanupClient.exists([k.job(job!.id)]);
    expect(exists).toBe(0);

    // Not in completed ZSet
    const score = await cleanupClient.zscore(k.completed, job!.id);
    expect(score).toBeNull();

    await queue.close();
  }, 15000);
});

describe('removeOnComplete: count', () => {
  const Q = 'test-ret-count-' + Date.now();

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('keeps only N most recent completed jobs', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);
    let processed = 0;
    const TOTAL = 5;
    const KEEP = 2;

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);
      const worker = new Worker(
        Q,
        async () => {
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
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
      await queue.add(`job-${i}`, { i }, { removeOnComplete: KEEP });
    }

    await done;

    // Wait a bit for all Lua retention cleanup to finish
    await new Promise(r => setTimeout(r, 200));

    // Should have at most KEEP entries in the completed ZSet
    const completedCount = await cleanupClient.zcard(k.completed);
    expect(completedCount).toBeLessThanOrEqual(KEEP);

    await queue.close();
  }, 20000);
});

describe('removeOnFail: true', () => {
  const Q = 'test-ret-fail-true-' + Date.now();

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('removes failed job hash and ZSet entry immediately', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker = new Worker(
        Q,
        async () => {
          throw new Error('intentional failure');
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
      );
      worker.on('failed', () => {
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });
      worker.on('error', () => {});
    });

    await new Promise(r => setTimeout(r, 500));
    const job = await queue.add('work', { v: 1 }, { removeOnFail: true });

    await done;

    // Job hash should be deleted
    const exists = await cleanupClient.exists([k.job(job!.id)]);
    expect(exists).toBe(0);

    // Not in failed ZSet
    const score = await cleanupClient.zscore(k.failed, job!.id);
    expect(score).toBeNull();

    await queue.close();
  }, 15000);
});

describe('removeOnFail: count', () => {
  const Q = 'test-ret-fail-count-' + Date.now();

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('keeps only N most recent failed jobs', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);
    let processed = 0;
    const TOTAL = 5;
    const KEEP = 2;

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);
      const worker = new Worker(
        Q,
        async () => {
          throw new Error('fail');
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
      );
      worker.on('failed', () => {
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
      await queue.add(`job-${i}`, { i }, { removeOnFail: KEEP });
    }

    await done;

    await new Promise(r => setTimeout(r, 200));

    const failedCount = await cleanupClient.zcard(k.failed);
    expect(failedCount).toBeLessThanOrEqual(KEEP);

    await queue.close();
  }, 20000);
});
