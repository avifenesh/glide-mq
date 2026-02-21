/**
 * Integration tests for global concurrency control.
 * Requires: valkey-server running on localhost:6379 and cluster on :7000-7005
 */
import { it, expect, beforeAll, afterAll } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

describeEachMode('Queue.setGlobalConcurrency', (CONNECTION) => {
  const Q = 'test-gc-set-' + Date.now();
  let queue: InstanceType<typeof Queue>;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
    queue = new Queue(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
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

describeEachMode('Global concurrency enforcement', (CONNECTION) => {
  const Q = 'test-gc-enforce-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
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
          await new Promise((r) => setTimeout(r, 200));
          current--;
          return 'ok';
        },
        {
          connection: CONNECTION,
          concurrency: 5,
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

    await new Promise((r) => setTimeout(r, 500));
    for (let i = 0; i < TOTAL; i++) {
      await queue.add(`gc-${i}`, { i });
    }

    await done;

    expect(processed).toBe(TOTAL);
    expect(maxConcurrent).toBeLessThanOrEqual(2);

    await queue.close();
  }, 25000);
});

describeEachMode('Global concurrency: no limit when not set', (CONNECTION) => {
  const Q = 'test-gc-nolimit-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
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
          await new Promise((r) => setTimeout(r, 100));
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

    await new Promise((r) => setTimeout(r, 500));
    for (let i = 0; i < TOTAL; i++) {
      await queue.add(`nc-${i}`, { i });
    }

    await done;

    expect(processed).toBe(TOTAL);
    expect(maxConcurrent).toBeGreaterThanOrEqual(2);

    await queue.close();
  }, 20000);
});
