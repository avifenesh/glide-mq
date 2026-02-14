/**
 * Integration tests for job retention (removeOnComplete / removeOnFail).
 * Requires: valkey-server running on localhost:6379 and cluster on :7000-7005
 */
import { it, expect, beforeAll, afterAll } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

describeEachMode('removeOnComplete: true', (CONNECTION) => {
  const Q = 'test-ret-true-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
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

    const exists = await cleanupClient.exists([k.job(job!.id)]);
    expect(exists).toBe(0);

    const score = await cleanupClient.zscore(k.completed, job!.id);
    expect(score).toBeNull();

    await queue.close();
  }, 15000);
});

describeEachMode('removeOnComplete: count', (CONNECTION) => {
  const Q = 'test-ret-count-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
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

    await new Promise(r => setTimeout(r, 200));

    const completedCount = await cleanupClient.zcard(k.completed);
    expect(completedCount).toBeLessThanOrEqual(KEEP);

    await queue.close();
  }, 20000);
});

describeEachMode('removeOnFail: true', (CONNECTION) => {
  const Q = 'test-ret-fail-true-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
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

    const exists = await cleanupClient.exists([k.job(job!.id)]);
    expect(exists).toBe(0);

    const score = await cleanupClient.zscore(k.failed, job!.id);
    expect(score).toBeNull();

    await queue.close();
  }, 15000);
});

describeEachMode('removeOnFail: count', (CONNECTION) => {
  const Q = 'test-ret-fail-count-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
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
