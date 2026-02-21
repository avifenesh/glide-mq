/**
 * Rate limiting integration tests against a real Valkey instance.
 * Requires: valkey-server running on localhost:6379 and cluster on :7000-7005
 *
 * Run: npx vitest run tests/rate-limit.test.ts
 */
import { it, expect, beforeAll, afterAll } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

describeEachMode('glidemq_rateLimit Lua function', (CONNECTION) => {
  const Q = 'test-ratelimit-lua-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('allows requests within the limit', async () => {
    const k = buildKeys(Q);
    const now = Date.now();

    for (let i = 0; i < 3; i++) {
      const result = await cleanupClient.fcall(
        'glidemq_rateLimit',
        [k.rate, k.meta],
        ['3', '10000', (now + i).toString()],
      );
      expect(Number(result)).toBe(0);
    }
  });

  it('returns delay when rate limit is exceeded', async () => {
    const k = buildKeys(Q);
    await cleanupClient.del([k.rate]);

    const now = Date.now();

    for (let i = 0; i < 2; i++) {
      const result = await cleanupClient.fcall('glidemq_rateLimit', [k.rate, k.meta], ['2', '5000', now.toString()]);
      expect(Number(result)).toBe(0);
    }

    const result = await cleanupClient.fcall(
      'glidemq_rateLimit',
      [k.rate, k.meta],
      ['2', '5000', (now + 1000).toString()],
    );
    const delay = Number(result);
    expect(delay).toBeGreaterThan(0);
    expect(delay).toBeLessThanOrEqual(5000);
    expect(delay).toBeGreaterThanOrEqual(3000);
  });

  it('resets the window after duration expires', async () => {
    const k = buildKeys(Q);
    await cleanupClient.del([k.rate]);

    const now = Date.now();
    const windowDuration = 1000;

    const r1 = await cleanupClient.fcall(
      'glidemq_rateLimit',
      [k.rate, k.meta],
      ['1', windowDuration.toString(), now.toString()],
    );
    expect(Number(r1)).toBe(0);

    const r2 = await cleanupClient.fcall(
      'glidemq_rateLimit',
      [k.rate, k.meta],
      ['1', windowDuration.toString(), (now + 500).toString()],
    );
    expect(Number(r2)).toBeGreaterThan(0);

    const r3 = await cleanupClient.fcall(
      'glidemq_rateLimit',
      [k.rate, k.meta],
      ['1', windowDuration.toString(), (now + windowDuration + 1).toString()],
    );
    expect(Number(r3)).toBe(0);
  });
});

describeEachMode('Worker with rate limiter', (CONNECTION) => {
  const Q = 'test-ratelimit-worker-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('rate limits job processing to max per duration', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const timestamps: number[] = [];

    for (let i = 0; i < 4; i++) {
      await queue.add(`job-${i}`, { i });
    }

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 20000);
      const worker = new Worker(
        Q,
        async (job: any) => {
          timestamps.push(Date.now());
          return 'ok';
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 1000,
          limiter: { max: 2, duration: 2000 },
        },
      );
      worker.on('error', () => {});
      worker.on('completed', () => {
        if (timestamps.length >= 4) {
          clearTimeout(timeout);
          setTimeout(() => worker.close(true).then(resolve), 200);
        }
      });
    });

    await done;
    await queue.close();

    expect(timestamps.length).toBe(4);

    const gap = timestamps[2] - timestamps[1];
    expect(gap).toBeGreaterThanOrEqual(1500);
  }, 25000);
});

describeEachMode('Worker.rateLimit() manual method', (CONNECTION) => {
  const Q = 'test-manual-ratelimit-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('rateLimit(ms) delays subsequent job processing', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const timestamps: number[] = [];

    await queue.add('job-0', { i: 0 });
    await queue.add('job-1', { i: 1 });

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);
      const worker = new Worker(
        Q,
        async (job: any) => {
          timestamps.push(Date.now());
          if (timestamps.length === 1) {
            await worker.rateLimit(2000);
          }
          return 'ok';
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 1000,
          limiter: { max: 100, duration: 100000 },
        },
      );
      worker.on('error', () => {});
      worker.on('completed', () => {
        if (timestamps.length >= 2) {
          clearTimeout(timeout);
          setTimeout(() => worker.close(true).then(resolve), 200);
        }
      });
    });

    await done;
    await queue.close();

    expect(timestamps.length).toBe(2);
    const gap = timestamps[1] - timestamps[0];
    expect(gap).toBeGreaterThanOrEqual(1500);
  }, 20000);
});

describeEachMode('Worker.RateLimitError in processor', (CONNECTION) => {
  const Q = 'test-ratelimit-error-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('RateLimitError re-queues the job for retry', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    let attempts = 0;

    await queue.add('limited', { x: 1 });

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);
      const worker = new Worker(
        Q,
        async (job: any) => {
          attempts++;
          if (attempts === 1) {
            throw new Worker.RateLimitError();
          }
          return 'completed';
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 1000,
          limiter: { max: 100, duration: 1000 },
        },
      );
      worker.on('error', () => {});
      worker.on('completed', () => {
        clearTimeout(timeout);
        setTimeout(() => worker.close(true).then(resolve), 200);
      });
    });

    await done;
    await queue.close();

    expect(attempts).toBe(2);
  }, 20000);
});
