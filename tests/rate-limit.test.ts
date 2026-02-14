/**
 * Rate limiting integration tests against a real Valkey instance.
 * Requires: valkey-server running on localhost:6379
 *
 * Run: npx vitest run tests/rate-limit.test.ts
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';

const { GlideClient } = require('speedkey') as typeof import('speedkey');
const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
const { LIBRARY_SOURCE, CONSUMER_GROUP } = require('../dist/functions/index') as typeof import('../src/functions/index');

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
  // Force reload the library to pick up new functions (glidemq_rateLimit etc.)
  await cleanupClient.functionLoad(LIBRARY_SOURCE, { replace: true });
});

afterAll(async () => {
  cleanupClient.close();
});

describe('glidemq_rateLimit Lua function', () => {
  const Q = 'test-ratelimit-lua-' + Date.now();

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('allows requests within the limit', async () => {
    const k = buildKeys(Q);
    const now = Date.now();

    // max=3, duration=10000ms - first 3 should be allowed
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
    // Clean up rate key from previous test
    await cleanupClient.del([k.rate]);

    const now = Date.now();

    // Fill the window (max=2, duration=5000ms)
    for (let i = 0; i < 2; i++) {
      const result = await cleanupClient.fcall(
        'glidemq_rateLimit',
        [k.rate, k.meta],
        ['2', '5000', now.toString()],
      );
      expect(Number(result)).toBe(0);
    }

    // Next request should be rate limited
    const result = await cleanupClient.fcall(
      'glidemq_rateLimit',
      [k.rate, k.meta],
      ['2', '5000', (now + 1000).toString()],
    );
    const delay = Number(result);
    expect(delay).toBeGreaterThan(0);
    // Should be roughly 4000ms (5000 - 1000)
    expect(delay).toBeLessThanOrEqual(5000);
    expect(delay).toBeGreaterThanOrEqual(3000);
  });

  it('resets the window after duration expires', async () => {
    const k = buildKeys(Q);
    await cleanupClient.del([k.rate]);

    const now = Date.now();
    const windowDuration = 1000;

    // Fill the window (max=1, duration=1000ms)
    const r1 = await cleanupClient.fcall(
      'glidemq_rateLimit',
      [k.rate, k.meta],
      ['1', windowDuration.toString(), now.toString()],
    );
    expect(Number(r1)).toBe(0);

    // Should be rate limited
    const r2 = await cleanupClient.fcall(
      'glidemq_rateLimit',
      [k.rate, k.meta],
      ['1', windowDuration.toString(), (now + 500).toString()],
    );
    expect(Number(r2)).toBeGreaterThan(0);

    // After window expires, should be allowed again
    const r3 = await cleanupClient.fcall(
      'glidemq_rateLimit',
      [k.rate, k.meta],
      ['1', windowDuration.toString(), (now + windowDuration + 1).toString()],
    );
    expect(Number(r3)).toBe(0);
  });
});

describe('Worker with rate limiter', () => {
  const Q = 'test-ratelimit-worker-' + Date.now();

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('rate limits job processing to max per duration', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const timestamps: number[] = [];

    // Add 4 jobs
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

    // The first 2 jobs should complete quickly (within the first window).
    // The next 2 should be delayed by at least ~2000ms (the window duration).
    // We check that there's a gap of at least 1500ms between the 2nd and 3rd job
    // (allowing some margin for test execution overhead).
    const gap = timestamps[2] - timestamps[1];
    expect(gap).toBeGreaterThanOrEqual(1500);
  }, 25000);
});

describe('Worker.rateLimit() manual method', () => {
  const Q = 'test-manual-ratelimit-' + Date.now();

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('rateLimit(ms) delays subsequent job processing', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const timestamps: number[] = [];
    let workerRef: any = null;

    // Add 2 jobs
    await queue.add('job-0', { i: 0 });
    await queue.add('job-1', { i: 1 });

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);
      const worker = new Worker(
        Q,
        async (job: any) => {
          timestamps.push(Date.now());
          // On first job, trigger manual rate limit of 2s
          if (timestamps.length === 1) {
            await worker.rateLimit(2000);
          }
          return 'ok';
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 1000,
          limiter: { max: 100, duration: 100000 }, // high limit so server-side won't trigger
        },
      );
      workerRef = worker;
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
    // Second job should be delayed by at least ~1500ms (allowing margin for the 2000ms rate limit)
    const gap = timestamps[1] - timestamps[0];
    expect(gap).toBeGreaterThanOrEqual(1500);
  }, 20000);
});

describe('Worker.RateLimitError in processor', () => {
  const Q = 'test-ratelimit-error-' + Date.now();

  afterAll(async () => {
    await flushQueue(Q);
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

    // The job should have been processed twice: once rejected, once completed
    expect(attempts).toBe(2);
  }, 20000);
});
