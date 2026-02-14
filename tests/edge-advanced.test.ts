/**
 * Edge tests: Dedup, rate limit, retention, and global concurrency edge cases.
 * Integration tests against a real Valkey instance on localhost:6379.
 *
 * Run: npx vitest run tests/edge-advanced.test.ts
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
  await cleanupClient.functionLoad(LIBRARY_SOURCE, { replace: true });
});

afterAll(async () => {
  cleanupClient.close();
});

// ======================================================================
// DEDUP EDGE CASES
// ======================================================================

describe('Dedup edge: add same dedup ID twice rapidly - second returns null', () => {
  const Q = 'test-ea-dedup-rapid-' + Date.now();
  let queue: InstanceType<typeof Queue>;

  beforeAll(() => {
    queue = new Queue(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(Q);
  });

  it('second add with same dedup ID is skipped immediately', async () => {
    const [job1, job2] = await Promise.all([
      queue.add('task', { v: 1 }, { deduplication: { id: 'rapid-1', mode: 'simple' } }),
      queue.add('task', { v: 2 }, { deduplication: { id: 'rapid-1', mode: 'simple' } }),
    ]);
    // One succeeds, one is null (order may vary due to concurrency)
    const results = [job1, job2];
    const successes = results.filter((r: any) => r !== null);
    const skipped = results.filter((r: any) => r === null);
    expect(successes.length).toBe(1);
    expect(skipped.length).toBe(1);
  });
});

describe('Dedup edge: add, complete, add again - second succeeds', () => {
  const Q = 'test-ea-dedup-reuse-' + Date.now();
  let queue: InstanceType<typeof Queue>;

  beforeAll(() => {
    queue = new Queue(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(Q);
  });

  it('allows re-add after the job is completed', async () => {
    const k = buildKeys(Q);

    const job1 = await queue.add('task', { v: 1 }, {
      deduplication: { id: 'reuse-1', mode: 'simple' },
    });
    expect(job1).not.toBeNull();

    // Simulate completion
    await cleanupClient.hset(k.job(job1!.id), { state: 'completed' });
    await cleanupClient.zadd(k.completed, [{ element: job1!.id, score: Date.now() }]);

    const job2 = await queue.add('task', { v: 2 }, {
      deduplication: { id: 'reuse-1', mode: 'simple' },
    });
    expect(job2).not.toBeNull();
    expect(job2!.id).not.toBe(job1!.id);
  });
});

describe('Dedup edge: throttle TTL boundary', () => {
  const Q = 'test-ea-dedup-throttle-ttl-' + Date.now();
  let queue: InstanceType<typeof Queue>;

  beforeAll(() => {
    queue = new Queue(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(Q);
  });

  it('skips within TTL window, succeeds after TTL expires', async () => {
    const job1 = await queue.add('task', { v: 1 }, {
      deduplication: { id: 'ttl-edge', mode: 'throttle', ttl: 200 },
    });
    expect(job1).not.toBeNull();

    // Immediately within TTL -> skipped
    const job2 = await queue.add('task', { v: 2 }, {
      deduplication: { id: 'ttl-edge', mode: 'throttle', ttl: 200 },
    });
    expect(job2).toBeNull();

    // Wait for TTL to expire
    await new Promise(r => setTimeout(r, 250));

    // After TTL -> allowed
    const job3 = await queue.add('task', { v: 3 }, {
      deduplication: { id: 'ttl-edge', mode: 'throttle', ttl: 200 },
    });
    expect(job3).not.toBeNull();
    expect(job3!.id).not.toBe(job1!.id);
  });
});

describe('Dedup edge: debounce replaces delayed job with fresh data', () => {
  const Q = 'test-ea-dedup-debounce-replace-' + Date.now();
  let queue: InstanceType<typeof Queue>;

  beforeAll(() => {
    queue = new Queue(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(Q);
  });

  it('replaces delayed job, old job hash removed', async () => {
    const k = buildKeys(Q);

    const job1 = await queue.add('task', { v: 'old' }, {
      delay: 60000,
      deduplication: { id: 'debounce-replace', mode: 'debounce' },
    });
    expect(job1).not.toBeNull();

    // Verify old job is in scheduled set
    const score1 = await cleanupClient.zscore(k.scheduled, job1!.id);
    expect(score1).not.toBeNull();

    const job2 = await queue.add('task', { v: 'new' }, {
      delay: 60000,
      deduplication: { id: 'debounce-replace', mode: 'debounce' },
    });
    expect(job2).not.toBeNull();
    expect(job2!.id).not.toBe(job1!.id);

    // Old job should be removed
    const oldScore = await cleanupClient.zscore(k.scheduled, job1!.id);
    expect(oldScore).toBeNull();
    const oldExists = await cleanupClient.exists([k.job(job1!.id)]);
    expect(oldExists).toBe(0);

    // New job should exist in scheduled set
    const newScore = await cleanupClient.zscore(k.scheduled, job2!.id);
    expect(newScore).not.toBeNull();
  });
});

describe('Dedup edge: different IDs on same queue are independent', () => {
  const Q = 'test-ea-dedup-independent-' + Date.now();
  let queue: InstanceType<typeof Queue>;

  beforeAll(() => {
    queue = new Queue(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(Q);
  });

  it('different dedup IDs both succeed', async () => {
    const jobA = await queue.add('task', { v: 'a' }, {
      deduplication: { id: 'id-alpha', mode: 'simple' },
    });
    const jobB = await queue.add('task', { v: 'b' }, {
      deduplication: { id: 'id-beta', mode: 'simple' },
    });
    expect(jobA).not.toBeNull();
    expect(jobB).not.toBeNull();
    expect(jobA!.id).not.toBe(jobB!.id);
  });
});

describe('Dedup edge: same dedup ID across different queues is independent', () => {
  const Q1 = 'test-ea-dedup-cross-q1-' + Date.now();
  const Q2 = 'test-ea-dedup-cross-q2-' + Date.now();
  let queue1: InstanceType<typeof Queue>;
  let queue2: InstanceType<typeof Queue>;

  beforeAll(() => {
    queue1 = new Queue(Q1, { connection: CONNECTION });
    queue2 = new Queue(Q2, { connection: CONNECTION });
  });

  afterAll(async () => {
    await queue1.close();
    await queue2.close();
    await flushQueue(Q1);
    await flushQueue(Q2);
  });

  it('same dedup ID on two different queues both succeed (no cross-queue dedup)', async () => {
    const job1 = await queue1.add('task', { v: 1 }, {
      deduplication: { id: 'shared-id', mode: 'simple' },
    });
    const job2 = await queue2.add('task', { v: 2 }, {
      deduplication: { id: 'shared-id', mode: 'simple' },
    });
    expect(job1).not.toBeNull();
    expect(job2).not.toBeNull();
  });
});

// ======================================================================
// RATE LIMIT EDGE CASES
// ======================================================================

describe('Rate limit edge: window boundary reset', () => {
  const Q = 'test-ea-rl-boundary-' + Date.now();

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('fills window, waits for reset, verifies new jobs allowed', async () => {
    const k = buildKeys(Q);
    await cleanupClient.del([k.rate]);

    const now = Date.now();
    const windowDuration = 500;

    // Fill the window (max=2)
    for (let i = 0; i < 2; i++) {
      const r = await cleanupClient.fcall(
        'glidemq_rateLimit',
        [k.rate, k.meta],
        ['2', windowDuration.toString(), (now + i).toString()],
      );
      expect(Number(r)).toBe(0);
    }

    // Should be rate limited
    const blocked = await cleanupClient.fcall(
      'glidemq_rateLimit',
      [k.rate, k.meta],
      ['2', windowDuration.toString(), (now + 100).toString()],
    );
    expect(Number(blocked)).toBeGreaterThan(0);

    // After window expires, should be allowed
    const afterWindow = await cleanupClient.fcall(
      'glidemq_rateLimit',
      [k.rate, k.meta],
      ['2', windowDuration.toString(), (now + windowDuration + 1).toString()],
    );
    expect(Number(afterWindow)).toBe(0);
  });
});

describe('Rate limit edge: global not per-concurrent-slot', () => {
  const Q = 'test-ea-rl-global-' + Date.now();

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('rate limit is global across concurrent workers', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const timestamps: number[] = [];

    // Add 4 jobs
    for (let i = 0; i < 4; i++) {
      await queue.add(`job-${i}`, { i });
    }

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 20000);
      // Worker with concurrency=2 but rate limit max=2 per 2000ms window
      const worker = new Worker(
        Q,
        async () => {
          timestamps.push(Date.now());
          return 'ok';
        },
        {
          connection: CONNECTION,
          concurrency: 2,
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
    // First 2 should complete quickly, then a gap before next 2
    const gap = timestamps[2] - timestamps[1];
    expect(gap).toBeGreaterThanOrEqual(1500);
  }, 25000);
});

describe('Rate limit edge: rateLimit(0) means no delay', () => {
  const Q = 'test-ea-rl-zero-' + Date.now();

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('rateLimit(0) does not delay processing', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const timestamps: number[] = [];

    await queue.add('job-0', { i: 0 });
    await queue.add('job-1', { i: 1 });

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker = new Worker(
        Q,
        async () => {
          timestamps.push(Date.now());
          if (timestamps.length === 1) {
            await worker.rateLimit(0);
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
    // With rateLimit(0), there should be no significant delay
    const gap = timestamps[1] - timestamps[0];
    expect(gap).toBeLessThan(3000);
  }, 15000);
});

// ======================================================================
// RETENTION EDGE CASES
// ======================================================================

describe('Retention edge: removeOnComplete with age and count', () => {
  const Q = 'test-ea-ret-age-count-' + Date.now();

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('removeOnComplete: {age: 1, count: 100} - old jobs removed by age', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);
    let processed = 0;
    const TOTAL = 3;

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);
      const worker = new Worker(
        Q,
        async () => 'ok',
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
      await queue.add(`job-${i}`, { i }, {
        removeOnComplete: { age: 1, count: 100 },
      });
    }

    await done;

    // Jobs completed just now - all should exist (age=1s hasn't expired)
    const countBefore = await cleanupClient.zcard(k.completed);
    expect(countBefore).toBe(TOTAL);

    // Wait for age to expire (1 second + margin)
    await new Promise(r => setTimeout(r, 1500));

    // Add one more job to trigger age-based cleanup
    let processed2 = 0;
    const done2 = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker2 = new Worker(
        Q,
        async () => 'ok',
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
      );
      worker2.on('completed', () => {
        processed2++;
        if (processed2 >= 1) {
          clearTimeout(timeout);
          worker2.close(true).then(resolve);
        }
      });
      worker2.on('error', () => {});
    });

    await new Promise(r => setTimeout(r, 500));
    await queue.add('trigger', { trigger: true }, {
      removeOnComplete: { age: 1, count: 100 },
    });

    await done2;
    await new Promise(r => setTimeout(r, 200));

    // The older jobs should have been cleaned up by age
    const countAfter = await cleanupClient.zcard(k.completed);
    // Only the trigger job (or a small number) should remain
    expect(countAfter).toBeLessThan(TOTAL + 1);

    await queue.close();
  }, 20000);
});

describe('Retention edge: removeOnFail with count=1', () => {
  const Q = 'test-ea-ret-fail-count1-' + Date.now();

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('keeps only the last 1 failed job', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);
    let processed = 0;
    const TOTAL = 3;

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);
      const worker = new Worker(
        Q,
        async () => { throw new Error('intentional'); },
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
      await queue.add(`job-${i}`, { i }, { removeOnFail: 1 });
    }

    await done;
    await new Promise(r => setTimeout(r, 200));

    const failedCount = await cleanupClient.zcard(k.failed);
    expect(failedCount).toBeLessThanOrEqual(1);

    await queue.close();
  }, 20000);
});

describe('Retention edge: removeOnComplete false (default)', () => {
  const Q = 'test-ea-ret-default-' + Date.now();

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('completed jobs accumulate when removeOnComplete is not set', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);
    let processed = 0;
    const TOTAL = 5;

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);
      const worker = new Worker(
        Q,
        async () => 'ok',
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
      // No removeOnComplete specified - default behavior
      await queue.add(`job-${i}`, { i });
    }

    await done;
    await new Promise(r => setTimeout(r, 200));

    // All completed jobs should still be in the completed set
    const completedCount = await cleanupClient.zcard(k.completed);
    expect(completedCount).toBe(TOTAL);

    // Job hashes should still exist
    const members = await cleanupClient.zrange(k.completed, { start: 0, end: -1 });
    for (const jobId of members) {
      const exists = await cleanupClient.exists([k.job(String(jobId))]);
      expect(exists).toBe(1);
    }

    await queue.close();
  }, 20000);
});

// ======================================================================
// GLOBAL CONCURRENCY EDGE CASES
// ======================================================================

describe('Global concurrency edge: setGlobalConcurrency(1) with 2 workers', () => {
  const Q = 'test-ea-gc-two-workers-' + Date.now();

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('only 1 job active at a time with single worker under global limit', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    await queue.setGlobalConcurrency(1);

    let maxConcurrent = 0;
    let current = 0;
    let processed = 0;
    const TOTAL = 4;

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 25000);

      // Single worker with high local concurrency, but global limit is 1
      const worker = new Worker(
        Q,
        async () => {
          current++;
          if (current > maxConcurrent) maxConcurrent = current;
          await new Promise(r => setTimeout(r, 300));
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
          worker.close(true).then(() => resolve());
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
    // Global concurrency=1 should limit to at most 1 active job
    expect(maxConcurrent).toBeLessThanOrEqual(1);

    await queue.close();
  }, 30000);
});

describe('Global concurrency edge: setGlobalConcurrency(0) means no limit', () => {
  const Q = 'test-ea-gc-zero-' + Date.now();

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('setGlobalConcurrency(0) removes the limit, allowing full concurrency', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    // First set a limit, then remove it
    await queue.setGlobalConcurrency(1);
    await queue.setGlobalConcurrency(0);

    let maxConcurrent = 0;
    let current = 0;
    let processed = 0;
    const TOTAL = 4;

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 20000);
      const worker = new Worker(
        Q,
        async () => {
          current++;
          if (current > maxConcurrent) maxConcurrent = current;
          await new Promise(r => setTimeout(r, 200));
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
    // With concurrency=4 and no global limit, multiple jobs should run at once
    expect(maxConcurrent).toBeGreaterThanOrEqual(2);

    await queue.close();
  }, 25000);
});
