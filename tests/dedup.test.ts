/**
 * Deduplication integration tests against a real Valkey instance.
 * Requires: valkey-server running on localhost:6379
 *
 * Run: npx vitest run tests/dedup.test.ts
 */
import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';

const { GlideClient } = require('speedkey') as typeof import('speedkey');
const { Queue } = require('../dist/queue') as typeof import('../src/queue');
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
  // Force-reload the library to ensure latest functions (including glidemq_dedup) are available
  await cleanupClient.functionLoad(LIBRARY_SOURCE, { replace: true });
});

afterAll(async () => {
  cleanupClient.close();
});

describe('Deduplication - simple mode', () => {
  const Q = 'test-dedup-simple-' + Date.now();
  let queue: InstanceType<typeof Queue>;

  beforeAll(() => {
    queue = new Queue(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(Q);
  });

  it('first add succeeds, duplicate is skipped while job is active', async () => {
    const job1 = await queue.add('task', { v: 1 }, {
      deduplication: { id: 'unique-1', mode: 'simple' },
    });
    expect(job1).not.toBeNull();
    expect(job1!.id).toBeTruthy();

    // Second add with same dedup id should be skipped
    const job2 = await queue.add('task', { v: 2 }, {
      deduplication: { id: 'unique-1', mode: 'simple' },
    });
    expect(job2).toBeNull();
  });

  it('allows re-add after job is completed', async () => {
    const k = buildKeys(Q);

    const job1 = await queue.add('task', { v: 10 }, {
      deduplication: { id: 'complete-test', mode: 'simple' },
    });
    expect(job1).not.toBeNull();

    // Manually mark the job as completed in its hash
    await cleanupClient.hset(k.job(job1!.id), { state: 'completed' });
    await cleanupClient.zadd(k.completed, [{ element: job1!.id, score: Date.now() }]);

    // Now adding with same dedup id should succeed
    const job2 = await queue.add('task', { v: 11 }, {
      deduplication: { id: 'complete-test', mode: 'simple' },
    });
    expect(job2).not.toBeNull();
    expect(job2!.id).not.toBe(job1!.id);
  });

  it('allows re-add after job is failed', async () => {
    const k = buildKeys(Q);

    const job1 = await queue.add('task', { v: 20 }, {
      deduplication: { id: 'fail-test', mode: 'simple' },
    });
    expect(job1).not.toBeNull();

    // Manually mark the job as failed
    await cleanupClient.hset(k.job(job1!.id), { state: 'failed' });

    // Now adding with same dedup id should succeed
    const job2 = await queue.add('task', { v: 21 }, {
      deduplication: { id: 'fail-test', mode: 'simple' },
    });
    expect(job2).not.toBeNull();
    expect(job2!.id).not.toBe(job1!.id);
  });

  it('different dedup ids are independent', async () => {
    const jobA = await queue.add('task', { v: 'a' }, {
      deduplication: { id: 'id-a', mode: 'simple' },
    });
    const jobB = await queue.add('task', { v: 'b' }, {
      deduplication: { id: 'id-b', mode: 'simple' },
    });
    expect(jobA).not.toBeNull();
    expect(jobB).not.toBeNull();
    expect(jobA!.id).not.toBe(jobB!.id);
  });
});

describe('Deduplication - throttle mode', () => {
  const Q = 'test-dedup-throttle-' + Date.now();
  let queue: InstanceType<typeof Queue>;

  beforeAll(() => {
    queue = new Queue(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(Q);
  });

  it('skips when within TTL window', async () => {
    const job1 = await queue.add('task', { v: 1 }, {
      deduplication: { id: 'throttle-1', mode: 'throttle', ttl: 60000 },
    });
    expect(job1).not.toBeNull();

    // Immediate second add should be throttled
    const job2 = await queue.add('task', { v: 2 }, {
      deduplication: { id: 'throttle-1', mode: 'throttle', ttl: 60000 },
    });
    expect(job2).toBeNull();
  });

  it('allows add after TTL expires', async () => {
    const k = buildKeys(Q);

    const job1 = await queue.add('task', { v: 10 }, {
      deduplication: { id: 'throttle-expire', mode: 'throttle', ttl: 100 },
    });
    expect(job1).not.toBeNull();

    // Wait for TTL to expire
    await new Promise(r => setTimeout(r, 150));

    const job2 = await queue.add('task', { v: 11 }, {
      deduplication: { id: 'throttle-expire', mode: 'throttle', ttl: 100 },
    });
    expect(job2).not.toBeNull();
    expect(job2!.id).not.toBe(job1!.id);
  });

  it('first add without prior entry always succeeds', async () => {
    const job = await queue.add('task', { v: 'first' }, {
      deduplication: { id: 'throttle-fresh', mode: 'throttle', ttl: 5000 },
    });
    expect(job).not.toBeNull();
  });
});

describe('Deduplication - debounce mode', () => {
  const Q = 'test-dedup-debounce-' + Date.now();
  let queue: InstanceType<typeof Queue>;

  beforeAll(() => {
    queue = new Queue(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(Q);
  });

  it('replaces a delayed job with fresh data', async () => {
    const k = buildKeys(Q);

    // Add a delayed job
    const job1 = await queue.add('task', { v: 'old' }, {
      delay: 60000,
      deduplication: { id: 'debounce-1', mode: 'debounce' },
    });
    expect(job1).not.toBeNull();

    // Verify it's in the scheduled ZSet
    const score1 = await cleanupClient.zscore(k.scheduled, job1!.id);
    expect(score1).not.toBeNull();

    // Add again with same dedup id - should replace the old job
    const job2 = await queue.add('task', { v: 'new' }, {
      delay: 60000,
      deduplication: { id: 'debounce-1', mode: 'debounce' },
    });
    expect(job2).not.toBeNull();
    expect(job2!.id).not.toBe(job1!.id);

    // Old job should be removed from scheduled ZSet
    const oldScore = await cleanupClient.zscore(k.scheduled, job1!.id);
    expect(oldScore).toBeNull();

    // Old job hash should be deleted
    const oldExists = await cleanupClient.exists([k.job(job1!.id)]);
    expect(oldExists).toBe(0);

    // New job should be in scheduled ZSet
    const newScore = await cleanupClient.zscore(k.scheduled, job2!.id);
    expect(newScore).not.toBeNull();
  });

  it('skips when existing job is in waiting state (not delayed)', async () => {
    // Add an immediate (non-delayed) job
    const job1 = await queue.add('task', { v: 'waiting' }, {
      deduplication: { id: 'debounce-waiting', mode: 'debounce' },
    });
    expect(job1).not.toBeNull();

    // Try to debounce it - should be skipped since job is in waiting state
    const job2 = await queue.add('task', { v: 'retry' }, {
      deduplication: { id: 'debounce-waiting', mode: 'debounce' },
    });
    expect(job2).toBeNull();
  });

  it('allows add after previous job completed', async () => {
    const k = buildKeys(Q);

    const job1 = await queue.add('task', { v: 'first' }, {
      delay: 60000,
      deduplication: { id: 'debounce-completed', mode: 'debounce' },
    });
    expect(job1).not.toBeNull();

    // Manually mark as completed
    await cleanupClient.hset(k.job(job1!.id), { state: 'completed' });
    await cleanupClient.zrem(k.scheduled, [job1!.id]);

    const job2 = await queue.add('task', { v: 'second' }, {
      delay: 60000,
      deduplication: { id: 'debounce-completed', mode: 'debounce' },
    });
    expect(job2).not.toBeNull();
    expect(job2!.id).not.toBe(job1!.id);
  });
});

describe('Deduplication - dedup hash tracking', () => {
  const Q = 'test-dedup-hash-' + Date.now();
  let queue: InstanceType<typeof Queue>;

  beforeAll(() => {
    queue = new Queue(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(Q);
  });

  it('stores dedup entry in hash with jobId:timestamp format', async () => {
    const k = buildKeys(Q);
    const before = Date.now();

    const job = await queue.add('task', { v: 1 }, {
      deduplication: { id: 'track-1', mode: 'simple' },
    });
    expect(job).not.toBeNull();

    const entry = await cleanupClient.hget(k.dedup, 'track-1');
    expect(entry).not.toBeNull();

    const entryStr = String(entry);
    const [storedJobId, storedTs] = entryStr.split(':');
    expect(storedJobId).toBe(job!.id);

    const ts = parseInt(storedTs, 10);
    expect(ts).toBeGreaterThanOrEqual(before);
    expect(ts).toBeLessThanOrEqual(Date.now());
  });

  it('updates dedup entry when new job replaces old one', async () => {
    const k = buildKeys(Q);

    const job1 = await queue.add('task', { v: 'a' }, {
      delay: 60000,
      deduplication: { id: 'replace-track', mode: 'debounce' },
    });
    expect(job1).not.toBeNull();

    const entry1 = String(await cleanupClient.hget(k.dedup, 'replace-track'));
    expect(entry1.startsWith(job1!.id + ':')).toBe(true);

    // Replace via debounce
    const job2 = await queue.add('task', { v: 'b' }, {
      delay: 60000,
      deduplication: { id: 'replace-track', mode: 'debounce' },
    });
    expect(job2).not.toBeNull();

    const entry2 = String(await cleanupClient.hget(k.dedup, 'replace-track'));
    expect(entry2.startsWith(job2!.id + ':')).toBe(true);
    expect(job2!.id).not.toBe(job1!.id);
  });
});

describe('Deduplication - default mode is simple', () => {
  const Q = 'test-dedup-default-' + Date.now();
  let queue: InstanceType<typeof Queue>;

  beforeAll(() => {
    queue = new Queue(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(Q);
  });

  it('uses simple mode when mode is not specified', async () => {
    const job1 = await queue.add('task', { v: 1 }, {
      deduplication: { id: 'default-mode' },
    });
    expect(job1).not.toBeNull();

    // Should be skipped (simple mode behavior)
    const job2 = await queue.add('task', { v: 2 }, {
      deduplication: { id: 'default-mode' },
    });
    expect(job2).toBeNull();
  });
});
