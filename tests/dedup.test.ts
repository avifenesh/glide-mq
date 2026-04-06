/**
 * Deduplication integration tests against a real Valkey instance.
 * Requires: valkey-server running on localhost:6379 and cluster on :7000-7005
 *
 * Run: npx vitest run tests/dedup.test.ts
 */
import { it, expect, beforeAll, afterAll } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

describeEachMode('Deduplication - simple mode', (CONNECTION) => {
  const Q = 'test-dedup-simple-' + Date.now();
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

  it('first add succeeds, duplicate is skipped while job is active', async () => {
    const job1 = await queue.add(
      'task',
      { v: 1 },
      {
        deduplication: { id: 'unique-1', mode: 'simple' },
      },
    );
    expect(job1).not.toBeNull();
    expect(job1!.id).toBeTruthy();

    const job2 = await queue.add(
      'task',
      { v: 2 },
      {
        deduplication: { id: 'unique-1', mode: 'simple' },
      },
    );
    expect(job2).toBeNull();
  });

  it('allows re-add after job is completed', async () => {
    const k = buildKeys(Q);

    const job1 = await queue.add(
      'task',
      { v: 10 },
      {
        deduplication: { id: 'complete-test', mode: 'simple' },
      },
    );
    expect(job1).not.toBeNull();

    await cleanupClient.hset(k.job(job1!.id), { state: 'completed' });
    await cleanupClient.zadd(k.completed, [{ element: job1!.id, score: Date.now() }]);

    const job2 = await queue.add(
      'task',
      { v: 11 },
      {
        deduplication: { id: 'complete-test', mode: 'simple' },
      },
    );
    expect(job2).not.toBeNull();
    expect(job2!.id).not.toBe(job1!.id);
  });

  it('allows re-add after job is failed', async () => {
    const k = buildKeys(Q);

    const job1 = await queue.add(
      'task',
      { v: 20 },
      {
        deduplication: { id: 'fail-test', mode: 'simple' },
      },
    );
    expect(job1).not.toBeNull();

    await cleanupClient.hset(k.job(job1!.id), { state: 'failed' });

    const job2 = await queue.add(
      'task',
      { v: 21 },
      {
        deduplication: { id: 'fail-test', mode: 'simple' },
      },
    );
    expect(job2).not.toBeNull();
    expect(job2!.id).not.toBe(job1!.id);
  });

  it('different dedup ids are independent', async () => {
    const jobA = await queue.add(
      'task',
      { v: 'a' },
      {
        deduplication: { id: 'id-a', mode: 'simple' },
      },
    );
    const jobB = await queue.add(
      'task',
      { v: 'b' },
      {
        deduplication: { id: 'id-b', mode: 'simple' },
      },
    );
    expect(jobA).not.toBeNull();
    expect(jobB).not.toBeNull();
    expect(jobA!.id).not.toBe(jobB!.id);
  });
});

describeEachMode('Deduplication - throttle mode', (CONNECTION) => {
  const Q = 'test-dedup-throttle-' + Date.now();
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

  it('skips when within TTL window', async () => {
    const job1 = await queue.add(
      'task',
      { v: 1 },
      {
        deduplication: { id: 'throttle-1', mode: 'throttle', ttl: 60000 },
      },
    );
    expect(job1).not.toBeNull();

    const job2 = await queue.add(
      'task',
      { v: 2 },
      {
        deduplication: { id: 'throttle-1', mode: 'throttle', ttl: 60000 },
      },
    );
    expect(job2).toBeNull();
  });

  it('allows add after TTL expires', async () => {
    const job1 = await queue.add(
      'task',
      { v: 10 },
      {
        deduplication: { id: 'throttle-expire', mode: 'throttle', ttl: 100 },
      },
    );
    expect(job1).not.toBeNull();

    await new Promise((r) => setTimeout(r, 150));

    const job2 = await queue.add(
      'task',
      { v: 11 },
      {
        deduplication: { id: 'throttle-expire', mode: 'throttle', ttl: 100 },
      },
    );
    expect(job2).not.toBeNull();
    expect(job2!.id).not.toBe(job1!.id);
  });

  it('first add without prior entry always succeeds', async () => {
    const job = await queue.add(
      'task',
      { v: 'first' },
      {
        deduplication: { id: 'throttle-fresh', mode: 'throttle', ttl: 5000 },
      },
    );
    expect(job).not.toBeNull();
  });
});

describeEachMode('Deduplication - debounce mode', (CONNECTION) => {
  const Q = 'test-dedup-debounce-' + Date.now();
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

  it('replaces a delayed job with fresh data', async () => {
    const k = buildKeys(Q);

    const job1 = await queue.add(
      'task',
      { v: 'old' },
      {
        delay: 60000,
        deduplication: { id: 'debounce-1', mode: 'debounce' },
      },
    );
    expect(job1).not.toBeNull();

    const score1 = await cleanupClient.zscore(k.scheduled, job1!.id);
    expect(score1).not.toBeNull();

    const job2 = await queue.add(
      'task',
      { v: 'new' },
      {
        delay: 60000,
        deduplication: { id: 'debounce-1', mode: 'debounce' },
      },
    );
    expect(job2).not.toBeNull();
    expect(job2!.id).not.toBe(job1!.id);

    const oldScore = await cleanupClient.zscore(k.scheduled, job1!.id);
    expect(oldScore).toBeNull();

    const oldExists = await cleanupClient.exists([k.job(job1!.id)]);
    expect(oldExists).toBe(0);

    const newScore = await cleanupClient.zscore(k.scheduled, job2!.id);
    expect(newScore).not.toBeNull();
  });

  it('skips when existing job is in waiting state (not delayed)', async () => {
    const job1 = await queue.add(
      'task',
      { v: 'waiting' },
      {
        deduplication: { id: 'debounce-waiting', mode: 'debounce' },
      },
    );
    expect(job1).not.toBeNull();

    const job2 = await queue.add(
      'task',
      { v: 'retry' },
      {
        deduplication: { id: 'debounce-waiting', mode: 'debounce' },
      },
    );
    expect(job2).toBeNull();
  });

  it('allows add after previous job completed', async () => {
    const k = buildKeys(Q);

    const job1 = await queue.add(
      'task',
      { v: 'first' },
      {
        delay: 60000,
        deduplication: { id: 'debounce-completed', mode: 'debounce' },
      },
    );
    expect(job1).not.toBeNull();

    await cleanupClient.hset(k.job(job1!.id), { state: 'completed' });
    await cleanupClient.zrem(k.scheduled, [job1!.id]);

    const job2 = await queue.add(
      'task',
      { v: 'second' },
      {
        delay: 60000,
        deduplication: { id: 'debounce-completed', mode: 'debounce' },
      },
    );
    expect(job2).not.toBeNull();
    expect(job2!.id).not.toBe(job1!.id);
  });
});

describeEachMode('Deduplication - debounce + ordering (skip marker)', (CONNECTION) => {
  const Q = 'test-dedup-debounce-ordering-' + Date.now();
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

  it('sets skip marker when debounce deletes an ordered delayed job', async () => {
    const k = buildKeys(Q);
    const groupKey = 'skip-test';

    const job1 = await queue.add(
      'task',
      { v: 'old' },
      {
        delay: 60000,
        ordering: { key: groupKey, concurrency: 5 },
        deduplication: { id: 'deb-ord-1', mode: 'debounce' },
      },
    );
    expect(job1).not.toBeNull();

    const seq1 = await cleanupClient.hget(k.job(job1!.id), 'orderingSeq');
    expect(Number(seq1)).toBe(1);

    // Debounce: replace job1 with job2
    const job2 = await queue.add(
      'task',
      { v: 'new' },
      {
        delay: 60000,
        ordering: { key: groupKey, concurrency: 5 },
        deduplication: { id: 'deb-ord-1', mode: 'debounce' },
      },
    );
    expect(job2).not.toBeNull();
    expect(job2!.id).not.toBe(job1!.id);

    // Old job should be gone
    const oldExists = await cleanupClient.exists([k.job(job1!.id)]);
    expect(oldExists).toBe(0);

    // Skip marker should exist for seq=1
    const skipMarker = await cleanupClient.hget(k.group(groupKey), 'skip:1');
    expect(skipMarker).toBe('1');
  });

  it('resolves skip chain from multiple consecutive debounces', async () => {
    const k = buildKeys(Q);
    const groupKey = 'chain-test';

    // Create and debounce 3 times
    const job1 = await queue.add(
      'task',
      { v: 1 },
      {
        delay: 60000,
        ordering: { key: groupKey, concurrency: 5 },
        deduplication: { id: 'deb-chain', mode: 'debounce' },
      },
    );
    expect(job1).not.toBeNull();

    const job2 = await queue.add(
      'task',
      { v: 2 },
      {
        delay: 60000,
        ordering: { key: groupKey, concurrency: 5 },
        deduplication: { id: 'deb-chain', mode: 'debounce' },
      },
    );
    expect(job2).not.toBeNull();

    const job3 = await queue.add(
      'task',
      { v: 3 },
      {
        delay: 60000,
        ordering: { key: groupKey, concurrency: 5 },
        deduplication: { id: 'deb-chain', mode: 'debounce' },
      },
    );
    expect(job3).not.toBeNull();

    // Skip markers for seq=1 and seq=2 should exist
    const skip1 = await cleanupClient.hget(k.group(groupKey), 'skip:1');
    const skip2 = await cleanupClient.hget(k.group(groupKey), 'skip:2');
    expect(skip1).toBe('1');
    expect(skip2).toBe('1');

    // job3 should have seq=3 and be the only live job
    const seq3 = await cleanupClient.hget(k.job(job3!.id), 'orderingSeq');
    expect(Number(seq3)).toBe(3);

    // nextSeq should be 1 (not yet resolved - lazy resolution)
    const nextSeqBefore = await cleanupClient.hget(k.group(groupKey), 'nextSeq');
    expect(Number(nextSeqBefore)).toBe(1);
  });

  it('end-to-end: jobs complete without deadlock after debounce deletes an ordered job', async () => {
    // Reproduction of issue #206:
    // debounce deletes a prioritized ordered job, creating a nextSeq gap.
    // Without the fix, all subsequent jobs deadlock.
    // With the fix (skip marker), the ordering gate resolves the gap and all jobs complete.
    const Q2 = 'test-dedup-e2e-deadlock-' + Date.now();
    const groupKey = 'e2e-group';
    const q = new Queue(Q2, { connection: CONNECTION });
    const completed: string[] = [];

    const ORD = { key: groupKey, concurrency: 2 };
    // jobA uses delay so it lands in `delayed` state (scheduled ZSet).
    // Debounce only fires on delayed/prioritized jobs - this ensures the gap is created.
    const jobA = await q.add(
      'task',
      { v: 'A' },
      {
        delay: 60000,
        ordering: ORD,
        deduplication: { id: 'deb-e2e-a', mode: 'debounce' },
      },
    );
    // jobB and jobC go directly to stream (no delay) - seq=2,3
    const jobB = await q.add(
      'task',
      { v: 'B' },
      { ordering: ORD, deduplication: { id: 'deb-e2e-b', mode: 'debounce' } },
    );
    const jobC = await q.add(
      'task',
      { v: 'C' },
      { ordering: ORD, deduplication: { id: 'deb-e2e-c', mode: 'debounce' } },
    );
    expect(jobA).not.toBeNull();

    // Debounce jobA (seq=1) before it is promoted - creates skip:1, replacement gets seq=4 (no delay)
    const jobA2 = await q.add(
      'task',
      { v: 'A-new' },
      { ordering: ORD, deduplication: { id: 'deb-e2e-a', mode: 'debounce' } },
    );
    expect(jobA2).not.toBeNull();
    expect(jobA2!.id).not.toBe(jobA!.id);

    // We now have: skip:1 on group hash, live jobs at seq=2 (B), seq=3 (C), seq=4 (A2)
    // Without fix: after processing seq=2,3, nextSeq would be stuck at 1 → A2 at seq=4 blocked forever
    // With fix: skip:1 resolved when ordering gate is reached → all jobs complete

    const allJobIds = new Set([jobB!.id, jobC!.id, jobA2!.id]);

    await new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('deadlock: not all jobs completed within timeout')), 15000);
      const worker = new Worker(
        Q2,
        async (job: any) => {
          completed.push(job.id);
          if (allJobIds.has(job.id) && completed.filter((id) => allJobIds.has(id)).length >= 3) {
            clearTimeout(timeout);
            setTimeout(() => worker.close(true).then(resolve), 200);
          }
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 2, blockTimeout: 500 },
      );
      worker.on('error', () => {});
    });

    await q.close();
    await flushQueue(cleanupClient, Q2);

    expect(completed.filter((id) => allJobIds.has(id))).toHaveLength(3);
  }, 20000);
});

describeEachMode('Deduplication - dedup hash tracking', (CONNECTION) => {
  const Q = 'test-dedup-hash-' + Date.now();
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

  it('stores dedup entry in hash with jobId:timestamp format', async () => {
    const k = buildKeys(Q);
    const before = Date.now();

    const job = await queue.add(
      'task',
      { v: 1 },
      {
        deduplication: { id: 'track-1', mode: 'simple' },
      },
    );
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

    const job1 = await queue.add(
      'task',
      { v: 'a' },
      {
        delay: 60000,
        deduplication: { id: 'replace-track', mode: 'debounce' },
      },
    );
    expect(job1).not.toBeNull();

    const entry1 = String(await cleanupClient.hget(k.dedup, 'replace-track'));
    expect(entry1.startsWith(job1!.id + ':')).toBe(true);

    const job2 = await queue.add(
      'task',
      { v: 'b' },
      {
        delay: 60000,
        deduplication: { id: 'replace-track', mode: 'debounce' },
      },
    );
    expect(job2).not.toBeNull();

    const entry2 = String(await cleanupClient.hget(k.dedup, 'replace-track'));
    expect(entry2.startsWith(job2!.id + ':')).toBe(true);
    expect(job2!.id).not.toBe(job1!.id);
  });
});

describeEachMode('Deduplication - default mode is simple', (CONNECTION) => {
  const Q = 'test-dedup-default-' + Date.now();
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

  it('uses simple mode when mode is not specified', async () => {
    const job1 = await queue.add(
      'task',
      { v: 1 },
      {
        deduplication: { id: 'default-mode' },
      },
    );
    expect(job1).not.toBeNull();

    const job2 = await queue.add(
      'task',
      { v: 2 },
      {
        deduplication: { id: 'default-mode' },
      },
    );
    expect(job2).toBeNull();
  });
});
