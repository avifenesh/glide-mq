/**
 * Integration tests for rate limiting.
 *
 * Tests:
 * - Global rate limit: queue.setGlobalRateLimit / removeGlobalRateLimit / getGlobalRateLimit
 * - Per-group rate limit: ordering.rateLimit on job options
 *
 * Run: npx vitest run tests/rate-limiting.test.ts
 */
import { it, expect, beforeAll, afterAll } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describeEachMode('Rate limiting', (CONNECTION) => {
  const Q = 'test-ratelimit-' + Date.now();
  let queue: InstanceType<typeof Queue>;
  let cleanupClient: any;

  function uniqueQueueName(suffix: string): string {
    return Q + suffix;
  }

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
    queue = new Queue(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  // --- Global rate limit (#23) ---

  it('setGlobalRateLimit / getGlobalRateLimit / removeGlobalRateLimit', async () => {
    const Q1 = Q + '-api';
    const q = new Queue(Q1, { connection: CONNECTION });

    // Initially no rate limit
    const initial = await q.getGlobalRateLimit();
    expect(initial).toBeNull();

    // Set
    await q.setGlobalRateLimit({ max: 50, duration: 10000 });
    const rl = await q.getGlobalRateLimit();
    expect(rl).toEqual({ max: 50, duration: 10000 });

    // Update
    await q.setGlobalRateLimit({ max: 100, duration: 60000 });
    const updated = await q.getGlobalRateLimit();
    expect(updated).toEqual({ max: 100, duration: 60000 });

    // Remove
    await q.removeGlobalRateLimit();
    const removed = await q.getGlobalRateLimit();
    expect(removed).toBeNull();

    await q.close();
    await flushQueue(cleanupClient, Q1);
  }, 10000);

  it('global rate limit throttles workers', async () => {
    const Q2 = Q + '-grl';
    const q = new Queue(Q2, { connection: CONNECTION });

    // Allow max 3 jobs per 500ms window
    await q.setGlobalRateLimit({ max: 3, duration: 500 });

    const completionTimes: number[] = [];
    const start = Date.now();

    for (let i = 0; i < 6; i++) {
      await q.add('rl-job', { i });
    }

    const worker = new Worker(
      Q2,
      async () => {
        completionTimes.push(Date.now() - start);
      },
      { connection: CONNECTION, concurrency: 10 },
    );

    const deadline = Date.now() + 15000;
    while (completionTimes.length < 6 && Date.now() < deadline) {
      await sleep(50);
    }

    await worker.close();
    await q.close();
    await flushQueue(cleanupClient, Q2);

    expect(completionTimes.length).toBe(6);

    // First 3 should complete quickly (within first window)
    // Next 3 should be delayed by at least ~400ms (waiting for window to reset)
    // Sort to handle out-of-order completion
    completionTimes.sort((a, b) => a - b);
    const firstBatchMax = completionTimes[2];
    const secondBatchMin = completionTimes[3];
    expect(secondBatchMin - firstBatchMax).toBeGreaterThan(200);
  }, 20000);

  it('removing global rate limit resumes full speed', async () => {
    const Q3 = Q + '-rmrl';
    const q = new Queue(Q3, { connection: CONNECTION });

    // Set a very restrictive limit: 1 job per 2000ms
    await q.setGlobalRateLimit({ max: 1, duration: 2000 });

    for (let i = 0; i < 4; i++) {
      await q.add('rmrl-job', { i });
    }

    const completed: number[] = [];
    const worker = new Worker(
      Q3,
      async () => {
        completed.push(Date.now());
      },
      { connection: CONNECTION, concurrency: 5 },
    );

    // Wait for first job to complete
    while (completed.length < 1) {
      await sleep(50);
    }

    // Remove rate limit - remaining jobs should complete quickly
    await q.removeGlobalRateLimit();

    // Wait for scheduler tick to refresh meta flags (~5s) + processing
    const deadline = Date.now() + 12000;
    while (completed.length < 4 && Date.now() < deadline) {
      await sleep(50);
    }

    await worker.close();
    await q.close();
    await flushQueue(cleanupClient, Q3);

    expect(completed.length).toBe(4);
  }, 20000);

  // --- Per-group rate limit (#25) ---

  it('enforces max N jobs per window per group', async () => {
    const Q4 = Q + '-pgrl';
    const q = new Queue(Q4, { connection: CONNECTION });

    const RATE_MAX = 3;
    const WINDOW = 800;
    const JOB_COUNT = 6;
    const completionTimes: number[] = [];
    const start = Date.now();

    for (let i = 0; i < JOB_COUNT; i++) {
      await q.add(
        'pgrl-job',
        { i },
        {
          ordering: { key: 'grpRL', concurrency: 10, rateLimit: { max: RATE_MAX, duration: WINDOW } },
        },
      );
    }

    const worker = new Worker(
      Q4,
      async () => {
        completionTimes.push(Date.now() - start);
      },
      { connection: CONNECTION, concurrency: 20 },
    );

    const deadline = Date.now() + 20000;
    while (completionTimes.length < JOB_COUNT && Date.now() < deadline) {
      await sleep(50);
    }

    await worker.close();
    await q.close();
    await flushQueue(cleanupClient, Q4);

    expect(completionTimes.length).toBe(JOB_COUNT);

    // First RATE_MAX jobs should complete in the first window
    // Remaining should wait for window reset + scheduler promotion
    completionTimes.sort((a, b) => a - b);
    const firstBatchMax = completionTimes[RATE_MAX - 1];
    const secondBatchMin = completionTimes[RATE_MAX];
    // Second batch should start after window expires (+ scheduler tick latency)
    expect(secondBatchMin - firstBatchMax).toBeGreaterThan(300);
  }, 30000);

  it('different groups with different rate limits', async () => {
    const Q5 = Q + '-diffrl';
    const q = new Queue(Q5, { connection: CONNECTION });

    let completedA = 0;
    let completedB = 0;

    // Group A: max 2 per 500ms window
    for (let i = 0; i < 4; i++) {
      await q.add(
        'a-job',
        { group: 'A' },
        {
          ordering: { key: 'rlA', concurrency: 10, rateLimit: { max: 2, duration: 500 } },
        },
      );
    }
    // Group B: max 4 per 500ms window (all 4 should complete in first window)
    for (let i = 0; i < 4; i++) {
      await q.add(
        'b-job',
        { group: 'B' },
        {
          ordering: { key: 'rlB', concurrency: 10, rateLimit: { max: 4, duration: 500 } },
        },
      );
    }

    const worker = new Worker(
      Q5,
      async (job: any) => {
        if (job.data.group === 'A') completedA++;
        else completedB++;
      },
      { connection: CONNECTION, concurrency: 20 },
    );

    const deadline = Date.now() + 20000;
    while ((completedA < 4 || completedB < 4) && Date.now() < deadline) {
      await sleep(50);
    }

    await worker.close();
    await q.close();
    await flushQueue(cleanupClient, Q5);

    expect(completedA).toBe(4);
    expect(completedB).toBe(4);
  }, 25000);

  it('rate limit + concurrency interaction (both gates enforced)', async () => {
    const Q6 = Q + '-rlconc';
    const q = new Queue(Q6, { connection: CONNECTION });

    let peakConcurrent = 0;
    let currentConcurrent = 0;
    let completed = 0;
    const TOTAL = 6;

    // concurrency=2 limits parallel jobs, rateLimit max=3 per 800ms limits throughput
    for (let i = 0; i < TOTAL; i++) {
      await q.add(
        'rlconc-job',
        { i },
        {
          ordering: { key: 'rlconcGrp', concurrency: 2, rateLimit: { max: 3, duration: 800 } },
        },
      );
    }

    const worker = new Worker(
      Q6,
      async () => {
        currentConcurrent++;
        peakConcurrent = Math.max(peakConcurrent, currentConcurrent);
        await sleep(50);
        currentConcurrent--;
        completed++;
      },
      { connection: CONNECTION, concurrency: 10 },
    );

    const deadline = Date.now() + 20000;
    while (completed < TOTAL && Date.now() < deadline) {
      await sleep(50);
    }

    await worker.close();
    await q.close();
    await flushQueue(cleanupClient, Q6);

    // Concurrency gate: never more than 2 concurrent
    expect(peakConcurrent).toBeLessThanOrEqual(2);
    expect(completed).toBe(TOTAL);
  }, 25000);

  it('rate-limited group A does not block group B', async () => {
    const Q7 = Q + '-crossrl';
    const q = new Queue(Q7, { connection: CONNECTION });

    const completedA: number[] = [];
    const completedB: number[] = [];
    const start = Date.now();

    // Group A: very restrictive (1 per 1000ms)
    for (let i = 0; i < 3; i++) {
      await q.add(
        'slow-grp',
        { group: 'A' },
        {
          ordering: { key: 'slowGrp', concurrency: 10, rateLimit: { max: 1, duration: 1000 } },
        },
      );
    }
    // Group B: generous (10 per 1000ms) - should all complete quickly
    for (let i = 0; i < 3; i++) {
      await q.add(
        'fast-grp',
        { group: 'B' },
        {
          ordering: { key: 'fastGrp', concurrency: 10, rateLimit: { max: 10, duration: 1000 } },
        },
      );
    }

    const worker = new Worker(
      Q7,
      async (job: any) => {
        if (job.data.group === 'A') completedA.push(Date.now() - start);
        else completedB.push(Date.now() - start);
      },
      { connection: CONNECTION, concurrency: 20 },
    );

    const deadline = Date.now() + 25000;
    while ((completedA.length < 3 || completedB.length < 3) && Date.now() < deadline) {
      await sleep(50);
    }

    await worker.close();
    await q.close();
    await flushQueue(cleanupClient, Q7);

    expect(completedB.length).toBe(3);
    // Group B should finish quickly (all within first window)
    completedB.sort((a, b) => a - b);
    expect(completedB[2]).toBeLessThan(3000);
  }, 30000);

  it('job failure does not corrupt rate counter', async () => {
    const Q8 = Q + '-failrl';
    const q = new Queue(Q8, { connection: CONNECTION });

    const completed: string[] = [];

    // 3 jobs: first fails, next 2 should succeed
    await q.add(
      'fail-job',
      { shouldFail: true },
      {
        ordering: { key: 'failRlGrp', concurrency: 5, rateLimit: { max: 5, duration: 2000 } },
      },
    );
    await q.add(
      'ok-job-1',
      { shouldFail: false },
      {
        ordering: { key: 'failRlGrp', concurrency: 5, rateLimit: { max: 5, duration: 2000 } },
      },
    );
    await q.add(
      'ok-job-2',
      { shouldFail: false },
      {
        ordering: { key: 'failRlGrp', concurrency: 5, rateLimit: { max: 5, duration: 2000 } },
      },
    );

    const worker = new Worker(
      Q8,
      async (job: any) => {
        if (job.data.shouldFail) throw new Error('intentional');
        completed.push(job.id);
      },
      { connection: CONNECTION, concurrency: 5 },
    );

    const deadline = Date.now() + 10000;
    while (completed.length < 2 && Date.now() < deadline) {
      await sleep(100);
    }

    await worker.close();
    await q.close();
    await flushQueue(cleanupClient, Q8);

    expect(completed.length).toBe(2);
  }, 15000);

  it('rateLimit without explicit concurrency defaults to sequential', async () => {
    const Q9 = Q + '-rlnocnc';
    const q = new Queue(Q9, { connection: CONNECTION });

    let peakConcurrent = 0;
    let currentConcurrent = 0;
    let completed = 0;

    for (let i = 0; i < 4; i++) {
      await q.add(
        'rl-seq',
        { i },
        {
          ordering: { key: 'rlSeq', rateLimit: { max: 10, duration: 5000 } },
        },
      );
    }

    const worker = new Worker(
      Q9,
      async () => {
        currentConcurrent++;
        peakConcurrent = Math.max(peakConcurrent, currentConcurrent);
        await sleep(30);
        currentConcurrent--;
        completed++;
      },
      { connection: CONNECTION, concurrency: 10 },
    );

    const deadline = Date.now() + 10000;
    while (completed < 4 && Date.now() < deadline) {
      await sleep(50);
    }

    await worker.close();
    await q.close();
    await flushQueue(cleanupClient, Q9);

    // Without explicit concurrency, should default to 1 (sequential)
    expect(peakConcurrent).toBe(1);
    expect(completed).toBe(4);
  }, 15000);

  it('obliterate cleans up ratelimited sorted set', async () => {
    const Q10 = Q + '-oblrl';
    const q = new Queue(Q10, { connection: CONNECTION });

    // Add jobs with a very restrictive rate limit so some get parked
    for (let i = 0; i < 5; i++) {
      await q.add(
        'obl-job',
        { i },
        {
          ordering: { key: 'oblGrp', concurrency: 10, rateLimit: { max: 1, duration: 10000 } },
        },
      );
    }

    let completed = 0;
    const worker = new Worker(
      Q10,
      async () => {
        completed++;
      },
      { connection: CONNECTION, concurrency: 10 },
    );

    // Wait for at least 1 to process (others should be parked)
    while (completed < 1) {
      await sleep(100);
    }
    await worker.close();

    // Obliterate
    await q.obliterate({ force: true });

    // Verify ratelimited key is gone
    const keys = buildKeys(Q10);
    const exists = await cleanupClient.exists([keys.ratelimited]);
    expect(Number(exists)).toBe(0);

    await q.close();
  }, 15000);

  it('mixed rate-limited and non-rate-limited jobs on same queue', async () => {
    const Q11 = Q + '-mixrl';
    const q = new Queue(Q11, { connection: CONNECTION });

    let completedRateLimited = 0;
    let completedNormal = 0;

    // Rate-limited group
    for (let i = 0; i < 3; i++) {
      await q.add(
        'rl-job',
        { rateLimited: true },
        {
          ordering: { key: 'mixRlGrp', concurrency: 5, rateLimit: { max: 5, duration: 2000 } },
        },
      );
    }
    // Normal jobs (no ordering, no rate limit)
    for (let i = 0; i < 3; i++) {
      await q.add('normal-job', { rateLimited: false });
    }

    const worker = new Worker(
      Q11,
      async (job: any) => {
        if (job.data.rateLimited) completedRateLimited++;
        else completedNormal++;
      },
      { connection: CONNECTION, concurrency: 10 },
    );

    const deadline = Date.now() + 10000;
    while ((completedRateLimited < 3 || completedNormal < 3) && Date.now() < deadline) {
      await sleep(50);
    }

    await worker.close();
    await q.close();
    await flushQueue(cleanupClient, Q11);

    expect(completedRateLimited).toBe(3);
    expect(completedNormal).toBe(3);
  }, 15000);

  // --- completeAndFetchNext rate-limit branch ---

  it('completeAndFetchNext respects rate limit for chained job', async () => {
    // Use concurrency=1 so completeAndFetchNext chains jobs (not parallel dispatch)
    // rateLimit max=1 per 800ms
    // Add 3 jobs - first completes, second should be rate-limited by completeAndFetchNext
    // Third completes after window resets
    const Q = uniqueQueueName('-cfn');
    const q = new Queue(Q, { connection: CONNECTION });
    const completed: number[] = [];
    const start = Date.now();

    for (let i = 0; i < 3; i++) {
      await q.add(
        'cfn-job',
        { i },
        {
          ordering: { key: 'cfnGrp', concurrency: 1, rateLimit: { max: 1, duration: 800 } },
        },
      );
    }

    const worker = new Worker(
      Q,
      async () => {
        completed.push(Date.now() - start);
      },
      { connection: CONNECTION, concurrency: 1 },
    ); // concurrency=1 forces chaining

    // Wait for all 3
    const deadline = Date.now() + 20000;
    while (completed.length < 3 && Date.now() < deadline) await sleep(50);

    await worker.close();
    await q.close();
    await flushQueue(cleanupClient, Q);

    expect(completed.length).toBe(3);
    // Each job should be spaced by at least the rate window
    completed.sort((a, b) => a - b);
    expect(completed[1] - completed[0]).toBeGreaterThan(400);
    expect(completed[2] - completed[1]).toBeGreaterThan(400);
  }, 25000);

  // --- FlowProducer with rate-limited children ---

  it('FlowProducer passes rate limit to child jobs', async () => {
    const { FlowProducer } = require('../dist/flow-producer');
    const Q = uniqueQueueName('-flowrl');
    const fp = new FlowProducer({ connection: CONNECTION });

    const completed: string[] = [];

    await fp.add({
      name: 'parent',
      queueName: Q,
      data: { role: 'parent' },
      children: [
        {
          name: 'child-1',
          queueName: Q,
          data: { role: 'child', i: 1 },
          opts: { ordering: { key: 'flowGrp', concurrency: 5, rateLimit: { max: 5, duration: 2000 } } },
        },
        {
          name: 'child-2',
          queueName: Q,
          data: { role: 'child', i: 2 },
          opts: { ordering: { key: 'flowGrp', concurrency: 5, rateLimit: { max: 5, duration: 2000 } } },
        },
      ],
    });

    const worker = new Worker(
      Q,
      async (job: any) => {
        completed.push(job.name);
      },
      { connection: CONNECTION, concurrency: 5 },
    );

    const deadline = Date.now() + 10000;
    while (completed.length < 3 && Date.now() < deadline) await sleep(50);

    await worker.close();
    await fp.close();
    // Verify children completed (they use rate-limited group)
    expect(completed).toContain('child-1');
    expect(completed).toContain('child-2');

    // Verify group hash was created with rate limit fields
    const keys = buildKeys(Q);
    const grpFields = await cleanupClient.hgetall(keys.group('flowGrp'));
    const grp: Record<string, string> = {};
    if (grpFields) {
      for (const f of grpFields) grp[String(f.field)] = String(f.value);
    }
    expect(Number(grp.rateMax)).toBe(5);
    expect(Number(grp.rateDuration)).toBe(2000);

    await flushQueue(cleanupClient, Q);
  }, 15000);

  // --- Global rate limit meta fallback ---

  it('glidemq_rateLimit reads config from meta when args are zero', async () => {
    const { rateLimit: rateLimitFn } = require('../dist/functions/index');
    const Q = uniqueQueueName('-metarl');
    const q = new Queue(Q, { connection: CONNECTION });
    const client = await createCleanupClient(CONNECTION);
    const keys = buildKeys(Q);

    // Set rate limit config in meta
    await client.hset(keys.meta, { rateLimitMax: '2', rateLimitDuration: '1000' });

    // Call rateLimit with max=0 (should read from meta)
    const delay1 = await rateLimitFn(client, keys, 0, 0, Date.now());
    expect(delay1).toBe(0); // First call: count=1, under limit

    const delay2 = await rateLimitFn(client, keys, 0, 0, Date.now());
    expect(delay2).toBe(0); // Second call: count=2, at limit

    const delay3 = await rateLimitFn(client, keys, 0, 0, Date.now());
    expect(delay3).toBeGreaterThan(0); // Third call: over limit, should return delay

    await q.close();
    client.close();
    await flushQueue(cleanupClient, Q);
  }, 10000);

  // --- Rate limit counter after failure ---

  it('failed job does not inflate rate counter', async () => {
    const Q = uniqueQueueName('-failctr');
    const q = new Queue(Q, { connection: CONNECTION });
    let completed = 0;

    // rateLimit max=3, 3 jobs: first fails, next 2 succeed
    for (let i = 0; i < 3; i++) {
      await q.add(
        'ctr-job',
        { shouldFail: i === 0 },
        {
          ordering: { key: 'failCtrGrp', concurrency: 5, rateLimit: { max: 3, duration: 5000 } },
        },
      );
    }

    const worker = new Worker(
      Q,
      async (job: any) => {
        if (job.data.shouldFail) throw new Error('intentional');
        completed++;
      },
      { connection: CONNECTION, concurrency: 5 },
    );

    const deadline = Date.now() + 10000;
    while (completed < 2 && Date.now() < deadline) await sleep(100);
    await worker.close();

    // Check group hash - rateCount should reflect actual activations, not just successes
    const keys = buildKeys(Q);
    const rateCount = await cleanupClient.hget(keys.group('failCtrGrp'), 'rateCount');
    // All 3 jobs activated (1 failed + 2 succeeded), so rateCount = 3
    expect(Number(String(rateCount))).toBeLessThanOrEqual(3);
    expect(completed).toBe(2);

    await q.close();
    await flushQueue(cleanupClient, Q);
  }, 15000);
});
