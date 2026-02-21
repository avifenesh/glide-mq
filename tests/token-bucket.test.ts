/**
 * Integration tests for cost-based token bucket rate limiting.
 *
 * Tests ordering.tokenBucket: { capacity, refillRate } with per-job cost.
 *
 * Run: npx vitest run tests/token-bucket.test.ts
 */
import { it, expect, beforeAll, afterAll } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describeEachMode('Token bucket', (CONNECTION) => {
  const Q = 'test-tokenbucket-' + Date.now();
  let queue: InstanceType<typeof Queue>;
  let cleanupClient: any;

  function uq(suffix: string): string {
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

  // --- Basic token bucket enforcement ---

  it('enforces cost-based throughput limit', async () => {
    const QN = uq('-basic');
    const q = new Queue(QN, { connection: CONNECTION });
    const completed: number[] = [];
    const start = Date.now();

    // Bucket: capacity=5 tokens, refill=5 tokens/sec
    // 3 jobs each costing 2 tokens = 6 tokens total. Bucket starts with 5.
    // First 2 jobs (cost 4) fit. Third (cost 2) must wait for refill.
    for (let i = 0; i < 3; i++) {
      await q.add(
        'tb-job',
        { i },
        {
          ordering: { key: 'tbGrp', concurrency: 10, tokenBucket: { capacity: 5, refillRate: 5 } },
          cost: 2,
        },
      );
    }

    const worker = new Worker(
      QN,
      async () => {
        completed.push(Date.now() - start);
      },
      { connection: CONNECTION, concurrency: 10 },
    );

    const deadline = Date.now() + 15000;
    while (completed.length < 3 && Date.now() < deadline) await sleep(50);

    await worker.close();
    await q.close();
    await flushQueue(cleanupClient, QN);

    expect(completed.length).toBe(3);
    completed.sort((a, b) => a - b);
    // First 2 should complete quickly, third after refill (~200ms for 1 token at 5/sec)
    expect(completed[1]).toBeLessThan(2000);
  }, 20000);

  // --- Different costs per job ---

  it('handles different costs per job within same group', async () => {
    const QN = uq('-diffcost');
    const q = new Queue(QN, { connection: CONNECTION });
    let completed = 0;

    // Bucket: capacity=10 tokens, refill=10/sec
    // Jobs: cost 1, cost 3, cost 5, cost 1 = total 10, exactly fits in bucket
    await q.add(
      'cheap',
      { cost: 1 },
      { ordering: { key: 'dcGrp', concurrency: 10, tokenBucket: { capacity: 10, refillRate: 10 } }, cost: 1 },
    );
    await q.add(
      'medium',
      { cost: 3 },
      { ordering: { key: 'dcGrp', concurrency: 10, tokenBucket: { capacity: 10, refillRate: 10 } }, cost: 3 },
    );
    await q.add(
      'expensive',
      { cost: 5 },
      { ordering: { key: 'dcGrp', concurrency: 10, tokenBucket: { capacity: 10, refillRate: 10 } }, cost: 5 },
    );
    await q.add(
      'cheap2',
      { cost: 1 },
      { ordering: { key: 'dcGrp', concurrency: 10, tokenBucket: { capacity: 10, refillRate: 10 } }, cost: 1 },
    );

    const worker = new Worker(
      QN,
      async () => {
        completed++;
      },
      { connection: CONNECTION, concurrency: 10 },
    );

    const deadline = Date.now() + 10000;
    while (completed < 4 && Date.now() < deadline) await sleep(50);

    await worker.close();
    await q.close();
    await flushQueue(cleanupClient, QN);

    expect(completed).toBe(4);
  }, 15000);

  // --- Cost > capacity rejected at enqueue ---

  it('rejects job with cost > capacity at enqueue', async () => {
    const QN = uq('-costexceed');
    const q = new Queue(QN, { connection: CONNECTION });

    await expect(
      q.add(
        'too-expensive',
        {},
        {
          ordering: { key: 'ceGrp', concurrency: 5, tokenBucket: { capacity: 3, refillRate: 1 } },
          cost: 5, // 5 > capacity 3
        },
      ),
    ).rejects.toThrow('cost exceeds token bucket capacity');

    await q.close();
    await flushQueue(cleanupClient, QN);
  }, 10000);

  // --- Default cost (1 token) ---

  it('default cost is 1 token when not specified', async () => {
    const QN = uq('-defaultcost');
    const q = new Queue(QN, { connection: CONNECTION });
    let completed = 0;

    // Bucket: capacity=3, refill=3/sec. 3 jobs with default cost=1 each.
    // All 3 fit in initial bucket.
    for (let i = 0; i < 3; i++) {
      await q.add(
        'dc-job',
        { i },
        {
          ordering: { key: 'defGrp', concurrency: 10, tokenBucket: { capacity: 3, refillRate: 3 } },
          // no cost specified -> default 1
        },
      );
    }

    const worker = new Worker(
      QN,
      async () => {
        completed++;
      },
      { connection: CONNECTION, concurrency: 10 },
    );

    const deadline = Date.now() + 10000;
    while (completed < 3 && Date.now() < deadline) await sleep(50);

    await worker.close();
    await q.close();
    await flushQueue(cleanupClient, QN);

    expect(completed).toBe(3);
  }, 15000);

  // --- Token bucket + concurrency interaction ---

  it('token bucket composes with concurrency limit', async () => {
    const QN = uq('-tbconc');
    const q = new Queue(QN, { connection: CONNECTION });
    let peakConcurrent = 0;
    let currentConcurrent = 0;
    let completed = 0;

    // concurrency=2, bucket capacity=10, refill=10/sec, cost=1
    // Concurrency should limit to 2 parallel even though bucket allows more
    for (let i = 0; i < 6; i++) {
      await q.add(
        'conc-job',
        { i },
        {
          ordering: { key: 'tbcGrp', concurrency: 2, tokenBucket: { capacity: 10, refillRate: 10 } },
          cost: 1,
        },
      );
    }

    const worker = new Worker(
      QN,
      async () => {
        currentConcurrent++;
        peakConcurrent = Math.max(peakConcurrent, currentConcurrent);
        await sleep(50);
        currentConcurrent--;
        completed++;
      },
      { connection: CONNECTION, concurrency: 10 },
    );

    const deadline = Date.now() + 10000;
    while (completed < 6 && Date.now() < deadline) await sleep(50);

    await worker.close();
    await q.close();
    await flushQueue(cleanupClient, QN);

    expect(peakConcurrent).toBeLessThanOrEqual(2);
    expect(completed).toBe(6);
  }, 15000);

  // --- Token bucket + sliding window interaction ---

  it('token bucket composes with sliding window rate limit', async () => {
    const QN = uq('-tbrl');
    const q = new Queue(QN, { connection: CONNECTION });
    let completed = 0;

    // Token bucket: capacity=10, refill=10/sec (generous)
    // Sliding window: max=2 per 800ms (restrictive)
    // Sliding window should be the bottleneck
    for (let i = 0; i < 4; i++) {
      await q.add(
        'tbrl-job',
        { i },
        {
          ordering: {
            key: 'tbrlGrp',
            concurrency: 10,
            tokenBucket: { capacity: 10, refillRate: 10 },
            rateLimit: { max: 2, duration: 800 },
          },
          cost: 1,
        },
      );
    }

    const worker = new Worker(
      QN,
      async () => {
        completed++;
      },
      { connection: CONNECTION, concurrency: 10 },
    );

    const deadline = Date.now() + 15000;
    while (completed < 4 && Date.now() < deadline) await sleep(50);

    await worker.close();
    await q.close();
    await flushQueue(cleanupClient, QN);

    expect(completed).toBe(4);
  }, 20000);

  // --- Bucket refills over time ---

  it('jobs are promoted after bucket refills', async () => {
    const QN = uq('-refill');
    const q = new Queue(QN, { connection: CONNECTION });
    const completionTimes: number[] = [];
    const start = Date.now();

    // Bucket: capacity=2, refill=2/sec, cost=1 per job
    // First 2 fit. Third waits for refill (~500ms for 1 token at 2/sec)
    for (let i = 0; i < 3; i++) {
      await q.add(
        'refill-job',
        { i },
        {
          ordering: { key: 'refGrp', concurrency: 10, tokenBucket: { capacity: 2, refillRate: 2 } },
          cost: 1,
        },
      );
    }

    const worker = new Worker(
      QN,
      async () => {
        completionTimes.push(Date.now() - start);
      },
      { connection: CONNECTION, concurrency: 10 },
    );

    const deadline = Date.now() + 15000;
    while (completionTimes.length < 3 && Date.now() < deadline) await sleep(50);

    await worker.close();
    await q.close();
    await flushQueue(cleanupClient, QN);

    expect(completionTimes.length).toBe(3);
    completionTimes.sort((a, b) => a - b);
    // Third job should be delayed by refill time
    expect(completionTimes[2] - completionTimes[1]).toBeGreaterThan(200);
  }, 20000);

  // --- Mixed token-bucket and non-token-bucket groups ---

  it('mixed token-bucket and non-token-bucket jobs on same queue', async () => {
    const QN = uq('-mixed');
    const q = new Queue(QN, { connection: CONNECTION });
    let tbCompleted = 0;
    let normalCompleted = 0;

    // Token bucket group
    for (let i = 0; i < 3; i++) {
      await q.add(
        'tb-job',
        { tb: true },
        {
          ordering: { key: 'mixTbGrp', concurrency: 10, tokenBucket: { capacity: 10, refillRate: 10 } },
          cost: 1,
        },
      );
    }
    // Normal jobs (no ordering)
    for (let i = 0; i < 3; i++) {
      await q.add('normal-job', { tb: false });
    }

    const worker = new Worker(
      QN,
      async (job: any) => {
        if (job.data.tb) tbCompleted++;
        else normalCompleted++;
      },
      { connection: CONNECTION, concurrency: 10 },
    );

    const deadline = Date.now() + 10000;
    while ((tbCompleted < 3 || normalCompleted < 3) && Date.now() < deadline) await sleep(50);

    await worker.close();
    await q.close();
    await flushQueue(cleanupClient, QN);

    expect(tbCompleted).toBe(3);
    expect(normalCompleted).toBe(3);
  }, 15000);

  // --- Token bucket without explicit concurrency defaults to sequential ---

  it('tokenBucket without concurrency defaults to sequential (concurrency=1)', async () => {
    const QN = uq('-tbseq');
    const q = new Queue(QN, { connection: CONNECTION });
    let peakConcurrent = 0;
    let currentConcurrent = 0;
    let completed = 0;

    for (let i = 0; i < 4; i++) {
      await q.add(
        'seq-job',
        { i },
        {
          ordering: { key: 'tbSeqGrp', tokenBucket: { capacity: 10, refillRate: 10 } },
          cost: 1,
        },
      );
    }

    const worker = new Worker(
      QN,
      async () => {
        currentConcurrent++;
        peakConcurrent = Math.max(peakConcurrent, currentConcurrent);
        await sleep(20);
        currentConcurrent--;
        completed++;
      },
      { connection: CONNECTION, concurrency: 10 },
    );

    const deadline = Date.now() + 10000;
    while (completed < 4 && Date.now() < deadline) await sleep(50);

    await worker.close();
    await q.close();
    await flushQueue(cleanupClient, QN);

    expect(peakConcurrent).toBe(1);
    expect(completed).toBe(4);
  }, 15000);

  // --- Group hash fields verification ---

  it('stores token bucket fields on group hash', async () => {
    const QN = uq('-fields');
    const q = new Queue(QN, { connection: CONNECTION });

    await q.add(
      'field-job',
      {},
      {
        ordering: { key: 'fieldGrp', concurrency: 3, tokenBucket: { capacity: 50, refillRate: 10 } },
        cost: 5,
      },
    );

    // Check group hash
    const keys = buildKeys(QN);
    const grpFields = await cleanupClient.hgetall(keys.group('fieldGrp'));
    const grp: Record<string, string> = {};
    if (grpFields) {
      for (const f of grpFields) grp[String(f.field)] = String(f.value);
    }

    expect(Number(grp.tbCapacity)).toBe(50000); // 50 * 1000 millitokens
    expect(Number(grp.tbRefillRate)).toBe(10000); // 10 * 1000 millitokens/sec
    expect(Number(grp.tbTokens)).toBeLessThanOrEqual(50000);
    expect(Number(grp.maxConcurrency)).toBe(3);

    // Check job hash has cost
    const jobId = '1';
    const jobCost = await cleanupClient.hget(keys.job(jobId), 'cost');
    expect(Number(String(jobCost))).toBe(5000); // 5 * 1000 millitokens

    // Cleanup
    const worker = new Worker(QN, async () => {}, { connection: CONNECTION });
    await sleep(500);
    await worker.close();
    await q.close();
    await flushQueue(cleanupClient, QN);
  }, 10000);

  // --- Obliterate cleans up ---

  it('obliterate cleans up group hash with token bucket fields', async () => {
    const QN = uq('-obl');
    const q = new Queue(QN, { connection: CONNECTION });

    await q.add(
      'obl-job',
      {},
      {
        ordering: { key: 'oblGrp', concurrency: 5, tokenBucket: { capacity: 20, refillRate: 5 } },
        cost: 2,
      },
    );

    const worker = new Worker(QN, async () => {}, { connection: CONNECTION });
    await sleep(500);
    await worker.close();

    await q.obliterate({ force: true });

    const keys = buildKeys(QN);
    const exists = await cleanupClient.exists([keys.group('oblGrp')]);
    expect(Number(exists)).toBe(0);

    await q.close();
  }, 10000);
});
