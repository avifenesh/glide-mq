/**
 * Job TTL (time-to-live) integration tests against a real Valkey instance.
 * Requires: valkey-server running on localhost:6379 and cluster on :7000-7005
 *
 * Run: npx vitest run tests/ttl.test.ts
 */
import { it, expect, beforeAll, afterAll } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

import { describeEachMode, createCleanupClient, flushQueue, waitFor } from './helpers/fixture';

describeEachMode('Job TTL - expiration', (CONNECTION) => {
  const Q = 'test-ttl-expire-' + Date.now();
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

  it('job expires before processing -> state failed, failedReason expired', async () => {
    // Add a job with a very short TTL
    const job = await queue.add('task', { v: 1 }, { ttl: 1 });
    expect(job).not.toBeNull();

    // Wait for TTL to pass
    await new Promise<void>((r) => setTimeout(r, 50));

    // Create a worker - the job should be expired on moveToActive
    const worker = new Worker(
      Q,
      async () => {
        throw new Error('Should not be called - job is expired');
      },
      { connection: CONNECTION },
    );
    await worker.waitUntilReady();

    // Wait for the worker to process (it should expire the job, not execute the processor)
    await waitFor(async () => {
      const counts = await queue.getJobCounts();
      return counts.failed > 0;
    }, 10000);

    const counts = await queue.getJobCounts();
    expect(counts.failed).toBe(1);

    const failedJob = await queue.getJob(job!.id);
    expect(failedJob).not.toBeNull();
    expect(failedJob!.failedReason).toBe('expired');

    await worker.close();
  });

  it('job processed before expiry -> completes normally', async () => {
    const Q2 = Q + '-completes';
    const queue2 = new Queue(Q2, { connection: CONNECTION });

    // Add a job with a long TTL
    const job = await queue2.add('task', { v: 2 }, { ttl: 60000 });
    expect(job).not.toBeNull();

    const completed: string[] = [];
    const worker = new Worker(
      Q2,
      async () => {
        return 'done';
      },
      { connection: CONNECTION },
    );
    worker.on('completed', (j: any) => completed.push(j.id));
    await worker.waitUntilReady();

    await waitFor(() => completed.length > 0, 10000);
    expect(completed).toContain(job!.id);

    const finishedJob = await queue2.getJob(job!.id);
    expect(finishedJob).not.toBeNull();

    await worker.close();
    await queue2.close();
    await flushQueue(cleanupClient, Q2);
  });

  it('TTL=0 means no expiry', async () => {
    const Q3 = Q + '-no-expiry-0';
    const queue3 = new Queue(Q3, { connection: CONNECTION });

    const job = await queue3.add('task', { v: 3 }, { ttl: 0 });
    expect(job).not.toBeNull();

    // Check that expireAt is not set on the hash
    const k = buildKeys(Q3);
    const expireAt = await cleanupClient.hget(k.job(job!.id), 'expireAt');
    expect(expireAt).toBeNull();

    await queue3.close();
    await flushQueue(cleanupClient, Q3);
  });

  it('TTL undefined means no expiry', async () => {
    const Q4 = Q + '-no-expiry-undef';
    const queue4 = new Queue(Q4, { connection: CONNECTION });

    const job = await queue4.add('task', { v: 4 });
    expect(job).not.toBeNull();

    const k = buildKeys(Q4);
    const expireAt = await cleanupClient.hget(k.job(job!.id), 'expireAt');
    expect(expireAt).toBeNull();

    await queue4.close();
    await flushQueue(cleanupClient, Q4);
  });
});

describeEachMode('Job TTL - delayed jobs', (CONNECTION) => {
  const Q = 'test-ttl-delayed-' + Date.now();
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

  it('TTL + delayed job - expires at promote time', async () => {
    // Add a delayed job with a TTL shorter than the delay
    // TTL=1ms, delay=50ms - job should expire before promotion
    const job = await queue.add('task', { v: 1 }, { ttl: 1, delay: 100 });
    expect(job).not.toBeNull();

    const completed: string[] = [];
    const worker = new Worker(Q, async () => 'done', { connection: CONNECTION, promotionInterval: 100 });
    worker.on('completed', (j: any) => completed.push(j.id));
    await worker.waitUntilReady();

    // Wait for the promotion cycle to run + TTL check
    await waitFor(async () => {
      const counts = await queue.getJobCounts();
      return counts.failed > 0;
    }, 10000);

    // The job should be failed (expired at promote time), NOT completed
    expect(completed).not.toContain(job!.id);
    const failedJob = await queue.getJob(job!.id);
    expect(failedJob).not.toBeNull();
    expect(failedJob!.failedReason).toBe('expired');

    await worker.close();
  });

  it('TTL + delayed job - processed before expiry', async () => {
    const Q2 = Q + '-delayed-ok';
    const queue2 = new Queue(Q2, { connection: CONNECTION });

    // Add a delayed job with a very long TTL
    const job = await queue2.add('task', { v: 2 }, { ttl: 60000, delay: 100 });
    expect(job).not.toBeNull();

    const completed: string[] = [];
    const worker = new Worker(Q2, async () => 'done', { connection: CONNECTION, promotionInterval: 100 });
    worker.on('completed', (j: any) => completed.push(j.id));
    await worker.waitUntilReady();

    await waitFor(() => completed.length > 0, 10000);
    expect(completed).toContain(job!.id);

    await worker.close();
    await queue2.close();
    await flushQueue(cleanupClient, Q2);
  });
});

describeEachMode('Job TTL - priority', (CONNECTION) => {
  const Q = 'test-ttl-priority-' + Date.now();
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

  it('TTL + priority job expires at promote time', async () => {
    // Priority jobs go through the scheduled ZSet -> promote cycle
    const job = await queue.add('task', { v: 1 }, { ttl: 1, priority: 5 });
    expect(job).not.toBeNull();

    await new Promise<void>((r) => setTimeout(r, 50));

    const worker = new Worker(Q, async () => 'done', { connection: CONNECTION, promotionInterval: 100 });
    await worker.waitUntilReady();

    await waitFor(async () => {
      const counts = await queue.getJobCounts();
      return counts.failed > 0;
    }, 10000);

    const failedJob = await queue.getJob(job!.id);
    expect(failedJob).not.toBeNull();
    expect(failedJob!.failedReason).toBe('expired');

    await worker.close();
  });
});

describeEachMode('Job TTL - retries/backoff', (CONNECTION) => {
  const Q = 'test-ttl-retry-' + Date.now();
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

  it('TTL spans all attempts - expires during retry', async () => {
    // Job with 3 attempts, very short TTL (50ms). First attempt runs and fails quickly,
    // the job goes to delayed state with 2000ms backoff. By the time the promote cycle
    // fires, the TTL has long expired, so the job is failed as 'expired' at promote time.
    const job = await queue.add(
      'task',
      { v: 1 },
      {
        ttl: 50,
        attempts: 3,
        backoff: { type: 'fixed', delay: 2000 },
      },
    );
    expect(job).not.toBeNull();

    let attempts = 0;
    const worker = new Worker(
      Q,
      async () => {
        attempts++;
        throw new Error('fail on purpose');
      },
      { connection: CONNECTION, promotionInterval: 200 },
    );
    await worker.waitUntilReady();

    // Wait for the job to end up as 'failed' with 'expired' reason
    await waitFor(async () => {
      const j = await queue.getJob(job!.id);
      return j?.failedReason === 'expired';
    }, 15000);

    const failedJob = await queue.getJob(job!.id);
    expect(failedJob).not.toBeNull();
    // The job should expire during the backoff delay (2000ms >> 50ms TTL)
    expect(failedJob!.failedReason).toBe('expired');

    await worker.close();
  });
});

describeEachMode('Job TTL - dedup', (CONNECTION) => {
  const Q = 'test-ttl-dedup-' + Date.now();
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

  it('TTL is stored on deduped jobs', async () => {
    const job = await queue.add(
      'task',
      { v: 1 },
      {
        ttl: 5000,
        deduplication: { id: 'dedup-ttl-1', mode: 'simple' },
      },
    );
    expect(job).not.toBeNull();

    const k = buildKeys(Q);
    const expireAt = await cleanupClient.hget(k.job(job!.id), 'expireAt');
    expect(expireAt).not.toBeNull();
    expect(Number(String(expireAt))).toBeGreaterThan(Date.now());

    // Second add is deduped
    const job2 = await queue.add(
      'task',
      { v: 2 },
      {
        ttl: 5000,
        deduplication: { id: 'dedup-ttl-1', mode: 'simple' },
      },
    );
    expect(job2).toBeNull();
  });
});

describeEachMode('Job TTL - addBulk', (CONNECTION) => {
  const Q = 'test-ttl-bulk-' + Date.now();
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

  it('addBulk passes TTL correctly', async () => {
    const jobs = await queue.addBulk([
      { name: 'a', data: { v: 1 }, opts: { ttl: 10000 } },
      { name: 'b', data: { v: 2 } },
      { name: 'c', data: { v: 3 }, opts: { ttl: 5000 } },
    ]);
    expect(jobs.length).toBe(3);

    const k = buildKeys(Q);

    // Job 'a' should have expireAt
    const expireAtA = await cleanupClient.hget(k.job(jobs[0].id), 'expireAt');
    expect(expireAtA).not.toBeNull();

    // Job 'b' should NOT have expireAt
    const expireAtB = await cleanupClient.hget(k.job(jobs[1].id), 'expireAt');
    expect(expireAtB).toBeNull();

    // Job 'c' should have expireAt
    const expireAtC = await cleanupClient.hget(k.job(jobs[2].id), 'expireAt');
    expect(expireAtC).not.toBeNull();
  });
});
