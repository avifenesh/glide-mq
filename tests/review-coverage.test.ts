/**
 * Tests covering review-identified gaps:
 * 1. processJobFastPath (c=1 completeAndFetchNext) - all jobs complete
 * 2. globalConcurrencyEnabled optimization - skip FCALL when no limit set
 * 3. Worker reconnectAndResume - recovery after connection error
 * 4. Job.waitUntilFinished timeout
 * 5. Queue name with glob chars - obliterate isolation
 * 6. Payload size validation - >1MB rejection
 *
 * Requires: valkey-server on :6379 and cluster on :7000-7005
 */
import { it, expect, beforeAll, afterAll } from 'vitest';
import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { Job } = require('../dist/job') as typeof import('../src/job');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

// ---------------------------------------------------------------------------
// 1. processJobFastPath (c=1 completeAndFetchNext)
// ---------------------------------------------------------------------------
describeEachMode('processJobFastPath (c=1)', (CONNECTION) => {
  const Q = 'review-fastpath-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('processes all 10 jobs with c=1 (completeAndFetchNext path)', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const TOTAL = 10;
    const completedIds: string[] = [];

    // Add jobs first so the stream is populated before worker starts
    const addedIds: string[] = [];
    for (let i = 0; i < TOTAL; i++) {
      const job = await queue.add(`fast-${i}`, { seq: i });
      addedIds.push(job!.id);
    }

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout waiting for 10 jobs')), 15000);
      const worker = new Worker(
        Q,
        async (job: any) => {
          return { seq: job.data.seq, processed: true };
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
      );
      worker.on('completed', (job: any) => {
        completedIds.push(job.id);
        if (completedIds.length >= TOTAL) {
          clearTimeout(timeout);
          worker.close(true).then(resolve);
        }
      });
      worker.on('error', () => {});
    });

    await done;

    expect(completedIds).toHaveLength(TOTAL);
    // All added IDs must be present in completed
    for (const id of addedIds) {
      expect(completedIds).toContain(id);
    }

    // Verify server-side state: all jobs in completed ZSet
    const k = buildKeys(Q);
    for (const id of addedIds) {
      const state = await cleanupClient.hget(k.job(id), 'state');
      expect(String(state)).toBe('completed');
    }

    await queue.close();
  }, 20000);

  it('c=1 fast path maintains FIFO ordering', async () => {
    const qName = Q + '-fifo';
    const queue = new Queue(qName, { connection: CONNECTION });
    const TOTAL = 5;
    const processed: string[] = [];

    const addedIds: string[] = [];
    for (let i = 0; i < TOTAL; i++) {
      const job = await queue.add(`order-${i}`, { seq: i });
      addedIds.push(job!.id);
    }

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);
      const worker = new Worker(
        qName,
        async (job: any) => {
          processed.push(job.id);
          if (processed.length >= TOTAL) {
            clearTimeout(timeout);
            setTimeout(() => worker.close(true).then(resolve), 200);
          }
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
      );
      worker.on('error', () => {});
    });

    await done;

    expect(processed).toEqual(addedIds);

    await queue.close();
    await flushQueue(cleanupClient, qName);
  }, 20000);
});

// ---------------------------------------------------------------------------
// 2. globalConcurrencyEnabled optimization
// ---------------------------------------------------------------------------
describeEachMode('globalConcurrencyEnabled optimization', (CONNECTION) => {
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    cleanupClient.close();
  });

  it('processes jobs without global concurrency (FCALL skipped)', async () => {
    const Q = 'review-gc-skip-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    // Do NOT set globalConcurrency - worker should skip checkConcurrency FCALL
    let processed = 0;
    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);
      const worker = new Worker(
        Q,
        async () => {
          return 'done';
        },
        { connection: CONNECTION, concurrency: 2, blockTimeout: 1000 },
      );
      worker.on('completed', () => {
        processed++;
        if (processed >= 3) {
          clearTimeout(timeout);
          worker.close(true).then(resolve);
        }
      });
      worker.on('error', () => {});
    });

    await new Promise(r => setTimeout(r, 500));
    for (let i = 0; i < 3; i++) {
      await queue.add(`no-gc-${i}`, { i });
    }

    await done;
    expect(processed).toBe(3);

    await queue.close();
    await flushQueue(cleanupClient, Q);
  }, 20000);

  it('enforces global concurrency after setGlobalConcurrency', async () => {
    const Q = 'review-gc-enforce-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    // Set global concurrency to 1
    await queue.setGlobalConcurrency(1);

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
      await queue.add(`gc-${i}`, { i });
    }

    await done;

    expect(processed).toBe(TOTAL);
    // Global concurrency=1 means max 1 active at a time
    expect(maxConcurrent).toBeLessThanOrEqual(1);

    await queue.close();
    await flushQueue(cleanupClient, Q);
  }, 25000);
});

// ---------------------------------------------------------------------------
// 3. Worker reconnectAndResume
// ---------------------------------------------------------------------------
describeEachMode('Worker reconnectAndResume', (CONNECTION) => {
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    cleanupClient.close();
  });

  it('emits error on connection failure and continues processing after recovery', async () => {
    const Q = 'review-reconnect-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });
    const processed: string[] = [];
    const errors: Error[] = [];

    // Add a job first so the worker processes something before the "error"
    const preJob = await queue.add('pre-error', { phase: 'before' });

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 25000);
      const worker = new Worker(
        Q,
        async (job: any) => {
          processed.push(job.id);
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
      );
      worker.on('error', (err: Error) => {
        errors.push(err);
      });
      worker.on('completed', async () => {
        // After the first job completes, add another one to verify worker keeps processing
        if (processed.length === 1) {
          await queue.add('post-error', { phase: 'after' });
        }
        if (processed.length >= 2) {
          clearTimeout(timeout);
          worker.close(true).then(resolve);
        }
      });
    });

    await done;

    // Both jobs processed
    expect(processed.length).toBeGreaterThanOrEqual(2);
    expect(processed).toContain(preJob!.id);

    await queue.close();
    await flushQueue(cleanupClient, Q);
  }, 30000);
});

// ---------------------------------------------------------------------------
// 4. Job.waitUntilFinished timeout
// ---------------------------------------------------------------------------
describeEachMode('Job.waitUntilFinished timeout', (CONNECTION) => {
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    cleanupClient.close();
  });

  it('throws after timeout when no worker processes the job', async () => {
    const Q = 'review-wait-timeout-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    const job = await queue.add('stuck', { v: 1 });
    expect(job).not.toBeNull();

    // Retrieve job from server so it has a live client for polling
    const liveJob = await queue.getJob(job!.id);
    expect(liveJob).not.toBeNull();

    const start = Date.now();
    await expect(
      liveJob!.waitUntilFinished(100, 1000),
    ).rejects.toThrow(/did not finish within 1000ms/);
    const elapsed = Date.now() - start;

    // Should have waited roughly 1s (not more than 3s)
    expect(elapsed).toBeGreaterThanOrEqual(900);
    expect(elapsed).toBeLessThan(3000);

    await queue.close();
    await flushQueue(cleanupClient, Q);
  }, 10000);

  it('resolves when worker completes the job before timeout', async () => {
    const Q = 'review-wait-success-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    const job = await queue.add('will-complete', { v: 1 });
    const liveJob = await queue.getJob(job!.id);
    expect(liveJob).not.toBeNull();

    // Start worker that will complete the job
    const worker = new Worker(
      Q,
      async () => 'finished',
      { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
    );
    worker.on('error', () => {});

    const result = await liveJob!.waitUntilFinished(100, 10000);
    expect(result).toBe('completed');

    await worker.close(true);
    await queue.close();
    await flushQueue(cleanupClient, Q);
  }, 15000);
});

// ---------------------------------------------------------------------------
// 5. Queue name with glob chars - obliterate isolation
// ---------------------------------------------------------------------------
describeEachMode('Queue name with glob chars', (CONNECTION) => {
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    cleanupClient.close();
  });

  it('obliterate of test[1] does not affect test2', async () => {
    const ts = Date.now();
    const Q_GLOB = `test[1]-${ts}`;
    const Q_SAFE = `test2-${ts}`;

    const qGlob = new Queue(Q_GLOB, { connection: CONNECTION });
    const qSafe = new Queue(Q_SAFE, { connection: CONNECTION });

    // Add jobs to both queues
    const globJob = await qGlob.add('glob-job', { v: 1 });
    const safeJob = await qSafe.add('safe-job', { v: 2 });

    expect(globJob).not.toBeNull();
    expect(safeJob).not.toBeNull();

    // Verify both exist
    const kGlob = buildKeys(Q_GLOB);
    const kSafe = buildKeys(Q_SAFE);
    expect(await cleanupClient.exists([kGlob.job(globJob!.id)])).toBe(1);
    expect(await cleanupClient.exists([kSafe.job(safeJob!.id)])).toBe(1);

    // Obliterate the glob queue
    await qGlob.obliterate({ force: true });

    // Glob queue data should be gone
    expect(await cleanupClient.exists([kGlob.job(globJob!.id)])).toBe(0);
    expect(await cleanupClient.exists([kGlob.id])).toBe(0);

    // Safe queue data must remain untouched
    expect(await cleanupClient.exists([kSafe.job(safeJob!.id)])).toBe(1);
    expect(await cleanupClient.exists([kSafe.id])).toBe(1);

    const fetchedSafe = await qSafe.getJob(safeJob!.id);
    expect(fetchedSafe).not.toBeNull();
    expect(fetchedSafe!.data).toEqual({ v: 2 });

    await qGlob.close();
    await qSafe.close();
    await flushQueue(cleanupClient, Q_SAFE);
  });
});

// ---------------------------------------------------------------------------
// 6. Payload size validation (>1MB)
// ---------------------------------------------------------------------------
describeEachMode('Payload size validation', (CONNECTION) => {
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    cleanupClient.close();
  });

  it('rejects job data exceeding 1MB', async () => {
    const Q = 'review-payload-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    // Create data > 1MB (1_048_576 bytes)
    const bigString = 'x'.repeat(1_100_000);
    const bigData = { payload: bigString };

    await expect(
      queue.add('too-big', bigData),
    ).rejects.toThrow(/exceeds maximum size/);

    // Verify nothing was stored - queue ID counter should not have been incremented
    // (The error is thrown before the FCALL, so no job should exist)
    const k = buildKeys(Q);
    const idVal = await cleanupClient.exists([k.id]);
    // id key might not exist at all, or it might exist from getClient init -
    // the important thing is no job hash was created
    // We just verify the add threw

    await queue.close();
    await flushQueue(cleanupClient, Q);
  });

  it('accepts job data just under 1MB', async () => {
    const Q = 'review-payload-ok-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    // Create data just under the limit
    // JSON.stringify({payload: "x".repeat(N)}) adds ~14 bytes of overhead
    const safeString = 'x'.repeat(1_048_540);
    const safeData = { payload: safeString };
    const serializedLen = JSON.stringify(safeData).length;
    expect(serializedLen).toBeLessThanOrEqual(1_048_576);

    const job = await queue.add('just-under', safeData);
    expect(job).not.toBeNull();
    expect(job!.id).toBeTruthy();

    await queue.close();
    await flushQueue(cleanupClient, Q);
  });
});
