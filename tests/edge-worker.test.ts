/**
 * Edge-case integration tests for Worker resilience and stalled recovery.
 * Requires: valkey-server running on localhost:6379
 *
 * Run: npx vitest run tests/edge-worker.test.ts
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';

const { GlideClient } = require('speedkey') as typeof import('speedkey');
const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
const { LIBRARY_SOURCE, CONSUMER_GROUP } = require('../dist/functions/index') as typeof import('../src/functions/index');
const { ensureFunctionLibrary } = require('../dist/connection') as typeof import('../src/connection');

const CONNECTION = {
  addresses: [{ host: 'localhost', port: 6379 }],
};

const TS = Date.now();
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
  await ensureFunctionLibrary(cleanupClient, LIBRARY_SOURCE);
});

afterAll(async () => {
  cleanupClient.close();
});

describe('Worker close(force=true) leaves jobs in PEL', () => {
  const Q = `ew-force-close-${TS}`;

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('force-closing a worker while a job is active does not ACK the job', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    let processorStarted = false;

    const workerReady = new Promise<InstanceType<typeof Worker>>((resolve) => {
      const w = new Worker(
        Q,
        async () => {
          processorStarted = true;
          // Simulate long-running job that never finishes before force close
          await new Promise((r) => setTimeout(r, 30000));
          return 'done';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      w.on('error', () => {});
      // Wait for the worker to initialize
      w.waitUntilReady().then(() => resolve(w));
    });

    const worker = await workerReady;

    // Add a job and wait for the processor to start
    await queue.add('long-task', { x: 1 });
    while (!processorStarted) {
      await new Promise((r) => setTimeout(r, 50));
    }

    // Force close - should NOT wait for active job
    await worker.close(true);

    // Check PEL: the stream entry should still be pending (not ACKed)
    const k = buildKeys(Q);
    const pending = await cleanupClient.customCommand([
      'XPENDING', k.stream, CONSUMER_GROUP,
    ]) as any[];
    // pending[0] is the total number of pending entries
    expect(Number(pending[0])).toBeGreaterThanOrEqual(1);

    await queue.close();
  }, 15000);
});

describe('Worker close(force=false) waits for active jobs', () => {
  const Q = `ew-graceful-close-${TS}`;

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('graceful close waits for active job to complete', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const completed: string[] = [];

    const worker = new Worker(
      Q,
      async (job: any) => {
        await new Promise((r) => setTimeout(r, 500));
        completed.push(job.id);
        return 'ok';
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
    );
    worker.on('error', () => {});
    await worker.waitUntilReady();

    const job = await queue.add('task', { x: 1 });

    // Wait for processor to pick up the job
    await new Promise((r) => setTimeout(r, 300));

    // Graceful close - should wait for the active job to finish
    await worker.close(false);

    // The job should have been completed before close returned
    expect(completed).toContain(job.id);

    await queue.close();
  }, 15000);
});

describe('Worker pause and resume', () => {
  const Q = `ew-pause-${TS}`;

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('pause allows current job to finish but blocks new jobs', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const processed: string[] = [];

    const worker = new Worker(
      Q,
      async (job: any) => {
        await new Promise((r) => setTimeout(r, 200));
        processed.push(job.id);
        return 'ok';
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
    );
    worker.on('error', () => {});
    await worker.waitUntilReady();

    // Add first job and let it be picked up
    const job1 = await queue.add('task1', { i: 1 });

    // Wait briefly, then pause (waits for current job to finish with force=false)
    await new Promise((r) => setTimeout(r, 100));
    await worker.pause();

    // job1 should be processed (pause waits for active jobs)
    expect(processed).toContain(job1.id);

    // Record count before adding job2
    const countBefore = processed.length;

    // Add second job while paused - wait long enough that it would have been
    // picked up if the worker were still running
    const job2 = await queue.add('task2', { i: 2 });
    await new Promise((r) => setTimeout(r, 1500));

    // job2 should NOT be processed while paused
    expect(processed.length).toBe(countBefore);

    // Resume
    await worker.resume();

    // Wait for job2 to be processed
    await new Promise((r) => setTimeout(r, 2000));
    expect(processed).toContain(job2.id);

    await worker.close(true);
    await queue.close();
  }, 15000);
});

describe('Worker concurrency=1 processes sequentially', () => {
  const Q = `ew-seq-${TS}`;

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('jobs are processed in order with concurrency=1', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const order: string[] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker = new Worker(
        Q,
        async (job: any) => {
          order.push(job.name);
          if (order.length >= 3) {
            clearTimeout(timeout);
            setTimeout(() => worker.close(true).then(resolve), 200);
          }
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});
    });

    await new Promise((r) => setTimeout(r, 500));
    // Add jobs sequentially so stream ordering is guaranteed
    await queue.add('first', {});
    await queue.add('second', {});
    await queue.add('third', {});

    await done;

    expect(order).toEqual(['first', 'second', 'third']);
    await queue.close();
  }, 15000);
});

describe('Worker error handler: processor throws', () => {
  const Q = `ew-throw-${TS}`;

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('emits failed event with the error when processor throws', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const failedEvents: { jobId: string; message: string }[] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker = new Worker(
        Q,
        async () => {
          throw new Error('processor exploded');
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});
      worker.on('failed', (job: any, err: Error) => {
        failedEvents.push({ jobId: job.id, message: err.message });
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });
    });

    await new Promise((r) => setTimeout(r, 500));
    const job = await queue.add('bad-job', { x: 1 });

    await done;

    expect(failedEvents).toHaveLength(1);
    expect(failedEvents[0].jobId).toBe(job.id);
    expect(failedEvents[0].message).toBe('processor exploded');

    // Verify the job is in the failed set
    const k = buildKeys(Q);
    const score = await cleanupClient.zscore(k.failed, job.id);
    expect(score).not.toBeNull();
    expect(String(await cleanupClient.hget(k.job(job.id), 'state'))).toBe('failed');

    await queue.close();
  }, 15000);
});

describe('Worker processor returns undefined', () => {
  const Q = `ew-undef-${TS}`;

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('job completes with null returnvalue when processor returns undefined', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker = new Worker(
        Q,
        async () => {
          // explicitly return undefined
          return undefined;
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});
      worker.on('completed', () => {
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });
    });

    await new Promise((r) => setTimeout(r, 500));
    const job = await queue.add('undef-job', { x: 1 });

    await done;

    const k = buildKeys(Q);
    const rv = await cleanupClient.hget(k.job(job.id), 'returnvalue');
    expect(String(rv)).toBe('null');
    expect(String(await cleanupClient.hget(k.job(job.id), 'state'))).toBe('completed');

    await queue.close();
  }, 15000);
});

describe('Worker with very short blockTimeout', () => {
  const Q = `ew-shorttimeout-${TS}`;

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('does not error with blockTimeout=100 on an empty queue, and still processes when a job arrives', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const errors: Error[] = [];
    const completed: string[] = [];

    const worker = new Worker(
      Q,
      async (job: any) => {
        completed.push(job.id);
        return 'ok';
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 100, stalledInterval: 60000 },
    );
    worker.on('error', (err: Error) => errors.push(err));
    await worker.waitUntilReady();

    // Let it spin on the empty queue for a bit
    await new Promise((r) => setTimeout(r, 1000));
    expect(errors).toHaveLength(0);

    // Now add a job and verify it gets processed
    const job = await queue.add('late-job', { x: 1 });
    await new Promise((r) => setTimeout(r, 1500));

    expect(completed).toContain(job.id);
    expect(errors).toHaveLength(0);

    await worker.close(true);
    await queue.close();
  }, 15000);
});

describe('Multiple workers on same queue', () => {
  const Q = `ew-multi-${TS}`;

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('distributes jobs across workers with no duplicates', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const processedByWorker1: string[] = [];
    const processedByWorker2: string[] = [];

    const worker1 = new Worker(
      Q,
      async (job: any) => {
        processedByWorker1.push(job.id);
        await new Promise((r) => setTimeout(r, 100));
        return 'w1';
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
    );
    worker1.on('error', () => {});

    const worker2 = new Worker(
      Q,
      async (job: any) => {
        processedByWorker2.push(job.id);
        await new Promise((r) => setTimeout(r, 100));
        return 'w2';
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
    );
    worker2.on('error', () => {});

    await worker1.waitUntilReady();
    await worker2.waitUntilReady();

    // Add several jobs
    const NUM_JOBS = 6;
    const jobIds: string[] = [];
    for (let i = 0; i < NUM_JOBS; i++) {
      const job = await queue.add(`multi-${i}`, { i });
      jobIds.push(job.id);
    }

    // Wait for all jobs to be processed
    await new Promise((r) => setTimeout(r, 3000));

    const allProcessed = [...processedByWorker1, ...processedByWorker2];
    // All jobs should be processed
    for (const id of jobIds) {
      expect(allProcessed).toContain(id);
    }
    // No duplicates
    expect(new Set(allProcessed).size).toBe(allProcessed.length);

    await worker1.close(true);
    await worker2.close(true);
    await queue.close();
  }, 15000);
});

describe('Worker on empty queue', () => {
  const Q = `ew-empty-${TS}`;

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('does not error when no jobs are available', async () => {
    const errors: Error[] = [];

    const worker = new Worker(
      Q,
      async () => {
        return 'should not run';
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
    );
    worker.on('error', (err: Error) => errors.push(err));
    await worker.waitUntilReady();

    // Let the worker poll on the empty queue
    await new Promise((r) => setTimeout(r, 2000));

    expect(errors).toHaveLength(0);

    await worker.close(true);
  }, 10000);
});

describe('Worker with slow processor', () => {
  const Q = `ew-slow-${TS}`;

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('completes a job even when the processor takes a while', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });

    const done = new Promise<string>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker = new Worker(
        Q,
        async () => {
          // Simulate a slow processor
          await new Promise((r) => setTimeout(r, 2000));
          return 'slow-result';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});
      worker.on('completed', (job: any, result: any) => {
        clearTimeout(timeout);
        worker.close(true).then(() => resolve(result));
      });
    });

    await new Promise((r) => setTimeout(r, 500));
    const job = await queue.add('slow-job', { x: 1 });

    const result = await done;
    expect(result).toBe('slow-result');

    const k = buildKeys(Q);
    expect(String(await cleanupClient.hget(k.job(job.id), 'state'))).toBe('completed');
    expect(String(await cleanupClient.hget(k.job(job.id), 'returnvalue'))).toBe('"slow-result"');

    await queue.close();
  }, 15000);
});

describe('Stalled job recovery', () => {
  const Q = `ew-stalled-${TS}`;

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('reclaims a stalled job and moves it to failed after exceeding maxStalledCount', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    // Worker 1: picks up the job, but we force-close it mid-processing
    let w1Started = false;
    const worker1 = new Worker(
      Q,
      async () => {
        w1Started = true;
        // Simulate long-running work that never completes
        await new Promise((r) => setTimeout(r, 60000));
        return 'w1-done';
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
    );
    worker1.on('error', () => {});
    await worker1.waitUntilReady();

    // Add job and wait for worker1 to pick it up
    const job = await queue.add('stall-test', { recover: true });
    while (!w1Started) {
      await new Promise((r) => setTimeout(r, 50));
    }

    // Force-close worker1 - job stays in PEL (not ACKed)
    await worker1.close(true);

    // Verify job is still in PEL
    const pending = await cleanupClient.customCommand([
      'XPENDING', k.stream, CONSUMER_GROUP,
    ]) as any[];
    expect(Number(pending[0])).toBeGreaterThanOrEqual(1);

    // Worker 2: with very short stalledInterval and maxStalledCount=1
    // After 1 reclaim (stalledCount=1 <= maxStalledCount=1), it stays active.
    // After 2nd reclaim (stalledCount=2 > maxStalledCount=1), it moves to failed.
    const stalledEvents: string[] = [];
    const failedEvents: string[] = [];

    const worker2 = new Worker(
      Q,
      async () => {
        // This processor should not be called for the stalled job
        // because XAUTOCLAIM reclaims to PEL but the poll loop reads only '>'
        return 'w2-done';
      },
      {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 500,
        stalledInterval: 1000,
        maxStalledCount: 1,
      },
    );
    worker2.on('error', () => {});
    worker2.on('stalled', (jobId: string) => stalledEvents.push(jobId));
    await worker2.waitUntilReady();

    // Wait for at least 2 stalled recovery cycles (1s interval)
    // First cycle: stalledCount becomes 1, <= maxStalledCount(1), emits 'stalled'
    // Second cycle: stalledCount becomes 2, > maxStalledCount(1), moves to 'failed'
    await new Promise((r) => setTimeout(r, 3500));

    await worker2.close(true);

    // The job should have been moved to failed after exceeding maxStalledCount
    const state = String(await cleanupClient.hget(k.job(job.id), 'state'));
    expect(state).toBe('failed');
    const failedReason = String(await cleanupClient.hget(k.job(job.id), 'failedReason'));
    expect(failedReason).toContain('stalled');

    // Job should be in the failed ZSet
    const failedScore = await cleanupClient.zscore(k.failed, job.id);
    expect(failedScore).not.toBeNull();

    await queue.close();
  }, 20000);

  it('increments stalledCount on the job hash during reclaim', async () => {
    const Q2 = `ew-stalled-count-${TS}`;
    const queue = new Queue(Q2, { connection: CONNECTION });
    const k = buildKeys(Q2);

    // Worker 1: picks up job, force-close
    let w1Started = false;
    const worker1 = new Worker(
      Q2,
      async () => {
        w1Started = true;
        await new Promise((r) => setTimeout(r, 60000));
        return 'never';
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
    );
    worker1.on('error', () => {});
    await worker1.waitUntilReady();

    const job = await queue.add('count-stall', { x: 1 });
    while (!w1Started) {
      await new Promise((r) => setTimeout(r, 50));
    }
    await worker1.close(true);

    // Worker 2: high maxStalledCount so the job doesn't fail
    const worker2 = new Worker(
      Q2,
      async () => {
        return 'w2';
      },
      {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 500,
        stalledInterval: 1000,
        maxStalledCount: 10,
      },
    );
    worker2.on('error', () => {});
    await worker2.waitUntilReady();

    // Wait for one stalled recovery cycle
    await new Promise((r) => setTimeout(r, 2000));

    await worker2.close(true);

    // stalledCount should have been incremented
    const stalledCount = await cleanupClient.hget(k.job(job.id), 'stalledCount');
    expect(Number(stalledCount)).toBeGreaterThanOrEqual(1);

    // Job should still be active (not failed) since maxStalledCount is high
    const state = String(await cleanupClient.hget(k.job(job.id), 'state'));
    expect(state).toBe('active');

    await queue.close();
    await flushQueue(Q2);
  }, 15000);
});
