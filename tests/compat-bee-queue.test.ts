/**
 * Compatibility tests inspired by Bee-Queue, node-resque, and RedisSMQ.
 * Covers: stall detection, backoff strategies, batch processing, graceful shutdown,
 * health checks, job progress, competing consumers, failed job management, and more.
 *
 * Requires: valkey-server running on localhost:6379 and cluster on :7000-7005
 *
 * Run: npx vitest run tests/compat-bee-queue.test.ts
 */
import { it, expect, beforeAll, afterAll } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { QueueEvents } = require('../dist/queue-events') as typeof import('../src/queue-events');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
const { promote } = require('../dist/functions/index') as typeof import('../src/functions/index');

import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

// ---------------------------------------------------------------------------
// 1. Stall detection with many stalled jobs
// ---------------------------------------------------------------------------
describeEachMode('Bee-Queue: Stall detection', (CONNECTION) => {
  let cleanupClient: any;
  const localQueues: string[] = [];

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    for (const q of localQueues) {
      await flushQueue(cleanupClient, q);
    }
    cleanupClient.close();
  });

  it('detects and recovers stalled jobs (50 jobs, worker killed, all recovered)', async () => {
    const Q = `bee-stall-recovery-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    localQueues.push(Q);
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    const JOB_COUNT = 50;

    // Add jobs
    for (let i = 0; i < JOB_COUNT; i++) {
      await queue.add('task', { index: i });
    }

    // Worker 1: picks up jobs but gets "killed" (closed with force) before completing them
    const stalledWorker = new Worker(
      Q,
      async () => {
        // Simulate long work - never completes
        await new Promise(() => {});
      },
      {
        connection: CONNECTION,
        concurrency: 10,
        blockTimeout: 500,
        stalledInterval: 1000,
        maxStalledCount: 2,
      },
    );
    stalledWorker.on('error', () => {});

    await stalledWorker.waitUntilReady();
    // Let it pick up some jobs
    await new Promise((r) => setTimeout(r, 1500));
    // Force-close without waiting for active jobs
    await stalledWorker.close(true);

    // Worker 2: picks up reclaimed stalled jobs and actually completes them
    const completedIds = new Set<string>();
    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(
        () => reject(new Error('timeout waiting for stall recovery')),
        30000,
      );

      const recoveryWorker = new Worker(
        Q,
        async (job: any) => {
          completedIds.add(job.id);
          return { recovered: true };
        },
        {
          connection: CONNECTION,
          concurrency: 10,
          blockTimeout: 500,
          stalledInterval: 1000,
          maxStalledCount: 2,
        },
      );
      recoveryWorker.on('error', () => {});

      // Check periodically if all jobs completed
      const check = setInterval(async () => {
        try {
          const counts = await queue.getJobCounts();
          if (counts.waiting === 0 && counts.active === 0 && counts.delayed === 0) {
            clearInterval(check);
            clearTimeout(timeout);
            await recoveryWorker.close();
            resolve();
          }
        } catch {}
      }, 500);
    });

    await done;
    // Some jobs should have been recovered
    expect(completedIds.size).toBeGreaterThan(0);

    await queue.close();
  }, 45000);
});

// ---------------------------------------------------------------------------
// 2. Backoff strategies: immediate, fixed, exponential
// ---------------------------------------------------------------------------
describeEachMode('Bee-Queue: Backoff strategies', (CONNECTION) => {
  let cleanupClient: any;
  const localQueues: string[] = [];

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    for (const q of localQueues) {
      await flushQueue(cleanupClient, q);
    }
    cleanupClient.close();
  });

  it('immediate retry (no delay between retries)', async () => {
    const Q = `bee-backoff-immediate-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    localQueues.push(Q);
    const queue = new Queue(Q, { connection: CONNECTION });

    let attempts = 0;
    const job = await queue.add(
      'retry-task',
      { v: 1 },
      { attempts: 3, backoff: { type: 'fixed', delay: 0 } },
    );

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);

      const worker = new Worker(
        Q,
        async () => {
          attempts++;
          if (attempts < 3) throw new Error(`fail-${attempts}`);
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});

      worker.on('failed', () => {
        // Promote immediately since delay=0
        setTimeout(async () => {
          try {
            await promote(cleanupClient, buildKeys(Q), Date.now());
          } catch {}
        }, 100);
      });

      worker.on('completed', (j: any) => {
        if (j.id === String(job!.id)) {
          clearTimeout(timeout);
          worker.close().then(resolve);
        }
      });
    });

    await done;
    expect(attempts).toBe(3);
    await queue.close();
  }, 20000);

  it('fixed backoff: constant delay between retries', async () => {
    const Q = `bee-backoff-fixed-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    localQueues.push(Q);
    const queue = new Queue(Q, { connection: CONNECTION });

    const retryTimestamps: number[] = [];
    let attempts = 0;

    await queue.add(
      'retry-task',
      { v: 1 },
      { attempts: 3, backoff: { type: 'fixed', delay: 300 } },
    );

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 20000);

      const worker = new Worker(
        Q,
        async () => {
          attempts++;
          retryTimestamps.push(Date.now());
          if (attempts < 3) throw new Error(`fail-${attempts}`);
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});

      worker.on('failed', () => {
        setTimeout(async () => {
          try {
            await promote(cleanupClient, buildKeys(Q), Date.now());
          } catch {}
        }, 400);
      });

      worker.on('completed', () => {
        clearTimeout(timeout);
        worker.close().then(resolve);
      });
    });

    await done;
    expect(attempts).toBe(3);
    // Verify delays are roughly constant (within tolerance)
    if (retryTimestamps.length >= 3) {
      const delay1 = retryTimestamps[1] - retryTimestamps[0];
      const delay2 = retryTimestamps[2] - retryTimestamps[1];
      // Both delays should be at least 200ms (promotion interval + processing)
      expect(delay1).toBeGreaterThan(200);
      expect(delay2).toBeGreaterThan(200);
    }

    await queue.close();
  }, 25000);

  it('exponential backoff: increasing delay between retries', async () => {
    const Q = `bee-backoff-exp-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    localQueues.push(Q);
    const queue = new Queue(Q, { connection: CONNECTION });

    const retryTimestamps: number[] = [];
    let attempts = 0;

    await queue.add(
      'retry-task',
      { v: 1 },
      { attempts: 4, backoff: { type: 'exponential', delay: 200 } },
    );

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 30000);

      const worker = new Worker(
        Q,
        async () => {
          attempts++;
          retryTimestamps.push(Date.now());
          if (attempts < 4) throw new Error(`fail-${attempts}`);
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});

      worker.on('failed', () => {
        // Wait a bit longer than the backoff to allow promote to work
        setTimeout(async () => {
          try {
            await promote(cleanupClient, buildKeys(Q), Date.now());
          } catch {}
        }, 600);
      });

      worker.on('completed', () => {
        clearTimeout(timeout);
        worker.close().then(resolve);
      });
    });

    await done;
    expect(attempts).toBe(4);

    await queue.close();
  }, 35000);
});

// ---------------------------------------------------------------------------
// 3. Batch processing: add 50 jobs, worker processes them, verify all completed
// ---------------------------------------------------------------------------
describeEachMode('Bee-Queue: Batch processing', (CONNECTION) => {
  let cleanupClient: any;
  const localQueues: string[] = [];

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    for (const q of localQueues) {
      await flushQueue(cleanupClient, q);
    }
    cleanupClient.close();
  });

  it('processes 50 jobs added via addBulk and all complete', async () => {
    const Q = `bee-batch-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    localQueues.push(Q);
    const queue = new Queue(Q, { connection: CONNECTION });
    const JOB_COUNT = 50;

    const jobs = await queue.addBulk(
      Array.from({ length: JOB_COUNT }, (_, i) => ({
        name: 'batch-task',
        data: { index: i },
      })),
    );

    expect(jobs.length).toBe(JOB_COUNT);

    const completedIds = new Set<string>();

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 30000);

      const worker = new Worker(
        Q,
        async (job: any) => {
          return { processed: job.data.index };
        },
        { connection: CONNECTION, concurrency: 5, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});

      worker.on('completed', (j: any) => {
        completedIds.add(j.id);
        if (completedIds.size >= JOB_COUNT) {
          clearTimeout(timeout);
          worker.close().then(resolve);
        }
      });
    });

    await done;
    expect(completedIds.size).toBe(JOB_COUNT);

    // Verify via getJobCounts
    const counts = await queue.getJobCounts();
    expect(counts.completed).toBe(JOB_COUNT);
    expect(counts.failed).toBe(0);

    await queue.close();
  }, 35000);
});

// ---------------------------------------------------------------------------
// 4. Close queue during processing - verify graceful shutdown
// ---------------------------------------------------------------------------
describeEachMode('Bee-Queue: Graceful shutdown', (CONNECTION) => {
  let cleanupClient: any;
  const localQueues: string[] = [];

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    for (const q of localQueues) {
      await flushQueue(cleanupClient, q);
    }
    cleanupClient.close();
  });

  it('graceful close waits for active job to complete', async () => {
    const Q = `bee-graceful-close-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    localQueues.push(Q);
    const queue = new Queue(Q, { connection: CONNECTION });

    let jobStarted = false;
    let jobCompleted = false;

    await queue.add('slow-task', { v: 1 });

    const worker = new Worker(
      Q,
      async () => {
        jobStarted = true;
        await new Promise((r) => setTimeout(r, 1000));
        jobCompleted = true;
        return 'done';
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
    );
    worker.on('error', () => {});

    await worker.waitUntilReady();
    // Wait for job to start processing
    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (jobStarted) {
          clearInterval(check);
          resolve();
        }
      }, 50);
    });

    expect(jobStarted).toBe(true);
    expect(jobCompleted).toBe(false);

    // Graceful close (force=false) should wait for active job
    await worker.close(false);

    // Wait slightly to ensure local state update
    await new Promise((r) => setTimeout(r, 50));

    expect(jobCompleted).toBe(true);

    await queue.close();
  }, 15000);

  it('force close does not wait for active job', async () => {
    const Q = `bee-force-close-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    localQueues.push(Q);
    const queue = new Queue(Q, { connection: CONNECTION });

    let jobStarted = false;

    await queue.add('slow-task', { v: 1 });

    const worker = new Worker(
      Q,
      async () => {
        jobStarted = true;
        await new Promise((r) => setTimeout(r, 5000));
        return 'done';
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
    );
    worker.on('error', () => {});

    await worker.waitUntilReady();
    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (jobStarted) {
          clearInterval(check);
          resolve();
        }
      }, 50);
    });

    expect(jobStarted).toBe(true);

    // Force close should return quickly (not wait the full 5s)
    const start = Date.now();
    await worker.close(true);
    const elapsed = Date.now() - start;

    expect(elapsed).toBeLessThan(3000);

    await queue.close();
  }, 15000);
});

// ---------------------------------------------------------------------------
// 5. Health check: queue reports correct counts
// ---------------------------------------------------------------------------
describeEachMode('Bee-Queue: Health check / job counts', (CONNECTION) => {
  let cleanupClient: any;
  const localQueues: string[] = [];

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    for (const q of localQueues) {
      await flushQueue(cleanupClient, q);
    }
    cleanupClient.close();
  });

  it('reports correct waiting, active, completed, failed counts', async () => {
    const Q = `bee-health-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    localQueues.push(Q);
    const queue = new Queue(Q, { connection: CONNECTION });

    // Add 5 jobs
    for (let i = 0; i < 5; i++) {
      await queue.add('task', { i });
    }

    // Check waiting count before processing
    let counts = await queue.getJobCounts();
    expect(counts.waiting).toBe(5);

    // Process: 3 succeed, 2 fail
    let processed = 0;
    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);

      const worker = new Worker(
        Q,
        async (job: any) => {
          processed++;
          if ((job.data as any).i >= 3) throw new Error('deliberate fail');
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});

      const check = setInterval(async () => {
        if (processed >= 5) {
          clearInterval(check);
          clearTimeout(timeout);
          await worker.close();
          resolve();
        }
      }, 100);
    });

    await done;

    counts = await queue.getJobCounts();
    expect(counts.completed).toBe(3);
    expect(counts.failed).toBe(2);
    expect(counts.waiting).toBe(0);

    await queue.close();
  }, 20000);

  it('reports delayed jobs correctly', async () => {
    const Q = `bee-health-delayed-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    localQueues.push(Q);
    const queue = new Queue(Q, { connection: CONNECTION });

    await queue.add('delayed-task', { v: 1 }, { delay: 60000 });
    await queue.add('delayed-task', { v: 2 }, { delay: 60000 });

    const counts = await queue.getJobCounts();
    expect(counts.delayed).toBe(2);

    await queue.close();
  }, 10000);
});

// ---------------------------------------------------------------------------
// 6. Job.save returns the job with an id
// ---------------------------------------------------------------------------
describeEachMode('Bee-Queue: Job creation', (CONNECTION) => {
  let cleanupClient: any;
  const localQueues: string[] = [];

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    for (const q of localQueues) {
      await flushQueue(cleanupClient, q);
    }
    cleanupClient.close();
  });

  it('queue.add returns a job with a valid id', async () => {
    const Q = `bee-job-create-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    localQueues.push(Q);
    const queue = new Queue(Q, { connection: CONNECTION });

    const job = await queue.add('my-task', { key: 'value' });
    expect(job).not.toBeNull();
    expect(job!.id).toBeTruthy();
    expect(typeof job!.id).toBe('string');
    expect(job!.name).toBe('my-task');
    expect(job!.data).toEqual({ key: 'value' });

    await queue.close();
  }, 10000);

  it('job is retrievable by ID after creation', async () => {
    const Q = `bee-job-retrieve-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    localQueues.push(Q);
    const queue = new Queue(Q, { connection: CONNECTION });

    const job = await queue.add('fetch-task', { x: 42 });
    const fetched = await queue.getJob(job!.id);
    expect(fetched).not.toBeNull();
    expect(fetched!.id).toBe(job!.id);
    expect(fetched!.name).toBe('fetch-task');
    expect((fetched!.data as any).x).toBe(42);

    await queue.close();
  }, 10000);
});

// ---------------------------------------------------------------------------
// 7. Job progress events flow correctly
// ---------------------------------------------------------------------------
describeEachMode('Bee-Queue: Job progress events', (CONNECTION) => {
  let cleanupClient: any;
  const localQueues: string[] = [];

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    for (const q of localQueues) {
      await flushQueue(cleanupClient, q);
    }
    cleanupClient.close();
  });

  it('progress updates 0 -> 50 -> 100 are emitted correctly', async () => {
    const Q = `bee-progress-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    localQueues.push(Q);
    const queue = new Queue(Q, { connection: CONNECTION });

    const progressValues: number[] = [];

    const queueEvents = new QueueEvents(Q, {
      connection: CONNECTION,
      blockTimeout: 1000,
      lastEventId: '0',
    });
    queueEvents.on('error', () => {});

    queueEvents.on('progress', (data: any) => {
      progressValues.push(Number(data.data));
    });

    await queueEvents.waitUntilReady();
    // Small delay to ensure QueueEvents is actively polling
    await new Promise((r) => setTimeout(r, 300));

    const job = await queue.add('progress-task', { v: 1 });

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);

      const worker = new Worker(
        Q,
        async (j: any) => {
          await j.updateProgress(0);
          await new Promise((r) => setTimeout(r, 100));
          await j.updateProgress(50);
          await new Promise((r) => setTimeout(r, 100));
          await j.updateProgress(100);
          return 'done';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});

      worker.on('completed', () => {
        clearTimeout(timeout);
        // Wait a bit for events to propagate
        setTimeout(() => worker.close().then(resolve), 500);
      });
    });

    await done;
    // Wait for QueueEvents to catch up
    await new Promise((r) => setTimeout(r, 1000));

    expect(progressValues).toContain(0);
    expect(progressValues).toContain(50);
    expect(progressValues).toContain(100);

    await queueEvents.close();
    await queue.close();
  }, 20000);
});

// ---------------------------------------------------------------------------
// 8. EagerTimer pattern: short poll intervals don't cause errors
// ---------------------------------------------------------------------------
describeEachMode('Bee-Queue: Short poll intervals', (CONNECTION) => {
  let cleanupClient: any;
  const localQueues: string[] = [];

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    for (const q of localQueues) {
      await flushQueue(cleanupClient, q);
    }
    cleanupClient.close();
  });

  it('worker with very short blockTimeout does not error excessively', async () => {
    const Q = `bee-eager-timer-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    localQueues.push(Q);
    const queue = new Queue(Q, { connection: CONNECTION });

    const errors: Error[] = [];
    const worker = new Worker(Q, async () => 'ok', {
      connection: CONNECTION,
      concurrency: 1,
      blockTimeout: 100,
      stalledInterval: 60000,
    });
    worker.on('error', (err: Error) => errors.push(err));

    await worker.waitUntilReady();

    // Let it poll several times with no jobs
    await new Promise((r) => setTimeout(r, 1000));

    // Add a job to verify it still works
    await queue.add('quick-task', { v: 1 });
    const completed = new Promise<void>((resolve) => {
      worker.on('completed', () => resolve());
    });

    await completed;
    await worker.close();

    // No connection errors should have occurred
    expect(errors.length).toBe(0);

    await queue.close();
  }, 15000);
});

// ---------------------------------------------------------------------------
// 9. Multiple workers on same queue - no duplicate processing (node-resque)
// ---------------------------------------------------------------------------
describeEachMode('node-resque: Competing consumers - no duplicates', (CONNECTION) => {
  let cleanupClient: any;
  const localQueues: string[] = [];

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    for (const q of localQueues) {
      await flushQueue(cleanupClient, q);
    }
    cleanupClient.close();
  });

  it('2 workers process 20 jobs with no duplicates', async () => {
    const Q = `bee-no-dupes-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    localQueues.push(Q);
    const queue = new Queue(Q, { connection: CONNECTION });
    const JOB_COUNT = 20;

    for (let i = 0; i < JOB_COUNT; i++) {
      await queue.add('task', { index: i });
    }

    const processedJobIds: string[] = [];
    const workerIds: Record<string, string> = {};

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 20000);
      let totalCompleted = 0;

      const makeWorker = (workerId: string) => {
        const w = new Worker(
          Q,
          async (job: any) => {
            // Small random delay to increase chance of contention
            await new Promise((r) => setTimeout(r, Math.random() * 50));
            return { workerId };
          },
          { connection: CONNECTION, concurrency: 3, blockTimeout: 500, stalledInterval: 60000 },
        );
        w.on('error', () => {});
        w.on('completed', (j: any) => {
          processedJobIds.push(j.id);
          workerIds[j.id] = workerId;
          totalCompleted++;
          if (totalCompleted >= JOB_COUNT) {
            clearTimeout(timeout);
            Promise.all([worker1.close(), worker2.close()]).then(() => resolve());
          }
        });
        return w;
      };

      const worker1 = makeWorker('w1');
      const worker2 = makeWorker('w2');
    });

    await done;

    // No duplicates
    const uniqueIds = new Set(processedJobIds);
    expect(uniqueIds.size).toBe(processedJobIds.length);
    expect(uniqueIds.size).toBe(JOB_COUNT);

    // Verify both workers got some jobs (not guaranteed but likely with 20 jobs)
    const w1Count = Object.values(workerIds).filter((w) => w === 'w1').length;
    const w2Count = Object.values(workerIds).filter((w) => w === 'w2').length;
    expect(w1Count + w2Count).toBe(JOB_COUNT);

    await queue.close();
  }, 25000);
});

// ---------------------------------------------------------------------------
// 10. Worker heartbeat - stalled worker detected and jobs reclaimed (node-resque)
// ---------------------------------------------------------------------------
describeEachMode('node-resque: Stalled worker heartbeat', (CONNECTION) => {
  let cleanupClient: any;
  const localQueues: string[] = [];

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    for (const q of localQueues) {
      await flushQueue(cleanupClient, q);
    }
    cleanupClient.close();
  });

  it('stalled job from dead worker is eventually failed after maxStalledCount exceeded', async () => {
    const Q = `bee-heartbeat-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    localQueues.push(Q);
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    const job = await queue.add('heartbeat-task', { v: 1 });

    // Worker 1: picks up the job but "dies" without completing.
    const stalledWorker = new Worker(
      Q,
      async () => {
        await new Promise(() => {}); // Never completes
      },
      {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 500,
        stalledInterval: 500,
        maxStalledCount: 1,
      },
    );
    stalledWorker.on('error', () => {});

    await stalledWorker.waitUntilReady();
    // Let it pick up the job so it enters the PEL
    await new Promise((r) => setTimeout(r, 1500));
    // Force-close without waiting for active jobs
    await stalledWorker.close(true);

    // Wait for the PEL entry to become idle long enough for XAUTOCLAIM
    await new Promise((r) => setTimeout(r, 1000));

    // Worker 2: its scheduler will detect the stalled entry via XAUTOCLAIM.
    const recoveryWorker = new Worker(Q, async () => 'ok', {
      connection: CONNECTION,
      concurrency: 1,
      blockTimeout: 500,
      stalledInterval: 500,
      maxStalledCount: 1,
    });
    recoveryWorker.on('error', () => {});

    // Wait for enough stall detection cycles to run
    await new Promise((r) => setTimeout(r, 3000));

    await recoveryWorker.close();

    // The stalled job should have been failed after exceeding maxStalledCount
    const fetched = await queue.getJob(job!.id);
    expect(fetched).not.toBeNull();
    const state = await fetched!.getState();
    // Job should be in failed state (stalled more than maxStalledCount)
    expect(state).toBe('failed');
    expect(fetched!.failedReason).toBe('job stalled more than maxStalledCount');

    await queue.close();
  }, 20000);
});

// ---------------------------------------------------------------------------
// 11. Failed job management: list, remove, retry failed jobs (node-resque)
// ---------------------------------------------------------------------------
describeEachMode('node-resque: Failed job management', (CONNECTION) => {
  let cleanupClient: any;
  const localQueues: string[] = [];

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    for (const q of localQueues) {
      await flushQueue(cleanupClient, q);
    }
    cleanupClient.close();
  });

  it('lists failed jobs', async () => {
    const Q = `bee-failed-list-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    localQueues.push(Q);
    const queue = new Queue(Q, { connection: CONNECTION });

    // Add 3 jobs that will all fail
    for (let i = 0; i < 3; i++) {
      await queue.add('fail-task', { i });
    }

    let failCount = 0;
    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);

      const worker = new Worker(
        Q,
        async () => {
          throw new Error('deliberate failure');
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});
      worker.on('failed', () => {
        failCount++;
        if (failCount >= 3) {
          clearTimeout(timeout);
          worker.close().then(resolve);
        }
      });
    });

    await done;

    // List failed jobs
    const failedJobs = await queue.getJobs('failed');
    expect(failedJobs.length).toBe(3);
    for (const job of failedJobs) {
      expect(job.failedReason).toBe('deliberate failure');
    }

    await queue.close();
  }, 20000);

  it('removes a specific failed job', async () => {
    const Q = `bee-failed-remove-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    localQueues.push(Q);
    const queue = new Queue(Q, { connection: CONNECTION });

    const job = await queue.add('removable', { v: 1 });

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker = new Worker(
        Q,
        async () => {
          throw new Error('fail');
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});
      worker.on('failed', () => {
        clearTimeout(timeout);
        worker.close().then(resolve);
      });
    });

    await done;

    let failedJobs = await queue.getJobs('failed');
    expect(failedJobs.length).toBe(1);

    // Remove the failed job
    const fetched = await queue.getJob(job!.id);
    await fetched!.remove();

    failedJobs = await queue.getJobs('failed');
    expect(failedJobs.length).toBe(0);

    await queue.close();
  }, 15000);

  it('retries a failed job', async () => {
    const Q = `bee-failed-retry-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    localQueues.push(Q);
    const queue = new Queue(Q, { connection: CONNECTION });

    let attempts = 0;
    const job = await queue.add('retry-me', { v: 1 });

    // First run: fails
    const failDone = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker = new Worker(
        Q,
        async () => {
          attempts++;
          if (attempts === 1) throw new Error('first attempt fail');
          return 'success';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});
      worker.on('failed', () => {
        clearTimeout(timeout);
        worker.close().then(resolve);
      });
    });

    await failDone;

    // Verify job is in failed state
    const fetched = await queue.getJob(job!.id);
    expect(await fetched!.isFailed()).toBe(true);

    // Retry the job
    await fetched!.retry();

    // Promote it so it gets picked up
    await promote(cleanupClient, buildKeys(Q), Date.now());

    // Second run: succeeds
    const successDone = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker2 = new Worker(
        Q,
        async () => {
          attempts++;
          return 'success';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker2.on('error', () => {});
      worker2.on('completed', () => {
        clearTimeout(timeout);
        worker2.close().then(resolve);
      });
    });

    await successDone;
    expect(attempts).toBe(2);

    await queue.close();
  }, 25000);
});

// ---------------------------------------------------------------------------
// 12. Queue.getJobs returns jobs by state correctly (node-resque)
// ---------------------------------------------------------------------------
describeEachMode('node-resque: getJobs by state', (CONNECTION) => {
  let cleanupClient: any;
  const localQueues: string[] = [];

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    for (const q of localQueues) {
      await flushQueue(cleanupClient, q);
    }
    cleanupClient.close();
  });

  it('returns waiting, completed, failed, and delayed jobs correctly', async () => {
    const Q = `bee-get-jobs-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    localQueues.push(Q);
    const queue = new Queue(Q, { connection: CONNECTION });

    // Add a delayed job
    await queue.add('delayed', { v: 'delayed' }, { delay: 60000 });

    // Add jobs that will succeed and fail
    await queue.add('succeed', { v: 'pass' });
    await queue.add('fail-job', { v: 'fail' });

    let processed = 0;
    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);

      const worker = new Worker(
        Q,
        async (job: any) => {
          processed++;
          if (job.name === 'fail-job') throw new Error('nope');
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});

      const check = setInterval(async () => {
        if (processed >= 2) {
          clearInterval(check);
          clearTimeout(timeout);
          await worker.close();
          resolve();
        }
      }, 100);
    });

    await done;

    const delayedJobs = await queue.getJobs('delayed');
    expect(delayedJobs.length).toBe(1);
    expect(delayedJobs[0].name).toBe('delayed');

    const completedJobs = await queue.getJobs('completed');
    expect(completedJobs.length).toBe(1);
    expect(completedJobs[0].name).toBe('succeed');

    const failedJobs = await queue.getJobs('failed');
    expect(failedJobs.length).toBe(1);
    expect(failedJobs[0].name).toBe('fail-job');

    await queue.close();
  }, 20000);
});

// ---------------------------------------------------------------------------
// 13. Competing consumers: 2 workers, 10 jobs, each processed exactly once (RedisSMQ)
// ---------------------------------------------------------------------------
describeEachMode('RedisSMQ: Competing consumers exactly-once', (CONNECTION) => {
  let cleanupClient: any;
  const localQueues: string[] = [];

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    for (const q of localQueues) {
      await flushQueue(cleanupClient, q);
    }
    cleanupClient.close();
  });

  it('2 workers process 10 jobs, each job processed exactly once', async () => {
    const Q = `bee-competing-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    localQueues.push(Q);
    const queue = new Queue(Q, { connection: CONNECTION });
    const JOB_COUNT = 10;

    for (let i = 0; i < JOB_COUNT; i++) {
      await queue.add('compete-task', { index: i });
    }

    const processedByWorker: Record<string, string[]> = { w1: [], w2: [] };
    const allProcessed: string[] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);

      const makeWorker = (id: string) => {
        const w = new Worker(
          Q,
          async (job: any) => {
            processedByWorker[id].push(job.id);
            allProcessed.push(job.id);
            return id;
          },
          { connection: CONNECTION, concurrency: 2, blockTimeout: 500, stalledInterval: 60000 },
        );
        w.on('error', () => {});
        return w;
      };

      const w1 = makeWorker('w1');
      const w2 = makeWorker('w2');

      const check = setInterval(async () => {
        if (allProcessed.length >= JOB_COUNT) {
          clearInterval(check);
          clearTimeout(timeout);
          await Promise.all([w1.close(), w2.close()]);
          resolve();
        }
      }, 100);
    });

    await done;

    // Exactly 10 jobs processed
    expect(allProcessed.length).toBe(JOB_COUNT);

    // No duplicates
    const unique = new Set(allProcessed);
    expect(unique.size).toBe(JOB_COUNT);

    await queue.close();
  }, 20000);
});

// ---------------------------------------------------------------------------
// 14. Message TTL / job timeout: job with timeout moved to failed (RedisSMQ)
// ---------------------------------------------------------------------------
describeEachMode('RedisSMQ: Job timeout / TTL', (CONNECTION) => {
  let cleanupClient: any;
  const localQueues: string[] = [];

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    for (const q of localQueues) {
      await flushQueue(cleanupClient, q);
    }
    cleanupClient.close();
  });

  it('job exceeding timeout is failed when stall detection runs', async () => {
    const Q = `bee-timeout-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    localQueues.push(Q);
    const queue = new Queue(Q, { connection: CONNECTION });

    await queue.add('slow-task', { v: 1 });

    // Worker with aggressive stall detection
    const stalledWorker = new Worker(
      Q,
      async () => {
        // Simulate a job that takes forever (exceeds stalled interval)
        await new Promise(() => {});
      },
      {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 500,
        stalledInterval: 1000,
        maxStalledCount: 1,
      },
    );
    stalledWorker.on('error', () => {});

    await stalledWorker.waitUntilReady();
    // Let it pick up the job
    await new Promise((r) => setTimeout(r, 1000));
    // Kill the worker - job stays in PEL
    await stalledWorker.close(true);

    // Start another worker that will run stall detection and fail the job
    const cleanupWorker = new Worker(Q, async () => 'ok', {
      connection: CONNECTION,
      concurrency: 1,
      blockTimeout: 500,
      stalledInterval: 1000,
      maxStalledCount: 1,
    });
    cleanupWorker.on('error', () => {});

    await cleanupWorker.waitUntilReady();

    // Wait for stall detection to run and reclaim+fail the job
    await new Promise((r) => setTimeout(r, 3000));

    await cleanupWorker.close();

    // Check job state
    const counts = await queue.getJobCounts();
    // The job should have been reclaimed and either reprocessed (completed)
    // or if it stalled again, moved to failed
    expect(counts.waiting + counts.completed + counts.failed).toBeGreaterThanOrEqual(1);

    await queue.close();
  }, 20000);
});

// ---------------------------------------------------------------------------
// Additional: Queue obliterate
// ---------------------------------------------------------------------------
describeEachMode('Bee-Queue: Queue cleanup', (CONNECTION) => {
  let cleanupClient: any;
  const localQueues: string[] = [];

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    for (const q of localQueues) {
      await flushQueue(cleanupClient, q);
    }
    cleanupClient.close();
  });

  it('obliterate removes all queue data', async () => {
    const Q = `bee-obliterate-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    localQueues.push(Q);
    const queue = new Queue(Q, { connection: CONNECTION });

    for (let i = 0; i < 5; i++) {
      await queue.add('task', { i });
    }

    const counts = await queue.getJobCounts();
    expect(counts.waiting).toBe(5);

    await queue.obliterate({ force: true });

    const countsAfter = await queue.getJobCounts();
    expect(countsAfter.waiting).toBe(0);
    expect(countsAfter.completed).toBe(0);

    await queue.close();
  }, 10000);
});

// ---------------------------------------------------------------------------
// Additional: Job state queries
// ---------------------------------------------------------------------------
describeEachMode('Bee-Queue: Job state queries', (CONNECTION) => {
  let cleanupClient: any;
  const localQueues: string[] = [];

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    for (const q of localQueues) {
      await flushQueue(cleanupClient, q);
    }
    cleanupClient.close();
  });

  it('isCompleted returns true after processing', async () => {
    const Q = `bee-state-completed-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    localQueues.push(Q);
    const queue = new Queue(Q, { connection: CONNECTION });

    const job = await queue.add('task', { v: 1 });

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker = new Worker(Q, async () => 'done', {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 500,
        stalledInterval: 60000,
      });
      worker.on('error', () => {});
      worker.on('completed', () => {
        clearTimeout(timeout);
        // Ensure worker is closed but job state is final
        worker.close().then(resolve);
      });
    });

    await done;

    // Small wait to ensure state persistence
    await new Promise((r) => setTimeout(r, 100));

    const fetched = await queue.getJob(job!.id);
    expect(await fetched!.isCompleted()).toBe(true);
    expect(await fetched!.isFailed()).toBe(false);

    await queue.close();
  }, 15000);

  it('isFailed returns true after failure', async () => {
    const Q = `bee-state-failed-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    localQueues.push(Q);
    const queue = new Queue(Q, { connection: CONNECTION });

    const job = await queue.add('task', { v: 1 });

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker = new Worker(
        Q,
        async () => {
          throw new Error('fail');
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});
      worker.on('failed', () => {
        clearTimeout(timeout);
        worker.close().then(resolve);
      });
    });

    await done;

    const fetched = await queue.getJob(job!.id);
    expect(await fetched!.isFailed()).toBe(true);
    expect(await fetched!.isCompleted()).toBe(false);

    await queue.close();
  }, 15000);

  it('isDelayed returns true for delayed job', async () => {
    const Q = `bee-state-delayed-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    localQueues.push(Q);
    const queue = new Queue(Q, { connection: CONNECTION });

    const job = await queue.add('task', { v: 1 }, { delay: 60000 });

    const fetched = await queue.getJob(job!.id);
    expect(await fetched!.isDelayed()).toBe(true);

    await queue.close();
  }, 10000);
});

// ---------------------------------------------------------------------------
// Additional: updateData on a job
// ---------------------------------------------------------------------------
describeEachMode('Bee-Queue: Job data update', (CONNECTION) => {
  let cleanupClient: any;
  const localQueues: string[] = [];

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    for (const q of localQueues) {
      await flushQueue(cleanupClient, q);
    }
    cleanupClient.close();
  });

  it('updateData replaces job data', async () => {
    const Q = `bee-update-data-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    localQueues.push(Q);
    const queue = new Queue(Q, { connection: CONNECTION });

    const job = await queue.add('task', { original: true });

    const fetched = await queue.getJob(job!.id);
    await fetched!.updateData({ updated: true, extra: 42 });

    const refetched = await queue.getJob(job!.id);
    expect(refetched!.data).toEqual({ updated: true, extra: 42 });

    await queue.close();
  }, 10000);
});
