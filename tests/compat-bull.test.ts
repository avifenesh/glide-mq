/**
 * Bull compatibility tests - adapted from Bull's test_queue.js and test_job.js.
 * Validates that glide-mq handles the same queue/job lifecycle patterns Bull users expect.
 *
 * Requires: valkey-server running on localhost:6379 and cluster on :7000-7005
 *
 * Run: npx vitest run tests/compat-bull.test.ts
 */
import { it, expect, beforeAll, afterAll } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

const TS = Date.now();

// ---------------------------------------------------------------------------
// test_queue.js patterns
// ---------------------------------------------------------------------------

describeEachMode('Bull compat: Queue.close', (CONNECTION) => {
  const Q = `bull-close-${TS}`;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    await flushQueue(cleanupClient, `${Q}-resolve-${TS}`);
    cleanupClient.close();
  });

  it('1. close terminates the client connection (client becomes null)', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    // Force client init by calling an operation
    await queue.add('probe', { x: 1 });
    await queue.close();
    // After close, internal client should be null - getClient should throw
    await expect(queue.add('after-close', {})).rejects.toThrow();
  });

  it('2. close resolves only after disconnect', async () => {
    const queue = new Queue(`${Q}-resolve-${TS}`, { connection: CONNECTION });
    await queue.add('probe', { x: 1 });
    // close() returns a promise that should resolve (not hang or reject)
    await expect(queue.close()).resolves.toBeUndefined();
  });
});

describeEachMode('Bull compat: Queue processes a job', (CONNECTION) => {
  const Q = `bull-process-${TS}`;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('3. add -> process -> completed', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const processed: string[] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker = new Worker(
        Q,
        async (job: any) => {
          processed.push(job.id);
          return { status: 'done' };
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500 },
      );
      worker.on('error', () => {});
      worker.on('completed', (job: any, result: any) => {
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });
    });

    await new Promise((r) => setTimeout(r, 500));
    const job = await queue.add('task', { payload: 'hello' });
    await done;

    expect(processed).toContain(job!.id);
    await queue.close();
  }, 15000);
});

describeEachMode('Bull compat: Serial processing', (CONNECTION) => {
  const Q = `bull-serial-${TS}`;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('4. processes 10 jobs serially with concurrency=1, verifying order', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const order: number[] = [];
    const JOB_COUNT = 10;

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 20000);
      const worker = new Worker(
        Q,
        async (job: any) => {
          order.push(job.data.seq);
          if (order.length >= JOB_COUNT) {
            clearTimeout(timeout);
            setTimeout(() => worker.close(true).then(resolve), 200);
          }
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500 },
      );
      worker.on('error', () => {});
    });

    await new Promise((r) => setTimeout(r, 500));
    for (let i = 0; i < JOB_COUNT; i++) {
      await queue.add(`serial-${i}`, { seq: i });
    }
    await done;

    expect(order).toHaveLength(JOB_COUNT);
    // FIFO: should be monotonically increasing
    for (let i = 1; i < order.length; i++) {
      expect(order[i]).toBeGreaterThan(order[i - 1]);
    }
    await queue.close();
  }, 25000);
});

describeEachMode('Bull compat: Custom data types', (CONNECTION) => {
  const Q = `bull-data-${TS}`;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('5. processes jobs with object, array, and nested data', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const received: any[] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker = new Worker(
        Q,
        async (job: any) => {
          received.push(job.data);
          if (received.length >= 3) {
            clearTimeout(timeout);
            setTimeout(() => worker.close(true).then(resolve), 200);
          }
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500 },
      );
      worker.on('error', () => {});
    });

    await new Promise((r) => setTimeout(r, 500));
    await queue.add('obj', { name: 'test', count: 42 });
    await queue.add('arr', [1, 2, 3] as any);
    await queue.add('nested', { a: { b: { c: [1, { d: true }] } } });
    await done;

    expect(received).toHaveLength(3);
    expect(received[0]).toEqual({ name: 'test', count: 42 });
    expect(received[1]).toEqual([1, 2, 3]);
    expect(received[2]).toEqual({ a: { b: { c: [1, { d: true }] } } });
    await queue.close();
  }, 15000);
});

describeEachMode('Bull compat: Retry on failure', (CONNECTION) => {
  const Q = `bull-retry-${TS}`;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    await flushQueue(cleanupClient, `bull-retry-backoff-${TS}`);
    cleanupClient.close();
  });

  it('6. retries a job that fails (attempts=3, verify attemptsMade)', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    let attempts = 0;

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);
      const worker = new Worker(
        Q,
        async () => {
          attempts++;
          if (attempts < 3) {
            throw new Error(`fail attempt ${attempts}`);
          }
          return 'success on third';
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
    await queue.add('retry-job', { x: 1 }, { attempts: 3, backoff: { type: 'fixed', delay: 200 } });
    await done;

    expect(attempts).toBe(3);
    await queue.close();
  }, 20000);

  it('7. retries with fixed backoff delay', async () => {
    const Q2 = `bull-retry-backoff-${TS}`;
    const queue = new Queue(Q2, { connection: CONNECTION });
    const timestamps: number[] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 20000);
      const worker = new Worker(
        Q2,
        async () => {
          timestamps.push(Date.now());
          if (timestamps.length < 3) {
            throw new Error('fail');
          }
          return 'ok';
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 500,
          stalledInterval: 60000,
          promotionInterval: 200,
        },
      );
      worker.on('error', () => {});
      worker.on('completed', () => {
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });
    });

    await new Promise((r) => setTimeout(r, 500));
    await queue.add('backoff-job', {}, { attempts: 3, backoff: { type: 'fixed', delay: 300 } });
    await done;

    // Each retry should be at least ~300ms apart (allow some tolerance)
    expect(timestamps).toHaveLength(3);
    for (let i = 1; i < timestamps.length; i++) {
      expect(timestamps[i] - timestamps[i - 1]).toBeGreaterThanOrEqual(200);
    }
    await queue.close();
  }, 25000);
});

describeEachMode('Bull compat: removeOnComplete', (CONNECTION) => {
  const Q = `bull-roc-${TS}`;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('8. removes job hash after completion when removeOnComplete=true', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    const done = new Promise<string>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker = new Worker(Q, async () => 'result', { connection: CONNECTION, concurrency: 1, blockTimeout: 500 });
      worker.on('error', () => {});
      worker.on('completed', (job: any) => {
        clearTimeout(timeout);
        worker.close(true).then(() => resolve(job.id));
      });
    });

    await new Promise((r) => setTimeout(r, 500));
    const job = await queue.add('roc-job', { x: 1 }, { removeOnComplete: true });
    const jobId = await done;

    // The job hash should be deleted
    const exists = await cleanupClient.exists([k.job(jobId)]);
    expect(exists).toBe(0);

    await queue.close();
  }, 15000);
});

describeEachMode('Bull compat: completed/failed events', (CONNECTION) => {
  const Q = `bull-events-${TS}`;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    await flushQueue(cleanupClient, `bull-failed-ev-${TS}`);
    cleanupClient.close();
  });

  it('9. emits completed event with return value', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    let returnValue: any;

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker = new Worker(Q, async () => ({ result: 42 }), {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 500,
      });
      worker.on('error', () => {});
      worker.on('completed', (_job: any, result: any) => {
        returnValue = result;
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });
    });

    await new Promise((r) => setTimeout(r, 500));
    await queue.add('comp-ev', {});
    await done;

    expect(returnValue).toEqual({ result: 42 });
    await queue.close();
  }, 15000);

  it('10. emits failed event with error', async () => {
    const Q2 = `bull-failed-ev-${TS}`;
    const queue = new Queue(Q2, { connection: CONNECTION });
    let failedError: Error | undefined;

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker = new Worker(
        Q2,
        async () => {
          throw new Error('boom');
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500 },
      );
      worker.on('error', () => {});
      worker.on('failed', (_job: any, err: Error) => {
        failedError = err;
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });
    });

    await new Promise((r) => setTimeout(r, 500));
    await queue.add('fail-ev', {});
    await done;

    expect(failedError).toBeDefined();
    expect(failedError!.message).toBe('boom');
    await queue.close();
  }, 15000);
});

describeEachMode('Bull compat: Stalled job recovery', (CONNECTION) => {
  const Q = `bull-stalled-${TS}`;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('11. stalled jobs are re-enqueued (kill worker mid-processing, verify recovery)', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    // Worker 1: picks up the job but we force-close it
    let w1Started = false;
    const worker1 = new Worker(
      Q,
      async () => {
        w1Started = true;
        await new Promise((r) => setTimeout(r, 60000));
        return 'never';
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
    );
    worker1.on('error', () => {});
    await worker1.waitUntilReady();

    const job = await queue.add('stall-test', { recover: true });
    while (!w1Started) {
      await new Promise((r) => setTimeout(r, 50));
    }
    await worker1.close(true);

    // Worker 2: with short stalled interval to detect and recover the job
    const stalledIds: string[] = [];
    const worker2 = new Worker(Q, async () => 'recovered', {
      connection: CONNECTION,
      concurrency: 1,
      blockTimeout: 500,
      stalledInterval: 1000,
      maxStalledCount: 1,
    });
    worker2.on('error', () => {});
    worker2.on('stalled', (jobId: string) => stalledIds.push(jobId));
    await worker2.waitUntilReady();

    // Wait for stalled recovery cycle to detect and move to failed
    await new Promise((r) => setTimeout(r, 3500));
    await worker2.close(true);

    // Verify the job was handled by stalled recovery (moved to failed after maxStalledCount=1)
    const state = String(await cleanupClient.hget(k.job(job!.id), 'state'));
    expect(state).toBe('failed');
    const failedReason = String(await cleanupClient.hget(k.job(job!.id), 'failedReason'));
    expect(failedReason).toContain('stalled');

    await queue.close();
  }, 20000);
});

describeEachMode('Bull compat: FIFO ordering', (CONNECTION) => {
  const Q = `bull-fifo-${TS}`;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('12. add 10 jobs, verify FIFO processing order', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const order: number[] = [];
    const JOB_COUNT = 10;

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);
      const worker = new Worker(
        Q,
        async (job: any) => {
          order.push(job.data.index);
          if (order.length >= JOB_COUNT) {
            clearTimeout(timeout);
            setTimeout(() => worker.close(true).then(resolve), 200);
          }
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500 },
      );
      worker.on('error', () => {});
    });

    // Add all jobs before worker starts processing
    await new Promise((r) => setTimeout(r, 500));
    for (let i = 0; i < JOB_COUNT; i++) {
      await queue.add(`fifo-${i}`, { index: i });
    }
    await done;

    expect(order).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    await queue.close();
  }, 20000);
});

describeEachMode('Bull compat: Delayed jobs', (CONNECTION) => {
  const Q = `bull-delayed-${TS}`;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('13. delayed job is not processed until delay expires', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);
    let processedAt = 0;

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);
      const worker = new Worker(
        Q,
        async () => {
          processedAt = Date.now();
          return 'delayed-ok';
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 500,
          promotionInterval: 200,
        },
      );
      worker.on('error', () => {});
      worker.on('completed', () => {
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });
    });

    await new Promise((r) => setTimeout(r, 500));
    const addedAt = Date.now();
    await queue.add('delayed-job', { x: 1 }, { delay: 1000 });
    await done;

    // Should have been processed at least ~1000ms after adding
    expect(processedAt - addedAt).toBeGreaterThanOrEqual(800);
    await queue.close();
  }, 20000);
});

describeEachMode('Bull compat: Job timeout', (CONNECTION) => {
  const Q = `bull-timeout-${TS}`;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('14. job that exceeds timeout fails with timeout error', async () => {
    // glide-mq implements per-job timeout: if the processor takes longer
    // than opts.timeout, the job fails with 'Job timeout exceeded'.
    const queue = new Queue(Q, { connection: CONNECTION });

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('test timeout')), 10000);
      const worker = new Worker(
        Q,
        async () => {
          await new Promise((r) => setTimeout(r, 2000));
          return 'should-not-reach';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});
      worker.on('failed', (_job: any, err: Error) => {
        expect(err.message).toBe('Job timeout exceeded');
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });
    });

    await new Promise((r) => setTimeout(r, 500));
    await queue.add('timeout-job', {}, { timeout: 300 });
    await done;
    await queue.close();
  }, 15000);
});

// ---------------------------------------------------------------------------
// test_job.js patterns
// ---------------------------------------------------------------------------

describeEachMode('Bull compat: Job.remove', (CONNECTION) => {
  const Q = `bull-job-remove-${TS}`;
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

  it('15. removes the job from all sets', async () => {
    const k = buildKeys(Q);
    const job = await queue.add('rm-test', { x: 1 });
    const live = await queue.getJob(job!.id);
    await live!.remove();

    // Job hash should be gone
    const exists = await cleanupClient.exists([k.job(job!.id)]);
    expect(exists).toBe(0);

    // Job should not be in scheduled or stream
    const scheduledScore = await cleanupClient.zscore(k.scheduled, job!.id);
    expect(scheduledScore).toBeNull();
  });
});

describeEachMode('Bull compat: Job.progress', (CONNECTION) => {
  const Q = `bull-job-progress-${TS}`;
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

  it('16. updates and persists progress', async () => {
    const k = buildKeys(Q);
    const job = await queue.add('prog-test', { x: 1 });
    const live = await queue.getJob(job!.id);

    await live!.updateProgress(50);
    const val50 = await cleanupClient.hget(k.job(job!.id), 'progress');
    expect(String(val50)).toBe('50');

    await live!.updateProgress({ current: 7, total: 10 });
    const valObj = await cleanupClient.hget(k.job(job!.id), 'progress');
    expect(JSON.parse(String(valObj))).toEqual({ current: 7, total: 10 });
  });
});

describeEachMode('Bull compat: Job.updateData', (CONNECTION) => {
  const Q = `bull-job-update-${TS}`;
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

  it('17. changes job data in place', async () => {
    const k = buildKeys(Q);
    const job = await queue.add('update-test', { original: true });
    const live = await queue.getJob(job!.id);

    await live!.updateData({ modified: true, count: 99 });

    const raw = await cleanupClient.hget(k.job(job!.id), 'data');
    expect(JSON.parse(String(raw))).toEqual({ modified: true, count: 99 });
  });
});

describeEachMode('Bull compat: Job.retry', (CONNECTION) => {
  const Q = `bull-job-retry-${TS}`;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('18. re-queues a failed job', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    // Process and fail the job
    const failDone = new Promise<string>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker = new Worker(
        Q,
        async () => {
          throw new Error('intentional fail');
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500 },
      );
      worker.on('error', () => {});
      worker.on('failed', (job: any) => {
        clearTimeout(timeout);
        worker.close(true).then(() => resolve(job.id));
      });
    });

    await new Promise((r) => setTimeout(r, 500));
    const job = await queue.add('retry-test', { x: 1 });
    const failedJobId = await failDone;
    expect(failedJobId).toBe(job!.id);

    // Verify it's in failed state
    const stateBeforeRetry = String(await cleanupClient.hget(k.job(job!.id), 'state'));
    expect(stateBeforeRetry).toBe('failed');

    // Retry via job.retry()
    const live = await queue.getJob(job!.id);
    await live!.retry();

    // After retry, job should be in delayed/scheduled (ready for promotion)
    const stateAfterRetry = String(await cleanupClient.hget(k.job(job!.id), 'state'));
    expect(stateAfterRetry).toBe('delayed');

    // And should be in the scheduled ZSet
    const score = await cleanupClient.zscore(k.scheduled, job!.id);
    expect(score).not.toBeNull();

    await queue.close();
  }, 15000);
});

describeEachMode('Bull compat: Job.getState', (CONNECTION) => {
  const Q = `bull-job-state-${TS}`;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    await flushQueue(cleanupClient, `bull-job-state-fail-${TS}`);
    cleanupClient.close();
  });

  it('19. returns correct state at each lifecycle point', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    // Add a job - initial state is "waiting"
    const job = await queue.add('state-test', { x: 1 });
    const live = await queue.getJob(job!.id);
    const initialState = await live!.getState();
    expect(initialState).toBe('waiting');

    // Process to completion
    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker = new Worker(Q, async () => 'done', { connection: CONNECTION, concurrency: 1, blockTimeout: 500 });
      worker.on('error', () => {});
      worker.on('completed', () => {
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });
    });

    await done;

    // After completion, state should be "completed"
    const completedLive = await queue.getJob(job!.id);
    const completedState = await completedLive!.getState();
    expect(completedState).toBe('completed');

    await queue.close();
  }, 15000);

  it('19b. failed job returns "failed" state', async () => {
    const Q2 = `bull-job-state-fail-${TS}`;
    const queue = new Queue(Q2, { connection: CONNECTION });

    const done = new Promise<string>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker = new Worker(
        Q2,
        async () => {
          throw new Error('fail');
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500 },
      );
      worker.on('error', () => {});
      worker.on('failed', (job: any) => {
        clearTimeout(timeout);
        worker.close(true).then(() => resolve(job.id));
      });
    });

    await new Promise((r) => setTimeout(r, 500));
    const job = await queue.add('fail-state', {});
    await done;

    const live = await queue.getJob(job!.id);
    const state = await live!.getState();
    expect(state).toBe('failed');

    await queue.close();
  }, 15000);
});

describeEachMode('Bull compat: Job completion tracking', (CONNECTION) => {
  const Q = `bull-finished-${TS}`;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('20. completed job has returnvalue persisted and retrievable', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker = new Worker(Q, async () => ({ answer: 42 }), {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 500,
      });
      worker.on('error', () => {});
      worker.on('completed', () => {
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });
    });

    await new Promise((r) => setTimeout(r, 500));
    const job = await queue.add('finish-test', { question: 'life' });
    await done;

    // Verify returnvalue persisted in Redis
    const raw = await cleanupClient.hget(k.job(job!.id), 'returnvalue');
    expect(JSON.parse(String(raw))).toEqual({ answer: 42 });

    // Verify via getJob()
    const live = await queue.getJob(job!.id);
    expect(live!.returnvalue).toEqual({ answer: 42 });

    // Verify state
    const state = await live!.getState();
    expect(state).toBe('completed');

    await queue.close();
  }, 15000);
});
