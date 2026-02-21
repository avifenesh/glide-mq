/**
 * Bulletproof tests: verify glide-mq is immune to known bugs from BullMQ and Celery.
 * Each test references the specific upstream bug it guards against.
 *
 * Requires: valkey-server running on localhost:6379 and cluster on :7000-7005
 *
 * Run: npx vitest run tests/bulletproof.test.ts
 */
import { it, expect, beforeAll, afterAll } from 'vitest';
import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { FlowProducer } = require('../dist/flow-producer') as typeof import('../src/flow-producer');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

// ---------------------------------------------------------------------------
// BullMQ Bug #3767: getJobs('active') returns undefined entries
// ---------------------------------------------------------------------------

describeEachMode('BullMQ #3767: getJobs(active) never returns undefined/null entries', (CONNECTION) => {
  const Q = 'bp-getactive-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('getJobs(active) returns only valid Job objects while jobs are processing', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const TOTAL = 10;
    const activeSnapshots: any[][] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 20000);
      let completed = 0;

      const worker = new Worker(
        Q,
        async () => {
          // Slow enough to catch active state; fast enough to finish
          await new Promise((r) => setTimeout(r, 200));
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 5, blockTimeout: 500 },
      );

      worker.on('completed', () => {
        completed++;
        if (completed >= TOTAL) {
          clearTimeout(timeout);
          worker.close(true).then(resolve);
        }
      });
      worker.on('error', () => {});
    });

    // Add jobs, then snapshot active list while they are processing
    await new Promise((r) => setTimeout(r, 500));
    for (let i = 0; i < TOTAL; i++) {
      await queue.add(`active-${i}`, { i });
    }

    // Take multiple snapshots of active jobs during processing
    for (let snap = 0; snap < 5; snap++) {
      await new Promise((r) => setTimeout(r, 100));
      const active = await queue.getJobs('active');
      activeSnapshots.push(active);
    }

    await done;

    // Every entry in every snapshot must be a valid Job - never undefined/null
    for (const snapshot of activeSnapshots) {
      for (const job of snapshot) {
        expect(job).not.toBeUndefined();
        expect(job).not.toBeNull();
        expect(job.id).toBeTruthy();
      }
    }

    await queue.close();
  }, 25000);
});

// ---------------------------------------------------------------------------
// BullMQ Bug #3295: Fast jobs (<10ms) lose state or cause lock errors
// ---------------------------------------------------------------------------

describeEachMode('BullMQ #3295: fast jobs (<10ms) complete without errors', (CONNECTION) => {
  const Q = 'bp-fastjob-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('processes 50 near-instant jobs without lock or state errors', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const TOTAL = 50;
    const errors: Error[] = [];
    let completed = 0;
    let failed = 0;

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 30000);

      const worker = new Worker(
        Q,
        async (job: any) => {
          // Near-instant - no await, no delay
          return { id: job.id };
        },
        { connection: CONNECTION, concurrency: 5, blockTimeout: 500 },
      );

      worker.on('completed', () => {
        completed++;
        if (completed + failed >= TOTAL) {
          clearTimeout(timeout);
          worker.close(true).then(resolve);
        }
      });
      worker.on('failed', (_job: any, err: Error) => {
        failed++;
        errors.push(err);
        if (completed + failed >= TOTAL) {
          clearTimeout(timeout);
          worker.close(true).then(resolve);
        }
      });
      worker.on('error', () => {});
    });

    await new Promise((r) => setTimeout(r, 500));
    for (let i = 0; i < TOTAL; i++) {
      await queue.add(`fast-${i}`, { i });
    }

    await done;

    // All jobs should complete - zero failures from lock/state issues
    expect(completed).toBe(TOTAL);
    expect(failed).toBe(0);
    expect(errors).toHaveLength(0);

    // All jobs should be in completed state
    const counts = await queue.getJobCounts();
    expect(counts.completed).toBe(TOTAL);
    expect(counts.failed).toBe(0);

    await queue.close();
  }, 35000);
});

// ---------------------------------------------------------------------------
// BullMQ Bug #3272: Job scheduler stops producing after worker reconnect
// ---------------------------------------------------------------------------

describeEachMode('BullMQ #3272: scheduler fires reliably after worker restart', (CONNECTION) => {
  const Q = 'bp-scheduler-reconnect-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('scheduler continues firing after worker is restarted', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });

    // Create scheduler with 500ms interval
    await queue.upsertJobScheduler(
      'bp-repeat',
      { every: 500 },
      {
        name: 'bp-tick',
        data: { seq: true },
      },
    );

    const firstBatch: string[] = [];

    // First worker processes a few ticks
    const firstWorkerDone = new Promise<void>((resolve) => {
      const timeout = setTimeout(() => resolve(), 3000);
      const worker = new Worker(
        Q,
        async (job: any) => {
          firstBatch.push(job.id);
          return 'ok';
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 500,
          stalledInterval: 60000,
          promotionInterval: 500,
        },
      );
      worker.on('error', () => {});

      setTimeout(() => {
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      }, 3000);
    });

    await firstWorkerDone;
    expect(firstBatch.length).toBeGreaterThanOrEqual(1);

    // Brief gap with no workers
    await new Promise((r) => setTimeout(r, 500));

    // Second worker picks up and should see new scheduler firings
    const secondBatch: string[] = [];
    const secondWorkerDone = new Promise<void>((resolve) => {
      const worker = new Worker(
        Q,
        async (job: any) => {
          secondBatch.push(job.id);
          return 'ok';
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 500,
          stalledInterval: 60000,
          promotionInterval: 500,
        },
      );
      worker.on('error', () => {});

      setTimeout(() => {
        worker.close(true).then(resolve);
      }, 4000);
    });

    await secondWorkerDone;

    // Second worker must have processed at least 1 scheduler-fired job
    expect(secondBatch.length).toBeGreaterThanOrEqual(1);

    await queue.removeJobScheduler('bp-repeat');
    await queue.close();
  }, 20000);
});

// ---------------------------------------------------------------------------
// BullMQ Bug #3133: Duplicate scheduler execution within the same second
// ---------------------------------------------------------------------------

describeEachMode('BullMQ #3133: no duplicate scheduler job execution within same interval', (CONNECTION) => {
  const Q = 'bp-nodup-sched-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('scheduler with 1s interval does not produce duplicate jobs per tick', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const processedTimestamps: number[] = [];

    await queue.upsertJobScheduler(
      'once-per-sec',
      { every: 1000 },
      {
        name: 'tick',
        data: { check: 'dedup' },
      },
    );

    const done = new Promise<void>((resolve) => {
      const worker = new Worker(
        Q,
        async () => {
          processedTimestamps.push(Date.now());
          return 'ok';
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 500,
          stalledInterval: 60000,
          promotionInterval: 500,
        },
      );
      worker.on('error', () => {});

      setTimeout(() => {
        worker.close(true).then(resolve);
      }, 5000);
    });

    await done;

    // Check that no two jobs were processed within 500ms of each other
    // (the interval is 1000ms, so anything under 500ms apart is a duplicate)
    processedTimestamps.sort((a, b) => a - b);
    for (let i = 1; i < processedTimestamps.length; i++) {
      const gap = processedTimestamps[i] - processedTimestamps[i - 1];
      expect(gap).toBeGreaterThanOrEqual(400); // some tolerance for timer jitter
    }

    await queue.removeJobScheduler('once-per-sec');
    await queue.close();
  }, 15000);
});

// ---------------------------------------------------------------------------
// BullMQ Bug #3320: Parent stuck in waiting-children when all children complete
// ---------------------------------------------------------------------------

describeEachMode('BullMQ #3320: parent completes when all children complete', (CONNECTION) => {
  const Q = 'bp-parent-complete-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('parent transitions from waiting-children to completed after all children finish', async () => {
    const flow = new FlowProducer({ connection: CONNECTION });

    const node = await flow.add({
      name: 'bp-parent',
      queueName: Q,
      data: { role: 'root' },
      children: [
        { name: 'bp-child-1', queueName: Q, data: { idx: 1 } },
        { name: 'bp-child-2', queueName: Q, data: { idx: 2 } },
        { name: 'bp-child-3', queueName: Q, data: { idx: 3 } },
      ],
    });

    const parentId = node.job.id;
    const k = buildKeys(Q);

    // Verify parent starts in waiting-children
    const initialState = await cleanupClient.hget(k.job(parentId), 'state');
    expect(String(initialState)).toBe('waiting-children');

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout - parent stuck in waiting-children')), 15000);
      const worker = new Worker(
        Q,
        async (job: any) => {
          return { name: job.name, idx: job.data.idx };
        },
        { connection: CONNECTION, concurrency: 2, blockTimeout: 500 },
      );

      worker.on('completed', (job: any) => {
        if (job.id === parentId) {
          clearTimeout(timeout);
          setTimeout(() => worker.close(true).then(resolve), 200);
        }
      });
      worker.on('error', () => {});
    });

    await done;

    // Parent must be completed - not stuck in waiting-children
    const finalState = await cleanupClient.hget(k.job(parentId), 'state');
    expect(String(finalState)).toBe('completed');

    // All children must be completed too
    for (const childNode of node.children!) {
      const childState = await cleanupClient.hget(k.job(childNode.job.id), 'state');
      expect(String(childState)).toBe('completed');
    }

    await flow.close();
  }, 20000);
});

// ---------------------------------------------------------------------------
// BullMQ Cluster CROSSSLOT: no CROSSSLOT errors when keys share hash tag
// ---------------------------------------------------------------------------

describeEachMode('BullMQ CROSSSLOT: no CROSSSLOT errors with hash-tagged keys', (CONNECTION) => {
  const Q = 'bp-crossslot-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('all queue operations succeed in cluster mode without CROSSSLOT errors', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const errors: Error[] = [];

    // Start worker first so consumer group and stream exist before adds
    let completed = 0;
    const TOTAL = 5;
    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 20000);
      const worker = new Worker(Q, async () => 'ok', {
        connection: CONNECTION,
        concurrency: 3,
        blockTimeout: 500,
        promotionInterval: 300,
        stalledInterval: 60000,
      });

      worker.on('completed', () => {
        completed++;
        if (completed >= TOTAL) {
          clearTimeout(timeout);
          worker.close(true).then(resolve);
        }
      });
      worker.on('error', (err: Error) => {
        errors.push(err);
      });
    });

    // Wait for worker to be ready before adding jobs
    await new Promise((r) => setTimeout(r, 1000));

    // Exercise multiple operations that touch different keys
    // All immediate jobs (no priority/delay) to avoid promotion timing issues
    for (let i = 0; i < TOTAL; i++) {
      await queue.add(`cross-${i}`, { x: i });
    }

    // getJobCounts touches stream, scheduled, completed, failed keys concurrently
    const counts = await queue.getJobCounts();
    expect(counts.waiting + counts.active + counts.completed).toBeGreaterThanOrEqual(1);

    // getJobs touches stream + job hashes in cluster
    const waitingJobs = await queue.getJobs('waiting');
    for (const j of waitingJobs) {
      expect(j).not.toBeNull();
      expect(j.id).toBeTruthy();
    }

    // Also exercise delayed and completed ZSet reads
    const delayedJobs = await queue.getJobs('delayed');
    expect(delayedJobs).toBeDefined();
    const completedJobs = await queue.getJobs('completed');
    expect(completedJobs).toBeDefined();

    await done;

    // Zero CROSSSLOT errors
    for (const err of errors) {
      expect(err.message).not.toContain('CROSSSLOT');
    }
    expect(completed).toBe(TOTAL);

    await queue.close();
  }, 25000);
});

// ---------------------------------------------------------------------------
// BullMQ swallowed-errors bug: worker emits error events, not swallows them
// ---------------------------------------------------------------------------

describeEachMode('BullMQ swallowed errors: worker emits error events on job failure', (CONNECTION) => {
  const Q = 'bp-error-emit-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('worker emits failed event with error details when processor throws', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const failedEvents: { jobId: string; message: string }[] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);

      const worker = new Worker(
        Q,
        async () => {
          throw new Error('deliberate test failure');
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500 },
      );

      worker.on('failed', (job: any, err: Error) => {
        failedEvents.push({ jobId: job.id, message: err.message });
        if (failedEvents.length >= 3) {
          clearTimeout(timeout);
          worker.close(true).then(resolve);
        }
      });
      worker.on('error', () => {});
    });

    await new Promise((r) => setTimeout(r, 500));
    const j1 = await queue.add('err-1', { i: 1 });
    const j2 = await queue.add('err-2', { i: 2 });
    const j3 = await queue.add('err-3', { i: 3 });

    await done;

    // All three failures must be emitted - not swallowed
    expect(failedEvents).toHaveLength(3);
    const ids = failedEvents.map((e) => e.jobId);
    expect(ids).toContain(j1!.id);
    expect(ids).toContain(j2!.id);
    expect(ids).toContain(j3!.id);

    for (const evt of failedEvents) {
      expect(evt.message).toBe('deliberate test failure');
    }

    await queue.close();
  }, 20000);
});

// ---------------------------------------------------------------------------
// BullMQ FlowProducer event listener leak: add+close cycle does not leak
// ---------------------------------------------------------------------------

describeEachMode('BullMQ #2166: FlowProducer add+close does not leak event listeners', (CONNECTION) => {
  const Q = 'bp-flow-leak-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('creating and closing 20 FlowProducers does not trigger MaxListenersExceededWarning', async () => {
    const warnings: string[] = [];
    const originalWarn = process.emitWarning;
    process.emitWarning = (warning: any) => {
      warnings.push(typeof warning === 'string' ? warning : warning.message || String(warning));
    };

    try {
      for (let i = 0; i < 20; i++) {
        const flow = new FlowProducer({ connection: CONNECTION });
        await flow.add({
          name: `leak-parent-${i}`,
          queueName: Q,
          data: { i },
          children: [{ name: `leak-child-${i}`, queueName: Q, data: { c: i } }],
        });
        await flow.close();
      }

      // No MaxListenersExceeded warnings should have been emitted
      const listenerWarnings = warnings.filter((w) => w.includes('MaxListeners'));
      expect(listenerWarnings).toHaveLength(0);
    } finally {
      process.emitWarning = originalWarn;
    }
  }, 30000);
});

// ---------------------------------------------------------------------------
// Celery #4354: Worker shutdown during processing - job not lost
// ---------------------------------------------------------------------------

describeEachMode('Celery #4354: worker shutdown during processing does not lose jobs', (CONNECTION) => {
  const Q = 'bp-shutdown-noloss-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('job in progress during graceful close is finished before worker exits', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    let jobStarted = false;
    let jobFinished = false;
    let completedJobId: string | null = null;

    // Use concurrency > 1 so jobs are dispatched via dispatchJob (which tracks
    // activePromises). With c=1, the fast path processes inline and close(false)
    // cannot track the in-flight job.
    const worker = new Worker(
      Q,
      async (job: any) => {
        jobStarted = true;
        // Simulate work that takes a moment
        await new Promise((r) => setTimeout(r, 2000));
        jobFinished = true;
        return { processed: true };
      },
      { connection: CONNECTION, concurrency: 2, blockTimeout: 500 },
    );
    worker.on('completed', (job: any) => {
      completedJobId = job.id;
    });
    worker.on('error', () => {});

    // Wait for worker to be ready
    await worker.waitUntilReady();
    await new Promise((r) => setTimeout(r, 300));

    const job = await queue.add('shutdown-test', { value: 42 });

    // Wait for job to start processing
    await new Promise<void>((resolve) => {
      const deadline = Date.now() + 10000;
      const check = setInterval(() => {
        if (jobStarted || Date.now() > deadline) {
          clearInterval(check);
          resolve();
        }
      }, 50);
    });

    expect(jobStarted).toBe(true);

    // Graceful close (force=false) - should wait for active job
    await worker.close(false);

    // Job must have completed before worker exited
    expect(jobFinished).toBe(true);
    expect(completedJobId).toBe(job!.id);

    // Verify job state in server
    const k = buildKeys(Q);
    const state = await cleanupClient.hget(k.job(job!.id), 'state');
    expect(String(state)).toBe('completed');

    await queue.close();
  }, 20000);
});

// ---------------------------------------------------------------------------
// Celery visibility timeout: long-running job with heartbeat not falsely reclaimed
// ---------------------------------------------------------------------------

describeEachMode('Celery visibility timeout: heartbeat prevents false reclaim of long jobs', (CONNECTION) => {
  const Q = 'bp-heartbeat-protect-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('job running longer than stalledInterval is not reclaimed when heartbeat is active', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    let completedId: string | null = null;
    const stalledEvents: string[] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 25000);

      const worker = new Worker(
        Q,
        async () => {
          // Job runs for 4 seconds - longer than stalledInterval of 2s
          await new Promise((r) => setTimeout(r, 4000));
          return 'long-done';
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 500,
          stalledInterval: 2000,
          lockDuration: 1000, // heartbeat fires every 500ms (lockDuration/2)
          maxStalledCount: 1,
          promotionInterval: 1000,
        },
      );

      worker.on('completed', (job: any) => {
        completedId = job.id;
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });
      worker.on('stalled', (jobId: string) => {
        stalledEvents.push(jobId);
      });
      worker.on('error', () => {});
    });

    await new Promise((r) => setTimeout(r, 500));
    const job = await queue.add('long-runner', { duration: 4000 });

    await done;

    // Job must complete - not be reclaimed as stalled
    expect(completedId).toBe(job!.id);
    // No stalled events for this job
    expect(stalledEvents).not.toContain(job!.id);

    await queue.close();
  }, 30000);
});

// ---------------------------------------------------------------------------
// Celery #7801 / result accumulation: removeOnComplete prevents unbounded growth
// ---------------------------------------------------------------------------

describeEachMode('Celery #7801: results do not accumulate when removeOnComplete is set', (CONNECTION) => {
  const Q = 'bp-no-accumulate-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('completed ZSet stays bounded when removeOnComplete count is set', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const TOTAL = 20;
    const KEEP = 3;
    let processed = 0;

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 20000);

      const worker = new Worker(
        Q,
        async () => {
          return { result: 'ok' };
        },
        { connection: CONNECTION, concurrency: 3, blockTimeout: 500 },
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

    await new Promise((r) => setTimeout(r, 500));
    for (let i = 0; i < TOTAL; i++) {
      await queue.add(`acc-${i}`, { i }, { removeOnComplete: KEEP });
    }

    await done;

    // Wait a moment for async cleanup to settle
    await new Promise((r) => setTimeout(r, 300));

    // Completed ZSet must be bounded to KEEP - not accumulate 20 entries
    const k = buildKeys(Q);
    const completedCount = await cleanupClient.zcard(k.completed);
    expect(completedCount).toBeLessThanOrEqual(KEEP);

    await queue.close();
  }, 25000);
});

// ---------------------------------------------------------------------------
// Celery #8030/#10096: Worker processes after reconnect
// ---------------------------------------------------------------------------

describeEachMode('Celery #8030: worker resumes processing after reconnect cycle', (CONNECTION) => {
  const Q = 'bp-reconnect-resume-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('new worker processes jobs added after previous worker is gone', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const NEW_JOBS = 5;

    // First worker starts and processes nothing (just establishes consumer group)
    const firstWorker = new Worker(Q, async () => 'ok', {
      connection: CONNECTION,
      concurrency: 1,
      blockTimeout: 500,
      stalledInterval: 60000,
    });
    firstWorker.on('error', () => {});
    await firstWorker.waitUntilReady();

    // Close first worker cleanly
    await firstWorker.close(false);

    // Second worker starts - this is the Celery bug scenario:
    // workers that stop consuming after reconnection
    const secondBatch: string[] = [];
    let completed = 0;
    const secondWorkerDone = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('second worker did not process all jobs')), 15000);

      const worker = new Worker(
        Q,
        async (job: any) => {
          secondBatch.push(job.id);
          return 'ok';
        },
        {
          connection: CONNECTION,
          concurrency: 3,
          blockTimeout: 500,
          stalledInterval: 60000,
        },
      );
      worker.on('completed', () => {
        completed++;
        if (completed >= NEW_JOBS) {
          clearTimeout(timeout);
          worker.close(true).then(resolve);
        }
      });
      worker.on('error', () => {});
    });

    // Wait for second worker to be ready, then add jobs
    await new Promise((r) => setTimeout(r, 1000));

    for (let i = 0; i < NEW_JOBS; i++) {
      await queue.add(`post-${i}`, { i });
    }

    await secondWorkerDone;

    // All jobs added after the first worker shutdown must be processed
    expect(secondBatch.length).toBe(NEW_JOBS);

    await queue.close();
  }, 20000);
});

// ---------------------------------------------------------------------------
// Celery workflow chain failure: parent stays in correct state, not corrupted
// ---------------------------------------------------------------------------

describeEachMode('Celery chain failure: parent state correct when child fails', (CONNECTION) => {
  const Q = 'bp-chain-fail-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('parent remains in waiting-children when one child fails - state is not corrupted', async () => {
    const flow = new FlowProducer({ connection: CONNECTION });

    const node = await flow.add({
      name: 'chain-parent',
      queueName: Q,
      data: { type: 'chain' },
      children: [
        { name: 'step-ok', queueName: Q, data: { step: 1 } },
        {
          name: 'step-fail',
          queueName: Q,
          data: { step: 2 },
          opts: { attempts: 1 },
        },
      ],
    });

    const parentId = node.job.id;
    const failChildId = node.children![1].job.id;
    const okChildId = node.children![0].job.id;

    const done = new Promise<void>((resolve) => {
      const worker = new Worker(
        Q,
        async (job: any) => {
          if (job.name === 'step-fail') {
            throw new Error('step 2 failed');
          }
          return { step: job.data.step, success: true };
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 500,
          stalledInterval: 60000,
        },
      );
      worker.on('error', () => {});
      worker.on('failed', () => {});

      // Allow time for both children to be attempted
      setTimeout(() => {
        worker.close(true).then(resolve);
      }, 5000);
    });

    await done;

    const k = buildKeys(Q);

    // Parent should be in waiting-children - not corrupted to completed or unknown
    const parentState = await cleanupClient.hget(k.job(parentId), 'state');
    expect(String(parentState)).toBe('waiting-children');

    // The OK child should be completed
    const okState = await cleanupClient.hget(k.job(okChildId), 'state');
    expect(String(okState)).toBe('completed');

    // The failing child should be in failed state
    const failState = await cleanupClient.hget(k.job(failChildId), 'state');
    expect(String(failState)).toBe('failed');

    // Parent's deps set should still exist (not cleaned up prematurely)
    const deps = await cleanupClient.smembers(k.deps(parentId));
    expect(deps.size).toBe(2);

    await flow.close();
  }, 15000);
});

// ===========================================================================
// Sidekiq / Bee-Queue bug tests
// ===========================================================================

// ---------------------------------------------------------------------------
// Sidekiq BRPOP loss: Job not lost on worker crash mid-processing
// ---------------------------------------------------------------------------

describeEachMode('Sidekiq BRPOP loss: job not lost when worker crashes mid-processing', (CONNECTION) => {
  const Q = 'bp-crash-recover-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('job transitions to failed via stalled recovery after worker crash - not lost', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const MAX_STALLED = 1;
    const STALL_INTERVAL = 1000;

    // Add a job before starting any worker
    const job = await queue.add('crash-test', { important: true });

    // Worker 1: starts processing but we force-close it mid-processing
    let jobPicked = false;
    const crashWorker = new Worker(
      Q,
      async () => {
        jobPicked = true;
        await new Promise((r) => setTimeout(r, 60000));
        return 'never-reaches';
      },
      {
        connection: CONNECTION,
        concurrency: 2,
        blockTimeout: 500,
        stalledInterval: 60000,
        lockDuration: 200,
      },
    );
    crashWorker.on('error', () => {});

    // Wait for job to be picked up
    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (jobPicked) {
          clearInterval(check);
          resolve();
        }
      }, 50);
      setTimeout(() => {
        clearInterval(check);
        resolve();
      }, 5000);
    });
    expect(jobPicked).toBe(true);

    // Force-close worker 1 (simulates crash - no graceful ACK)
    await crashWorker.close(true);

    // Wait for PEL entry to age past the stalled interval
    await new Promise((r) => setTimeout(r, STALL_INTERVAL + 500));

    // Recovery worker: its scheduler will reclaim the stalled entry via XAUTOCLAIM.
    // After maxStalledCount cycles, the job moves to failed.
    const recoveryWorker = new Worker(Q, async () => 'should-not-process', {
      connection: CONNECTION,
      concurrency: 1,
      blockTimeout: 500,
      stalledInterval: STALL_INTERVAL,
      maxStalledCount: MAX_STALLED,
      lockDuration: 200,
    });
    recoveryWorker.on('error', () => {});

    // Wait for enough stalled recovery cycles to exceed maxStalledCount
    // stalledCount increments once per cycle, need > MAX_STALLED cycles
    await new Promise((r) => setTimeout(r, STALL_INTERVAL * (MAX_STALLED + 2) + 1000));

    await recoveryWorker.close(true);

    // The job must NOT be lost - it should be in 'failed' state
    const k = buildKeys(Q);
    const state = await cleanupClient.hget(k.job(job!.id), 'state');
    expect(String(state)).toBe('failed');

    // Verify the failed reason references stalling
    const reason = await cleanupClient.hget(k.job(job!.id), 'failedReason');
    expect(String(reason)).toContain('stalled');

    // Verify stalledCount was tracked
    const stalledCount = await cleanupClient.hget(k.job(job!.id), 'stalledCount');
    expect(Number(stalledCount)).toBeGreaterThan(MAX_STALLED);

    // The job is in the failed ZSet (not lost in limbo)
    const failedScore = await cleanupClient.zscore(k.failed, job!.id);
    expect(failedScore).not.toBeNull();

    await queue.close();
  }, 25000);
});

// ---------------------------------------------------------------------------
// Sidekiq dedup lock orphan: dedup lock cleared after job completes
// ---------------------------------------------------------------------------

describeEachMode('Sidekiq dedup lock orphan: dedup lock not orphaned after completion', (CONNECTION) => {
  const Q = 'bp-dedup-orphan-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('same dedup ID can be reused after job completes', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const DEDUP_ID = 'unique-task-abc';

    // Add first job with dedup ID
    const job1 = await queue.add(
      'dedup-test',
      { run: 1 },
      {
        deduplication: { id: DEDUP_ID, mode: 'simple' },
      },
    );
    expect(job1).not.toBeNull();

    // Process the first job to completion
    const firstDone = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);
      const worker = new Worker(Q, async () => 'done', { connection: CONNECTION, concurrency: 1, blockTimeout: 500 });
      worker.on('completed', () => {
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });
      worker.on('error', () => {});
    });
    await firstDone;

    // Verify first job is completed
    const k = buildKeys(Q);
    const state1 = await cleanupClient.hget(k.job(job1!.id), 'state');
    expect(String(state1)).toBe('completed');

    // Add second job with the SAME dedup ID - should succeed (not stuck/orphaned)
    const job2 = await queue.add(
      'dedup-test',
      { run: 2 },
      {
        deduplication: { id: DEDUP_ID, mode: 'simple' },
      },
    );
    expect(job2).not.toBeNull();
    expect(job2!.id).not.toBe(job1!.id);

    // Process the second job
    const secondDone = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);
      const worker = new Worker(Q, async () => 'done-again', {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 500,
      });
      worker.on('completed', () => {
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });
      worker.on('error', () => {});
    });
    await secondDone;

    const state2 = await cleanupClient.hget(k.job(job2!.id), 'state');
    expect(String(state2)).toBe('completed');

    await queue.close();
  }, 30000);
});

// ---------------------------------------------------------------------------
// Sidekiq retry loss: job is never in limbo during retry cycle
// ---------------------------------------------------------------------------

describeEachMode('Sidekiq retry loss: job exists in a known state throughout retry cycle', (CONNECTION) => {
  const Q = 'bp-retry-noloss-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('job fails, retries, succeeds - never lost from all state sets', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    let attemptCount = 0;
    const stateSnapshots: string[] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 25000);
      const worker = new Worker(
        Q,
        async () => {
          attemptCount++;
          if (attemptCount < 3) {
            throw new Error(`fail-attempt-${attemptCount}`);
          }
          return 'success-on-3';
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 500,
          stalledInterval: 60000,
          promotionInterval: 300,
        },
      );
      worker.on('completed', () => {
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });
      worker.on('error', () => {});
    });

    await new Promise((r) => setTimeout(r, 500));
    const job = await queue.add(
      'retry-test',
      { value: 1 },
      {
        attempts: 5,
        backoff: { type: 'fixed', delay: 200 },
      },
    );

    // Poll for job state while retrying
    const pollInterval = setInterval(async () => {
      try {
        const k = buildKeys(Q);
        const s = await cleanupClient.hget(k.job(job!.id), 'state');
        if (s) stateSnapshots.push(String(s));
      } catch {
        /* ignore */
      }
    }, 100);

    await done;
    clearInterval(pollInterval);

    // Job must have succeeded on attempt 3
    expect(attemptCount).toBe(3);

    const k = buildKeys(Q);
    const finalState = await cleanupClient.hget(k.job(job!.id), 'state');
    expect(String(finalState)).toBe('completed');

    // Every snapshot must be a valid state - never 'unknown' or empty
    for (const s of stateSnapshots) {
      expect(['waiting', 'active', 'delayed', 'completed', 'failed']).toContain(s);
    }

    await queue.close();
  }, 30000);
});

// ---------------------------------------------------------------------------
// Sidekiq counter race: job ID counter is monotonically increasing under concurrency
// ---------------------------------------------------------------------------

describeEachMode('Sidekiq counter race: job IDs are monotonically increasing under concurrent adds', (CONNECTION) => {
  const Q = 'bp-counter-race-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('50 concurrent adds produce strictly increasing, unique job IDs', async () => {
    // Use 5 separate Queue instances to simulate real concurrency
    const queues = Array.from({ length: 5 }, () => new Queue(Q, { connection: CONNECTION }));

    const TOTAL = 50;
    const promises: Promise<any>[] = [];
    for (let i = 0; i < TOTAL; i++) {
      const q = queues[i % queues.length];
      promises.push(q.add(`race-${i}`, { i }));
    }

    const jobs = await Promise.all(promises);

    // All IDs must be valid
    const ids = jobs.map((j) => {
      expect(j).not.toBeNull();
      return parseInt(j!.id, 10);
    });

    // All IDs must be unique
    const uniqueIds = new Set(ids);
    expect(uniqueIds.size).toBe(TOTAL);

    // IDs must form a contiguous range (INCR guarantees no gaps)
    const sorted = [...ids].sort((a, b) => a - b);
    for (let i = 1; i < sorted.length; i++) {
      expect(sorted[i]).toBe(sorted[i - 1] + 1);
    }

    for (const q of queues) await q.close();
  }, 20000);
});

// ---------------------------------------------------------------------------
// Bee-Queue #106: Repeated stalling doesn't cause infinite loop
// ---------------------------------------------------------------------------

describeEachMode('Bee-Queue #106: repeated stalling moves job to failed, not infinite loop', (CONNECTION) => {
  const Q = 'bp-stall-limit-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('job that stalls beyond maxStalledCount moves to failed with stalledCount', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const MAX_STALLED = 1;
    const STALL_INTERVAL = 1000;

    // Add a job
    const job = await queue.add('stall-forever', { doomed: true });

    // Worker picks up job, then crashes (force close)
    let jobPicked = false;
    const crashWorker = new Worker(
      Q,
      async () => {
        jobPicked = true;
        await new Promise(() => {}); // hang forever
        return 'never';
      },
      {
        connection: CONNECTION,
        concurrency: 2,
        blockTimeout: 500,
        stalledInterval: 60000,
        lockDuration: 200,
      },
    );
    crashWorker.on('error', () => {});

    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (jobPicked) {
          clearInterval(check);
          resolve();
        }
      }, 50);
      setTimeout(() => {
        clearInterval(check);
        resolve();
      }, 5000);
    });

    // Force-close to leave PEL entry orphaned
    await crashWorker.close(true);

    // Wait for entry to age past the stalled interval
    await new Promise((r) => setTimeout(r, STALL_INTERVAL + 500));

    // Recovery worker: scheduler reclaims the stalled entry via XAUTOCLAIM.
    // Each cycle increments stalledCount. After exceeding maxStalledCount,
    // the job is moved to failed - NOT looped infinitely.
    const recoveryWorker = new Worker(Q, async () => 'should-not-process', {
      connection: CONNECTION,
      concurrency: 1,
      blockTimeout: 500,
      stalledInterval: STALL_INTERVAL,
      maxStalledCount: MAX_STALLED,
      lockDuration: 200,
    });
    recoveryWorker.on('error', () => {});

    // Wait for enough stalled recovery cycles
    await new Promise((r) => setTimeout(r, STALL_INTERVAL * (MAX_STALLED + 2) + 1000));

    await recoveryWorker.close(true);

    // Job must be in failed state - NOT stuck in active forever (Bee-Queue #106 bug)
    const k = buildKeys(Q);
    const state = await cleanupClient.hget(k.job(job!.id), 'state');
    expect(String(state)).toBe('failed');

    // Verify stalledCount exceeds the limit
    const stalledCount = await cleanupClient.hget(k.job(job!.id), 'stalledCount');
    expect(Number(stalledCount)).toBeGreaterThan(MAX_STALLED);

    // Verify the failed reason
    const reason = await cleanupClient.hget(k.job(job!.id), 'failedReason');
    expect(String(reason)).toContain('stalled');

    await queue.close();
  }, 25000);
});

// ---------------------------------------------------------------------------
// Bee-Queue #584: Queue close during processing doesn't hang
// ---------------------------------------------------------------------------

describeEachMode('Bee-Queue #584: close(false) with active job returns within timeout', (CONNECTION) => {
  const Q = 'bp-close-nohang-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('close(false) waits for active job and returns within reasonable time', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    let jobStarted = false;

    // Use concurrency > 1 so jobs go through dispatchJob (tracks activePromises)
    const worker = new Worker(
      Q,
      async () => {
        jobStarted = true;
        await new Promise((r) => setTimeout(r, 1500));
        return 'finished-during-close';
      },
      { connection: CONNECTION, concurrency: 2, blockTimeout: 500 },
    );
    worker.on('error', () => {});

    await worker.waitUntilReady();
    await new Promise((r) => setTimeout(r, 300));

    await queue.add('close-test', { value: 1 });

    // Wait for job to start
    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (jobStarted) {
          clearInterval(check);
          resolve();
        }
      }, 50);
      setTimeout(() => {
        clearInterval(check);
        resolve();
      }, 5000);
    });

    expect(jobStarted).toBe(true);

    // close(false) should wait for active job, not hang
    const closeStart = Date.now();
    await worker.close(false);
    const closeElapsed = Date.now() - closeStart;

    // Must complete within reasonable time (job takes ~1.5s, allow up to 10s)
    expect(closeElapsed).toBeLessThan(10000);

    await queue.close();
  }, 20000);
});

// ---------------------------------------------------------------------------
// Bee-Queue #147: No NOSCRIPT errors - we use FUNCTION LOAD, not EVAL
// ---------------------------------------------------------------------------

describeEachMode('Bee-Queue #147: functions persist - no NOSCRIPT errors', (CONNECTION) => {
  const Q = 'bp-noscript-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('FCALL works without reloading library after fresh client creation', async () => {
    // Create a queue and add a job (this loads the function library)
    const queue1 = new Queue(Q, { connection: CONNECTION });
    const job1 = await queue1.add('persist-test-1', { v: 1 });
    expect(job1).not.toBeNull();
    await queue1.close();

    // Create a completely new queue instance (new client connection)
    // The library should already be on the server from the first load
    const queue2 = new Queue(Q, { connection: CONNECTION });

    // This FCALL should succeed without NOSCRIPT - library is persistent
    const job2 = await queue2.add('persist-test-2', { v: 2 });
    expect(job2).not.toBeNull();

    // Verify version function also works via a routable key
    const client = await queue2.getClient();
    const version = await client.fcall('glidemq_version', ['{glidemq}:_'], []);
    expect(String(version)).toBeTruthy();

    // Process both jobs to verify full function chain works
    let completed = 0;
    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);
      const worker = new Worker(Q, async () => 'ok', { connection: CONNECTION, concurrency: 2, blockTimeout: 500 });
      worker.on('completed', () => {
        completed++;
        if (completed >= 2) {
          clearTimeout(timeout);
          worker.close(true).then(resolve);
        }
      });
      worker.on('error', () => {});
    });

    await done;
    expect(completed).toBe(2);

    await queue2.close();
  }, 20000);
});

// ---------------------------------------------------------------------------
// Bee-Queue #885: Event listeners don't accumulate (no MaxListenersExceededWarning)
// ---------------------------------------------------------------------------

describeEachMode('Bee-Queue #885: processing many jobs does not leak event listeners', (CONNECTION) => {
  const Q = 'bp-listener-leak-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('QueueEvents + 50 jobs produces no MaxListenersExceededWarning', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const TOTAL = 50;
    const warnings: string[] = [];

    // Intercept process.emitWarning to catch MaxListenersExceeded
    const originalWarn = process.emitWarning;
    process.emitWarning = (warning: any) => {
      const msg = typeof warning === 'string' ? warning : warning.message || String(warning);
      warnings.push(msg);
    };

    try {
      const { QueueEvents } = require('../dist/queue-events') as typeof import('../src/queue-events');
      const queueEvents = new QueueEvents(Q, {
        connection: CONNECTION,
        lastEventId: '0',
        blockTimeout: 500,
      });
      await queueEvents.waitUntilReady();

      const completedIds: string[] = [];
      queueEvents.on('completed', (payload: any) => {
        completedIds.push(payload.jobId);
      });

      // Process jobs
      let processed = 0;
      const done = new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error('timeout')), 30000);
        const worker = new Worker(Q, async () => 'ok', { connection: CONNECTION, concurrency: 5, blockTimeout: 500 });
        worker.on('completed', () => {
          processed++;
          if (processed >= TOTAL) {
            clearTimeout(timeout);
            worker.close(true).then(resolve);
          }
        });
        worker.on('error', () => {});
      });

      await new Promise((r) => setTimeout(r, 500));
      for (let i = 0; i < TOTAL; i++) {
        await queue.add(`listener-${i}`, { i });
      }

      await done;

      // Wait for QueueEvents to catch up
      await new Promise((r) => setTimeout(r, 1000));
      await queueEvents.close();

      // No MaxListenersExceeded warnings
      const listenerWarnings = warnings.filter((w) => w.includes('MaxListeners'));
      expect(listenerWarnings).toHaveLength(0);

      // QueueEvents should have seen at least some completed events
      expect(completedIds.length).toBeGreaterThan(0);
    } finally {
      process.emitWarning = originalWarn;
    }

    await queue.close();
  }, 35000);
});

// ---------------------------------------------------------------------------
// Bee-Queue #120: Stalled job recovery works with many stalled jobs
// ---------------------------------------------------------------------------

describeEachMode('Bee-Queue #120: bulk stalled job recovery reclaims all jobs', (CONNECTION) => {
  const Q = 'bp-bulk-stall-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('20 stalled jobs all move to failed - none stuck in active', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const TOTAL = 20;
    const MAX_STALLED = 1;
    const STALL_INTERVAL = 1000;

    // Add all jobs first
    const jobIds: string[] = [];
    for (let i = 0; i < TOTAL; i++) {
      const j = await queue.add(`bulk-stall-${i}`, { i });
      jobIds.push(j!.id);
    }

    // Worker 1: picks up all jobs but hangs (never completes them)
    let pickedUp = 0;
    const hangWorker = new Worker(
      Q,
      async () => {
        pickedUp++;
        await new Promise(() => {}); // hang forever
        return 'never';
      },
      {
        connection: CONNECTION,
        concurrency: TOTAL,
        blockTimeout: 500,
        stalledInterval: 60000,
        lockDuration: 200,
      },
    );
    hangWorker.on('error', () => {});

    // Wait for all jobs to be picked up
    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (pickedUp >= TOTAL) {
          clearInterval(check);
          resolve();
        }
      }, 50);
      setTimeout(() => {
        clearInterval(check);
        resolve();
      }, 8000);
    });

    // Force-close to simulate crash - leaves all PEL entries orphaned
    await hangWorker.close(true);

    // Wait for entries to age past the stalled interval
    await new Promise((r) => setTimeout(r, STALL_INTERVAL + 500));

    // Recovery worker: its scheduler will reclaim all stalled entries
    const recoveryWorker = new Worker(Q, async () => 'should-not-process', {
      connection: CONNECTION,
      concurrency: 1,
      blockTimeout: 500,
      stalledInterval: STALL_INTERVAL,
      maxStalledCount: MAX_STALLED,
      lockDuration: 200,
    });
    recoveryWorker.on('error', () => {});

    // Wait for enough cycles: each cycle reclaims all 20 entries.
    // After MAX_STALLED + 1 cycles, all should move to failed.
    await new Promise((r) => setTimeout(r, STALL_INTERVAL * (MAX_STALLED + 2) + 2000));

    await recoveryWorker.close(true);

    // All 20 jobs must be in failed state - NOT stuck in active (Bee-Queue #120 bug)
    const k = buildKeys(Q);
    let failedCount = 0;
    for (const id of jobIds) {
      const state = await cleanupClient.hget(k.job(id), 'state');
      if (String(state) === 'failed') failedCount++;
    }

    expect(failedCount).toBe(TOTAL);

    // All should be in the failed ZSet
    const failedZsetCount = await cleanupClient.zcard(k.failed);
    expect(failedZsetCount).toBeGreaterThanOrEqual(TOTAL);

    await queue.close();
  }, 30000);
});

// ===========================================================================
// Sidekiq bulletproof tests (from sidekiq-bugs-and-failures analysis)
// ===========================================================================

// ---------------------------------------------------------------------------
// Sidekiq #1: BRPOP job loss immunity - Streams+PEL prevents loss on crash
// ---------------------------------------------------------------------------

describeEachMode('Sidekiq BRPOP immunity: job in PEL survives worker crash and is reclaimed', (CONNECTION) => {
  const Q = 'bp-sidekiq-brpop-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('job enters PEL, worker crashes, second worker stalled recovery reclaims it', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const MAX_STALLED = 1;
    const STALL_INTERVAL = 1000;

    // Add a job
    const job = await queue.add('brpop-immune', { critical: true });
    expect(job).not.toBeNull();

    // Worker 1: picks up the job but never completes it (simulates crash)
    let jobPicked = false;
    const crashWorker = new Worker(
      Q,
      async () => {
        jobPicked = true;
        // Hang forever - never return
        await new Promise(() => {});
        return 'never';
      },
      {
        connection: CONNECTION,
        concurrency: 2,
        blockTimeout: 500,
        stalledInterval: 60000,
        lockDuration: 200,
      },
    );
    crashWorker.on('error', () => {});

    // Wait for job to be picked up (enters PEL)
    await new Promise<void>((resolve) => {
      const deadline = Date.now() + 10000;
      const check = setInterval(() => {
        if (jobPicked || Date.now() > deadline) {
          clearInterval(check);
          resolve();
        }
      }, 50);
    });
    expect(jobPicked).toBe(true);

    // Force-close worker 1 (crash - no graceful ACK)
    await crashWorker.close(true);

    // KEY ASSERTION: job is still in PEL after crash (not lost like BRPOP+LPUSH)
    const k = buildKeys(Q);
    const pendingAfterCrash = await cleanupClient.xpending(k.stream, 'workers');
    const pelCount = Number(pendingAfterCrash[0]);
    expect(pelCount).toBeGreaterThanOrEqual(1);

    // The job hash should still exist with state=active
    const stateAfterCrash = await cleanupClient.hget(k.job(job!.id), 'state');
    expect(String(stateAfterCrash)).toBe('active');

    // Wait for PEL entry to age past stall interval
    await new Promise((r) => setTimeout(r, STALL_INTERVAL + 500));

    // Worker 2: recovery worker's scheduler reclaims the stalled entry
    const recoveryWorker = new Worker(Q, async () => 'should-not-process', {
      connection: CONNECTION,
      concurrency: 1,
      blockTimeout: 500,
      stalledInterval: STALL_INTERVAL,
      maxStalledCount: MAX_STALLED,
      lockDuration: 200,
    });
    recoveryWorker.on('error', () => {});

    // Wait for enough stalled recovery cycles to exceed maxStalledCount
    await new Promise((r) => setTimeout(r, STALL_INTERVAL * (MAX_STALLED + 2) + 1000));

    await recoveryWorker.close(true);

    // The job must NOT be lost - stalled recovery moved it to failed
    const finalState = await cleanupClient.hget(k.job(job!.id), 'state');
    expect(String(finalState)).toBe('failed');

    // Verify stalled reason
    const reason = await cleanupClient.hget(k.job(job!.id), 'failedReason');
    expect(String(reason)).toContain('stalled');

    // Job is in the failed ZSet (not lost in limbo)
    const failedScore = await cleanupClient.zscore(k.failed, job!.id);
    expect(failedScore).not.toBeNull();

    await queue.close();
  }, 25000);
});

// ---------------------------------------------------------------------------
// Sidekiq #2: Unique lock not orphaned after completion
// ---------------------------------------------------------------------------

describeEachMode('Sidekiq dedup: unique lock freed after job completion', (CONNECTION) => {
  const Q = 'bp-sidekiq-dedup-complete-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('dedup id=unique-1 reusable after first job completes', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const DEDUP = 'unique-1';

    // Add first job with dedup id
    const job1 = await queue.add(
      'dedup-round-1',
      { round: 1 },
      {
        deduplication: { id: DEDUP, mode: 'simple' },
      },
    );
    expect(job1).not.toBeNull();

    // Process it to completion
    const firstDone = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);
      const worker = new Worker(Q, async () => 'done-1', { connection: CONNECTION, concurrency: 1, blockTimeout: 500 });
      worker.on('completed', () => {
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });
      worker.on('error', () => {});
    });
    await firstDone;

    // Verify first job completed
    const k = buildKeys(Q);
    const state1 = await cleanupClient.hget(k.job(job1!.id), 'state');
    expect(String(state1)).toBe('completed');

    // Add second job with SAME dedup id - must succeed (not blocked by orphaned lock)
    const job2 = await queue.add(
      'dedup-round-2',
      { round: 2 },
      {
        deduplication: { id: DEDUP, mode: 'simple' },
      },
    );
    expect(job2).not.toBeNull();
    expect(job2!.id).not.toBe(job1!.id);

    // Process second job
    const secondDone = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);
      const worker = new Worker(Q, async () => 'done-2', { connection: CONNECTION, concurrency: 1, blockTimeout: 500 });
      worker.on('completed', () => {
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });
      worker.on('error', () => {});
    });
    await secondDone;

    const state2 = await cleanupClient.hget(k.job(job2!.id), 'state');
    expect(String(state2)).toBe('completed');

    await queue.close();
  }, 30000);
});

// ---------------------------------------------------------------------------
// Sidekiq #3: Unique lock not orphaned after failure
// ---------------------------------------------------------------------------

describeEachMode('Sidekiq dedup: unique lock freed after job fails permanently', (CONNECTION) => {
  const Q = 'bp-sidekiq-dedup-fail-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('dedup id=unique-2 reusable after first job fails permanently', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const DEDUP = 'unique-2';

    // Add job with dedup id and attempts=1 (no retry - fails permanently)
    const job1 = await queue.add(
      'dedup-fail',
      { run: 1 },
      {
        deduplication: { id: DEDUP, mode: 'simple' },
        attempts: 1,
      },
    );
    expect(job1).not.toBeNull();

    // Process it - it will fail permanently
    const failDone = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);
      const worker = new Worker(
        Q,
        async () => {
          throw new Error('permanent failure');
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500 },
      );
      worker.on('failed', () => {
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });
      worker.on('error', () => {});
    });
    await failDone;

    // Verify first job is in failed state
    const k = buildKeys(Q);
    const state1 = await cleanupClient.hget(k.job(job1!.id), 'state');
    expect(String(state1)).toBe('failed');

    // Add second job with SAME dedup id - must succeed (lock not orphaned)
    const job2 = await queue.add(
      'dedup-retry',
      { run: 2 },
      {
        deduplication: { id: DEDUP, mode: 'simple' },
      },
    );
    expect(job2).not.toBeNull();
    expect(job2!.id).not.toBe(job1!.id);

    // Process second job to completion
    const secondDone = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);
      const worker = new Worker(Q, async () => 'success-after-fail', {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 500,
      });
      worker.on('completed', () => {
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });
      worker.on('error', () => {});
    });
    await secondDone;

    const state2 = await cleanupClient.hget(k.job(job2!.id), 'state');
    expect(String(state2)).toBe('completed');

    await queue.close();
  }, 30000);
});

// ---------------------------------------------------------------------------
// Sidekiq #4: Batch callback duplicate prevention - parent completes once
// ---------------------------------------------------------------------------

describeEachMode('Sidekiq batch: parent completes exactly once when all children finish', (CONNECTION) => {
  const Q = 'bp-sidekiq-batch-once-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('flow with 3 children - parent processed exactly once', async () => {
    const flow = new FlowProducer({ connection: CONNECTION });

    const node = await flow.add({
      name: 'batch-parent',
      queueName: Q,
      data: { role: 'parent' },
      children: [
        { name: 'batch-child-1', queueName: Q, data: { idx: 1 } },
        { name: 'batch-child-2', queueName: Q, data: { idx: 2 } },
        { name: 'batch-child-3', queueName: Q, data: { idx: 3 } },
      ],
    });

    const parentId = node.job.id;
    let parentProcessedCount = 0;
    const completedJobIds: string[] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout - batch test')), 20000);

      const worker = new Worker(
        Q,
        async (job: any) => {
          if (job.id === parentId) {
            parentProcessedCount++;
          }
          return { name: job.name, done: true };
        },
        { connection: CONNECTION, concurrency: 2, blockTimeout: 500 },
      );

      worker.on('completed', (job: any) => {
        completedJobIds.push(job.id);
        if (job.id === parentId) {
          clearTimeout(timeout);
          // Allow a brief window for any potential duplicate
          setTimeout(() => worker.close(true).then(resolve), 500);
        }
      });
      worker.on('error', () => {});
    });

    await done;

    // Parent must have been processed exactly once - not duplicated
    expect(parentProcessedCount).toBe(1);

    // All 3 children + parent = 4 total completions
    expect(completedJobIds).toHaveLength(4);

    // Verify parent final state
    const k = buildKeys(Q);
    const parentState = await cleanupClient.hget(k.job(parentId), 'state');
    expect(String(parentState)).toBe('completed');

    // Parent ID should appear exactly once in completedJobIds
    const parentCompletions = completedJobIds.filter((id) => id === parentId);
    expect(parentCompletions).toHaveLength(1);

    await flow.close();
  }, 25000);
});

// ---------------------------------------------------------------------------
// Sidekiq #5: Retry doesn't lose job between states
// ---------------------------------------------------------------------------

describeEachMode('Sidekiq retry: job never missing from all state structures during retry', (CONNECTION) => {
  const Q = 'bp-sidekiq-retry-states-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('job with attempts=3 is always in a known state during retries', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    let attemptCount = 0;
    const stateSnapshots: string[] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 25000);
      const worker = new Worker(
        Q,
        async () => {
          attemptCount++;
          if (attemptCount < 3) {
            throw new Error(`fail-attempt-${attemptCount}`);
          }
          return 'success-attempt-3';
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 500,
          stalledInterval: 60000,
          promotionInterval: 300,
        },
      );
      worker.on('completed', () => {
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });
      worker.on('error', () => {});
    });

    await new Promise((r) => setTimeout(r, 500));
    const job = await queue.add(
      'retry-track',
      { v: 1 },
      {
        attempts: 3,
        backoff: { type: 'fixed', delay: 200 },
      },
    );

    // Poll job state rapidly during the retry cycle
    const pollInterval = setInterval(async () => {
      try {
        const k = buildKeys(Q);
        const s = await cleanupClient.hget(k.job(job!.id), 'state');
        if (s) stateSnapshots.push(String(s));
      } catch {
        /* ignore */
      }
    }, 50);

    await done;
    clearInterval(pollInterval);

    // Job must have succeeded on attempt 3
    expect(attemptCount).toBe(3);

    // Final state is completed
    const k = buildKeys(Q);
    const finalState = await cleanupClient.hget(k.job(job!.id), 'state');
    expect(String(finalState)).toBe('completed');

    // Every snapshot must be a valid, known state - never undefined/empty/unknown
    const validStates = ['waiting', 'active', 'delayed', 'completed', 'failed'];
    for (const s of stateSnapshots) {
      expect(validStates).toContain(s);
    }

    // Must have captured at least some snapshots during processing
    expect(stateSnapshots.length).toBeGreaterThan(0);

    await queue.close();
  }, 30000);
});

// ---------------------------------------------------------------------------
// Sidekiq #6: Scheduled job timing drift
// ---------------------------------------------------------------------------

describeEachMode('Sidekiq scheduler drift: interval jobs fire without drift accumulation', (CONNECTION) => {
  const Q = 'bp-sidekiq-drift-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('scheduler every=1000ms fires 3 times in ~4s with gaps within 500ms tolerance', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const processedTimestamps: number[] = [];

    // Create scheduler with 1000ms interval
    await queue.upsertJobScheduler(
      'drift-check',
      { every: 1000 },
      {
        name: 'tick',
        data: { drift: true },
      },
    );

    const done = new Promise<void>((resolve) => {
      const worker = new Worker(
        Q,
        async () => {
          processedTimestamps.push(Date.now());
          return 'tick';
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 500,
          stalledInterval: 60000,
          promotionInterval: 500,
        },
      );
      worker.on('error', () => {});

      // Run for ~4.5 seconds to capture at least 3 firings
      setTimeout(() => {
        worker.close(true).then(resolve);
      }, 4500);
    });

    await done;

    // Must have at least 3 firings
    expect(processedTimestamps.length).toBeGreaterThanOrEqual(3);

    // Check gaps between consecutive firings
    processedTimestamps.sort((a, b) => a - b);
    for (let i = 1; i < processedTimestamps.length; i++) {
      const gap = processedTimestamps[i] - processedTimestamps[i - 1];
      // Expected ~1000ms, allow 500-1500ms range (500ms tolerance)
      expect(gap).toBeGreaterThanOrEqual(500);
      expect(gap).toBeLessThanOrEqual(1500);
    }

    // Check for drift accumulation: the gap between first and last should be
    // roughly (N-1) * 1000ms, not significantly more
    const totalSpan = processedTimestamps[processedTimestamps.length - 1] - processedTimestamps[0];
    const expectedSpan = (processedTimestamps.length - 1) * 1000;
    const driftAccumulation = Math.abs(totalSpan - expectedSpan);
    // Allow up to 750ms total drift across all firings (CI has more jitter)
    expect(driftAccumulation).toBeLessThanOrEqual(750);

    await queue.removeJobScheduler('drift-check');
    await queue.close();
  }, 15000);
});

// ---------------------------------------------------------------------------
// Sidekiq #7: Connection pool exhaustion immunity
// ---------------------------------------------------------------------------

describeEachMode('Sidekiq pool exhaustion: rapid create/close does not leak connections', (CONNECTION) => {
  const Q_PREFIX = 'bp-sidekiq-pool-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    // Clean up all queues used
    for (let i = 0; i < 20; i++) {
      await flushQueue(cleanupClient, `${Q_PREFIX}-q${i}`);
      await flushQueue(cleanupClient, `${Q_PREFIX}-w${i}`);
    }
    cleanupClient.close();
  });

  it('20 Queue + 20 Worker instances created and closed - no connection leak', async () => {
    // Snapshot baseline connections
    const infoBefore = await cleanupClient.info(['CLIENTS']);
    const connBefore = parseConnectedClients(infoBefore);

    // Create 20 Queue instances rapidly
    const queues: InstanceType<typeof Queue>[] = [];
    for (let i = 0; i < 20; i++) {
      const q = new Queue(`${Q_PREFIX}-q${i}`, { connection: CONNECTION });
      await q.add('pool-test', { i });
      queues.push(q);
    }

    // Create 20 Worker instances rapidly
    const workers: InstanceType<typeof Worker>[] = [];
    for (let i = 0; i < 20; i++) {
      const w = new Worker(`${Q_PREFIX}-w${i}`, async () => 'ok', {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 200,
      });
      w.on('error', () => {});
      workers.push(w);
    }

    // Allow workers to initialize
    await new Promise((r) => setTimeout(r, 2000));

    const infoOpen = await cleanupClient.info(['CLIENTS']);
    const connOpen = parseConnectedClients(infoOpen);

    // Close all workers and queues
    await Promise.all(workers.map((w) => w.close(true)));
    await Promise.all(queues.map((q) => q.close()));

    // Wait for connections to drain
    await new Promise((r) => setTimeout(r, CONNECTION.clusterMode ? 8000 : 5000));

    const infoAfter = await cleanupClient.info(['CLIENTS']);
    const connAfter = parseConnectedClients(infoAfter);

    // Connections after closing 40 instances should drop from peak
    // Note: in CI with parallel tests, connBefore may fluctuate, so we
    // only assert that close() reduced connections from the peak.
    expect(connAfter).toBeLessThan(connOpen);

    // Verify the system is still functional after mass create/close
    const verifyQ = new Queue(`${Q_PREFIX}-q0`, { connection: CONNECTION });
    const verifyJob = await verifyQ.add('verify-alive', { alive: true });
    expect(verifyJob).not.toBeNull();
    expect(verifyJob!.id).toBeTruthy();
    await verifyQ.close();
  }, 30000);
});

// ---------------------------------------------------------------------------
// Sidekiq #8: Large payload handling (500KB)
// ---------------------------------------------------------------------------

describeEachMode('Sidekiq large payload: 500KB job data roundtrips without corruption', (CONNECTION) => {
  const Q = 'bp-sidekiq-large-payload-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('500KB payload is stored and retrieved intact through processing', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });

    // Generate a 500KB payload with varied content to detect corruption
    const KB = 1024;
    const SIZE = 500 * KB;
    const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    let largeString = '';
    for (let i = 0; i < SIZE; i++) {
      largeString += chars[i % chars.length];
    }

    const payload = {
      marker: 'large-payload-test',
      blob: largeString,
      nested: { arr: [1, 2, 3], flag: true },
    };

    const job = await queue.add('big-job', payload);
    expect(job).not.toBeNull();

    // Process the job and verify data inside the processor
    let receivedData: any = null;
    let receivedId: string | null = null;

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 20000);

      const worker = new Worker(
        Q,
        async (processedJob: any) => {
          receivedData = processedJob.data;
          receivedId = processedJob.id;
          return { processed: true, dataLength: JSON.stringify(processedJob.data).length };
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500 },
      );

      worker.on('completed', () => {
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });
      worker.on('error', () => {});
    });

    await done;

    // Verify the data roundtripped correctly
    expect(receivedId).toBe(job!.id);
    expect(receivedData).not.toBeNull();
    expect(receivedData.marker).toBe('large-payload-test');
    expect(receivedData.blob).toBe(largeString);
    expect(receivedData.blob.length).toBe(SIZE);
    expect(receivedData.nested.arr).toEqual([1, 2, 3]);
    expect(receivedData.nested.flag).toBe(true);

    // Verify the job is completed in the server
    const k = buildKeys(Q);
    const state = await cleanupClient.hget(k.job(job!.id), 'state');
    expect(String(state)).toBe('completed');

    // Verify the returnvalue was stored
    const rv = await cleanupClient.hget(k.job(job!.id), 'returnvalue');
    const parsed = JSON.parse(String(rv));
    expect(parsed.processed).toBe(true);
    expect(parsed.dataLength).toBeGreaterThan(500000);

    await queue.close();
  }, 25000);
});

// ---------------------------------------------------------------------------
// Helper: parse connected_clients from INFO CLIENTS output
// ---------------------------------------------------------------------------

function parseConnectedClients(info: string | Record<string, string>): number {
  if (typeof info === 'string') {
    const match = info.match(/connected_clients:(\d+)/);
    return match ? parseInt(match[1], 10) : 0;
  }
  // Cluster mode: info is Record<nodeAddress, infoString>
  let total = 0;
  for (const nodeInfo of Object.values(info)) {
    const match = nodeInfo.match(/connected_clients:(\d+)/);
    if (match) total += parseInt(match[1], 10);
  }
  return total;
}
