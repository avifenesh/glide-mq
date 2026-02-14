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
          await new Promise(r => setTimeout(r, 200));
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
    await new Promise(r => setTimeout(r, 500));
    for (let i = 0; i < TOTAL; i++) {
      await queue.add(`active-${i}`, { i });
    }

    // Take multiple snapshots of active jobs during processing
    for (let snap = 0; snap < 5; snap++) {
      await new Promise(r => setTimeout(r, 100));
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

    await new Promise(r => setTimeout(r, 500));
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
    await queue.upsertJobScheduler('bp-repeat', { every: 500 }, {
      name: 'bp-tick',
      data: { seq: true },
    });

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
    await new Promise(r => setTimeout(r, 500));

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

    await queue.upsertJobScheduler('once-per-sec', { every: 1000 }, {
      name: 'tick',
      data: { check: 'dedup' },
    });

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
      const worker = new Worker(
        Q,
        async () => 'ok',
        {
          connection: CONNECTION,
          concurrency: 3,
          blockTimeout: 500,
          promotionInterval: 300,
          stalledInterval: 60000,
        },
      );

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
    await new Promise(r => setTimeout(r, 1000));

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

    await new Promise(r => setTimeout(r, 500));
    const j1 = await queue.add('err-1', { i: 1 });
    const j2 = await queue.add('err-2', { i: 2 });
    const j3 = await queue.add('err-3', { i: 3 });

    await done;

    // All three failures must be emitted - not swallowed
    expect(failedEvents).toHaveLength(3);
    const ids = failedEvents.map(e => e.jobId);
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
          children: [
            { name: `leak-child-${i}`, queueName: Q, data: { c: i } },
          ],
        });
        await flow.close();
      }

      // No MaxListenersExceeded warnings should have been emitted
      const listenerWarnings = warnings.filter(w => w.includes('MaxListeners'));
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
        await new Promise(r => setTimeout(r, 2000));
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
    await new Promise(r => setTimeout(r, 300));

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
          await new Promise(r => setTimeout(r, 4000));
          return 'long-done';
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 500,
          stalledInterval: 2000,
          lockDuration: 1000,  // heartbeat fires every 500ms (lockDuration/2)
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

    await new Promise(r => setTimeout(r, 500));
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

    await new Promise(r => setTimeout(r, 500));
    for (let i = 0; i < TOTAL; i++) {
      await queue.add(`acc-${i}`, { i }, { removeOnComplete: KEEP });
    }

    await done;

    // Wait a moment for async cleanup to settle
    await new Promise(r => setTimeout(r, 300));

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
    const firstWorker = new Worker(
      Q,
      async () => 'ok',
      {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 500,
        stalledInterval: 60000,
      },
    );
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
    await new Promise(r => setTimeout(r, 1000));

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
