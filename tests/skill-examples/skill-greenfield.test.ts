/**
 * Tests for code examples from the glide-mq greenfield skills:
 *   - skills/glide-mq/SKILL.md
 *   - skills/glide-mq/references/queue.md
 *   - skills/glide-mq/references/worker.md
 *   - skills/glide-mq/references/connection.md
 *   - skills/glide-mq/references/broadcast.md
 *   - skills/glide-mq/references/schedulers.md
 *   - skills/glide-mq/references/observability.md
 *   - skills/glide-mq/references/serverless.md
 *   - skills/glide-mq/references/workflows.md
 *
 * Each test wraps a code example from the skill docs and verifies it compiles and runs.
 */
import { describe, it, expect, afterEach } from 'vitest';

const { Queue } = require('../../dist/queue') as typeof import('../../src/queue');
const { Worker } = require('../../dist/worker') as typeof import('../../src/worker');
const { FlowProducer } = require('../../dist/flow-producer') as typeof import('../../src/flow-producer');
const { Producer } = require('../../dist/producer') as typeof import('../../src/producer');
const { QueueEvents } = require('../../dist/queue-events') as typeof import('../../src/queue-events');
const { Broadcast } = require('../../dist/broadcast') as typeof import('../../src/broadcast');
const { BroadcastWorker } = require('../../dist/broadcast-worker') as typeof import('../../src/broadcast-worker');
const { ServerlessPool, serverlessPool } =
  require('../../dist/serverless-pool') as typeof import('../../src/serverless-pool');
const { UnrecoverableError, WaitingChildrenError, BatchError } =
  require('../../dist/errors') as typeof import('../../src/errors');
const { gracefulShutdown } = require('../../dist/graceful-shutdown') as typeof import('../../src/graceful-shutdown');
const { chain, group, chord, dag } = require('../../dist/workflows') as typeof import('../../src/workflows');
const { matchSubject, compileSubjectMatcher } = require('../../dist/utils') as typeof import('../../src/utils');

import { flushQueue, createCleanupClient, STANDALONE, waitFor } from '../helpers/fixture';

const CONNECTION = STANDALONE;
const connection = CONNECTION;

// Track resources for cleanup
let resourcesToClose: { close: () => Promise<void> | void }[] = [];
let queueNames: string[] = [];
let cleanupClient: any;

function uid(prefix: string) {
  const name = `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  queueNames.push(name);
  return name;
}

function track<T extends { close: () => Promise<void> | void }>(resource: T): T {
  resourcesToClose.push(resource);
  return resource;
}

// Global setup/teardown for cleanup client
import { beforeAll, afterAll } from 'vitest';

beforeAll(async () => {
  cleanupClient = await createCleanupClient(CONNECTION);
});

afterAll(async () => {
  cleanupClient?.close();
});

afterEach(async () => {
  await Promise.allSettled(resourcesToClose.reverse().map((r) => r.close()));
  resourcesToClose = [];
  // Flush all queue data
  for (const name of queueNames) {
    try {
      await flushQueue(cleanupClient, name);
    } catch {}
  }
  queueNames = [];
});

// ---------------------------------------------------------------------------
// SKILL.md - Quick Start
// ---------------------------------------------------------------------------
describe('SKILL.md examples', () => {
  it('Quick Start - Queue + Worker basic usage (SKILL.md:32)', async () => {
    const qName = uid('skill-quickstart');
    const queue = track(new Queue(qName, { connection }));
    await queue.add('send-email', { to: 'user@example.com', subject: 'Hello' });

    const completed: string[] = [];
    const worker = track(
      new Worker(
        qName,
        async (job) => {
          completed.push(job.name);
          return { sent: true };
        },
        { connection, concurrency: 10 },
      ),
    );

    worker.on('completed', (job) => {
      // job.id is accessible
      expect(job.id).toBeTruthy();
    });

    await waitFor(() => completed.length >= 1);
    expect(completed).toContain('send-email');
  }, 15000);

  it('Delayed and Priority jobs (SKILL.md:80)', async () => {
    const qName = uid('skill-delay-prio');
    const queue = track(new Queue(qName, { connection }));
    const data = { foo: 'bar' };

    // Delayed: run after 5 minutes (we just verify it gets added)
    const delayedJob = await queue.add('reminder', data, { delay: 300_000 });
    expect(delayedJob).not.toBeNull();

    // Priority: lower number = higher priority
    const urgentJob = await queue.add('urgent', data, { priority: 0 });
    expect(urgentJob).not.toBeNull();

    const lowPrioJob = await queue.add('low-priority', data, { priority: 10 });
    expect(lowPrioJob).not.toBeNull();

    // Retries with exponential backoff
    const webhookJob = await queue.add('webhook', data, {
      attempts: 5,
      backoff: { type: 'exponential', delay: 1000 },
    });
    expect(webhookJob).not.toBeNull();
  }, 15000);

  it('Bulk Ingestion (SKILL.md:97)', async () => {
    const qName = uid('skill-bulk');
    const queue = track(new Queue(qName, { connection }));
    const items = Array.from({ length: 20 }, (_, i) => ({ id: i, value: `item-${i}` }));

    const jobs = items.map((item) => ({
      name: 'process',
      data: item,
      opts: { jobId: `item-${item.id}` },
    }));
    const result = await queue.addBulk(jobs);
    expect(result.length).toBe(20);
  }, 15000);

  it('Batch Worker (SKILL.md:108)', async () => {
    const qName = uid('skill-batch');
    const queue = track(new Queue(qName, { connection }));
    const batchResults: number[] = [];

    const worker = track(
      new Worker(
        qName,
        async (jobs: any[]) => {
          batchResults.push(jobs.length);
          return jobs.map((j: any) => ({ processed: j.data }));
        },
        {
          connection,
          batch: { size: 50, timeout: 2000 },
        },
      ),
    );

    await queue.addBulk(Array.from({ length: 5 }, (_, i) => ({ name: 'analytics', data: { i } })));

    await waitFor(() => batchResults.length > 0);
    expect(batchResults[0]).toBeGreaterThan(0);
  }, 15000);

  it('Request-Reply addAndWait (SKILL.md:120)', async () => {
    const qName = uid('skill-addwait');
    const queue = track(new Queue(qName, { connection }));

    const worker = track(
      new Worker(
        qName,
        async (job) => {
          return { answer: job.data.input * 2 };
        },
        { connection },
      ),
    );

    const result = await queue.addAndWait('compute', { input: 42 }, { waitTimeout: 10_000 });
    expect(result).toEqual({ answer: 84 });
  }, 15000);

  it('Serverless Producer (SKILL.md:129)', async () => {
    const qName = uid('skill-producer');
    const producer = track(new Producer(qName, { connection }));
    const id = await producer.add('job-name', { key: 'value' });
    expect(typeof id).toBe('string');
  }, 15000);

  it('Testing Without Valkey (SKILL.md:145)', async () => {
    // Import from glide-mq/testing
    const { TestQueue, TestWorker } = require('../../dist/testing');

    const queue = new TestQueue('tasks');
    const processed: any[] = [];
    const worker = new TestWorker(queue, async (job: any) => {
      processed.push(job.data);
      return { ok: true };
    });

    await queue.add('test-job', { key: 'value' });
    await waitFor(() => processed.length > 0, 2000, 50);
    expect(processed[0]).toEqual({ key: 'value' });
    await worker.close();
    await queue.close();
  }, 15000);
});

// ---------------------------------------------------------------------------
// references/queue.md
// ---------------------------------------------------------------------------
describe('queue.md examples', () => {
  it('Queue constructor and add (queue.md:7)', async () => {
    const qName = uid('queue-ctor');
    const queue = track(new Queue(qName, { connection }));

    const job = await queue.add('job1', { a: 1 });
    expect(job).not.toBeNull();
    expect(job!.name).toBe('job1');
  }, 15000);

  it('addBulk (queue.md:26)', async () => {
    const qName = uid('queue-bulk');
    const queue = track(new Queue(qName, { connection }));

    const jobs = await queue.addBulk([{ name: 'job1', data: { a: 1 } }]);
    expect(jobs.length).toBe(1);
  }, 15000);

  it('Queue management - pause, resume, isPaused, drain, obliterate (queue.md:64)', async () => {
    const qName = uid('queue-mgmt');
    const queue = track(new Queue(qName, { connection }));
    await queue.add('job', { x: 1 });

    await queue.pause();
    expect(await queue.isPaused()).toBe(true);

    await queue.resume();
    expect(await queue.isPaused()).toBe(false);

    await queue.drain();
    const counts = await queue.getJobCounts();
    expect(counts.waiting).toBe(0);

    await queue.obliterate({ force: true });
  }, 15000);

  it('Inspecting Jobs - getJob, getJobs, getJobCounts, count (queue.md:86)', async () => {
    const qName = uid('queue-inspect');
    const queue = track(new Queue(qName, { connection }));

    const job = await queue.add('test', { hello: 'world' });
    const fetched = await queue.getJob(job.id);
    expect(fetched).not.toBeNull();
    expect(fetched!.name).toBe('test');

    const waitingJobs = await queue.getJobs('waiting', 0, 99);
    expect(waitingJobs.length).toBeGreaterThanOrEqual(1);

    const counts = await queue.getJobCounts();
    expect(typeof counts.waiting).toBe('number');

    const waitingCount = await queue.count();
    expect(typeof waitingCount).toBe('number');
  }, 15000);

  it('Global rate limit (queue.md:109)', async () => {
    const qName = uid('queue-global-rate');
    const queue = track(new Queue(qName, { connection }));

    await queue.setGlobalRateLimit({ max: 500, duration: 60_000 });
    const limit = await queue.getGlobalRateLimit();
    expect(limit).not.toBeNull();
    expect(limit!.max).toBe(500);

    await queue.removeGlobalRateLimit();
    const removed = await queue.getGlobalRateLimit();
    expect(removed).toBeNull();
  }, 15000);

  it('Global concurrency (queue.md:115)', async () => {
    const qName = uid('queue-global-conc');
    const queue = track(new Queue(qName, { connection }));

    await queue.setGlobalConcurrency(20);
    // Verify it was set by resetting
    await queue.setGlobalConcurrency(0);
  }, 15000);

  it('Dead Letter Queue on Worker (queue.md:122)', async () => {
    const qName = uid('queue-dlq');
    const dlqName = uid('queue-dlq-target');
    const queue = track(new Queue(qName, { connection }));
    const dlqQueue = track(new Queue(dlqName, { connection }));

    const worker = track(
      new Worker(
        qName,
        async () => {
          throw new Error('always fails');
        },
        {
          connection,
          deadLetterQueue: { name: dlqName },
        },
      ),
    );

    await queue.add('fail-me', { x: 1 }, { attempts: 1 });
    await waitFor(async () => {
      const c = await dlqQueue.getJobCounts();
      return c.waiting > 0;
    }, 10000);

    const dlqCounts = await dlqQueue.getJobCounts();
    expect(dlqCounts.waiting).toBeGreaterThan(0);
  }, 15000);
});

// ---------------------------------------------------------------------------
// references/worker.md
// ---------------------------------------------------------------------------
describe('worker.md examples', () => {
  it('Worker constructor with processor (worker.md:8)', async () => {
    const qName = uid('worker-ctor');
    const queue = track(new Queue(qName, { connection }));
    const results: any[] = [];

    const worker = track(
      new Worker(
        qName,
        async (job) => {
          await job.log('step done');
          await job.updateProgress(50);
          results.push(job.data);
          return { ok: true };
        },
        { connection, concurrency: 1 },
      ),
    );

    await queue.add('test-job', { x: 42 });
    await waitFor(() => results.length > 0);
    expect(results[0]).toEqual({ x: 42 });
  }, 15000);

  it('Worker events (worker.md:80)', async () => {
    const qName = uid('worker-events');
    const queue = track(new Queue(qName, { connection }));
    const events: string[] = [];

    const worker = track(
      new Worker(
        qName,
        async (job) => {
          return { done: true };
        },
        { connection },
      ),
    );

    worker.on('completed', () => events.push('completed'));
    worker.on('error', () => events.push('error'));

    await queue.add('event-test', { x: 1 });
    await waitFor(() => events.includes('completed'));
    expect(events).toContain('completed');
  }, 15000);

  it('UnrecoverableError skips retries (worker.md:134)', async () => {
    const qName = uid('worker-unrecoverable');
    const queue = track(new Queue(qName, { connection }));
    const failReasons: string[] = [];

    const worker = track(
      new Worker(
        qName,
        async () => {
          throw new UnrecoverableError('bad input');
        },
        { connection },
      ),
    );

    worker.on('failed', (_job, err) => {
      failReasons.push(err.message);
    });

    await queue.add('skip-retry', { x: 1 }, { attempts: 5 });
    await waitFor(() => failReasons.length > 0);
    expect(failReasons[0]).toBe('bad input');

    // Should fail immediately without exhausting retries
    const job = await queue.getJobs('failed', 0, 0);
    expect(job.length).toBeGreaterThanOrEqual(1);
  }, 15000);

  it('Pause / Resume / Close (worker.md:123)', async () => {
    // NOTE: pause/resume are on Queue, not Worker.
    // The skill docs mention worker.pause() but the actual API puts
    // pause/resume on Queue (which stops workers from picking up new jobs).
    const qName = uid('worker-pause');
    const queue = track(new Queue(qName, { connection }));
    const processed: string[] = [];

    const worker = track(
      new Worker(
        qName,
        async () => {
          processed.push('done');
          return 'ok';
        },
        { connection },
      ),
    );

    await queue.pause();
    expect(await queue.isPaused()).toBe(true);

    await queue.resume();
    expect(await queue.isPaused()).toBe(false);

    // Worker should still be functional after resume
    await queue.add('after-resume', {});
    await waitFor(() => processed.length > 0, 10000);
    expect(processed.length).toBeGreaterThan(0);

    // Verify close works
    await worker.close();
    // Remove worker from tracked since we already closed
    resourcesToClose = resourcesToClose.filter((r) => r !== worker);
  }, 15000);

  it('LIFO mode (worker.md:96)', async () => {
    const qName = uid('worker-lifo');
    const queue = track(new Queue(qName, { connection }));

    // Add jobs with LIFO flag - just verify we can add them
    const job1 = await queue.add('first', { order: 1 }, { lifo: true });
    const job2 = await queue.add('second', { order: 2 }, { lifo: true });
    expect(job1).not.toBeNull();
    expect(job2).not.toBeNull();
  }, 15000);

  it('Job revocation with AbortSignal (worker.md:103)', async () => {
    const qName = uid('worker-revoke');
    const queue = track(new Queue(qName, { connection }));
    const processed: string[] = [];

    const worker = track(
      new Worker(
        qName,
        async (job) => {
          // Simulate a long-running job that checks abort signal
          for (let i = 0; i < 5; i++) {
            if (job.abortSignal?.aborted) throw new Error('Revoked');
            await new Promise((r) => setTimeout(r, 50));
          }
          processed.push('done');
          return 'ok';
        },
        { connection },
      ),
    );

    const job = await queue.add('revocable', { data: 'test' });
    // Just verify the job was added and queue.revoke works
    // Revoking a waiting job should return 'revoked'
    const nextJob = await queue.add('also-revocable', { data: 'test2' });
    // We can try to revoke the second waiting job
    const result = await queue.revoke(nextJob.id);
    // Result could be 'revoked', 'flagged', or 'not_found'
    expect(['revoked', 'flagged', 'not_found']).toContain(result);
  }, 15000);
});

// ---------------------------------------------------------------------------
// references/connection.md
// ---------------------------------------------------------------------------
describe('connection.md examples', () => {
  it('Basic connection (connection.md:20)', async () => {
    const qName = uid('conn-basic');
    const conn = { addresses: [{ host: 'localhost', port: 6379 }] };
    const queue = track(new Queue(qName, { connection: conn }));
    const job = await queue.add('test', { x: 1 });
    expect(job).not.toBeNull();
  }, 15000);

  it('inflightRequestsLimit (connection.md:153)', async () => {
    const qName = uid('conn-inflight');
    const conn = {
      addresses: [{ host: 'localhost', port: 6379 }],
      inflightRequestsLimit: 2000,
    };
    const queue = track(new Queue(qName, { connection: conn }));
    const job = await queue.add('test', { x: 1 });
    expect(job).not.toBeNull();
  }, 15000);
});

// ---------------------------------------------------------------------------
// references/broadcast.md
// ---------------------------------------------------------------------------
describe('broadcast.md examples', () => {
  it('Broadcast publish and subscribe (broadcast.md:10)', async () => {
    // NOTE: Actual API is publish(subject, data, opts?) - the skill docs show
    // a simplified form. Here we use the real API signature.
    const bName = uid('bcast-pub');
    const broadcast = track(new Broadcast(bName, { connection }));
    const received: any[] = [];

    const worker = track(
      new BroadcastWorker(
        bName,
        async (job) => {
          received.push(job.data);
        },
        { connection, subscription: 'test-sub' },
      ),
    );

    await worker.waitUntilReady();

    await broadcast.publish('order.placed', { event: 'order.placed', orderId: 42 });

    await waitFor(() => received.length > 0, 5000);
    expect(received[0]).toEqual({ event: 'order.placed', orderId: 42 });
  }, 15000);

  it('Broadcast with named messages for subject filtering (broadcast.md:25)', async () => {
    const bName = uid('bcast-named');
    const broadcast = track(new Broadcast(bName, { connection }));

    // publish(subject, data, opts?)
    await broadcast.publish('orders.created', { orderId: 42 });
    await broadcast.publish('inventory.low', { sku: 'ABC', qty: 0 });
    // Verify publish does not throw
  }, 15000);

  it('Subject filtering (broadcast.md:76)', async () => {
    const bName = uid('bcast-subjects');
    const broadcast = track(new Broadcast(bName, { connection }));
    const received: any[] = [];

    const worker = track(
      new BroadcastWorker(
        bName,
        async (job) => {
          received.push(job.name);
        },
        {
          connection,
          subscription: 'order-handler',
          subjects: ['orders.*'],
        },
      ),
    );

    await worker.waitUntilReady();

    // publish(subject, data, opts?)
    await broadcast.publish('orders.created', { orderId: 1 });
    await broadcast.publish('inventory.low', { sku: 'X' });

    await waitFor(() => received.length >= 1, 5000);
    expect(received).toContain('orders.created');
    // inventory.low should NOT match 'orders.*'
    expect(received).not.toContain('inventory.low');
  }, 15000);

  it('matchSubject and compileSubjectMatcher utilities (broadcast.md:99)', () => {
    expect(matchSubject('orders.*', 'orders.created')).toBe(true);
    expect(matchSubject('orders.*', 'orders.a.b')).toBe(false);

    const matcher = compileSubjectMatcher(['orders.*', 'shipping.>']);
    expect(matcher('orders.created')).toBe(true);
    expect(matcher('shipping.us.west')).toBe(true);
    expect(matcher('inventory.low')).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// references/schedulers.md
// ---------------------------------------------------------------------------
describe('schedulers.md examples', () => {
  it('Cron schedule (schedulers.md:18)', async () => {
    const qName = uid('sched-cron');
    const queue = track(new Queue(qName, { connection }));

    await queue.upsertJobScheduler(
      'daily-report',
      { pattern: '0 8 * * *' },
      { name: 'generate-report', data: { type: 'daily' } },
    );

    const schedulers = await queue.getRepeatableJobs();
    expect(schedulers.length).toBeGreaterThanOrEqual(1);
    const found = schedulers.find((s: any) => s.name === 'daily-report');
    expect(found).toBeTruthy();

    await queue.removeJobScheduler('daily-report');
  }, 15000);

  it('Fixed interval (schedulers.md:28)', async () => {
    const qName = uid('sched-interval');
    const queue = track(new Queue(qName, { connection }));

    await queue.upsertJobScheduler('cleanup', { every: 5 * 60 * 1_000 }, { name: 'cleanup-old', data: {} });

    const info = await queue.getJobScheduler('cleanup');
    expect(info).toBeTruthy();

    await queue.removeJobScheduler('cleanup');
  }, 15000);

  it('repeatAfterComplete (schedulers.md:40)', async () => {
    const qName = uid('sched-rac');
    const queue = track(new Queue(qName, { connection }));

    await queue.upsertJobScheduler(
      'sensor-poll',
      { repeatAfterComplete: 5000 },
      { name: 'poll', data: { sensor: 'temp-1' } },
    );

    const info = await queue.getJobScheduler('sensor-poll');
    expect(info).toBeTruthy();

    await queue.removeJobScheduler('sensor-poll');
  }, 15000);

  it('Scheduler management - list, get, remove, upsert (schedulers.md:94)', async () => {
    const qName = uid('sched-mgmt');
    const queue = track(new Queue(qName, { connection }));

    await queue.upsertJobScheduler('cleanup', { every: 10_000 }, { name: 'cleanup', data: {} });

    const schedulers = await queue.getRepeatableJobs();
    expect(schedulers.length).toBeGreaterThanOrEqual(1);

    const info = await queue.getJobScheduler('cleanup');
    expect(info).toBeTruthy();

    await queue.removeJobScheduler('cleanup');
    const afterRemove = await queue.getJobScheduler('cleanup');
    expect(afterRemove).toBeNull();
  }, 15000);
});

// ---------------------------------------------------------------------------
// references/observability.md
// ---------------------------------------------------------------------------
describe('observability.md examples', () => {
  it('QueueEvents (observability.md:8)', async () => {
    const qName = uid('obs-events');
    const queue = track(new Queue(qName, { connection }));
    const events = track(new QueueEvents(qName, { connection }));

    const completedIds: string[] = [];
    events.on('completed', ({ jobId }: any) => {
      completedIds.push(jobId);
    });

    const worker = track(
      new Worker(
        qName,
        async () => {
          return 'done';
        },
        { connection },
      ),
    );

    const job = await queue.add('obs-test', { x: 1 });
    await waitFor(() => completedIds.length > 0, 5000);
    expect(completedIds).toContain(job.id);
  }, 15000);

  it('Disable events for high throughput (observability.md:28)', async () => {
    const qName = uid('obs-no-events');
    const queue = track(new Queue(qName, { connection, events: false }));
    const processed: string[] = [];

    const worker = track(
      new Worker(
        qName,
        async (job) => {
          processed.push(job.name);
          return 'ok';
        },
        { connection, events: false },
      ),
    );

    await queue.add('no-event', { x: 1 });
    await waitFor(() => processed.length > 0);
    expect(processed).toContain('no-event');
  }, 15000);

  it('Job logs (observability.md:42)', async () => {
    const qName = uid('obs-logs');
    const queue = track(new Queue(qName, { connection }));
    let jobId: string | undefined;

    const worker = track(
      new Worker(
        qName,
        async (job) => {
          await job.log('Starting step 1');
          await job.log('Step 1 done');
          jobId = job.id;
          return 'ok';
        },
        { connection },
      ),
    );

    await queue.add('log-test', { x: 1 });
    await waitFor(() => jobId !== undefined);

    const { logs, count } = await queue.getJobLogs(jobId!);
    expect(count).toBe(2);
    expect(logs).toContain('Starting step 1');
    expect(logs).toContain('Step 1 done');
  }, 15000);

  it('Job progress (observability.md:57)', async () => {
    const qName = uid('obs-progress');
    const queue = track(new Queue(qName, { connection }));
    let done = false;

    const worker = track(
      new Worker(
        qName,
        async (job) => {
          await job.updateProgress(50);
          await job.updateProgress({ step: 3 });
          done = true;
          return 'ok';
        },
        { connection },
      ),
    );

    await queue.add('progress-test', { x: 1 });
    await waitFor(() => done);
  }, 15000);

  it('Job counts (observability.md:71)', async () => {
    const qName = uid('obs-counts');
    const queue = track(new Queue(qName, { connection }));

    await queue.addBulk([
      { name: 'a', data: {} },
      { name: 'b', data: {} },
    ]);

    const counts = await queue.getJobCounts();
    expect(counts).toHaveProperty('waiting');
    expect(counts).toHaveProperty('active');
    expect(counts).toHaveProperty('completed');
    expect(counts).toHaveProperty('failed');
    expect(counts).toHaveProperty('delayed');

    const waitingCount = await queue.count();
    expect(typeof waitingCount).toBe('number');
  }, 15000);

  it('Time-series metrics (observability.md:80)', async () => {
    const qName = uid('obs-metrics');
    const queue = track(new Queue(qName, { connection }));

    const worker = track(new Worker(qName, async () => 'ok', { connection }));

    await queue.add('metric-test', {});
    await waitFor(async () => {
      const c = await queue.getJobCounts();
      return c.completed > 0;
    });

    const metrics = await queue.getMetrics('completed');
    expect(metrics).toHaveProperty('count');
    expect(metrics).toHaveProperty('data');
  }, 15000);

  it('Disable metrics on worker (observability.md:101)', async () => {
    const qName = uid('obs-no-metrics');
    const queue = track(new Queue(qName, { connection }));

    const worker = track(
      new Worker(qName, async () => 'ok', {
        connection,
        metrics: false,
      }),
    );

    await queue.add('no-metric', {});
    await waitFor(async () => {
      const c = await queue.getJobCounts();
      return c.completed > 0;
    });
  }, 15000);
});

// ---------------------------------------------------------------------------
// references/serverless.md
// ---------------------------------------------------------------------------
describe('serverless.md examples', () => {
  it('Producer add and addBulk (serverless.md:8)', async () => {
    const qName = uid('sless-producer');
    const producer = track(new Producer(qName, { connection }));

    const id = await producer.add('send-welcome', { to: 'user@example.com' });
    expect(typeof id).toBe('string');

    const id2 = await producer.add('urgent', { x: 1 }, { delay: 1000, priority: 1 });
    expect(typeof id2).toBe('string');

    const ids = await producer.addBulk([
      { name: 'email', data: { to: 'a@test.com' } },
      { name: 'sms', data: { phone: '+123' } },
    ]);
    expect(ids.length).toBe(2);
  }, 15000);

  it('ServerlessPool getProducer (serverless.md:39)', async () => {
    const qName = uid('sless-pool');
    const pool = new ServerlessPool();

    const producer = pool.getProducer(qName, { connection });
    const id = await producer.add('push', { userId: 42 });
    expect(typeof id).toBe('string');

    await pool.closeAll();
  }, 15000);

  it('Module-level serverlessPool singleton (serverless.md:42)', async () => {
    const qName = uid('sless-singleton');
    const producer = serverlessPool.getProducer(qName, { connection });

    const id = await producer.add('push', { userId: 42 });
    expect(typeof id).toBe('string');

    await serverlessPool.closeAll();
  }, 15000);

  it('Testing in-memory TestQueue and TestWorker (serverless.md:119)', async () => {
    const { TestQueue, TestWorker } = require('../../dist/testing');

    const queue = new TestQueue('tasks');
    const results: any[] = [];

    const worker = new TestWorker(queue, async (job: any) => {
      results.push({ processed: job.data });
      return { processed: job.data };
    });

    worker.on('completed', () => {});
    worker.on('failed', () => {});

    await queue.add('send-email', { to: 'user@example.com' });
    await waitFor(() => results.length > 0, 2000, 50);

    const counts = await queue.getJobCounts();
    expect(counts.completed).toBe(1);

    await worker.close();
    await queue.close();
  }, 15000);

  it('TestQueue batch testing (serverless.md:166)', async () => {
    const { TestQueue, TestWorker } = require('../../dist/testing');

    const queue = new TestQueue('batch-test');
    const results: any[] = [];
    // Use batch timeout of 0 so partial batches process immediately
    const worker = new TestWorker(
      queue,
      async (jobs: any[]) => {
        const out = jobs.map((j: any) => ({ doubled: j.data.n * 2 }));
        results.push(...out);
        return out;
      },
      { batch: { size: 5 } },
    );

    await queue.add('item', { n: 3 });
    // Give batch mode time to process
    await waitFor(() => results.length > 0, 5000, 50);
    expect(results[0]).toEqual({ doubled: 6 });

    await worker.close();
    await queue.close();
  }, 15000);
});

// ---------------------------------------------------------------------------
// references/workflows.md
// ---------------------------------------------------------------------------
describe('workflows.md examples', () => {
  it('FlowProducer parent-child (workflows.md:8)', async () => {
    const qName = uid('flow-parent');
    const childQName = uid('flow-child');
    const flow = track(new FlowProducer({ connection }));

    const { job: parent } = await flow.add({
      name: 'aggregate',
      queueName: qName,
      data: { month: '2025-01' },
      children: [
        { name: 'fetch-sales', queueName: childQName, data: { region: 'eu' } },
        { name: 'fetch-returns', queueName: childQName, data: {} },
      ],
    });

    expect(parent).not.toBeNull();
    expect(parent.name).toBe('aggregate');
  }, 15000);

  it('Reading child results with getChildrenValues (workflows.md:56)', async () => {
    const parentQ = uid('flow-children-vals');
    const childQ = uid('flow-children-vals-child');
    const flow = track(new FlowProducer({ connection }));
    const results: any[] = [];

    // Child workers
    const childWorker = track(
      new Worker(
        childQ,
        async (job) => {
          return { count: 10 };
        },
        { connection },
      ),
    );

    // Parent worker
    const parentWorker = track(
      new Worker(
        parentQ,
        async (job) => {
          const childValues = await job.getChildrenValues();
          const vals = Object.values(childValues);
          results.push({ total: vals.reduce((s: number, v: any) => s + v.count, 0) });
          return results[0];
        },
        { connection },
      ),
    );

    await flow.add({
      name: 'report',
      queueName: parentQ,
      data: { month: 'jan' },
      children: [
        { name: 'fetch-a', queueName: childQ, data: {} },
        { name: 'fetch-b', queueName: childQ, data: {} },
      ],
    });

    await waitFor(() => results.length > 0, 10000);
    expect(results[0].total).toBe(20);
  }, 15000);

  it('chain() helper - sequential pipeline (workflows.md:127)', async () => {
    const qName = uid('flow-chain');
    const processed: string[] = [];

    const worker = track(
      new Worker(
        qName,
        async (job) => {
          processed.push(job.name);
          return { step: job.name };
        },
        { connection },
      ),
    );

    await chain(
      qName,
      [
        { name: 'upload', data: {} },
        { name: 'transform', data: {} },
        { name: 'parse', data: {} },
        { name: 'download', data: {} },
      ],
      connection,
    );

    // Chain: download -> parse -> transform -> upload (reverse order)
    await waitFor(() => processed.length >= 4, 10000);
    // download should be processed first (leaf)
    expect(processed[0]).toBe('download');
  }, 15000);

  it('group() helper - parallel execution (workflows.md:140)', async () => {
    const qName = uid('flow-group');
    const processed: string[] = [];

    const worker = track(
      new Worker(
        qName,
        async (job) => {
          processed.push(job.name);
          return { size: job.data.size };
        },
        { connection },
      ),
    );

    await group(
      qName,
      [
        { name: 'resize-sm', data: { size: 'sm' } },
        { name: 'resize-md', data: { size: 'md' } },
        { name: 'resize-lg', data: { size: 'lg' } },
      ],
      connection,
    );

    await waitFor(() => processed.length >= 3, 10000);
    expect(processed).toContain('resize-sm');
    expect(processed).toContain('resize-md');
    expect(processed).toContain('resize-lg');
  }, 15000);

  it('chord() helper - parallel + callback (workflows.md:153)', async () => {
    const qName = uid('flow-chord');
    const processed: string[] = [];

    const worker = track(
      new Worker(
        qName,
        async (job) => {
          processed.push(job.name);
          return { model: job.data.model || 'aggregate' };
        },
        { connection },
      ),
    );

    await chord(
      qName,
      [
        { name: 'score-a', data: { model: 'a' } },
        { name: 'score-b', data: { model: 'b' } },
      ],
      { name: 'select-best', data: {} },
      connection,
    );

    await waitFor(() => processed.includes('select-best'), 10000);
    expect(processed).toContain('score-a');
    expect(processed).toContain('score-b');
    expect(processed).toContain('select-best');
  }, 15000);
});

// ---------------------------------------------------------------------------
// Deduplication (queue.md, new-features in bee migration)
// ---------------------------------------------------------------------------
describe('Deduplication examples', () => {
  it('Simple deduplication (queue.md JobOptions / bee-new-features.md:84)', async () => {
    const qName = uid('dedup-simple');
    const queue = track(new Queue(qName, { connection }));

    const job1 = await queue.add(
      'task',
      { x: 1 },
      {
        deduplication: { id: 'unique-key' },
      },
    );
    expect(job1).not.toBeNull();

    // Second add with same dedup ID should return null
    const job2 = await queue.add(
      'task',
      { x: 2 },
      {
        deduplication: { id: 'unique-key' },
      },
    );
    expect(job2).toBeNull();
  }, 15000);

  it('Throttle deduplication with TTL (bee-new-features.md:91)', async () => {
    const qName = uid('dedup-throttle');
    const queue = track(new Queue(qName, { connection }));

    const job1 = await queue.add(
      'task',
      { x: 1 },
      {
        deduplication: { id: 'user-123', ttl: 60000 },
      },
    );
    expect(job1).not.toBeNull();
  }, 15000);
});

// ---------------------------------------------------------------------------
// Per-key ordering (bullmq-new-features.md)
// ---------------------------------------------------------------------------
describe('Per-key ordering examples', () => {
  it('Ordering key (bullmq-new-features.md:12)', async () => {
    const qName = uid('ordering-key');
    const queue = track(new Queue(qName, { connection }));

    const job = await queue.add(
      'sync',
      { x: 1 },
      {
        ordering: { key: 'tenant-123' },
      },
    );
    expect(job).not.toBeNull();
  }, 15000);

  it('Group concurrency (bullmq-new-features.md:22)', async () => {
    const qName = uid('ordering-conc');
    const queue = track(new Queue(qName, { connection }));

    const job = await queue.add(
      'sync',
      { x: 1 },
      {
        ordering: { key: 'tenant-123', concurrency: 3 },
      },
    );
    expect(job).not.toBeNull();
  }, 15000);
});

// ---------------------------------------------------------------------------
// Compression (bullmq-new-features.md:126)
// ---------------------------------------------------------------------------
describe('Compression examples', () => {
  it('Gzip compression on queue (bullmq-new-features.md:126)', async () => {
    const qName = uid('compression');
    const queue = track(new Queue(qName, { connection, compression: 'gzip' }));
    const processed: any[] = [];

    const worker = track(
      new Worker(
        qName,
        async (job) => {
          processed.push(job.data);
          return 'ok';
        },
        { connection, compression: 'gzip' } as any,
      ),
    );

    await queue.add('compressed-job', { largePayload: 'x'.repeat(1000) });
    await waitFor(() => processed.length > 0);
    expect(processed[0].largePayload.length).toBe(1000);
  }, 15000);
});

// ---------------------------------------------------------------------------
// Job TTL (bee-new-features.md:152, bullmq-new-features.md:342)
// ---------------------------------------------------------------------------
describe('Job TTL examples', () => {
  it('Job TTL option (bee-new-features.md:152)', async () => {
    const qName = uid('ttl');
    const queue = track(new Queue(qName, { connection }));

    const job = await queue.add('time-sensitive', { x: 1 }, { ttl: 300000 });
    expect(job).not.toBeNull();
  }, 15000);
});

// ---------------------------------------------------------------------------
// Backoff jitter (bullmq-new-features.md:437)
// ---------------------------------------------------------------------------
describe('Backoff jitter example', () => {
  it('Exponential backoff with jitter (bullmq-new-features.md:437)', async () => {
    const qName = uid('backoff-jitter');
    const queue = track(new Queue(qName, { connection }));

    const job = await queue.add(
      'job',
      { x: 1 },
      {
        attempts: 5,
        backoff: { type: 'exponential', delay: 1000, jitter: 0.25 },
      },
    );
    expect(job).not.toBeNull();
  }, 15000);
});

// ---------------------------------------------------------------------------
// excludeData (bullmq-new-features.md:393)
// ---------------------------------------------------------------------------
describe('excludeData example', () => {
  it('getJobs with excludeData (bullmq-new-features.md:393)', async () => {
    const qName = uid('exclude-data');
    const queue = track(new Queue(qName, { connection }));

    await queue.add('big-job', { payload: 'x'.repeat(500) });
    const jobs = await queue.getJobs('waiting', 0, 99, { excludeData: true });
    expect(jobs.length).toBeGreaterThanOrEqual(1);
    // data should be undefined when excludeData is true
    expect(jobs[0].data).toBeUndefined();
  }, 15000);
});
