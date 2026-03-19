/**
 * Tests for code examples from the Bee-Queue migration skills:
 *   - skills/glide-mq-migrate-bee/SKILL.md
 *   - skills/glide-mq-migrate-bee/references/api-mapping.md
 *   - skills/glide-mq-migrate-bee/references/new-features.md
 *
 * Only the "AFTER" (glide-mq) code from each migration example is tested.
 */
import { describe, it, expect, afterEach, beforeAll, afterAll } from 'vitest';

const { Queue } = require('../../dist/queue') as typeof import('../../src/queue');
const { Worker } = require('../../dist/worker') as typeof import('../../src/worker');
const { FlowProducer } = require('../../dist/flow-producer') as typeof import('../../src/flow-producer');
const { QueueEvents } = require('../../dist/queue-events') as typeof import('../../src/queue-events');
const { Producer } = require('../../dist/producer') as typeof import('../../src/producer');
const { Broadcast } = require('../../dist/broadcast') as typeof import('../../src/broadcast');
const { BroadcastWorker } = require('../../dist/broadcast-worker') as typeof import('../../src/broadcast-worker');
const {
  UnrecoverableError,
} = require('../../dist/errors') as typeof import('../../src/errors');
const { gracefulShutdown } = require('../../dist/graceful-shutdown') as typeof import('../../src/graceful-shutdown');

import { flushQueue, createCleanupClient, STANDALONE, waitFor } from '../helpers/fixture';

const CONNECTION = STANDALONE;
const connection = CONNECTION;

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

beforeAll(async () => {
  cleanupClient = await createCleanupClient(CONNECTION);
});

afterAll(async () => {
  cleanupClient?.close();
});

afterEach(async () => {
  for (const r of resourcesToClose.reverse()) {
    try {
      await r.close();
    } catch {}
  }
  resourcesToClose = [];
  for (const name of queueNames) {
    try {
      await flushQueue(cleanupClient, name);
    } catch {}
  }
  queueNames = [];
});

// ---------------------------------------------------------------------------
// SKILL.md - Step-by-step conversion (AFTER examples)
// ---------------------------------------------------------------------------
describe('Bee SKILL.md - connection and basic usage', () => {
  it('Step 1: Connection (SKILL.md:129)', async () => {
    const qName = uid('bee-conn');
    const conn = { addresses: [{ host: 'localhost', port: 6379 }] };
    const queue = track(new Queue(qName, { connection: conn }));
    const job = await queue.add('test', { x: 1 });
    expect(job).not.toBeNull();
  }, 15000);

  it('Step 2: Job creation with options (SKILL.md:148)', async () => {
    const qName = uid('bee-jobcreate');
    const queue = track(new Queue(qName, { connection }));

    const job = await queue.add(
      'send-email',
      { email: 'user@example.com' },
      {
        attempts: 3,
        backoff: { type: 'exponential', delay: 1000 },
        delay: 60000,
        jobId: 'unique-123',
      },
    );
    // May return null if jobId already exists from a previous run
    // But the API call should not throw
  }, 15000);

  it('Step 3: Worker (SKILL.md:169)', async () => {
    const qName = uid('bee-worker');
    const queue = track(new Queue(qName, { connection }));
    const processed: any[] = [];

    const worker = track(
      new Worker(
        qName,
        async (job) => {
          processed.push({ processed: true });
          return { processed: true };
        },
        { connection, concurrency: 10 },
      ),
    );

    worker.on('completed', (job) => {
      // Verify returnValue is accessible
      expect(job).toBeTruthy();
    });

    await queue.add('task', { x: 1 });
    await waitFor(() => processed.length > 0);
    expect(processed[0]).toEqual({ processed: true });
  }, 15000);

  it('Step 4: Batch save -> addBulk (SKILL.md:183)', async () => {
    const qName = uid('bee-bulk');
    const queue = track(new Queue(qName, { connection }));
    const items = [{ x: 1 }, { x: 2 }, { x: 3 }];

    const results = await queue.addBulk(
      items.map((item) => ({ name: 'process', data: item })),
    );
    expect(results.length).toBe(3);
  }, 15000);

  it('Step 5: Producer-only (SKILL.md:199)', async () => {
    const qName = uid('bee-producer');
    const producer = track(new Producer(qName, { connection }));
    const id = await producer.add('job-name', { key: 'value' });
    expect(typeof id).toBe('string');
  }, 15000);

  it('Step 6: Progress reporting (SKILL.md:215)', async () => {
    const qName = uid('bee-progress');
    const queue = track(new Queue(qName, { connection }));
    let done = false;

    const worker = track(
      new Worker(
        qName,
        async (job) => {
          await job.updateProgress(50);
          await job.updateProgress({ page: 3, total: 10 });
          await job.log('halfway done');
          done = true;
          return 'ok';
        },
        { connection },
      ),
    );

    await queue.add('progress-job', { x: 1 });
    await waitFor(() => done);
  }, 15000);

  it('Step 7: Stall detection on Worker (SKILL.md:231)', async () => {
    const qName = uid('bee-stall');
    const queue = track(new Queue(qName, { connection }));

    const worker = track(
      new Worker(qName, async () => 'ok', {
        connection,
        lockDuration: 30000,
        stalledInterval: 30000,
        maxStalledCount: 2,
      }),
    );

    await queue.add('stall-test', {});
    await waitFor(async () => {
      const c = await queue.getJobCounts();
      return c.completed > 0;
    });
  }, 15000);

  it('Step 8: Health check -> getJobCounts (SKILL.md:248)', async () => {
    const qName = uid('bee-health');
    const queue = track(new Queue(qName, { connection }));

    await queue.add('health-job', {});
    const counts = await queue.getJobCounts();
    expect(counts).toHaveProperty('waiting');
    expect(counts).toHaveProperty('active');
    expect(counts).toHaveProperty('completed');
    expect(counts).toHaveProperty('failed');
    expect(counts).toHaveProperty('delayed');
  }, 15000);
});

// ---------------------------------------------------------------------------
// references/api-mapping.md (AFTER examples)
// ---------------------------------------------------------------------------
describe('Bee api-mapping.md', () => {
  it('Constructor - Queue + Worker + QueueEvents split (api-mapping.md:24)', async () => {
    const qName = uid('bee-am-ctor');
    const conn = { addresses: [{ host: 'localhost', port: 6379 }] };

    const queue = track(new Queue(qName, { connection: conn, prefix: 'glide' }));
    const processed: any[] = [];

    const worker = track(
      new Worker(
        qName,
        async (job) => {
          processed.push(job.data);
          return 'ok';
        },
        {
          connection: conn,
          lockDuration: 30000,
          stalledInterval: 30000,
        },
      ),
    );

    const events = track(new QueueEvents(qName, { connection: conn }));

    await queue.add('test', { x: 1 });
    await waitFor(() => processed.length > 0);
    expect(processed[0]).toEqual({ x: 1 });
  }, 15000);

  it('createJob + save -> add (api-mapping.md:46)', async () => {
    const qName = uid('bee-am-add');
    const queue = track(new Queue(qName, { connection }));

    const job = await queue.add('compute', { x: 1 });
    expect(job.id).toBeTruthy();
  }, 15000);

  it('setId -> jobId option (api-mapping.md:58)', async () => {
    const qName = uid('bee-am-setid');
    const queue = track(new Queue(qName, { connection }));

    const job = await queue.add('task', { x: 1 }, { jobId: 'unique-key' });
    expect(job).not.toBeNull();
    expect(job!.id).toBe('unique-key');
  }, 15000);

  it('retries -> attempts (api-mapping.md:70)', async () => {
    const qName = uid('bee-am-retries');
    const queue = track(new Queue(qName, { connection }));

    const job = await queue.add('task', { x: 1 }, { attempts: 3 });
    expect(job).not.toBeNull();
  }, 15000);

  it('backoff options - fixed and exponential (api-mapping.md:80)', async () => {
    const qName = uid('bee-am-backoff');
    const queue = track(new Queue(qName, { connection }));

    // Fixed backoff
    const j1 = await queue.add('task', { x: 1 }, {
      attempts: 3,
      backoff: { type: 'fixed', delay: 0 },
    });
    expect(j1).not.toBeNull();

    // Fixed with delay
    const j2 = await queue.add('task2', { x: 2 }, {
      attempts: 3,
      backoff: { type: 'fixed', delay: 1000 },
    });
    expect(j2).not.toBeNull();

    // Exponential
    const j3 = await queue.add('task3', { x: 3 }, {
      attempts: 3,
      backoff: { type: 'exponential', delay: 1000 },
    });
    expect(j3).not.toBeNull();
  }, 15000);

  it('delayUntil -> delay (api-mapping.md:112)', async () => {
    const qName = uid('bee-am-delay');
    const queue = track(new Queue(qName, { connection }));

    const job = await queue.add('task', { x: 1 }, { delay: 60000 });
    expect(job).not.toBeNull();
  }, 15000);

  it('timeout option (api-mapping.md:122)', async () => {
    const qName = uid('bee-am-timeout');
    const queue = track(new Queue(qName, { connection }));

    const job = await queue.add('task', { x: 1 }, { timeout: 30000 });
    expect(job).not.toBeNull();
  }, 15000);

  it('Full chained builder conversion (api-mapping.md:141)', async () => {
    const qName = uid('bee-am-full');
    const queue = track(new Queue(qName, { connection }));

    const job = await queue.add(
      'send-email',
      { email: 'user@example.com' },
      {
        jobId: `email-${Date.now()}`,
        attempts: 3,
        backoff: { type: 'exponential', delay: 1000 },
        delay: 60000,
        timeout: 30000,
      },
    );
    expect(job).not.toBeNull();
  }, 15000);

  it('process -> Worker (api-mapping.md:164)', async () => {
    const qName = uid('bee-am-process');
    const queue = track(new Queue(qName, { connection }));
    const results: any[] = [];

    const worker = track(
      new Worker(
        qName,
        async (job) => {
          results.push({ result: job.data.x * 2 });
          return { result: job.data.x * 2 };
        },
        { connection },
      ),
    );

    await queue.add('compute', { x: 5 });
    await waitFor(() => results.length > 0);
    expect(results[0]).toEqual({ result: 10 });
  }, 15000);

  it('process with concurrency (api-mapping.md:174)', async () => {
    const qName = uid('bee-am-process-conc');
    const queue = track(new Queue(qName, { connection }));
    const results: any[] = [];

    const worker = track(
      new Worker(
        qName,
        async (job) => {
          results.push(job.data);
          return job.data;
        },
        { connection, concurrency: 10 },
      ),
    );

    await queue.add('task', { x: 1 });
    await waitFor(() => results.length > 0);
  }, 15000);

  it('reportProgress -> updateProgress (api-mapping.md:201)', async () => {
    const qName = uid('bee-am-progress');
    const queue = track(new Queue(qName, { connection }));
    let done = false;

    const worker = track(
      new Worker(
        qName,
        async (job) => {
          await job.updateProgress(30);
          await job.updateProgress({ page: 3, total: 10 });
          await job.log('Processing page 3 of 10');
          await job.updateProgress(50);
          done = true;
          return 'ok';
        },
        { connection },
      ),
    );

    await queue.add('progress', { x: 1 });
    await waitFor(() => done);
  }, 15000);

  it('saveAll -> addBulk (api-mapping.md:225)', async () => {
    const qName = uid('bee-am-bulk');
    const queue = track(new Queue(qName, { connection }));

    const results = await queue.addBulk([
      { name: 'compute', data: { x: 1 } },
      { name: 'compute', data: { x: 2 } },
      { name: 'compute', data: { x: 3 } },
    ]);
    expect(results.length).toBe(3);
  }, 15000);

  it('getJob by ID (api-mapping.md:238)', async () => {
    const qName = uid('bee-am-getjob');
    const queue = track(new Queue(qName, { connection }));

    const job = await queue.add('test', { x: 1 });
    const fetched = await queue.getJob(job.id);
    expect(fetched).not.toBeNull();
    expect(fetched!.name).toBe('test');
  }, 15000);

  it('getJobs by type with range (api-mapping.md:248)', async () => {
    const qName = uid('bee-am-getjobs');
    const queue = track(new Queue(qName, { connection }));

    await queue.add('j1', {});
    await queue.add('j2', {});

    const waiting = await queue.getJobs('waiting', 0, 25);
    expect(waiting.length).toBeGreaterThanOrEqual(2);
  }, 15000);

  it('removeJob via Job instance (api-mapping.md:259)', async () => {
    const qName = uid('bee-am-remove');
    const queue = track(new Queue(qName, { connection }));

    const job = await queue.add('removable', { x: 1 });
    const fetched = await queue.getJob(job.id);
    await fetched!.remove();

    const gone = await queue.getJob(job.id);
    expect(gone).toBeNull();
  }, 15000);

  it('checkHealth -> getJobCounts (api-mapping.md:275)', async () => {
    const qName = uid('bee-am-health');
    const queue = track(new Queue(qName, { connection }));

    await queue.add('health', {});
    const counts = await queue.getJobCounts();
    expect(counts).toHaveProperty('waiting');
    expect(counts).toHaveProperty('active');
    expect(counts).toHaveProperty('completed');
    expect(counts).toHaveProperty('failed');
    expect(counts).toHaveProperty('delayed');
  }, 15000);

  it('destroy -> obliterate (api-mapping.md:303)', async () => {
    const qName = uid('bee-am-obliterate');
    const queue = track(new Queue(qName, { connection }));

    await queue.add('doomed', {});
    await queue.obliterate({ force: true });

    const counts = await queue.getJobCounts();
    expect(counts.waiting).toBe(0);
  }, 15000);

  it('close components individually (api-mapping.md:289)', async () => {
    const qName = uid('bee-am-close');
    const queue = new Queue(qName, { connection });
    const worker = new Worker(qName, async () => 'ok', { connection });
    const events = new QueueEvents(qName, { connection });

    await worker.close();
    await queue.close();
    await events.close();
  }, 15000);

  it('gracefulShutdown (api-mapping.md:294)', async () => {
    const qName = uid('bee-am-graceful');
    const queue = new Queue(qName, { connection });

    // Add a job first to create the stream so the worker doesn't error
    await queue.add('init', {});

    const worker = new Worker(qName, async () => 'ok', { connection });

    // Wait briefly for worker to process the job
    await waitFor(async () => {
      const c = await queue.getJobCounts();
      return c.completed > 0;
    }, 5000);

    // gracefulShutdown registers signal handlers and returns a handle.
    // Call .shutdown() to programmatically trigger the shutdown.
    const handle = gracefulShutdown([worker, queue]);
    await handle.shutdown();
  }, 15000);

  it('Stall detection on Worker (api-mapping.md:337)', async () => {
    const qName = uid('bee-am-stall');
    const queue = track(new Queue(qName, { connection }));
    const stalledEvents: string[] = [];

    const worker = track(
      new Worker(qName, async () => 'ok', {
        connection,
        lockDuration: 30000,
        stalledInterval: 30000,
        maxStalledCount: 2,
      }),
    );

    worker.on('stalled', (jobId) => stalledEvents.push(jobId));

    await queue.add('stall-check', {});
    await waitFor(async () => {
      const c = await queue.getJobCounts();
      return c.completed > 0;
    });
  }, 15000);

  it('Worker events - completed, failed, error (api-mapping.md:362)', async () => {
    const qName = uid('bee-am-events');
    const queue = track(new Queue(qName, { connection }));
    const events: string[] = [];

    const worker = track(
      new Worker(
        qName,
        async () => {
          return 'ok';
        },
        { connection },
      ),
    );

    worker.on('completed', () => events.push('completed'));
    worker.on('failed', () => events.push('failed'));
    worker.on('error', () => events.push('error'));

    await queue.add('event-test', { x: 1 });
    await waitFor(() => events.includes('completed'));
    expect(events).toContain('completed');
  }, 15000);

  it('QueueEvents - completed, failed, progress (api-mapping.md:378)', async () => {
    const qName = uid('bee-am-qevents');
    const queue = track(new Queue(qName, { connection }));
    const events = track(new QueueEvents(qName, { connection }));

    const completedIds: string[] = [];
    events.on('completed', ({ jobId }: any) => completedIds.push(jobId));

    const worker = track(
      new Worker(qName, async () => 'ok', { connection }),
    );

    const job = await queue.add('qe-test', { x: 1 });
    await waitFor(() => completedIds.length > 0, 5000);
    expect(completedIds).toContain(job.id);
  }, 15000);

  it('addAndWait for request-reply (api-mapping.md:401)', async () => {
    const qName = uid('bee-am-addwait');
    const queue = track(new Queue(qName, { connection }));

    const worker = track(
      new Worker(
        qName,
        async (job) => {
          return { result: 'done' };
        },
        { connection },
      ),
    );

    const result = await queue.addAndWait('task', { x: 1 }, { waitTimeout: 10_000 });
    expect(result).toEqual({ result: 'done' });
  }, 15000);

  it('Custom backoff strategies (api-mapping.md:414)', async () => {
    const qName = uid('bee-am-custom-backoff');
    const queue = track(new Queue(qName, { connection }));

    const worker = track(
      new Worker(qName, async () => { throw new Error('fail'); }, {
        connection,
        backoffStrategies: {
          linear: (attemptsMade) => attemptsMade * 1000,
        },
      }),
    );

    const job = await queue.add('backoff', { x: 1 }, {
      attempts: 2,
      backoff: { type: 'linear', delay: 100 },
    });
    expect(job).not.toBeNull();

    // Wait for at least one failure
    await waitFor(async () => {
      const c = await queue.getJobCounts();
      return c.failed > 0;
    }, 10000);
  }, 15000);
});

// ---------------------------------------------------------------------------
// references/new-features.md (AFTER examples - features Bee-Queue doesn't have)
// ---------------------------------------------------------------------------
describe('Bee new-features.md', () => {
  it('Priority queues (new-features.md:10)', async () => {
    const qName = uid('bee-nf-prio');
    const queue = track(new Queue(qName, { connection }));

    const j1 = await queue.add('urgent-alert', { x: 1 }, { priority: 0 });
    const j2 = await queue.add('report', { x: 2 }, { priority: 5 });
    const j3 = await queue.add('cleanup', { x: 3 }, { priority: 20 });

    expect(j1).not.toBeNull();
    expect(j2).not.toBeNull();
    expect(j3).not.toBeNull();
  }, 15000);

  it('FlowProducer workflows (new-features.md:27)', async () => {
    const reportQ = uid('bee-nf-flow-report');
    const dataQ = uid('bee-nf-flow-data');
    const flow = track(new FlowProducer({ connection }));

    const { job: parent } = await flow.add({
      name: 'assemble-report',
      queueName: reportQ,
      data: { reportId: 42 },
      children: [
        { name: 'fetch-users', queueName: dataQ, data: { source: 'users' } },
        { name: 'fetch-orders', queueName: dataQ, data: { source: 'orders' } },
        { name: 'fetch-metrics', queueName: dataQ, data: { source: 'metrics' } },
      ],
    });

    expect(parent.name).toBe('assemble-report');
  }, 15000);

  it('Broadcast fan-out (new-features.md:47)', async () => {
    const bName = uid('bee-nf-bcast');
    const broadcast = track(new Broadcast(bName, { connection, maxMessages: 1000 }));
    const received: any[] = [];

    const inventory = track(
      new BroadcastWorker(
        bName,
        async (job) => {
          received.push({ service: 'inventory', data: job.data });
        },
        { connection, subscription: 'inventory-service' },
      ),
    );

    await new Promise((r) => setTimeout(r, 500));
    // publish(subject, data, opts?) - actual API signature
    await broadcast.publish('order.placed', { event: 'order.placed', orderId: 42 });

    await waitFor(() => received.length > 0, 5000);
    expect(received[0].service).toBe('inventory');
  }, 15000);

  it('Batch processing (new-features.md:68)', async () => {
    const qName = uid('bee-nf-batch');
    const queue = track(new Queue(qName, { connection }));
    const batchSizes: number[] = [];

    const worker = track(
      new Worker(
        qName,
        async (jobs: any[]) => {
          batchSizes.push(jobs.length);
          return jobs.map(() => ({ ok: true }));
        },
        { connection, batch: { size: 50, timeout: 2000 } },
      ),
    );

    await queue.addBulk([
      { name: 'item', data: { i: 1 } },
      { name: 'item', data: { i: 2 } },
    ]);

    await waitFor(() => batchSizes.length > 0);
    expect(batchSizes[0]).toBeGreaterThan(0);
  }, 15000);

  it('Deduplication (new-features.md:84)', async () => {
    const qName = uid('bee-nf-dedup');
    const queue = track(new Queue(qName, { connection }));

    const j1 = await queue.add('task', { x: 1 }, {
      deduplication: { id: 'unique-key' },
    });
    expect(j1).not.toBeNull();

    const j2 = await queue.add('task', { x: 2 }, {
      deduplication: { id: 'unique-key' },
    });
    expect(j2).toBeNull();
  }, 15000);

  it('Schedulers - cron and interval (new-features.md:101)', async () => {
    const qName = uid('bee-nf-sched');
    const queue = track(new Queue(qName, { connection }));

    // Cron
    await queue.upsertJobScheduler(
      'daily-report',
      { pattern: '0 0 * * *' },
      { name: 'daily-report', data: {} },
    );

    // Interval
    await queue.upsertJobScheduler(
      'health-check',
      { every: 300000 },
      { name: 'health-check', data: {} },
    );

    const schedulers = await queue.getRepeatableJobs();
    expect(schedulers.length).toBeGreaterThanOrEqual(2);

    await queue.removeJobScheduler('daily-report');
    await queue.removeJobScheduler('health-check');
  }, 15000);

  it('Rate limiting on worker (new-features.md:121)', async () => {
    const qName = uid('bee-nf-ratelimit');
    const queue = track(new Queue(qName, { connection }));

    const worker = track(
      new Worker(qName, async () => 'ok', {
        connection,
        limiter: { max: 100, duration: 60000 },
      }),
    );

    await queue.add('rate-test', {});
    await waitFor(async () => {
      const c = await queue.getJobCounts();
      return c.completed > 0;
    });
  }, 15000);

  it('Dead letter queue (new-features.md:134)', async () => {
    const qName = uid('bee-nf-dlq');
    const dlqName = uid('bee-nf-dlq-target');
    const queue = track(new Queue(qName, { connection }));

    const worker = track(
      new Worker(
        qName,
        async () => {
          throw new Error('fail');
        },
        {
          connection,
          deadLetterQueue: { name: dlqName },
        },
      ),
    );

    await queue.add('dlq-job', {}, { attempts: 1 });

    const dlqQueue = track(new Queue(dlqName, { connection }));
    await waitFor(async () => {
      const c = await dlqQueue.getJobCounts();
      return c.waiting > 0;
    }, 10000);
  }, 15000);

  it('LIFO mode (new-features.md:146)', async () => {
    const qName = uid('bee-nf-lifo');
    const queue = track(new Queue(qName, { connection }));

    const job = await queue.add('urgent-report', { x: 1 }, { lifo: true });
    expect(job).not.toBeNull();
  }, 15000);

  it('Job TTL (new-features.md:152)', async () => {
    const qName = uid('bee-nf-ttl');
    const queue = track(new Queue(qName, { connection }));

    const job = await queue.add('time-sensitive', { x: 1 }, { ttl: 300000 });
    expect(job).not.toBeNull();
  }, 15000);

  it('Per-key ordering (new-features.md:161)', async () => {
    const qName = uid('bee-nf-ordering');
    const queue = track(new Queue(qName, { connection }));

    const j1 = await queue.add('process-order', { x: 1 }, {
      ordering: { key: 'customer-123' },
    });
    const j2 = await queue.add('process-order', { x: 2 }, {
      ordering: { key: 'customer-456' },
    });
    expect(j1).not.toBeNull();
    expect(j2).not.toBeNull();
  }, 15000);

  it('Request-reply addAndWait (new-features.md:172)', async () => {
    const qName = uid('bee-nf-addwait');
    const queue = track(new Queue(qName, { connection }));

    const worker = track(
      new Worker(
        qName,
        async (job) => {
          return { reply: job.data.prompt };
        },
        { connection },
      ),
    );

    const result = await queue.addAndWait(
      'inference',
      { prompt: 'Hello' },
      { waitTimeout: 10_000 },
    );
    expect(result).toEqual({ reply: 'Hello' });
  }, 15000);

  it('UnrecoverableError (new-features.md:202)', async () => {
    const qName = uid('bee-nf-unrecoverable');
    const queue = track(new Queue(qName, { connection }));
    const failedReasons: string[] = [];

    const worker = track(
      new Worker(
        qName,
        async (job) => {
          if (!job.data.requiredField) {
            throw new UnrecoverableError('missing required field');
          }
          return 'ok';
        },
        { connection },
      ),
    );

    worker.on('failed', (_job, err) => failedReasons.push(err.message));

    await queue.add('validate', {}, { attempts: 5 });
    await waitFor(() => failedReasons.length > 0);
    expect(failedReasons[0]).toBe('missing required field');
  }, 15000);

  it('Serverless Producer (new-features.md:216)', async () => {
    const qName = uid('bee-nf-serverless');
    const producer = track(new Producer(qName, { connection }));
    const id = await producer.add('process', { body: 'hello' });
    expect(typeof id).toBe('string');
  }, 15000);

  it('Testing without Valkey (new-features.md:231)', async () => {
    const { TestQueue, TestWorker } = require('../../dist/testing');

    const queue = new TestQueue('tasks');
    const processed: any[] = [];

    const worker = new TestWorker(queue, async (job: any) => {
      processed.push({ processed: true });
      return { processed: true };
    });

    await queue.add('test-job', { key: 'value' });
    await waitFor(() => processed.length > 0, 2000, 50);

    await worker.close();
    await queue.close();
  }, 15000);

  it('QueueEvents for real-time events (new-features.md:285)', async () => {
    const qName = uid('bee-nf-qevents');
    const queue = track(new Queue(qName, { connection }));
    const events = track(new QueueEvents(qName, { connection }));

    const addedIds: string[] = [];
    events.on('added', ({ jobId }: any) => addedIds.push(jobId));

    const completedIds: string[] = [];
    events.on('completed', ({ jobId }: any) => completedIds.push(jobId));

    const worker = track(
      new Worker(qName, async () => 'ok', { connection }),
    );

    const job = await queue.add('qe-test', {});
    await waitFor(() => completedIds.length > 0, 5000);
    expect(completedIds).toContain(job.id);
  }, 15000);

  it('Time-series metrics (new-features.md:300)', async () => {
    const qName = uid('bee-nf-metrics');
    const queue = track(new Queue(qName, { connection }));

    const worker = track(
      new Worker(qName, async () => 'ok', { connection }),
    );

    await queue.add('metric', {});
    await waitFor(async () => {
      const c = await queue.getJobCounts();
      return c.completed > 0;
    });

    const metrics = await queue.getMetrics('completed');
    expect(metrics).toHaveProperty('count');
  }, 15000);

  it('Queue management - pause, resume, drain, clean, obliterate (new-features.md:307)', async () => {
    const qName = uid('bee-nf-mgmt');
    const queue = track(new Queue(qName, { connection }));

    await queue.add('j1', {});
    await queue.add('j2', {});

    await queue.pause();
    await queue.resume();
    await queue.drain();

    const counts = await queue.getJobCounts();
    expect(counts.waiting).toBe(0);

    await queue.obliterate({ force: true });
  }, 15000);
});
