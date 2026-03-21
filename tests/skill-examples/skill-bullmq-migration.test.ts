/**
 * Tests for code examples from the BullMQ migration skills:
 *   - skills/glide-mq-migrate-bullmq/SKILL.md
 *   - skills/glide-mq-migrate-bullmq/references/connection-mapping.md
 *   - skills/glide-mq-migrate-bullmq/references/new-features.md
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
const { UnrecoverableError } = require('../../dist/errors') as typeof import('../../src/errors');
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
  await Promise.allSettled(resourcesToClose.reverse().map((r) => r.close()));
  resourcesToClose = [];
  for (const name of queueNames) {
    try {
      await flushQueue(cleanupClient, name);
    } catch {}
  }
  queueNames = [];
});

// ---------------------------------------------------------------------------
// SKILL.md - Step-by-step conversion (AFTER examples only)
// ---------------------------------------------------------------------------
describe('BullMQ SKILL.md - connection conversion', () => {
  it('Step 1: Connection config (SKILL.md:89)', async () => {
    const qName = uid('bmq-conn');
    const conn = { addresses: [{ host: 'localhost', port: 6379 }] };
    const queue = track(new Queue(qName, { connection: conn }));
    const job = await queue.add('test', { x: 1 });
    expect(job).not.toBeNull();
  }, 15000);

  it('Step 2: Queue.add identical API (SKILL.md:105)', async () => {
    const qName = uid('bmq-add');
    const queue = track(new Queue(qName, { connection }));
    await queue.add('send-email', { to: 'user@example.com' });
    const counts = await queue.getJobCounts();
    expect(counts.waiting).toBeGreaterThanOrEqual(1);
  }, 15000);

  it('Step 3: Worker identical API (SKILL.md:120)', async () => {
    const qName = uid('bmq-worker');
    const queue = track(new Queue(qName, { connection }));
    const processed: string[] = [];

    const worker = track(
      new Worker(
        qName,
        async (job) => {
          processed.push(job.data.to);
        },
        { connection: { addresses: [{ host: 'localhost', port: 6379 }] }, concurrency: 10 },
      ),
    );

    await queue.add('email', { to: 'user@test.com' });
    await waitFor(() => processed.length > 0);
    expect(processed[0]).toBe('user@test.com');
  }, 15000);

  it('Step 4: FlowProducer identical API (SKILL.md:130)', async () => {
    const parentQ = uid('bmq-flow-parent');
    const childQ = uid('bmq-flow-child');
    const flow = track(new FlowProducer({ connection }));

    const { job: parent } = await flow.add({
      name: 'parent',
      queueName: parentQ,
      data: { step: 'final' },
      children: [
        { name: 'child-1', queueName: childQ, data: { step: '1' } },
        { name: 'child-2', queueName: childQ, data: { step: '2' } },
      ],
    });

    expect(parent.name).toBe('parent');
  }, 15000);

  it('Step 5: QueueEvents identical API (SKILL.md:144)', async () => {
    const qName = uid('bmq-qe');
    const queue = track(new Queue(qName, { connection }));
    const qe = track(new QueueEvents(qName, { connection }));

    const completedIds: string[] = [];
    qe.on('completed', ({ jobId }: any) => completedIds.push(jobId));

    const worker = track(new Worker(qName, async () => 'done', { connection }));

    const job = await queue.add('test', { x: 1 });
    await waitFor(() => completedIds.length > 0, 5000);
    expect(completedIds).toContain(job.id);
  }, 15000);

  it('Step 6: Graceful shutdown (SKILL.md:162)', async () => {
    const qName = uid('bmq-shutdown');
    const queue = track(new Queue(qName, { connection }));
    const worker = track(new Worker(qName, async () => 'ok', { connection }));

    // Verify close works without errors
    await worker.close();
    await queue.close();

    // Remove from tracked since we already closed
    resourcesToClose = [];
  }, 15000);

  it('Step 7: UnrecoverableError (SKILL.md:170)', async () => {
    const qName = uid('bmq-unrecoverable');
    const queue = track(new Queue(qName, { connection }));
    const failedReasons: string[] = [];

    const worker = track(
      new Worker(
        qName,
        async () => {
          throw new UnrecoverableError('permanent failure');
        },
        { connection },
      ),
    );

    worker.on('failed', (_job, err) => failedReasons.push(err.message));

    await queue.add('perm-fail', {}, { attempts: 5 });
    await waitFor(() => failedReasons.length > 0);
    expect(failedReasons[0]).toBe('permanent failure');
  }, 15000);

  it('Step 8: Scheduling with upsertJobScheduler (SKILL.md:186)', async () => {
    const qName = uid('bmq-scheduler');
    const queue = track(new Queue(qName, { connection }));

    await queue.upsertJobScheduler(
      'report',
      { pattern: '0 9 * * *', tz: 'America/New_York' },
      { name: 'report', data: { v: 1 } },
    );

    const schedulers = await queue.getRepeatableJobs();
    expect(schedulers.length).toBeGreaterThanOrEqual(1);

    await queue.removeJobScheduler('report');
  }, 15000);

  it('Step 9: Custom backoff strategies (SKILL.md:210)', async () => {
    const qName = uid('bmq-backoff');
    const queue = track(new Queue(qName, { connection }));
    const failedAttempts: number[] = [];

    const worker = track(
      new Worker(
        qName,
        async () => {
          throw new Error('retry me');
        },
        {
          connection,
          backoffStrategies: {
            jitter: (attemptsMade) => 100 + Math.random() * 100,
            linear: (attemptsMade) => 100 * attemptsMade,
          },
        },
      ),
    );

    worker.on('failed', (job) => {
      if (job) failedAttempts.push(job.attemptsMade);
    });

    await queue.add(
      'backoff-test',
      {},
      {
        attempts: 3,
        backoff: { type: 'linear', delay: 100 },
      },
    );

    await waitFor(() => failedAttempts.length >= 3, 10000);
  }, 15000);

  it('Step 10: defaultJobOptions removal - wrapper pattern (SKILL.md:231)', async () => {
    const qName = uid('bmq-defaults');
    const queue = track(new Queue(qName, { connection }));

    const DEFAULTS = { attempts: 3, backoff: { type: 'exponential' as const, delay: 1000 } };
    const add = (name: string, data: unknown, opts?: any) => queue.add(name, data, { ...DEFAULTS, ...opts });

    const job = await add('test', { x: 1 });
    expect(job).not.toBeNull();
    expect(job!.opts?.attempts).toBe(3);
  }, 15000);

  it('Step 11: getJobs with multiple types (SKILL.md:245)', async () => {
    const qName = uid('bmq-getjobs');
    const queue = track(new Queue(qName, { connection }));

    await queue.add('job1', {});
    await queue.add('job2', {}, { delay: 60000 });

    const [waiting, delayed] = await Promise.all([queue.getJobs('waiting', 0, 99), queue.getJobs('delayed', 0, 99)]);
    const jobs = [...waiting, ...delayed];
    expect(jobs.length).toBeGreaterThanOrEqual(2);
  }, 15000);

  it('Step 13: BullMQ Pro groups to ordering keys (SKILL.md:277)', async () => {
    const qName = uid('bmq-ordering');
    const queue = track(new Queue(qName, { connection }));

    const job = await queue.add(
      'job',
      { x: 1 },
      {
        ordering: { key: 'tenant-123', concurrency: 2 },
      },
    );
    expect(job).not.toBeNull();
  }, 15000);
});

// ---------------------------------------------------------------------------
// references/connection-mapping.md (AFTER examples only)
// ---------------------------------------------------------------------------
describe('BullMQ connection-mapping.md', () => {
  it('Basic standalone connection (connection-mapping.md:15)', async () => {
    const qName = uid('bmq-cm-basic');
    const conn = { addresses: [{ host: 'localhost', port: 6379 }] };
    const queue = track(new Queue(qName, { connection: conn }));
    const job = await queue.add('test', {});
    expect(job).not.toBeNull();
  }, 15000);

  // TLS, password, cluster, IAM, AZ-affinity examples are config-only (cannot test
  // against localhost:6379 without those features). Skipped.
});

// ---------------------------------------------------------------------------
// references/new-features.md (glide-mq only features)
// ---------------------------------------------------------------------------
describe('BullMQ new-features.md', () => {
  it('Per-key ordering (new-features.md:12)', async () => {
    const qName = uid('bmq-nf-ordering');
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

  it('Group concurrency (new-features.md:22)', async () => {
    const qName = uid('bmq-nf-grp-conc');
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

  it('Per-group rate limiting (new-features.md:34)', async () => {
    const qName = uid('bmq-nf-grp-rate');
    const queue = track(new Queue(qName, { connection }));

    const job = await queue.add(
      'sync',
      { x: 1 },
      {
        ordering: {
          key: 'tenant-123',
          concurrency: 3,
          rateLimit: { max: 10, duration: 60_000 },
        },
      },
    );
    expect(job).not.toBeNull();
  }, 15000);

  it('Cost-based token bucket (new-features.md:51)', async () => {
    const qName = uid('bmq-nf-token');
    const queue = track(new Queue(qName, { connection }));

    const job = await queue.add(
      'heavy-job',
      { x: 1 },
      {
        ordering: {
          key: 'tenant-123',
          tokenBucket: { capacity: 100, refillRate: 10 },
        },
        cost: 25,
      },
    );
    expect(job).not.toBeNull();
  }, 15000);

  it('Global rate limiting (new-features.md:67)', async () => {
    const qName = uid('bmq-nf-global-rate');
    const queue = track(new Queue(qName, { connection }));

    await queue.setGlobalRateLimit({ max: 500, duration: 60_000 });
    const limit = await queue.getGlobalRateLimit();
    expect(limit).not.toBeNull();
    expect(limit!.max).toBe(500);

    await queue.removeGlobalRateLimit();
  }, 15000);

  it('Dead letter queue (new-features.md:82)', async () => {
    const qName = uid('bmq-nf-dlq');
    const dlqName = uid('bmq-nf-dlq-target');
    const queue = track(
      new Queue(qName, {
        connection,
        deadLetterQueue: { name: dlqName, maxRetries: 3 },
      }),
    );

    const job = await queue.add('test', {});
    expect(job).not.toBeNull();
  }, 15000);

  it('Job revocation (new-features.md:104)', async () => {
    const qName = uid('bmq-nf-revoke');
    const queue = track(new Queue(qName, { connection }));

    const job = await queue.add('task', { x: 1 });
    const result = await queue.revoke(job.id);
    expect(['revoked', 'flagged', 'not_found']).toContain(result);
  }, 15000);

  it('Transparent compression (new-features.md:126)', async () => {
    const qName = uid('bmq-nf-compress');
    const queue = track(new Queue(qName, { connection, compression: 'gzip' }));

    const job = await queue.add('compressed', { payload: 'x'.repeat(100) });
    expect(job).not.toBeNull();
  }, 15000);

  it('In-memory test mode (new-features.md:178)', async () => {
    const { TestQueue, TestWorker } = require('../../dist/testing');

    const queue = new TestQueue<{ email: string }, { sent: boolean }>('tasks');
    const _worker = new TestWorker(queue, async (job: any) => {
      return { sent: true };
    });

    await queue.add('send-email', { email: 'user@example.com' });
    await new Promise((r) => setTimeout(r, 50));

    const jobs = await queue.getJobs('completed');
    expect(jobs.length).toBeGreaterThanOrEqual(1);

    await worker.close();
    await queue.close();
  }, 15000);

  it('Broadcast / BroadcastWorker (new-features.md:200)', async () => {
    const bName = uid('bmq-nf-bcast');
    const broadcast = track(new Broadcast(bName, { connection }));
    const received: any[] = [];

    const bw = track(
      new BroadcastWorker(
        bName,
        async (message) => {
          received.push(message.data);
        },
        { connection, subscription: 'my-group' },
      ),
    );

    await bw.waitUntilReady();
    // publish(subject, data, opts?) - actual API signature
    await broadcast.publish('alert', { type: 'alert', text: 'Server restarting' });

    await waitFor(() => received.length > 0, 5000);
    expect(received[0]).toEqual({ type: 'alert', text: 'Server restarting' });
  }, 15000);

  it('Batch processing (new-features.md:217)', async () => {
    const qName = uid('bmq-nf-batch');
    const queue = track(new Queue(qName, { connection }));
    const batchSizes: number[] = [];

    const worker = track(
      new Worker(
        qName,
        async (jobs: any[]) => {
          batchSizes.push(jobs.length);
          return jobs.map((j: any) => ({ processed: j.data }));
        },
        { connection, batch: { size: 50, timeout: 2000 } },
      ),
    );

    await queue.addBulk(Array.from({ length: 3 }, (_, i) => ({ name: 'item', data: { i } })));

    await waitFor(() => batchSizes.length > 0);
    expect(batchSizes[0]).toBeGreaterThan(0);
  }, 15000);

  it('Workflow helpers - chain, group, chord (new-features.md:253)', async () => {
    const { chain, group, chord } = require('../../dist/workflows');
    const qName = uid('bmq-nf-workflow');
    const processed: string[] = [];

    const worker = track(
      new Worker(
        qName,
        async (job) => {
          processed.push(job.name);
          return {};
        },
        { connection },
      ),
    );

    // chain
    await chain(
      qName,
      [
        { name: 'step-1', data: {} },
        { name: 'step-2', data: {} },
        { name: 'step-3', data: {} },
      ],
      connection,
    );

    await waitFor(() => processed.length >= 3, 10000);
    expect(processed).toContain('step-1');
    expect(processed).toContain('step-2');
    expect(processed).toContain('step-3');
  }, 15000);

  it('addAndWait request-reply (new-features.md:311)', async () => {
    const qName = uid('bmq-nf-addwait');
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

  it('Job TTL (new-features.md:342)', async () => {
    const qName = uid('bmq-nf-ttl');
    const queue = track(new Queue(qName, { connection }));

    const job = await queue.add('ephemeral', { x: 1 }, { ttl: 60_000 });
    expect(job).not.toBeNull();
  }, 15000);

  it('repeatAfterComplete scheduler (new-features.md:354)', async () => {
    const qName = uid('bmq-nf-rac');
    const queue = track(new Queue(qName, { connection }));

    await queue.upsertJobScheduler('sequential-poll', { repeatAfterComplete: 5000 }, { name: 'poll', data: {} });

    const info = await queue.getJobScheduler('sequential-poll');
    expect(info).toBeTruthy();

    await queue.removeJobScheduler('sequential-poll');
  }, 15000);

  it('LIFO mode (new-features.md:368)', async () => {
    const qName = uid('bmq-nf-lifo');
    const queue = track(new Queue(qName, { connection }));

    const job = await queue.add('urgent', { x: 1 }, { lifo: true });
    expect(job).not.toBeNull();
  }, 15000);

  it('excludeData on getJobs (new-features.md:393)', async () => {
    const qName = uid('bmq-nf-exclude');
    const queue = track(new Queue(qName, { connection }));

    await queue.add('big-job', { payload: 'x'.repeat(200) });
    const jobs = await queue.getJobs('waiting', 0, 99, { excludeData: true });
    expect(jobs.length).toBeGreaterThanOrEqual(1);
    expect(jobs[0].data).toBeUndefined();
  }, 15000);

  it('globalConcurrency on WorkerOptions (new-features.md:405)', async () => {
    const qName = uid('bmq-nf-globalconc');
    const queue = track(new Queue(qName, { connection }));

    const worker = track(
      new Worker(qName, async () => 'ok', {
        connection,
        concurrency: 10,
        globalConcurrency: 50,
      }),
    );

    await queue.add('test', {});
    await waitFor(async () => {
      const c = await queue.getJobCounts();
      return c.completed > 0;
    });
  }, 15000);

  it('Deduplication modes (new-features.md:419)', async () => {
    const qName = uid('bmq-nf-dedup');
    const queue = track(new Queue(qName, { connection }));

    const job1 = await queue.add(
      'job',
      { x: 1 },
      {
        deduplication: {
          id: 'my-dedup-key',
          ttl: 60_000,
          mode: 'simple',
        },
      },
    );
    expect(job1).not.toBeNull();

    // Duplicate should be dropped
    const job2 = await queue.add(
      'job',
      { x: 2 },
      {
        deduplication: {
          id: 'my-dedup-key',
          ttl: 60_000,
          mode: 'simple',
        },
      },
    );
    expect(job2).toBeNull();
  }, 15000);

  it('Backoff jitter (new-features.md:437)', async () => {
    const qName = uid('bmq-nf-jitter');
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

  it('Queue management - pause, resume, drain, clean, obliterate (new-features.md:307)', async () => {
    const qName = uid('bmq-nf-mgmt');
    const queue = track(new Queue(qName, { connection }));

    await queue.add('j1', {});
    await queue.add('j2', {});

    await queue.pause();
    expect(await queue.isPaused()).toBe(true);

    await queue.resume();
    expect(await queue.isPaused()).toBe(false);

    await queue.drain();
    const counts = await queue.getJobCounts();
    expect(counts.waiting).toBe(0);

    await queue.obliterate({ force: true });
  }, 15000);
});
