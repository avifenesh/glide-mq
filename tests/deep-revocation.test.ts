/**
 * Deep tests: Job revocation functionality
 * Requires: valkey-server on localhost:6379 and cluster on :7000-7005
 *
 * Run: npx vitest run tests/deep-revocation.test.ts
 */
import { it, expect, beforeAll, afterAll, afterEach } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { Job } = require('../dist/job') as typeof import('../src/job');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

function uid() {
  return `revoke-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
}

describeEachMode('Job Revocation', (CONNECTION) => {
  let cleanupClient: any;
  let queueName: string;
  let queue: InstanceType<typeof Queue>;
  let worker: InstanceType<typeof Worker>;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterEach(async () => {
    if (worker) {
      try {
        await worker.close(true);
      } catch {}
    }
    if (queue) {
      try {
        await queue.close();
      } catch {}
    }
    if (queueName) await flushQueue(cleanupClient, queueName);
  });

  afterAll(async () => {
    cleanupClient.close();
  });

  it('revoke waiting job returns "revoked"', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });
    const job = await queue.add('wait-revoke', { x: 1 });
    const result = await queue.revoke(job!.id);
    expect(result).toBe('revoked');
  });

  it('revoked waiting job is moved to failed set', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });
    const job = await queue.add('fail-set', { x: 1 });
    await queue.revoke(job!.id);

    const fetched = await queue.getJob(job!.id);
    expect(fetched).not.toBeNull();
    const state = await fetched!.getState();
    expect(state).toBe('failed');
  });

  it('revoked waiting job has failedReason "revoked"', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });
    const job = await queue.add('reason-test', { x: 1 });
    await queue.revoke(job!.id);

    const fetched = await queue.getJob(job!.id);
    expect(fetched!.failedReason).toBe('revoked');
  });

  it('revoke delayed job returns "revoked"', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });
    const job = await queue.add('delay-revoke', { x: 1 }, { delay: 60000 });
    const result = await queue.revoke(job!.id);
    expect(result).toBe('revoked');
  });

  it('revoked delayed job is moved to failed', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });
    const job = await queue.add('delay-fail', { x: 1 }, { delay: 60000 });
    await queue.revoke(job!.id);

    const fetched = await queue.getJob(job!.id);
    const state = await fetched!.getState();
    expect(state).toBe('failed');
  });

  it('revoked delayed job is removed from scheduled set', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });
    const job = await queue.add('sched-remove', { x: 1 }, { delay: 60000 });
    await queue.revoke(job!.id);

    const k = buildKeys(queueName);
    const score = await cleanupClient.zscore(k.scheduled, job!.id);
    expect(score).toBeNull();
  });

  it('revoke non-existent job returns "not_found"', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });
    const result = await queue.revoke('999999');
    expect(result).toBe('not_found');
  });

  it('revoke job with state "active" returns "flagged"', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });

    const job = await queue.add('active-revoke', { x: 1 });
    const k = buildKeys(queueName);
    await cleanupClient.hset(k.job(job!.id), { state: 'active' });

    const result = await queue.revoke(job!.id);
    expect(result).toBe('flagged');
  });

  it('revoked flag is set on job hash after revoke', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });

    const job = await queue.add('flag-check', { x: 1 });
    await queue.revoke(job!.id);

    const fetched = await queue.getJob(job!.id);
    const isRevoked = await fetched!.isRevoked();
    expect(isRevoked).toBe(true);
  });

  it('job.isRevoked returns false for non-revoked job', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });
    const job = await queue.add('not-revoked', { x: 1 });
    const fetched = await queue.getJob(job!.id);
    const isRevoked = await fetched!.isRevoked();
    expect(isRevoked).toBe(false);
  });

  it('revoked waiting job does not get processed', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });
    const job = await queue.add('no-process', { x: 1 });
    await queue.revoke(job!.id);

    let processed = false;
    worker = new Worker(
      queueName,
      async () => {
        processed = true;
        return 'done';
      },
      {
        connection: CONNECTION,
        stalledInterval: 60000,
      },
    );

    await new Promise((r) => setTimeout(r, 1000));
    expect(processed).toBe(false);
  });

  it('worker skips job with revoked=1 flag at pickup', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });

    const job = await queue.add('pre-revoked', { x: 1 });
    const k = buildKeys(queueName);
    await cleanupClient.hset(k.job(job!.id), { revoked: '1' });

    const events: string[] = [];
    worker = new Worker(
      queueName,
      async () => {
        events.push('processed');
        return 'done';
      },
      {
        connection: CONNECTION,
        stalledInterval: 60000,
      },
    );
    worker.on('completed', () => events.push('completed'));
    worker.on('failed', () => events.push('failed'));

    await new Promise((r) => setTimeout(r, 1500));

    expect(events).not.toContain('processed');
    expect(events).not.toContain('completed');
  });

  it('revoke emits revoked event in events stream', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });
    const job = await queue.add('event-revoke', { x: 1 });
    await queue.revoke(job!.id);

    const k = buildKeys(queueName);
    const entries = (await cleanupClient.xrange(k.events, '-', '+')) as any;
    const events: string[] = [];
    if (entries) {
      for (const fieldPairs of Object.values(entries) as any[]) {
        for (const [field, value] of fieldPairs) {
          if (String(field) === 'event') events.push(String(value));
        }
      }
    }
    expect(events).toContain('revoked');
  });

  it('revoked waiting job appears in failed jobs list', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });
    const job = await queue.add('failed-list', { x: 1 });
    await queue.revoke(job!.id);

    const failedJobs = await queue.getJobs('failed');
    const ids = failedJobs.map((j: any) => j.id);
    expect(ids).toContain(job!.id);
  });

  it('revoked job has finishedOn timestamp set', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });
    const before = Date.now();
    const job = await queue.add('finished-on', { x: 1 });
    await queue.revoke(job!.id);
    const after = Date.now();

    const fetched = await queue.getJob(job!.id);
    expect(fetched!.finishedOn).toBeDefined();
    expect(fetched!.finishedOn).toBeGreaterThanOrEqual(before);
    expect(fetched!.finishedOn).toBeLessThanOrEqual(after + 100);
  });

  it('revoke prioritized job returns "revoked"', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });
    const job = await queue.add('prio-revoke', { x: 1 }, { priority: 5 });
    const result = await queue.revoke(job!.id);
    expect(result).toBe('revoked');
  });

  it('multiple revocations: revoke same job twice is idempotent', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });
    const job = await queue.add('double-revoke', { x: 1 });
    const r1 = await queue.revoke(job!.id);
    const r2 = await queue.revoke(job!.id);
    expect(r1).toBe('revoked');
    expect(r2).toBe('flagged');
  });

  it('revoked waiting job is removed from waiting stream', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });
    const job = await queue.add('stream-remove', { x: 1 });

    const k = buildKeys(queueName);
    const beforeLen = await cleanupClient.xlen(k.stream);
    expect(beforeLen).toBeGreaterThan(0);

    await queue.revoke(job!.id);

    const entries = (await cleanupClient.xrange(k.stream, '-', '+')) as any;
    let foundJob = false;
    if (entries) {
      for (const fieldPairs of Object.values(entries) as any[]) {
        for (const [field, value] of fieldPairs) {
          if (String(field) === 'jobId' && String(value) === job!.id) {
            foundJob = true;
          }
        }
      }
    }
    expect(foundJob).toBe(false);
  });

  it('revoke updates job counts correctly', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });
    await queue.add('counts-revoke', { x: 1 });
    const before = await queue.getJobCounts();
    expect(before.waiting + before.delayed).toBeGreaterThan(0);

    const jobs = await queue.getJobs('waiting');
    if (jobs.length > 0) {
      await queue.revoke(jobs[0].id);
    }

    const after = await queue.getJobCounts();
    expect(after.failed).toBeGreaterThanOrEqual(1);
  });
});
