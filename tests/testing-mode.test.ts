/**
 * Pure unit tests for the in-memory testing mode.
 * No Valkey required.
 *
 * Run: npx vitest run tests/testing-mode.test.ts
 */
import { describe, it, expect, afterEach, vi } from 'vitest';
import path from 'path';
import { TestQueue, TestWorker, TestJob } from '../src/testing';
import type { TestQueueOptions } from '../src/testing';
import type { JobOptions } from '../src/types';
import { BatchError } from '../src/errors';
import { MAX_JOB_DATA_SIZE } from '../src/utils';
import { waitFor } from './helpers/fixture';

const ECHO_PROCESSOR = path.resolve(__dirname, 'fixtures/processors/echo.js');

describe('TestQueue', () => {
  let queue: TestQueue;

  afterEach(async () => {
    if (queue) await queue.close();
  });

  it('add creates a job with an ID', async () => {
    queue = new TestQueue('test-q');
    const job = await queue.add('my-job', { key: 'value' });
    expect(job).not.toBeNull();
    expect(job!.id).toBe('1');
    expect(job!.name).toBe('my-job');
    expect(job!.data).toEqual({ key: 'value' });
    expect(job!).toBeInstanceOf(TestJob);
  });

  it('add auto-increments IDs', async () => {
    queue = new TestQueue('test-q');
    const j1 = await queue.add('a', {});
    const j2 = await queue.add('b', {});
    expect(j1!.id).toBe('1');
    expect(j2!.id).toBe('2');
  });

  it('addBulk creates multiple jobs', async () => {
    queue = new TestQueue('test-q');
    const jobs = await queue.addBulk([
      { name: 'j1', data: { x: 1 } },
      { name: 'j2', data: { x: 2 } },
      { name: 'j3', data: { x: 3 } },
    ]);
    expect(jobs).toHaveLength(3);
    expect(jobs[0].name).toBe('j1');
    expect(jobs[1].name).toBe('j2');
    expect(jobs[2].name).toBe('j3');
  });

  it('getJob retrieves by ID', async () => {
    queue = new TestQueue('test-q');
    const added = await queue.add('fetch-me', { val: 42 });
    const fetched = await queue.getJob(added!.id);
    expect(fetched).not.toBeNull();
    expect(fetched!.id).toBe(added!.id);
    expect(fetched!.name).toBe('fetch-me');
    expect(fetched!.data).toEqual({ val: 42 });
  });

  it('getJob returns null for unknown ID', async () => {
    queue = new TestQueue('test-q');
    const result = await queue.getJob('999');
    expect(result).toBeNull();
  });

  it('getJobs returns jobs by state', async () => {
    queue = new TestQueue('test-q');
    await queue.add('a', {});
    await queue.add('b', {});

    const waiting = await queue.getJobs('waiting');
    expect(waiting).toHaveLength(2);

    const completed = await queue.getJobs('completed');
    expect(completed).toHaveLength(0);
  });

  it('getJobCounts returns accurate counts', async () => {
    queue = new TestQueue('test-q');
    await queue.add('a', {});
    await queue.add('b', {});

    const counts = await queue.getJobCounts();
    expect(counts.waiting).toBe(2);
    expect(counts.active).toBe(0);
    expect(counts.completed).toBe(0);
    expect(counts.failed).toBe(0);
    expect(counts.delayed).toBe(0);
  });

  it('pause stops worker processing', async () => {
    queue = new TestQueue('test-q');
    const processed: string[] = [];

    const worker = new TestWorker(queue, async (job) => {
      processed.push(job.id);
      return 'done';
    });

    await queue.pause();
    await queue.add('should-not-run', {});

    // Give microtasks a chance
    await new Promise((r) => setTimeout(r, 50));
    expect(processed).toHaveLength(0);

    await worker.close();
  });

  it('resume allows processing after pause', async () => {
    queue = new TestQueue('test-q');
    const processed: string[] = [];

    const worker = new TestWorker(queue, async (job) => {
      processed.push(job.id);
      return 'done';
    });

    await queue.pause();
    await queue.add('paused-job', {});
    await new Promise((r) => setTimeout(r, 50));
    expect(processed).toHaveLength(0);

    await queue.resume();
    await waitFor(() => processed.length === 1, 5000);
    expect(processed).toHaveLength(1);

    await worker.close();
  });

  it('dedup simple mode skips duplicate IDs', async () => {
    queue = new TestQueue('test-q', { dedup: true });

    const j1 = await queue.add('a', { v: 1 }, { deduplication: { id: 'dup-1' } });
    const j2 = await queue.add('a', { v: 2 }, { deduplication: { id: 'dup-1' } });
    const j3 = await queue.add('b', { v: 3 }, { deduplication: { id: 'dup-2' } });

    expect(j1).not.toBeNull();
    expect(j2).toBeNull();
    expect(j3).not.toBeNull();
    expect(queue.jobs.size).toBe(2);
  });

  it('searchJobs by name', async () => {
    queue = new TestQueue('test-q');
    await queue.add('email', { to: 'a@b.com' });
    await queue.add('sms', { phone: '123' });
    await queue.add('email', { to: 'c@d.com' });

    const results = await queue.searchJobs({ name: 'email' });
    expect(results).toHaveLength(2);
    expect(results.every((j) => j.name === 'email')).toBe(true);
  });

  it('searchJobs by data fields', async () => {
    queue = new TestQueue('test-q');
    await queue.add('process', { region: 'us', priority: 'high' });
    await queue.add('process', { region: 'eu', priority: 'low' });
    await queue.add('process', { region: 'us', priority: 'low' });

    const results = await queue.searchJobs({ data: { region: 'us' } });
    expect(results).toHaveLength(2);
  });

  it('searchJobs by name and data combined', async () => {
    queue = new TestQueue('test-q');
    await queue.add('send', { channel: 'slack' });
    await queue.add('send', { channel: 'email' });
    await queue.add('notify', { channel: 'slack' });

    const results = await queue.searchJobs({ name: 'send', data: { channel: 'slack' } });
    expect(results).toHaveLength(1);
    expect(results[0].name).toBe('send');
    expect((results[0].data as any).channel).toBe('slack');
  });

  it('searchJobs by state', async () => {
    queue = new TestQueue('test-q');
    await queue.add('a', {});
    await queue.add('b', {});
    // Manually set one to completed for testing
    queue.jobs.get('1')!.state = 'completed';

    const waiting = await queue.searchJobs({ state: 'waiting' });
    expect(waiting).toHaveLength(1);
    expect(waiting[0].id).toBe('2');

    const completed = await queue.searchJobs({ state: 'completed' });
    expect(completed).toHaveLength(1);
    expect(completed[0].id).toBe('1');
  });

  describe('excludeData', () => {
    it('getJob with excludeData returns job without data or returnvalue', async () => {
      queue = new TestQueue('test-q');
      const added = await queue.add('job-a', { big: 'payload' });
      // Simulate completion with returnvalue
      queue.jobs.get(added!.id)!.returnvalue = 'result' as any;

      const job = await queue.getJob(added!.id, { excludeData: true });
      expect(job).not.toBeNull();
      expect(job!.id).toBe(added!.id);
      expect(job!.name).toBe('job-a');
      expect(job!.data).toBeUndefined();
      expect(job!.returnvalue).toBeUndefined();
    });

    it('getJob without excludeData returns full data', async () => {
      queue = new TestQueue('test-q');
      await queue.add('job-b', { big: 'payload' });
      const job = await queue.getJob('1');
      expect(job).not.toBeNull();
      expect(job!.data).toEqual({ big: 'payload' });
    });

    it('getJobs with excludeData returns jobs without data', async () => {
      queue = new TestQueue('test-q');
      await queue.add('j1', { x: 1 });
      await queue.add('j2', { x: 2 });

      const jobs = await queue.getJobs('waiting', 0, -1, { excludeData: true });
      expect(jobs).toHaveLength(2);
      for (const j of jobs) {
        expect(j.data).toBeUndefined();
        expect(j.returnvalue).toBeUndefined();
        expect(j.name).toBeDefined();
      }
    });

    it('getJobs without opts returns full data (backwards compat)', async () => {
      queue = new TestQueue('test-q');
      await queue.add('j1', { x: 1 });
      const jobs = await queue.getJobs('waiting');
      expect(jobs).toHaveLength(1);
      expect(jobs[0].data).toEqual({ x: 1 });
    });

    it('searchJobs with excludeData returns jobs without data', async () => {
      queue = new TestQueue('test-q');
      await queue.add('find-me', { val: 100 });
      await queue.add('find-me', { val: 200 });

      const results = await queue.searchJobs({ name: 'find-me', excludeData: true });
      expect(results).toHaveLength(2);
      for (const j of results) {
        expect(j.name).toBe('find-me');
        expect(j.data).toBeUndefined();
        expect(j.returnvalue).toBeUndefined();
      }
    });

    it('searchJobs with excludeData and data filter strips data after filtering', async () => {
      queue = new TestQueue('test-q');
      await queue.add('j1', { color: 'red' });
      await queue.add('j2', { color: 'blue' });

      // Data filter is applied first, then data is stripped since excludeData was requested
      const results = await queue.searchJobs({ data: { color: 'red' }, excludeData: true });
      expect(results).toHaveLength(1);
      expect(results[0].name).toBe('j1');
      expect(results[0].data).toBeUndefined();
      expect(results[0].returnvalue).toBeUndefined();
    });
  });
});

describe('TestWorker', () => {
  it('rejects a queue name where a TestQueue instance is required', () => {
    expect(() => new TestWorker('test-q' as any, async () => 'ok')).toThrow(
      'TestWorker expects a TestQueue instance as its first argument, got string',
    );
    expect(() => new TestWorker(undefined as any, async () => 'ok')).toThrow('got undefined');
    expect(() => new TestWorker(null as any, async () => 'ok')).toThrow('got null');
  });

  let queue: TestQueue;
  let worker: TestWorker;

  afterEach(async () => {
    if (worker) await worker.close();
    if (queue) await queue.close();
  });

  it('processes a job and emits completed', async () => {
    queue = new TestQueue('test-q');
    const completed: { job: TestJob; result: any }[] = [];

    worker = new TestWorker(queue, async (job) => {
      return `processed-${job.id}`;
    });

    worker.on('completed', (job, result) => {
      completed.push({ job, result });
    });

    await queue.add('task', { data: 1 });
    await waitFor(() => completed.length === 1, 5000);

    expect(completed).toHaveLength(1);
    expect(completed[0].result).toBe('processed-1');
    expect(completed[0].job.returnvalue).toBe('processed-1');

    const record = queue.jobs.get('1')!;
    expect(record.state).toBe('completed');
    expect(record.returnvalue).toBe('processed-1');
  });

  it('handles failure and emits failed', async () => {
    queue = new TestQueue('test-q');
    const failures: { job: TestJob; err: Error }[] = [];

    worker = new TestWorker(queue, async () => {
      throw new Error('boom');
    });

    worker.on('failed', (job, err) => {
      failures.push({ job, err });
    });

    await queue.add('fail-task', {});
    await waitFor(() => failures.length === 1, 5000);

    expect(failures).toHaveLength(1);
    expect(failures[0].err.message).toBe('boom');

    const record = queue.jobs.get('1')!;
    expect(record.state).toBe('failed');
    expect(record.failedReason).toBe('boom');
  });

  it('respects concurrency', async () => {
    queue = new TestQueue('test-q');
    let maxConcurrent = 0;
    let currentConcurrent = 0;

    worker = new TestWorker(
      queue,
      async () => {
        currentConcurrent++;
        if (currentConcurrent > maxConcurrent) maxConcurrent = currentConcurrent;
        await new Promise((r) => setTimeout(r, 30));
        currentConcurrent--;
        return 'ok';
      },
      { concurrency: 2 },
    );

    // Add 4 jobs
    await queue.addBulk([
      { name: 'a', data: {} },
      { name: 'b', data: {} },
      { name: 'c', data: {} },
      { name: 'd', data: {} },
    ]);

    // Wait for all to complete
    await waitFor(async () => (await queue.getJobCounts()).completed === 4, 5000);

    const counts = await queue.getJobCounts();
    expect(counts.completed).toBe(4);
    expect(maxConcurrent).toBe(2);
  });

  it('retries a failed job', async () => {
    queue = new TestQueue('test-q');
    let callCount = 0;

    worker = new TestWorker(queue, async () => {
      callCount++;
      if (callCount < 3) throw new Error('temporary');
      return 'success';
    });

    await queue.add('retry-task', {}, { attempts: 3 });
    await waitFor(() => callCount === 3, 5000);

    expect(callCount).toBe(3);

    const record = queue.jobs.get('1')!;
    expect(record.state).toBe('completed');
    expect(record.returnvalue).toBe('success');
  });

  it('fails after exhausting retries', async () => {
    queue = new TestQueue('test-q');
    const failures: Error[] = [];

    worker = new TestWorker(queue, async () => {
      throw new Error('permanent');
    });

    worker.on('failed', (_job, err) => {
      failures.push(err);
    });

    await queue.add('exhaust-task', {}, { attempts: 2 });
    await waitFor(() => failures.length === 2, 5000);

    // attempts=2 means max 2 tries. The worker emits 'failed' on each attempt, like production.
    expect(failures).toHaveLength(2);

    const record = queue.jobs.get('1')!;
    expect(record.state).toBe('failed');
    expect(record.attemptsMade).toBe(2);
  });

  it('processes jobs already in the queue at construction', async () => {
    queue = new TestQueue('test-q');
    await queue.add('pre-existing', { x: 1 });

    const completed: string[] = [];
    worker = new TestWorker(queue, async (job) => {
      completed.push(job.id);
      return 'done';
    });

    await waitFor(() => completed.length === 1, 5000);
    expect(completed).toHaveLength(1);
    expect(completed[0]).toBe('1');
  });

  it('close stops processing new jobs', async () => {
    queue = new TestQueue('test-q');
    const processed: string[] = [];

    worker = new TestWorker(queue, async (job) => {
      processed.push(job.id);
      return 'ok';
    });

    await worker.close();
    await queue.add('after-close', {});
    await new Promise((r) => setTimeout(r, 50));

    expect(processed).toHaveLength(0);
  });

  it('should accept a file path string as processor', async () => {
    queue = new TestQueue('test-q');
    const completed: { job: TestJob; result: any }[] = [];

    worker = new TestWorker(queue, ECHO_PROCESSOR);

    worker.on('completed', (job, result) => {
      completed.push({ job, result });
    });

    await queue.add('echo-task', { greeting: 'hello' });
    await waitFor(() => completed.length === 1, 5000);

    expect(completed).toHaveLength(1);
    expect(completed[0].result).toEqual({ greeting: 'hello' });
  });
});

describe('TestJob.changePriority', () => {
  let queue: TestQueue;

  afterEach(async () => {
    if (queue) await queue.close();
  });

  it('updates opts.priority', async () => {
    queue = new TestQueue('cp-test');
    const job = await queue.add('task', { x: 1 }, { priority: 3 });
    expect(job!.opts.priority).toBe(3);
    await job!.changePriority(7);
    expect(job!.opts.priority).toBe(7);
  });

  it('throws on negative priority', async () => {
    queue = new TestQueue('cp-neg');
    const job = await queue.add('task', { x: 1 });
    await expect(job!.changePriority(-1)).rejects.toThrow('Priority must be >= 0');
  });
});

describe('TestJob.changeDelay', () => {
  let queue: TestQueue;

  afterEach(async () => {
    if (queue) await queue.close();
  });

  it('updates opts.delay', async () => {
    queue = new TestQueue('cd-test');
    const job = await queue.add('task', { x: 1 }, { delay: 5000 });
    expect(job!.opts.delay).toBe(5000);
    await job!.changeDelay(10000);
    expect(job!.opts.delay).toBe(10000);
  });

  it('throws on negative delay', async () => {
    queue = new TestQueue('cd-neg');
    const job = await queue.add('task', { x: 1 });
    await expect(job!.changeDelay(-1)).rejects.toThrow('Delay must be >= 0');
  });
});

describe('TestJob.promote', () => {
  let queue: TestQueue;

  afterEach(async () => {
    if (queue) await queue.close();
  });

  it('updates opts.delay to 0', async () => {
    queue = new TestQueue('promote-test');
    const job = await queue.add('task', { x: 1 }, { delay: 5000 });
    expect(job!.opts.delay).toBe(5000);
    await job!.promote();
    expect(job!.opts.delay).toBe(0);
  });

  it('rejects a job that is not delayed, like Job.promote', async () => {
    queue = new TestQueue('promote-noop');
    const job = await queue.add('task', { x: 1 });
    expect(job!.opts.delay).toBeUndefined();
    await expect(job!.promote()).rejects.toThrow('Cannot promote: not_delayed');
    expect(job!.opts.delay).toBeUndefined();
  });
});

describe('TestQueue.retryJobs', () => {
  let queue: TestQueue;
  let worker: InstanceType<typeof TestWorker> | undefined;

  afterEach(async () => {
    if (worker) await worker.close();
    if (queue) await queue.close();
    worker = undefined;
  });

  it('retries all failed jobs', async () => {
    queue = new TestQueue('retry-all');
    const failures: string[] = [];

    worker = new TestWorker(queue, async () => {
      throw new Error('fail');
    });
    worker.on('failed', (job) => failures.push(job.id));

    await queue.add('t1', {});
    await queue.add('t2', {});
    await queue.add('t3', {});

    await waitFor(() => failures.length === 3, 5000);
    expect(failures).toHaveLength(3);

    const counts = await queue.getJobCounts();
    expect(counts.failed).toBe(3);

    await worker.close();
    worker = undefined;

    const retried = await queue.retryJobs();
    expect(retried).toBe(3);

    const after = await queue.getJobCounts();
    expect(after.failed).toBe(0);
    expect(after.waiting).toBe(3);

    // Verify reset fields
    for (const record of queue.jobs.values()) {
      expect(record.state).toBe('waiting');
      expect(record.attemptsMade).toBe(0);
      expect(record.failedReason).toBeUndefined();
      expect(record.finishedOn).toBeUndefined();
    }
  });

  it('respects count limit', async () => {
    queue = new TestQueue('retry-count');

    worker = new TestWorker(queue, async () => {
      throw new Error('fail');
    });

    await queue.add('t1', {});
    await queue.add('t2', {});
    await queue.add('t3', {});

    await waitFor(async () => (await queue.getJobCounts()).failed === 3, 5000);
    await worker.close();
    worker = undefined;

    const retried = await queue.retryJobs({ count: 2 });
    expect(retried).toBe(2);

    const counts = await queue.getJobCounts();
    expect(counts.failed).toBe(1);
    expect(counts.waiting).toBe(2);
  });

  it('returns 0 when no failed jobs', async () => {
    queue = new TestQueue('retry-empty');
    await queue.add('t1', {});

    const retried = await queue.retryJobs();
    expect(retried).toBe(0);
  });

  it('retried jobs with mixed priorities all go to waiting in TestQueue', async () => {
    queue = new TestQueue('retry-prio');

    worker = new TestWorker(queue, async () => {
      throw new Error('fail');
    });

    await queue.add('t1', {}, { priority: 0 });
    await queue.add('t2', {}, { priority: 5 });

    await waitFor(async () => (await queue.getJobCounts()).failed === 2, 5000);
    await worker.close();
    worker = undefined;

    const retried = await queue.retryJobs();
    expect(retried).toBe(2);

    const records = [...queue.jobs.values()];
    const noPrio = records.find((r) => r.name === 't1');
    const withPrio = records.find((r) => r.name === 't2');
    expect(noPrio!.state).toBe('waiting');
    expect(withPrio!.state).toBe('waiting');
  });

  it('count > total failed still retries all available', async () => {
    queue = new TestQueue('retry-over');

    worker = new TestWorker(queue, async () => {
      throw new Error('fail');
    });

    await queue.add('t1', {});
    await queue.add('t2', {});

    await waitFor(async () => (await queue.getJobCounts()).failed === 2, 5000);
    await worker.close();
    worker = undefined;

    const retried = await queue.retryJobs({ count: 100 });
    expect(retried).toBe(2);

    const counts = await queue.getJobCounts();
    expect(counts.failed).toBe(0);
  });

  it('retried jobs get processed by workers', async () => {
    queue = new TestQueue('retry-process');
    let callCount = 0;
    const completed: string[] = [];

    worker = new TestWorker(queue, async () => {
      callCount++;
      if (callCount <= 1) throw new Error('first attempt fails');
      return 'ok';
    });
    worker.on('completed', (job) => completed.push(job.id));

    await queue.add('t1', {});
    await waitFor(async () => (await queue.getJobCounts()).failed === 1, 5000);

    // Job should have failed (no retries configured)
    expect((await queue.getJobCounts()).failed).toBe(1);

    const retried = await queue.retryJobs();
    expect(retried).toBe(1);

    await waitFor(() => completed.length === 1, 5000);
    expect(completed).toHaveLength(1);
    expect(completed[0]).toBe('1');
  });
});

describe('TestQueue.getWorkers', () => {
  let queue: TestQueue;
  let worker: InstanceType<typeof TestWorker> | undefined;

  afterEach(async () => {
    if (worker) await worker.close();
    if (queue) await queue.close();
    worker = undefined;
  });

  it('returns empty when no workers', async () => {
    queue = new TestQueue('gw-empty');
    const workers = await queue.getWorkers();
    expect(workers).toEqual([]);
  });

  it('lists active TestWorker', async () => {
    queue = new TestQueue('gw-single');
    worker = new TestWorker(queue, async () => 'ok');

    const workers = await queue.getWorkers();
    expect(workers).toHaveLength(1);
    expect(workers[0].id).toBeTruthy();
    expect(typeof workers[0].addr).toBe('string');
    expect(typeof workers[0].pid).toBe('number');
    expect(workers[0].pid).toBeGreaterThan(0);
    expect(typeof workers[0].startedAt).toBe('number');
    expect(workers[0].age).toBeGreaterThanOrEqual(0);
    expect(typeof workers[0].activeJobs).toBe('number');
  });

  it('reports the TestWorker concurrency, defaulting to 1', async () => {
    queue = new TestQueue('gw-concurrency');
    const wide = new TestWorker(queue, async () => 'ok', { concurrency: 4 });
    const narrow = new TestWorker(queue, async () => 'ok');

    const workers = await queue.getWorkers();
    expect(workers.find((w) => w.id === wide.id)!.concurrency).toBe(4);
    expect(workers.find((w) => w.id === narrow.id)!.concurrency).toBe(1);

    await wide.close();
    await narrow.close();
  });

  it('worker removed after close', async () => {
    queue = new TestQueue('gw-close');
    worker = new TestWorker(queue, async () => 'ok');

    expect(await queue.getWorkers()).toHaveLength(1);

    await worker.close();
    worker = undefined;

    expect(await queue.getWorkers()).toHaveLength(0);
  });

  it('activeJobs tracks processing count', async () => {
    queue = new TestQueue('gw-active');
    let finishJob!: () => void;
    const jobPromise = new Promise<void>((r) => {
      finishJob = r;
    });

    worker = new TestWorker(queue, async () => {
      await jobPromise;
      return 'ok';
    });

    await queue.add('slow', {});
    // Let the microtask schedule processing
    await waitFor(async () => (await queue.getWorkers())[0]?.activeJobs === 1, 5000);

    const during = await queue.getWorkers();
    expect(during).toHaveLength(1);
    expect(during[0].activeJobs).toBe(1);

    finishJob();
    await waitFor(async () => (await queue.getWorkers())[0]?.activeJobs === 0, 5000);

    const after = await queue.getWorkers();
    expect(after).toHaveLength(1);
    expect(after[0].activeJobs).toBe(0);
  });

  it('multiple workers with distinct IDs', async () => {
    queue = new TestQueue('gw-multi');
    const w1 = new TestWorker(queue, async () => 'ok');
    const w2 = new TestWorker(queue, async () => 'ok');

    const workers = await queue.getWorkers();
    expect(workers).toHaveLength(2);

    const ids = workers.map((w) => w.id);
    expect(new Set(ids).size).toBe(2);

    await w1.close();
    await w2.close();
    worker = undefined;
  });
});

describe('TestQueue.getJobScheduler', () => {
  let queue: TestQueue;

  afterEach(async () => {
    if (queue) await queue.close();
  });

  it('getJobScheduler returns entry after upsert', async () => {
    queue = new TestQueue('sched-test');
    await queue.upsertJobScheduler('test-sched', { every: 1000 }, { name: 'sched-job', data: { a: 1 } });

    const entry = await queue.getJobScheduler('test-sched');
    expect(entry).not.toBeNull();
    expect(entry!.every).toBe(1000);
    expect(entry!.template?.name).toBe('sched-job');
    expect(entry!.template?.data).toEqual({ a: 1 });
    expect(entry!.nextRun).toBeGreaterThan(0);

    await queue.removeJobScheduler('test-sched');
  });

  it('stores scheduler bounds and iteration count after upsert', async () => {
    queue = new TestQueue('sched-bounds');
    const startDate = Date.now() + 1000;
    const endDate = startDate + 2000;
    await queue.upsertJobScheduler('bounded', {
      every: 250,
      startDate: new Date(startDate),
      endDate,
      limit: 2,
    });

    const entry = await queue.getJobScheduler('bounded');
    expect(entry).not.toBeNull();
    expect(entry!.startDate).toBe(startDate);
    expect(entry!.endDate).toBe(endDate);
    expect(entry!.limit).toBe(2);
    expect(entry!.iterationCount).toBe(0);
    expect(entry!.nextRun).toBe(startDate);

    await queue.removeJobScheduler('bounded');
  });

  it('preserves iteration state when re-upserting an unchanged scheduler', async () => {
    queue = new TestQueue('sched-preserve');
    const startDate = Date.now() + 2000;
    await queue.upsertJobScheduler('preserve', { every: 250, startDate, limit: 3 }, { name: 'preserve-job' });
    (queue as any).schedulers.set('preserve', {
      every: 250,
      startDate,
      limit: 3,
      iterationCount: 2,
      lastRun: startDate,
      nextRun: startDate + 250,
      template: { name: 'preserve-job' },
    });

    await queue.upsertJobScheduler('preserve', { every: 250, startDate, limit: 3 }, { name: 'preserve-job-v2' });

    const entry = await queue.getJobScheduler('preserve');
    expect(entry).not.toBeNull();
    expect(entry!.iterationCount).toBe(2);
    expect(entry!.lastRun).toBe(startDate);
    expect(entry!.nextRun).toBe(startDate + 250);
  });

  it('resets iteration state when re-upserting a changed scheduler', async () => {
    queue = new TestQueue('sched-reset');
    const startDate = Date.now() + 2000;
    await queue.upsertJobScheduler('reset', { every: 250, startDate, limit: 3 }, { name: 'reset-job' });
    (queue as any).schedulers.set('reset', {
      every: 250,
      startDate,
      limit: 3,
      iterationCount: 2,
      lastRun: startDate,
      nextRun: startDate + 250,
      template: { name: 'reset-job' },
    });

    await queue.upsertJobScheduler('reset', { every: 500, startDate, limit: 3 }, { name: 'reset-job-v2' });

    const entry = await queue.getJobScheduler('reset');
    expect(entry).not.toBeNull();
    expect(entry!.iterationCount).toBe(0);
    expect(entry!.lastRun).toBeUndefined();
    expect(entry!.nextRun).toBe(startDate);
  });

  it('getJobScheduler returns null for missing name', async () => {
    queue = new TestQueue('sched-miss');
    const entry = await queue.getJobScheduler('nonexistent');
    expect(entry).toBeNull();
  });

  it('getJobScheduler returns scheduler with cron pattern', async () => {
    queue = new TestQueue('sched-cron');
    await queue.upsertJobScheduler('cron-entry', { pattern: '*/5 * * * *' });

    const entry = await queue.getJobScheduler('cron-entry');
    expect(entry).not.toBeNull();
    expect(entry!.pattern).toBe('*/5 * * * *');
    expect(entry!.every).toBeUndefined();
    expect(entry!.template).toBeUndefined();

    await queue.removeJobScheduler('cron-entry');
  });

  it('getRepeatableJobs returns all scheduler entries', async () => {
    queue = new TestQueue('sched-all');
    await queue.upsertJobScheduler('a', { every: 100 });
    await queue.upsertJobScheduler('b', { every: 200 });

    const all = await queue.getRepeatableJobs();
    expect(all).toHaveLength(2);
    const names = all.map((s) => s.name).sort();
    expect(names).toEqual(['a', 'b']);
    for (const item of all) {
      expect(item.entry.every).toBeGreaterThan(0);
      expect(item.entry.nextRun).toBeGreaterThan(0);
    }

    await queue.removeJobScheduler('a');
    await queue.removeJobScheduler('b');
  });

  // --- Timezone support (#74) ---

  it('upsertJobScheduler stores tz for cron scheduler', async () => {
    queue = new TestQueue('sched-tz');
    await queue.upsertJobScheduler('tz-cron', { pattern: '0 9 * * *', tz: 'America/New_York' });

    const entry = await queue.getJobScheduler('tz-cron');
    expect(entry).not.toBeNull();
    expect(entry!.pattern).toBe('0 9 * * *');
    expect(entry!.tz).toBe('America/New_York');
    expect(entry!.nextRun).toBeGreaterThan(0);

    await queue.removeJobScheduler('tz-cron');
  });

  it('upsertJobScheduler without tz does not include tz in entry', async () => {
    queue = new TestQueue('sched-no-tz');
    await queue.upsertJobScheduler('no-tz', { pattern: '0 9 * * *' });

    const entry = await queue.getJobScheduler('no-tz');
    expect(entry).not.toBeNull();
    expect(entry!.tz).toBeUndefined();

    await queue.removeJobScheduler('no-tz');
  });

  it('upsertJobScheduler rejects invalid timezone', async () => {
    queue = new TestQueue('sched-bad-tz');
    await expect(queue.upsertJobScheduler('bad', { pattern: '0 9 * * *', tz: 'Fake/Zone' })).rejects.toThrow(
      'Invalid timezone',
    );
  });

  it('upsertJobScheduler rejects invalid bounds', async () => {
    queue = new TestQueue('sched-bad-bounds');
    const startDate = Date.now() + 5000;
    const endDate = startDate - 1000;
    await expect(queue.upsertJobScheduler('bad-window', { every: 1000, startDate, endDate })).rejects.toThrow(
      'startDate must be less than or equal to endDate',
    );
    await expect(queue.upsertJobScheduler('bad-limit', { every: 1000, limit: 0 })).rejects.toThrow(
      'limit must be a positive integer',
    );
  });

  it('upsertJobScheduler rejects invalid every intervals', async () => {
    queue = new TestQueue('sched-bad-every');
    await expect(queue.upsertJobScheduler('bad-every-negative', { every: -100 })).rejects.toThrow(
      'every must be a positive safe integer',
    );
    await expect(queue.upsertJobScheduler('bad-every-zero', { every: 0 as any })).rejects.toThrow(
      'every must be a positive safe integer',
    );
    await expect(queue.upsertJobScheduler('bad-every-string', { every: '100' as any })).rejects.toThrow(
      'every must be a positive safe integer',
    );
    await expect(queue.upsertJobScheduler('bad-every-float', { every: 1.5 as any })).rejects.toThrow(
      'every must be a positive safe integer',
    );
  });

  it('upsertJobScheduler rejects schedules with no occurrences inside the configured bounds', async () => {
    queue = new TestQueue('sched-empty-bounds');
    const startDate = new Date('2024-01-02T00:00:00Z').getTime();
    const endDate = new Date('2024-01-02T00:00:00Z').getTime();
    await expect(
      queue.upsertJobScheduler('no-window', {
        pattern: '0 0 1 1 *',
        startDate,
        endDate,
      }),
    ).rejects.toThrow('Schedule has no occurrences within the configured bounds');
  });

  it('upsertJobScheduler rejects invalid dates', async () => {
    queue = new TestQueue('sched-bad-dates');
    await expect(
      queue.upsertJobScheduler('bad-start-date', { every: 1000, startDate: new Date(Number.NaN) }),
    ).rejects.toThrow('startDate must be a valid Date or timestamp');
    await expect(queue.upsertJobScheduler('bad-end-date', { every: 1000, endDate: Number.NaN as any })).rejects.toThrow(
      'endDate must be a valid Date or timestamp',
    );
  });
});

describe('TestWorker - TTL', () => {
  let queue: TestQueue;
  let worker: TestWorker;

  afterEach(async () => {
    if (worker) await worker.close();
    if (queue) await queue.close();
  });

  it('expired job is failed with reason "expired"', async () => {
    queue = new TestQueue('ttl-test');
    // Add a job with ttl=1ms
    const job = await queue.add('task', { v: 1 }, { ttl: 1 });
    expect(job).not.toBeNull();
    expect(job!.opts.ttl).toBe(1);

    // Wait for TTL to pass
    await new Promise<void>((r) => setTimeout(r, 10));

    const failed: { job: any; err: Error }[] = [];
    const completed: any[] = [];

    worker = new TestWorker(queue, async () => {
      return 'should not run';
    });
    worker.on('failed', (j: any, err: Error) => failed.push({ job: j, err }));
    worker.on('completed', (j: any) => completed.push(j));

    // Wait for processing
    await waitFor(() => failed.length === 1, 5000);

    expect(completed).toHaveLength(0);
    expect(failed).toHaveLength(1);
    expect(failed[0].err.message).toBe('expired');
    expect(failed[0].job.failedReason).toBe('expired');

    const record = queue.jobs.get(job!.id);
    expect(record?.state).toBe('failed');
    expect(record?.failedReason).toBe('expired');
  });

  it('job with ttl processes normally when not expired', async () => {
    queue = new TestQueue('ttl-test-ok');
    const job = await queue.add('task', { v: 2 }, { ttl: 60000 });
    expect(job).not.toBeNull();

    const completed: any[] = [];
    worker = new TestWorker(queue, async () => 'done');
    worker.on('completed', (j: any) => completed.push(j));

    await waitFor(() => completed.length === 1, 5000);

    expect(completed).toHaveLength(1);
    expect(completed[0].returnvalue).toBe('done');
  });

  it('job without ttl has no expireAt', async () => {
    queue = new TestQueue('ttl-test-none');
    const job = await queue.add('task', { v: 3 });
    expect(job).not.toBeNull();

    const record = queue.jobs.get(job!.id);
    expect(record?.expireAt).toBeUndefined();
  });

  it('expireAt is stored correctly on the record', async () => {
    queue = new TestQueue('ttl-test-store');
    const before = Date.now();
    const job = await queue.add('task', { v: 4 }, { ttl: 5000 });
    const after = Date.now();

    const record = queue.jobs.get(job!.id);
    expect(record?.expireAt).toBeDefined();
    expect(record!.expireAt!).toBeGreaterThanOrEqual(before + 5000);
    expect(record!.expireAt!).toBeLessThanOrEqual(after + 5000);
  });
});

describe('TestQueue scheduler runtime', () => {
  let queue: TestQueue;
  let worker: TestWorker;

  afterEach(async () => {
    if (worker) await worker.close();
    if (queue) await queue.close();
  });

  it('fires repeat schedulers and removes them after reaching limit', async () => {
    queue = new TestQueue('sched-runtime-limit');
    const processed: string[] = [];
    worker = new TestWorker(queue, async (job: any) => {
      processed.push(job.id);
      return 'ok';
    });

    await queue.upsertJobScheduler('runtime-repeat', { every: 20, limit: 2 }, { name: 'tick', data: { ok: true } });

    const deadline = Date.now() + 1000;
    while (processed.length < 2 && Date.now() < deadline) {
      await new Promise<void>((r) => setTimeout(r, 20));
    }

    expect(processed).toHaveLength(2);
    await waitFor(async () => (await queue.getJobScheduler('runtime-repeat')) === null, 5000);
    expect(await queue.getJobScheduler('runtime-repeat')).toBeNull();
  });

  it('waits for future startDate before firing a scheduler in testing mode', async () => {
    queue = new TestQueue('sched-runtime-start');
    const processed: string[] = [];
    worker = new TestWorker(queue, async (job: any) => {
      processed.push(job.id);
      return 'ok';
    });

    const startDate = Date.now() + 120;
    await queue.upsertJobScheduler('runtime-start', { every: 50, startDate, limit: 1 }, { name: 'tick' });

    await new Promise<void>((r) => setTimeout(r, 60));
    expect(processed).toHaveLength(0);

    const deadline = Date.now() + 500;
    while (processed.length < 1 && Date.now() < deadline) {
      await new Promise<void>((r) => setTimeout(r, 20));
    }

    expect(processed).toHaveLength(1);
    expect(await queue.getJobScheduler('runtime-start')).toBeNull();
  });

  it('reschedules the testing-mode wake-up when a sooner scheduler is added later', async () => {
    queue = new TestQueue('sched-runtime-reschedule');
    const processed: string[] = [];
    worker = new TestWorker(queue, async (job: any) => {
      processed.push(job.name);
      return 'ok';
    });

    await queue.upsertJobScheduler(
      'later',
      { every: 200, startDate: Date.now() + 500, limit: 1 },
      { name: 'later-job' },
    );
    await new Promise<void>((r) => setTimeout(r, 20));
    await queue.upsertJobScheduler(
      'sooner',
      { every: 200, startDate: Date.now() + 60, limit: 1 },
      { name: 'sooner-job' },
    );

    const deadline = Date.now() + 500;
    while (processed.length < 1 && Date.now() < deadline) {
      await new Promise<void>((r) => setTimeout(r, 20));
    }

    expect(processed[0]).toBe('sooner-job');
  });

  it('clamps far-future testing-mode wake-ups to the maximum timer delay', async () => {
    queue = new TestQueue('sched-runtime-far-future');
    worker = new TestWorker(queue, async () => 'ok');

    const maxTimerDelayMs = 2_147_483_647;
    const farFutureStartDate = Date.now() + maxTimerDelayMs + 60_000;
    await queue.upsertJobScheduler(
      'far-future',
      { every: 60_000, startDate: farFutureStartDate, limit: 1 },
      { name: 'far-future-job' },
    );

    const nextWakeAt = (queue as any).nextSchedulerWakeAt as number | null;
    expect(nextWakeAt).not.toBeNull();
    expect(nextWakeAt! - Date.now()).toBeLessThanOrEqual(maxTimerDelayMs);
    expect((queue as any).schedulerTimer).not.toBeNull();
  });

  it('rejects oversized testing-mode scheduler templates at upsert, like production', async () => {
    queue = new TestQueue('sched-runtime-oversized');
    worker = new TestWorker(queue, async () => 'ok');

    await expect(
      queue.upsertJobScheduler(
        'oversized',
        { every: 20, limit: 1 },
        { name: 'oversized-job', data: 'x'.repeat(MAX_JOB_DATA_SIZE + 1) },
      ),
    ).rejects.toThrow('Scheduler template: Job data exceeds maximum size');

    expect(await queue.getJobScheduler('oversized')).toBeNull();
  });

  it('scheduler runs ignore stored template deduplication, delay and jobId, like the real tick', async () => {
    queue = new TestQueue('sched-runtime-template-opts');
    worker = new TestWorker(queue, async () => 'ok');
    // Keep the seed job waiting: simple dedup releases the id once the job completes.
    await queue.pause();

    const seed = await queue.add('seed', { ok: true }, { deduplication: { id: 'dup-key', mode: 'simple' } });
    // upsertJobScheduler rejects these template options, so seed a stored legacy entry directly.
    await queue.upsertJobScheduler(
      'legacy-scheduler',
      { every: 20, limit: 1 },
      { name: 'scheduled-job', data: { ok: true } },
    );
    const seeded = (queue as any).schedulers.get('legacy-scheduler');
    seeded.template.opts = { deduplication: { id: 'dup-key', mode: 'simple' }, delay: 60_000, jobId: 'fixed-id' };

    await waitFor(async () => (await queue.getJobScheduler('legacy-scheduler')) === null, 2000, 10);

    const produced = (await queue.searchJobs({ name: 'scheduled-job' }))[0];
    expect(produced).toBeDefined();
    expect(produced.id).not.toBe('fixed-id');
    expect(produced.id).not.toBe(seed!.id);
    expect(await produced.getState()).toBe('waiting');
    expect(queue.jobs.size).toBe(2);
  });
});

// ---- TestWorker batch mode ----

describe('TestWorker batch mode', () => {
  let queue: TestQueue;
  let worker: TestWorker;

  afterEach(async () => {
    if (worker) await worker.close();
    if (queue) await queue.close();
  });

  it('processes jobs as a batch', async () => {
    queue = new TestQueue('batch-test');
    const batchSizes: number[] = [];

    // Add jobs first so they are all available before worker starts
    await queue.addBulk([
      { name: 'a', data: { i: 1 } },
      { name: 'b', data: { i: 2 } },
      { name: 'c', data: { i: 3 } },
    ]);

    worker = new TestWorker(
      queue,
      async (jobs: TestJob[]) => {
        batchSizes.push(jobs.length);
        return jobs.map((j: any) => j.data.i * 2);
      },
      { batch: { size: 3 } },
    );

    // Wait for processing
    await new Promise<void>((resolve) => {
      const check = setInterval(async () => {
        const counts = await queue.getJobCounts();
        if (counts.completed === 3) {
          clearInterval(check);
          resolve();
        }
      }, 10);
    });

    // All 3 should be processed in a single batch since they were waiting before worker started
    expect(batchSizes).toEqual([3]);
    const completed = await queue.getJobs('completed');
    expect(completed).toHaveLength(3);
    const returnValues = completed.map((j) => j.returnvalue).sort();
    expect(returnValues).toEqual([2, 4, 6]);
  });

  it('flushes a partial batch after batch.timeout', async () => {
    queue = new TestQueue('batch-timeout');
    const batchSizes: number[] = [];

    await queue.add('a', { i: 1 });
    await queue.add('b', { i: 2 });

    worker = new TestWorker(
      queue,
      async (jobs: TestJob[]) => {
        batchSizes.push(jobs.length);
        return jobs.map(() => 'ok');
      },
      { batch: { size: 5, timeout: 50 } },
    );

    await waitFor(async () => (await queue.getJobCounts()).completed === 2, 2000, 20);
    expect(batchSizes).toEqual([2]);
    expect((await queue.getJobs('waiting')).length).toBe(0);
  });

  it('flushes leftover pending jobs after a full batch takes only part of them', async () => {
    queue = new TestQueue('batch-remainder');
    const batchSizes: number[] = [];

    await queue.addBulk([
      { name: 'a', data: { i: 1 } },
      { name: 'b', data: { i: 2 } },
      { name: 'c', data: { i: 3 } },
      { name: 'd', data: { i: 4 } },
    ]);

    worker = new TestWorker(
      queue,
      async (jobs: TestJob[]) => {
        batchSizes.push(jobs.length);
        return jobs.map(() => 'ok');
      },
      { batch: { size: 5, timeout: 50 } },
    );

    await waitFor(async () => (await queue.getJobCounts()).waiting === 4, 500, 10);
    await queue.addBulk([
      { name: 'e', data: { i: 5 } },
      { name: 'f', data: { i: 6 } },
      { name: 'g', data: { i: 7 } },
      { name: 'h', data: { i: 8 } },
    ]);

    await waitFor(async () => (await queue.getJobCounts()).completed === 8, 2000, 20);
    expect(batchSizes).toEqual([5, 3]);
  });

  it('does not process a pending batch after drain()', async () => {
    queue = new TestQueue('batch-drain');
    const batchSizes: number[] = [];

    await queue.add('a', { i: 1 });
    await queue.add('b', { i: 2 });

    worker = new TestWorker(
      queue,
      async (jobs: TestJob[]) => {
        batchSizes.push(jobs.length);
        return jobs.map(() => 'ok');
      },
      { batch: { size: 5, timeout: 80 } },
    );

    await waitFor(async () => (await queue.getJobCounts()).waiting === 2, 500, 10);
    await queue.drain();
    await new Promise((r) => setTimeout(r, 150));
    expect(batchSizes).toEqual([]);
    expect((await queue.getJobCounts()).completed).toBe(0);
  });

  it('hands a closing worker pending batch to remaining workers', async () => {
    queue = new TestQueue('batch-close-handoff');
    const batchSizes: number[] = [];

    await queue.add('a', { i: 1 });
    await queue.add('b', { i: 2 });

    worker = new TestWorker(
      queue,
      async () => {
        throw new Error('closing worker should not flush');
      },
      { batch: { size: 5, timeout: 5000 } },
    );

    await waitFor(async () => (await queue.getJobCounts()).waiting === 2, 500, 10);

    const peer = new TestWorker(
      queue,
      async (jobs: TestJob[]) => {
        batchSizes.push(jobs.length);
        return jobs.map(() => 'ok');
      },
      { batch: { size: 5, timeout: 50 } },
    );

    await worker.close();
    worker = peer;

    await waitFor(async () => (await queue.getJobCounts()).completed === 2, 2000, 20);
    expect(batchSizes).toEqual([2]);
  });

  it('emits active and completed events per job', async () => {
    queue = new TestQueue('batch-events');
    const activeIds: string[] = [];
    const completedIds: string[] = [];

    worker = new TestWorker(
      queue,
      async (jobs: TestJob[]) => {
        return jobs.map(() => 'done');
      },
      { batch: { size: 2 } },
    );

    worker.on('active', (_job: any, jobId: string) => {
      activeIds.push(jobId);
    });
    worker.on('completed', (job: any) => {
      completedIds.push(job.id);
    });

    const jobs = await queue.addBulk([
      { name: 'x', data: {} },
      { name: 'y', data: {} },
    ]);

    await new Promise<void>((resolve) => {
      const check = setInterval(async () => {
        const counts = await queue.getJobCounts();
        if (counts.completed === 2) {
          clearInterval(check);
          resolve();
        }
      }, 10);
    });

    for (const job of jobs) {
      expect(activeIds).toContain(job!.id);
      expect(completedIds).toContain(job!.id);
    }
  });

  it('handles BatchError partial failure', async () => {
    queue = new TestQueue('batch-partial-fail');
    const failedIds: string[] = [];
    const completedIds: string[] = [];

    worker = new TestWorker(
      queue,
      async (_jobs: TestJob[]) => {
        throw new BatchError(['success-result', new Error('this one failed'), 'another-success']);
      },
      { batch: { size: 3 } },
    );

    worker.on('completed', (job: any) => {
      completedIds.push(job.id);
    });
    worker.on('failed', (job: any) => {
      failedIds.push(job.id);
    });

    const jobs = await queue.addBulk([
      { name: 'a', data: {} },
      { name: 'b', data: {} },
      { name: 'c', data: {} },
    ]);

    await new Promise<void>((resolve) => {
      const check = setInterval(async () => {
        const counts = await queue.getJobCounts();
        if (counts.completed + counts.failed === 3) {
          clearInterval(check);
          resolve();
        }
      }, 10);
    });

    expect(completedIds).toHaveLength(2);
    expect(failedIds).toHaveLength(1);
    // The second job (index 1) should be the one that failed
    expect(failedIds[0]).toBe(jobs[1]!.id);
  });

  it('processor throw fails all jobs in batch', async () => {
    queue = new TestQueue('batch-all-fail');
    const failedIds: string[] = [];

    worker = new TestWorker(
      queue,
      async (_jobs: TestJob[]) => {
        throw new Error('everything broke');
      },
      { batch: { size: 3 } },
    );

    worker.on('failed', (job: any) => {
      failedIds.push(job.id);
    });

    await queue.addBulk([
      { name: 'a', data: {} },
      { name: 'b', data: {} },
      { name: 'c', data: {} },
    ]);

    await new Promise<void>((resolve) => {
      const check = setInterval(async () => {
        const counts = await queue.getJobCounts();
        if (counts.failed === 3) {
          clearInterval(check);
          resolve();
        }
      }, 10);
    });

    expect(failedIds).toHaveLength(3);
  });

  it('partial batch processes immediately without timeout', async () => {
    queue = new TestQueue('batch-partial-no-timeout');
    const batchSizes: number[] = [];

    worker = new TestWorker(
      queue,
      async (jobs: TestJob[]) => {
        batchSizes.push(jobs.length);
        return jobs.map(() => 'ok');
      },
      { batch: { size: 10 } },
    );

    // Add only 2 jobs (less than batch size of 10)
    await queue.addBulk([
      { name: 'a', data: {} },
      { name: 'b', data: {} },
    ]);

    await new Promise<void>((resolve) => {
      const check = setInterval(async () => {
        const counts = await queue.getJobCounts();
        if (counts.completed === 2) {
          clearInterval(check);
          resolve();
        }
      }, 10);
    });

    // Without timeout, should process immediately with 2 jobs
    expect(batchSizes).toContain(2);
  });

  it('fails all jobs when processor returns wrong number of results', async () => {
    queue = new TestQueue('batch-mismatch');
    const failedIds: string[] = [];
    const failReasons: string[] = [];

    // Add jobs BEFORE creating worker so all 3 are waiting
    await queue.addBulk([
      { name: 'a', data: {} },
      { name: 'b', data: {} },
      { name: 'c', data: {} },
    ]);

    worker = new TestWorker(
      queue,
      async (_jobs: TestJob[]) => {
        // Return 1 result for 3 jobs - mismatch
        return ['only-one'] as any;
      },
      { batch: { size: 3 } },
    );

    worker.on('failed', (job: any, err: Error) => {
      failedIds.push(job.id);
      failReasons.push(err.message);
    });

    await new Promise<void>((resolve) => {
      const check = setInterval(async () => {
        const counts = await queue.getJobCounts();
        if (counts.failed === 3) {
          clearInterval(check);
          resolve();
        }
      }, 10);
    });

    expect(failedIds).toHaveLength(3);
    for (const reason of failReasons) {
      expect(reason).toContain('returned 1 results but batch had 3 jobs');
    }
  });

  it('retries failed jobs from batch', async () => {
    queue = new TestQueue('batch-retry');
    let callCount = 0;

    worker = new TestWorker(
      queue,
      async (_jobs: TestJob[]) => {
        callCount++;
        if (callCount === 1) {
          throw new Error('first attempt fails');
        }
        return _jobs.map(() => 'ok');
      },
      { batch: { size: 2 } },
    );

    await queue.addBulk([
      { name: 'a', data: {}, opts: { attempts: 2 } },
      { name: 'b', data: {}, opts: { attempts: 2 } },
    ]);

    await new Promise<void>((resolve) => {
      const check = setInterval(async () => {
        const counts = await queue.getJobCounts();
        if (counts.completed === 2) {
          clearInterval(check);
          resolve();
        }
      }, 10);
    });

    // First call failed, then retried and succeeded
    expect(callCount).toBeGreaterThanOrEqual(2);
    const completed = await queue.getJobs('completed');
    expect(completed).toHaveLength(2);
  });

  it('getMetrics returns time-series data for completed jobs', async () => {
    queue = new TestQueue('test-metrics');
    const worker = new TestWorker(queue, async () => 'done');

    await queue.add('j1', {});
    await queue.add('j2', {});
    await queue.add('j3', {});

    await waitFor(async () => (await queue.getJobCounts()).completed === 3, 5000);

    const metrics = await queue.getMetrics('completed');
    expect(metrics.count).toBe(3);
    expect(metrics.meta).toEqual({ resolution: 'minute' });
    expect(metrics.data.length).toBeGreaterThanOrEqual(1);

    const totalCount = metrics.data.reduce((sum: number, dp: any) => sum + dp.count, 0);
    expect(totalCount).toBe(3);

    for (const dp of metrics.data) {
      expect(dp.timestamp % 60000).toBe(0);
      expect(dp.avgDuration).toBeGreaterThanOrEqual(0);
    }

    await worker.close();
  });

  it('getMetrics returns time-series data for failed jobs', async () => {
    queue = new TestQueue('test-metrics-fail');
    const worker = new TestWorker(queue, async () => {
      throw new Error('fail');
    });

    await queue.add('f1', {});

    await waitFor(async () => (await queue.getJobCounts()).failed === 1, 5000);

    const metrics = await queue.getMetrics('failed');
    expect(metrics.count).toBe(1);
    expect(metrics.data.length).toBeGreaterThanOrEqual(1);
    expect(metrics.data[0].count).toBe(1);
    expect(metrics.data[0].avgDuration).toBeGreaterThanOrEqual(0);

    const completedMetrics = await queue.getMetrics('completed');
    expect(completedMetrics.count).toBe(0);
    expect(completedMetrics.data).toEqual([]);

    await worker.close();
  });

  it('getMetrics empty queue returns zero count and empty data', async () => {
    queue = new TestQueue('test-metrics-empty');

    const metrics = await queue.getMetrics('completed');
    expect(metrics.count).toBe(0);
    expect(metrics.data).toEqual([]);
    expect(metrics.meta).toEqual({ resolution: 'minute' });
  });

  it('getMetrics supports start/end slicing', async () => {
    queue = new TestQueue('test-metrics-slice');
    const worker = new TestWorker(queue, async () => 'ok');

    await queue.add('s1', {});
    await waitFor(async () => (await queue.getJobCounts()).completed === 1, 5000);

    const all = await queue.getMetrics('completed');
    expect(all.data.length).toBeGreaterThanOrEqual(1);

    const sliced = await queue.getMetrics('completed', { start: 0, end: 0 });
    expect(sliced.data.length).toBe(1);
    expect(sliced.count).toBe(all.count);

    await worker.close();
  });
});

describe('TestWorker - event payloads (T2)', () => {
  it('emits active, completed, failed events with correct payloads', async () => {
    const queue = new TestQueue('events-test');
    const activeEvents: any[] = [];
    const completedEvents: any[] = [];
    const failedEvents: any[] = [];

    let callCount = 0;
    const worker = new TestWorker(queue, async (job) => {
      callCount++;
      if (callCount === 2) throw new Error('forced failure');
      return 'result-' + job.id;
    });

    worker.on('active', (job: any) => activeEvents.push({ id: job.id, name: job.name }));
    worker.on('completed', (job: any, result: any) => completedEvents.push({ id: job.id, result }));
    worker.on('failed', (job: any, err: any) => failedEvents.push({ id: job.id, message: err.message }));

    await queue.add('job-a', { x: 1 });
    await queue.add('job-b', { x: 2 });
    await queue.add('job-c', { x: 3 });

    await waitFor(() => completedEvents.length + failedEvents.length === 3, 5000);

    expect(activeEvents.length).toBe(3);
    expect(completedEvents.length).toBe(2);
    expect(failedEvents.length).toBe(1);
    expect(completedEvents[0].result).toBe('result-1');
    expect(completedEvents[1].result).toBe('result-3');
    expect(failedEvents[0].message).toBe('forced failure');

    await worker.close();
    await queue.close();
  });

  it('processes 1000 jobs without performance degradation', async () => {
    const queue = new TestQueue('perf-test');
    const results: string[] = [];

    const worker = new TestWorker(
      queue,
      async (job) => {
        results.push(job.id);
        return 'ok';
      },
      { concurrency: 10 },
    );

    const start = Date.now();
    for (let i = 0; i < 1000; i++) {
      await queue.add('task', { i });
    }

    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (results.length >= 1000) {
          clearInterval(check);
          resolve();
        }
      }, 50);
    });

    const elapsed = Date.now() - start;
    expect(results.length).toBe(1000);
    // Should process 1000 jobs in well under 10 seconds (generous bound for CI)
    expect(elapsed).toBeLessThan(10000);

    await worker.close();
    await queue.close();
  }, 15000);
});

describe('TestQueue.add validation parity (T11)', () => {
  let queue: TestQueue;

  afterEach(async () => {
    if (queue) await queue.close();
  });

  it('rejects the same invalid options as Queue.add', async () => {
    queue = new TestQueue('validation-parity');
    await expect(queue.add('j', {}, { priority: 2049 })).rejects.toThrow('Priority must be <= 2048');
    await expect(queue.add('j', {}, { ttl: -1 })).rejects.toThrow('ttl must be a non-negative finite number');
    await expect(queue.add('j', {}, { lockDuration: 10 })).rejects.toThrow('lockDuration must be a finite number');
    await expect(queue.add('j', {}, { cost: -1 })).rejects.toThrow('cost must be a non-negative finite number');
    await expect(queue.add('j', {}, { lifo: true, ordering: { key: 'k' } })).rejects.toThrow(
      'lifo and ordering.key cannot be used together',
    );
    await expect(queue.add('j', {}, { ordering: { key: '__' } })).rejects.toThrow('reserved');
    await expect(
      queue.add('j', {}, { ordering: { key: 'k', tokenBucket: { capacity: 0, refillRate: 1 } } }),
    ).rejects.toThrow('tokenBucket.capacity must be a positive finite number');
    await expect(queue.add('j', 'x'.repeat(MAX_JOB_DATA_SIZE + 1))).rejects.toThrow('Job data exceeds maximum size');
    await expect(queue.addBulk([{ name: 'j', data: {}, opts: { ttl: Number.NaN } }])).rejects.toThrow(
      'ttl must be a non-negative finite number',
    );
    expect(queue.jobs.size).toBe(0);
  });
});

describe('TestQueue.upsertJobScheduler mode switch', () => {
  it('switching every to repeatAfterComplete does not fire before the old nextRun', async () => {
    const queue = new TestQueue('switch-mode');
    await queue.upsertJobScheduler('s', { every: 60_000 }, { name: 'j' });
    const before = await queue.getJobScheduler('s');
    await queue.upsertJobScheduler('s', { repeatAfterComplete: 500 }, { name: 'j' });
    const after = await queue.getJobScheduler('s');
    expect(after!.repeatAfterComplete).toBe(500);
    expect(after!.nextRun).toBe(before!.nextRun);
    await queue.close();
  });
});

describe('TestQueue.upsertJobScheduler template validation parity', () => {
  it('rejects the template options Queue.upsertJobScheduler rejects', async () => {
    const queue = new TestQueue('tmpl-validate');
    await expect(
      queue.upsertJobScheduler('a', { every: 1000 }, { name: 'j', opts: { jobId: 'fixed' } as any }),
    ).rejects.toThrow('Scheduler template: jobId is not supported');
    await expect(
      queue.upsertJobScheduler('b', { every: 1000 }, { name: 'j', opts: { lifo: true, ordering: { key: 'g' } } }),
    ).rejects.toThrow('Scheduler template: lifo and ordering.key cannot be used together');
    await expect(queue.upsertJobScheduler('c', { every: 1000 }, { name: 'j', opts: { cost: -1 } })).rejects.toThrow(
      'Scheduler template: cost must be a non-negative finite number',
    );
    await expect(
      queue.upsertJobScheduler('d', { every: 1000 }, { name: 'j', opts: { priority: 5000 } }),
    ).rejects.toThrow('Scheduler template: Priority must be <= 2048');
    await expect(
      queue.upsertJobScheduler('e', { every: 1000 }, { name: 'j', data: 'a'.repeat(MAX_JOB_DATA_SIZE + 1) }),
    ).rejects.toThrow('Scheduler template: Job data exceeds maximum size');
    for (const [opts, field] of [
      [{ delay: 1000 }, 'delay'],
      [{ deduplication: { id: 'd' } }, 'deduplication'],
      [{ parent: { queue: 'p', id: '1' } }, 'parent'],
    ] as const) {
      await expect(queue.upsertJobScheduler('f', { every: 1000 }, { name: 'j', opts: opts as any })).rejects.toThrow(
        `Scheduler template: ${field} is not supported`,
      );
    }
    expect(await queue.getRepeatableJobs()).toEqual([]);
    await queue.close();
  });
});

describe('TestJob.updateData / updateProgress persistence (T11)', () => {
  it('persists updateData and updateProgress to the stored job', async () => {
    const queue = new TestQueue<{ v: number }>('persist-updates');
    const worker = new TestWorker(queue, async (job) => {
      await job.updateProgress(50);
      await job.updateData({ v: 2 });
      await job.updateProgress({ step: 'done' });
      return 'ok';
    });
    const added = await queue.add('j', { v: 1 });
    await waitFor(async () => (await queue.getJob(added!.id))?.returnvalue === 'ok', 2000, 5);

    const stored = await queue.getJob(added!.id);
    expect(stored!.data).toEqual({ v: 2 });
    expect(stored!.progress).toEqual({ step: 'done' });

    const outside = await queue.getJob(added!.id);
    await outside!.updateData({ v: 3 });
    expect((await queue.getJob(added!.id))!.data).toEqual({ v: 3 });
    await expect(outside!.updateData({ v: 'x'.repeat(MAX_JOB_DATA_SIZE) } as any)).rejects.toThrow(
      'Job data exceeds maximum size',
    );
    await expect(outside!.updateProgress({ big: 'x'.repeat(MAX_JOB_DATA_SIZE) })).rejects.toThrow(
      'Progress data exceeds maximum size',
    );

    await worker.close();
    await queue.close();
  });
});

describe('TestWorker ordering and retention parity (T9)', () => {
  it('dispatches priority (lowest number first), then LIFO, then FIFO', async () => {
    const queue = new TestQueue('ordering-parity');
    await queue.add('fifo-a', {});
    await queue.add('p5', {}, { priority: 5 });
    await queue.add('lifo-x', {}, { lifo: true });
    await queue.add('p1-a', {}, { priority: 1 });
    await queue.add('fifo-b', {});
    await queue.add('lifo-y', {}, { lifo: true });
    await queue.add('p1-b', {}, { priority: 1 });

    const order: string[] = [];
    const worker = new TestWorker(queue, async (job) => {
      order.push(job.name);
      return 'ok';
    });
    await waitFor(() => order.length === 7, 2000, 5);
    expect(order).toEqual(['p1-a', 'p1-b', 'p5', 'lifo-y', 'lifo-x', 'fifo-a', 'fifo-b']);

    await worker.close();
    await queue.close();
  });

  it('removeOnComplete: true deletes the job before the completed event', async () => {
    const queue = new TestQueue('roc-true');
    const seen: (TestJob | null)[] = [];
    const worker = new TestWorker(queue, async () => 'ok');
    worker.on('completed', async (job: TestJob) => {
      seen.push(await queue.getJob(job.id));
    });
    await queue.add('keep', {});
    await queue.add('drop', {}, { removeOnComplete: true });
    await waitFor(() => seen.length === 2, 2000, 5);
    expect(seen[0]).not.toBeNull();
    expect(seen[1]).toBeNull();
    expect((await queue.getJobCounts()).completed).toBe(1);

    await worker.close();
    await queue.close();
  });

  it('removeOnComplete: number keeps only the newest N completed jobs', async () => {
    const queue = new TestQueue('roc-count');
    let done = 0;
    const worker = new TestWorker(queue, async () => 'ok');
    worker.on('completed', () => done++);
    for (let i = 0; i < 5; i++) {
      await queue.add(`j${i}`, {}, { removeOnComplete: 2 });
      await waitFor(() => done === i + 1, 2000, 1);
      await new Promise((r) => setTimeout(r, 2));
    }
    const completed = await queue.getJobs('completed');
    expect(completed.map((j) => j.name).sort()).toEqual(['j3', 'j4']);

    await worker.close();
    await queue.close();
  });

  it('removeOnComplete: { age } removes completed jobs older than age seconds', async () => {
    const queue = new TestQueue('roc-age');
    let done = 0;
    const worker = new TestWorker(queue, async () => 'ok');
    worker.on('completed', () => done++);
    const old = await queue.add('old', {});
    await waitFor(() => done === 1, 2000, 1);
    queue.jobs.get(old!.id)!.finishedOn = Date.now() - 10_000;
    await queue.add('new', {}, { removeOnComplete: { age: 5, count: 0 } });
    await waitFor(() => done === 2, 2000, 1);
    expect(await queue.getJob(old!.id)).toBeNull();
    expect((await queue.getJobs('completed')).map((j) => j.name)).toEqual(['new']);

    await worker.close();
    await queue.close();
  });

  it('removeOnFail applies on terminal failure only', async () => {
    const queue = new TestQueue('rof');
    let terminal = 0;
    queue.on('failed', () => terminal++);
    const worker = new TestWorker(queue, async () => {
      throw new Error('boom');
    });
    const kept = await queue.add('kept', {});
    const dropped = await queue.add('dropped', {}, { removeOnFail: true });
    await waitFor(() => terminal === 2, 2000, 5);
    expect(await queue.getJob(kept!.id)).not.toBeNull();
    expect(await queue.getJob(dropped!.id)).toBeNull();

    await worker.close();
    await queue.close();
  });

  it('batch mode applies removeOnComplete', async () => {
    const queue = new TestQueue('roc-batch');
    let done = 0;
    const worker = new TestWorker(queue, async (jobs: TestJob[]) => jobs.map(() => 'ok'), { batch: { size: 2 } });
    worker.on('completed', () => done++);
    await queue.addBulk([
      { name: 'a', data: {}, opts: { removeOnComplete: true } },
      { name: 'b', data: {} },
    ]);
    await waitFor(() => done === 2, 2000, 5);
    expect((await queue.getJobs('completed')).map((j) => j.name)).toEqual(['b']);

    await worker.close();
    await queue.close();
  });
});

describe('TestWorker retry parity (T8)', () => {
  it('emits worker failed on every attempt and parks retries in delayed with failedReason', async () => {
    const queue = new TestQueue('retry-parity');
    const workerFailed: string[] = [];
    const queueFailed: string[] = [];
    const retrying: string[] = [];
    queue.on('failed', (_job: TestJob, err: Error) => queueFailed.push(err.message));
    queue.on('retrying', (_job: TestJob, err: Error) => retrying.push(err.message));
    let calls = 0;
    const worker = new TestWorker(queue, async () => {
      calls++;
      throw new Error(`fail-${calls}`);
    });
    worker.on('failed', (job: TestJob, err: Error) => {
      workerFailed.push(err.message);
      expect(job.failedReason).toBe(err.message);
    });

    const job = await queue.add('flaky', {}, { attempts: 3, backoff: { type: 'fixed', delay: 150 } });
    await waitFor(() => workerFailed.length === 1, 2000, 2);

    const afterFirst = await queue.getJob(job!.id);
    expect(afterFirst!.failedReason).toBe('fail-1');
    expect(afterFirst!.attemptsMade).toBe(1);
    expect((await queue.getJobCounts()).delayed).toBe(1);
    expect((await queue.getJobs('delayed')).map((j) => j.id)).toEqual([job!.id]);

    // Backoff holds the job in delayed until the delay elapses.
    await new Promise((r) => setTimeout(r, 60));
    expect(calls).toBe(1);

    await waitFor(() => workerFailed.length === 3, 3000, 5);
    expect(workerFailed).toEqual(['fail-1', 'fail-2', 'fail-3']);
    expect(retrying).toEqual(['fail-1', 'fail-2']);
    expect(queueFailed).toEqual(['fail-3']);
    const final = await queue.getJob(job!.id);
    expect(final!.failedReason).toBe('fail-3');
    expect((await queue.getJobCounts()).failed).toBe(1);

    await worker.close();
    await queue.close();
  });

  it('uses exponential backoff and custom backoffStrategies', async () => {
    const queue = new TestQueue('retry-backoff');
    const delays: number[] = [];
    const worker = new TestWorker(
      queue,
      async () => {
        throw new Error('x');
      },
      { backoffStrategies: { custom: (attemptsMade) => attemptsMade * 7 } },
    );
    const origPark = queue.parkDelayed.bind(queue);
    queue.parkDelayed = (record, delay) => {
      delays.push(delay);
      origPark(record, delay);
    };
    let failed = 0;
    worker.on('failed', () => failed++);
    await queue.add('exp', {}, { attempts: 3, backoff: { type: 'exponential', delay: 10 } });
    await waitFor(() => failed === 3, 2000, 2);
    await queue.add('custom', {}, { attempts: 3, backoff: { type: 'custom', delay: 0 } });
    await waitFor(() => failed === 6, 2000, 2);
    expect(delays).toEqual([10, 20, 7, 14]);

    await worker.close();
    await queue.close();
  });

  it('drain(true) removes jobs parked in delayed for retry', async () => {
    const queue = new TestQueue('retry-drain');
    let calls = 0;
    const worker = new TestWorker(queue, async () => {
      calls++;
      throw new Error('x');
    });
    await queue.add('j', {}, { attempts: 2, backoff: { type: 'fixed', delay: 50 } });
    await waitFor(() => calls === 1, 2000, 2);
    await queue.drain(true);
    await new Promise((r) => setTimeout(r, 100));
    expect(calls).toBe(1);
    expect(queue.jobs.size).toBe(0);

    await worker.close();
    await queue.close();
  });
});

describe('TestQueue deduplication parity (T10)', () => {
  let queue: TestQueue;

  afterEach(async () => {
    if (queue) await queue.close();
  });

  it('dedups without the dedup flag, like Queue.add', async () => {
    queue = new TestQueue('dedup-default');
    const a = await queue.add('t', {}, { deduplication: { id: 'd' } });
    const b = await queue.add('t', {}, { deduplication: { id: 'd' } });
    expect(a).not.toBeNull();
    expect(b).toBeNull();
  });

  it('dedup: false keeps the legacy opt-out', async () => {
    queue = new TestQueue('dedup-off', { dedup: false });
    expect(await queue.add('t', {}, { deduplication: { id: 'd' } })).not.toBeNull();
    expect(await queue.add('t', {}, { deduplication: { id: 'd' } })).not.toBeNull();
  });

  it('simple mode releases the id once the job completes, fails, or is removed', async () => {
    queue = new TestQueue('dedup-simple');
    let done = 0;
    const worker = new TestWorker(queue, async (job) => {
      if (job.data.fail) throw new Error('boom');
      return 'ok';
    });
    worker.on('completed', () => done++);
    worker.on('failed', () => done++);

    expect(await queue.add('t', {}, { deduplication: { id: 'd', mode: 'simple' } })).not.toBeNull();
    await waitFor(() => done === 1, 2000, 2);
    expect(await queue.add('t', { fail: true }, { deduplication: { id: 'd', mode: 'simple' } })).not.toBeNull();
    await waitFor(() => done === 2, 2000, 2);
    const third = await queue.add('t', {}, { deduplication: { id: 'd', mode: 'simple' }, removeOnComplete: true });
    expect(third).not.toBeNull();
    await waitFor(() => done === 3, 2000, 2);
    expect(await queue.getJob(third!.id)).toBeNull();
    expect(await queue.add('t', {}, { deduplication: { id: 'd', mode: 'simple' } })).not.toBeNull();

    await worker.close();
  });

  it('throttle mode skips inside ttl and accepts after it, regardless of job state', async () => {
    queue = new TestQueue('dedup-throttle');
    const opts = { deduplication: { id: 'd', mode: 'throttle' as const, ttl: 50 } };
    expect(await queue.add('t', { v: 1 }, opts)).not.toBeNull();
    expect(await queue.add('t', { v: 2 }, opts)).toBeNull();
    await new Promise((r) => setTimeout(r, 60));
    expect(await queue.add('t', { v: 3 }, opts)).not.toBeNull();

    const noTtl = { deduplication: { id: 'n', mode: 'throttle' as const } };
    expect(await queue.add('t', {}, noTtl)).not.toBeNull();
    expect(await queue.add('t', {}, noTtl)).not.toBeNull();
  });

  it('debounce mode skips while waiting and replaces a delayed job', async () => {
    queue = new TestQueue('dedup-debounce');
    const opts = { deduplication: { id: 'd', mode: 'debounce' as const } };
    const first = await queue.add('t', { v: 1 }, opts);
    expect(await queue.add('t', { v: 2 }, opts)).toBeNull();

    const removed: string[] = [];
    queue.on('removed', (id: string) => removed.push(id));
    await first!.changeDelay(60_000);
    const replacement = await queue.add('t', { v: 3 }, opts);
    expect(replacement).not.toBeNull();
    expect(await queue.getJob(first!.id)).toBeNull();
    expect(removed).toEqual([first!.id]);
  });

  it('debounce mode replaces a job added with delay', async () => {
    queue = new TestQueue('dedup-debounce-delay');
    const opts = { deduplication: { id: 'd', mode: 'debounce' as const }, delay: 60_000 };
    const first = await queue.add('t', { v: 1 }, opts);
    const second = await queue.add('t', { v: 2 }, opts);
    expect(second).not.toBeNull();
    expect(await queue.getJob(first!.id)).toBeNull();
    expect((await queue.getJobCounts()).delayed).toBe(1);
  });

  it('a duplicate custom jobId does not claim the dedup id', async () => {
    queue = new TestQueue('dedup-custom-id');
    await queue.add('t', {}, { jobId: 'x' });
    expect(await queue.add('t', {}, { jobId: 'x', deduplication: { id: 'd' } })).toBeNull();
    expect(await queue.add('t', {}, { deduplication: { id: 'd' } })).not.toBeNull();
  });
});

describe('TestWorker failure paths (coverage)', () => {
  it('fails a job whose flow budget is already exceeded with onExceeded fail', async () => {
    const queue = new TestQueue('budget-fail-parity');
    queue.setBudget('flow-1', { maxTotalTokens: 1, onExceeded: 'fail' });
    queue.recordBudgetUsage('flow-1', { input: 5 }, {}, 5, 0);
    await queue.pause();
    const job = await queue.add('capped', {});
    queue.jobs.get(job!.id)!.budgetKey = 'flow-1';
    const failed: string[] = [];
    const worker = new TestWorker(queue, async () => 'never');
    worker.on('failed', (_job: TestJob, err: Error) => failed.push(err.message));
    await queue.resume();
    await waitFor(() => failed.length === 1, 2000, 2);
    expect(failed).toEqual(['Budget exceeded']);
    const final = await queue.getJob(job!.id);
    expect(final!.failedReason).toBe('Budget exceeded');
    await worker.close();
    await queue.close();
  });

  it('advances the fallback chain on each retry', async () => {
    const queue = new TestQueue('fallback-retry-parity');
    const seen: (string | undefined)[] = [];
    const worker = new TestWorker(queue, async (job: TestJob) => {
      seen.push(job.currentFallback?.model);
      if (seen.length < 3) throw new Error('retry');
      return 'ok';
    });
    await queue.add(
      'fb',
      {},
      { attempts: 3, backoff: { type: 'fixed', delay: 1 }, fallbacks: [{ model: 'a' }, { model: 'b' }] },
    );
    await waitFor(() => seen.length === 3, 2000, 2);
    expect(seen).toEqual([undefined, 'a', 'b']);
    await worker.close();
    await queue.close();
  });

  it('rejects the same invalid options as Queue.add', async () => {
    const queue = new TestQueue('validation-coverage');
    await expect(queue.add('x', {}, { ordering: { key: 'k'.repeat(257) } })).rejects.toThrow(/Ordering key exceeds/);
    await expect(queue.add('x', {}, { ordering: { key: '__' } })).rejects.toThrow(/reserved/);
    await expect(
      queue.add('x', {}, { ordering: { key: 'k', tokenBucket: { capacity: 1, refillRate: 0 } } }),
    ).rejects.toThrow(/refillRate/);
    await expect(queue.add('x', 'a'.repeat(MAX_JOB_DATA_SIZE + 1))).rejects.toThrow(/exceeds maximum size/);
    await queue.close();
  });
});

describe('TestWorker with a job removed or obliterated while active', () => {
  let queue: TestQueue;
  let worker: TestWorker | undefined;

  afterEach(async () => {
    if (worker) await worker.close();
    worker = undefined;
    if (queue) await queue.close();
  });

  const settle = () => new Promise<void>((resolve) => setTimeout(resolve, 20));

  /** Holds every job open until `release(tag)` is called, keyed by the tag of the first job in the call. */
  function gate() {
    const waiters = new Map<string, () => void>();
    const hold = (tag: string) => new Promise<void>((resolve) => waiters.set(tag, resolve));
    return { hold, release: (tag: string) => waiters.get(tag)?.() };
  }

  /** Collects the 'completed' and 'failed' events of the worker and of the queue as `<type> <tag>`. */
  function collect(w: TestWorker, q: TestQueue) {
    const events = { worker: [] as string[], queue: [] as string[] };
    for (const type of ['completed', 'failed'] as const) {
      w.on(type, (job) => events.worker.push(`${type} ${job.data.tag}`));
      q.on(type, (job) => events.queue.push(`${type} ${job.data.tag}`));
    }
    return events;
  }

  it.each([
    ['completes', { removeOnComplete: true }, {}, 'completed a'],
    ['fails', { removeOnFail: true }, { fail: true }, 'failed a'],
  ])(
    'a job that %s after the queue was obliterated only reaches its worker and leaves the job that reuses its id alone',
    async (_label, opts, extra, workerEvent) => {
      queue = new TestQueue('obliterate-inflight-terminal');
      const { hold, release } = gate();
      worker = new TestWorker(queue, async (job) => {
        await hold(job.data.tag);
        if (job.data.fail) throw new Error(`failed ${job.data.tag}`);
        return job.data.tag;
      });
      const events = collect(worker, queue);

      const a = await queue.add('job', { tag: 'a', ...extra }, opts);
      await waitFor(() => worker!.getActiveCount() === 1, 2000, 2);
      await queue.obliterate({ force: true });
      const b = await queue.add('job', { tag: 'b' });
      // obliterate restarts the id counter, as the key wipe does in production
      expect(b!.id).toBe(a!.id);

      release('a');
      await settle();
      // like the worker after glidemq_completeAndFetchNext / glidemq_fail skipped the missing hash
      expect(events.worker).toEqual([workerEvent]);
      expect(events.queue).toEqual([]);
      expect((await queue.getMetrics('completed')).data).toEqual([]);
      expect((await queue.getMetrics('failed')).data).toEqual([]);
      // b is neither deleted by the retention of a nor dequeued, and is dispatched next
      expect(queue.jobs.get(b!.id)?.state).toBe('active');
      expect(worker.getActiveCount()).toBe(1);

      release('b');
      expect(await b!.waitUntilFinished(5, 2000)).toBe('completed');
      expect(events.worker).toEqual([workerEvent, 'completed b']);
      expect(events.queue).toEqual(['completed b']);
      expect(worker.getActiveCount()).toBe(0);
    },
  );

  it('a retryable failure after the queue was obliterated does not park the job that reuses its id', async () => {
    queue = new TestQueue('obliterate-inflight-retry');
    const { hold, release } = gate();
    worker = new TestWorker(queue, async (job) => {
      await hold(job.data.tag);
      if (job.data.fail) throw new Error(`failed ${job.data.tag}`);
      return job.data.tag;
    });
    const events = collect(worker, queue);
    queue.on('retrying', () => events.queue.push('retrying'));

    const a = await queue.add('job', { tag: 'a', fail: true }, { attempts: 3, backoff: { type: 'fixed', delay: 0 } });
    await waitFor(() => worker!.getActiveCount() === 1, 2000, 2);
    await queue.obliterate({ force: true });
    const b = await queue.add('job', { tag: 'b' });
    expect(b!.id).toBe(a!.id);

    release('a');
    await settle();
    // no retry is scheduled, so b stays dispatchable and is not dequeued by the retry of a
    expect(events.worker).toEqual(['failed a']);
    expect(events.queue).toEqual([]);
    expect(queue.jobs.get(b!.id)?.state).toBe('active');
    expect(worker.getActiveCount()).toBe(1);

    release('b');
    expect(await b!.waitUntilFinished(5, 2000)).toBe('completed');
    expect(worker.getActiveCount()).toBe(0);
  });

  it.each([
    ['returns results', (jobs: TestJob[]) => jobs.map((j) => j.data.tag), ['completed a1', 'completed a2']],
    [
      'throws a BatchError',
      (jobs: TestJob[]) => {
        throw new BatchError(jobs.map((j, i) => (i === 0 ? new Error('bad') : j.data.tag)));
      },
      ['failed a1', 'completed a2'],
    ],
    [
      'throws',
      () => {
        throw new Error('boom');
      },
      ['failed a1', 'failed a2'],
    ],
  ])(
    'a batch that %s after the queue was obliterated only reaches its worker',
    async (_label, outcome, workerEvents) => {
      queue = new TestQueue('obliterate-inflight-batch');
      const { hold, release } = gate();
      worker = new TestWorker(
        queue,
        async (jobs: TestJob[]) => {
          await hold(jobs[0].data.tag);
          return outcome(jobs);
        },
        { batch: { size: 2 } },
      );
      const events = collect(worker, queue);

      const retention = { removeOnComplete: true, removeOnFail: true };
      await queue.pause();
      await queue.add('job', { tag: 'a1' }, retention);
      await queue.add('job', { tag: 'a2' }, retention);
      await queue.resume();
      await waitFor(() => worker!.getActiveCount() === 2, 2000, 2);
      await queue.obliterate({ force: true });
      await queue.add('job', { tag: 'b1' });
      await queue.add('job', { tag: 'b2' });

      release('a1');
      await settle();
      expect(events.worker).toEqual(workerEvents);
      expect(events.queue).toEqual([]);
      expect((await queue.getMetrics('completed')).data).toEqual([]);
      expect((await queue.getMetrics('failed')).data).toEqual([]);
      expect([...queue.jobs.values()].map((r) => `${r.data.tag}:${r.state}`)).toEqual(['b1:active', 'b2:active']);
      expect(worker.getActiveCount()).toBe(2);

      release('b1');
      await waitFor(() => worker!.getActiveCount() === 0, 2000, 2);
      expect(events.worker.slice(workerEvents.length)).toHaveLength(2);
      expect(events.queue).toHaveLength(2);
      expect([...events.worker.slice(workerEvents.length), ...events.queue].every((e) => /b[12]$/.test(e))).toBe(true);
    },
  );

  it('a job removed while active only reaches its worker', async () => {
    queue = new TestQueue('removed-inflight');
    const { hold, release } = gate();
    worker = new TestWorker(queue, async (job) => {
      await hold(job.data.tag);
      return job.data.tag;
    });
    const events = collect(worker, queue);

    const a = await queue.add('job', { tag: 'a' });
    await waitFor(() => worker!.getActiveCount() === 1, 2000, 2);
    await a!.remove();
    release('a');
    await settle();
    expect(events.worker).toEqual(['completed a']);
    expect(events.queue).toEqual([]);
    expect((await queue.getMetrics('completed')).data).toEqual([]);
    expect(await queue.getJob(a!.id)).toBeNull();
  });

  it('a job removed while active still charges its usage to the flow budget', async () => {
    queue = new TestQueue('removed-inflight-budget');
    queue.setBudget('flow-r', { maxTotalTokens: 100, onExceeded: 'fail' });
    const { hold, release } = gate();
    worker = new TestWorker(queue, async (job) => {
      await job.reportUsage({ tokens: { input: 7 }, costs: { usd: 0.5 }, totalTokens: 7, totalCost: 0.5 });
      await hold(job.data.tag);
      return 'x';
    });
    const exceeded: string[] = [];
    worker.on('budget-exceeded', (_job, id) => exceeded.push(String(id)));

    await queue.pause();
    const a = await queue.add('job', { tag: 'a' });
    queue.jobs.get(a!.id)!.budgetKey = 'flow-r';
    await queue.resume();
    await waitFor(() => worker!.getActiveCount() === 1, 2000, 2);
    await a!.remove();
    release('a');
    await settle();
    const budget = queue.budgets.get('flow-r')!;
    expect(budget.usedTokens).toBe(7);
    expect(budget.usedCost).toBe(0.5);
    expect(exceeded).toEqual([]);
  });

  it('a removed job whose usage exceeds the budget emits budget-exceeded on its worker', async () => {
    queue = new TestQueue('removed-inflight-budget-exceeded');
    queue.setBudget('flow-e', { maxTotalTokens: 5, onExceeded: 'fail' });
    const { hold, release } = gate();
    worker = new TestWorker(queue, async (job) => {
      await job.reportUsage({ tokens: { input: 9 }, totalTokens: 9 });
      await hold(job.data.tag);
      return 'x';
    });
    const exceeded: string[] = [];
    worker.on('budget-exceeded', (_job, id) => exceeded.push(String(id)));

    await queue.pause();
    const a = await queue.add('job', { tag: 'a' });
    queue.jobs.get(a!.id)!.budgetKey = 'flow-e';
    await queue.resume();
    await waitFor(() => worker!.getActiveCount() === 1, 2000, 2);
    await a!.remove();
    release('a');
    await settle();
    expect(exceeded).toEqual([a!.id]);
    expect(queue.budgets.get('flow-e')!.exceeded).toBe(true);
  });

  it('a job removed while active still counts toward the worker TPM window', async () => {
    queue = new TestQueue('removed-inflight-tpm');
    const { hold, release } = gate();
    worker = new TestWorker(
      queue,
      async (job) => {
        await job.reportTokens(40);
        await hold(job.data.tag);
        return 'x';
      },
      { tokenLimiter: { maxTokens: 1000, duration: 60_000 } },
    );

    const a = await queue.add('job', { tag: 'a' });
    await waitFor(() => worker!.getActiveCount() === 1, 2000, 2);
    await a!.remove();
    release('a');
    await settle();
    expect((worker as unknown as { tpmLocalCounter: number }).tpmLocalCounter).toBe(40);
  });

  it.each([
    ['only totalTokens', { totalTokens: 3 }, 3, 0],
    ['only totalCost', { totalCost: 0.25 }, 0, 0.25],
    ['only token categories', { tokens: { input: 4 } }, 4, 0],
    ['only cost categories', { costs: { usd: 0.1 } }, 0, 0],
    ['an empty usage', {}, 0, 0],
  ])('charges a removed job with %s without requiring the other usage fields', async (_label, usage, tokens, cost) => {
    queue = new TestQueue('removed-inflight-partial-usage');
    queue.setBudget('flow-p', { maxTotalTokens: 100, onExceeded: 'fail' });
    const { hold, release } = gate();
    worker = new TestWorker(
      queue,
      async (job) => {
        job.usage = usage as TestJob['usage'];
        await hold(job.data.tag);
        return 'x';
      },
      { tokenLimiter: { maxTokens: 1000, duration: 60_000 } },
    );

    await queue.pause();
    const a = await queue.add('job', { tag: 'a' });
    queue.jobs.get(a!.id)!.budgetKey = 'flow-p';
    await queue.resume();
    await waitFor(() => worker!.getActiveCount() === 1, 2000, 2);
    await a!.remove();
    release('a');
    await settle();
    const budget = queue.budgets.get('flow-p')!;
    expect(budget.usedTokens).toBe(tokens);
    expect(budget.usedCost).toBe(cost);
    expect((worker as unknown as { tpmLocalCounter: number }).tpmLocalCounter).toBe(
      (usage as { totalTokens?: number }).totalTokens ?? 0,
    );
  });

  describe('return value the serializer rejects', () => {
    const circular = () => {
      const value: Record<string, unknown> = {};
      value.self = value;
      return value;
    };

    it.each([
      ['a job removed while active', true],
      ['a job still in the store', false],
    ])('fails %s with the worker message instead of completing it', async (_label, remove) => {
      queue = new TestQueue('serialize-inflight');
      const { hold, release } = gate();
      worker = new TestWorker(queue, async (job) => {
        await hold(job.data.tag);
        return circular();
      });
      const events = collect(worker, queue);
      const reasons: string[] = [];
      worker.on('failed', (job) => reasons.push(job.failedReason ?? ''));

      const a = await queue.add('job', { tag: 'a' });
      await waitFor(() => worker!.getActiveCount() === 1, 2000, 2);
      if (remove) await a!.remove();
      release('a');
      await settle();
      expect(events.worker).toEqual(['failed a']);
      expect(reasons).toHaveLength(1);
      expect(reasons[0]).toMatch(/^Serializer failed on return value: .*circular/i);
      // a removed job leaves no queue event, a live one fails terminally
      expect(events.queue).toEqual(remove ? [] : ['failed a']);
      expect(worker.getActiveCount()).toBe(0);
    });

    it('reports a serializer that throws something other than an Error', async () => {
      const serializer = {
        serialize: (value: unknown) => {
          if (value && (value as { bad?: boolean }).bad) throw 'refused';
          return JSON.stringify(value);
        },
        deserialize: (raw: string) => JSON.parse(raw),
      };
      queue = new TestQueue('serialize-inflight-string', { serializer });
      const { hold, release } = gate();
      worker = new TestWorker(queue, async (job) => {
        await hold(job.data.tag);
        return { bad: true };
      });
      const reasons: string[] = [];
      worker.on('failed', (job) => reasons.push(job.failedReason ?? ''));

      const a = await queue.add('job', { tag: 'a' });
      await waitFor(() => worker!.getActiveCount() === 1, 2000, 2);
      await a!.remove();
      release('a');
      await settle();
      expect(reasons).toEqual(['Serializer failed on return value: refused']);
    });

    it.each([
      ['returns results', (jobs: TestJob[]) => jobs.map((j) => (j.data.tag === 'a1' ? circular() : j.data.tag))],
      [
        'throws a BatchError',
        (jobs: TestJob[]) => {
          throw new BatchError(jobs.map((j) => (j.data.tag === 'a1' ? circular() : j.data.tag)));
        },
      ],
    ])('a batch that %s fails only the removed job whose value cannot be serialized', async (_label, outcome) => {
      queue = new TestQueue('serialize-inflight-batch');
      const { hold, release } = gate();
      worker = new TestWorker(
        queue,
        async (jobs: TestJob[]) => {
          await hold(jobs[0].data.tag);
          return outcome(jobs);
        },
        { batch: { size: 2 } },
      );
      const events = collect(worker, queue);

      await queue.pause();
      await queue.add('job', { tag: 'a1' });
      await queue.add('job', { tag: 'a2' });
      await queue.resume();
      await waitFor(() => worker!.getActiveCount() === 2, 2000, 2);
      const [a1] = await queue.getJobs('active');
      await a1.remove();
      release('a1');
      await settle();
      // a1 is gone and cannot be serialized: worker-local failure only. a2 still completes.
      expect(events.worker.sort()).toEqual(['completed a2', 'failed a1']);
      expect(events.queue).toEqual(['completed a2']);
    });
  });

  it('keeps the worker paused for a RateLimitError thrown after the job was removed', async () => {
    queue = new TestQueue('removed-inflight-ratelimit');
    const { hold, release } = gate();
    worker = new TestWorker(queue, async (job) => {
      if (job.data.limit) {
        await hold(job.data.tag);
        const err = new TestWorker.RateLimitError();
        err.delayMs = 300;
        throw err;
      }
      return job.data.tag;
    });
    const events = collect(worker, queue);

    const a = await queue.add('job', { tag: 'a', limit: true });
    const b = await queue.add('job', { tag: 'b' });
    await waitFor(() => worker!.getActiveCount() === 1, 2000, 2);
    await a!.remove();
    release('a');
    await settle();
    // like BaseWorker, the pause outlives the removed job: b is not dispatched inside the window
    expect(queue.jobs.get(b!.id)?.state).toBe('waiting');
    expect(worker.getActiveCount()).toBe(0);
    expect(events.worker).toEqual([]);

    expect(await b!.waitUntilFinished(5, 3000)).toBe('completed');
    expect(events.worker).toEqual(['completed b']);
  });

  describe('a job dropped while active that then pauses itself', () => {
    const wait = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
    const drops: Array<[string, (q: TestQueue, job: TestJob) => Promise<unknown>]> = [
      ['removed', (_q, job) => job.remove()],
      ['obliterated', (q) => q.obliterate({ force: true })],
    ];

    it.each(drops)('is not suspended again when it was %s', async (_label, drop) => {
      queue = new TestQueue('suspend-inflight');
      const { hold, release } = gate();
      worker = new TestWorker(queue, async (job) => {
        await hold(job.data.tag);
        await job.suspend({ reason: 'wait', timeout: 30 });
      });
      const suspended: string[] = [];
      queue.on('suspended', (job) => suspended.push(job.data.tag));

      const a = await queue.add('job', { tag: 'a' });
      await waitFor(() => worker!.getActiveCount() === 1, 2000, 2);
      await drop(queue, a!);
      release('a');
      await wait(80);
      // no suspension was recorded for a job the store no longer holds
      expect(suspended).toEqual([]);
      expect(queue.jobs.size).toBe(0);
      expect(worker.getActiveCount()).toBe(0);
    });

    it('does not park a record of an obliterated queue over the job that reuses its id', async () => {
      queue = new TestQueue('delayed-inflight');
      const { hold, release } = gate();
      let runs = 0;
      worker = new TestWorker(queue, async (job) => {
        if (job.data.tag === 'b') return job.data.tag;
        runs++;
        await hold(job.data.tag);
        await job.moveToDelayed(Date.now() + 20);
      });

      const a = await queue.add('job', { tag: 'a' });
      await waitFor(() => worker!.getActiveCount() === 1, 2000, 2);
      await queue.obliterate({ force: true });
      const b = await queue.add('job', { tag: 'b' }, { delay: 100 });
      expect(b!.id).toBe(a!.id);

      release('a');
      // b keeps its own promotion timer and is not dropped from the dispatch order by a's late park
      expect(await b!.waitUntilFinished(5, 3000)).toBe('completed');
      expect(runs).toBe(1);
      expect(queue.jobs.get(b!.id)?.data).toEqual({ tag: 'b' });
    });
  });

  describe('a processor that returns no value', () => {
    it('completes the job with an undefined return value', async () => {
      queue = new TestQueue('undefined-result');
      worker = new TestWorker(queue, async () => {});

      const a = await queue.add('job', { tag: 'a' });
      expect(await a!.waitUntilFinished(5, 3000)).toBe('completed');
      expect(queue.jobs.get(a!.id)?.returnvalue).toBeUndefined();
    });

    it.each([
      ['returns results', (jobs: TestJob[]) => jobs.map((j) => (j.data.tag === 'u1' ? undefined : j.data.tag))],
      [
        'throws a BatchError',
        (jobs: TestJob[]) => {
          throw new BatchError(jobs.map((j) => (j.data.tag === 'u1' ? undefined : j.data.tag)));
        },
      ],
    ])('a batch that %s keeps an undefined entry undefined', async (_label, outcome) => {
      queue = new TestQueue('undefined-result-batch');
      worker = new TestWorker(queue, async (jobs: TestJob[]) => outcome(jobs), { batch: { size: 2 } });

      await queue.pause();
      const u1 = await queue.add('job', { tag: 'u1' });
      const u2 = await queue.add('job', { tag: 'u2' });
      await queue.resume();
      await waitFor(
        () => queue.jobs.get(u1!.id)?.state === 'completed' && queue.jobs.get(u2!.id)?.state === 'completed',
        2000,
        2,
      );
      expect(queue.jobs.get(u1!.id)?.returnvalue).toBeUndefined();
      expect(queue.jobs.get(u2!.id)?.returnvalue).toBe('u2');
    });
  });

  it('does not dispatch an obliterated record left in a partial batch', async () => {
    queue = new TestQueue('obliterate-pending-batch');
    const batches: string[][] = [];
    worker = new TestWorker(
      queue,
      async (jobs: TestJob[]) => {
        batches.push(jobs.map((j) => j.data.tag));
        return jobs.map((j) => j.data.tag);
      },
      { batch: { size: 2, timeout: 60_000 } },
    );

    await queue.add('job', { tag: 'a' });
    // let the worker take a into its partial batch before the queue is wiped
    await settle();
    await queue.obliterate({ force: true });
    await queue.add('job', { tag: 'b' });
    await queue.add('job', { tag: 'c' });
    await waitFor(() => batches.length > 0, 2000, 5);
    expect(batches).toEqual([['b', 'c']]);
  });

  it('does not hand an obliterated partial-batch record back to the queue on close', async () => {
    queue = new TestQueue('obliterate-pending-close');
    worker = new TestWorker(queue, async (jobs: TestJob[]) => jobs.map((j) => j.data.tag), {
      batch: { size: 3, timeout: 60_000 },
    });

    await queue.add('job', { tag: 'a' });
    await settle();
    await queue.obliterate({ force: true });
    await queue.add('job', { tag: 'b' });
    await settle();
    await worker.close();
    worker = undefined;
    expect(queue.waitingQueue.map((r) => r.data.tag)).toEqual(['b']);
  });
});

describe('TestJob.moveToDelayed parity', () => {
  let queue: TestQueue;
  let worker: TestWorker;

  afterEach(async () => {
    if (worker) await worker.close();
    if (queue) await queue.close();
  });

  it('parks the job in delayed until the timestamp, then runs the next step', async () => {
    queue = new TestQueue('move-to-delayed');
    const steps: string[] = [];
    const failed: string[] = [];
    worker = new TestWorker(queue, async (job: any) => {
      const step = job.data.step ?? 'start';
      steps.push(step);
      if (step === 'start') {
        await job.moveToDelayed(Date.now() + 200, 'finish');
      }
      return { done: step };
    });
    worker.on('failed', (job: any) => failed.push(job.id));

    const job = await queue.add('steps', { input: 1 });
    await waitFor(() => queue.jobs.get(job!.id)!.state === 'delayed', 1000, 5);
    await new Promise((r) => setTimeout(r, 50));
    expect(queue.jobs.get(job!.id)!.state).toBe('delayed');
    expect(steps).toEqual(['start']);
    expect(await queue.getJobCounts()).toMatchObject({ delayed: 1, waiting: 0, active: 0 });
    expect(queue.jobs.get(job!.id)!.data).toEqual({ input: 1, step: 'finish' });

    await waitFor(() => queue.jobs.get(job!.id)!.state === 'completed', 2000, 10);
    expect(steps).toEqual(['start', 'finish']);
    expect(failed).toEqual([]);
    const done = queue.jobs.get(job!.id)!;
    expect(done.attemptsMade).toBe(0);
    expect(done.returnvalue).toEqual({ done: 'finish' });
  });

  it('validates the timestamp, the step payload and the active state like Job.moveToDelayed', async () => {
    queue = new TestQueue('move-to-delayed-validate');
    const errors: string[] = [];
    worker = new TestWorker(queue, async (job: any) => {
      for (const call of [() => job.moveToDelayed(Number.NaN), () => job.moveToDelayed(Date.now(), 'next')]) {
        try {
          await call();
        } catch (err) {
          errors.push((err as Error).message);
        }
      }
      return 'ok';
    });
    const added = await queue.add('v', 'not-an-object' as any);
    await waitFor(() => queue.jobs.get(added!.id)!.state === 'completed', 1000, 5);
    expect(errors).toEqual([
      'Timestamp must be a finite Unix millisecond value >= 0',
      'moveToDelayed(nextStep) requires plain-object job data',
    ]);

    const idle = await queue.getJob(added!.id);
    await expect(idle!.moveToDelayed(Date.now() + 1000)).rejects.toThrow(
      'moveToDelayed() can only be used while the job is active in a Worker',
    );
  });
});

describe('TestQueue delayed jobs parity', () => {
  let queue: TestQueue;
  let worker: TestWorker | undefined;

  afterEach(async () => {
    if (worker) await worker.close();
    worker = undefined;
    if (queue) await queue.close();
  });

  it('parks a job with delay in delayed and runs it once the delay elapses', async () => {
    queue = new TestQueue('delay-basic');
    const started: number[] = [];
    worker = new TestWorker(queue, async () => {
      started.push(Date.now());
      return 'ok';
    });
    const t0 = Date.now();
    const job = await queue.add('later', { x: 1 }, { delay: 60 });
    expect(await job!.getState()).toBe('delayed');
    expect(await queue.getJobCounts()).toMatchObject({ waiting: 0, delayed: 1, completed: 0 });
    expect((await queue.getJobs('delayed')).map((j) => j.id)).toEqual([job!.id]);
    expect(await queue.getJobs('waiting')).toEqual([]);

    await new Promise((r) => setTimeout(r, 20));
    expect(started).toEqual([]);

    await waitFor(() => started.length === 1, 2000, 5);
    expect(started[0] - t0).toBeGreaterThanOrEqual(55);
    expect(await job!.getState()).toBe('completed');
    expect((await queue.getJobCounts()).delayed).toBe(0);
  });

  it('emits promoted when a delayed job returns to waiting', async () => {
    queue = new TestQueue('delay-promoted-event');
    const promoted: string[] = [];
    queue.on('promoted', (id: string) => promoted.push(id));
    const job = await queue.add('later', {}, { delay: 20 });
    await waitFor(() => promoted.length === 1, 2000, 5);
    expect(promoted).toEqual([job!.id]);
    expect(await job!.getState()).toBe('waiting');
  });

  it('lists delayed jobs in scheduled order: priority, then due time', async () => {
    queue = new TestQueue('delay-order');
    const late = await queue.add('late', {}, { delay: 60_000 });
    const soon = await queue.add('soon', {}, { delay: 30_000 });
    const prio = await queue.add('prio', {}, { delay: 10_000, priority: 1 });
    const ids = (await queue.getJobs('delayed')).map((j) => j.id);
    expect(ids).toEqual([soon!.id, late!.id, prio!.id]);
    expect((await queue.getJobs('delayed', 1, 1)).map((j) => j.id)).toEqual([late!.id]);
  });

  it('promote() moves a delayed job to waiting immediately and dispatches it', async () => {
    queue = new TestQueue('delay-promote');
    const done: string[] = [];
    worker = new TestWorker(queue, async (job) => {
      done.push(job.id);
      return 'ok';
    });
    const job = await queue.add('later', {}, { delay: 60_000 });
    expect(await job!.getState()).toBe('delayed');
    await job!.promote();
    expect(job!.opts.delay).toBe(0);
    await waitFor(() => done.length === 1, 2000, 5);
    expect(done).toEqual([job!.id]);
    await expect(job!.promote()).rejects.toThrow('Cannot promote: not_delayed');
  });

  it('changeDelay follows glidemq_changeDelay state rules', async () => {
    queue = new TestQueue('delay-change');
    const delayed = await queue.add('a', {}, { delay: 60_000 });
    const waiting = await queue.add('b', {});
    const changed: [string, number][] = [];
    queue.on('delay-changed', (id: string, delay: number) => changed.push([id, delay]));

    await delayed!.changeDelay(120_000);
    expect(await delayed!.getState()).toBe('delayed');
    expect(delayed!.opts.delay).toBe(120_000);

    await delayed!.changeDelay(0);
    expect(await delayed!.getState()).toBe('waiting');
    expect(delayed!.opts.delay).toBe(0);

    await waiting!.changeDelay(0);
    expect(await waiting!.getState()).toBe('waiting');

    await waiting!.changeDelay(60_000);
    expect(await waiting!.getState()).toBe('delayed');
    expect((await queue.getJobCounts()).delayed).toBe(1);
    expect((await queue.getJobs('waiting')).map((j) => j.id)).toEqual([delayed!.id]);
    expect(changed).toEqual([
      [delayed!.id, 120_000],
      [delayed!.id, 0],
      [waiting!.id, 60_000],
    ]);

    worker = new TestWorker(queue, async () => 'ok');
    await waitFor(async () => (await delayed!.getState()) === 'completed', 2000, 5);
    await expect(delayed!.changeDelay(10)).rejects.toThrow('Cannot change delay: invalid_state');
    await waiting!.remove();
    await expect(waiting!.changeDelay(10)).rejects.toThrow('Cannot change delay: not_found');
  });

  it('drain(true) removes delayed jobs and cancels their promotion', async () => {
    queue = new TestQueue('delay-drain');
    const done: string[] = [];
    worker = new TestWorker(queue, async (job) => {
      done.push(job.id);
      return 'ok';
    });
    await queue.pause();
    await queue.add('later', {}, { delay: 10 });
    await queue.drain(true);
    await queue.resume();
    await new Promise((r) => setTimeout(r, 40));
    expect(done).toEqual([]);
    expect(await queue.getJobCounts()).toMatchObject({ waiting: 0, delayed: 0, completed: 0 });
  });

  it('a delayed job with priority is dispatched from the priority list after promotion', async () => {
    queue = new TestQueue('delay-priority');
    const order: string[] = [];
    await queue.add('fifo', {});
    await queue.add('prio-delayed', {}, { delay: 20, priority: 1 });
    await queue.add('fifo-2', {});
    await queue.pause();
    await new Promise((r) => setTimeout(r, 40));
    worker = new TestWorker(queue, async (job) => {
      order.push(job.name);
      return 'ok';
    });
    await queue.resume();
    await waitFor(() => order.length === 3, 2000, 5);
    expect(order).toEqual(['prio-delayed', 'fifo', 'fifo-2']);
  });
});

describe('TestQueue prioritized state parity', () => {
  let queue: TestQueue;
  let worker: TestWorker | undefined;

  afterEach(async () => {
    if (worker) await worker.close();
    worker = undefined;
    if (queue) await queue.close();
  });

  it('keeps a priority job in prioritized until a worker promotes it', async () => {
    queue = new TestQueue('prio-state');
    const job = await queue.add('p', {}, { priority: 2 });
    expect(await job!.getState()).toBe('prioritized');
    expect(await queue.getJobCounts()).toMatchObject({ waiting: 0, delayed: 1 });
    expect((await queue.getJobs('delayed')).map((j) => j.id)).toEqual([job!.id]);
    expect(await queue.getJobs('waiting')).toEqual([]);
    expect((await queue.searchJobs({ state: 'prioritized' })).map((j) => j.id)).toEqual([job!.id]);

    const promoted: string[] = [];
    queue.on('promoted', (id: string) => promoted.push(id));
    worker = new TestWorker(queue, async () => 'ok');
    await waitFor(async () => (await job!.getState()) === 'completed', 2000, 5);
    expect(promoted).toEqual([job!.id]);
    expect(await queue.getJobCounts()).toMatchObject({ waiting: 0, delayed: 0, completed: 1 });
  });

  it('a worker promotes prioritized jobs to waiting while the queue is paused', async () => {
    queue = new TestQueue('prio-paused');
    const done: string[] = [];
    worker = new TestWorker(queue, async (job) => {
      done.push(job.id);
      return 'ok';
    });
    await queue.pause();
    const job = await queue.add('p', {}, { priority: 1 });
    // The attached worker's promotion pass runs on the next microtask, before the add() caller resumes.
    await waitFor(async () => (await job!.getState()) === 'waiting', 2000, 2);
    expect(await queue.getJobCounts()).toMatchObject({ waiting: 1, delayed: 0 });
    expect(done).toEqual([]);
    await queue.resume();
    await waitFor(() => done.length === 1, 2000, 5);
  });

  it('changePriority follows glidemq_changePriority state rules', async () => {
    queue = new TestQueue('prio-change');
    const changed: [string, number][] = [];
    queue.on('priority-changed', (id: string, priority: number) => changed.push([id, priority]));
    const waiting = await queue.add('w', {});
    const prio = await queue.add('p', {}, { priority: 3 });
    const delayed = await queue.add('d', {}, { delay: 60_000 });

    await waiting!.changePriority(0);
    expect(await waiting!.getState()).toBe('waiting');
    await waiting!.changePriority(2);
    expect(await waiting!.getState()).toBe('prioritized');
    expect(waiting!.opts.priority).toBe(2);

    await prio!.changePriority(1);
    expect(await prio!.getState()).toBe('prioritized');
    expect(prio!.opts.priority).toBe(1);
    await prio!.changePriority(0);
    expect(await prio!.getState()).toBe('waiting');
    expect(prio!.opts.priority).toBe(0);

    await delayed!.changePriority(4);
    expect(await delayed!.getState()).toBe('delayed');
    expect(delayed!.opts.priority).toBe(4);
    expect((await queue.getJobs('delayed')).map((j) => j.id)).toEqual([waiting!.id, delayed!.id]);

    expect(changed).toEqual([
      [waiting!.id, 2],
      [prio!.id, 1],
      [prio!.id, 0],
      [delayed!.id, 4],
    ]);

    await expect(prio!.changePriority(2049)).rejects.toThrow('Cannot change priority: invalid_priority');
    await expect(prio!.changePriority(1.5)).rejects.toThrow('Cannot change priority: invalid_priority');

    worker = new TestWorker(queue, async () => 'ok');
    await waitFor(async () => (await prio!.getState()) === 'completed', 2000, 5);
    await expect(prio!.changePriority(1)).rejects.toThrow('Cannot change priority: invalid_state');
    await delayed!.remove();
    await expect(delayed!.changePriority(1)).rejects.toThrow('Cannot change priority: not_found');
  });

  it('changeDelay moves between prioritized and delayed like production', async () => {
    queue = new TestQueue('prio-delay');
    const job = await queue.add('p', {}, { priority: 3 });
    await job!.changeDelay(0);
    expect(await job!.getState()).toBe('prioritized');
    await job!.changeDelay(60_000);
    expect(await job!.getState()).toBe('delayed');
    await job!.changeDelay(0);
    expect(await job!.getState()).toBe('prioritized');
    expect((await queue.getJobCounts()).delayed).toBe(1);
  });

  it('debounce dedup replaces a prioritized job that has not been promoted', async () => {
    queue = new TestQueue('prio-debounce');
    const opts = { deduplication: { id: 'd', mode: 'debounce' as const }, priority: 1 };
    const first = await queue.add('t', { v: 1 }, opts);
    const second = await queue.add('t', { v: 2 }, opts);
    expect(second).not.toBeNull();
    expect(await queue.getJob(first!.id)).toBeNull();
  });

  it('drain() keeps prioritized jobs and drain(true) removes them', async () => {
    queue = new TestQueue('prio-drain');
    await queue.add('w', {});
    const prio = await queue.add('p', {}, { priority: 1 });
    await queue.drain();
    expect(await prio!.getState()).toBe('prioritized');
    expect(await queue.getJobCounts()).toMatchObject({ waiting: 0, delayed: 1 });
    await queue.drain(true);
    expect(await queue.getJob(prio!.id)).toBeNull();
  });

  it('a prioritized job kept by drain() is still promoted and run by a worker attached later', async () => {
    queue = new TestQueue('prio-drain-then-run');
    await queue.add('w', {});
    const prio = await queue.add('p', {}, { priority: 1 });
    await queue.drain();
    expect(await queue.getJobCounts()).toMatchObject({ waiting: 0, delayed: 1 });

    const done: string[] = [];
    worker = new TestWorker(queue, async (job) => {
      done.push(job.name);
      return 'ok';
    });
    // Worker.drain() only returns once nothing is waiting, prioritized or delayed.
    await Promise.race([
      worker.drain(),
      new Promise((_, reject) => setTimeout(() => reject(new Error('worker.drain() hung')), 2000)),
    ]);
    worker = undefined;
    expect(done).toEqual(['p']);
    expect(await prio!.getState()).toBe('completed');
    expect(await queue.getJobCounts()).toMatchObject({ waiting: 0, delayed: 0, completed: 1 });
  });
});

describe('TestWorker rate limit parity', () => {
  let queue: TestQueue;
  let worker: TestWorker | undefined;

  afterEach(async () => {
    if (worker) await worker.close();
    worker = undefined;
    if (queue) await queue.close();
  });

  it('requeues a RateLimitError job after the limiter window without counting an attempt', async () => {
    queue = new TestQueue('rl-error');
    const attempts: number[] = [];
    const failed: string[] = [];
    const retrying: [string, string][] = [];
    worker = new TestWorker(
      queue,
      async (job) => {
        attempts.push(Date.now());
        if (attempts.length === 1) throw new TestWorker.RateLimitError();
        return { attemptsMade: job.attemptsMade };
      },
      { limiter: { max: 100, duration: 60 } },
    );
    worker.on('failed', (job) => failed.push(job.id));
    queue.on('retrying', (job, err: Error) => retrying.push([job.failedReason!, err.name]));

    const job = await queue.add('limited', {});
    await waitFor(async () => (await job!.getState()) === 'delayed', 2000, 2);
    expect(await queue.getJob(job!.id)).toMatchObject({ attemptsMade: 0, failedReason: 'rate limited' });
    expect(retrying).toEqual([['rate limited', 'RateLimitError']]);

    await waitFor(() => attempts.length === 2, 2000, 5);
    expect(attempts[1] - attempts[0]).toBeGreaterThanOrEqual(55);
    await waitFor(async () => (await job!.getState()) === 'completed', 2000, 5);
    expect((await queue.getJob(job!.id))!.returnvalue).toEqual({ attemptsMade: 0 });
    expect(failed).toEqual([]);
  });

  it('honours err.delayMs, defaults to 1000ms without a limiter, and recognises the error by name', async () => {
    queue = new TestQueue('rl-delay');
    let calls = 0;
    worker = new TestWorker(queue, async () => {
      calls++;
      if (calls === 1) {
        const err = new Error('Rate limit exceeded') as Error & { delayMs?: number };
        err.name = 'RateLimitError';
        err.delayMs = 30;
        throw err;
      }
      return 'ok';
    });
    const job = await queue.add('a', {});
    await waitFor(() => calls === 2, 2000, 5);
    await waitFor(async () => (await job!.getState()) === 'completed', 2000, 5);

    expect(TestWorker.isRateLimitError(new TestWorker.RateLimitError())).toBe(true);
    expect(TestWorker.isRateLimitError(new Error('x'))).toBe(false);
    expect(new TestWorker.RateLimitError().message).toBe('Rate limit exceeded');
  });

  it('a RateLimitError pauses the worker for the delay before it dispatches other jobs', async () => {
    queue = new TestQueue('rl-pause-others');
    const started: [string, number][] = [];
    let first = true;
    worker = new TestWorker(queue, async (job) => {
      started.push([job.name, Date.now()]);
      if (first) {
        first = false;
        const err = new TestWorker.RateLimitError() as Error & { delayMs?: number };
        err.delayMs = 60;
        throw err;
      }
      return 'ok';
    });
    await queue.add('one', {});
    await queue.add('two', {});
    await waitFor(() => started.length === 3, 2000, 5);
    expect(started.map(([n]) => n)).toEqual(['one', 'two', 'one']);
    expect(started[1][1] - started[0][1]).toBeGreaterThanOrEqual(55);
  });

  it('limiter caps dispatches per window like glidemq_rateLimit', async () => {
    queue = new TestQueue('rl-window');
    const started: number[] = [];
    worker = new TestWorker(
      queue,
      async () => {
        started.push(Date.now());
        return 'ok';
      },
      { concurrency: 10, limiter: { max: 2, duration: 80 } },
    );
    await queue.addBulk([1, 2, 3, 4].map((n) => ({ name: 'j', data: { n } })));
    await waitFor(() => started.length === 4, 2000, 5);
    expect(started[1] - started[0]).toBeLessThan(40);
    expect(started[2] - started[0]).toBeGreaterThanOrEqual(75);
    expect(started[3] - started[0]).toBeGreaterThanOrEqual(75);
    expect(started[3] - started[2]).toBeLessThan(40);
  });

  it('rateLimit(ms) pauses dispatch for the given duration', async () => {
    queue = new TestQueue('rl-manual');
    const started: number[] = [];
    worker = new TestWorker(queue, async () => {
      started.push(Date.now());
      return 'ok';
    });
    const t0 = Date.now();
    await worker.rateLimit(60);
    await queue.add('a', {});
    await new Promise((r) => setTimeout(r, 20));
    expect(started).toEqual([]);
    await waitFor(() => started.length === 1, 2000, 5);
    expect(started[0] - t0).toBeGreaterThanOrEqual(55);
  });
});

describe('TestQueue.getJobs waiting order parity', () => {
  it('lists waiting jobs in dispatch order: priority list, LIFO list, FIFO stream', async () => {
    const queue = new TestQueue('waiting-order');
    // A paused queue with a worker attached promotes priority jobs to waiting without running them.
    await queue.pause();
    const worker = new TestWorker(queue, async () => 'ok');
    await queue.add('fifo-a', {});
    await queue.add('p5', {}, { priority: 5 });
    await queue.add('lifo-x', {}, { lifo: true });
    await queue.add('p1-a', {}, { priority: 1 });
    await queue.add('fifo-b', {});
    await queue.add('lifo-y', {}, { lifo: true });
    await queue.add('p1-b', {}, { priority: 1 });
    await waitFor(async () => (await queue.getJobCounts()).waiting === 7, 2000, 2);

    const names = (await queue.getJobs('waiting')).map((j) => j.name);
    expect(names).toEqual(['p1-a', 'p1-b', 'p5', 'lifo-y', 'lifo-x', 'fifo-a', 'fifo-b']);
    expect((await queue.getJobs('waiting', 2, 4)).map((j) => j.name)).toEqual(['p5', 'lifo-y', 'lifo-x']);
    expect((await queue.getJobs('waiting', 5)).map((j) => j.name)).toEqual(['fifo-a', 'fifo-b']);

    const order: string[] = [];
    worker.on('completed', (job) => order.push(job.name));
    await queue.resume();
    await waitFor(() => order.length === 7, 2000, 5);
    expect(order).toEqual(names);

    await worker.close();
    await queue.close();
  });
});

describe('TestJob / TestQueue / TestWorker surface parity', () => {
  let queue: TestQueue;
  let worker: TestWorker | undefined;

  afterEach(async () => {
    if (worker) await worker.close();
    worker = undefined;
    if (queue) await queue.close();
  });

  it('TestJob exposes the Job state helpers, waitUntilFinished and logs', async () => {
    queue = new TestQueue('surface-job');
    const job = await queue.add('a', {}, { delay: 30 });
    expect(await job!.isDelayed()).toBe(true);
    expect(await job!.isWaiting()).toBe(false);
    worker = new TestWorker(queue, async (j) => {
      await j.log('step one');
      await j.log('step two');
      return 'ok';
    });
    expect(await job!.waitUntilFinished(5, 2000)).toBe('completed');
    expect(await job!.isCompleted()).toBe(true);
    expect(await job!.isFailed()).toBe(false);
    expect(await job!.isActive()).toBe(false);
    expect(await queue.getJobLogs(job!.id)).toEqual({ logs: ['step one', 'step two'], count: 2 });
    expect(await queue.getJobLogs(job!.id, 1, 1)).toEqual({ logs: ['step two'], count: 2 });
    expect(await queue.getJobLogs('missing')).toEqual({ logs: [], count: 0 });
    await expect(job!.log('x'.repeat(MAX_JOB_DATA_SIZE + 1))).rejects.toThrow('Log message exceeds maximum size');
    const stuck = await queue.add('never', {}, { delay: 60_000 });
    await expect(stuck!.waitUntilFinished(5, 30)).rejects.toThrow(`Job ${stuck!.id} did not finish within 30ms`);
  });

  it('TestJob.retry() moves a failed job back to waiting like glidemq_retryJob', async () => {
    queue = new TestQueue('surface-retry');
    let calls = 0;
    worker = new TestWorker(queue, async () => {
      calls++;
      if (calls === 1) throw new Error('boom');
      return 'ok';
    });
    const job = await queue.add('a', {}, { ttl: 60_000 });
    expect(await job!.waitUntilFinished(5, 2000)).toBe('failed');
    await expect(job!.retry()).resolves.toBeUndefined();
    expect(job!.attemptsMade).toBe(0);
    expect(job!.failedReason).toBeUndefined();
    expect(await job!.waitUntilFinished(5, 2000)).toBe('completed');
    await expect(job!.retry()).rejects.toThrow('Cannot retry: not_failed');
    await job!.remove();
    await expect(job!.retry()).rejects.toThrow('Cannot retry: not_found');
  });

  it('TestJob.moveToFailed() inside the processor fails the job instead of completing it', async () => {
    queue = new TestQueue('surface-move-to-failed');
    const outside = await queue.add('outside', {}, { delay: 60_000 });
    await expect(outside!.moveToFailed(new Error('x'))).rejects.toThrow(
      'moveToFailed can only be called while job is active in a Worker',
    );
    let calls = 0;
    const completed: string[] = [];
    const failed: string[] = [];
    worker = new TestWorker(queue, async (job) => {
      calls++;
      if (calls === 1) {
        await job.moveToFailed(new Error('manual failure'));
        return 'ignored';
      }
      return 'ok';
    });
    worker.on('completed', (job) => completed.push(job.id));
    worker.on('failed', (_job, err: Error) => failed.push(err.message));
    const job = await queue.add('a', {}, { attempts: 2, backoff: { type: 'fixed', delay: 0 } });
    expect(await job!.waitUntilFinished(5, 2000)).toBe('completed');
    expect(failed).toEqual(['manual failure']);
    expect(completed).toEqual([job!.id]);
    expect((await queue.getJob(job!.id))!.attemptsMade).toBe(1);
  });

  it('addAndWait resolves with the return value and rejects with the failed reason', async () => {
    queue = new TestQueue('surface-add-and-wait');
    worker = new TestWorker(queue, async (job) => {
      if (job.data.fail) throw new Error('bad input');
      return { echoed: job.data.v };
    });
    await expect(queue.addAndWait('a', { v: 1 })).resolves.toEqual({ echoed: 1 });
    await expect(queue.addAndWait('a', { fail: true })).rejects.toThrow('bad input');
    await expect(queue.addAndWait('a', { v: 1 }, { waitTimeout: 0 })).rejects.toThrow(
      'waitTimeout must be a positive finite number',
    );
    await expect(queue.addAndWait('a', { v: 1 }, { removeOnComplete: true })).rejects.toThrow(
      'does not support removeOnComplete/removeOnFail',
    );
    await queue.add('dup', {}, { jobId: 'fixed' });
    await expect(queue.addAndWait('dup', {}, { jobId: 'fixed' })).rejects.toThrow('returned null');
    await expect(queue.addAndWait('slow', { v: 2 }, { delay: 60_000, waitTimeout: 20 })).rejects.toThrow(
      'did not finish within 20ms',
    );
  });

  it('count(), getJobCountByTypes() and getSuspendedJobs() mirror the Queue readers', async () => {
    queue = new TestQueue('surface-counts');
    await queue.add('fifo', {});
    await queue.add('lifo', {}, { lifo: true });
    await queue.add('prio', {}, { priority: 1 });
    await queue.add('later', {}, { delay: 60_000 });
    expect(await queue.count()).toBe(1);
    expect(await queue.getJobCountByTypes()).toEqual(await queue.getJobCounts());

    worker = new TestWorker(queue, async (job) => {
      if (job.name === 'fifo') await job.suspend({ reason: 'wait', timeout: 60_000 });
      return 'ok';
    });
    await waitFor(async () => (await queue.getSuspendedJobs()).length === 1, 2000, 5);
    const suspended = await queue.getSuspendedJobs(0, -1, { excludeData: true });
    expect(suspended.map((j) => j.name)).toEqual(['fifo']);
    expect(suspended[0].data).toBeUndefined();
  });

  it('obliterate() refuses with active jobs unless forced, then wipes the queue', async () => {
    queue = new TestQueue('surface-obliterate');
    let release: () => void = () => {};
    worker = new TestWorker(queue, () => new Promise<string>((r) => (release = () => r('ok'))));
    await queue.add('active', {});
    await queue.add('later', {}, { delay: 60_000 });
    await queue.upsertJobScheduler('tick', { every: 60_000 }, { name: 'tick' });
    await waitFor(() => worker!.getActiveCount() === 1, 2000, 2);
    await expect(queue.obliterate()).rejects.toThrow(
      'Cannot obliterate queue "surface-obliterate": 1 active jobs. Use { force: true } to override.',
    );
    await queue.obliterate({ force: true });
    expect(await queue.getJobCounts()).toEqual({ waiting: 0, active: 0, delayed: 0, completed: 0, failed: 0 });
    expect(await queue.getRepeatableJobs()).toEqual([]);
    release();
    await new Promise((r) => setTimeout(r, 10));
    expect(queue.jobs.size).toBe(0);
  });

  it('revoke() fails a waiting job with reason revoked and flags an active one', async () => {
    queue = new TestQueue('surface-revoke');
    const revoked: string[] = [];
    queue.on('revoked', (id: string) => revoked.push(id));
    const waiting = await queue.add('w', {});
    const delayed = await queue.add('d', {}, { delay: 60_000 });
    expect(await queue.revoke(waiting!.id)).toBe('revoked');
    expect(await queue.revoke(delayed!.id)).toBe('revoked');
    expect(await queue.revoke('missing')).toBe('not_found');
    expect(await queue.getJob(waiting!.id)).toMatchObject({ failedReason: 'revoked' });
    expect(await waiting!.isFailed()).toBe(true);
    expect(await waiting!.isRevoked()).toBe(true);
    expect((await queue.getJobCounts()).failed).toBe(2);

    let release: () => void = () => {};
    worker = new TestWorker(queue, () => new Promise<string>((r) => (release = () => r('ok'))));
    const active = await queue.add('a', {});
    await waitFor(() => worker!.getActiveCount() === 1, 2000, 2);
    expect(await queue.revoke(active!.id)).toBe('flagged');
    expect(await active!.isRevoked()).toBe(true);
    release();
    expect(await active!.waitUntilFinished(5, 2000)).toBe('completed');
    expect(revoked).toEqual([waiting!.id, delayed!.id, active!.id]);
  });

  it('TestWorker exposes pause/resume/isPaused/isRunning/waitUntilReady/drain like Worker', async () => {
    queue = new TestQueue('surface-worker');
    const done: string[] = [];
    worker = new TestWorker(queue, async (job) => {
      done.push(job.name);
      return 'ok';
    });
    await worker.waitUntilReady();
    expect(worker.isRunning()).toBe(true);
    expect(worker.isPaused()).toBe(false);
    await worker.pause();
    expect(worker.isPaused()).toBe(true);
    expect(worker.isRunning()).toBe(false);
    await queue.add('held', {});
    await new Promise((r) => setTimeout(r, 20));
    expect(done).toEqual([]);
    expect((await queue.getJobCounts()).waiting).toBe(1);
    await worker.resume();
    await waitFor(() => done.length === 1, 2000, 2);

    await queue.add('one', {});
    await queue.add('two', {}, { delay: 30 });
    await worker.drain();
    expect(done).toEqual(['held', 'one', 'two']);
    expect(worker.isRunning()).toBe(false);
    expect(await queue.getWorkers()).toEqual([]);
    worker = undefined;
  });

  it('repeatAfterComplete schedulers wait for the job to finish before the next run', async () => {
    queue = new TestQueue('surface-repeat-after-complete');
    const runs: number[] = [];
    worker = new TestWorker(queue, async () => {
      runs.push(Date.now());
      await new Promise((r) => setTimeout(r, 40));
      return 'ok';
    });
    await queue.upsertJobScheduler('rac', { repeatAfterComplete: 30, limit: 3 }, { name: 'rac-job' });
    await waitFor(() => runs.length === 3, 3000, 5);
    // Each run starts at least processing time (40ms) + 30ms after the previous one.
    expect(runs[1] - runs[0]).toBeGreaterThanOrEqual(65);
    expect(runs[2] - runs[1]).toBeGreaterThanOrEqual(65);
    const entry = await queue.getJobScheduler('rac');
    expect(entry === null || entry.nextRun === 0 || entry.iterationCount === 3).toBe(true);
    await waitFor(async () => (await queue.getJobScheduler('rac')) === null, 2000, 5);
    expect((await queue.searchJobs({ name: 'rac-job' }))[0].schedulerName).toBe('rac');
  });

  it('a budget pause parks the job in delayed like moveActiveToDelayed', async () => {
    queue = new TestQueue('surface-budget-pause');
    queue.setBudget('flow', { maxTotalTokens: 1, onExceeded: 'pause' });
    queue.budgets.get('flow')!.exceeded = true;
    await queue.pause();
    const job = await queue.add('capped', {});
    queue.jobs.get(job!.id)!.budgetKey = 'flow';
    worker = new TestWorker(queue, async () => 'never');
    await queue.resume();
    await waitFor(async () => (await job!.getState()) === 'delayed', 2000, 2);
    expect((await queue.getJobs('delayed')).map((j) => j.id)).toEqual([job!.id]);
  });
});

describe('TestWorker batch mode honours job.moveToFailed() (revuto #313)', () => {
  let queue: TestQueue;
  let worker: TestWorker | undefined;

  afterEach(async () => {
    if (worker) await worker.close();
    worker = undefined;
    if (queue) await queue.close();
  });

  it('a moved-to-failed job is not completed when the batch succeeds', async () => {
    queue = new TestQueue('batch-move-to-failed');
    const completed: string[] = [];
    const failed: [string, string][] = [];
    worker = new TestWorker(
      queue,
      async (jobs: TestJob[]) => {
        for (const job of jobs) {
          if (job.data.bad && job.attemptsMade === 0) await job.moveToFailed(new Error('manual failure'));
        }
        return jobs.map(() => 'ok');
      },
      { batch: { size: 10 } },
    );
    worker.on('completed', (job) => completed.push(job.name));
    worker.on('failed', (job, err: Error) => failed.push([job.name, err.message]));
    await queue.pause();
    await queue.add('good', {});
    const bad = await queue.add('bad', { bad: true }, { attempts: 2, backoff: { type: 'fixed', delay: 0 } });
    await queue.resume();
    expect(await bad!.waitUntilFinished(5, 2000)).toBe('completed');
    expect(failed).toEqual([['bad', 'manual failure']]);
    expect(completed.sort()).toEqual(['bad', 'good']);
    expect((await queue.getJob(bad!.id))!.attemptsMade).toBe(1);
  });

  it('a moved-to-failed job stays failed through BatchError results and whole-batch throws', async () => {
    queue = new TestQueue('batch-move-to-failed-error');
    const failed: [string, string][] = [];
    const completed: string[] = [];
    let call = 0;
    worker = new TestWorker(
      queue,
      async (jobs: TestJob[]) => {
        call++;
        for (const job of jobs) if (job.data.move) await job.moveToFailed(new Error('moved'));
        if (call === 1) throw new BatchError(jobs.map(() => 'fine'));
        throw new Error('batch exploded');
      },
      { batch: { size: 10 } },
    );
    worker.on('failed', (job, err: Error) => failed.push([job.name, err.message]));
    worker.on('completed', (job) => completed.push(job.name));
    await queue.pause();
    const moved = await queue.add('moved-1', { move: true });
    await queue.add('plain-1', {});
    await queue.resume();
    expect(await moved!.waitUntilFinished(5, 2000)).toBe('failed');
    expect(completed).toEqual(['plain-1']);

    await queue.pause();
    const moved2 = await queue.add('moved-2', { move: true });
    const plain2 = await queue.add('plain-2', {});
    await queue.resume();
    expect(await moved2!.waitUntilFinished(5, 2000)).toBe('failed');
    expect(await plain2!.waitUntilFinished(5, 2000)).toBe('failed');
    expect(failed).toEqual([
      ['moved-1', 'moved'],
      ['moved-2', 'moved'],
      ['plain-2', 'batch exploded'],
    ]);
  });
});

describe('TestWorker rate limit keeps queue order (revuto #313)', () => {
  let queue: TestQueue;
  let worker: TestWorker | undefined;

  afterEach(async () => {
    if (worker) await worker.close();
    worker = undefined;
    if (queue) await queue.close();
  });

  it('a rate-limited worker does not pop LIFO jobs, so LIFO order is kept', async () => {
    queue = new TestQueue('rl-lifo-order');
    const order: string[] = [];
    await queue.pause();
    worker = new TestWorker(
      queue,
      async (job) => {
        order.push(job.name);
        return 'ok';
      },
      { concurrency: 1, limiter: { max: 1, duration: 40 } },
    );
    await queue.add('A', {}, { lifo: true });
    await queue.add('B', {}, { lifo: true });
    await queue.add('C', {}, { lifo: true });
    await queue.resume();
    await waitFor(() => order.length === 3, 3000, 5);
    expect(order).toEqual(['C', 'B', 'A']);
  });

  it('priority and FIFO order are kept across rate-limit windows', async () => {
    queue = new TestQueue('rl-mixed-order');
    const order: string[] = [];
    await queue.pause();
    worker = new TestWorker(
      queue,
      async (job) => {
        order.push(job.name);
        return 'ok';
      },
      { concurrency: 1, limiter: { max: 1, duration: 40 } },
    );
    await queue.add('fifo-a', {});
    await queue.add('p5', {}, { priority: 5 });
    await queue.add('lifo-x', {}, { lifo: true });
    await queue.add('p1', {}, { priority: 1 });
    await queue.add('fifo-b', {});
    await queue.add('lifo-y', {}, { lifo: true });
    await queue.resume();
    await waitFor(() => order.length === 6, 4000, 5);
    expect(order).toEqual(['p1', 'p5', 'lifo-y', 'lifo-x', 'fifo-a', 'fifo-b']);
  });
});

describe('waitingQueue holds each record at most once (revuto #313 round 2)', () => {
  let queue: TestQueue;
  let worker: TestWorker | undefined;

  afterEach(async () => {
    if (worker) await worker.close();
    worker = undefined;
    if (queue) await queue.close();
  });

  const duplicates = (q: TestQueue): string[] => {
    const seen = new Set<string>();
    const dupes: string[] = [];
    for (const r of q.waitingQueue) {
      if (seen.has(r.id)) dupes.push(r.id);
      seen.add(r.id);
    }
    return dupes;
  };

  it('a job parked with changeDelay and promoted later is dispatched once, also in batch mode', async () => {
    queue = new TestQueue('wq-park-once');
    const job = await queue.add('once', {});
    await job!.changeDelay(20);
    expect(queue.waitingQueue.map((r) => r.id)).toEqual([]);
    await new Promise((r) => setTimeout(r, 40));
    expect(duplicates(queue)).toEqual([]);

    const seen: string[] = [];
    worker = new TestWorker(
      queue,
      async (jobs: TestJob[]) => {
        seen.push(...jobs.map((j) => j.id));
        return jobs.map(() => 'ok');
      },
      { batch: { size: 10 } },
    );
    await waitFor(async () => (await job!.getState()) === 'completed', 2000, 5);
    await new Promise((r) => setTimeout(r, 20));
    expect(seen).toEqual([job!.id]);
  });

  it('never holds a record twice across park, promote, priority, revoke, remove and retry transitions', async () => {
    queue = new TestQueue('wq-invariant');
    await queue.pause();
    const a = await queue.add('a', {});
    const b = await queue.add('b', {}, { priority: 2 });
    const c = await queue.add('c', {}, { lifo: true });
    const d = await queue.add('d', {}, { delay: 10 });
    const check = () => expect(duplicates(queue)).toEqual([]);
    check();

    await a!.changeDelay(10);
    await b!.changeDelay(10);
    check();
    expect(queue.waitingQueue.map((r) => r.id)).toEqual([c!.id]);
    await a!.promote();
    await b!.changeDelay(0);
    await d!.promote();
    check();
    await a!.changePriority(1);
    await a!.changePriority(0);
    await b!.changePriority(0);
    await b!.changePriority(3);
    check();
    await a!.changeDelay(5);
    await new Promise((r) => setTimeout(r, 20));
    check();
    expect(await queue.revoke(c!.id)).toBe('revoked');
    await d!.remove();
    check();
    expect(await queue.retryJobs()).toBe(1);
    check();
    await queue.resume();
    worker = new TestWorker(queue, async () => 'ok');
    await waitFor(async () => (await queue.getJobCounts()).completed === 3, 2000, 5);
    check();
    expect(queue.waitingQueue).toEqual([]);
    expect(await queue.getJobCounts()).toMatchObject({ waiting: 0, delayed: 0, completed: 3, failed: 0 });
  });
});

describe('TestWorker dead-letter queue parity', () => {
  const queues: TestQueue[] = [];
  const workers: TestWorker[] = [];

  function openQueue(name: string, opts?: TestQueueOptions): TestQueue {
    const q = new TestQueue(name, opts);
    queues.push(q);
    return q;
  }

  function startWorker(queue: TestQueue, processor: (job: TestJob) => Promise<unknown>, dlq?: string): TestWorker {
    const w = new TestWorker(queue, processor, dlq ? { deadLetterQueue: { name: dlq } } : undefined);
    workers.push(w);
    return w;
  }

  const alwaysFail = async () => {
    throw new Error('boom');
  };

  afterEach(async () => {
    for (const w of workers.splice(0)) await w.close();
    for (const q of queues.splice(0)) await q.close();
  });

  it('copies a terminally failed job into the DLQ with the production envelope', async () => {
    const dlq = openQueue('dlq-envelope-dlq');
    const queue = openQueue('dlq-envelope');
    startWorker(queue, alwaysFail, 'dlq-envelope-dlq');
    const data = { to: 'a', nested: { n: 1 } };
    const job = await queue.add('send', data, { attempts: 3, backoff: { type: 'fixed', delay: 1 } });
    expect(await job!.waitUntilFinished(5, 2000)).toBe('failed');

    const dlqJobs = await queue.getDeadLetterJobs();
    expect(dlqJobs).toHaveLength(1);
    expect(dlqJobs[0].name).toBe('send');
    // attemptsMade is the count before the failing attempt, as BaseWorker.moveToDLQ writes it.
    expect(dlqJobs[0].data).toEqual({
      originalQueue: 'dlq-envelope',
      originalJobId: job!.id,
      data,
      failedReason: 'boom',
      attemptsMade: 2,
    });
    // The DLQ is an ordinary queue, so the same job is visible through it.
    expect((await dlq.getJobCounts()).waiting).toBe(1);
    expect(await (await queue.getJob(job!.id))!.getState()).toBe('failed');
  });

  it('does not copy retried attempts, completed jobs or failures without a DLQ configured', async () => {
    const dlq = openQueue('dlq-skip-dlq');
    const queue = openQueue('dlq-skip');
    let calls = 0;
    startWorker(
      queue,
      async (job) => {
        if (job.name === 'flaky' && ++calls === 1) throw new Error('first try');
        return 'ok';
      },
      'dlq-skip-dlq',
    );
    const flaky = await queue.add('flaky', {}, { attempts: 2, backoff: { type: 'fixed', delay: 1 } });
    const fine = await queue.add('fine', {});
    expect(await flaky!.waitUntilFinished(5, 2000)).toBe('completed');
    expect(await fine!.waitUntilFinished(5, 2000)).toBe('completed');
    expect(await queue.getDeadLetterJobs()).toEqual([]);
    expect((await dlq.getJobCounts()).waiting).toBe(0);

    // A worker without the option writes nothing, even when the queue names a DLQ to read.
    const plain = openQueue('dlq-plain', { deadLetterQueue: { name: 'dlq-plain-dlq' } });
    const plainDlq = openQueue('dlq-plain-dlq');
    startWorker(plain, alwaysFail);
    const failed = await plain.add('x', {}, { attempts: 1 });
    expect(await failed!.waitUntilFinished(5, 2000)).toBe('failed');
    expect(await plain.getDeadLetterJobs()).toEqual([]);
    expect((await plainDlq.getJobCounts()).waiting).toBe(0);
  });

  it('writes the DLQ copy before the failed events fire', async () => {
    const dlq = openQueue('dlq-order-dlq');
    const queue = openQueue('dlq-order');
    const worker = startWorker(queue, alwaysFail, 'dlq-order-dlq');
    const seen: number[] = [];
    worker.on('failed', () => seen.push(dlq.jobs.size));
    queue.on('failed', () => seen.push(dlq.jobs.size));
    const job = await queue.add('x', {}, { attempts: 1 });
    expect(await job!.waitUntilFinished(5, 2000)).toBe('failed');
    expect(seen).toEqual([1, 1]);
  });

  it('creates the DLQ queue on first use when none is open', async () => {
    const queue = openQueue('dlq-auto');
    startWorker(queue, alwaysFail, 'dlq-auto-dlq');
    const job = await queue.add('x', { v: 1 }, { attempts: 1 });
    expect(await job!.waitUntilFinished(5, 2000)).toBe('failed');
    const dlqJobs = await queue.getDeadLetterJobs();
    expect(dlqJobs).toHaveLength(1);
    expect(dlqJobs[0].data).toMatchObject({ originalQueue: 'dlq-auto', originalJobId: job!.id, data: { v: 1 } });
    await TestQueue['registry'].get('dlq-auto-dlq')?.close();
  });

  it('reads the DLQ named by the queue option without a worker and returns empty until one exists', async () => {
    const queue = openQueue('dlq-option', { deadLetterQueue: { name: 'dlq-option-dlq' } });
    expect(await queue.getDeadLetterJobs()).toEqual([]);
    expect(await queue.getDeadLetterJob('1')).toBeNull();
    expect(await queue.removeDeadLetterJob('1')).toBe(false);
    expect(await queue.replayDeadLetterJob('1')).toBeNull();

    const unconfigured = openQueue('dlq-none');
    expect(await unconfigured.getDeadLetterJobs()).toEqual([]);
    expect(await unconfigured.getDeadLetterJob('1')).toBeNull();
  });

  it('records failures from job.moveToFailed() and from batch processors', async () => {
    const queue = openQueue('dlq-paths');
    const batchQueue = openQueue('dlq-paths-batch');
    openQueue('dlq-paths-dlq');
    startWorker(
      queue,
      async (job) => {
        await job.moveToFailed(new Error('moved by processor'));
        return 'ignored';
      },
      'dlq-paths-dlq',
    );
    const moved = await queue.add('moved', { m: 1 }, { attempts: 1 });
    expect(await moved!.waitUntilFinished(5, 2000)).toBe('failed');
    const movedDlq = await queue.getDeadLetterJobs();
    expect(movedDlq).toHaveLength(1);
    expect(movedDlq[0].data).toMatchObject({ originalJobId: moved!.id, failedReason: 'moved by processor' });

    const batchWorker = new TestWorker(
      batchQueue,
      async (jobs: TestJob[]) => {
        throw new BatchError(jobs.map((j) => (j.data.bad ? new Error('bad item') : 'ok')));
      },
      { batch: { size: 10 }, deadLetterQueue: { name: 'dlq-paths-dlq' } },
    );
    workers.push(batchWorker);
    await batchQueue.pause();
    await batchQueue.add('good', {}, { attempts: 1 });
    const bad = await batchQueue.add('bad', { bad: true }, { attempts: 1 });
    await batchQueue.resume();
    expect(await bad!.waitUntilFinished(5, 2000)).toBe('failed');
    const batchDlq = await batchQueue.getDeadLetterJobs();
    expect(batchDlq).toHaveLength(1);
    expect(batchDlq[0].name).toBe('bad');
    expect(batchDlq[0].data).toMatchObject({ originalQueue: 'dlq-paths-batch', failedReason: 'bad item' });
  });

  it('reports a DLQ write failure on the worker error event without changing the job outcome', async () => {
    // A BigInt survives this serializer but not the plain-JSON envelope, which production also cannot write.
    const bigintSerializer = {
      serialize: (value: unknown) =>
        JSON.stringify(value, (_key, v) => (typeof v === 'bigint' ? { big: String(v) } : v)),
      deserialize: (raw: string) =>
        JSON.parse(raw, (_key, v) => (v && typeof v === 'object' && 'big' in v ? BigInt(v.big) : v)),
    };
    openQueue('dlq-error-dlq');
    const queue = openQueue('dlq-error', { serializer: bigintSerializer });
    const worker = startWorker(queue, alwaysFail, 'dlq-error-dlq');
    const failures: string[] = [];
    worker.on('failed', (_job, err: Error) => failures.push(err.message));

    // No error listener: the failure becomes a process warning instead of throwing from the failure path.
    const warn = vi.spyOn(process, 'emitWarning').mockImplementation(() => {});
    try {
      const first = await queue.add('a', { n: BigInt(1) }, { attempts: 1 });
      expect(await first!.waitUntilFinished(5, 2000)).toBe('failed');
      expect(warn).toHaveBeenCalledTimes(1);
      expect(String(warn.mock.calls[0][0])).toMatch(/dead-letter write failed: .*BigInt/);
      expect(warn.mock.calls[0][1]).toBe('GlideMQWarning');
    } finally {
      warn.mockRestore();
    }

    const errors: string[] = [];
    worker.on('error', (err: Error) => errors.push(err.message));
    const second = await queue.add('b', { n: BigInt(2) }, { attempts: 1 });
    expect(await second!.waitUntilFinished(5, 2000)).toBe('failed');
    expect(errors).toHaveLength(1);
    expect(errors[0]).toMatch(/BigInt/);
    expect(failures).toEqual(['boom', 'boom']);
    expect(await queue.getDeadLetterJobs()).toEqual([]);
  });

  it('writes the envelope as plain JSON without using the serializer of the DLQ queue', async () => {
    const unusable = {
      serialize: () => {
        throw new Error('the DLQ queue serializer must not be used');
      },
      deserialize: () => {
        throw new Error('the DLQ queue serializer must not be used');
      },
    };
    const dlq = openQueue('dlq-serializer-dlq', { serializer: unusable });
    const queue = openQueue('dlq-serializer');
    const worker = startWorker(queue, alwaysFail, 'dlq-serializer-dlq');
    const errors: Error[] = [];
    worker.on('error', (err: Error) => errors.push(err));

    const job = await queue.add('x', { v: 1 }, { attempts: 1 });
    expect(await job!.waitUntilFinished(5, 2000)).toBe('failed');
    expect(errors).toEqual([]);
    expect((await dlq.getJobCounts()).waiting).toBe(1);
    const [entry] = await queue.getDeadLetterJobs();
    expect(entry.data).toMatchObject({ originalQueue: 'dlq-serializer', originalJobId: job!.id, data: { v: 1 } });
  });

  it('copies the data the processor holds when it replaced job.data before failing', async () => {
    openQueue('dlq-data-dlq');
    const queue = openQueue('dlq-data');
    startWorker(
      queue,
      async (job) => {
        job.data = { value: 2 };
        throw new Error('boom');
      },
      'dlq-data-dlq',
    );
    const job = await queue.add('x', { value: 1 }, { attempts: 1 });
    expect(await job!.waitUntilFinished(5, 2000)).toBe('failed');
    const [entry] = await queue.getDeadLetterJobs();
    expect(entry.data).toMatchObject({ originalJobId: job!.id, data: { value: 2 } });
  });

  describe('reading and managing DLQ jobs', () => {
    async function failJobs(queue: TestQueue, n: number, opts?: JobOptions): Promise<void> {
      const jobs: TestJob[] = [];
      for (let i = 0; i < n; i++) jobs.push((await queue.add('item', { i }, { attempts: 1, ...opts }))!);
      for (const job of jobs) expect(await job.waitUntilFinished(5, 2000)).toBe('failed');
    }

    it('pages with start/end and honours excludeData', async () => {
      openQueue('dlq-page-dlq');
      const queue = openQueue('dlq-page');
      startWorker(queue, alwaysFail, 'dlq-page-dlq');
      await failJobs(queue, 3);

      const all = await queue.getDeadLetterJobs();
      expect(all.map((j) => (j.data as { data: { i: number } }).data.i)).toEqual([0, 1, 2]);
      expect((await queue.getDeadLetterJobs(0, 1)).map((j) => j.id)).toEqual([all[0].id, all[1].id]);
      expect((await queue.getDeadLetterJobs(1)).map((j) => j.id)).toEqual([all[1].id, all[2].id]);
      expect(await queue.getDeadLetterJobs(5, 9)).toEqual([]);

      const stripped = await queue.getDeadLetterJobs(0, -1, { excludeData: true });
      expect(stripped).toHaveLength(3);
      expect(stripped[0].data).toBeUndefined();
      expect((await queue.getDeadLetterJob(all[0].id, { excludeData: true }))!.data).toBeUndefined();
      expect((await queue.getDeadLetterJob(all[0].id))!.data).toMatchObject({ originalJobId: expect.any(String) });
    });

    it('scopes a shared DLQ to the queue that owns each job', async () => {
      openQueue('dlq-shared-dlq');
      const allowed = openQueue('dlq-shared-a');
      const blocked = openQueue('dlq-shared-b');
      startWorker(allowed, alwaysFail, 'dlq-shared-dlq');
      startWorker(blocked, alwaysFail, 'dlq-shared-dlq');
      await failJobs(allowed, 2);
      await failJobs(blocked, 1);

      expect(await allowed.getDeadLetterJobs()).toHaveLength(2);
      const blockedJobs = await blocked.getDeadLetterJobs();
      expect(blockedJobs).toHaveLength(1);
      expect(await allowed.getDeadLetterJobs(1, 1)).toHaveLength(1);

      const foreignId = blockedJobs[0].id;
      expect(await allowed.getDeadLetterJob(foreignId)).toBeNull();
      expect(await allowed.removeDeadLetterJob(foreignId)).toBe(false);
      expect(await allowed.replayDeadLetterJob(foreignId)).toBeNull();
      expect(await blocked.getDeadLetterJobs()).toHaveLength(1);
    });

    it('lists only waiting and active DLQ jobs but looks one up in any state', async () => {
      const dlq = openQueue('dlq-states-dlq');
      const queue = openQueue('dlq-states');
      startWorker(queue, alwaysFail, 'dlq-states-dlq');
      await failJobs(queue, 1);
      const [listed] = await queue.getDeadLetterJobs();

      startWorker(dlq, async () => 'handled');
      expect(await listed.waitUntilFinished(5, 2000)).toBe('completed');
      expect(await queue.getDeadLetterJobs()).toEqual([]);
      const found = await queue.getDeadLetterJob(listed.id);
      expect(found).not.toBeNull();
      expect(await found!.getState()).toBe('completed');
    });

    it('removeDeadLetterJob deletes the entry and reports whether it existed', async () => {
      openQueue('dlq-remove-dlq');
      const queue = openQueue('dlq-remove');
      startWorker(queue, alwaysFail, 'dlq-remove-dlq');
      await failJobs(queue, 2);
      const [first, second] = await queue.getDeadLetterJobs();

      expect(await queue.removeDeadLetterJob(first.id)).toBe(true);
      expect(await queue.removeDeadLetterJob(first.id)).toBe(false);
      expect(await queue.getDeadLetterJob(first.id)).toBeNull();
      expect((await queue.getDeadLetterJobs()).map((j) => j.id)).toEqual([second.id]);
    });

    it('replayDeadLetterJob re-adds the original job with its options and drops the DLQ entry', async () => {
      openQueue('dlq-replay-dlq');
      const queue = openQueue('dlq-replay');
      let shouldFail = true;
      startWorker(
        queue,
        async () => {
          if (shouldFail) throw new Error('boom');
          return 'recovered';
        },
        'dlq-replay-dlq',
      );
      const original = await queue.add('send', { to: 'a' }, { attempts: 1, jobId: 'custom-1', priority: 2 });
      expect(await original!.waitUntilFinished(5, 2000)).toBe('failed');
      const [dlqJob] = await queue.getDeadLetterJobs();

      shouldFail = false;
      const replayed = await queue.replayDeadLetterJob(dlqJob.id);
      expect(replayed).not.toBeNull();
      expect(replayed!.id).not.toBe('custom-1');
      expect(replayed!.name).toBe('send');
      expect(replayed!.data).toEqual({ to: 'a' });
      expect(replayed!.opts.attempts).toBe(1);
      expect(replayed!.opts.priority).toBe(2);
      expect(replayed!.opts.jobId).toBeUndefined();
      expect(await replayed!.waitUntilFinished(5, 2000)).toBe('completed');
      expect(await queue.getDeadLetterJob(dlqJob.id)).toBeNull();
      expect(await queue.getDeadLetterJobs()).toEqual([]);
      expect(await queue.replayDeadLetterJob(dlqJob.id)).toBeNull();
    });

    it('replayDeadLetterJob falls back to the envelope data when the original job is gone', async () => {
      openQueue('dlq-replay-gone-dlq');
      const queue = openQueue('dlq-replay-gone');
      startWorker(queue, alwaysFail, 'dlq-replay-gone-dlq');
      const original = await queue.add('send', { to: 'b' }, { attempts: 1, priority: 3 });
      expect(await original!.waitUntilFinished(5, 2000)).toBe('failed');
      await original!.remove();
      const [dlqJob] = await queue.getDeadLetterJobs();

      const replayed = await queue.replayDeadLetterJob(dlqJob.id);
      expect(replayed!.data).toEqual({ to: 'b' });
      expect(replayed!.opts.attempts).toBeUndefined();
      expect(replayed!.opts.priority).toBeUndefined();
    });

    it('replayDeadLetterJob replays an entry without an original job id or data as a null payload', async () => {
      openQueue('dlq-replay-bare-dlq');
      const queue = openQueue('dlq-replay-bare', { deadLetterQueue: { name: 'dlq-replay-bare-dlq' } });
      queue.addDeadLetter('dlq-replay-bare-dlq', 'send', {
        originalQueue: 'dlq-replay-bare',
        originalJobId: '',
        data: undefined,
        failedReason: 'boom',
        attemptsMade: 0,
      });
      const [dlqJob] = await queue.getDeadLetterJobs();

      const replayed = await queue.replayDeadLetterJob(dlqJob.id);
      expect(replayed!.name).toBe('send');
      expect(replayed!.data).toBeNull();
      expect(await queue.getDeadLetterJobs()).toEqual([]);
    });

    it('replayDeadLetterJob throws and keeps the DLQ entry when the add is skipped', async () => {
      openQueue('dlq-replay-skip-dlq');
      const queue = openQueue('dlq-replay-skip');
      startWorker(queue, alwaysFail, 'dlq-replay-skip-dlq');
      await failJobs(queue, 1);
      const [dlqJob] = await queue.getDeadLetterJobs();

      // The replay drops jobId, delay, deduplication and parent, so only a skipped add exercises this guard.
      const add = vi.spyOn(queue, 'add').mockResolvedValueOnce(null);
      await expect(queue.replayDeadLetterJob(dlqJob.id)).rejects.toThrow(
        'DLQ replay was skipped due to duplicate or deduplicated job constraints',
      );
      add.mockRestore();
      expect(await queue.getDeadLetterJob(dlqJob.id)).not.toBeNull();
    });
  });
});
