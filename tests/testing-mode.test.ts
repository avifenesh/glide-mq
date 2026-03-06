/**
 * Pure unit tests for the in-memory testing mode.
 * No Valkey required.
 *
 * Run: npx vitest run tests/testing-mode.test.ts
 */
import { describe, it, expect, afterEach } from 'vitest';
import path from 'path';
import { TestQueue, TestWorker, TestJob } from '../src/testing';

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
    await new Promise((r) => setTimeout(r, 50));
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
});

describe('TestWorker', () => {
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
    await new Promise((r) => setTimeout(r, 50));

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
    await new Promise((r) => setTimeout(r, 50));

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
    await new Promise((r) => setTimeout(r, 200));

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
    await new Promise((r) => setTimeout(r, 100));

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
    await new Promise((r) => setTimeout(r, 100));

    // attempts=2 means max 2 tries. 2 failures = exhausted.
    expect(failures).toHaveLength(1);

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

    await new Promise((r) => setTimeout(r, 50));
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
    await new Promise((r) => setTimeout(r, 50));

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

  it('is a no-op when delay is already 0', async () => {
    queue = new TestQueue('promote-noop');
    const job = await queue.add('task', { x: 1 });
    expect(job!.opts.delay).toBeUndefined();
    await job!.promote();
    expect(job!.opts.delay).toBe(0);
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

    await new Promise((r) => setTimeout(r, 100));
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

    await new Promise((r) => setTimeout(r, 100));
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

    await new Promise((r) => setTimeout(r, 100));
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

    await new Promise((r) => setTimeout(r, 100));
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
    await new Promise((r) => setTimeout(r, 100));

    // Job should have failed (no retries configured)
    expect((await queue.getJobCounts()).failed).toBe(1);

    const retried = await queue.retryJobs();
    expect(retried).toBe(1);

    await new Promise((r) => setTimeout(r, 100));
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
    await new Promise((r) => setTimeout(r, 50));

    const during = await queue.getWorkers();
    expect(during).toHaveLength(1);
    expect(during[0].activeJobs).toBe(1);

    finishJob();
    await new Promise((r) => setTimeout(r, 50));

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
    await new Promise<void>((r) => setTimeout(r, 50));

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

    await new Promise<void>((r) => setTimeout(r, 50));

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
    await new Promise<void>((r) => setTimeout(r, 50));
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

    await queue.upsertJobScheduler('later', { every: 200, startDate: Date.now() + 500, limit: 1 }, { name: 'later-job' });
    await new Promise<void>((r) => setTimeout(r, 20));
    await queue.upsertJobScheduler('sooner', { every: 200, startDate: Date.now() + 60, limit: 1 }, { name: 'sooner-job' });

    const deadline = Date.now() + 500;
    while (processed.length < 1 && Date.now() < deadline) {
      await new Promise<void>((r) => setTimeout(r, 20));
    }

    expect(processed[0]).toBe('sooner-job');
  });
});
