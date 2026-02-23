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
