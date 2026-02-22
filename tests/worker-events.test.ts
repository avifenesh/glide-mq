import { describe, it, expect } from 'vitest';
import { TestQueue, TestWorker } from '../src/testing';

describe('TestWorker active and drained events', () => {
  it('should emit active before completed', async () => {
    const queue = new TestQueue('test-active');
    const events: string[] = [];

    const worker = new TestWorker(queue, async () => 'done');

    worker.on('active', (job: any, jobId: string) => {
      expect(job).toBeDefined();
      expect(typeof jobId).toBe('string');
      events.push('active');
    });
    worker.on('completed', () => events.push('completed'));

    const completed = new Promise<void>((resolve) => {
      worker.on('completed', () => resolve());
    });

    await queue.add('test-job', { value: 1 });
    await completed;

    expect(events).toEqual(['active', 'completed']);

    await worker.close();
    await queue.close();
  });

  it('should emit active before failed when processor throws', async () => {
    const queue = new TestQueue('test-active-fail');
    const events: string[] = [];

    const worker = new TestWorker(queue, async () => {
      throw new Error('boom');
    });

    worker.on('active', () => events.push('active'));
    worker.on('failed', () => events.push('failed'));

    const failed = new Promise<void>((resolve) => {
      worker.on('failed', () => resolve());
    });

    await queue.add('fail-job', { value: 1 });
    await failed;

    expect(events).toEqual(['active', 'failed']);

    await worker.close();
    await queue.close();
  });

  it('should emit drained after all jobs complete', async () => {
    const queue = new TestQueue('test-drained-multi');
    let drainedCount = 0;
    let completedCount = 0;

    const worker = new TestWorker(queue, async () => 'ok');

    const drained = new Promise<void>((resolve) => {
      worker.on('drained', () => {
        drainedCount++;
        if (completedCount === 3) resolve();
      });
    });
    worker.on('completed', () => completedCount++);

    await queue.add('job1', { v: 1 });
    await queue.add('job2', { v: 2 });
    await queue.add('job3', { v: 3 });

    await drained;

    expect(completedCount).toBe(3);
    expect(drainedCount).toBe(1);

    await worker.close();
    await queue.close();
  });

  it('should emit active with correct job data and matching id', async () => {
    const queue = new TestQueue('test-active-data');
    const activeJobs: any[] = [];

    const worker = new TestWorker(queue, async () => 'ok');

    worker.on('active', (job: any, jobId: string) => {
      activeJobs.push({ name: job.name, data: job.data, id: job.id, emittedId: jobId });
    });

    const completed = new Promise<void>((resolve) => {
      worker.on('completed', () => resolve());
    });

    await queue.add('my-job', { foo: 'bar' });
    await completed;

    expect(activeJobs).toHaveLength(1);
    expect(activeJobs[0].name).toBe('my-job');
    expect(activeJobs[0].data).toEqual({ foo: 'bar' });
    expect(activeJobs[0].emittedId).toBe(activeJobs[0].id);

    await worker.close();
    await queue.close();
  });

  it('should emit drained after single job completes', async () => {
    const queue = new TestQueue('test-drained-single');

    const worker = new TestWorker(queue, async () => 'ok');

    const drained = new Promise<void>((resolve) => {
      worker.on('drained', () => resolve());
    });

    await queue.add('job1', { value: 1 });
    await drained;

    await worker.close();
    await queue.close();
  });

  it('should handle concurrency > 1 correctly', async () => {
    const queue = new TestQueue('test-concurrent');
    const activeIds: string[] = [];
    let drainedCount = 0;
    let completedCount = 0;

    const worker = new TestWorker(queue, async () => 'ok', { concurrency: 2 });

    worker.on('active', (_job: any, jobId: string) => activeIds.push(jobId));
    worker.on('completed', () => completedCount++);

    const drained = new Promise<void>((resolve) => {
      worker.on('drained', () => {
        drainedCount++;
        if (completedCount === 2) resolve();
      });
    });

    await queue.add('job1', { v: 1 });
    await queue.add('job2', { v: 2 });

    await drained;

    expect(activeIds).toHaveLength(2);
    expect(completedCount).toBe(2);
    expect(drainedCount).toBe(1);

    await worker.close();
    await queue.close();
  });

  it('should use the same job object in active and completed events', async () => {
    const queue = new TestQueue('test-same-job');
    let activeJob: any = null;
    let completedJob: any = null;

    const worker = new TestWorker(queue, async () => 'ok');

    worker.on('active', (job: any) => { activeJob = job; });
    worker.on('completed', (job: any) => { completedJob = job; });

    const completed = new Promise<void>((resolve) => {
      worker.on('completed', () => resolve());
    });

    await queue.add('job1', { v: 1 });
    await completed;

    expect(activeJob).toBe(completedJob);

    await worker.close();
    await queue.close();
  });
});
