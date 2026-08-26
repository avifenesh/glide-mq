/**
 * TestWorker batch.timeout leftover/drain/close paths.
 * Lives outside tests/testing-mode.test.ts so integration coverage collects it.
 *
 * Run: npx vitest run tests/testworker-batch-flush.test.ts
 */
import { afterEach, describe, expect, it } from 'vitest';
import { TestQueue, TestWorker, TestJob } from '../src/testing';
import { waitFor } from './helpers/fixture';

describe('TestWorker pending batch flush', () => {
  let queue: TestQueue;
  let worker: TestWorker;

  afterEach(async () => {
    if (worker) await worker.close();
    if (queue) await queue.close();
  });

  it('flushes a partial batch after batch.timeout', async () => {
    queue = new TestQueue('cov-batch-timeout');
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
  });

  it('flushes leftover pending jobs after a full batch takes only part of them', async () => {
    queue = new TestQueue('cov-batch-remainder');
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
    queue = new TestQueue('cov-batch-drain');
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
    queue = new TestQueue('cov-batch-close-handoff');
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

  it('does not execute drained jobs when later adds fill the batch', async () => {
    queue = new TestQueue('cov-batch-drain-fill');
    const processed: number[] = [];

    await queue.add('a', { i: 1 });
    await queue.add('b', { i: 2 });

    worker = new TestWorker(
      queue,
      async (jobs: TestJob[]) => {
        processed.push(...jobs.map((j) => (j.data as { i: number }).i));
        return jobs.map(() => 'ok');
      },
      { batch: { size: 5, timeout: 5000 } },
    );

    await waitFor(async () => (await queue.getJobCounts()).waiting === 2, 500, 10);
    await queue.drain();
    await queue.addBulk([
      { name: 'c', data: { i: 3 } },
      { name: 'd', data: { i: 4 } },
      { name: 'e', data: { i: 5 } },
      { name: 'f', data: { i: 6 } },
      { name: 'g', data: { i: 7 } },
    ]);

    await waitFor(async () => (await queue.getJobCounts()).completed === 5, 2000, 20);
    expect(processed.sort((a, b) => a - b)).toEqual([3, 4, 5, 6, 7]);
  });

  it('does not hand drained pending jobs to a peer on close', async () => {
    queue = new TestQueue('cov-batch-drain-close');
    const processed: number[] = [];

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
    await queue.drain();

    const peer = new TestWorker(
      queue,
      async (jobs: TestJob[]) => {
        processed.push(...jobs.map((j) => (j.data as { i: number }).i));
        return jobs.map(() => 'ok');
      },
      { batch: { size: 5, timeout: 50 } },
    );

    await worker.close();
    worker = peer;
    await new Promise((r) => setTimeout(r, 150));
    expect(processed).toEqual([]);
    expect((await queue.getJobCounts()).completed).toBe(0);
  });

  it('flushes reserved jobs on timeout but does not claim more while paused', async () => {
    queue = new TestQueue('cov-batch-pause-fill');
    const processed: number[] = [];

    await queue.add('a', { i: 1 });
    await queue.add('b', { i: 2 });

    worker = new TestWorker(
      queue,
      async (jobs: TestJob[]) => {
        processed.push(...jobs.map((j) => (j.data as { i: number }).i));
        return jobs.map(() => 'ok');
      },
      { batch: { size: 5, timeout: 50 } },
    );

    await waitFor(async () => (await queue.getJobCounts()).waiting === 2, 500, 10);
    await queue.pause();
    await queue.add('c', { i: 3 });
    await waitFor(async () => (await queue.getJobCounts()).completed === 2, 2000, 20);
    expect(processed.sort((a, b) => a - b)).toEqual([1, 2]);
    expect((await queue.getJobCounts()).waiting).toBe(1);
  });
});
