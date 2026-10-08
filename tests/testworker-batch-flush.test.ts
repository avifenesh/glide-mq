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

/**
 * Batch processor whose calls block until released, recording how many
 * processors and how many jobs are in flight at once.
 */
function gatedBatchProcessor() {
  const gates: Array<() => void> = [];
  let opened = false;
  const state = { running: 0, maxRunning: 0, jobsInFlight: 0, maxJobsInFlight: 0, batchSizes: [] as number[] };
  const processor = async (jobs: TestJob[]) => {
    state.batchSizes.push(jobs.length);
    state.running += 1;
    state.jobsInFlight += jobs.length;
    state.maxRunning = Math.max(state.maxRunning, state.running);
    state.maxJobsInFlight = Math.max(state.maxJobsInFlight, state.jobsInFlight);
    if (!opened) await new Promise<void>((resolve) => gates.push(resolve));
    state.running -= 1;
    state.jobsInFlight -= jobs.length;
    return jobs.map(() => 'ok');
  };
  return {
    processor,
    state,
    releaseOldest: () => gates.shift()?.(),
    openAll: () => {
      opened = true;
      for (const resolve of gates.splice(0)) resolve();
    },
  };
}

const jobsOf = (n: number) => Array.from({ length: n }, (_, i) => ({ name: `j${i}`, data: { i } }));
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe('TestWorker batch concurrency limit', () => {
  let queue: TestQueue;
  let worker: TestWorker;
  let gate: ReturnType<typeof gatedBatchProcessor>;

  afterEach(async () => {
    gate.openAll();
    if (worker) await worker.close();
    if (queue) await queue.close();
  });

  it('runs one batch at a time at concurrency 1 when a full batch follows a slow partial batch', async () => {
    queue = new TestQueue('cov-batch-c1-partial-then-full');
    gate = gatedBatchProcessor();
    await queue.addBulk(jobsOf(2));
    worker = new TestWorker(queue, gate.processor, { concurrency: 1, batch: { size: 5, timeout: 50 } });
    await waitFor(() => gate.state.batchSizes.length === 1, 2000, 10);

    await queue.addBulk(jobsOf(5));
    await sleep(150);
    expect(gate.state.maxRunning).toBe(1);
    expect(gate.state.batchSizes).toEqual([2]);

    gate.releaseOldest();
    await waitFor(() => gate.state.batchSizes.length === 2, 2000, 10);
    gate.openAll();
    await waitFor(async () => (await queue.getJobCounts()).completed === 7, 2000, 20);
    expect(gate.state.maxRunning).toBe(1);
    expect(gate.state.batchSizes).toEqual([2, 5]);
  });

  it('runs one batch at a time at concurrency 1 when a timed flush follows a slow partial batch', async () => {
    queue = new TestQueue('cov-batch-c1-partial-then-flush');
    gate = gatedBatchProcessor();
    await queue.addBulk(jobsOf(2));
    worker = new TestWorker(queue, gate.processor, { concurrency: 1, batch: { size: 5, timeout: 50 } });
    await waitFor(() => gate.state.batchSizes.length === 1, 2000, 10);

    await queue.add('late', { i: 99 });
    await sleep(150);
    expect(gate.state.maxRunning).toBe(1);
    expect(gate.state.batchSizes).toEqual([2]);

    gate.openAll();
    await waitFor(async () => (await queue.getJobCounts()).completed === 3, 2000, 20);
    expect(gate.state.maxRunning).toBe(1);
    expect(gate.state.batchSizes).toEqual([2, 1]);
  });

  it('claims only the remaining budget when a full batch arrives and leaves the rest waiting', async () => {
    queue = new TestQueue('cov-batch-budget-full');
    gate = gatedBatchProcessor();
    await queue.addBulk(jobsOf(5));
    worker = new TestWorker(queue, gate.processor, { concurrency: 2, batch: { size: 5, timeout: 50 } });
    await waitFor(() => gate.state.batchSizes.length === 1, 2000, 10);
    await queue.addBulk(jobsOf(3));
    await waitFor(() => gate.state.batchSizes.length === 2, 2000, 10);

    // 8 of 10 job slots are in flight: only 2 of the 5 new jobs are claimed and started.
    await queue.addBulk(jobsOf(5));
    await waitFor(() => gate.state.batchSizes.length === 3, 2000, 10);
    await sleep(100);
    expect(gate.state.batchSizes).toEqual([5, 3, 2]);
    expect(gate.state.maxJobsInFlight).toBe(10);
    expect((await queue.getJobCounts()).waiting).toBe(3);

    gate.releaseOldest();
    await waitFor(() => gate.state.batchSizes.length === 4, 2000, 10);
    gate.openAll();
    await waitFor(async () => (await queue.getJobCounts()).completed === 13, 2000, 20);
    expect(gate.state.batchSizes).toEqual([5, 3, 2, 3]);
    expect(gate.state.maxJobsInFlight).toBeLessThanOrEqual(10);
  });

  it('claims only the remaining budget on a timed refill and flushes the leftover after the timeout', async () => {
    queue = new TestQueue('cov-batch-budget-flush');
    gate = gatedBatchProcessor();
    await queue.addBulk(jobsOf(5));
    worker = new TestWorker(queue, gate.processor, { concurrency: 2, batch: { size: 5, timeout: 50 } });
    await waitFor(() => gate.state.batchSizes.length === 1, 2000, 10);
    await queue.addBulk(jobsOf(3));
    await waitFor(() => gate.state.batchSizes.length === 2, 2000, 10);

    await queue.addBulk(jobsOf(3));
    await waitFor(() => gate.state.batchSizes.length === 3, 2000, 10);
    await sleep(100);
    expect(gate.state.batchSizes).toEqual([5, 3, 2]);
    expect((await queue.getJobCounts()).waiting).toBe(1);

    gate.releaseOldest();
    await waitFor(() => gate.state.batchSizes.length === 4, 2000, 10);
    gate.openAll();
    await waitFor(async () => (await queue.getJobCounts()).completed === 11, 2000, 20);
    expect(gate.state.batchSizes).toEqual([5, 3, 2, 1]);
    expect(gate.state.maxJobsInFlight).toBeLessThanOrEqual(10);
  });

  it('claims only the remaining budget without batch.timeout', async () => {
    queue = new TestQueue('cov-batch-budget-immediate');
    gate = gatedBatchProcessor();
    worker = new TestWorker(queue, gate.processor, { concurrency: 2, batch: { size: 5 } });
    // Without a timeout every wake-up starts what is waiting; pause/resume hands the worker n jobs at once.
    const feed = async (n: number) => {
      await queue.pause();
      await queue.addBulk(jobsOf(n));
      await queue.resume();
    };
    await feed(5);
    await waitFor(() => gate.state.batchSizes.length === 1, 2000, 10);
    await feed(3);
    await waitFor(() => gate.state.batchSizes.length === 2, 2000, 10);

    await feed(3);
    await waitFor(() => gate.state.batchSizes.length === 3, 2000, 10);
    await sleep(100);
    expect(gate.state.batchSizes).toEqual([5, 3, 2]);
    expect(gate.state.maxJobsInFlight).toBe(10);
    expect((await queue.getJobCounts()).waiting).toBe(1);

    gate.releaseOldest();
    await waitFor(() => gate.state.batchSizes.length === 4, 2000, 10);
    gate.openAll();
    await waitFor(async () => (await queue.getJobCounts()).completed === 11, 2000, 20);
    expect(gate.state.batchSizes).toEqual([5, 3, 2, 1]);
    expect(gate.state.maxJobsInFlight).toBeLessThanOrEqual(10);
  });

  it('leaves jobs beyond the budget visible to another worker on the same queue', async () => {
    queue = new TestQueue('cov-batch-budget-handoff');
    gate = gatedBatchProcessor();
    await queue.addBulk(jobsOf(5));
    worker = new TestWorker(queue, gate.processor, { concurrency: 2, batch: { size: 5, timeout: 50 } });
    await waitFor(() => gate.state.batchSizes.length === 1, 2000, 10);
    await queue.addBulk(jobsOf(3));
    await waitFor(() => gate.state.batchSizes.length === 2, 2000, 10);
    await queue.addBulk(jobsOf(5));
    await waitFor(() => gate.state.batchSizes.length === 3, 2000, 10);

    const peerSizes: number[] = [];
    const peer = new TestWorker(
      queue,
      async (jobs: TestJob[]) => {
        peerSizes.push(jobs.length);
        return jobs.map(() => 'ok');
      },
      { batch: { size: 5, timeout: 20 } },
    );
    try {
      await waitFor(() => peerSizes.reduce((a, b) => a + b, 0) === 3, 2000, 10);
      expect(peerSizes).toEqual([3]);
      expect((await queue.getJobCounts()).waiting).toBe(0);
    } finally {
      await peer.close();
    }
  });

  it('does not claim more jobs when a running batch settles while the queue is paused', async () => {
    queue = new TestQueue('cov-batch-settle-paused');
    gate = gatedBatchProcessor();
    await queue.addBulk(jobsOf(5));
    worker = new TestWorker(queue, gate.processor, { concurrency: 1, batch: { size: 5, timeout: 50 } });
    await waitFor(() => gate.state.batchSizes.length === 1, 2000, 10);
    await queue.addBulk(jobsOf(2));
    await queue.pause();
    gate.openAll();
    await waitFor(async () => (await queue.getJobCounts()).completed === 5, 2000, 20);
    await sleep(100);
    expect(gate.state.batchSizes).toEqual([5]);
    expect((await queue.getJobCounts()).waiting).toBe(2);
  });

  it('does not claim more jobs when a running batch settles after close()', async () => {
    queue = new TestQueue('cov-batch-settle-closed');
    gate = gatedBatchProcessor();
    await queue.addBulk(jobsOf(5));
    worker = new TestWorker(queue, gate.processor, { concurrency: 1, batch: { size: 5, timeout: 50 } });
    await waitFor(() => gate.state.batchSizes.length === 1, 2000, 10);
    await queue.addBulk(jobsOf(2));
    const closing = worker.close();
    gate.openAll();
    await closing;
    await sleep(100);
    expect(gate.state.batchSizes).toEqual([5]);
  });
});
