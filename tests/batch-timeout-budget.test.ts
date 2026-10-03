/**
 * A batch.timeout refill must not claim past the worker's in-flight budget.
 * The poll caps its first read at min(prefetch - activeCount, batch.size), and
 * every refill read inside the timeout window is held to the same budget.
 *
 * Requires: valkey-server running on localhost:6379 and cluster on :7000-7005
 *
 * Run: npx vitest run tests/batch-timeout-budget.test.ts
 */
import { it, expect, beforeAll, afterAll } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { Broadcast } = require('../dist/broadcast') as typeof import('../src/broadcast');
const { BroadcastWorker } = require('../dist/broadcast-worker') as typeof import('../src/broadcast-worker');

import { describeEachMode, createCleanupClient, flushQueue, waitFor } from './helpers/fixture';

/** Batch processor that blocks until released and records batch sizes and jobs in flight. */
function gatedBatchProcessor() {
  const gates: Array<() => void> = [];
  let opened = false;
  const state = { jobsInFlight: 0, maxJobsInFlight: 0, batchSizes: [] as number[] };
  const processor = async (jobs: any[]) => {
    state.batchSizes.push(jobs.length);
    state.jobsInFlight += jobs.length;
    state.maxJobsInFlight = Math.max(state.maxJobsInFlight, state.jobsInFlight);
    if (!opened) await new Promise<void>((resolve) => gates.push(resolve));
    state.jobsInFlight -= jobs.length;
    return jobs.map(() => 'ok');
  };
  return {
    processor,
    state,
    openAll: () => {
      opened = true;
      for (const resolve of gates.splice(0)) resolve();
    },
  };
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const total = (sizes: number[]) => sizes.reduce((a, b) => a + b, 0);

describeEachMode('batch.timeout refill budget', (CONNECTION) => {
  const Q = 'test-batch-budget-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    for (const s of ['-worker', '-prefetch', '-broadcast']) await flushQueue(cleanupClient, Q + s);
    cleanupClient.close();
  });

  it('Worker keeps at most concurrency * batch.size jobs in flight when a refill fills a batch', async () => {
    const qName = Q + '-worker';
    const queue = new Queue(qName, { connection: CONNECTION });
    const gate = gatedBatchProcessor();
    const worker = new Worker(qName, gate.processor, {
      connection: CONNECTION,
      concurrency: 2,
      blockTimeout: 500,
      batch: { size: 5, timeout: 300 },
    });
    worker.on('error', () => {});
    try {
      await worker.waitUntilReady();
      const add = (n: number, tag: string) =>
        queue.addBulk(Array.from({ length: n }, (_, i) => ({ name: tag, data: { i } })));

      await add(5, 'a');
      await waitFor(() => gate.state.batchSizes.length === 1, 5000, 10);
      await add(3, 'b');
      await waitFor(() => gate.state.batchSizes.length === 2, 5000, 10);

      // 8 of 10 job slots are in flight. The next poll may claim 2; the refill
      // inside the timeout window must not top the batch up to 5.
      await add(5, 'c');
      await waitFor(() => gate.state.batchSizes.length === 3, 5000, 10);
      await sleep(600);
      expect(gate.state.batchSizes).toEqual([5, 3, 2]);
      expect(gate.state.maxJobsInFlight).toBe(10);

      gate.openAll();
      await waitFor(async () => (await queue.getJobCounts()).completed === 13, 10000, 50);
      expect(gate.state.maxJobsInFlight).toBeLessThanOrEqual(10);
    } finally {
      gate.openAll();
      await worker.close();
      await queue.close();
    }
  }, 30000);

  it('Worker keeps each batch within a prefetch lower than batch.size', async () => {
    const qName = Q + '-prefetch';
    const queue = new Queue(qName, { connection: CONNECTION });
    const gate = gatedBatchProcessor();
    gate.openAll();
    const worker = new Worker(qName, gate.processor, {
      connection: CONNECTION,
      prefetch: 2,
      blockTimeout: 500,
      batch: { size: 5, timeout: 300 },
    });
    worker.on('error', () => {});
    try {
      await worker.waitUntilReady();
      await queue.addBulk(Array.from({ length: 5 }, (_, i) => ({ name: 'p', data: { i } })));
      await waitFor(() => total(gate.state.batchSizes) === 5, 10000, 20);
      for (const size of gate.state.batchSizes) expect(size).toBeLessThanOrEqual(2);
    } finally {
      await worker.close();
      await queue.close();
    }
  }, 30000);

  it('BroadcastWorker keeps at most concurrency * batch.size messages in flight when a refill fills a batch', async () => {
    const qName = Q + '-broadcast';
    const broadcast = new Broadcast(qName, { connection: CONNECTION });
    const gate = gatedBatchProcessor();
    const worker = new BroadcastWorker(qName, gate.processor, {
      connection: CONNECTION,
      subscription: 'budget',
      concurrency: 2,
      blockTimeout: 500,
      batch: { size: 5, timeout: 300 },
    });
    worker.on('error', () => {});
    try {
      await worker.waitUntilReady();
      const publish = async (n: number, tag: string) => {
        for (let i = 0; i < n; i++) await broadcast.publish(tag, { i });
      };

      await publish(5, 'a');
      await waitFor(() => gate.state.batchSizes.length === 1, 5000, 10);
      await publish(3, 'b');
      await waitFor(() => gate.state.batchSizes.length === 2, 5000, 10);

      await publish(5, 'c');
      await waitFor(() => gate.state.batchSizes.length === 3, 5000, 10);
      await sleep(600);
      expect(gate.state.batchSizes).toEqual([5, 3, 2]);
      expect(gate.state.maxJobsInFlight).toBe(10);

      gate.openAll();
      await waitFor(() => total(gate.state.batchSizes) === 13, 10000, 50);
      expect(gate.state.maxJobsInFlight).toBeLessThanOrEqual(10);
    } finally {
      gate.openAll();
      await worker.close();
      await broadcast.close();
    }
  }, 30000);
});
