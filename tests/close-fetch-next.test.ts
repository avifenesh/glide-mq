/**
 * close(false) must not leave the next job claimed by completeAndFetchNext.
 *
 * concurrency=1 chains via completeAndFetchNext. If close() sets closing while
 * job A is running, CAF still claims job B, then the loop exits and drops B
 * in state=active with no processor.
 *
 * Run: npx vitest run tests/close-fetch-next.test.ts
 */
import { afterAll, beforeAll, expect, it } from 'vitest';
import { createCleanupClient, describeEachMode, flushQueue, waitFor } from './helpers/fixture';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');

describeEachMode('close(false) vs completeAndFetchNext', (CONNECTION) => {
  let cleanupClient: any;
  const queues: string[] = [];

  function uniqueQueue(prefix: string): string {
    const name = `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    queues.push(name);
    return name;
  }

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await Promise.all(queues.map((q) => flushQueue(cleanupClient, q).catch(() => {})));
    cleanupClient.close();
  });

  it('does not leave the chained next job active after close(false)', async () => {
    const Q = uniqueQueue('close-caf');
    const queue = new Queue(Q, { connection: CONNECTION });

    const jobA = await queue.add('task', { n: 1 });
    const jobB = await queue.add('task', { n: 2 });

    let releaseA!: () => void;
    const holdA = new Promise<void>((resolve) => {
      releaseA = resolve;
    });
    let aStarted = false;
    const processed: number[] = [];

    const worker = new Worker(
      Q,
      async (job: { data: { n: number } }) => {
        if (job.data.n === 1) {
          aStarted = true;
          await holdA;
        }
        processed.push(job.data.n);
        return 'ok';
      },
      {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 50,
        stalledInterval: 60_000,
      },
    );
    worker.on('error', () => {});

    await waitFor(() => aStarted);

    const closePromise = worker.close(false);
    releaseA();
    await closePromise;

    expect(processed).toEqual([1]);
    expect(await jobA.getState()).toBe('completed');
    expect(await jobB.getState()).not.toBe('active');

    const recovered: number[] = [];
    const worker2 = new Worker(
      Q,
      async (job: { data: { n: number } }) => {
        recovered.push(job.data.n);
        return 'ok';
      },
      {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 50,
        stalledInterval: 60_000,
      },
    );
    worker2.on('error', () => {});

    try {
      await waitFor(() => recovered.includes(2), 4000, 50);
      expect(await jobB.getState()).toBe('completed');
    } finally {
      await worker2.close(true);
      await queue.close();
    }
  }, 15000);
});
