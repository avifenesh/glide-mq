/**
 * Priority/LIFO jobs are dispatched with entryId ''. Job.moveToDelayed,
 * suspend, and moveToWaitingChildren currently treat that as "not in a worker".
 *
 * Run: npx vitest run tests/list-entryid-guard.test.ts
 */
import { afterAll, beforeAll, expect, it } from 'vitest';
import { createCleanupClient, describeEachMode, flushQueue, waitFor } from './helpers/fixture';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');

describeEachMode('list-job worker methods', (CONNECTION) => {
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

  it('lets a priority job call moveToDelayed from the processor', async () => {
    const Q = uniqueQueue('list-entryid-delay');
    const queue = new Queue(Q, { connection: CONNECTION });
    const job = await queue.add('task', { n: 1 }, { priority: 1 });

    const errors: Error[] = [];
    const worker = new Worker(
      Q,
      async (active) => {
        await active.moveToDelayed(Date.now() + 60_000);
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 50, stalledInterval: 60_000 },
    );
    worker.on('error', () => {});
    worker.on('failed', (_job: unknown, err: Error) => errors.push(err));

    try {
      await waitFor(
        async () => {
          const state = await job.getState();
          return state === 'delayed' || state === 'failed';
        },
        4000,
        50,
      );
      expect(await job.getState()).toBe('delayed');
      expect(errors).toEqual([]);
    } finally {
      await worker.close(true);
      await queue.close();
    }
  }, 15000);

  it('lets a priority job call suspend from the processor', async () => {
    const Q = uniqueQueue('list-entryid-suspend');
    const queue = new Queue(Q, { connection: CONNECTION });
    const job = await queue.add('task', { n: 1 }, { priority: 1 });

    const errors: Error[] = [];
    const worker = new Worker(
      Q,
      async (active) => {
        await active.suspend({ reason: 'wait' });
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 50, stalledInterval: 60_000 },
    );
    worker.on('error', () => {});
    worker.on('failed', (_job: unknown, err: Error) => errors.push(err));

    try {
      await waitFor(
        async () => {
          const state = await job.getState();
          return state === 'suspended' || state === 'failed';
        },
        4000,
        50,
      );
      expect(await job.getState()).toBe('suspended');
      expect(errors).toEqual([]);
    } finally {
      await worker.close(true);
      await queue.close();
    }
  }, 15000);
});
