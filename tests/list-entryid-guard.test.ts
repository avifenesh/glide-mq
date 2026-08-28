/**
 * Priority/LIFO jobs are dispatched with entryId ''. Job.moveToDelayed,
 * suspend, and moveToWaitingChildren currently treat that as "not in a worker".
 *
 * Run: npx vitest run tests/list-entryid-guard.test.ts
 */
import path from 'path';
import { afterAll, beforeAll, expect, it } from 'vitest';
import { createCleanupClient, describeEachMode, flushQueue, waitFor } from './helpers/fixture';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { BatchError, UnrecoverableError } = require('../dist/errors') as typeof import('../src/errors');

const DELAY_PROCESSOR = path.resolve(__dirname, 'fixtures/processors/move-to-delayed-future.js');

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

  it('keeps a priority job failed after moveToFailed returns normally', async () => {
    const Q = uniqueQueue('list-entryid-fail');
    const queue = new Queue(Q, { connection: CONNECTION });
    const job = await queue.add('task', { n: 1 }, { priority: 1 });

    const worker = new Worker(
      Q,
      async (active) => {
        await active.moveToFailed(new Error('nope'));
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 50, stalledInterval: 60_000 },
    );
    worker.on('error', () => {});

    try {
      await waitFor(
        async () => {
          const state = await job.getState();
          return state === 'failed' || state === 'completed';
        },
        4000,
        50,
      );
      expect(await job.getState()).toBe('failed');
    } finally {
      await worker.close(true);
      await queue.close();
    }
  }, 15000);

  it('emits failed and does not complete after moveToFailed plus a later throw', async () => {
    const Q = uniqueQueue('list-entryid-fail-throw');
    const queue = new Queue(Q, { connection: CONNECTION });
    const job = await queue.add('task', { n: 1 }, { priority: 1 });
    const failed: Error[] = [];

    const worker = new Worker(
      Q,
      async (active) => {
        await active.moveToFailed(new Error('nope'));
        throw new Error('cleanup');
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 50, stalledInterval: 60_000 },
    );
    worker.on('error', () => {});
    worker.on('failed', (_job: unknown, err: Error) => failed.push(err));
    worker.on('completed', () => {
      throw new Error('should not complete');
    });

    try {
      await waitFor(async () => (await job.getState()) === 'failed', 4000, 50);
      expect(await job.getState()).toBe('failed');
      expect(failed.map((e) => e.message)).toEqual(['nope']);
    } finally {
      await worker.close(true);
      await queue.close();
    }
  }, 15000);

  it('does not retry explicit discarded or unrecoverable failures', async () => {
    const Q = uniqueQueue('list-entryid-no-retry');
    const queue = new Queue(Q, { connection: CONNECTION });
    const [discarded, unrecoverable] = await queue.addBulk([
      { name: 'task', data: { mode: 'discard' }, opts: { priority: 1, attempts: 3, backoff: 10 } },
      { name: 'task', data: { mode: 'unrecoverable' }, opts: { priority: 1, attempts: 3, backoff: 10 } },
    ]);
    const calls = new Map<string, number>();

    const worker = new Worker(
      Q,
      async (active) => {
        calls.set(active.id, (calls.get(active.id) ?? 0) + 1);
        if (active.data.mode === 'discard') {
          active.discard();
          await active.moveToFailed(new Error('discarded'));
        } else {
          await active.moveToFailed(new UnrecoverableError('unrecoverable'));
        }
      },
      { connection: CONNECTION, concurrency: 2, blockTimeout: 50, stalledInterval: 100, lockDuration: 100 },
    );
    worker.on('error', () => {});

    try {
      await waitFor(
        async () => (await discarded.getState()) === 'failed' && (await unrecoverable.getState()) === 'failed',
        4000,
        50,
      );
      await new Promise((resolve) => setTimeout(resolve, 500));
      expect(calls.get(discarded.id)).toBe(1);
      expect(calls.get(unrecoverable.id)).toBe(1);
    } finally {
      await worker.close(true);
      await queue.close();
    }
  }, 15000);

  it('routes a terminal moveToFailed job to the dead-letter queue', async () => {
    const Q = uniqueQueue('list-entryid-fail-dlq');
    const DLQ = `${Q}-dead`;
    const queue = new Queue(Q, { connection: CONNECTION, deadLetterQueue: { name: DLQ } });
    const job = await queue.add('task', { n: 1 }, { priority: 1 });

    const worker = new Worker(
      Q,
      async (active) => {
        await active.moveToFailed(new Error('nope'));
      },
      {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 50,
        stalledInterval: 60_000,
        deadLetterQueue: { name: DLQ },
      },
    );
    worker.on('error', () => {});

    try {
      await waitFor(async () => (await queue.getDeadLetterJobs()).length > 0, 4000, 50);
      const dlqJobs = await queue.getDeadLetterJobs();
      expect(dlqJobs[0]?.data?.originalJobId).toBe(job.id);
      expect(dlqJobs[0]?.data?.failedReason).toBe('nope');
    } finally {
      await worker.close(true);
      await queue.close();
      await flushQueue(cleanupClient, DLQ);
    }
  }, 15000);

  it('advances repeatAfterComplete after an explicit moveToFailed', async () => {
    const Q = uniqueQueue('list-entryid-fail-rac');
    const queue = new Queue(Q, { connection: CONNECTION });
    await queue.upsertJobScheduler('rac-mtf', { repeatAfterComplete: 200 }, { name: 'task', data: { n: 1 } });

    let jobCount = 0;
    const worker = new Worker(
      Q,
      async (active) => {
        jobCount++;
        if (jobCount === 1) {
          await active.moveToFailed(new Error('nope'));
          return;
        }
        return 'ok';
      },
      {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 50,
        stalledInterval: 60_000,
        promotionInterval: 100,
      },
    );
    worker.on('error', () => {});

    try {
      await waitFor(() => jobCount >= 2, 6000, 50);
      expect(jobCount).toBeGreaterThanOrEqual(2);
    } finally {
      await worker.close(true);
      await queue.close();
    }
  }, 15000);

  it('keeps a resumed priority job failed when onResume calls moveToFailed', async () => {
    const Q = uniqueQueue('list-entryid-resume-fail');
    const queue = new Queue(Q, { connection: CONNECTION });
    const job = await queue.add('task', { n: 1 }, { priority: 1 });
    let captured: any;

    const worker = new Worker(
      Q,
      async (active) => {
        if (active.signals.length === 0) {
          captured = active;
          await active.suspend({
            onResume: async () => {
              await captured.moveToFailed(new Error('resume-failed'));
            },
          });
        }
        return 'completed';
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 50, stalledInterval: 100, lockDuration: 100 },
    );
    worker.on('error', () => {});

    try {
      await waitFor(async () => (await queue.getSuspendInfo(job.id)) !== null, 4000, 50);
      await queue.signal(job.id, 'resume');
      await waitFor(async () => ['failed', 'completed'].includes(await job.getState()), 4000, 50);
      await new Promise((resolve) => setTimeout(resolve, 500));
      expect(await job.getState()).toBe('failed');
    } finally {
      await worker.close(true);
      await queue.close();
    }
  }, 15000);

  it('preserves delayed data requested from onResume', async () => {
    const Q = uniqueQueue('list-entryid-resume-delay');
    const queue = new Queue(Q, { connection: CONNECTION });
    const job = await queue.add('task', { step: 'start' }, { priority: 1 });
    let captured: any;

    const worker = new Worker(
      Q,
      async (active) => {
        if (active.signals.length === 0) {
          captured = active;
          await active.suspend({
            onResume: async () => captured.moveToDelayed(Date.now() + 60_000, 'next'),
          });
        }
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 50, stalledInterval: 60_000 },
    );
    worker.on('error', () => {});

    try {
      await waitFor(async () => (await queue.getSuspendInfo(job.id)) !== null, 4000, 50);
      await queue.signal(job.id, 'resume');
      await waitFor(async () => (await job.getState()) === 'delayed', 4000, 50);
      const refreshed = await queue.getJob(job.id);
      expect(refreshed?.data.step).toBe('next');
    } finally {
      await worker.close(true);
      await queue.close();
    }
  }, 15000);

  it('preserves a second suspend callback requested from onResume', async () => {
    const Q = uniqueQueue('list-entryid-resume-suspend');
    const queue = new Queue(Q, { connection: CONNECTION });
    const job = await queue.add('task', { n: 1 }, { priority: 1 });
    let captured: any;
    let secondResumeCalled = false;

    const worker = new Worker(
      Q,
      async (active) => {
        if (active.signals.length === 0) {
          captured = active;
          await active.suspend({
            onResume: async () =>
              captured.suspend({
                reason: 'second',
                onResume: async () => {
                  secondResumeCalled = true;
                  return 'done';
                },
              }),
          });
        }
        return 'processor';
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 50, stalledInterval: 60_000 },
    );
    worker.on('error', () => {});

    try {
      await waitFor(async () => (await queue.getSuspendInfo(job.id)) !== null, 4000, 50);
      await queue.signal(job.id, 'first');
      await waitFor(async () => (await queue.getSuspendInfo(job.id))?.reason === 'second', 4000, 50);
      await queue.signal(job.id, 'second');
      await waitFor(async () => (await job.getState()) === 'completed', 4000, 50);
      expect(secondResumeCalled).toBe(true);
    } finally {
      await worker.close(true);
      await queue.close();
    }
  }, 15000);

  it('preserves discard requested from onResume', async () => {
    const Q = uniqueQueue('list-entryid-resume-discard');
    const queue = new Queue(Q, { connection: CONNECTION });
    const job = await queue.add('task', { n: 1 }, { priority: 1, attempts: 3, backoff: 10 });
    let captured: any;
    let processorCalls = 0;

    const worker = new Worker(
      Q,
      async (active) => {
        processorCalls++;
        if (active.signals.length === 0) {
          captured = active;
          await active.suspend({
            onResume: async () => {
              captured.discard();
              throw new Error('discarded-resume');
            },
          });
        }
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 50, stalledInterval: 100, lockDuration: 100 },
    );
    worker.on('error', () => {});

    try {
      await waitFor(async () => (await queue.getSuspendInfo(job.id)) !== null, 4000, 50);
      await queue.signal(job.id, 'resume');
      await waitFor(async () => (await job.getState()) === 'failed', 4000, 50);
      await new Promise((resolve) => setTimeout(resolve, 500));
      expect(await job.getState()).toBe('failed');
      expect(processorCalls).toBe(1);
    } finally {
      await worker.close(true);
      await queue.close();
    }
  }, 15000);

  it('preserves explicit failure from a second chained onResume', async () => {
    const Q = uniqueQueue('list-entryid-resume-chain-fail');
    const queue = new Queue(Q, { connection: CONNECTION });
    const job = await queue.add('task', { n: 1 }, { priority: 1 });
    let captured: any;

    const worker = new Worker(
      Q,
      async (active) => {
        if (active.signals.length === 0) {
          captured = active;
          await active.suspend({
            onResume: async () =>
              captured.suspend({
                reason: 'second',
                onResume: async () => {
                  await captured.moveToFailed(new Error('second-resume-failed'));
                },
              }),
          });
        }
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 50, stalledInterval: 100, lockDuration: 100 },
    );
    worker.on('error', () => {});

    try {
      await waitFor(async () => (await queue.getSuspendInfo(job.id)) !== null, 4000, 50);
      await queue.signal(job.id, 'first');
      await waitFor(async () => (await queue.getSuspendInfo(job.id))?.reason === 'second', 4000, 50);
      await queue.signal(job.id, 'second');
      await waitFor(async () => ['failed', 'completed'].includes(await job.getState()), 4000, 50);
      await new Promise((resolve) => setTimeout(resolve, 500));
      expect(await job.getState()).toBe('failed');
    } finally {
      await worker.close(true);
      await queue.close();
    }
  }, 15000);

  it('does not complete a batch job that already called moveToFailed', async () => {
    const Q = uniqueQueue('list-entryid-fail-batch');
    const queue = new Queue(Q, { connection: CONNECTION });
    const [a, b] = await queue.addBulk([
      { name: 'task', data: { n: 1 }, opts: { priority: 1 } },
      { name: 'task', data: { n: 2 }, opts: { priority: 1 } },
    ]);
    const failedIds: string[] = [];
    const completedIds: string[] = [];

    const worker = new Worker(
      Q,
      async (jobs: Array<{ id: string; moveToFailed: (err: Error) => Promise<void> }>) => {
        await jobs[0].moveToFailed(new Error('nope'));
        throw new BatchError([new Error('other'), 'should-not-complete-first']);
      },
      {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 50,
        stalledInterval: 60_000,
        batch: { size: 2 },
      },
    );
    worker.on('error', () => {});
    worker.on('failed', (job: { id: string }) => failedIds.push(job.id));
    worker.on('completed', (job: { id: string }) => completedIds.push(job.id));

    try {
      await waitFor(async () => (await a.getState()) === 'failed', 4000, 50);
      expect(await a.getState()).toBe('failed');
      expect(failedIds).toContain(a.id);
      expect(completedIds).not.toContain(a.id);
      expect(completedIds.concat(failedIds)).toContain(b.id);
    } finally {
      await worker.close(true);
      await queue.close();
    }
  }, 15000);

  it('lets a sandboxed priority job call moveToDelayed', async () => {
    const Q = uniqueQueue('list-entryid-sandbox-delay');
    const queue = new Queue(Q, { connection: CONNECTION });
    const job = await queue.add('task', { n: 1 }, { priority: 1 });

    const errors: Error[] = [];
    const worker = new Worker(Q, DELAY_PROCESSOR, {
      connection: CONNECTION,
      concurrency: 1,
      blockTimeout: 50,
      stalledInterval: 60_000,
    });
    worker.on('error', () => {});
    worker.on('failed', (_job: unknown, err: Error) => errors.push(err));

    try {
      await waitFor(
        async () => {
          const state = await job.getState();
          return state === 'delayed' || state === 'failed';
        },
        8000,
        50,
      );
      expect(await job.getState()).toBe('delayed');
      expect(errors).toEqual([]);
    } finally {
      await worker.close(true);
      await queue.close();
    }
  }, 20000);
});
