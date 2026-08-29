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
import { createCleanupClient, describeEachMode, flushQueue, waitFor, type ConnectionConfig } from './helpers/fixture';

import { Queue } from '../src/queue';
import { Worker } from '../src/worker';
import { deferActive } from '../src/functions';
import { buildKeys } from '../src/utils';
import type { Job } from '../src/job';
import type { Processor } from '../src/types';

type TaskData = { n: number };

function useQueueCleanup(connection: ConnectionConfig) {
  let cleanupClient: Awaited<ReturnType<typeof createCleanupClient>>;
  const queues: string[] = [];
  beforeAll(async () => {
    cleanupClient = await createCleanupClient(connection);
  });
  afterAll(async () => {
    await Promise.all(queues.map((q) => flushQueue(cleanupClient, q).catch(() => {})));
    cleanupClient.close();
  });
  return {
    get client() {
      return cleanupClient;
    },
    uniqueQueue(prefix: string): string {
      const name = `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
      queues.push(name);
      return name;
    },
  };
}

function hashFromFields(fields: { field: unknown; value: unknown }[] | null | undefined): Record<string, string> {
  const out: Record<string, string> = {};
  if (!fields) return out;
  for (const f of fields) out[String(f.field)] = String(f.value);
  return out;
}

function closeTestWorker(
  name: string,
  connection: ConnectionConfig,
  processor: Processor<TaskData, string>,
  extra?: { events?: false },
): Worker<TaskData, string> {
  const worker = new Worker<TaskData, string>(name, processor, {
    connection,
    concurrency: 1,
    blockTimeout: 50,
    stalledInterval: 60_000,
    ...extra,
  });
  worker.on('error', () => {});
  return worker;
}

function heldFirstJob(): {
  processor: Processor<TaskData, string>;
  started: () => boolean;
  release: () => void;
} {
  let release!: () => void;
  const hold = new Promise<void>((resolve) => {
    release = resolve;
  });
  let started = false;
  return {
    processor: async (job) => {
      if (job.data.n === 1) {
        started = true;
        await hold;
      }
      return 'ok';
    },
    started: () => started,
    release: () => release(),
  };
}

async function recoverSecondJob(name: string, connection: ConnectionConfig, queue: Queue, jobB: Job): Promise<void> {
  const recovered: number[] = [];
  const worker = closeTestWorker(name, connection, async (job) => {
    recovered.push(job.data.n);
    return 'ok';
  });
  try {
    await waitFor(() => recovered.includes(2), 4000, 50);
    expect(await jobB.getState()).toBe('completed');
  } finally {
    await worker.close(true);
    await queue.close();
  }
}

describeEachMode('close(false) vs completeAndFetchNext', (CONNECTION) => {
  const ctx = useQueueCleanup(CONNECTION);

  it('does not leave the chained next job active after close(false)', async () => {
    const Q = ctx.uniqueQueue('close-caf');
    const queue = new Queue(Q, { connection: CONNECTION });
    const jobA = await queue.add('task', { n: 1 });
    const jobB = await queue.add('task', { n: 2 });
    const processed: number[] = [];
    const hold = heldFirstJob();
    const worker = closeTestWorker(Q, CONNECTION, async (job) => {
      const result = await hold.processor(job);
      processed.push(job.data.n);
      return result;
    });

    await waitFor(() => hold.started());
    const closePromise = worker.close(false);
    hold.release();
    await closePromise;

    expect(processed).toEqual([1]);
    expect(await jobA.getState()).toBe('completed');
    expect(await jobB.getState()).not.toBe('active');
    await recoverSecondJob(Q, CONNECTION, queue, jobB);
  }, 15000);

  it('completes the current grouped job without fetching next when already closing', async () => {
    const Q = ctx.uniqueQueue('close-caf-group');
    const queue = new Queue(Q, { connection: CONNECTION });
    const group = { key: 'g', concurrency: 1 };
    const jobA = await queue.add('task', { n: 1 }, { ordering: group });
    const jobB = await queue.add('task', { n: 2 }, { ordering: group });
    const hold = heldFirstJob();
    const worker = closeTestWorker(Q, CONNECTION, hold.processor);

    await waitFor(() => hold.started());
    const closePromise = worker.close(false);
    hold.release();
    await closePromise;

    expect(await jobA.getState()).toBe('completed');
    expect(await jobB.getState()).not.toBe('group-waiting');
    expect(await jobB.getState()).not.toBe('active');
    await recoverSecondJob(Q, CONNECTION, queue, jobB);
  }, 15000);

  it('undoes a grouped CAF claim when close races with completeAndFetchNext', async () => {
    const Q = ctx.uniqueQueue('close-caf-undo');
    const queue = new Queue(Q, { connection: CONNECTION });
    const group = {
      key: 'g',
      concurrency: 1,
      tokenBucket: { capacity: 10, refillRate: 0.001 },
      rateLimit: { max: 10, duration: 60_000 },
    };
    const jobA = await queue.add('task', { n: 1 }, { ordering: group, cost: 1 });
    const jobB = await queue.add('task', { n: 2 }, { ordering: group, cost: 1 });
    const processed: number[] = [];
    const worker = closeTestWorker(Q, CONNECTION, async (job) => {
      processed.push(job.data.n);
      return 'ok';
    });
    // 'completed' fires after CAF has claimed job B and before the close-defer.
    worker.once('completed', () => {
      void worker.close(false);
    });

    await waitFor(() => processed.includes(1), 4000, 50);
    await worker.close(false);

    expect(processed).toEqual([1]);
    expect(await jobA.getState()).toBe('completed');
    expect(await jobB.getState()).not.toBe('active');
    expect(await jobB.getState()).not.toBe('group-waiting');

    const keys = buildKeys(Q);
    const grp = hashFromFields(await ctx.client.hgetall(keys.group('g')));
    const bSeq = Number(await ctx.client.hget(keys.job(jobB.id), 'orderingSeq'));
    expect(Number(grp.active ?? 0)).toBe(0);
    expect(Number(grp.nextSeq)).toBe(bSeq);
    expect(Number(grp.tbTokens)).toBeGreaterThanOrEqual(9000);
    expect(Number(grp.rateCount ?? 0)).toBe(1);
    await recoverSecondJob(Q, CONNECTION, queue, jobB);
  }, 15000);

  it('does not write completion events when events are disabled during close', async () => {
    const Q = ctx.uniqueQueue('close-caf-events');
    const queue = new Queue(Q, { connection: CONNECTION, events: false });
    const jobA = await queue.add('task', { n: 1 });
    await queue.add('task', { n: 2 });
    const hold = heldFirstJob();
    const worker = closeTestWorker(Q, CONNECTION, hold.processor, { events: false });

    await waitFor(() => hold.started());
    const closePromise = worker.close(false);
    hold.release();
    await closePromise;

    const keys = buildKeys(Q);
    const entries = (await ctx.client.xrange(keys.events, '-', '+')) as Record<string, [string, string][]>;
    const types: string[] = [];
    for (const fields of Object.values(entries ?? {})) {
      for (const [f, v] of fields) {
        if (String(f) === 'event') types.push(String(v));
      }
    }
    expect(types).not.toContain('completed');
    expect(await jobA.getState()).toBe('completed');
    await queue.close();
  }, 15000);

  it('does not decrement group.active when undoing a retained-slot returning claim', async () => {
    const Q = ctx.uniqueQueue('close-caf-retained');
    const keys = buildKeys(Q);
    const jobId = '1';
    await ctx.client.hset(keys.job(jobId), {
      state: 'active',
      name: 'task',
      groupKey: 'g',
      orderingSeq: '1',
      retainedSlot: '1',
      cost: '1000',
    });
    await ctx.client.hset(keys.group('g'), {
      active: '1',
      nextSeq: '2',
      tbCapacity: '10000',
      tbTokens: '8000',
      rateCount: '1',
    });
    const entryId = String(
      await ctx.client.xadd(keys.stream, [
        ['jobId', jobId],
        ['name', 'task'],
      ]),
    );
    await ctx.client.xgroupCreate(keys.stream, 'workers', '0');
    await deferActive(ctx.client, keys, jobId, entryId, 'workers', false, {
      pausedRestore: true,
      undoGroupClaim: true,
    });

    const grp = hashFromFields(await ctx.client.hgetall(keys.group('g')));
    expect(Number(grp.active)).toBe(1);
    expect(Number(grp.nextSeq)).toBe(2);
    expect(Number(grp.tbTokens)).toBe(9000);
    expect(Number(grp.rateCount)).toBe(0);
  });
});
