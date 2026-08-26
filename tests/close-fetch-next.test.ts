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

import { Queue } from '../src/queue';
import { Worker } from '../src/worker';
import { deferActive } from '../src/functions';
import { buildKeys } from '../src/utils';

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

  it('completes the current grouped job without fetching next when already closing', async () => {
    const Q = uniqueQueue('close-caf-group');
    const queue = new Queue(Q, { connection: CONNECTION });
    const group = { key: 'g', concurrency: 1 };

    const jobA = await queue.add('task', { n: 1 }, { ordering: group });
    const jobB = await queue.add('task', { n: 2 }, { ordering: group });

    let releaseA!: () => void;
    const holdA = new Promise<void>((resolve) => {
      releaseA = resolve;
    });
    let aStarted = false;

    const worker = new Worker(
      Q,
      async (job: { data: { n: number } }) => {
        if (job.data.n === 1) {
          aStarted = true;
          await holdA;
        }
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

    expect(await jobA.getState()).toBe('completed');
    expect(await jobB.getState()).not.toBe('group-waiting');
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

  it('undoes a grouped CAF claim when close races with completeAndFetchNext', async () => {
    const Q = uniqueQueue('close-caf-undo');
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
    const worker = new Worker(
      Q,
      async (job: { data: { n: number } }) => {
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
    const grpFields = await cleanupClient.hgetall(keys.group('g'));
    const grp: Record<string, string> = {};
    if (grpFields) {
      for (const f of grpFields) grp[String(f.field)] = String(f.value);
    }
    const bSeq = Number(await cleanupClient.hget(keys.job(jobB.id), 'orderingSeq'));
    expect(Number(grp.active ?? 0)).toBe(0);
    expect(Number(grp.nextSeq)).toBe(bSeq);
    expect(Number(grp.tbTokens)).toBeGreaterThanOrEqual(9000);
    expect(Number(grp.rateCount ?? 0)).toBe(1);

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

  it('does not write completion events when events are disabled during close', async () => {
    const Q = uniqueQueue('close-caf-events');
    const queue = new Queue(Q, { connection: CONNECTION, events: false });
    const jobA = await queue.add('task', { n: 1 });
    await queue.add('task', { n: 2 });

    let releaseA!: () => void;
    const holdA = new Promise<void>((resolve) => {
      releaseA = resolve;
    });
    let aStarted = false;

    const worker = new Worker(
      Q,
      async (job: { data: { n: number } }) => {
        if (job.data.n === 1) {
          aStarted = true;
          await holdA;
        }
        return 'ok';
      },
      {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 50,
        stalledInterval: 60_000,
        events: false,
      },
    );
    worker.on('error', () => {});

    await waitFor(() => aStarted);
    const closePromise = worker.close(false);
    releaseA();
    await closePromise;

    const keys = buildKeys(Q);
    const entries = (await cleanupClient.xrange(keys.events, '-', '+')) as Record<string, [string, string][]>;
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
    const Q = uniqueQueue('close-caf-retained');
    const keys = buildKeys(Q);
    const jobId = '1';
    await cleanupClient.hset(keys.job(jobId), {
      state: 'active',
      name: 'task',
      groupKey: 'g',
      orderingSeq: '1',
      retainedSlot: '1',
      cost: '1000',
    });
    await cleanupClient.hset(keys.group('g'), {
      active: '1',
      nextSeq: '2',
      tbCapacity: '10000',
      tbTokens: '8000',
      rateCount: '1',
    });
    const entryId = String(
      await cleanupClient.xadd(keys.stream, [
        ['jobId', jobId],
        ['name', 'task'],
      ]),
    );
    await cleanupClient.xgroupCreate(keys.stream, 'workers', '0');

    await deferActive(cleanupClient, keys, jobId, entryId, 'workers', false, true, true);

    const grpFields = await cleanupClient.hgetall(keys.group('g'));
    const grp: Record<string, string> = {};
    if (grpFields) {
      for (const f of grpFields) grp[String(f.field)] = String(f.value);
    }
    expect(Number(grp.active)).toBe(1);
    expect(Number(grp.nextSeq)).toBe(2);
    expect(Number(grp.tbTokens)).toBe(9000);
    expect(Number(grp.rateCount)).toBe(0);
  });
});
