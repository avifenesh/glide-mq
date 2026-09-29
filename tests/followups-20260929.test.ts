/**
 * Follow-up regressions for server functions and worker paths (2026-09-29).
 *
 * Run: npx vitest run tests/followups-20260929.test.ts
 */
import { afterAll, beforeAll, expect, it } from 'vitest';
import { createCleanupClient, describeEachMode, flushQueue, waitFor } from './helpers/fixture';

const { InfBoundary } = require('@glidemq/speedkey') as typeof import('@glidemq/speedkey');
const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys, keyPrefix } = require('../dist/utils') as typeof import('../src/utils');
const { addFlow, addJob, dedup, failJob, moveToActive, popLists, reclaimStalled, CONSUMER_GROUP } =
  require('../dist/functions') as typeof import('../src/functions');

describeEachMode('Follow-ups 2026-09-29', (CONNECTION) => {
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

  it('F1: obliterate without force refuses while a list job is active', async () => {
    const Q = uniqueQueue('fu-obl-list');
    const k = buildKeys(Q);
    const queue = new Queue(Q, { connection: CONNECTION });
    try {
      const job = await queue.add('a', {}, { lifo: true });
      expect(await popLists(cleanupClient, k, 1)).toEqual([job!.id]);
      await moveToActive(cleanupClient, k, job!.id, Date.now());
      await expect(queue.obliterate()).rejects.toThrow(/1 active jobs/);
      expect(await cleanupClient.exists([k.job(job!.id)])).toBe(1);
      await queue.obliterate({ force: true });
      expect(await cleanupClient.exists([k.job(job!.id)])).toBe(0);
    } finally {
      await queue.close();
    }
  });

  async function workerSeesStalled(Q: string, jobId: string): Promise<[string, string][]> {
    const stalled: [string, string][] = [];
    const completed: string[] = [];
    const worker = new Worker(Q, async () => 'ok', {
      connection: CONNECTION,
      stalledInterval: 300,
      lockDuration: 300,
      maxStalledCount: 5,
    });
    worker.on('stalled', (id: string, prev: string) => stalled.push([id, prev]));
    worker.on('completed', (job: any) => completed.push(job.id));
    try {
      await waitFor(() => completed.includes(jobId), 8000);
    } finally {
      await worker.close();
    }
    return stalled;
  }

  it('F2: worker emits stalled for a stream job its scheduler returns to waiting', async () => {
    const Q = uniqueQueue('fu-stalled-stream');
    const k = buildKeys(Q);
    const queue = new Queue(Q, { connection: CONNECTION });
    try {
      const job = await queue.add('a', {});
      await cleanupClient.xgroupCreate(k.stream, CONSUMER_GROUP, '0', { mkStream: true });
      // A dead consumer claimed and activated the job long ago.
      await cleanupClient.xreadgroup(CONSUMER_GROUP, 'dead', { [k.stream]: '>' }, { count: 1 });
      await cleanupClient.hset(k.job(job!.id), { state: 'active', lastActive: '1' });
      await new Promise((r) => setTimeout(r, 400));
      expect(await workerSeesStalled(Q, job!.id)).toEqual([[job!.id, 'active']]);
    } finally {
      await queue.close();
    }
  });

  it('F2: worker emits stalled for a list job its scheduler returns to waiting', async () => {
    const Q = uniqueQueue('fu-stalled-list');
    const k = buildKeys(Q);
    const queue = new Queue(Q, { connection: CONNECTION });
    try {
      const job = await queue.add('a', {}, { lifo: true });
      expect(await popLists(cleanupClient, k, 1)).toEqual([job!.id]);
      await moveToActive(cleanupClient, k, job!.id, Date.now());
      await cleanupClient.hset(k.job(job!.id), { lastActive: '1' });
      expect(await workerSeesStalled(Q, job!.id)).toEqual([[job!.id, 'active']]);
    } finally {
      await queue.close();
    }
  });

  it('F2: reclaimStalled without the IDs arg keeps the integer reply', async () => {
    const Q = uniqueQueue('fu-stalled-int');
    const k = buildKeys(Q);
    const queue = new Queue(Q, { connection: CONNECTION });
    try {
      const job = await queue.add('a', {});
      await cleanupClient.xgroupCreate(k.stream, CONSUMER_GROUP, '0', { mkStream: true });
      await cleanupClient.xreadgroup(CONSUMER_GROUP, 'dead', { [k.stream]: '>' }, { count: 1 });
      await cleanupClient.hset(k.job(job!.id), { state: 'active', lastActive: '1' });
      expect(await reclaimStalled(cleanupClient, k, 'rescuer', 0, 5, Date.now())).toBe(1);
    } finally {
      await queue.close();
    }
  });

  async function eventTypes(Q: string): Promise<string[]> {
    const entries = await cleanupClient.xrange(
      buildKeys(Q).events,
      InfBoundary.NegativeInfinity,
      InfBoundary.PositiveInfinity,
    );
    const types: string[] = [];
    for (const fields of Object.values(entries ?? {}) as [string, string][][]) {
      for (const [f, v] of fields) if (String(f) === 'event') types.push(String(v));
    }
    return types;
  }

  it('F3: a worker with events:false and metrics:false writes no retrying/failed events or failure metrics', async () => {
    const Q = uniqueQueue('fu-fail-skip');
    const k = buildKeys(Q);
    const queue = new Queue(Q, { connection: CONNECTION, events: false });
    const worker = new Worker(
      Q,
      async () => {
        throw new Error('boom');
      },
      { connection: CONNECTION, events: false, metrics: false, promotionInterval: 100 },
    );
    const failed: string[] = [];
    worker.on('failed', (job: any) => failed.push(job.id));
    try {
      await worker.waitUntilReady();
      const job = await queue.add('a', {}, { attempts: 2 });
      await waitFor(async () => (await queue.getJobCounts()).failed === 1, 8000);
      expect(failed).toEqual([job!.id, job!.id]);
      expect((await eventTypes(Q)).filter((t) => t === 'retrying' || t === 'failed')).toEqual([]);
      expect(await cleanupClient.hlen(k.metricsFailed)).toBe(0);
    } finally {
      await worker.close();
      await queue.close();
    }
  });

  it('F3: glidemq_fail without the skip args still writes events and metrics', async () => {
    const Q = uniqueQueue('fu-fail-default');
    const k = buildKeys(Q);
    await cleanupClient.hset(k.job('j'), { id: 'j', name: 'j', state: 'active', processedOn: '1' });
    expect(await failJob(cleanupClient, k, 'j', '', 'boom', Date.now(), 0, 0)).toBe('failed');
    expect(await eventTypes(Q)).toEqual(['failed']);
    expect(await cleanupClient.hlen(k.metricsFailed)).toBeGreaterThan(0);
  });

  it('F5: a job failed at activation for cost over capacity gets a DLQ copy', async () => {
    const Q = uniqueQueue('fu-cost-dlq');
    const DLQ = uniqueQueue('fu-cost-dlq-dead');
    const k = buildKeys(Q);
    const queue = new Queue(Q, { connection: CONNECTION });
    const dlq = new Queue(DLQ, { connection: CONNECTION });
    const processed: string[] = [];
    try {
      const job = await queue.add(
        'costly',
        { n: 1 },
        { ordering: { key: 'g', tokenBucket: { capacity: 10, refillRate: 1 } }, cost: 5 },
      );
      // The group capacity shrank after the job was added.
      await cleanupClient.hset(k.group('g'), { tbCapacity: '1', tbTokens: '1' });
      const worker = new Worker(Q, async (j: any) => processed.push(j.id), {
        connection: CONNECTION,
        deadLetterQueue: { name: DLQ },
      });
      try {
        await waitFor(async () => (await dlq.getJobCounts()).waiting === 1, 8000);
      } finally {
        await worker.close();
      }
      expect(processed).toEqual([]);
      expect(await cleanupClient.hget(k.job(job!.id), 'state')).toBe('failed');
      const [dead] = await dlq.getJobs('waiting');
      expect(dead.name).toBe('costly');
      expect(dead.data).toMatchObject({
        originalQueue: Q,
        originalJobId: job!.id,
        data: { n: 1 },
        failedReason: 'cost exceeds token bucket capacity',
      });
    } finally {
      await dlq.close();
      await queue.close();
    }
  });

  async function addWithPriority(Q: string, priority: string): Promise<unknown> {
    return cleanupClient.fcall(
      'glidemq_addJob',
      [buildKeys(Q).id, buildKeys(Q).stream, buildKeys(Q).scheduled, buildKeys(Q).events],
      ['p', '{}', '{}', Date.now().toString(), '0', priority, '', '0', '', '0', '0', '0', '0', '0', '0', '0', '', '0'],
    );
  }

  it('F6: glidemq_addJob rejects a non-integer or out-of-range priority', async () => {
    const Q = uniqueQueue('fu-pri-add');
    const k = buildKeys(Q);
    for (const bad of ['-1', '2049', '1.5', 'abc']) {
      await expect(addWithPriority(Q, bad)).rejects.toThrow(/invalid priority/);
    }
    expect(await cleanupClient.exists([k.id, k.scheduled, k.stream])).toBe(0);
    const id = await addJob(cleanupClient, k, 'p', '{}', '{}', Date.now(), 0, 2048, '', 0);
    expect(await cleanupClient.hget(k.job(String(id)), 'priority')).toBe('2048');
    for (const bad of ['1.5', '2049']) {
      const reply = await cleanupClient.fcall(
        'glidemq_changePriority',
        [k.job(String(id)), k.stream, k.scheduled, k.events],
        [String(id), bad, CONSUMER_GROUP],
      );
      expect(String(reply)).toBe('error:invalid_priority');
    }
  });

  it('F6: glidemq_dedup rejects an invalid priority', async () => {
    const Q = uniqueQueue('fu-pri-dedup');
    const k = buildKeys(Q);
    await expect(dedup(cleanupClient, k, 'd', 0, 'simple', 'p', '{}', '{}', Date.now(), 0, 3.5, '', 0)).rejects.toThrow(
      /invalid priority/,
    );
    expect(await cleanupClient.exists([k.dedup, k.id])).toBe(0);
  });

  it('F6: glidemq_addFlow rejects an invalid parent or child priority before writing', async () => {
    const Q = uniqueQueue('fu-pri-flow');
    const k = buildKeys(Q);
    const child = (priority: number) => ({
      name: 'c',
      data: '{}',
      opts: '{}',
      delay: 0,
      priority,
      maxAttempts: 0,
      keys: k,
      queuePrefix: keyPrefix('glide', Q),
      parentQueueName: Q,
      customId: '',
    });
    await expect(addFlow(cleanupClient, k, 'p', '{}', '{}', Date.now(), 0, -2, 0, [child(0)])).rejects.toThrow(
      /invalid priority/,
    );
    await expect(addFlow(cleanupClient, k, 'p', '{}', '{}', Date.now(), 0, 0, 0, [child(5000)])).rejects.toThrow(
      /invalid priority/,
    );
    expect(await cleanupClient.exists([k.id, k.stream, k.scheduled])).toBe(0);
  });
});
