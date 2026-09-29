/**
 * Follow-up regressions for server functions and worker paths (2026-09-29).
 *
 * Run: npx vitest run tests/followups-20260929.test.ts
 */
import { afterAll, beforeAll, expect, it } from 'vitest';
import { createCleanupClient, describeEachMode, flushQueue, waitFor } from './helpers/fixture';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
const { moveToActive, popLists, reclaimStalled, CONSUMER_GROUP } =
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
});
