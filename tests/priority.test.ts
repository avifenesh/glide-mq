/**
 * Integration tests for priority job lifecycle.
 * Requires: valkey-server running on localhost:6379 and cluster on :7000-7005
 *
 * Run: npx vitest run tests/priority.test.ts
 */
import { it, expect, beforeAll, afterAll } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
const { promote } = require('../dist/functions/index') as typeof import('../src/functions/index');

import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

describeEachMode('Priority jobs', (CONNECTION) => {
  const Q = 'test-priority-' + Date.now();
  let queue: InstanceType<typeof Queue>;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
    queue = new Queue(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('prioritized job goes to scheduled ZSet with correct score encoding', async () => {
    const k = buildKeys(Q);
    const job = await queue.add('prio-task', { x: 1 }, { priority: 3 });

    const score = await cleanupClient.zscore(k.scheduled, job.id);
    expect(score).not.toBeNull();

    const PRIORITY_SHIFT = 2 ** 42;
    const expectedScore = 3 * PRIORITY_SHIFT;
    expect(Number(score)).toBe(expectedScore);

    const state = await cleanupClient.hget(k.job(job.id), 'state');
    expect(String(state)).toBe('prioritized');
  });

  it('prioritized jobs are processed in priority order (lower number first)', async () => {
    const qName = Q + '-order';
    const localQueue = new Queue(qName, { connection: CONNECTION });
    const k = buildKeys(qName);

    const jobP3 = await localQueue.add('p3', { prio: 3 }, { priority: 3 });
    const jobP1 = await localQueue.add('p1', { prio: 1 }, { priority: 1 });
    const jobP2 = await localQueue.add('p2', { prio: 2 }, { priority: 2 });

    expect(await cleanupClient.zscore(k.scheduled, jobP3.id)).not.toBeNull();
    expect(await cleanupClient.zscore(k.scheduled, jobP1.id)).not.toBeNull();
    expect(await cleanupClient.zscore(k.scheduled, jobP2.id)).not.toBeNull();

    const processedPrios: number[] = [];
    const allDone = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      let count = 0;
      const worker = new Worker(
        qName,
        async (j: any) => {
          processedPrios.push(j.data.prio);
          count++;
          if (count >= 3) {
            clearTimeout(timeout);
            setTimeout(() => worker.close(true).then(resolve), 200);
          }
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
      );
      worker.on('error', () => {});
    });

    await new Promise(r => setTimeout(r, 500));

    const promoted = await promote(cleanupClient, buildKeys(qName), Number.MAX_SAFE_INTEGER);
    expect(promoted).toBe(3);

    await allDone;

    expect(processedPrios).toEqual([1, 2, 3]);

    await localQueue.close();
    await flushQueue(cleanupClient, qName);
  }, 15000);

  it('priority + delay: combined score respects both priority and delay', async () => {
    const qName = Q + '-prio-delay';
    const localQueue = new Queue(qName, { connection: CONNECTION });
    const k = buildKeys(qName);

    const jobShort = await localQueue.add('short', { label: 'short' }, { priority: 2, delay: 200 });
    const jobLong = await localQueue.add('long', { label: 'long' }, { priority: 2, delay: 600 });

    expect(await cleanupClient.zscore(k.scheduled, jobShort.id)).not.toBeNull();
    expect(await cleanupClient.zscore(k.scheduled, jobLong.id)).not.toBeNull();

    const scoreShort = Number(await cleanupClient.zscore(k.scheduled, jobShort.id));
    const scoreLong = Number(await cleanupClient.zscore(k.scheduled, jobLong.id));
    expect(scoreShort).toBeLessThan(scoreLong);

    const processedLabels: string[] = [];
    const allDone = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      let count = 0;
      const worker = new Worker(
        qName,
        async (j: any) => {
          processedLabels.push(j.data.label);
          count++;
          if (count >= 2) {
            clearTimeout(timeout);
            setTimeout(() => worker.close(true).then(resolve), 200);
          }
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
      );
      worker.on('error', () => {});
    });

    await new Promise(r => setTimeout(r, 500));

    const promoted = await promote(cleanupClient, buildKeys(qName), Number.MAX_SAFE_INTEGER);
    expect(promoted).toBe(2);

    await allDone;
    expect(processedLabels).toEqual(['short', 'long']);

    await localQueue.close();
    await flushQueue(cleanupClient, qName);
  }, 15000);

  it('non-prioritized job (priority=0) goes directly to stream, bypassing scheduled ZSet', async () => {
    const qName = Q + '-noprio';
    const localQueue = new Queue(qName, { connection: CONNECTION });
    const k = buildKeys(qName);

    const job = await localQueue.add('normal', { x: 1 });

    const score = await cleanupClient.zscore(k.scheduled, job.id);
    expect(score).toBeNull();

    const state = await cleanupClient.hget(k.job(job.id), 'state');
    expect(String(state)).toBe('waiting');

    await localQueue.close();
    await flushQueue(cleanupClient, qName);
  });
});
