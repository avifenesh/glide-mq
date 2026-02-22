/**
 * Integration tests for priority job lifecycle.
 * Requires: valkey-server running on localhost:6379 and cluster on :7000-7005
 *
 * Run: npx vitest run tests/priority.test.ts
 */
import { it, expect, describe, beforeAll, afterAll } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
const { promote, changePriority } = require('../dist/functions/index') as typeof import('../src/functions/index');

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

    await new Promise((r) => setTimeout(r, 500));

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

  it('worker scheduler promotes prioritized jobs without manual promote()', async () => {
    const qName = Q + '-auto-promote';
    const localQueue = new Queue(qName, { connection: CONNECTION });

    let processed = 0;
    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 7000);
      const worker = new Worker(
        qName,
        async () => {
          processed += 1;
          if (processed === 2) {
            clearTimeout(timeout);
            setTimeout(() => worker.close(true).then(resolve), 50);
          }
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1, promotionInterval: 100, blockTimeout: 500 },
      );
      worker.on('error', () => {});
    });

    await localQueue.add('p1', { x: 1 }, { priority: 1 });
    await localQueue.add('p2', { x: 2 }, { priority: 2 });

    await done;

    const counts = await localQueue.getJobCounts();
    expect(counts.completed).toBe(2);
    expect(counts.delayed).toBe(0);

    await localQueue.close();
    await flushQueue(cleanupClient, qName);
  }, 10000);

  describe('changePriority', () => {
    it('waiting (priority 0) -> prioritized (priority 5)', async () => {
      const qName = Q + '-cp-wait-to-prio';
      const localQueue = new Queue(qName, { connection: CONNECTION });
      const k = buildKeys(qName);

      const job = await localQueue.add('task', { x: 1 });
      expect(String(await cleanupClient.hget(k.job(job.id), 'state'))).toBe('waiting');

      const result = await changePriority(cleanupClient, k, job.id, 5);
      expect(result).toBe('ok');

      expect(String(await cleanupClient.hget(k.job(job.id), 'state'))).toBe('prioritized');
      expect(String(await cleanupClient.hget(k.job(job.id), 'priority'))).toBe('5');

      const score = await cleanupClient.zscore(k.scheduled, job.id);
      expect(score).not.toBeNull();
      const PRIORITY_SHIFT = 2 ** 42;
      expect(Number(score)).toBe(5 * PRIORITY_SHIFT);

      await localQueue.close();
      await flushQueue(cleanupClient, qName);
    });

    it('prioritized (5) -> prioritized (1): new score ordering', async () => {
      const qName = Q + '-cp-prio-to-prio';
      const localQueue = new Queue(qName, { connection: CONNECTION });
      const k = buildKeys(qName);

      const job = await localQueue.add('task', { x: 1 }, { priority: 5 });
      expect(String(await cleanupClient.hget(k.job(job.id), 'state'))).toBe('prioritized');

      const result = await changePriority(cleanupClient, k, job.id, 1);
      expect(result).toBe('ok');

      expect(String(await cleanupClient.hget(k.job(job.id), 'priority'))).toBe('1');
      const PRIORITY_SHIFT = 2 ** 42;
      const score = Number(await cleanupClient.zscore(k.scheduled, job.id));
      expect(score).toBe(1 * PRIORITY_SHIFT);

      await localQueue.close();
      await flushQueue(cleanupClient, qName);
    });

    it('prioritized -> waiting (priority 0): back in stream', async () => {
      const qName = Q + '-cp-prio-to-wait';
      const localQueue = new Queue(qName, { connection: CONNECTION });
      const k = buildKeys(qName);

      const job = await localQueue.add('task', { x: 1 }, { priority: 3 });
      expect(String(await cleanupClient.hget(k.job(job.id), 'state'))).toBe('prioritized');

      const result = await changePriority(cleanupClient, k, job.id, 0);
      expect(result).toBe('ok');

      expect(String(await cleanupClient.hget(k.job(job.id), 'state'))).toBe('waiting');
      expect(String(await cleanupClient.hget(k.job(job.id), 'priority'))).toBe('0');

      const score = await cleanupClient.zscore(k.scheduled, job.id);
      expect(score).toBeNull();

      await localQueue.close();
      await flushQueue(cleanupClient, qName);
    });

    it('delayed job: delay preserved, priority updated', async () => {
      const qName = Q + '-cp-delayed';
      const localQueue = new Queue(qName, { connection: CONNECTION });
      const k = buildKeys(qName);

      const job = await localQueue.add('task', { x: 1 }, { priority: 2, delay: 60000 });
      expect(String(await cleanupClient.hget(k.job(job.id), 'state'))).toBe('delayed');

      const oldScore = Number(await cleanupClient.zscore(k.scheduled, job.id));
      const PRIORITY_SHIFT = 2 ** 42;
      const oldTimestamp = oldScore % PRIORITY_SHIFT;

      const result = await changePriority(cleanupClient, k, job.id, 7);
      expect(result).toBe('ok');

      expect(String(await cleanupClient.hget(k.job(job.id), 'priority'))).toBe('7');
      const newScore = Number(await cleanupClient.zscore(k.scheduled, job.id));
      expect(newScore).toBe(7 * PRIORITY_SHIFT + oldTimestamp);

      await localQueue.close();
      await flushQueue(cleanupClient, qName);
    });

    it('active job: returns error', async () => {
      const qName = Q + '-cp-active';
      const localQueue = new Queue(qName, { connection: CONNECTION });
      const k = buildKeys(qName);

      const job = await localQueue.add('task', { x: 1 });

      // Manually set state to active to simulate
      await cleanupClient.hset(k.job(job.id), { state: 'active' });

      const result = await changePriority(cleanupClient, k, job.id, 5);
      expect(result).toBe('error:invalid_state');

      await localQueue.close();
      await flushQueue(cleanupClient, qName);
    });

    it('completed job: returns error', async () => {
      const qName = Q + '-cp-completed';
      const localQueue = new Queue(qName, { connection: CONNECTION });
      const k = buildKeys(qName);

      const job = await localQueue.add('task', { x: 1 });
      await cleanupClient.hset(k.job(job.id), { state: 'completed' });

      const result = await changePriority(cleanupClient, k, job.id, 5);
      expect(result).toBe('error:invalid_state');

      await localQueue.close();
      await flushQueue(cleanupClient, qName);
    });

    it('failed job: returns error', async () => {
      const qName = Q + '-cp-failed';
      const localQueue = new Queue(qName, { connection: CONNECTION });
      const k = buildKeys(qName);

      const job = await localQueue.add('task', { x: 1 });
      await cleanupClient.hset(k.job(job.id), { state: 'failed' });

      const result = await changePriority(cleanupClient, k, job.id, 5);
      expect(result).toBe('error:invalid_state');

      await localQueue.close();
      await flushQueue(cleanupClient, qName);
    });

    it('waiting job with priority 0 -> 0: returns no_op', async () => {
      const qName = Q + '-cp-noop';
      const localQueue = new Queue(qName, { connection: CONNECTION });
      const k = buildKeys(qName);

      const job = await localQueue.add('task', { x: 1 });
      expect(String(await cleanupClient.hget(k.job(job.id), 'state'))).toBe('waiting');

      const result = await changePriority(cleanupClient, k, job.id, 0);
      expect(result).toBe('no_op');

      // State unchanged
      expect(String(await cleanupClient.hget(k.job(job.id), 'state'))).toBe('waiting');

      await localQueue.close();
      await flushQueue(cleanupClient, qName);
    });

    it('nonexistent job: returns not_found', async () => {
      const k = buildKeys(Q);

      const result = await changePriority(cleanupClient, k, 'nonexistent-999', 5);
      expect(result).toBe('error:not_found');
    });

    it('Job instance method: updates opts.priority and throws on invalid state', async () => {
      const qName = Q + '-cp-instance';
      const localQueue = new Queue(qName, { connection: CONNECTION });
      const k = buildKeys(qName);

      const job = await localQueue.add('task', { x: 1 }, { priority: 3 });
      expect(job!.opts.priority).toBe(3);

      await job!.changePriority(1);
      expect(job!.opts.priority).toBe(1);
      expect(String(await cleanupClient.hget(k.job(job!.id), 'priority'))).toBe('1');

      // Negative priority throws locally
      await expect(job!.changePriority(-1)).rejects.toThrow('Priority must be >= 0');

      // Active state throws via server
      await cleanupClient.hset(k.job(job!.id), { state: 'active' });
      await expect(job!.changePriority(5)).rejects.toThrow('Cannot change priority');

      await localQueue.close();
      await flushQueue(cleanupClient, qName);
    });
  });
});
