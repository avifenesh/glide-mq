/**
 * Integration tests for delayed job lifecycle.
 * Requires: valkey-server running on localhost:6379 and cluster on :7000-7005
 *
 * Run: npx vitest run tests/delayed.test.ts
 */
import { it, expect, describe, beforeAll, afterAll } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
const { promote, changeDelay } = require('../dist/functions/index') as typeof import('../src/functions/index');

import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

describeEachMode('Delayed jobs', (CONNECTION) => {
  const Q = 'test-delayed-' + Date.now();
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

  it('delayed job lands in scheduled ZSet, not in stream', async () => {
    const job = await queue.add('delayed-task', { x: 1 }, { delay: 60000 });
    const k = buildKeys(Q);

    const score = await cleanupClient.zscore(k.scheduled, job.id);
    expect(score).not.toBeNull();

    const streamEntries = (await cleanupClient.xrange(k.stream, '-', '+')) as Record<string, [string, string][]>;
    const streamJobIds: string[] = [];
    for (const entryId of Object.keys(streamEntries)) {
      const fields = streamEntries[entryId];
      for (const [f, v] of fields) {
        if (String(f) === 'jobId' && String(v) === job.id) {
          streamJobIds.push(String(v));
        }
      }
    }
    expect(streamJobIds).toHaveLength(0);

    const state = await cleanupClient.hget(k.job(job.id), 'state');
    expect(String(state)).toBe('delayed');
  });

  it('delayed job is promoted after delay elapses and processed by worker', async () => {
    const k = buildKeys(Q);
    const delayMs = 500;
    const job = await queue.add('short-delay', { val: 42 }, { delay: delayMs });

    const scoreBefore = await cleanupClient.zscore(k.scheduled, job.id);
    expect(scoreBefore).not.toBeNull();

    await new Promise((r) => setTimeout(r, delayMs + 100));

    const promoted = await promote(cleanupClient, buildKeys(Q), Date.now());
    expect(promoted).toBeGreaterThanOrEqual(1);

    const scoreAfter = await cleanupClient.zscore(k.scheduled, job.id);
    expect(scoreAfter).toBeNull();

    const state = await cleanupClient.hget(k.job(job.id), 'state');
    expect(String(state)).toBe('waiting');

    const processed: string[] = [];
    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker = new Worker(
        Q,
        async (j: any) => {
          processed.push(j.id);
          return { result: 'done' };
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
      );
      worker.on('completed', () => {
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });
      worker.on('error', () => {});
    });

    await done;
    expect(processed).toContain(job.id);

    const completedScore = await cleanupClient.zscore(k.completed, job.id);
    expect(completedScore).not.toBeNull();
    const finalState = await cleanupClient.hget(k.job(job.id), 'state');
    expect(String(finalState)).toBe('completed');
  }, 15000);

  it('multiple delayed jobs with different delays promote in correct order', async () => {
    const qName = Q + '-multi';
    const localQueue = new Queue(qName, { connection: CONNECTION });
    const k = buildKeys(qName);

    const job1 = await localQueue.add('d1', { order: 1 }, { delay: 200 });
    const job2 = await localQueue.add('d2', { order: 2 }, { delay: 400 });
    const job3 = await localQueue.add('d3', { order: 3 }, { delay: 800 });

    expect(await cleanupClient.zscore(k.scheduled, job1.id)).not.toBeNull();
    expect(await cleanupClient.zscore(k.scheduled, job2.id)).not.toBeNull();
    expect(await cleanupClient.zscore(k.scheduled, job3.id)).not.toBeNull();

    await new Promise((r) => setTimeout(r, 300));
    const promoted1 = await promote(cleanupClient, buildKeys(qName), Date.now());
    expect(promoted1).toBe(1);
    expect(await cleanupClient.zscore(k.scheduled, job1.id)).toBeNull();
    expect(await cleanupClient.zscore(k.scheduled, job2.id)).not.toBeNull();
    expect(await cleanupClient.zscore(k.scheduled, job3.id)).not.toBeNull();

    await new Promise((r) => setTimeout(r, 200));
    const promoted2 = await promote(cleanupClient, buildKeys(qName), Date.now());
    expect(promoted2).toBe(1);
    expect(await cleanupClient.zscore(k.scheduled, job2.id)).toBeNull();
    expect(await cleanupClient.zscore(k.scheduled, job3.id)).not.toBeNull();

    await new Promise((r) => setTimeout(r, 400));
    const promoted3 = await promote(cleanupClient, buildKeys(qName), Date.now());
    expect(promoted3).toBe(1);
    expect(await cleanupClient.zscore(k.scheduled, job3.id)).toBeNull();

    const processedOrder: number[] = [];
    const allDone = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      let count = 0;
      const worker = new Worker(
        qName,
        async (j: any) => {
          processedOrder.push(j.data.order);
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
    expect(processedOrder).toHaveLength(3);
    expect(processedOrder).toEqual([1, 2, 3]);

    await localQueue.close();
    await flushQueue(cleanupClient, qName);
  }, 20000);

  describe('changeDelay', () => {
    const PRIORITY_SHIFT = 2 ** 42;

    it('delayed (no priority) -> new delay', async () => {
      const qName = Q + '-cd-delay-newdelay';
      const localQueue = new Queue(qName, { connection: CONNECTION });
      const k = buildKeys(qName);

      const job = await localQueue.add('task', { x: 1 }, { delay: 60000 });
      expect(String(await cleanupClient.hget(k.job(job.id), 'state'))).toBe('delayed');

      const result = await changeDelay(cleanupClient, k, job.id, 30000);
      expect(result).toBe('ok');

      const newScore = Number(await cleanupClient.zscore(k.scheduled, job.id));
      const now = Date.now();
      const timestamp = newScore % PRIORITY_SHIFT;
      expect(timestamp).toBeGreaterThan(now + 30000 - 5000);
      expect(timestamp).toBeLessThan(now + 30000 + 5000);

      const delay = await cleanupClient.hget(k.job(job.id), 'delay');
      expect(String(delay)).toBe('30000');

      await localQueue.close();
      await flushQueue(cleanupClient, qName);
    });

    it('delayed (with priority) -> new delay: priority preserved', async () => {
      const qName = Q + '-cd-priodelay';
      const localQueue = new Queue(qName, { connection: CONNECTION });
      const k = buildKeys(qName);

      const job = await localQueue.add('task', { x: 1 }, { priority: 3, delay: 60000 });
      expect(String(await cleanupClient.hget(k.job(job.id), 'state'))).toBe('delayed');

      const result = await changeDelay(cleanupClient, k, job.id, 30000);
      expect(result).toBe('ok');

      const newScore = Number(await cleanupClient.zscore(k.scheduled, job.id));
      expect(Math.floor(newScore / PRIORITY_SHIFT)).toBe(3);

      const now = Date.now();
      const timestamp = newScore % PRIORITY_SHIFT;
      expect(timestamp).toBeGreaterThan(now + 30000 - 5000);
      expect(timestamp).toBeLessThan(now + 30000 + 5000);

      await localQueue.close();
      await flushQueue(cleanupClient, qName);
    });

    it('delayed (priority=0) -> delay 0: promoted to waiting', async () => {
      const qName = Q + '-cd-delay0-wait';
      const localQueue = new Queue(qName, { connection: CONNECTION });
      const k = buildKeys(qName);

      const job = await localQueue.add('task', { x: 1 }, { delay: 60000 });
      expect(String(await cleanupClient.hget(k.job(job.id), 'state'))).toBe('delayed');

      const result = await changeDelay(cleanupClient, k, job.id, 0);
      expect(result).toBe('ok');

      expect(String(await cleanupClient.hget(k.job(job.id), 'state'))).toBe('waiting');
      expect(String(await cleanupClient.hget(k.job(job.id), 'delay'))).toBe('0');

      const score = await cleanupClient.zscore(k.scheduled, job.id);
      expect(score).toBeNull();

      await localQueue.close();
      await flushQueue(cleanupClient, qName);
    });

    it('delayed (priority>0) -> delay 0: becomes prioritized', async () => {
      const qName = Q + '-cd-delay0-prio';
      const localQueue = new Queue(qName, { connection: CONNECTION });
      const k = buildKeys(qName);

      const job = await localQueue.add('task', { x: 1 }, { priority: 2, delay: 60000 });
      expect(String(await cleanupClient.hget(k.job(job.id), 'state'))).toBe('delayed');

      const result = await changeDelay(cleanupClient, k, job.id, 0);
      expect(result).toBe('ok');

      expect(String(await cleanupClient.hget(k.job(job.id), 'state'))).toBe('prioritized');
      expect(String(await cleanupClient.hget(k.job(job.id), 'delay'))).toBe('0');

      const score = Number(await cleanupClient.zscore(k.scheduled, job.id));
      expect(score).toBe(2 * PRIORITY_SHIFT);

      await localQueue.close();
      await flushQueue(cleanupClient, qName);
    });

    it('waiting -> add delay: moves to scheduled', async () => {
      const qName = Q + '-cd-wait-to-delay';
      const localQueue = new Queue(qName, { connection: CONNECTION });
      const k = buildKeys(qName);

      const job = await localQueue.add('task', { x: 1 });
      expect(String(await cleanupClient.hget(k.job(job.id), 'state'))).toBe('waiting');

      const result = await changeDelay(cleanupClient, k, job.id, 30000);
      expect(result).toBe('ok');

      expect(String(await cleanupClient.hget(k.job(job.id), 'state'))).toBe('delayed');

      const newScore = Number(await cleanupClient.zscore(k.scheduled, job.id));
      expect(newScore).not.toBeNull();

      const now = Date.now();
      const timestamp = newScore % PRIORITY_SHIFT;
      expect(timestamp).toBeGreaterThan(now + 30000 - 5000);
      expect(timestamp).toBeLessThan(now + 30000 + 5000);

      await localQueue.close();
      await flushQueue(cleanupClient, qName);
    });

    it('prioritized -> add delay: adds timestamp to score', async () => {
      const qName = Q + '-cd-prio-to-delay';
      const localQueue = new Queue(qName, { connection: CONNECTION });
      const k = buildKeys(qName);

      const job = await localQueue.add('task', { x: 1 }, { priority: 5 });
      expect(String(await cleanupClient.hget(k.job(job.id), 'state'))).toBe('prioritized');

      const result = await changeDelay(cleanupClient, k, job.id, 30000);
      expect(result).toBe('ok');

      expect(String(await cleanupClient.hget(k.job(job.id), 'state'))).toBe('delayed');

      const newScore = Number(await cleanupClient.zscore(k.scheduled, job.id));
      expect(Math.floor(newScore / PRIORITY_SHIFT)).toBe(5);

      const now = Date.now();
      const timestamp = newScore % PRIORITY_SHIFT;
      expect(timestamp).toBeGreaterThan(now + 30000 - 5000);
      expect(timestamp).toBeLessThan(now + 30000 + 5000);

      await localQueue.close();
      await flushQueue(cleanupClient, qName);
    });

    it('waiting with no delay -> delay 0: returns no_op', async () => {
      const qName = Q + '-cd-wait-noop';
      const localQueue = new Queue(qName, { connection: CONNECTION });
      const k = buildKeys(qName);

      const job = await localQueue.add('task', { x: 1 });
      expect(String(await cleanupClient.hget(k.job(job.id), 'state'))).toBe('waiting');

      const result = await changeDelay(cleanupClient, k, job.id, 0);
      expect(result).toBe('no_op');

      expect(String(await cleanupClient.hget(k.job(job.id), 'state'))).toBe('waiting');

      await localQueue.close();
      await flushQueue(cleanupClient, qName);
    });

    it('active job: returns error', async () => {
      const qName = Q + '-cd-active';
      const localQueue = new Queue(qName, { connection: CONNECTION });
      const k = buildKeys(qName);

      const job = await localQueue.add('task', { x: 1 });
      await cleanupClient.hset(k.job(job.id), { state: 'active' });

      const result = await changeDelay(cleanupClient, k, job.id, 5000);
      expect(result).toBe('error:invalid_state');

      await localQueue.close();
      await flushQueue(cleanupClient, qName);
    });

    it('nonexistent job: returns error', async () => {
      const k = buildKeys(Q);

      const result = await changeDelay(cleanupClient, k, 'nonexistent-999', 5000);
      expect(result).toBe('error:not_found');
    });

    it('Job instance method: updates opts.delay and throws on invalid state', async () => {
      const qName = Q + '-cd-instance';
      const localQueue = new Queue(qName, { connection: CONNECTION });
      const k = buildKeys(qName);

      const job = await localQueue.add('task', { x: 1 }, { delay: 60000 });
      expect(job!.opts.delay).toBe(60000);

      await job!.changeDelay(30000);
      expect(job!.opts.delay).toBe(30000);

      // Negative delay throws locally
      await expect(job!.changeDelay(-1)).rejects.toThrow('Delay must be >= 0');

      // Active state throws via server
      await cleanupClient.hset(k.job(job!.id), { state: 'active' });
      await expect(job!.changeDelay(5000)).rejects.toThrow('Cannot change delay');

      await localQueue.close();
      await flushQueue(cleanupClient, qName);
    });
  });
});
