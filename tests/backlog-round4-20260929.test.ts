/**
 * Backlog round 4 regressions (2026-09-29): batch budget charging, rate-limit
 * requeue attempts, budget pause re-check, failAndFetchNext, broadcast trim
 * unread counts.
 *
 * Run: npx vitest run tests/backlog-round4-20260929.test.ts
 */
import { afterAll, beforeAll, expect, it } from 'vitest';
import { createCleanupClient, describeEachMode, flushQueue, waitFor } from './helpers/fixture';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { FlowProducer } = require('../dist/flow-producer') as typeof import('../src/flow-producer');
const { BatchError } = require('../dist/errors') as typeof import('../src/errors');
const { Broadcast } = require('../dist/broadcast') as typeof import('../src/broadcast');
const { BroadcastWorker } = require('../dist/broadcast-worker') as typeof import('../src/broadcast-worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

describeEachMode('Backlog round 4 2026-09-29', (CONNECTION) => {
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

  it('B1: batch workers charge flow budgets on completion and failure, once per attempt', async () => {
    const Q = uniqueQueue('r4-batch-budget');
    const queue = new Queue(Q, { connection: CONNECTION });
    const flow = new FlowProducer({ connection: CONNECTION });
    let flakyRuns = 0;
    const completed: string[] = [];
    const worker = new Worker(
      Q,
      async (jobs: any[]) => {
        const results: unknown[] = [];
        for (const job of jobs) {
          if (job.name === 'parent') {
            results.push('parent');
          } else if (job.name === 'flaky') {
            flakyRuns++;
            if (flakyRuns === 1) {
              await job.reportUsage({ costs: { total: 0.5 } });
              results.push(new Error('boom'));
            } else {
              // The retry reports nothing new: the first attempt's charge stands.
              results.push('ok');
            }
          } else {
            await job.reportUsage({ costs: { total: 1 } });
            results.push('ok');
          }
        }
        if (results.some((r) => r instanceof Error)) throw new BatchError(results);
        return results;
      },
      { connection: CONNECTION, batch: { size: 4, timeout: 100 }, blockTimeout: 300 },
    );
    worker.on('error', () => {});
    worker.on('completed', (job: any) => completed.push(job.name));
    try {
      const node = await flow.add(
        {
          name: 'parent',
          queueName: Q,
          data: {},
          children: [
            { name: 'ok-1', queueName: Q, data: {} },
            { name: 'ok-2', queueName: Q, data: {} },
            { name: 'flaky', queueName: Q, data: {}, opts: { attempts: 2 } },
          ],
        },
        { budget: { maxTotalCost: 10 } },
      );
      await waitFor(() => completed.includes('parent'), 15000);
      expect(flakyRuns).toBe(2);
      const budget = await queue.getFlowBudget(node.job.id);
      expect(budget).not.toBeNull();
      expect(budget!.usedCost).toBeCloseTo(2.5, 6);
      expect(budget!.exceeded).toBe(false);
    } finally {
      await worker.close();
      await flow.close();
      await queue.close();
    }
  }, 20000);

  it('B1: a batch worker does not run jobs of an exceeded budget', async () => {
    const Q = uniqueQueue('r4-batch-budget-check');
    const queue = new Queue(Q, { connection: CONNECTION });
    const flow = new FlowProducer({ connection: CONNECTION });
    const ran: string[] = [];
    const failed: [string, string][] = [];
    const worker = new Worker(
      Q,
      async (jobs: any[]) => {
        for (const job of jobs) {
          ran.push(job.name);
          if (job.name !== 'parent') await job.reportUsage({ costs: { total: 1.5 } });
        }
        return jobs.map(() => 'ok');
      },
      { connection: CONNECTION, batch: { size: 1, timeout: 50 }, blockTimeout: 300 },
    );
    worker.on('error', () => {});
    worker.on('failed', (job: any, err: Error) => failed.push([job.name, err.message]));
    try {
      await flow.add(
        {
          name: 'parent',
          queueName: Q,
          data: {},
          children: [
            { name: 'c1', queueName: Q, data: {} },
            { name: 'c2', queueName: Q, data: {} },
          ],
        },
        { budget: { maxTotalCost: 1 } },
      );
      await waitFor(() => failed.length >= 1, 15000);
      expect(failed).toEqual([[expect.stringMatching(/^c[12]$/), 'Budget exceeded']]);
      expect(ran.filter((n) => n !== 'parent')).toHaveLength(1);
    } finally {
      await worker.close();
      await flow.close();
      await queue.close();
    }
  }, 20000);

  it('B2: a RateLimitError requeue does not consume an attempt', async () => {
    const Q = uniqueQueue('r4-ratelimit-attempt');
    const k = buildKeys(Q);
    const queue = new Queue(Q, { connection: CONNECTION });
    const runs: string[] = [];
    const completed: string[] = [];
    const failed: string[] = [];
    const worker = new Worker(
      Q,
      async (job: any) => {
        runs.push(job.id);
        if (runs.length === 1) {
          const err = new Worker.RateLimitError();
          (err as any).delayMs = 100;
          throw err;
        }
        if (runs.length === 2) throw new Error('boom');
        return 'ok';
      },
      { connection: CONNECTION, blockTimeout: 300, limiter: { max: 100, duration: 1000 } },
    );
    worker.on('error', () => {});
    worker.on('completed', (job: any) => completed.push(job.id));
    worker.on('failed', (job: any) => failed.push(job.id));
    try {
      const job = await queue.add('limited', {}, { attempts: 2 });
      await waitFor(() => completed.includes(job!.id), 15000);
      // The rate-limit requeue is not an attempt: 'failed' fires once, for the
      // real failure, and the job still completes within attempts: 2.
      expect(failed).toEqual([job!.id]);
      expect(runs).toEqual([job!.id, job!.id, job!.id]);
      const hash = await cleanupClient.hmget(k.job(job!.id), ['attemptsMade', 'state']);
      expect(hash.map(String)).toEqual(['1', 'completed']);
    } finally {
      await worker.close();
      await queue.close();
    }
  }, 20000);

  it('B2: the rate-limit requeue leaves attemptsMade and failedReason untouched', async () => {
    const Q = uniqueQueue('r4-ratelimit-hash');
    const k = buildKeys(Q);
    const queue = new Queue(Q, { connection: CONNECTION });
    let runs = 0;
    const completed: string[] = [];
    const worker = new Worker(
      Q,
      async () => {
        runs++;
        if (runs === 1) throw new Worker.RateLimitError();
        return 'ok';
      },
      { connection: CONNECTION, blockTimeout: 300, limiter: { max: 100, duration: 200 } },
    );
    worker.on('error', () => {});
    worker.on('completed', (job: any) => completed.push(job.id));
    try {
      const job = await queue.add('limited', {}, { attempts: 1 });
      await waitFor(() => completed.includes(job!.id), 15000);
      expect(runs).toBe(2);
      const hash = await cleanupClient.hmget(k.job(job!.id), ['attemptsMade', 'failedReason']);
      expect(hash.map((v: any) => (v === null ? null : String(v)))).toEqual(['0', null]);
    } finally {
      await worker.close();
      await queue.close();
    }
  }, 20000);

  it('B2: a broadcast RateLimitError requeue does not touch the per-subscription attempt counter', async () => {
    const Q = uniqueQueue('r4-ratelimit-bcast');
    const k = buildKeys(Q);
    const broadcast = new Broadcast(Q, { connection: CONNECTION });
    let runs = 0;
    const completed: string[] = [];
    const failed: string[] = [];
    const worker = new BroadcastWorker(
      Q,
      async () => {
        runs++;
        if (runs === 1) throw new Worker.RateLimitError();
        if (runs === 2) throw new Error('boom');
        return 'ok';
      },
      { connection: CONNECTION, subscription: 'sub-a', blockTimeout: 300, limiter: { max: 100, duration: 200 } },
    );
    worker.on('error', () => {});
    worker.on('completed', (job: any) => completed.push(job.id));
    worker.on('failed', (job: any) => failed.push(job.id));
    try {
      await worker.waitUntilReady();
      const id = await broadcast.publish('evt', {}, { attempts: 2 });
      await waitFor(() => completed.includes(id!), 15000);
      expect(failed).toEqual([id]);
      expect(runs).toBe(3);
      expect(String(await cleanupClient.hget(`${k.job(id!)}:sub:sub-a`, 'a'))).toBe('1');
    } finally {
      await worker.close();
      await broadcast.close();
    }
  }, 20000);
});
