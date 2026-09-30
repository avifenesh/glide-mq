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
const { BaseWorker } = require('../dist/base-worker') as typeof import('../src/base-worker');

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

  it('B3: onExceeded pause parks the job for a bounded re-check and updateFlowBudget resumes it', async () => {
    const Q = uniqueQueue('r4-budget-pause');
    const k = buildKeys(Q);
    const queue = new Queue(Q, { connection: CONNECTION });
    const flow = new FlowProducer({ connection: CONNECTION });
    const completed: string[] = [];
    const paused: string[] = [];
    let reported = false;
    const worker = new Worker(
      Q,
      async (job: any) => {
        if (job.name !== 'parent' && !reported) {
          reported = true;
          await job.reportUsage({ costs: { total: 1.5 } });
        }
        return 'ok';
      },
      { connection: CONNECTION, blockTimeout: 300 },
    );
    worker.on('error', () => {});
    worker.on('completed', (job: any) => completed.push(job.name));
    worker.on('budget-exceeded', (job: any) => paused.push(job.id));
    try {
      const node = await flow.add(
        {
          name: 'parent',
          queueName: Q,
          data: {},
          children: [
            { name: 'c1', queueName: Q, data: {} },
            { name: 'c2', queueName: Q, data: {} },
          ],
        },
        { budget: { maxTotalCost: 1, onExceeded: 'pause' } },
      );
      const flowId = node.job.id;
      const childIds = node.children!.map((c) => c.job.id);
      // The child that crossed the limit completes; the other one is parked.
      let pausedId = '';
      await waitFor(async () => {
        for (const id of childIds) {
          if (String(await cleanupClient.hget(k.job(id), 'state')) === 'delayed') pausedId = id;
        }
        return pausedId !== '';
      }, 15000);
      // Parked for one re-check interval, not a day.
      const score = Number(await cleanupClient.zscore(k.scheduled, pausedId));
      expect(score).toBeGreaterThan(Date.now() + 1000);
      expect(score).toBeLessThanOrEqual(Date.now() + BaseWorker.BUDGET_PAUSE_RECHECK_MS + 1000);

      // A limit still below the charged usage keeps the budget exceeded.
      let budget = await queue.updateFlowBudget(flowId, { maxTotalCost: 1.2 });
      expect(budget).toMatchObject({ maxTotalCost: 1.2, exceeded: true, usedCost: 1.5 });
      expect(String(await cleanupClient.hget(k.job(pausedId), 'state'))).toBe('delayed');

      // Raising above the usage clears exceeded; promoting runs the job now.
      budget = await queue.updateFlowBudget(flowId, { maxTotalCost: 100, costUnit: 'usd' });
      expect(budget).toMatchObject({ maxTotalCost: 100, costUnit: 'usd', exceeded: false, onExceeded: 'pause' });
      expect(await cleanupClient.hget(k.budget(flowId), 'exceeded')).toBeNull();
      await (await queue.getJob(pausedId))!.promote();
      await waitFor(() => completed.includes('parent'), 15000);
      expect(completed.sort()).toEqual(['c1', 'c2', 'parent']);
      // budget-exceeded fires for the job that crossed the limit and for the parked one.
      expect(new Set(paused)).toEqual(new Set(childIds));

      expect(await queue.updateFlowBudget('no-such-flow', { maxTotalCost: 1 })).toBeNull();
      await expect(queue.updateFlowBudget(flowId, { maxTotalCost: -1 })).rejects.toThrow(/finite number/);
      // null deletes a limit.
      budget = await queue.updateFlowBudget(flowId, { maxTotalCost: null, maxTokens: { input: 10 } });
      expect(budget!.maxTotalCost).toBeUndefined();
      expect(budget!.maxTokens).toEqual({ input: 10 });
    } finally {
      await worker.close();
      await flow.close();
      await queue.close();
    }
  }, 30000);

  it('B4: a mixed success/failure stream is processed on the chain with one FCALL per failure', async () => {
    const Q = uniqueQueue('r4-faf');
    const DLQ = uniqueQueue('r4-faf-dlq');
    const k = buildKeys(Q);
    const queue = new Queue(Q, { connection: CONNECTION });
    const dlq = new Queue(DLQ, { connection: CONNECTION });
    const completed: string[] = [];
    const failed: [string, string][] = [];
    const worker = new Worker(
      Q,
      async (job: any) => {
        if (job.data.mode === 'fail') throw new Error(`boom ${job.data.i}`);
        if (job.data.mode === 'flaky' && job.attemptsMade === 0) throw new Error('flaky');
        return `ok ${job.data.i}`;
      },
      { connection: CONNECTION, blockTimeout: 300, deadLetterQueue: { name: DLQ } },
    );
    worker.on('error', () => {});
    worker.on('completed', (job: any) => completed.push(job.id));
    worker.on('failed', (job: any, err: Error) => failed.push([job.id, err.message]));
    try {
      await worker.waitUntilReady();
      const calls: Record<string, number> = {};
      const client = (worker as any).commandClient;
      const realFcall = client.fcall.bind(client);
      client.fcall = (func: string, ...rest: any[]) => {
        calls[func] = (calls[func] ?? 0) + 1;
        return realFcall(func, ...rest);
      };

      const jobs: { id: string; mode: string }[] = [];
      for (let i = 0; i < 24; i++) {
        const mode = i % 3 === 0 ? 'fail' : i % 3 === 1 ? 'flaky' : 'ok';
        const opts = mode === 'flaky' ? { attempts: 2 } : { attempts: 1 };
        const job = await queue.add('j', { i, mode }, opts);
        jobs.push({ id: job!.id, mode });
      }
      const terminalFails = jobs.filter((j) => j.mode === 'fail').map((j) => j.id);
      await waitFor(() => completed.length === 16 && failed.length === 16, 20000);

      expect(completed.sort()).toEqual(
        jobs
          .filter((j) => j.mode !== 'fail')
          .map((j) => j.id)
          .sort(),
      );
      // Each terminal failure and each flaky first attempt emits 'failed'.
      expect(failed.filter(([, m]) => m === 'flaky')).toHaveLength(8);
      expect(
        failed
          .filter(([, m]) => m.startsWith('boom'))
          .map(([id]) => id)
          .sort(),
      ).toEqual([...terminalFails].sort());
      for (const j of jobs) {
        const [state, attemptsMade, failedReason] = (
          await cleanupClient.hmget(k.job(j.id), ['state', 'attemptsMade', 'failedReason'])
        ).map((v: any) => (v === null ? null : String(v)));
        if (j.mode === 'fail')
          expect([state, attemptsMade, failedReason]).toEqual(['failed', '1', expect.stringMatching(/^boom/)]);
        else if (j.mode === 'flaky') expect([state, attemptsMade, failedReason]).toEqual(['completed', '1', 'flaky']);
        else expect([state, attemptsMade]).toEqual(['completed', '0']);
      }
      const counts = await queue.getJobCounts();
      expect(counts.completed).toBe(16);
      expect(counts.failed).toBe(8);
      expect(counts.active).toBe(0);
      expect(counts.waiting).toBe(0);
      // Terminal failures reached the DLQ through the chained path.
      const dlqJobs = await dlq.getJobs('waiting');
      expect(dlqJobs.map((j: any) => j.data.originalJobId).sort()).toEqual([...terminalFails].sort());
      expect(dlqJobs.map((j: any) => j.data.data.i).sort((a: number, b: number) => a - b)).toEqual(
        jobs.filter((j) => j.mode === 'fail').map((_, idx) => idx * 3),
      );
      // Every failure went through failAndFetchNext; plain glidemq_fail was never needed.
      expect(calls.glidemq_failAndFetchNext).toBe(16);
      expect(calls.glidemq_fail ?? 0).toBe(0);
    } finally {
      await worker.close();
      await queue.close();
      await flushQueue(cleanupClient, DLQ).catch(() => {});
      await dlq.close();
    }
  }, 30000);

  it('B5: Broadcast emits trimmed with the (message, subscription) pairs dropped unread', async () => {
    const Q = uniqueQueue('r4-bcast-trim');
    const k = buildKeys(Q);
    const broadcast = new Broadcast(Q, { connection: CONNECTION, maxMessages: 3 });
    const events: { trimmed: number; unread: number }[] = [];
    broadcast.on('trimmed', (e: { trimmed: number; unread: number }) => events.push(e));
    try {
      for (let i = 0; i < 3; i++) await broadcast.publish('evt', { i });
      // No trim while the stream fits.
      expect(events).toEqual([]);
      // 'slow' has read nothing; 'caught-up' has read everything published so far.
      await cleanupClient.xgroupCreate(k.stream, 'slow', '0');
      await cleanupClient.xgroupCreate(k.stream, 'caught-up', '$');

      for (let i = 3; i < 6; i++) await broadcast.publish('evt', { i });
      // Each publish trimmed one message that only 'slow' had not read.
      expect(events).toEqual([
        { trimmed: 1, unread: 1 },
        { trimmed: 1, unread: 1 },
        { trimmed: 1, unread: 1 },
      ]);
      await broadcast.publish('evt', { i: 6 });
      // Message 4 was published after both groups existed: unread by both.
      expect(events.at(-1)).toEqual({ trimmed: 1, unread: 2 });
      expect(await cleanupClient.xlen(k.stream)).toBe(3);
    } finally {
      await broadcast.close();
    }
  }, 20000);
});
