/**
 * Server-function correctness regressions (2026-09-29 audit).
 *
 * Run: npx vitest run tests/lua-correctness-20260929.test.ts
 */
import { afterAll, beforeAll, expect, it } from 'vitest';
import { createCleanupClient, describeEachMode, flushQueue, waitFor } from './helpers/fixture';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { FlowProducer } = require('../dist/flow-producer') as typeof import('../src/flow-producer');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

describeEachMode('Lua correctness 2026-09-29', (CONNECTION) => {
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

  async function hget(key: string, field: string): Promise<string | null> {
    const v = await cleanupClient.hget(key, field);
    return v == null ? null : String(v);
  }

  it('removing every same-queue child releases the parent', async () => {
    const Q = uniqueQueue('lc-rm-child');
    const flow = new FlowProducer({ connection: CONNECTION });
    const queue = new Queue(Q, { connection: CONNECTION });
    try {
      const node = await flow.add({
        name: 'parent',
        queueName: Q,
        data: {},
        children: [
          { name: 'c1', queueName: Q, data: {} },
          { name: 'c2', queueName: Q, data: {} },
        ],
      });
      const k = buildKeys(Q);
      for (const child of node.children!) {
        const job = await queue.getJob(child.job.id);
        await job!.remove();
      }
      expect(await hget(k.job(node.job.id), 'state')).toBe('waiting');
    } finally {
      await flow.close();
      await queue.close();
    }
  });

  it('removing a child after a sibling completed releases the parent once', async () => {
    const Q = uniqueQueue('lc-rm-child-mix');
    const flow = new FlowProducer({ connection: CONNECTION });
    const queue = new Queue(Q, { connection: CONNECTION });
    try {
      const node = await flow.add({
        name: 'parent',
        queueName: Q,
        data: {},
        children: [
          { name: 'c1', queueName: Q, data: {}, opts: { delay: 60_000 } },
          { name: 'c2', queueName: Q, data: {} },
        ],
      });
      const k = buildKeys(Q);
      const processed: string[] = [];
      const worker = new Worker(
        Q,
        async (job) => {
          processed.push(job.name);
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 50, stalledInterval: 60_000 },
      );
      worker.on('error', () => {});
      try {
        await waitFor(() => processed.includes('c2'), 5000, 25);
        const delayed = await queue.getJob(node.children![0].job.id);
        await delayed!.remove();
        await waitFor(() => processed.includes('parent'), 5000, 25);
        expect(await hget(k.job(node.job.id), 'depsCompleted')).toBe('2');
        expect(processed.filter((n) => n === 'parent')).toHaveLength(1);
      } finally {
        await worker.close(true);
      }
    } finally {
      await flow.close();
      await queue.close();
    }
  });

  it('removing a cross-queue child releases the parent', async () => {
    const PQ = uniqueQueue('lc-rm-xq-parent');
    const CQ = uniqueQueue('lc-rm-xq-child');
    const flow = new FlowProducer({ connection: CONNECTION });
    const childQueue = new Queue(CQ, { connection: CONNECTION });
    try {
      const node = await flow.add({
        name: 'parent',
        queueName: PQ,
        data: {},
        // A nested child keeps the flow slot-safe in cluster mode.
        children: [{ name: 'c1', queueName: CQ, data: {}, children: [{ name: 'gc', queueName: CQ, data: {} }] }],
      });
      const job = await childQueue.getJob(node.children![0].job.id);
      await job!.remove();
      expect(await hget(buildKeys(PQ).job(node.job.id), 'state')).toBe('waiting');
      expect((await cleanupClient.smembers(buildKeys(CQ).xqPending)).size).toBe(0);
    } finally {
      await flow.close();
      await childQueue.close();
    }
  });

  it('removing a parent leaves no deps set and later child completion is harmless', async () => {
    const Q = uniqueQueue('lc-rm-parent');
    const flow = new FlowProducer({ connection: CONNECTION });
    const queue = new Queue(Q, { connection: CONNECTION });
    try {
      const node = await flow.add({
        name: 'parent',
        queueName: Q,
        data: {},
        children: [{ name: 'c1', queueName: Q, data: {} }],
      });
      const k = buildKeys(Q);
      const parent = await queue.getJob(node.job.id);
      await parent!.remove();
      expect(await cleanupClient.exists([k.deps(node.job.id)])).toBe(0);
      const worker = new Worker(Q, async () => 'ok', {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 50,
        stalledInterval: 60_000,
      });
      worker.on('error', () => {});
      try {
        await waitFor(async () => (await hget(k.job(node.children![0].job.id), 'state')) === 'completed', 5000, 25);
        expect(await cleanupClient.exists([k.job(node.job.id)])).toBe(0);
      } finally {
        await worker.close(true);
      }
    } finally {
      await flow.close();
      await queue.close();
    }
  });
  async function processedBy(Q: string, names: string[], timeoutMs = 5000): Promise<string[]> {
    const processed: string[] = [];
    const worker = new Worker(
      Q,
      async (job) => {
        processed.push(job.name);
        return 'ok';
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 50, stalledInterval: 60_000 },
    );
    worker.on('error', () => {});
    try {
      await waitFor(() => names.every((n) => processed.includes(n)), timeoutMs, 25).catch(() => {});
    } finally {
      await worker.close(true);
    }
    return processed;
  }

  it('drain closes ordering holes so later ordered jobs still run', async () => {
    const Q = uniqueQueue('lc-drain-ord');
    const queue = new Queue(Q, { connection: CONNECTION });
    try {
      for (let i = 0; i < 3; i++) await queue.add(`old-${i}`, {}, { ordering: { key: 'k' } });
      await queue.add('old-delayed', {}, { ordering: { key: 'k' }, delay: 60_000 });
      await queue.drain(true);
      await queue.add('fresh', {}, { ordering: { key: 'k' } });
      expect(await processedBy(Q, ['fresh'])).toEqual(['fresh']);
    } finally {
      await queue.close();
    }
  });
});
