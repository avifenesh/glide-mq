/**
 * Tests for custom job IDs (issue #79).
 * Requires: valkey-server running on localhost:6379 and cluster on :7000-7005
 *
 * Run: npx vitest run tests/custom-job-id.test.ts
 */
import { it, expect, beforeAll, afterAll, describe } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { FlowProducer } = require('../dist/flow-producer') as typeof import('../src/flow-producer');
const { TestQueue, TestWorker } = require('../dist/testing') as typeof import('../src/testing');

import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

describeEachMode('Custom Job IDs', (CONNECTION) => {
  const Q = 'test-custom-id-' + Date.now();
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

  it('assigns custom job ID when jobId is provided', async () => {
    const job = await queue.add('task', { x: 1 }, { jobId: 'my-custom-id' });
    expect(job).not.toBeNull();
    expect(job!.id).toBe('my-custom-id');

    const fetched = await queue.getJob('my-custom-id');
    expect(fetched).not.toBeNull();
    expect(fetched!.name).toBe('task');
    expect(fetched!.data).toEqual({ x: 1 });
  });

  it('returns null for duplicate custom job ID', async () => {
    const job1 = await queue.add('task', { a: 1 }, { jobId: 'dup-test-id' });
    expect(job1).not.toBeNull();

    const job2 = await queue.add('task', { a: 2 }, { jobId: 'dup-test-id' });
    expect(job2).toBeNull();
  });

  it('auto-generates ID when jobId is not provided', async () => {
    const job = await queue.add('task', { auto: true });
    expect(job).not.toBeNull();
    expect(Number(job!.id)).toBeGreaterThan(0);
  });

  it('custom ID with delay', async () => {
    const job = await queue.add('delayed', { d: 1 }, { jobId: 'delayed-custom', delay: 60000 });
    expect(job).not.toBeNull();
    expect(job!.id).toBe('delayed-custom');

    const fetched = await queue.getJob('delayed-custom');
    expect(fetched).not.toBeNull();
  });

  it('custom ID with priority', async () => {
    const job = await queue.add('prio', { p: 1 }, { jobId: 'prio-custom', priority: 5 });
    expect(job).not.toBeNull();
    expect(job!.id).toBe('prio-custom');

    const fetched = await queue.getJob('prio-custom');
    expect(fetched).not.toBeNull();
  });

  it('custom ID processed by worker', async () => {
    const workerQ = 'test-worker-custom-' + Date.now();
    const wQueue = new Queue(workerQ, { connection: CONNECTION });
    let worker: InstanceType<typeof Worker> | undefined;
    try {
      const processedId = await new Promise<string>(async (resolve, reject) => {
        worker = new Worker(workerQ, async (job: any) => {
          resolve(job.id);
          return 'done';
        }, { connection: CONNECTION });

        try {
          await wQueue.add('work', { val: 42 }, { jobId: 'worker-custom-id' });
          await new Promise((r) => setTimeout(r, 2000));
        } catch (e) {
          reject(e);
        }
      });

      expect(processedId).toBe('worker-custom-id');
    } finally {
      if (worker) await worker.close();
      await wQueue.close();
      await flushQueue(cleanupClient, workerQ);
    }
  });

  it('throws for jobId exceeding 256 characters', async () => {
    const longId = 'x'.repeat(257);
    await expect(queue.add('task', {}, { jobId: longId })).rejects.toThrow('jobId must be at most 256 characters');
  });

  it('accepts jobId of exactly 256 characters', async () => {
    const id256 = 'b'.repeat(256);
    const job = await queue.add('task', { boundary: true }, { jobId: id256 });
    expect(job).not.toBeNull();
    expect(job!.id).toBe(id256);
  });

  it('custom ID with deduplication', async () => {
    const job = await queue.add('dedup-task', { v: 1 }, {
      jobId: 'custom-with-dedup',
      deduplication: { id: 'dedup-key-1', mode: 'simple' },
    });
    expect(job).not.toBeNull();
    expect(job!.id).toBe('custom-with-dedup');

    // Same dedup key should skip (dedup takes precedence)
    const job2 = await queue.add('dedup-task', { v: 2 }, {
      jobId: 'custom-with-dedup-2',
      deduplication: { id: 'dedup-key-1', mode: 'simple' },
    });
    expect(job2).toBeNull();
  });
});

describeEachMode('Custom Job IDs - addBulk', (CONNECTION) => {
  const Q = 'test-bulk-custom-id-' + Date.now();
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

  it('addBulk with custom IDs', async () => {
    const jobs = await queue.addBulk([
      { name: 'a', data: { i: 1 }, opts: { jobId: 'bulk-id-1' } },
      { name: 'b', data: { i: 2 }, opts: { jobId: 'bulk-id-2' } },
      { name: 'c', data: { i: 3 } }, // auto-generated
    ]);

    expect(jobs.length).toBe(3);
    expect(jobs[0].id).toBe('bulk-id-1');
    expect(jobs[1].id).toBe('bulk-id-2');
    expect(Number(jobs[2].id)).toBeGreaterThan(0);
  });

  it('addBulk with duplicate custom IDs skips duplicates', async () => {
    const jobs = await queue.addBulk([
      { name: 'x', data: {}, opts: { jobId: 'bulk-dup-1' } },
      { name: 'y', data: {}, opts: { jobId: 'bulk-dup-1' } }, // duplicate
      { name: 'z', data: {}, opts: { jobId: 'bulk-dup-2' } },
    ]);

    // First and third succeed, second is skipped
    expect(jobs.length).toBe(2);
    expect(jobs[0].id).toBe('bulk-dup-1');
    expect(jobs[1].id).toBe('bulk-dup-2');
  });

  it('addBulk throws for jobId exceeding 256 characters', async () => {
    const longId = 'z'.repeat(257);
    await expect(
      queue.addBulk([{ name: 'task', data: {}, opts: { jobId: longId } }]),
    ).rejects.toThrow('jobId must be at most 256 characters');
  });
});

describeEachMode('Custom Job IDs - FlowProducer', (CONNECTION) => {
  const Q_PARENT = 'test-flow-parent-custom-' + Date.now();
  const Q_CHILD = 'test-flow-child-custom-' + Date.now();
  let flow: InstanceType<typeof FlowProducer>;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
    flow = new FlowProducer({ connection: CONNECTION });
  });

  afterAll(async () => {
    await flow.close();
    await flushQueue(cleanupClient, Q_PARENT);
    await flushQueue(cleanupClient, Q_CHILD);
    cleanupClient.close();
  });

  it('flow with custom IDs on parent and children', async () => {
    const result = await flow.add({
      name: 'parent-task',
      queueName: Q_PARENT,
      data: { parent: true },
      opts: { jobId: 'flow-parent-1' },
      children: [
        {
          name: 'child-task-1',
          queueName: Q_CHILD,
          data: { child: 1 },
          opts: { jobId: 'flow-child-1' },
        },
        {
          name: 'child-task-2',
          queueName: Q_CHILD,
          data: { child: 2 },
          opts: { jobId: 'flow-child-2' },
        },
      ],
    });

    expect(result.job.id).toBe('flow-parent-1');
    expect(result.children).toHaveLength(2);
    expect(result.children![0].job.id).toBe('flow-child-1');
    expect(result.children![1].job.id).toBe('flow-child-2');
  });

  it('flow with duplicate parent ID throws', async () => {
    await expect(
      flow.add({
        name: 'parent-dup',
        queueName: Q_PARENT,
        data: {},
        opts: { jobId: 'flow-parent-1' }, // already exists
        children: [
          { name: 'child', queueName: Q_CHILD, data: {} },
        ],
      }),
    ).rejects.toThrow('Duplicate job ID');
  });

  it('flow with duplicate child ID throws', async () => {
    await expect(
      flow.add({
        name: 'parent-new',
        queueName: Q_PARENT,
        data: {},
        children: [
          { name: 'child-dup', queueName: Q_CHILD, data: {}, opts: { jobId: 'flow-child-1' } }, // already exists
        ],
      }),
    ).rejects.toThrow('Duplicate job ID');
  });

  it('flow leaf (no children) with duplicate ID throws', async () => {
    // First add a leaf flow job
    await flow.add({
      name: 'leaf',
      queueName: Q_PARENT,
      data: {},
      opts: { jobId: 'leaf-custom-1' },
    });

    // Duplicate leaf should throw
    await expect(
      flow.add({
        name: 'leaf-dup',
        queueName: Q_PARENT,
        data: {},
        opts: { jobId: 'leaf-custom-1' },
      }),
    ).rejects.toThrow('Duplicate job ID');
  });
});

describeEachMode('Custom Job IDs - auto-increment collision guard', (CONNECTION) => {
  const Q = 'test-collision-guard-' + Date.now();
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

  it('auto-increment skips over existing custom-ID jobs', async () => {
    // Add auto-generated job to get counter to 1
    const auto1 = await queue.add('first', { i: 1 });
    expect(auto1!.id).toBe('1');

    // Now add a custom job with ID "2" (the next INCR value)
    const custom = await queue.add('custom', { i: 2 }, { jobId: '2' });
    expect(custom!.id).toBe('2');

    // Next auto-generated job should skip "2" and use "3"
    const auto2 = await queue.add('third', { i: 3 });
    expect(auto2!.id).toBe('3');
  });
});

describe('Custom Job IDs - Testing Mode', () => {
  it('TestQueue assigns custom job ID', async () => {
    const queue = new TestQueue('test-q');
    const job = await queue.add('task', { x: 1 }, { jobId: 'test-custom' });
    expect(job).not.toBeNull();
    expect(job!.id).toBe('test-custom');

    const fetched = await queue.getJob('test-custom');
    expect(fetched).not.toBeNull();
    expect(fetched!.data).toEqual({ x: 1 });
  });

  it('TestQueue returns null for duplicate custom ID', async () => {
    const queue = new TestQueue('test-q2');
    const job1 = await queue.add('task', { a: 1 }, { jobId: 'dup-id' });
    expect(job1).not.toBeNull();

    const job2 = await queue.add('task', { a: 2 }, { jobId: 'dup-id' });
    expect(job2).toBeNull();
  });

  it('TestQueue mixes custom and auto-generated IDs', async () => {
    const queue = new TestQueue('test-q3');
    const auto1 = await queue.add('auto', { i: 1 });
    const custom = await queue.add('custom', { i: 2 }, { jobId: 'my-id' });
    const auto2 = await queue.add('auto', { i: 3 });

    expect(auto1!.id).toBe('1');
    expect(custom!.id).toBe('my-id');
    expect(auto2!.id).toBe('2');
  });

  it('TestQueue custom ID processed by TestWorker', async () => {
    const queue = new TestQueue('test-q4');
    let processedId = '';
    const worker = new TestWorker(queue, async (job) => {
      processedId = job.id;
      return 'ok';
    });

    await queue.add('task', {}, { jobId: 'worker-test-id' });
    await new Promise((r) => setTimeout(r, 100));

    expect(processedId).toBe('worker-test-id');
    await worker.close();
  });

  it('TestQueue throws for jobId exceeding 256 characters', async () => {
    const queue = new TestQueue('test-q5');
    const longId = 'a'.repeat(257);
    await expect(queue.add('task', {}, { jobId: longId })).rejects.toThrow('jobId must be at most 256 characters');
  });
});
