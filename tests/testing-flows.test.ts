/**
 * Flows in the in-memory testing mode: TestFlowProducer, getChildrenValues, getParents and
 * moveToWaitingChildren. No Valkey required.
 *
 * Run: npx vitest run tests/testing-flows.test.ts
 */
import { describe, it, expect, afterEach } from 'vitest';
import { TestQueue, TestWorker, TestFlowProducer, TestJob } from '../src/testing';
import type { FlowJob } from '../src/types';
import { MAX_JOB_DATA_SIZE } from '../src/utils';
import { BatchError, WaitingChildrenError } from '../src/errors';
import { waitFor } from './helpers/fixture';

let queues: TestQueue[] = [];
let workers: TestWorker[] = [];

function open(name: string, opts?: ConstructorParameters<typeof TestQueue>[1]): TestQueue {
  const queue = new TestQueue(name, opts);
  queues.push(queue);
  return queue;
}

function work(queue: TestQueue, processor: (job: TestJob) => Promise<any>, concurrency = 1): TestWorker {
  const worker = new TestWorker(queue, processor as any, { concurrency });
  workers.push(worker);
  return worker;
}

afterEach(async () => {
  for (const worker of workers) await worker.close();
  for (const queue of queues) await queue.close();
  workers = [];
  queues = [];
});

describe('TestFlowProducer.add', () => {
  it('returns a bare node for a flow without children', async () => {
    const q = open('flow-leaf');
    const node = await new TestFlowProducer().add({ name: 'solo', queueName: 'flow-leaf', data: { a: 1 } });
    expect(node.children).toBeUndefined();
    expect(node.job).toBeInstanceOf(TestJob);
    expect(await node.job.getState()).toBe('waiting');
    expect((await q.getJob(node.job.id))!.data).toEqual({ a: 1 });
  });

  it('starts the parent in waiting-children and the children in waiting', async () => {
    const q = open('flow-shape');
    const node = await new TestFlowProducer().add({
      name: 'parent',
      queueName: 'flow-shape',
      data: {},
      children: [
        { name: 'c1', queueName: 'flow-shape', data: 1 },
        { name: 'c2', queueName: 'flow-shape', data: 2 },
      ],
    });
    expect(node.children).toHaveLength(2);
    expect(await node.job.getState()).toBe('waiting-children');
    expect(await Promise.all(node.children!.map((c) => c.job.getState()))).toEqual(['waiting', 'waiting']);
    expect(node.children!.map((c) => [c.job.parentId, c.job.parentQueue])).toEqual([
      [node.job.id, 'flow-shape'],
      [node.job.id, 'flow-shape'],
    ]);
    expect(await q.getJobCounts()).toEqual({ waiting: 2, active: 0, delayed: 0, completed: 0, failed: 0 });
    expect(await q.getJobs('waiting')).toHaveLength(2);
  });

  it('moves the parent to waiting only when its last child completes', async () => {
    const q = open('flow-release');
    q.pause();
    const node = await new TestFlowProducer().add({
      name: 'parent',
      queueName: 'flow-release',
      data: {},
      children: [
        { name: 'c1', queueName: 'flow-release', data: 1 },
        { name: 'c2', queueName: 'flow-release', data: 2 },
      ],
    });
    const order: string[] = [];
    const worker = work(q, async (job) => {
      order.push(job.name);
      if (job.name === 'parent') return Object.values(await job.getChildrenValues());
      return job.data * 10;
    });
    worker.on('completed', (job: TestJob) => {
      if (job.name === 'c1') expect(node.job.getState()).resolves.toBe('waiting-children');
    });
    q.resume();
    await waitFor(async () => (await node.job.getState()) === 'completed', 2000, 2);
    expect(order).toEqual(['c1', 'c2', 'parent']);
    expect((await q.getJob(node.job.id))!.returnvalue).toEqual([10, 20]);
  });

  it('keys getChildrenValues by prefix:{queue}:id and honours the prefix option', async () => {
    const q = open('flow-keys');
    const node = await new TestFlowProducer({ prefix: 'pfx' }).add({
      name: 'parent',
      queueName: 'flow-keys',
      data: {},
      children: [{ name: 'c', queueName: 'flow-keys', data: 5 }],
    });
    work(q, async (job) => (job.name === 'parent' ? job.getChildrenValues() : 'done'));
    await waitFor(async () => (await node.job.getState()) === 'completed', 2000, 2);
    expect((await q.getJob(node.job.id))!.returnvalue).toEqual({
      [`pfx:{flow-keys}:${node.children![0].job.id}`]: 'done',
    });
  });

  it('defaults the key prefix to glide', async () => {
    const q = open('flow-default-prefix');
    const node = await new TestFlowProducer().add({
      name: 'parent',
      queueName: 'flow-default-prefix',
      data: {},
      children: [{ name: 'c', queueName: 'flow-default-prefix', data: 5 }],
    });
    work(q, async (job) => (job.name === 'parent' ? job.getChildrenValues() : 7));
    await waitFor(async () => (await node.job.getState()) === 'completed', 2000, 2);
    expect(Object.keys((await q.getJob(node.job.id))!.returnvalue)).toEqual([
      `glide:{flow-default-prefix}:${node.children![0].job.id}`,
    ]);
  });

  it('runs children in other queues and reads their values by their own queue name', async () => {
    const parentQ = open('flow-x-parent');
    const emailQ = open('flow-x-email');
    const smsQ = open('flow-x-sms');
    const node = await new TestFlowProducer().add({
      name: 'notify',
      queueName: 'flow-x-parent',
      data: {},
      children: [
        { name: 'email', queueName: 'flow-x-email', data: 'e' },
        { name: 'sms', queueName: 'flow-x-sms', data: 's' },
      ],
    });
    expect(await node.job.getState()).toBe('waiting-children');
    work(parentQ, async (job) => job.getChildrenValues());
    work(emailQ, async () => 'sent-email');
    await new Promise((r) => setTimeout(r, 20));
    expect(await node.job.getState()).toBe('waiting-children');
    work(smsQ, async () => 'sent-sms');
    await waitFor(async () => (await node.job.getState()) === 'completed', 2000, 2);
    expect((await parentQ.getJob(node.job.id))!.returnvalue).toEqual({
      'glide:{flow-x-email}:1': 'sent-email',
      'glide:{flow-x-sms}:1': 'sent-sms',
    });
    expect(await node.children![0].job.getParents()).toEqual([{ queue: 'flow-x-parent', id: node.job.id }]);
  });

  it('builds nested flows bottom-up and releases each level in turn', async () => {
    const q = open('flow-nested');
    const flow: FlowJob = {
      name: 'root',
      queueName: 'flow-nested',
      data: {},
      children: [
        {
          name: 'mid',
          queueName: 'flow-nested',
          data: {},
          children: [{ name: 'leaf', queueName: 'flow-nested', data: {} }],
        },
        { name: 'side', queueName: 'flow-nested', data: {} },
      ],
    };
    q.pause();
    const node = await new TestFlowProducer().add(flow);
    const mid = node.children![0];
    // A sub-flow is created first (its parent, then its leaves), then the root, then the root's leaf children.
    expect([mid.job.id, mid.children![0].job.id, node.job.id, node.children![1].job.id]).toEqual(['1', '2', '3', '4']);
    expect(await mid.job.getState()).toBe('waiting-children');
    expect(await mid.children![0].job.getParents()).toEqual([{ queue: 'flow-nested', id: mid.job.id }]);
    expect(await mid.job.getParents()).toEqual([{ queue: 'flow-nested', id: node.job.id }]);
    const order: string[] = [];
    work(q, async (job) => {
      order.push(job.name);
      return job.name;
    });
    q.resume();
    await waitFor(async () => (await node.job.getState()) === 'completed', 2000, 2);
    expect(order.indexOf('leaf')).toBeLessThan(order.indexOf('mid'));
    expect(order.indexOf('mid')).toBeLessThan(order.indexOf('root'));
    expect(order.indexOf('side')).toBeLessThan(order.indexOf('root'));
  });

  it('ignores delay on a parent and keeps it waiting for its children', async () => {
    open('flow-delay');
    const node = await new TestFlowProducer().add({
      name: 'parent',
      queueName: 'flow-delay',
      data: {},
      opts: { delay: 50, priority: 2 },
      children: [{ name: 'c', queueName: 'flow-delay', data: {}, opts: { delay: 5_000 } }],
    });
    expect(await node.job.getState()).toBe('waiting-children');
    expect(await node.children![0].job.getState()).toBe('delayed');
  });

  it('uses a custom job id', async () => {
    const q = open('flow-custom-id');
    const node = await new TestFlowProducer().add({
      name: 'p',
      queueName: 'flow-custom-id',
      data: {},
      opts: { jobId: 'root-1' },
      children: [{ name: 'c', queueName: 'flow-custom-id', data: {}, opts: { jobId: 'child-1' } }],
    });
    expect([node.job.id, node.children![0].job.id]).toEqual(['root-1', 'child-1']);
    expect(await q.getJob('child-1')).not.toBeNull();
  });

  it('rejects a flow with an unknown queue before adding anything', async () => {
    const q = open('flow-known');
    await expect(
      new TestFlowProducer().add({
        name: 'p',
        queueName: 'flow-known',
        data: {},
        children: [{ name: 'c', queueName: 'flow-missing', data: {} }],
      }),
    ).rejects.toThrow('TestQueue "flow-missing" is not open');
    expect(await q.getJobCounts()).toEqual({ waiting: 0, active: 0, delayed: 0, completed: 0, failed: 0 });
  });

  it('rejects an invalid queue name, option or payload and adds nothing', async () => {
    const q = open('flow-invalid');
    const producer = new TestFlowProducer();
    const child = (opts?: FlowJob['opts'], data: unknown = {}): FlowJob => ({
      name: 'c',
      queueName: 'flow-invalid',
      data,
      opts,
    });
    const parent = (children: FlowJob[]): FlowJob => ({ name: 'p', queueName: 'flow-invalid', data: {}, children });
    await expect(producer.add({ name: 'p', queueName: 'bad:name', data: {} })).rejects.toThrow('Queue name');
    await expect(producer.add(parent([child({ priority: -1 })]))).rejects.toThrow('Priority');
    await expect(producer.add(parent([child({ jobId: 'a:b' })]))).rejects.toThrow('jobId');
    await expect(producer.add(parent([child({}, 'x'.repeat(MAX_JOB_DATA_SIZE + 1))]))).rejects.toThrow(
      'Job data exceeds maximum size',
    );
    expect((await q.getJobCounts()).waiting).toBe(0);
  });

  it('rejects a custom id used twice in a flow or already taken', async () => {
    const q = open('flow-dup');
    const producer = new TestFlowProducer();
    await expect(
      producer.add({
        name: 'p',
        queueName: 'flow-dup',
        data: {},
        children: [
          { name: 'c1', queueName: 'flow-dup', data: {}, opts: { jobId: 'x' } },
          { name: 'c2', queueName: 'flow-dup', data: {}, opts: { jobId: 'x' } },
        ],
      }),
    ).rejects.toThrow('Duplicate job ID in flow');
    await q.add('existing', {}, { jobId: 'taken' });
    await expect(
      producer.add({ name: 'p', queueName: 'flow-dup', data: {}, opts: { jobId: 'taken' } }),
    ).rejects.toThrow('Duplicate job ID in flow');
    expect((await q.getJobCounts()).waiting).toBe(1);
  });

  it('generates ids that skip the custom ids of the same flow', async () => {
    open('flow-reserved');
    const node = await new TestFlowProducer().add({
      name: 'p',
      queueName: 'flow-reserved',
      data: {},
      children: [{ name: 'c', queueName: 'flow-reserved', data: {}, opts: { jobId: '1' } }],
    });
    expect([node.job.id, node.children![0].job.id]).toEqual(['2', '1']);
    expect(Object.keys(await node.job.getChildrenValues())).toEqual([]);
    const bulk = await new TestFlowProducer().addBulk([
      { name: 'a', queueName: 'flow-reserved', data: {} },
      { name: 'b', queueName: 'flow-reserved', data: {}, opts: { jobId: '4' } },
      { name: 'c', queueName: 'flow-reserved', data: {} },
    ]);
    expect(bulk.map((n) => n.job.id)).toEqual(['3', '4', '5']);
  });

  it('wires a leaf child to its parent before its added event', async () => {
    const q = open('flow-added');
    const seen: { parentId?: string; deps: number }[] = [];
    q.on('added', (job: TestJob) => {
      const parent = job.parentId ? q.jobs.get(job.parentId) : undefined;
      seen.push({ parentId: job.parentId, deps: parent?.deps?.size ?? 0 });
    });
    const node = await new TestFlowProducer().add({
      name: 'p',
      queueName: 'flow-added',
      data: {},
      children: [{ name: 'c', queueName: 'flow-added', data: {} }],
    });
    expect(seen).toEqual([
      { parentId: undefined, deps: 0 },
      { parentId: node.job.id, deps: 1 },
    ]);
    const dynamic = await q.add('d', {}, { parent: { queue: 'flow-added', id: node.job.id } });
    expect(seen[2]).toEqual({ parentId: node.job.id, deps: 2 });
    expect(await dynamic!.getParents()).toEqual([{ queue: 'flow-added', id: node.job.id }]);
  });

  it('keeps the flow prefix for children added later with opts.parent', async () => {
    const q = open('flow-late-prefix');
    const node = await new TestFlowProducer({ prefix: 'pfx' }).add({
      name: 'p',
      queueName: 'flow-late-prefix',
      data: {},
      children: [{ name: 'c', queueName: 'flow-late-prefix', data: {} }],
    });
    await q.add('d', {}, { parent: { queue: 'flow-late-prefix', id: node.job.id } });
    expect([...q.jobs.get(node.job.id)!.deps!.keys()]).toEqual([
      `pfx:{flow-late-prefix}:${node.children![0].job.id}`,
      'pfx:{flow-late-prefix}:3',
    ]);
  });

  it('allows the same custom id in different queues', async () => {
    open('flow-dup-a');
    open('flow-dup-b');
    const node = await new TestFlowProducer().add({
      name: 'p',
      queueName: 'flow-dup-a',
      data: {},
      opts: { jobId: 'same' },
      children: [{ name: 'c', queueName: 'flow-dup-b', data: {}, opts: { jobId: 'same' } }],
    });
    expect(node.children![0].job.id).toBe('same');
  });

  it('throws once closed', async () => {
    open('flow-closed');
    const producer = new TestFlowProducer();
    await producer.close();
    const flow: FlowJob = { name: 'p', queueName: 'flow-closed', data: {} };
    await expect(producer.add(flow)).rejects.toThrow('FlowProducer is closing');
    await expect(producer.addBulk([flow])).rejects.toThrow('FlowProducer is closing');
    await expect(producer.addDAG({ nodes: [{ name: 'a', queueName: 'flow-closed', data: {} }] })).rejects.toThrow(
      'FlowProducer is closing',
    );
  });
});

describe('TestFlowProducer.addBulk', () => {
  it('adds independent flows and returns one node per flow', async () => {
    const q = open('flow-bulk');
    const nodes = await new TestFlowProducer().addBulk([
      { name: 'a', queueName: 'flow-bulk', data: {}, children: [{ name: 'a1', queueName: 'flow-bulk', data: {} }] },
      { name: 'b', queueName: 'flow-bulk', data: {} },
    ]);
    expect(nodes).toHaveLength(2);
    expect(await nodes[0].job.getState()).toBe('waiting-children');
    expect(nodes[1].children).toBeUndefined();
    expect((await q.getJobCounts()).waiting).toBe(2);
  });

  it('validates every flow before adding the first', async () => {
    const q = open('flow-bulk-invalid');
    await expect(
      new TestFlowProducer().addBulk([
        { name: 'ok', queueName: 'flow-bulk-invalid', data: {} },
        { name: 'bad', queueName: 'flow-bulk-nope', data: {} },
      ]),
    ).rejects.toThrow('is not open');
    expect((await q.getJobCounts()).waiting).toBe(0);
  });
});

describe('TestFlowProducer budget', () => {
  it('creates a budget keyed by the root job id and stamps every job with it', async () => {
    const q = open('flow-budget');
    const node = await new TestFlowProducer().add(
      {
        name: 'p',
        queueName: 'flow-budget',
        data: {},
        children: [{ name: 'c', queueName: 'flow-budget', data: {} }],
      },
      { budget: { maxTotalTokens: 100, costUnit: 'usd' } },
    );
    expect(node.job.budgetKey).toBe(node.job.id);
    expect(node.children![0].job.budgetKey).toBe(node.job.id);
    expect(q.jobs.get(node.children![0].job.id)!.budgetKey).toBe(node.job.id);
    expect(await q.getFlowBudget(node.job.id)).toMatchObject({
      maxTotalTokens: 100,
      costUnit: 'usd',
      usedTokens: 0,
      exceeded: false,
      onExceeded: 'fail',
    });
  });

  it('fails the parent when a child exceeds the budget, across queues', async () => {
    const rootQ = open('flow-budget-root');
    const childQ = open('flow-budget-child');
    const node = await new TestFlowProducer().add(
      {
        name: 'p',
        queueName: 'flow-budget-root',
        data: {},
        children: [{ name: 'c', queueName: 'flow-budget-child', data: {} }],
      },
      { budget: { maxTotalTokens: 10 } },
    );
    const childKey = node.children![0].job.budgetKey!;
    expect(childKey).toBe(`flow-budget-root:${node.job.id}`);
    work(childQ, async (job) => {
      await job.reportUsage({ totalTokens: 50, tokens: { input: 50 } });
      return 'spent';
    });
    work(rootQ, async () => 'never');
    await waitFor(async () => (await node.job.getState()) === 'failed', 2000, 2);
    expect((await rootQ.getJob(node.job.id))!.failedReason).toBe('Budget exceeded');
    expect((await rootQ.getFlowBudget(node.job.id))!.exceeded).toBe(true);
    expect((await rootQ.getFlowBudget(node.job.id))!.usedTokens).toBe(50);
  });

  it('does not disturb another budget with the same id in a child queue', async () => {
    const rootQ = open('flow-budget-keep-root');
    const childQ = open('flow-budget-keep-child');
    childQ.setBudget('1', { maxTotalTokens: 7 });
    await new TestFlowProducer().add(
      {
        name: 'p',
        queueName: 'flow-budget-keep-root',
        data: {},
        children: [{ name: 'c', queueName: 'flow-budget-keep-child', data: {} }],
      },
      { budget: { maxTotalTokens: 99 } },
    );
    expect((await childQ.getFlowBudget('1'))!.maxTotalTokens).toBe(7);
    expect((await rootQ.getFlowBudget('1'))!.maxTotalTokens).toBe(99);
  });

  it('adds no budget without the option', async () => {
    const q = open('flow-no-budget');
    const node = await new TestFlowProducer().add({ name: 'p', queueName: 'flow-no-budget', data: {} });
    expect(node.job.budgetKey).toBeUndefined();
    expect(await q.getFlowBudget(node.job.id)).toBeNull();
  });
});

describe('TestJob.getChildrenValues', () => {
  it('is empty without children and omits children that are not completed', async () => {
    const q = open('cv-basic');
    q.pause();
    const lone = await q.add('lone', {});
    expect(await lone!.getChildrenValues()).toEqual({});
    const node = await new TestFlowProducer().add({
      name: 'p',
      queueName: 'cv-basic',
      data: {},
      children: [
        { name: 'c1', queueName: 'cv-basic', data: {} },
        { name: 'c2', queueName: 'cv-basic', data: {} },
      ],
    });
    expect(await node.job.getChildrenValues()).toEqual({});
    q.resume();
    const worker = work(q, async (job) => (job.name === 'c2' ? new Promise(() => {}) : 'first'));
    await waitFor(async () => (await node.children![0].job.getState()) === 'completed', 2000, 2);
    expect(await node.job.getChildrenValues()).toEqual({
      [`glide:{cv-basic}:${node.children![0].job.id}`]: 'first',
    });
    expect(Object.getPrototypeOf(await node.job.getChildrenValues())).toBeNull();
    await worker.close();
  });

  it('reads null for a child that completed without a result', async () => {
    const q = open('cv-null');
    const node = await new TestFlowProducer().add({
      name: 'p',
      queueName: 'cv-null',
      data: {},
      children: [{ name: 'c', queueName: 'cv-null', data: {} }],
    });
    work(q, async (job) => (job.name === 'p' ? job.getChildrenValues() : undefined));
    await waitFor(async () => (await node.job.getState()) === 'completed', 2000, 2);
    expect((await q.getJob(node.job.id))!.returnvalue).toEqual({
      [`glide:{cv-null}:${node.children![0].job.id}`]: null,
    });
  });

  it('skips a child removed after it completed and one whose queue is closed', async () => {
    const parentQ = open('cv-removed-parent');
    const childQ = open('cv-removed-child');
    const node = await new TestFlowProducer().add({
      name: 'p',
      queueName: 'cv-removed-parent',
      data: {},
      children: [
        { name: 'keep', queueName: 'cv-removed-parent', data: {} },
        { name: 'gone', queueName: 'cv-removed-child', data: {}, opts: { removeOnComplete: true } },
        { name: 'orphan', queueName: 'cv-removed-child', data: {} },
      ],
    });
    work(parentQ, async (job) => (job.name === 'keep' ? 'kept' : 'parent-ran'));
    work(childQ, async () => 'x');
    await waitFor(async () => (await node.job.getState()) === 'completed', 2000, 2);
    const keep = `glide:{cv-removed-parent}:${node.children![0].job.id}`;
    const orphan = `glide:{cv-removed-child}:${node.children![2].job.id}`;
    expect(await node.job.getChildrenValues()).toEqual({ [keep]: 'kept', [orphan]: 'x' });
    await childQ.close();
    expect(await node.job.getChildrenValues()).toEqual({ [keep]: 'kept' });
  });
});

describe('TestJob.getParents', () => {
  it('is empty for a job without a parent', async () => {
    const q = open('gp-none');
    expect(await (await q.add('x', {}))!.getParents()).toEqual([]);
  });

  it('returns the parent of a flow child and of a dynamic child', async () => {
    const q = open('gp-single');
    const other = open('gp-other');
    const node = await new TestFlowProducer().add({
      name: 'p',
      queueName: 'gp-single',
      data: {},
      children: [{ name: 'c', queueName: 'gp-single', data: {} }],
    });
    expect(await node.children![0].job.getParents()).toEqual([{ queue: 'gp-single', id: node.job.id }]);
    const dynamic = await other.add('d', {}, { parent: { queue: 'gp-single', id: node.job.id } });
    expect(dynamic!.parentId).toBe(node.job.id);
    expect(dynamic!.parentQueue).toBe('gp-single');
    expect(await dynamic!.getParents()).toEqual([{ queue: 'gp-single', id: node.job.id }]);
    expect(Object.keys(await node.job.getChildrenValues())).toEqual([]);
    expect(q.jobs.get(node.job.id)!.deps!.size).toBe(2);
  });

  it('returns every parent of a DAG job', async () => {
    open('gp-dag');
    const jobs = await new TestFlowProducer().addDAG({
      nodes: [
        { name: 'shared', queueName: 'gp-dag', data: {} },
        { name: 'left', queueName: 'gp-dag', data: {}, deps: ['shared'] },
        { name: 'right', queueName: 'gp-dag', data: {}, deps: ['shared'] },
      ],
    });
    const parents = await jobs.get('shared')!.getParents();
    expect(parents).toHaveLength(2);
    expect(parents).toEqual(
      expect.arrayContaining([
        { queue: 'gp-dag', id: jobs.get('left')!.id },
        { queue: 'gp-dag', id: jobs.get('right')!.id },
      ]),
    );
    expect(jobs.get('shared')!.parentId).toBe(jobs.get('left')!.id);
  });
});

describe('TestFlowProducer.addDAG', () => {
  it('runs a diamond in dependency order and hands each node its dependencies values', async () => {
    const q = open('dag-diamond');
    const jobs = await new TestFlowProducer().addDAG({
      nodes: [
        { name: 'join', queueName: 'dag-diamond', data: {}, deps: ['left', 'right'] },
        { name: 'left', queueName: 'dag-diamond', data: {}, deps: ['root'] },
        { name: 'right', queueName: 'dag-diamond', data: {}, deps: ['root'] },
        { name: 'root', queueName: 'dag-diamond', data: {} },
      ],
    });
    expect(await jobs.get('root')!.getState()).toBe('waiting');
    expect(await jobs.get('join')!.getState()).toBe('waiting-children');
    const order: string[] = [];
    work(q, async (job) => {
      order.push(job.name);
      return job.name === 'join' ? Object.values(await job.getChildrenValues()).sort() : job.name;
    });
    await waitFor(async () => (await jobs.get('join')!.getState()) === 'completed', 2000, 2);
    expect(order[0]).toBe('root');
    expect(order[3]).toBe('join');
    expect((await q.getJob(jobs.get('join')!.id))!.returnvalue).toEqual(['left', 'right']);
  });

  it('lists a parent once when a node repeats a dependency', async () => {
    open('dag-repeat');
    const jobs = await new TestFlowProducer().addDAG({
      nodes: [
        { name: 'leaf', queueName: 'dag-repeat', data: {} },
        { name: 'top', queueName: 'dag-repeat', data: {}, deps: ['leaf', 'leaf'] },
      ],
    });
    expect(await jobs.get('leaf')!.getParents()).toEqual([{ queue: 'dag-repeat', id: jobs.get('top')!.id }]);
  });

  it('rejects a cycle, an unknown dependency and a duplicate node', async () => {
    const q = open('dag-invalid');
    const producer = new TestFlowProducer();
    const node = (name: string, deps?: string[]) => ({ name, queueName: 'dag-invalid', data: {}, deps });
    await expect(producer.addDAG({ nodes: [node('a', ['b']), node('b', ['a'])] })).rejects.toThrow('cycle');
    await expect(producer.addDAG({ nodes: [node('a', ['zz'])] })).rejects.toThrow('unknown node');
    await expect(producer.addDAG({ nodes: [node('a'), node('a')] })).rejects.toThrow('Duplicate node');
    expect((await q.getJobCounts()).waiting).toBe(0);
  });
});

describe('dynamic children and moveToWaitingChildren', () => {
  it('parks the parent until children added with opts.parent complete, then re-runs it', async () => {
    const parentQ = open('dyn-parent');
    const childQ = open('dyn-child');
    const events: string[] = [];
    parentQ.on('waiting-children', (job: TestJob) => events.push(job.id));
    const runs: string[] = [];
    work(parentQ, async (job) => {
      runs.push(job.data.step ?? 'spawn');
      if ((job.data.step ?? 'spawn') === 'spawn') {
        await childQ.add('a', 1, { parent: { queue: 'dyn-parent', id: job.id } });
        await childQ.add('b', 2, { parent: { queue: 'dyn-parent', id: job.id } });
        await job.updateData({ ...job.data, step: 'collect' });
        await job.moveToWaitingChildren();
      }
      return Object.values(await job.getChildrenValues());
    });
    const parent = await parentQ.add('orchestrate', {});
    await waitFor(async () => (await parent!.getState()) === 'waiting-children', 2000, 2);
    expect(events).toEqual([parent!.id]);
    work(childQ, async (job) => job.data * 100);
    await waitFor(async () => (await parent!.getState()) === 'completed', 2000, 2);
    expect(runs).toEqual(['spawn', 'collect']);
    expect((await parentQ.getJob(parent!.id))!.returnvalue).toEqual([100, 200]);
    expect((await parentQ.getJob(parent!.id))!.attemptsMade).toBe(0);
  });

  it('goes straight back to waiting when every child already completed', async () => {
    const q = open('dyn-done');
    const runs: string[] = [];
    work(
      q,
      async (job) => {
        if (job.name === 'child') return 'child-result';
        runs.push(job.data.step ?? 'spawn');
        if (!job.data.step) {
          await q.add('child', {}, { parent: { queue: 'dyn-done', id: job.id } });
          await waitFor(async () => (await q.getJob('2'))!.returnvalue === 'child-result', 2000, 2);
          await job.updateData({ step: 'collect' });
          await job.moveToWaitingChildren();
        }
        return job.getChildrenValues();
      },
      2,
    );
    const parent = await q.add('p', {});
    await waitFor(async () => (await parent!.getState()) === 'completed', 2000, 2);
    expect(runs).toEqual(['spawn', 'collect']);
    expect((await q.getJob(parent!.id))!.returnvalue).toEqual({ 'glide:{dyn-done}:2': 'child-result' });
  });

  it('goes straight back to waiting when there are no children at all', async () => {
    const q = open('dyn-none');
    const runs: number[] = [];
    work(q, async (job) => {
      runs.push(runs.length);
      if (runs.length === 1) await job.moveToWaitingChildren();
      return 'ok';
    });
    const job = await q.add('p', {});
    await waitFor(async () => (await job!.getState()) === 'completed', 2000, 2);
    expect(runs).toHaveLength(2);
  });

  it.each([
    ['returns normally', async () => 'swallowed'],
    [
      'throws another error',
      async () => {
        throw new Error('other');
      },
    ],
  ])('still parks the job when the processor swallows the error and %s', async (_label, after) => {
    const q = open(`dyn-swallow-${_label.replace(/\W/g, '')}`);
    const spawned: string[] = [];
    work(
      q,
      async (job) => {
        if (job.name === 'child') return 'c';
        if (job.data.step === 'collect') return 'resumed';
        await q.add('child', {}, { parent: { queue: q.name, id: job.id } });
        spawned.push(job.id);
        await job.updateData({ step: 'collect' });
        await job.moveToWaitingChildren().catch(() => {});
        return after();
      },
      2,
    );
    const parent = await q.add('p', {});
    await waitFor(async () => (await parent!.getState()) === 'completed', 2000, 2);
    expect((await q.getJob(parent!.id))!.returnvalue).toBe('resumed');
    expect((await q.getJob(parent!.id))!.attemptsMade).toBe(0);
    expect(spawned).toEqual([parent!.id]);
  });

  it('parks a job whose processor throws WaitingChildrenError itself', async () => {
    const q = open('dyn-manual');
    const runs: number[] = [];
    work(q, async () => {
      runs.push(runs.length);
      if (runs.length === 1) throw new WaitingChildrenError();
      return 'ok';
    });
    const job = await q.add('p', {});
    await waitFor(async () => (await job!.getState()) === 'completed', 2000, 2);
    expect(runs).toHaveLength(2);
  });

  it('rejects moveToWaitingChildren outside an active worker', async () => {
    const q = open('dyn-outside');
    q.pause();
    const job = await q.add('p', {});
    await expect(job!.moveToWaitingChildren()).rejects.toThrow('can only be used while the job is active in a Worker');
  });

  it('counts a completion in a parent that is not waiting without releasing it', async () => {
    const parentQ = open('dyn-active-parent');
    const childQ = open('dyn-active-child');
    parentQ.pause();
    const parent = await parentQ.add('p', {});
    const child = await childQ.add('c', {}, { parent: { queue: 'dyn-active-parent', id: parent!.id } });
    work(childQ, async () => 'x');
    await waitFor(async () => (await child!.getState()) === 'completed', 2000, 2);
    expect(await parent!.getState()).toBe('waiting');
    expect(parentQ.jobs.get(parent!.id)!.depsDone!.size).toBe(1);
  });

  it('keeps waiting while a child is pending', async () => {
    const q = open('dyn-pending');
    const gate: { open: () => void } = { open: () => {} };
    const blocked = new Promise<void>((resolve) => (gate.open = resolve));
    work(q, async (job) => {
      if (job.name === 'fast') return 'fast';
      if (job.name === 'slow') {
        await blocked;
        return 'slow';
      }
      if (!job.data.step) {
        await q.add('fast', {}, { parent: { queue: 'dyn-pending', id: job.id } });
        await q.add('slow', {}, { parent: { queue: 'dyn-pending', id: job.id } });
        await job.updateData({ step: 'collect' });
        await job.moveToWaitingChildren();
      }
      return 'done';
    });
    const parent = await q.add('p', {});
    await waitFor(async () => (await q.getJob('2'))?.returnvalue === 'fast', 2000, 2);
    expect(await parent!.getState()).toBe('waiting-children');
    gate.open();
    await waitFor(async () => (await parent!.getState()) === 'completed', 2000, 2);
  });

  it('ignores a parent that does not exist', async () => {
    const q = open('dyn-ghost');
    const noQueue = await q.add('a', {}, { parent: { queue: 'dyn-nowhere', id: '1' } });
    const noJob = await q.add('b', {}, { parent: { queue: 'dyn-ghost', id: '404' } });
    expect(noQueue!.parentId).toBe('1');
    expect(noJob!.parentId).toBe('404');
    work(q, async () => 'ok');
    await waitFor(
      async () => (await noJob!.getState()) === 'completed' && (await noQueue!.getState()) === 'completed',
      2000,
      2,
    );
  });

  it('does not count a child that completes before its parent job exists', async () => {
    const q = open('dyn-late');
    const child = await q.add('c', {}, { parent: { queue: 'dyn-late', id: 'late' } });
    q.pause();
    const parent = await q.add('p', {}, { jobId: 'late' });
    work(q, async () => 'x');
    q.resume();
    await waitFor(async () => (await child!.getState()) === 'completed', 2000, 2);
    expect(q.jobs.get(parent!.id)!.deps).toBeUndefined();
  });

  it('counts getFlowUsage for the children of a flow', async () => {
    const q = open('dyn-usage');
    const node = await new TestFlowProducer().add({
      name: 'p',
      queueName: 'dyn-usage',
      data: {},
      children: [{ name: 'c', queueName: 'dyn-usage', data: {} }],
    });
    work(q, async (job) => {
      if (job.name === 'c') await job.reportUsage({ totalTokens: 12, tokens: { input: 12 } });
      return 'ok';
    });
    await waitFor(async () => (await node.job.getState()) === 'completed', 2000, 2);
    expect((await q.getFlowUsage(node.job.id)).totalTokens).toBe(12);
    const lone = await q.add('lone', {});
    expect((await q.getFlowUsage(lone!.id)).jobCount).toBe(0);
  });

  it('reads getFlowUsage from the deps: children in other queues count, grandchildren do not', async () => {
    const rootQ = open('usage-root');
    const childQ = open('usage-child');
    const node = await new TestFlowProducer().add({
      name: 'p',
      queueName: 'usage-root',
      data: {},
      children: [
        {
          name: 'direct',
          queueName: 'usage-child',
          data: {},
          children: [{ name: 'grand', queueName: 'usage-child', data: {} }],
        },
        { name: 'gone', queueName: 'usage-child', data: {}, opts: { removeOnComplete: true } },
      ],
    });
    const spend = (tokens: number) => async (job: TestJob) => {
      await job.reportUsage({ totalTokens: tokens, tokens: { input: tokens } });
      return 'ok';
    };
    work(rootQ, spend(1));
    work(childQ, async (job) => spend(job.name === 'grand' ? 100 : 10)(job));
    await waitFor(async () => (await node.job.getState()) === 'completed', 2000, 2);
    const usage = await rootQ.getFlowUsage(node.job.id);
    expect(usage.totalTokens).toBe(1 + 10);
    expect(usage.jobCount).toBe(2);
    await childQ.close();
    expect((await rootQ.getFlowUsage(node.job.id)).totalTokens).toBe(1);
  });
});

describe('parents released by other completion paths', () => {
  it('releases the parent when a batch worker completes its children', async () => {
    const q = open('batch-release');
    open('batch-release-parent');
    q.pause();
    const node = await new TestFlowProducer().add({
      name: 'p',
      queueName: 'batch-release-parent',
      data: {},
      children: [
        { name: 'c1', queueName: 'batch-release', data: {} },
        { name: 'c2', queueName: 'batch-release', data: {} },
      ],
    });
    const worker = new TestWorker(q, async (jobs: TestJob[]) => jobs.map((j) => j.name), { batch: { size: 2 } } as any);
    workers.push(worker);
    q.resume();
    await waitFor(async () => (await q.getJobCounts()).completed >= 2, 2000, 2);
    expect(await node.job.getState()).toBe('waiting');
  });

  it('releases the parent when a BatchError carries a result for each child', async () => {
    const q = open('batch-partial');
    q.pause();
    const node = await new TestFlowProducer().add({
      name: 'p',
      queueName: 'batch-partial',
      data: {},
      children: [
        { name: 'c1', queueName: 'batch-partial', data: {} },
        { name: 'c2', queueName: 'batch-partial', data: {} },
      ],
    });
    const worker = new TestWorker(
      q,
      async (jobs: TestJob[]) => {
        throw new BatchError(jobs.map((j) => (j.name === 'c1' ? 'fine' : new Error('boom'))));
      },
      { batch: { size: 2 } } as any,
    );
    workers.push(worker);
    q.resume();
    await waitFor(async () => (await q.getJobCounts()).completed === 1, 2000, 2);
    expect(await node.job.getState()).toBe('waiting-children');
  });
});
