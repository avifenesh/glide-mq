/**
 * chain, group, chord and dag from glide-mq/testing. No Valkey required.
 *
 * Run: npx vitest run tests/testing-workflows.test.ts
 */
import { describe, it, expect, afterEach } from 'vitest';
import { TestQueue, TestWorker, chain, group, chord, dag } from '../src/testing';
import type { TestJob } from '../src/testing';
import { waitFor } from './helpers/fixture';

let queues: TestQueue[] = [];
let workers: TestWorker[] = [];

function open(name: string): TestQueue {
  const queue = new TestQueue(name);
  queues.push(queue);
  return queue;
}

function work(queue: TestQueue, processor: (job: TestJob) => Promise<any>): TestWorker {
  const worker = new TestWorker(queue, processor as any);
  workers.push(worker);
  return worker;
}

afterEach(async () => {
  for (const worker of workers) await worker.close();
  for (const queue of queues) await queue.close();
  workers = [];
  queues = [];
});

describe('chain', () => {
  it('rejects an empty chain', async () => {
    await expect(chain('wf-chain-empty', [])).rejects.toThrow('chain() requires at least one job');
  });

  it('returns a single job as the root', async () => {
    const q = open('wf-chain-one');
    const tree = await chain('wf-chain-one', [{ name: 'only', data: { n: 1 } }]);
    expect(tree.children).toBeUndefined();
    expect((await q.getJob(tree.job.id))!.name).toBe('only');
    expect(await tree.job.getState()).toBe('waiting');
  });

  it('runs the last job first and the first job last', async () => {
    const q = open('wf-chain');
    const tree = await chain('wf-chain', [
      { name: 'step-1', data: {} },
      { name: 'step-2', data: {}, opts: { priority: 0 } },
      { name: 'step-3', data: {} },
    ]);
    expect(tree.job.id).toBeDefined();
    expect(tree.children![0].job.parentId).toBe(tree.job.id);
    expect(tree.children![0].children![0].children).toBeUndefined();
    expect(await tree.job.getState()).toBe('waiting-children');
    const order: string[] = [];
    work(q, async (job) => {
      order.push(job.name);
      return job.name;
    });
    await waitFor(async () => (await tree.job.getState()) === 'completed', 2000, 2);
    expect(order).toEqual(['step-3', 'step-2', 'step-1']);
    await tree.close();
  });

  it('accepts a production connection argument and a prefix', async () => {
    const q = open('wf-chain-conn');
    const tree = await chain(
      'wf-chain-conn',
      [
        { name: 'a', data: {} },
        { name: 'b', data: {} },
      ],
      { addresses: [] } as any,
      'p',
    );
    work(q, async (job) => (job.name === 'a' ? job.getChildrenValues() : 'b-result'));
    await waitFor(async () => (await tree.job.getState()) === 'completed', 2000, 2);
    expect((await q.getJob(tree.job.id))!.returnvalue).toEqual({
      [`p:{wf-chain-conn}:${tree.children![0].job.id}`]: 'b-result',
    });
  });
});

describe('group', () => {
  it('rejects an empty group', async () => {
    await expect(group('wf-group-empty', [])).rejects.toThrow('group() requires at least one job');
  });

  it('runs members in parallel under a __group__ parent that reads their results', async () => {
    const q = open('wf-group');
    const tree = await group('wf-group', [
      { name: 'a', data: 1 },
      { name: 'b', data: 2 },
    ]);
    expect((await q.getJob(tree.job.id))!.name).toBe('__group__');
    expect(tree.children).toHaveLength(2);
    work(q, async (job) => (job.name === '__group__' ? Object.values(await job.getChildrenValues()) : job.data * 2));
    await waitFor(async () => (await tree.job.getState()) === 'completed', 2000, 2);
    expect((await q.getJob(tree.job.id))!.returnvalue).toEqual([2, 4]);
  });
});

describe('chord', () => {
  it('rejects an empty group', async () => {
    await expect(chord('wf-chord-empty', [], { name: 'cb', data: {} })).rejects.toThrow(
      'chord() requires at least one group job',
    );
  });

  it('runs the callback with the group results', async () => {
    const q = open('wf-chord');
    const tree = await chord(
      'wf-chord',
      [
        { name: 'a', data: 1 },
        { name: 'b', data: 2, opts: { attempts: 2 } },
      ],
      { name: 'callback', data: { tag: 'done' }, opts: { jobId: 'the-callback' } },
    );
    expect(tree.job.id).toBe('the-callback');
    expect(await tree.job.getState()).toBe('waiting-children');
    work(q, async (job) => (job.name === 'callback' ? Object.values(await job.getChildrenValues()) : job.data + 10));
    await waitFor(async () => (await tree.job.getState()) === 'completed', 2000, 2);
    expect((await q.getJob('the-callback'))!.returnvalue).toEqual([11, 12]);
  });
});

describe('dag', () => {
  it('rejects an empty graph', async () => {
    await expect(dag([])).rejects.toThrow('dag() requires at least one node');
  });

  it('returns the jobs by node name and runs dependencies first', async () => {
    const q = open('wf-dag');
    const jobs = await dag([
      { name: 'report', queueName: 'wf-dag', data: {}, deps: ['fetch', 'parse'] },
      { name: 'fetch', queueName: 'wf-dag', data: {} },
      { name: 'parse', queueName: 'wf-dag', data: {}, deps: ['fetch'] },
    ]);
    expect([...jobs.keys()].sort()).toEqual(['fetch', 'parse', 'report']);
    expect(await jobs.get('report')!.getState()).toBe('waiting-children');
    expect(await jobs.get('fetch')!.getParents()).toHaveLength(2);
    const order: string[] = [];
    work(q, async (job) => {
      order.push(job.name);
      return job.name === 'report' ? Object.keys(await job.getChildrenValues()).length : job.name;
    });
    await waitFor(async () => (await jobs.get('report')!.getState()) === 'completed', 2000, 2);
    expect(order).toEqual(['fetch', 'parse', 'report']);
    expect((await q.getJob(jobs.get('report')!.id))!.returnvalue).toBe(2);
    await jobs.close();
  });

  it('rejects a cycle', async () => {
    open('wf-dag-cycle');
    await expect(
      dag(
        [
          { name: 'a', queueName: 'wf-dag-cycle', data: {}, deps: ['b'] },
          { name: 'b', queueName: 'wf-dag-cycle', data: {}, deps: ['a'] },
        ],
        undefined,
        'p',
      ),
    ).rejects.toThrow('cycle');
  });
});
