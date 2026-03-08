/**
 * moveToWaitingChildren integration tests.
 * Tests dynamic child job spawning and fork-join pattern.
 * Runs against both standalone (:6379) and cluster (:7000).
 */
import { it, expect, beforeAll, afterAll } from 'vitest';
import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { FlowProducer } = require('../dist/flow-producer') as typeof import('../src/flow-producer');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
const { WaitingChildrenError } = require('../dist/errors') as typeof import('../src/errors');

describeEachMode('moveToWaitingChildren', (CONNECTION) => {
  let cleanupClient: any;
  const Q = 'test-wtc-' + Date.now();

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('parent spawns dynamic children, waits, then resumes after all complete', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const parentCalls = new Map<string, number>();

    const worker = new Worker(
      Q,
      async (job: any) => {
        if (job.name === 'parent') {
          const count = (parentCalls.get(job.id) ?? 0) + 1;
          parentCalls.set(job.id, count);
          if (count === 1) {
            await queue.add('child', { idx: 1 }, { parent: { queue: Q, id: job.id } });
            await queue.add('child', { idx: 2 }, { parent: { queue: Q, id: job.id } });
            await job.moveToWaitingChildren();
          }
          const childValues = await job.getChildrenValues();
          return { childCount: Object.keys(childValues).length };
        }
        return { processed: job.data.idx };
      },
      { connection: CONNECTION },
    );
    await worker.waitUntilReady();

    const parentJob = await queue.add('parent', { type: 'batch' });

    const completed = await new Promise<any>((resolve) => {
      worker.on('completed', (job: any, result: any) => {
        if (job.name === 'parent') resolve(result);
      });
    });

    expect(completed.childCount).toBe(2);
    expect(parentCalls.get(parentJob!.id)).toBe(2);

    await worker.close();
    await queue.close();
  }, 30000);

  it('parent resumes only after ALL children complete', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const parentCalls = new Map<string, number>();
    const childCompletionOrder: number[] = [];

    const worker = new Worker(
      Q,
      async (job: any) => {
        if (job.name === 'parent-multi') {
          const count = (parentCalls.get(job.id) ?? 0) + 1;
          parentCalls.set(job.id, count);
          if (count === 1) {
            await queue.add('child-multi', { idx: 1 }, { parent: { queue: Q, id: job.id } });
            await queue.add('child-multi', { idx: 2 }, { parent: { queue: Q, id: job.id } });
            await queue.add('child-multi', { idx: 3 }, { parent: { queue: Q, id: job.id } });
            await job.moveToWaitingChildren();
          }
          return { done: true };
        }
        childCompletionOrder.push(job.data.idx);
        return { idx: job.data.idx };
      },
      { connection: CONNECTION },
    );
    await worker.waitUntilReady();

    await queue.add('parent-multi', { type: 'multi' });

    const result = await new Promise<any>((resolve) => {
      worker.on('completed', (job: any, res: any) => {
        if (job.name === 'parent-multi') resolve(res);
      });
    });

    expect(result.done).toBe(true);
    expect(childCompletionOrder).toHaveLength(3);

    await worker.close();
    await queue.close();
  }, 30000);

  it('handles race: children complete before moveToWaitingChildren is called', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const parentCalls = new Map<string, number>();

    const worker = new Worker(
      Q,
      async (job: any) => {
        if (job.name === 'race-parent') {
          const count = (parentCalls.get(job.id) ?? 0) + 1;
          parentCalls.set(job.id, count);
          if (count === 1) {
            await queue.add('fast-child', { idx: 1 }, { parent: { queue: Q, id: job.id } });
            // Wait for child to be processed
            await new Promise<void>((r) => setTimeout(r, 1000));
            await job.moveToWaitingChildren();
          }
          return { raceHandled: true };
        }
        // fast-child
        return { fast: true };
      },
      { connection: CONNECTION },
    );
    await worker.waitUntilReady();

    await queue.add('race-parent', {});

    const result = await new Promise<any>((resolve) => {
      worker.on('completed', (job: any, res: any) => {
        if (job.name === 'race-parent') resolve(res);
      });
    });

    expect(result.raceHandled).toBe(true);

    await worker.close();
    await queue.close();
  }, 30000);

  it('getChildrenValues returns dynamic children return values', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const parentCalls = new Map<string, number>();

    const worker = new Worker(
      Q,
      async (job: any) => {
        if (job.name === 'gcv-parent') {
          const count = (parentCalls.get(job.id) ?? 0) + 1;
          parentCalls.set(job.id, count);
          if (count === 1) {
            await queue.add('gcv-child', { val: 'a' }, { parent: { queue: Q, id: job.id } });
            await queue.add('gcv-child', { val: 'b' }, { parent: { queue: Q, id: job.id } });
            await job.moveToWaitingChildren();
          }
          const childValues = await job.getChildrenValues();
          return { children: childValues };
        }
        return { value: job.data.val };
      },
      { connection: CONNECTION },
    );
    await worker.waitUntilReady();

    await queue.add('gcv-parent', {});

    const result = await new Promise<any>((resolve) => {
      worker.on('completed', (job: any, res: any) => {
        if (job.name === 'gcv-parent') resolve(res);
      });
    });

    const values = Object.values(result.children) as any[];
    expect(values).toHaveLength(2);
    const childVals = values.map((v: any) => v.value).sort();
    expect(childVals).toEqual(['a', 'b']);

    await worker.close();
    await queue.close();
  }, 30000);

  it('throws when called outside a worker', async () => {
    const { Job } = require('../dist/job') as typeof import('../src/job');
    const k = buildKeys(Q);
    const client = await createCleanupClient(CONNECTION);

    const job = new Job(client, k, '999', 'test', {}, {});

    await expect(job.moveToWaitingChildren()).rejects.toThrow(
      'moveToWaitingChildren() can only be used while the job is active in a Worker',
    );

    client.close();
  });

  it('WaitingChildrenError is exported and catchable', () => {
    expect(WaitingChildrenError).toBeDefined();
    const err = new WaitingChildrenError();
    expect(err.name).toBe('WaitingChildrenError');
    expect(err.message).toBe('Job moved to waiting-children state');
    expect(err instanceof Error).toBe(true);
  });

  it('re-entrant: parent processes twice with step pattern', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const parentCalls = new Map<string, number>();
    const steps: string[] = [];

    const worker = new Worker(
      Q,
      async (job: any) => {
        if (job.name === 'step-parent') {
          const count = (parentCalls.get(job.id) ?? 0) + 1;
          parentCalls.set(job.id, count);
          if (count === 1) {
            steps.push('step1');
            await queue.add('step-child', { n: 1 }, { parent: { queue: Q, id: job.id } });
            await job.moveToWaitingChildren();
          }
          steps.push('step2');
          return { steps: [...steps] };
        }
        return { childDone: true };
      },
      { connection: CONNECTION },
    );
    await worker.waitUntilReady();

    await queue.add('step-parent', { step: 'init' });

    const result = await new Promise<any>((resolve) => {
      worker.on('completed', (job: any, res: any) => {
        if (job.name === 'step-parent') resolve(res);
      });
    });

    expect(result.steps).toContain('step1');
    expect(result.steps).toContain('step2');

    await worker.close();
    await queue.close();
  }, 30000);

  it('mixed: static flow children + dynamic children', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const flow = new FlowProducer({ connection: CONNECTION });
    const parentCalls = new Map<string, number>();

    const worker = new Worker(
      Q,
      async (job: any) => {
        if (job.name === 'mixed-parent') {
          const count = (parentCalls.get(job.id) ?? 0) + 1;
          parentCalls.set(job.id, count);
          if (count === 1) {
            await queue.add('dynamic-child', { dyn: true }, { parent: { queue: Q, id: job.id } });
            await job.moveToWaitingChildren();
          }
          const children = await job.getChildrenValues();
          return { totalChildren: Object.keys(children).length };
        }
        return { childType: job.name };
      },
      { connection: CONNECTION },
    );
    await worker.waitUntilReady();

    await flow.add({
      name: 'mixed-parent',
      queueName: Q,
      data: { type: 'mixed' },
      children: [{ name: 'static-child', queueName: Q, data: { static: true } }],
    });

    const result = await new Promise<any>((resolve) => {
      worker.on('completed', (job: any, res: any) => {
        if (job.name === 'mixed-parent') resolve(res);
      });
    });

    expect(result.totalChildren).toBe(2);

    await worker.close();
    await flow.close();
    await queue.close();
  }, 30000);
});
