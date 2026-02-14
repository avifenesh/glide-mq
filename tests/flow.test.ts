/**
 * Flow (parent-child job trees) integration tests.
 * Requires: valkey-server running on localhost:6379
 *
 * Run: npx vitest run tests/flow.test.ts
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';

const { GlideClient } = require('speedkey') as typeof import('speedkey');
const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { FlowProducer } = require('../dist/flow-producer') as typeof import('../src/flow-producer');
const { buildKeys, keyPrefix } = require('../dist/utils') as typeof import('../src/utils');
const { LIBRARY_SOURCE, CONSUMER_GROUP } = require('../dist/functions/index') as typeof import('../src/functions/index');
const { ensureFunctionLibrary } = require('../dist/connection') as typeof import('../src/connection');

const CONNECTION = {
  addresses: [{ host: 'localhost', port: 6379 }],
};

let cleanupClient: InstanceType<typeof GlideClient>;

async function flushQueue(queueName: string) {
  const k = buildKeys(queueName);
  const keysToDelete = [
    k.id, k.stream, k.scheduled, k.completed, k.failed,
    k.events, k.meta, k.dedup, k.rate, k.schedulers,
  ];
  for (const key of keysToDelete) {
    try { await cleanupClient.del([key]); } catch {}
  }
  // Clean job hashes + deps
  const prefix = `glide:{${queueName}}:`;
  let cursor = '0';
  do {
    const result = await cleanupClient.scan(cursor, { match: `${prefix}*`, count: 100 });
    cursor = result[0] as string;
    const keys = result[1] as string[];
    if (keys.length > 0) {
      await cleanupClient.del(keys);
    }
  } while (cursor !== '0');
}

beforeAll(async () => {
  cleanupClient = await GlideClient.createClient({
    addresses: [{ host: 'localhost', port: 6379 }],
  });
  // Force reload the library to ensure new functions are available
  await (cleanupClient as any).functionLoad(LIBRARY_SOURCE, { replace: true });
});

afterAll(async () => {
  cleanupClient.close();
});

describe('FlowProducer', () => {
  const Q = 'test-flow-' + Date.now();

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('creates parent in waiting-children state with children in waiting state', async () => {
    const flow = new FlowProducer({ connection: CONNECTION });

    const node = await flow.add({
      name: 'parent-job',
      queueName: Q,
      data: { type: 'parent' },
      children: [
        { name: 'child-1', queueName: Q, data: { idx: 1 } },
        { name: 'child-2', queueName: Q, data: { idx: 2 } },
      ],
    });

    expect(node.job.id).toBeTruthy();
    expect(node.children).toHaveLength(2);
    expect(node.children![0].job.name).toBe('child-1');
    expect(node.children![1].job.name).toBe('child-2');

    // Verify parent state
    const k = buildKeys(Q);
    const parentState = await cleanupClient.hget(k.job(node.job.id), 'state');
    expect(String(parentState)).toBe('waiting-children');

    // Verify children have parentId set
    for (const childNode of node.children!) {
      const cParentId = await cleanupClient.hget(k.job(childNode.job.id), 'parentId');
      expect(String(cParentId)).toBe(node.job.id);
    }

    // Verify deps set contains both children
    const deps = await cleanupClient.smembers(k.deps(node.job.id));
    expect(deps.size).toBe(2);

    await flow.close();
  });

  it('parent completes after all children are processed', async () => {
    const qName = Q + '-complete';
    const flow = new FlowProducer({ connection: CONNECTION });

    const node = await flow.add({
      name: 'parent',
      queueName: qName,
      data: { type: 'parent' },
      children: [
        { name: 'child-a', queueName: qName, data: { v: 'a' } },
        { name: 'child-b', queueName: qName, data: { v: 'b' } },
      ],
    });

    const parentId = node.job.id;
    const completedJobs: string[] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);
      const worker = new Worker(
        qName,
        async (job: any) => {
          completedJobs.push(job.name);
          return { result: job.data.v || 'parent-done' };
        },
        { connection: CONNECTION, concurrency: 2, blockTimeout: 1000 },
      );

      worker.on('completed', (job: any) => {
        if (job.id === parentId) {
          clearTimeout(timeout);
          setTimeout(() => worker.close(true).then(resolve), 200);
        }
      });
      worker.on('error', () => {});
    });

    await done;

    // Parent should be completed
    const k = buildKeys(qName);
    const parentState = await cleanupClient.hget(k.job(parentId), 'state');
    expect(String(parentState)).toBe('completed');

    // Both children and parent should be completed
    expect(completedJobs).toContain('child-a');
    expect(completedJobs).toContain('child-b');
    expect(completedJobs).toContain('parent');

    await flow.close();
    await flushQueue(qName);
  }, 20000);

  it('nested flow (grandchild)', async () => {
    const qName = Q + '-nested';
    const flow = new FlowProducer({ connection: CONNECTION });

    const node = await flow.add({
      name: 'grandparent',
      queueName: qName,
      data: { level: 0 },
      children: [
        {
          name: 'parent-child',
          queueName: qName,
          data: { level: 1 },
          children: [
            { name: 'grandchild', queueName: qName, data: { level: 2 } },
          ],
        },
      ],
    });

    expect(node.job.name).toBe('grandparent');
    expect(node.children).toHaveLength(1);
    expect(node.children![0].job.name).toBe('parent-child');
    expect(node.children![0].children).toHaveLength(1);
    expect(node.children![0].children![0].job.name).toBe('grandchild');

    const completedNames: string[] = [];
    const grandparentId = node.job.id;

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 20000);
      const worker = new Worker(
        qName,
        async (job: any) => {
          completedNames.push(job.name);
          return { level: job.data.level };
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
      );

      worker.on('completed', (job: any) => {
        if (job.id === grandparentId) {
          clearTimeout(timeout);
          setTimeout(() => worker.close(true).then(resolve), 200);
        }
      });
      worker.on('error', () => {});
    });

    await done;

    // Grandchild should complete first, then parent-child, then grandparent
    expect(completedNames.indexOf('grandchild')).toBeLessThan(completedNames.indexOf('parent-child'));
    expect(completedNames.indexOf('parent-child')).toBeLessThan(completedNames.indexOf('grandparent'));

    await flow.close();
    await flushQueue(qName);
  }, 25000);

  it('parent getChildrenValues returns all child results', async () => {
    const qName = Q + '-values';
    const flow = new FlowProducer({ connection: CONNECTION });

    const node = await flow.add({
      name: 'parent',
      queueName: qName,
      data: {},
      children: [
        { name: 'c1', queueName: qName, data: { x: 10 } },
        { name: 'c2', queueName: qName, data: { x: 20 } },
      ],
    });

    const parentId = node.job.id;
    let parentJobRef: any = null;

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);
      const worker = new Worker(
        qName,
        async (job: any) => {
          if (job.name === 'parent') {
            parentJobRef = job;
            const childValues = await job.getChildrenValues();
            return { childValues };
          }
          return { doubled: job.data.x * 2 };
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
      );

      worker.on('completed', (job: any) => {
        if (job.id === parentId) {
          clearTimeout(timeout);
          setTimeout(() => worker.close(true).then(resolve), 200);
        }
      });
      worker.on('error', () => {});
    });

    await done;

    // Verify parent has completed with child values
    const k = buildKeys(qName);
    const returnval = await cleanupClient.hget(k.job(parentId), 'returnvalue');
    const parsed = JSON.parse(String(returnval));
    // childValues is a Record<depsMember, result>
    const values = Object.values(parsed.childValues) as any[];
    expect(values).toHaveLength(2);
    const doubled = values.map((v: any) => v.doubled).sort((a: number, b: number) => a - b);
    expect(doubled).toEqual([20, 40]);

    await flow.close();
    await flushQueue(qName);
  }, 20000);

  it('addBulk creates multiple flows', async () => {
    const qName = Q + '-bulk';
    const flow = new FlowProducer({ connection: CONNECTION });

    const nodes = await flow.addBulk([
      {
        name: 'flow-1',
        queueName: qName,
        data: { f: 1 },
        children: [
          { name: 'f1-child', queueName: qName, data: { c: 1 } },
        ],
      },
      {
        name: 'flow-2',
        queueName: qName,
        data: { f: 2 },
        children: [
          { name: 'f2-child', queueName: qName, data: { c: 2 } },
        ],
      },
    ]);

    expect(nodes).toHaveLength(2);
    expect(nodes[0].job.name).toBe('flow-1');
    expect(nodes[1].job.name).toBe('flow-2');
    expect(nodes[0].children).toHaveLength(1);
    expect(nodes[1].children).toHaveLength(1);

    await flow.close();
    await flushQueue(qName);
  });
});
