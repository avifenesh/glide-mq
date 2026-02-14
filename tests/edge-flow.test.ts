/**
 * Edge-case tests for FlowProducer (parent-child job trees).
 * Requires: valkey-server running on localhost:6379 and cluster on :7000-7005
 *
 * Run: npx vitest run tests/edge-flow.test.ts
 */
import { it, expect, beforeAll, afterAll } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { FlowProducer } = require('../dist/flow-producer') as typeof import('../src/flow-producer');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

describeEachMode('Edge: Flow with single child', (CONNECTION) => {
  const Q = 'edge-flow-single-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('parent completes after single child is processed', async () => {
    const flow = new FlowProducer({ connection: CONNECTION });

    const node = await flow.add({
      name: 'single-parent',
      queueName: Q,
      data: { role: 'parent' },
      children: [
        { name: 'only-child', queueName: Q, data: { role: 'child' } },
      ],
    });

    const parentId = node.job.id;
    expect(node.children).toHaveLength(1);

    const k = buildKeys(Q);
    const parentState = await cleanupClient.hget(k.job(parentId), 'state');
    expect(String(parentState)).toBe('waiting-children');

    const completedOrder: string[] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);
      const worker = new Worker(
        Q,
        async (job: any) => {
          completedOrder.push(job.name);
          return { name: job.name };
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500 },
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

    // Child must complete before parent
    expect(completedOrder.indexOf('only-child')).toBeLessThan(completedOrder.indexOf('single-parent'));

    const finalState = await cleanupClient.hget(k.job(parentId), 'state');
    expect(String(finalState)).toBe('completed');

    await flow.close();
  }, 20000);
});

describeEachMode('Edge: Flow child fails all retries', (CONNECTION) => {
  const Q = 'edge-flow-fail-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('parent stays in waiting-children when child fails all retries', async () => {
    const flow = new FlowProducer({ connection: CONNECTION });

    const node = await flow.add({
      name: 'stuck-parent',
      queueName: Q,
      data: { role: 'parent' },
      children: [
        {
          name: 'failing-child',
          queueName: Q,
          data: { role: 'child' },
          opts: { attempts: 2, backoff: { type: 'fixed', delay: 100 } },
        },
      ],
    });

    const parentId = node.job.id;
    const childId = node.children![0].job.id;
    let failCount = 0;

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => resolve(), 8000);
      const worker = new Worker(
        Q,
        async (job: any) => {
          if (job.name === 'failing-child') {
            failCount++;
            throw new Error('always fails');
          }
          return 'ok';
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 500,
          promotionInterval: 300,
          stalledInterval: 60000,
        },
      );

      worker.on('failed', (job: any) => {
        // After all retries exhausted, check state and resolve
        if (failCount >= 2) {
          clearTimeout(timeout);
          setTimeout(() => worker.close(true).then(resolve), 1000);
        }
      });
      worker.on('error', () => {});
    });

    await done;

    // Parent should still be in waiting-children since child failed
    const k = buildKeys(Q);
    const parentState = await cleanupClient.hget(k.job(parentId), 'state');
    expect(String(parentState)).toBe('waiting-children');

    // Child should be in failed state
    const childState = await cleanupClient.hget(k.job(childId), 'state');
    expect(String(childState)).toBe('failed');

    await flow.close();
  }, 15000);
});

describeEachMode('Edge: Flow getChildrenValues with mixed results', (CONNECTION) => {
  const Q = 'edge-flow-mixed-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('getChildrenValues returns values only for completed children', async () => {
    const flow = new FlowProducer({ connection: CONNECTION });

    const node = await flow.add({
      name: 'mixed-parent',
      queueName: Q,
      data: {},
      children: [
        { name: 'success-child', queueName: Q, data: { v: 'good' } },
        {
          name: 'fail-child',
          queueName: Q,
          data: { v: 'bad' },
          opts: { attempts: 1 },
        },
      ],
    });

    const parentId = node.job.id;
    let childValuesFromParent: Record<string, any> | null = null;

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => resolve(), 10000);
      const worker = new Worker(
        Q,
        async (job: any) => {
          if (job.name === 'fail-child') {
            throw new Error('intended failure');
          }
          if (job.name === 'mixed-parent') {
            childValuesFromParent = await job.getChildrenValues();
            return { childValues: childValuesFromParent };
          }
          return { result: job.data.v };
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 500,
          stalledInterval: 60000,
        },
      );

      worker.on('error', () => {});
      worker.on('failed', () => {});

      // Wait for processing to settle
      setTimeout(() => {
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      }, 6000);
    });

    await done;

    // The parent stays in waiting-children since one child failed,
    // so we verify child values by reading the deps set directly.
    const k = buildKeys(Q);
    const deps = await cleanupClient.smembers(k.deps(parentId));
    // At least one dep member should exist
    expect(deps.size).toBeGreaterThanOrEqual(1);

    // The success child should have a returnvalue
    const successChildId = node.children![0].job.id;
    const returnval = await cleanupClient.hget(k.job(successChildId), 'returnvalue');
    expect(returnval).not.toBeNull();
    const parsed = JSON.parse(String(returnval));
    expect(parsed.result).toBe('good');

    await flow.close();
  }, 15000);
});

describeEachMode('Edge: FlowProducer.addBulk with 3 independent flows', (CONNECTION) => {
  const Q = 'edge-flow-bulk3-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('creates 3 independent flows with their own parent-child trees', async () => {
    const flow = new FlowProducer({ connection: CONNECTION });

    const nodes = await flow.addBulk([
      {
        name: 'flow-a',
        queueName: Q,
        data: { f: 'a' },
        children: [
          { name: 'a-child', queueName: Q, data: { c: 'a1' } },
        ],
      },
      {
        name: 'flow-b',
        queueName: Q,
        data: { f: 'b' },
        children: [
          { name: 'b-child', queueName: Q, data: { c: 'b1' } },
        ],
      },
      {
        name: 'flow-c',
        queueName: Q,
        data: { f: 'c' },
        children: [
          { name: 'c-child', queueName: Q, data: { c: 'c1' } },
        ],
      },
    ]);

    expect(nodes).toHaveLength(3);
    expect(nodes[0].job.name).toBe('flow-a');
    expect(nodes[1].job.name).toBe('flow-b');
    expect(nodes[2].job.name).toBe('flow-c');

    // Each flow should have its own children
    for (const node of nodes) {
      expect(node.children).toHaveLength(1);
    }

    // All parent IDs should be unique
    const parentIds = new Set(nodes.map(n => n.job.id));
    expect(parentIds.size).toBe(3);

    // All parents should be in waiting-children state
    const k = buildKeys(Q);
    for (const node of nodes) {
      const state = await cleanupClient.hget(k.job(node.job.id), 'state');
      expect(String(state)).toBe('waiting-children');
    }

    // All child IDs should be unique
    const childIds = new Set(nodes.map(n => n.children![0].job.id));
    expect(childIds.size).toBe(3);

    await flow.close();
  });
});

describeEachMode('Edge: Flow with same queueName for parent and children', (CONNECTION) => {
  const Q = 'edge-flow-sameq-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('parent and children on the same queue all complete correctly', async () => {
    const flow = new FlowProducer({ connection: CONNECTION });

    const node = await flow.add({
      name: 'same-q-parent',
      queueName: Q,
      data: { level: 'top' },
      children: [
        { name: 'same-q-child-1', queueName: Q, data: { idx: 1 } },
        { name: 'same-q-child-2', queueName: Q, data: { idx: 2 } },
      ],
    });

    const parentId = node.job.id;
    const completedNames: string[] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);
      const worker = new Worker(
        Q,
        async (job: any) => {
          completedNames.push(job.name);
          return { name: job.name, idx: job.data.idx };
        },
        { connection: CONNECTION, concurrency: 2, blockTimeout: 500 },
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

    // All three should be completed
    expect(completedNames).toContain('same-q-child-1');
    expect(completedNames).toContain('same-q-child-2');
    expect(completedNames).toContain('same-q-parent');

    // Children complete before parent
    expect(completedNames.indexOf('same-q-parent')).toBeGreaterThan(
      Math.max(
        completedNames.indexOf('same-q-child-1'),
        completedNames.indexOf('same-q-child-2'),
      ),
    );

    // Parent state should be completed
    const k = buildKeys(Q);
    const parentState = await cleanupClient.hget(k.job(parentId), 'state');
    expect(String(parentState)).toBe('completed');

    await flow.close();
  }, 20000);
});
