/**
 * Budget middleware tests - flow-level token/cost caps.
 * Requires: valkey-server on localhost:6379 and cluster on :7000-7005
 *
 * Run: npx vitest run tests/budget.test.ts
 */
import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { FlowProducer } = require('../dist/flow-producer') as typeof import('../src/flow-producer');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

import { TestQueue, TestWorker } from '../src/testing';
import { describeEachMode, createCleanupClient, flushQueue, waitFor } from './helpers/fixture';

function uid() {
  return `budget-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
}

// ---- Integration tests (real Valkey) ----

describeEachMode('Budget middleware', (CONNECTION) => {
  let cleanupClient: any;
  let queueName: string;
  let queue: InstanceType<typeof Queue>;
  let worker: InstanceType<typeof Worker>;
  let flow: InstanceType<typeof FlowProducer>;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterEach(async () => {
    if (worker) {
      try { await worker.close(true); } catch {}
    }
    if (queue) {
      try { await queue.close(); } catch {}
    }
    if (flow) {
      try { await flow.close(); } catch {}
    }
    if (queueName) await flushQueue(cleanupClient, queueName);
  });

  afterAll(async () => {
    cleanupClient.close();
  });

  it('creates budget hash when flow is added with budget option', async () => {
    queueName = uid();
    flow = new FlowProducer({ connection: CONNECTION });
    queue = new Queue(queueName, { connection: CONNECTION });

    const node = await flow.add(
      {
        name: 'parent',
        queueName,
        data: { task: 'summarize' },
        children: [
          { name: 'child-1', queueName, data: { step: 'embed' } },
          { name: 'child-2', queueName, data: { step: 'generate' } },
        ],
      },
      { budget: { maxTotalTokens: 5000, maxCostUsd: 1.0 } },
    );

    const budget = await queue.getFlowBudget(node.job.id);
    expect(budget).not.toBeNull();
    expect(budget!.maxTotalTokens).toBe(5000);
    expect(budget!.maxCostUsd).toBe(1.0);
    expect(budget!.usedTokens).toBe(0);
    expect(budget!.usedCost).toBe(0);
    expect(budget!.exceeded).toBe(false);
    expect(budget!.onExceeded).toBe('fail');

    // Verify budgetKey is written to parent and children
    const k = buildKeys(queueName);
    const parentBudgetKey = await cleanupClient.hget(k.job(node.job.id), 'budgetKey');
    expect(parentBudgetKey).toBeTruthy();

    for (const child of node.children!) {
      const childBudgetKey = await cleanupClient.hget(k.job(child.job.id), 'budgetKey');
      expect(String(childBudgetKey)).toBe(String(parentBudgetKey));
    }
  });

  it('flow with maxCostUsd stops after exceeding', async () => {
    queueName = uid();
    flow = new FlowProducer({ connection: CONNECTION });
    queue = new Queue(queueName, { connection: CONNECTION });

    const node = await flow.add(
      {
        name: 'parent',
        queueName,
        data: {},
        children: [
          { name: 'child-1', queueName, data: { step: 1 } },
          { name: 'child-2', queueName, data: { step: 2 } },
          { name: 'child-3', queueName, data: { step: 3 } },
        ],
      },
      { budget: { maxCostUsd: 0.05 } },
    );

    const processed: string[] = [];
    const budgetExceededJobs: string[] = [];

    worker = new Worker(
      queueName,
      async (job: any) => {
        processed.push(job.name);
        // Each child reports $0.03 - second child should trigger exceeded
        if (job.name.startsWith('child')) {
          await job.reportUsage({ model: 'gpt-4o', inputTokens: 100, costUsd: 0.03 });
        }
        return 'done';
      },
      { connection: CONNECTION },
    );

    worker.on('budget-exceeded', (job: any) => {
      budgetExceededJobs.push(job.name);
    });

    // Wait for children to process (parent stays in waiting-children)
    await waitFor(async () => {
      const counts = await queue.getJobCounts();
      return counts.failed >= 1 || processed.length >= 3;
    }, 10000);

    // At least the first two children should have been processed
    expect(processed.length).toBeGreaterThanOrEqual(2);

    // Budget should be exceeded after 2 children ($0.06 > $0.05)
    const budget = await queue.getFlowBudget(node.job.id);
    expect(budget).not.toBeNull();
    expect(budget!.exceeded).toBe(true);
    expect(budget!.usedCost).toBeGreaterThanOrEqual(0.06);

    // Budget-exceeded event should have been emitted
    expect(budgetExceededJobs.length).toBeGreaterThanOrEqual(1);
  });

  it('maxTotalTokens enforced independently of cost', async () => {
    queueName = uid();
    flow = new FlowProducer({ connection: CONNECTION });
    queue = new Queue(queueName, { connection: CONNECTION });

    const node = await flow.add(
      {
        name: 'parent',
        queueName,
        data: {},
        children: [
          { name: 'child-1', queueName, data: {} },
          { name: 'child-2', queueName, data: {} },
        ],
      },
      { budget: { maxTotalTokens: 500 } },
    );

    worker = new Worker(
      queueName,
      async (job: any) => {
        if (job.name === 'child-1') {
          await job.reportUsage({ inputTokens: 300, outputTokens: 100 });
        } else if (job.name === 'child-2') {
          await job.reportUsage({ inputTokens: 200, outputTokens: 50 });
        }
        return 'ok';
      },
      { connection: CONNECTION },
    );

    await waitFor(async () => {
      const budget = await queue.getFlowBudget(node.job.id);
      return budget !== null && budget.exceeded;
    }, 10000);

    const budget = await queue.getFlowBudget(node.job.id);
    expect(budget!.exceeded).toBe(true);
    expect(budget!.usedTokens).toBeGreaterThanOrEqual(500);
  });

  it('pre-dispatch check prevents starting job when budget already exceeded', async () => {
    queueName = uid();
    flow = new FlowProducer({ connection: CONNECTION });
    queue = new Queue(queueName, { connection: CONNECTION });

    const node = await flow.add(
      {
        name: 'parent',
        queueName,
        data: {},
        children: [
          { name: 'child-1', queueName, data: {} },
          { name: 'child-2', queueName, data: {} },
        ],
      },
      { budget: { maxCostUsd: 0.01 } },
    );

    // Manually set budget to exceeded before any worker runs
    const budgetKey = node.job.budgetKey!;
    await cleanupClient.hset(budgetKey, { exceeded: '1' });

    const processedJobs: string[] = [];
    const failedJobs: string[] = [];

    worker = new Worker(
      queueName,
      async (job: any) => {
        processedJobs.push(job.name);
        return 'ok';
      },
      { connection: CONNECTION },
    );

    worker.on('failed', (job: any) => {
      failedJobs.push(job.name);
    });

    // Wait for children to fail
    await waitFor(async () => {
      return failedJobs.length >= 2;
    }, 10000);

    // No children should have been processed (budget was pre-exceeded)
    expect(processedJobs).toHaveLength(0);
    expect(failedJobs.length).toBeGreaterThanOrEqual(2);
  });

  it('onExceeded: fail fails remaining jobs', async () => {
    queueName = uid();
    flow = new FlowProducer({ connection: CONNECTION });
    queue = new Queue(queueName, { connection: CONNECTION });

    const node = await flow.add(
      {
        name: 'parent',
        queueName,
        data: {},
        children: [
          { name: 'child-1', queueName, data: {} },
          { name: 'child-2', queueName, data: {} },
          { name: 'child-3', queueName, data: {} },
        ],
      },
      { budget: { maxCostUsd: 0.01, onExceeded: 'fail' } },
    );

    let childOneDone = false;
    const failedJobs: string[] = [];

    worker = new Worker(
      queueName,
      async (job: any) => {
        if (job.name === 'child-1') {
          // First child exceeds the budget
          await job.reportUsage({ costUsd: 0.02 });
          childOneDone = true;
        }
        return 'ok';
      },
      { connection: CONNECTION, concurrency: 1 },
    );

    worker.on('failed', (job: any) => {
      failedJobs.push(job.name);
    });

    await waitFor(async () => {
      return childOneDone && failedJobs.length >= 2;
    }, 10000);

    // Remaining children should have failed with budget exceeded
    const failedChildren = failedJobs.filter((n) => n.startsWith('child'));
    expect(failedChildren.length).toBeGreaterThanOrEqual(2);
  });

  it('budget state readable via getFlowBudget()', async () => {
    queueName = uid();
    flow = new FlowProducer({ connection: CONNECTION });
    queue = new Queue(queueName, { connection: CONNECTION });

    const node = await flow.add(
      {
        name: 'parent',
        queueName,
        data: {},
        children: [{ name: 'child-1', queueName, data: {} }],
      },
      { budget: { maxTotalTokens: 10000, maxCostUsd: 5.0, onExceeded: 'pause' } },
    );

    const budget = await queue.getFlowBudget(node.job.id);
    expect(budget).not.toBeNull();
    expect(budget!.maxTotalTokens).toBe(10000);
    expect(budget!.maxCostUsd).toBe(5.0);
    expect(budget!.usedTokens).toBe(0);
    expect(budget!.usedCost).toBe(0);
    expect(budget!.exceeded).toBe(false);
    expect(budget!.onExceeded).toBe('pause');
  });

  it('getFlowBudget returns null for jobs without budget', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });
    const budget = await queue.getFlowBudget('nonexistent-id');
    expect(budget).toBeNull();
  });

  it('worker emits budget-exceeded event', async () => {
    queueName = uid();
    flow = new FlowProducer({ connection: CONNECTION });
    queue = new Queue(queueName, { connection: CONNECTION });

    const node = await flow.add(
      {
        name: 'parent',
        queueName,
        data: {},
        children: [{ name: 'child-1', queueName, data: {} }],
      },
      { budget: { maxCostUsd: 0.001 } },
    );

    let budgetExceededEmitted = false;

    worker = new Worker(
      queueName,
      async (job: any) => {
        if (job.name === 'child-1') {
          await job.reportUsage({ costUsd: 0.01 });
        }
        return 'ok';
      },
      { connection: CONNECTION },
    );

    worker.on('budget-exceeded', () => {
      budgetExceededEmitted = true;
    });

    await waitFor(() => budgetExceededEmitted, 10000);
    expect(budgetExceededEmitted).toBe(true);
  });

  it('budget not checked for jobs without budgetKey', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });

    // Add a plain job (no flow, no budget)
    await queue.add('plain-job', { data: 'test' });

    const processed: string[] = [];
    worker = new Worker(
      queueName,
      async (job: any) => {
        processed.push(job.name);
        return 'ok';
      },
      { connection: CONNECTION },
    );

    await waitFor(() => processed.length >= 1, 5000);
    expect(processed).toContain('plain-job');
  });
});

// ---- Testing mode tests ----

describe('Budget middleware (testing mode)', () => {
  it('budget enforcement in TestWorker', async () => {
    const queue = new TestQueue('test-budget');
    const budgetFlowId = 'flow-1';

    queue.setBudget(budgetFlowId, {
      maxCostUsd: 0.05,
    });

    // Add jobs with budgetKey
    const job1 = await queue.add('child-1', { step: 1 }, {});
    const job2 = await queue.add('child-2', { step: 2 }, {});
    const job3 = await queue.add('child-3', { step: 3 }, {});

    // Set budgetKey on records
    const r1 = queue.jobs.get(job1!.id)!;
    const r2 = queue.jobs.get(job2!.id)!;
    const r3 = queue.jobs.get(job3!.id)!;
    r1.budgetKey = budgetFlowId;
    r2.budgetKey = budgetFlowId;
    r3.budgetKey = budgetFlowId;

    const processed: string[] = [];
    const budgetExceededJobs: string[] = [];
    const failedJobs: string[] = [];

    const worker = new TestWorker(
      queue,
      async (job: any) => {
        processed.push(job.name);
        // Each child costs $0.03
        await job.reportUsage({ costUsd: 0.03 });
        return 'ok';
      },
      { concurrency: 1 },
    );

    worker.on('budget-exceeded', (job: any) => {
      budgetExceededJobs.push(job.name);
    });

    worker.on('failed', (job: any) => {
      failedJobs.push(job.name);
    });

    // Wait for processing
    await new Promise((r) => setTimeout(r, 200));

    // First child completes, second child triggers exceeded
    expect(processed.length).toBeGreaterThanOrEqual(1);

    // Budget should be exceeded
    const budget = await queue.getFlowBudget(budgetFlowId);
    expect(budget!.exceeded).toBe(true);

    // Third child should be failed (budget was exceeded before it ran)
    expect(failedJobs.length).toBeGreaterThanOrEqual(1);

    await worker.close();
    await queue.close();
  });

  it('getFlowBudget returns null when no budget set', async () => {
    const queue = new TestQueue('test-no-budget');
    const budget = await queue.getFlowBudget('nonexistent');
    expect(budget).toBeNull();
    await queue.close();
  });

  it('onExceeded pause does not fail jobs', async () => {
    const queue = new TestQueue('test-budget-pause');
    const budgetFlowId = 'flow-pause';

    queue.setBudget(budgetFlowId, {
      maxCostUsd: 0.01,
      onExceeded: 'pause',
    });

    // Manually mark as exceeded
    const budgetState = queue.budgets.get(budgetFlowId)!;
    budgetState.exceeded = true;

    const job1 = await queue.add('paused-job', {}, {});
    const r1 = queue.jobs.get(job1!.id)!;
    r1.budgetKey = budgetFlowId;

    const failedJobs: string[] = [];
    const budgetExceededJobs: string[] = [];

    const worker = new TestWorker(
      queue,
      async () => 'ok',
      { concurrency: 1 },
    );

    worker.on('failed', (job: any) => failedJobs.push(job.name));
    worker.on('budget-exceeded', (job: any) => budgetExceededJobs.push(job.name));

    await new Promise((r) => setTimeout(r, 200));

    // Job should NOT be failed - it should be moved back to waiting
    expect(failedJobs).toHaveLength(0);
    // Budget-exceeded event should fire
    expect(budgetExceededJobs.length).toBeGreaterThanOrEqual(1);

    await worker.close();
    await queue.close();
  });

  it('maxTotalTokens works in testing mode', async () => {
    const queue = new TestQueue('test-budget-tokens');
    const budgetFlowId = 'flow-tokens';

    queue.setBudget(budgetFlowId, {
      maxTotalTokens: 500,
    });

    const job1 = await queue.add('child-1', {}, {});
    const r1 = queue.jobs.get(job1!.id)!;
    r1.budgetKey = budgetFlowId;

    const worker = new TestWorker(
      queue,
      async (job: any) => {
        await job.reportUsage({ inputTokens: 300, outputTokens: 300 });
        return 'ok';
      },
      { concurrency: 1 },
    );

    let budgetExceeded = false;
    worker.on('budget-exceeded', () => { budgetExceeded = true; });

    await new Promise((r) => setTimeout(r, 200));

    const budget = await queue.getFlowBudget(budgetFlowId);
    expect(budget!.exceeded).toBe(true);
    expect(budget!.usedTokens).toBe(600);
    expect(budgetExceeded).toBe(true);

    await worker.close();
    await queue.close();
  });
});
