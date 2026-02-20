/**
 * Gap-advanced tests: lock renewal, dead letter queue, workflow primitives.
 * Requires: valkey-server on localhost:6379 and cluster on :7000-7005
 *
 * Run: npx vitest run tests/gap-advanced.test.ts
 */
import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { FlowProducer } = require('../dist/flow-producer') as typeof import('../src/flow-producer');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
const { chain, group, chord } = require('../dist/workflows') as typeof import('../src/workflows');

import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

describeEachMode('Gap Advanced', (CONNECTION) => {
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    cleanupClient.close();
  });

  // ---------------------------------------------------------------------------
  // Lock Renewal (heartbeat)
  // ---------------------------------------------------------------------------
  describe('Lock Renewal', () => {
    const workers: InstanceType<typeof Worker>[] = [];

    afterEach(async () => {
      for (const w of workers) {
        try {
          await w.close(true);
        } catch {}
      }
      workers.length = 0;
    });

    it('long-running job with heartbeat is NOT reclaimed as stalled', async () => {
      const Q = 'test-lock-renewal-' + Date.now();
      const queue = new Queue(Q, { connection: CONNECTION });

      const job = await queue.add('long-task', { value: 42 });
      expect(job!.id).toBeTruthy();

      const completed = new Promise<void>((resolve) => {
        const worker = new Worker(
          Q,
          async () => {
            await new Promise<void>((r) => setTimeout(r, 3000));
            return 'done';
          },
          {
            connection: CONNECTION,
            stalledInterval: 1000,
            maxStalledCount: 1,
            lockDuration: 1000,
            concurrency: 1,
          },
        );
        workers.push(worker);

        worker.on('completed', () => {
          resolve();
        });
      });

      await completed;

      const finishedJob = await queue.getJob(job!.id);
      expect(finishedJob).toBeTruthy();
      expect(await finishedJob!.getState()).toBe('completed');

      await queue.close();
      await flushQueue(cleanupClient, Q);
    }, 15000);

    it('job WITHOUT heartbeat gets reclaimed when stalled', async () => {
      const Q = 'test-lock-renewal-no-heartbeat-' + Date.now();
      const queue = new Queue(Q, { connection: CONNECTION });

      await queue.add('stall-test', { value: 1 });

      let stalledEmitted = false;
      const stalledPromise = new Promise<void>((resolve) => {
        const worker1 = new Worker(
          Q,
          async () => {
            await new Promise<void>((r) => setTimeout(r, 60000));
            return 'never';
          },
          {
            connection: CONNECTION,
            stalledInterval: 500,
            maxStalledCount: 2,
            lockDuration: 60000,
            concurrency: 1,
          },
        );
        workers.push(worker1);

        worker1.on('stalled', () => {
          stalledEmitted = true;
          resolve();
        });

        setTimeout(resolve, 5000);
      });

      await stalledPromise;

      await queue.close();
      await flushQueue(cleanupClient, Q);
    }, 10000);
  });

  // ---------------------------------------------------------------------------
  // Dead Letter Queue
  // ---------------------------------------------------------------------------
  describe('Dead Letter Queue', () => {
    const workers: InstanceType<typeof Worker>[] = [];

    afterEach(async () => {
      for (const w of workers) {
        try {
          await w.close(true);
        } catch {}
      }
      workers.length = 0;
    });

    it('job that exhausts retries moves to DLQ', async () => {
      const Q = 'test-dlq-' + Date.now();
      const DLQ = Q + '-dead';
      const queue = new Queue(Q, {
        connection: CONNECTION,
        deadLetterQueue: { name: DLQ },
      });

      await queue.add(
        'fail-job',
        { value: 'important' },
        {
          attempts: 3,
          backoff: { type: 'fixed', delay: 100 },
        },
      );

      let failCount = 0;
      const allDone = new Promise<void>((resolve) => {
        const worker = new Worker(
          Q,
          async () => {
            throw new Error('deliberate failure');
          },
          {
            connection: CONNECTION,
            stalledInterval: 30000,
            promotionInterval: 200,
            deadLetterQueue: { name: DLQ },
          },
        );
        workers.push(worker);

        worker.on('failed', () => {
          failCount++;
          if (failCount >= 3) {
            setTimeout(resolve, 500);
          }
        });
      });

      await allDone;

      const dlqJobs = await queue.getDeadLetterJobs();
      expect(dlqJobs.length).toBeGreaterThanOrEqual(1);

      const dlqJob = dlqJobs[0];
      const dlqData = dlqJob.data as any;
      expect(dlqData.originalQueue).toBe(Q);
      expect(dlqData.failedReason).toBe('deliberate failure');

      await queue.close();
      await flushQueue(cleanupClient, Q);
      await flushQueue(cleanupClient, DLQ);
    }, 30000);

    it('getDeadLetterJobs returns empty when no DLQ configured', async () => {
      const Q = 'test-dlq-nodlq-' + Date.now();
      const queueNoDlq = new Queue(Q, { connection: CONNECTION });
      const jobs = await queueNoDlq.getDeadLetterJobs();
      expect(jobs).toEqual([]);
      await queueNoDlq.close();
      await flushQueue(cleanupClient, Q);
    });
  });

  // ---------------------------------------------------------------------------
  // Workflow: chain
  // ---------------------------------------------------------------------------
  describe('Workflow: chain', () => {
    const workers: InstanceType<typeof Worker>[] = [];

    afterEach(async () => {
      for (const w of workers) {
        try {
          await w.close(true);
        } catch {}
      }
      workers.length = 0;
    });

    it('chain of 3 jobs executes in order', async () => {
      const Q = 'test-chain-' + Date.now();
      const executionOrder: string[] = [];

      const allCompleted = new Promise<void>((resolve) => {
        const worker = new Worker(
          Q,
          async (job: any) => {
            executionOrder.push(job.name);
            return `result-${job.name}`;
          },
          {
            connection: CONNECTION,
            stalledInterval: 30000,
            promotionInterval: 200,
          },
        );
        workers.push(worker);

        let completedCount = 0;
        worker.on('completed', () => {
          completedCount++;
          if (completedCount >= 3) {
            resolve();
          }
        });
      });

      const node = await chain(
        Q,
        [
          { name: 'step-1', data: { order: 1 } },
          { name: 'step-2', data: { order: 2 } },
          { name: 'step-3', data: { order: 3 } },
        ],
        CONNECTION,
      );

      expect(node.job.name).toBe('step-1');
      expect(node.children).toHaveLength(1);
      expect(node.children![0].job.name).toBe('step-2');

      await allCompleted;

      expect(executionOrder[0]).toBe('step-3');
      expect(executionOrder[1]).toBe('step-2');
      expect(executionOrder[2]).toBe('step-1');

      await flushQueue(cleanupClient, Q);
    }, 15000);
  });

  // ---------------------------------------------------------------------------
  // Workflow: group
  // ---------------------------------------------------------------------------
  describe('Workflow: group', () => {
    const workers: InstanceType<typeof Worker>[] = [];

    afterEach(async () => {
      for (const w of workers) {
        try {
          await w.close(true);
        } catch {}
      }
      workers.length = 0;
    });

    it('group of 3 jobs runs in parallel', async () => {
      const Q = 'test-group-' + Date.now();
      const startTimes: Record<string, number> = {};

      const allCompleted = new Promise<void>((resolve) => {
        const worker = new Worker(
          Q,
          async (job: any) => {
            if (job.name === '__group__') {
              return 'group-done';
            }
            startTimes[job.name] = Date.now();
            await new Promise<void>((r) => setTimeout(r, 500));
            return `result-${job.name}`;
          },
          {
            connection: CONNECTION,
            stalledInterval: 30000,
            concurrency: 3,
            promotionInterval: 200,
          },
        );
        workers.push(worker);

        let completedCount = 0;
        worker.on('completed', () => {
          completedCount++;
          if (completedCount >= 4) {
            resolve();
          }
        });
      });

      const node = await group(
        Q,
        [
          { name: 'task-a', data: { idx: 1 } },
          { name: 'task-b', data: { idx: 2 } },
          { name: 'task-c', data: { idx: 3 } },
        ],
        CONNECTION,
      );

      expect(node.job.name).toBe('__group__');
      expect(node.children).toHaveLength(3);

      await allCompleted;

      expect(Object.keys(startTimes)).toHaveLength(3);
      expect(startTimes['task-a']).toBeDefined();
      expect(startTimes['task-b']).toBeDefined();
      expect(startTimes['task-c']).toBeDefined();

      await flushQueue(cleanupClient, Q);
    }, 15000);
  });

  // ---------------------------------------------------------------------------
  // Workflow: chord
  // ---------------------------------------------------------------------------
  describe('Workflow: chord', () => {
    const workers: InstanceType<typeof Worker>[] = [];

    afterEach(async () => {
      for (const w of workers) {
        try {
          await w.close(true);
        } catch {}
      }
      workers.length = 0;
    });

    it('chord runs callback after group completes', async () => {
      const Q = 'test-chord-' + Date.now();
      let callbackExecuted = false;
      let callbackReceivedChildResults = false;

      const allDone = new Promise<void>((resolve) => {
        const worker = new Worker(
          Q,
          async (job: any) => {
            if (job.name === 'callback') {
              callbackExecuted = true;
              const childValues = await job.getChildrenValues();
              if (Object.keys(childValues).length > 0) {
                callbackReceivedChildResults = true;
              }
              return 'final-result';
            }
            return job.data.value * 2;
          },
          {
            connection: CONNECTION,
            stalledInterval: 30000,
            concurrency: 3,
            promotionInterval: 200,
          },
        );
        workers.push(worker);

        let completedCount = 0;
        worker.on('completed', (completedJob: any) => {
          completedCount++;
          if (completedJob.name === 'callback') {
            resolve();
          }
          if (completedCount >= 4) {
            resolve();
          }
        });
      });

      const node = await chord(
        Q,
        [
          { name: 'multiply-1', data: { value: 10 } },
          { name: 'multiply-2', data: { value: 20 } },
          { name: 'multiply-3', data: { value: 30 } },
        ],
        { name: 'callback', data: { type: 'aggregate' } },
        CONNECTION,
      );

      expect(node.job.name).toBe('callback');
      expect(node.children).toHaveLength(3);

      await allDone;

      expect(callbackExecuted).toBe(true);
      expect(callbackReceivedChildResults).toBe(true);

      await flushQueue(cleanupClient, Q);
    }, 15000);
  });
});
