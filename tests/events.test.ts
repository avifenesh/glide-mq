/**
 * Integration tests for QueueEvents (stream-based event subscription).
 * Runs against both standalone (:6379) and cluster (:7000).
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import {
  describeEachMode,
  createCleanupClient,
  flushQueue,
  ConnectionConfig,
} from './helpers/fixture';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { QueueEvents } = require('../dist/queue-events') as typeof import('../src/queue-events');

describeEachMode('QueueEvents', (CONNECTION) => {
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    cleanupClient.close();
  });

  describe('added event', () => {
    const Q = 'test-qe-added-' + Date.now();
    let queue: InstanceType<typeof Queue>;
    let queueEvents: InstanceType<typeof QueueEvents>;

    beforeAll(async () => {
      queue = new Queue(Q, { connection: CONNECTION });
    });

    afterAll(async () => {
      await queueEvents.close();
      await queue.close();
      await flushQueue(cleanupClient, Q);
    });

    it('emits added event when a job is added', async () => {
      const received = new Promise<{ jobId: string; name: string }>((resolve, reject) => {
        const timeout = setTimeout(
          () => reject(new Error('timeout waiting for added event')),
          10000,
        );
        queueEvents = new QueueEvents(Q, { connection: CONNECTION, blockTimeout: 1000 });
        queueEvents.on('added', (data: any) => {
          clearTimeout(timeout);
          resolve(data);
        });
        queueEvents.on('error', () => {});
      });

      // Wait for QueueEvents to connect and start listening
      await queueEvents.waitUntilReady();
      // Small delay to ensure XREAD BLOCK is active
      await new Promise((r) => setTimeout(r, 200));

      const job = await queue.add('test-job', { x: 1 });

      const event = await received;
      expect(event.jobId).toBe(job.id);
      expect(event.name).toBe('test-job');
    }, 15000);
  });

  describe('completed event', () => {
    const Q = 'test-qe-completed-' + Date.now();
    let queue: InstanceType<typeof Queue>;
    let queueEvents: InstanceType<typeof QueueEvents>;

    afterAll(async () => {
      await queueEvents.close();
      await queue.close();
      await flushQueue(cleanupClient, Q);
    });

    it('emits completed event when a job is processed', async () => {
      queue = new Queue(Q, { connection: CONNECTION });
      queueEvents = new QueueEvents(Q, { connection: CONNECTION, blockTimeout: 1000 });
      queueEvents.on('error', () => {});

      const received = new Promise<{ jobId: string; returnvalue: string }>((resolve, reject) => {
        const timeout = setTimeout(
          () => reject(new Error('timeout waiting for completed event')),
          15000,
        );
        queueEvents.on('completed', (data: any) => {
          clearTimeout(timeout);
          resolve(data);
        });
      });

      await queueEvents.waitUntilReady();
      await new Promise((r) => setTimeout(r, 200));

      const worker = new Worker(
        Q,
        async () => {
          return { result: 42 };
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
      );
      worker.on('error', () => {});

      await new Promise((r) => setTimeout(r, 500));
      const job = await queue.add('compute', { input: 1 });

      const event = await received;
      expect(event.jobId).toBe(job.id);
      expect(event.returnvalue).toBe(JSON.stringify({ result: 42 }));

      await worker.close(true);
    }, 20000);
  });

  describe('failed event', () => {
    const Q = 'test-qe-failed-' + Date.now();
    let queue: InstanceType<typeof Queue>;
    let queueEvents: InstanceType<typeof QueueEvents>;

    afterAll(async () => {
      await queueEvents.close();
      await queue.close();
      await flushQueue(cleanupClient, Q);
    });

    it('emits failed event when a job fails', async () => {
      queue = new Queue(Q, { connection: CONNECTION });
      queueEvents = new QueueEvents(Q, { connection: CONNECTION, blockTimeout: 1000 });
      queueEvents.on('error', () => {});

      const received = new Promise<{ jobId: string; failedReason: string }>((resolve, reject) => {
        const timeout = setTimeout(
          () => reject(new Error('timeout waiting for failed event')),
          15000,
        );
        queueEvents.on('failed', (data: any) => {
          clearTimeout(timeout);
          resolve(data);
        });
      });

      await queueEvents.waitUntilReady();
      await new Promise((r) => setTimeout(r, 200));

      const worker = new Worker(
        Q,
        async () => {
          throw new Error('something went wrong');
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
      );
      worker.on('error', () => {});
      worker.on('failed', () => {});

      await new Promise((r) => setTimeout(r, 500));
      const job = await queue.add('fail-job', { input: 1 });

      const event = await received;
      expect(event.jobId).toBe(job.id);
      expect(event.failedReason).toBe('something went wrong');

      await worker.close(true);
    }, 20000);
  });

  describe('multiple events in sequence', () => {
    const Q = 'test-qe-multi-' + Date.now();
    let queue: InstanceType<typeof Queue>;
    let queueEvents: InstanceType<typeof QueueEvents>;

    afterAll(async () => {
      await queueEvents.close();
      await queue.close();
      await flushQueue(cleanupClient, Q);
    });

    it('receives added and completed events for the same job', async () => {
      queue = new Queue(Q, { connection: CONNECTION });
      queueEvents = new QueueEvents(Q, { connection: CONNECTION, blockTimeout: 1000 });
      queueEvents.on('error', () => {});

      const addedEvents: any[] = [];
      const completedEvents: any[] = [];

      const allDone = new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error('timeout waiting for events')), 15000);
        queueEvents.on('added', (data: any) => {
          addedEvents.push(data);
        });
        queueEvents.on('completed', (data: any) => {
          completedEvents.push(data);
          if (completedEvents.length >= 2) {
            clearTimeout(timeout);
            resolve();
          }
        });
      });

      await queueEvents.waitUntilReady();
      await new Promise((r) => setTimeout(r, 200));

      const worker = new Worker(
        Q,
        async (job: any) => {
          return `done-${job.id}`;
        },
        { connection: CONNECTION, concurrency: 2, blockTimeout: 1000 },
      );
      worker.on('error', () => {});

      await new Promise((r) => setTimeout(r, 500));
      const job1 = await queue.add('multi-1', { i: 1 });
      const job2 = await queue.add('multi-2', { i: 2 });

      await allDone;

      // Verify added events
      const addedJobIds = addedEvents.map((e: any) => e.jobId);
      expect(addedJobIds).toContain(job1.id);
      expect(addedJobIds).toContain(job2.id);

      // Verify completed events
      const completedJobIds = completedEvents.map((e: any) => e.jobId);
      expect(completedJobIds).toContain(job1.id);
      expect(completedJobIds).toContain(job2.id);

      await worker.close(true);
    }, 20000);
  });
});
