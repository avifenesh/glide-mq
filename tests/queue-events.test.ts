/**
 * Integration tests for QueueEvents emissions not covered by events.test.ts /
 * edge-events.test.ts: progress, drained, paused, resumed, removed, plus close() lifecycle.
 * Runs against standalone (:6379) and cluster (:7000).
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { QueueEvents } = require('../dist/queue-events') as typeof import('../src/queue-events');

/** Resolve with the first payload emitted for `event`, or reject after `ms`. */
function nextEvent(qe: any, event: string, ms = 10000): Promise<any> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error(`timeout waiting for ${event} event`)), ms);
    qe.once(event, (data: any) => {
      clearTimeout(timeout);
      resolve(data);
    });
  });
}

describeEachMode('QueueEvents emissions', (CONNECTION) => {
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    cleanupClient.close();
  });

  describe('progress event', () => {
    const Q = 'test-qe-progress-' + Date.now();
    let queue: InstanceType<typeof Queue>;
    let queueEvents: InstanceType<typeof QueueEvents>;
    let worker: any;

    afterAll(async () => {
      await worker?.close(true);
      await queueEvents?.close();
      await queue?.close();
      await flushQueue(cleanupClient, Q);
    });

    it('emits progress event when a job reports progress', async () => {
      queue = new Queue(Q, { connection: CONNECTION });
      queueEvents = new QueueEvents(Q, { connection: CONNECTION, blockTimeout: 1000 });
      queueEvents.on('error', () => {});
      await queueEvents.waitUntilReady();
      await new Promise((r) => setTimeout(r, 200));

      const received = nextEvent(queueEvents, 'progress', 15000);

      worker = new Worker(
        Q,
        async (job: any) => {
          await job.updateProgress(42);
          return null;
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
      );
      worker.on('error', () => {});

      await new Promise((r) => setTimeout(r, 500));
      const job = await queue.add('with-progress', { x: 1 });
      expect(job).not.toBeNull();

      const event = await received;
      expect(event.jobId).toBe(job!.id);
      expect(event.data).toBe('42');
    }, 20000);
  });

  describe('paused and resumed events', () => {
    const Q = 'test-qe-pause-' + Date.now();
    let queue: InstanceType<typeof Queue>;
    let queueEvents: InstanceType<typeof QueueEvents>;

    afterAll(async () => {
      await queueEvents?.close();
      await queue?.close();
      await flushQueue(cleanupClient, Q);
    });

    it('emits paused then resumed', async () => {
      queue = new Queue(Q, { connection: CONNECTION });
      queueEvents = new QueueEvents(Q, { connection: CONNECTION, blockTimeout: 1000 });
      queueEvents.on('error', () => {});
      await queueEvents.waitUntilReady();
      await new Promise((r) => setTimeout(r, 200));

      const paused = nextEvent(queueEvents, 'paused');
      await queue.pause();
      await paused;

      const resumed = nextEvent(queueEvents, 'resumed');
      await queue.resume();
      await resumed;
    }, 20000);
  });

  describe('removed event', () => {
    const Q = 'test-qe-removed-' + Date.now();
    let queue: InstanceType<typeof Queue>;
    let queueEvents: InstanceType<typeof QueueEvents>;

    afterAll(async () => {
      await queueEvents?.close();
      await queue?.close();
      await flushQueue(cleanupClient, Q);
    });

    it('emits removed when a job is removed', async () => {
      queue = new Queue(Q, { connection: CONNECTION });
      queueEvents = new QueueEvents(Q, { connection: CONNECTION, blockTimeout: 1000 });
      queueEvents.on('error', () => {});
      await queueEvents.waitUntilReady();
      await new Promise((r) => setTimeout(r, 200));

      const job = await queue.add('to-remove', { x: 1 });
      expect(job).not.toBeNull();
      const received = nextEvent(queueEvents, 'removed');

      const fetched = await queue.getJob(job!.id);
      expect(fetched).not.toBeNull();
      await fetched!.remove();

      const event = await received;
      expect(event.jobId).toBe(job!.id);
    }, 20000);
  });

  describe('drained event', () => {
    const Q = 'test-qe-drained-' + Date.now();
    let queue: InstanceType<typeof Queue>;
    let queueEvents: InstanceType<typeof QueueEvents>;

    afterAll(async () => {
      await queueEvents?.close();
      await queue?.close();
      await flushQueue(cleanupClient, Q);
    });

    it('emits drained when the queue is drained', async () => {
      queue = new Queue(Q, { connection: CONNECTION });
      queueEvents = new QueueEvents(Q, { connection: CONNECTION, blockTimeout: 1000 });
      queueEvents.on('error', () => {});
      await queueEvents.waitUntilReady();
      await new Promise((r) => setTimeout(r, 200));

      await queue.add('a', { x: 1 });
      await queue.add('b', { x: 2 });

      const received = nextEvent(queueEvents, 'drained');
      await queue.drain();
      await received;
    }, 20000);
  });

  describe('close() lifecycle', () => {
    const Q = 'test-qe-close-' + Date.now();

    afterAll(async () => {
      await flushQueue(cleanupClient, Q);
    });

    it('close() is idempotent', async () => {
      const qe = new QueueEvents(Q, { connection: CONNECTION, blockTimeout: 1000 });
      qe.on('error', () => {});
      await qe.waitUntilReady();
      await qe.close();
      await expect(qe.close()).resolves.toBeUndefined();
    }, 15000);

    it('close() before ready resolves cleanly', async () => {
      const qe = new QueueEvents(Q, { connection: CONNECTION, blockTimeout: 1000 });
      qe.on('error', () => {});
      // Do not await waitUntilReady - close immediately to exercise the init race.
      await expect(qe.close()).resolves.toBeUndefined();
    }, 15000);
  });
});
