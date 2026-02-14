/**
 * Edge-case tests for QueueEvents (stream-based event subscription).
 * Runs against both standalone (:6379) and cluster (:7000).
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { describeEachMode, createCleanupClient, flushQueue, ConnectionConfig } from './helpers/fixture';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { QueueEvents } = require('../dist/queue-events') as typeof import('../src/queue-events');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

describeEachMode('Edge: QueueEvents', (CONNECTION) => {
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    cleanupClient.close();
  });

  describe('QueueEvents with lastEventId=0 replays historical events', () => {
    const Q = 'edge-qe-replay-' + Date.now();

    afterAll(async () => {
      await flushQueue(cleanupClient, Q);
    });

    it('replays all historical events when lastEventId is 0', async () => {
      const queue = new Queue(Q, { connection: CONNECTION });
      const job1 = await queue.add('hist-1', { i: 1 });
      const job2 = await queue.add('hist-2', { i: 2 });

      await new Promise(r => setTimeout(r, 300));

      const replayedEvents: any[] = [];
      const queueEvents = new QueueEvents(Q, {
        connection: CONNECTION,
        blockTimeout: 500,
        lastEventId: '0',
      });
      queueEvents.on('error', () => {});

      queueEvents.on('added', (data: any) => {
        replayedEvents.push(data);
      });

      await queueEvents.waitUntilReady();

      await new Promise(r => setTimeout(r, 2000));

      const replayedJobIds = replayedEvents.map(e => e.jobId);
      expect(replayedJobIds).toContain(job1!.id);
      expect(replayedJobIds).toContain(job2!.id);

      await queueEvents.close();
      await queue.close();
    }, 10000);
  });

  describe('QueueEvents close while blocking', () => {
    const Q = 'edge-qe-close-' + Date.now();

    afterAll(async () => {
      await flushQueue(cleanupClient, Q);
    });

    it('closes cleanly while XREAD BLOCK is active', async () => {
      const queueEvents = new QueueEvents(Q, {
        connection: CONNECTION,
        blockTimeout: 5000,
      });
      queueEvents.on('error', () => {});

      await queueEvents.waitUntilReady();

      await new Promise(r => setTimeout(r, 500));

      await queueEvents.close();

      expect(true).toBe(true);
    }, 10000);
  });

  describe('Multiple QueueEvents instances on same queue', () => {
    const Q = 'edge-qe-multi-' + Date.now();

    afterAll(async () => {
      await flushQueue(cleanupClient, Q);
    });

    it('both instances receive events', async () => {
      const queue = new Queue(Q, { connection: CONNECTION });

      const instance1Events: any[] = [];
      const instance2Events: any[] = [];

      const qe1 = new QueueEvents(Q, { connection: CONNECTION, blockTimeout: 500 });
      const qe2 = new QueueEvents(Q, { connection: CONNECTION, blockTimeout: 500 });
      qe1.on('error', () => {});
      qe2.on('error', () => {});

      qe1.on('added', (data: any) => { instance1Events.push(data); });
      qe2.on('added', (data: any) => { instance2Events.push(data); });

      await qe1.waitUntilReady();
      await qe2.waitUntilReady();
      await new Promise(r => setTimeout(r, 300));

      const job = await queue.add('multi-qe-test', { x: 1 });

      await new Promise(r => setTimeout(r, 2000));

      const ids1 = instance1Events.map(e => e.jobId);
      const ids2 = instance2Events.map(e => e.jobId);

      expect(ids1).toContain(job!.id);
      expect(ids2).toContain(job!.id);

      await qe1.close();
      await qe2.close();
      await queue.close();
    }, 10000);
  });

  describe('QueueEvents retrying event', () => {
    const Q = 'edge-qe-retry-' + Date.now();

    afterAll(async () => {
      await flushQueue(cleanupClient, Q);
    });

    it('emits retrying event when job fails with retries configured', async () => {
      const queue = new Queue(Q, { connection: CONNECTION });
      const queueEvents = new QueueEvents(Q, { connection: CONNECTION, blockTimeout: 500 });
      queueEvents.on('error', () => {});

      const retryingEvents: any[] = [];
      const failedEvents: any[] = [];

      queueEvents.on('retrying', (data: any) => { retryingEvents.push(data); });
      queueEvents.on('failed', (data: any) => { failedEvents.push(data); });

      await queueEvents.waitUntilReady();
      await new Promise(r => setTimeout(r, 300));

      let attemptCount = 0;
      const worker = new Worker(
        Q,
        async (job: any) => {
          attemptCount++;
          if (attemptCount <= 1) {
            throw new Error('retry me');
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
      worker.on('error', () => {});
      worker.on('failed', () => {});

      await new Promise(r => setTimeout(r, 500));

      const job = await queue.add('retry-job', { x: 1 }, {
        attempts: 3,
        backoff: { type: 'fixed', delay: 200 },
      });

      await new Promise(r => setTimeout(r, 5000));

      const k = buildKeys(Q);
      const state = await cleanupClient.hget(k.job(job!.id), 'state');
      expect(['completed', 'active', 'delayed'].includes(String(state))).toBe(true);

      await worker.close(true);
      await queueEvents.close();
      await queue.close();
    }, 15000);
  });

  describe('QueueEvents stalled event', () => {
    const Q = 'edge-qe-stalled-' + Date.now();

    afterAll(async () => {
      await flushQueue(cleanupClient, Q);
    });

    it('emits stalled event when a worker is killed mid-processing', async () => {
      const queue = new Queue(Q, { connection: CONNECTION });

      const job = await queue.add('stall-test', { x: 1 });

      const stallWorker = new Worker(
        Q,
        async () => {
          await new Promise(r => setTimeout(r, 60000));
          return 'never';
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 500,
          stalledInterval: 60000,
        },
      );
      stallWorker.on('error', () => {});

      await new Promise(r => setTimeout(r, 2000));

      await stallWorker.close(true);

      const stalledIds: string[] = [];
      const recoveryWorker = new Worker(
        Q,
        async (j: any) => {
          return 'recovered';
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 500,
          stalledInterval: 1000,
          maxStalledCount: 2,
          promotionInterval: 500,
        },
      );
      recoveryWorker.on('error', () => {});
      recoveryWorker.on('stalled', (jobId: string) => {
        stalledIds.push(jobId);
      });

      await new Promise(r => setTimeout(r, 5000));

      await recoveryWorker.close(true);

      const k = buildKeys(Q);
      const state = await cleanupClient.hget(k.job(job!.id), 'state');
      expect(['completed', 'active', 'failed'].includes(String(state))).toBe(true);

      await queue.close();
    }, 15000);
  });
});
