/**
 * Integration tests for Broadcast/BroadcastWorker (pub/sub fan-out).
 * Requires: valkey-server running on localhost:6379 and cluster on :7000-7005
 */
import { it, expect, beforeAll, afterAll } from 'vitest';

const { Broadcast } = require('../dist/broadcast') as typeof import('../src/broadcast');
const { BroadcastWorker } = require('../dist/broadcast-worker') as typeof import('../src/broadcast-worker');

import { describeEachMode, createCleanupClient, flushQueue, waitFor } from './helpers/fixture';

describeEachMode('Broadcast fan-out', (CONNECTION) => {
  const Q = 'test-broadcast-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('delivers one message to multiple subscribers', async () => {
    const broadcast = new Broadcast(Q, { connection: CONNECTION });
    const received = { sub1: [], sub2: [], sub3: [] } as Record<string, any[]>;

    const worker1 = new BroadcastWorker(
      Q,
      async (job) => {
        received.sub1.push(job.data);
      },
      { connection: CONNECTION, subscription: 'subscriber-1', blockTimeout: 500 },
    );

    const worker2 = new BroadcastWorker(
      Q,
      async (job) => {
        received.sub2.push(job.data);
      },
      { connection: CONNECTION, subscription: 'subscriber-2', blockTimeout: 500 },
    );

    const worker3 = new BroadcastWorker(
      Q,
      async (job) => {
        received.sub3.push(job.data);
      },
      { connection: CONNECTION, subscription: 'subscriber-3', blockTimeout: 500 },
    );

    await Promise.all([worker1.waitUntilReady(), worker2.waitUntilReady(), worker3.waitUntilReady()]);

    await broadcast.publish('message', { event: 'test', seq: 1 });

    await waitFor(() => {
      return received.sub1.length === 1 && received.sub2.length === 1 && received.sub3.length === 1;
    });

    // All 3 subscribers received the same message
    expect(received.sub1[0]).toEqual({ event: 'test', seq: 1 });
    expect(received.sub2[0]).toEqual({ event: 'test', seq: 1 });
    expect(received.sub3[0]).toEqual({ event: 'test', seq: 1 });

    await worker1.close(true);
    await worker2.close(true);
    await worker3.close(true);
    await broadcast.close();
  });

  it('late subscriber with startFrom="$" only receives new messages', async () => {
    const broadcast = new Broadcast(Q + '-late', { connection: CONNECTION });

    // Publish 2 messages before subscriber joins
    await broadcast.publish('message', { seq: 1 });
    await broadcast.publish('message', { seq: 2 });

    await new Promise((r) => setTimeout(r, 200));

    const received: any[] = [];
    const lateWorker = new BroadcastWorker(
      Q + '-late',
      async (job) => {
        received.push(job.data);
      },
      { connection: CONNECTION, subscription: 'late-sub', startFrom: '$', blockTimeout: 500 },
    );

    await lateWorker.waitUntilReady();

    // Publish a new message
    await broadcast.publish('message', { seq: 3 });

    await waitFor(() => received.length === 1, 5000);

    // Only the new message after subscription
    expect(received[0]).toEqual({ seq: 3 });
    expect(received.length).toBe(1);

    await lateWorker.close(true);
    await broadcast.close();
    await flushQueue(cleanupClient, Q + '-late');
  });

  it('late subscriber with startFrom="0-0" receives all history', async () => {
    const broadcast = new Broadcast(Q + '-backfill', { connection: CONNECTION });

    // Publish 2 messages
    await broadcast.publish('message', { seq: 1 });
    await broadcast.publish('message', { seq: 2 });

    await new Promise((r) => setTimeout(r, 200));

    const received: any[] = [];
    const backfillWorker = new BroadcastWorker(
      Q + '-backfill',
      async (job) => {
        received.push(job.data);
      },
      { connection: CONNECTION, subscription: 'backfill-sub', startFrom: '0-0', blockTimeout: 500 },
    );

    await waitFor(() => received.length === 2, 5000);

    // Got full history
    const seqs = received.map((m) => m.seq);
    expect(seqs).toEqual([1, 2]);

    await backfillWorker.close(true);
    await broadcast.close();
    await flushQueue(cleanupClient, Q + '-backfill');
  });

  it('one subscriber failure does not affect others', async () => {
    const broadcast = new Broadcast(Q + '-independent', { connection: CONNECTION });
    const received = { success1: [], success2: [], failed: [] } as Record<string, any[]>;

    const successWorker1 = new BroadcastWorker(
      Q + '-independent',
      async (job) => {
        received.success1.push(job.data);
      },
      { connection: CONNECTION, subscription: 'success-1', blockTimeout: 500 },
    );

    const successWorker2 = new BroadcastWorker(
      Q + '-independent',
      async (job) => {
        received.success2.push(job.data);
      },
      { connection: CONNECTION, subscription: 'success-2', blockTimeout: 500 },
    );

    const failingWorker = new BroadcastWorker(
      Q + '-independent',
      async (job) => {
        received.failed.push(job.data);
        throw new Error('intentional failure');
      },
      { connection: CONNECTION, subscription: 'failing', blockTimeout: 500, attempts: 1 },
    );

    await Promise.all([
      successWorker1.waitUntilReady(),
      successWorker2.waitUntilReady(),
      failingWorker.waitUntilReady(),
    ]);

    await broadcast.publish('message', { event: 'test-failure' });

    await waitFor(() => {
      return received.success1.length === 1 && received.success2.length === 1 && received.failed.length === 1;
    });

    // All subscribers received the message
    expect(received.success1[0]).toEqual({ event: 'test-failure' });
    expect(received.success2[0]).toEqual({ event: 'test-failure' });
    expect(received.failed[0]).toEqual({ event: 'test-failure' });

    await successWorker1.close(true);
    await successWorker2.close(true);
    await failingWorker.close(true);
    await broadcast.close();
    await flushQueue(cleanupClient, Q + '-independent');
  });

  it('respects maxMessages retention limit', async () => {
    const broadcast = new Broadcast(Q + '-retention', { connection: CONNECTION, maxMessages: 5 });

    // Publish 10 messages
    for (let i = 1; i <= 10; i++) {
      await broadcast.publish('message', { seq: i });
    }

    // Exact XTRIM is used; stream should be trimmed to maxMessages (5)
    const len = await cleanupClient.xlen(`glide:{${Q}-retention}:stream`);
    expect(Number(len)).toBeLessThanOrEqual(5);

    await broadcast.close();
    await flushQueue(cleanupClient, Q + '-retention');
  });

  it('throws error if subscription is missing', () => {
    expect(() => {
      new BroadcastWorker(Q + '-invalid', async () => {}, { connection: CONNECTION, subscription: '' } as any);
    }).toThrow(/subscription/);
  });
});

describeEachMode('Broadcast with scheduler integration', (CONNECTION) => {
  const Q = 'test-broadcast-scheduler-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('scheduled messages delivered to all subscribers', async () => {
    const broadcast = new Broadcast(Q, { connection: CONNECTION });
    const received = { sub1: [], sub2: [] } as Record<string, any[]>;

    const worker1 = new BroadcastWorker(
      Q,
      async (job) => {
        received.sub1.push(job.data);
      },
      { connection: CONNECTION, subscription: 'sched-sub-1', blockTimeout: 500, promotionInterval: 500 },
    );

    const worker2 = new BroadcastWorker(
      Q,
      async (job) => {
        received.sub2.push(job.data);
      },
      { connection: CONNECTION, subscription: 'sched-sub-2', blockTimeout: 500, promotionInterval: 500 },
    );

    await Promise.all([worker1.waitUntilReady(), worker2.waitUntilReady()]);

    // Schedule a message with 2 second delay
    await broadcast.publish('message', { event: 'scheduled' }, { delay: 2000 });

    // Should not receive immediately
    await new Promise((r) => setTimeout(r, 500));
    expect(received.sub1.length).toBe(0);
    expect(received.sub2.length).toBe(0);

    // Wait for scheduled promotion (increased timeout)
    await waitFor(() => received.sub1.length === 1 && received.sub2.length === 1, 5000);

    expect(received.sub1[0]).toEqual({ event: 'scheduled' });
    expect(received.sub2[0]).toEqual({ event: 'scheduled' });

    await worker1.close(true);
    await worker2.close(true);
    await broadcast.close();
  });
});

describeEachMode('Broadcast with dedup integration', (CONNECTION) => {
  const Q = 'test-broadcast-dedup-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('deduplicated messages still fanout to all subscribers', async () => {
    const broadcast = new Broadcast(Q, { connection: CONNECTION });

    const received = { sub1: [], sub2: [] } as Record<string, any[]>;

    const worker1 = new BroadcastWorker(
      Q,
      async (job) => {
        received.sub1.push(job.data);
      },
      { connection: CONNECTION, subscription: 'dedup-sub-1', blockTimeout: 500 },
    );

    const worker2 = new BroadcastWorker(
      Q,
      async (job) => {
        received.sub2.push(job.data);
      },
      { connection: CONNECTION, subscription: 'dedup-sub-2', blockTimeout: 500 },
    );

    await Promise.all([worker1.waitUntilReady(), worker2.waitUntilReady()]);

    // Publish with dedup ID - dedup is configured via JobOptions
    const id1 = await broadcast.publish(
      'message',
      { event: 'deduped' },
      { deduplication: { id: 'unique-1', mode: 'simple', ttl: 5000 } },
    );
    expect(id1).not.toBeNull();

    // Duplicate - should be skipped
    const id2 = await broadcast.publish(
      'message',
      { event: 'deduped' },
      { deduplication: { id: 'unique-1', mode: 'simple', ttl: 5000 } },
    );
    expect(id2).toBeNull();

    await waitFor(() => received.sub1.length === 1 && received.sub2.length === 1, 3000);

    // Both subscribers got the first message, duplicate was skipped
    expect(received.sub1.length).toBe(1);
    expect(received.sub2.length).toBe(1);
    expect(received.sub1[0]).toEqual({ event: 'deduped' });
    expect(received.sub2[0]).toEqual({ event: 'deduped' });

    await worker1.close(true);
    await worker2.close(true);
    await broadcast.close();
  });

  it('pause() stops message flow to subscribers', async () => {
    const qName = Q + '-pause';
    const broadcast = new Broadcast(qName, { connection: CONNECTION });
    const received: any[] = [];

    const worker = new BroadcastWorker(
      qName,
      async (job: any) => {
        received.push(job.data);
      },
      { connection: CONNECTION, subscription: 'sub-pause', blockTimeout: 500 },
    );

    // Wait for worker to be fully ready (poll loop running)
    await worker.waitUntilReady();

    // Pause the worker - wait for it to finish any in-flight poll cycle
    await worker.pause(true);
    // Allow the blocking XREADGROUP to unblock naturally (blockTimeout=500ms)
    await new Promise<void>((resolve) => setTimeout(resolve, 600));

    // Publish a message while paused
    await broadcast.publish('message', { msg: 'while-paused' });

    // Wait to confirm message is NOT processed while paused
    await new Promise<void>((resolve) => setTimeout(resolve, 500));
    expect(received).toHaveLength(0);

    await worker.close(true);
    await broadcast.close();
  }, 10000);

  it('resume() restarts message flow after pause', async () => {
    const qName = Q + '-resume';
    const broadcast = new Broadcast(qName, { connection: CONNECTION });
    const received: any[] = [];

    const worker = new BroadcastWorker(
      qName,
      async (job: any) => {
        received.push(job.data);
      },
      { connection: CONNECTION, subscription: 'sub-resume', startFrom: '0' },
    );

    // Wait for worker to be ready
    await new Promise<void>((resolve) => setTimeout(resolve, 500));

    // Pause and publish
    await worker.pause(true);
    await broadcast.publish('message', { msg: 'before-resume' });

    // Resume - worker should pick up pending messages
    await worker.resume();

    // Wait for message to be processed
    await waitFor(() => received.length >= 1, 8000);
    expect(received.length).toBeGreaterThanOrEqual(1);

    await worker.close(true);
    await broadcast.close();
  }, 15000);
});
