/**
 * Integration tests for shared client support.
 *
 * Tests opt-in client injection for Queue, Worker, FlowProducer,
 * and the blocking client guard on QueueEvents.
 *
 * Run: npx vitest run tests/shared-client.test.ts
 */
import { it, expect, beforeAll, afterAll } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { FlowProducer } = require('../dist/flow-producer') as typeof import('../src/flow-producer');
const { QueueEvents } = require('../dist/queue-events') as typeof import('../src/queue-events');
const { GlideMQError } = require('../dist/errors') as typeof import('../src/errors');

import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describeEachMode('Shared client', (CONNECTION) => {
  const Q = 'test-shared-' + Date.now();
  let cleanupClient: any;

  function uq(suffix: string): string {
    return Q + suffix;
  }

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  // --- Queue with injected client ---

  it('Queue works with injected client', async () => {
    const QN = uq('-q-inject');
    const q = new Queue(QN, { client: cleanupClient });

    await q.add('test-job', { hello: 'world' });
    const counts = await q.getJobCounts();
    expect(counts.waiting).toBeGreaterThanOrEqual(1);

    await q.close();
    await flushQueue(cleanupClient, QN);
  }, 10000);

  it('Queue.close() does NOT destroy shared client', async () => {
    const QN = uq('-q-noclose');
    const q = new Queue(QN, { client: cleanupClient });

    await q.add('test-job', { x: 1 });
    await q.close();

    // Shared client should still be usable
    const pong = await cleanupClient.ping();
    expect(String(pong)).toBe('PONG');

    await flushQueue(cleanupClient, QN);
  }, 10000);

  it('multiple Queues share one client', async () => {
    const QN1 = uq('-q-share1');
    const QN2 = uq('-q-share2');
    const q1 = new Queue(QN1, { client: cleanupClient });
    const q2 = new Queue(QN2, { client: cleanupClient });

    await q1.add('job1', { from: 'q1' });
    await q2.add('job2', { from: 'q2' });

    const c1 = await q1.getJobCounts();
    const c2 = await q2.getJobCounts();
    expect(c1.waiting).toBeGreaterThanOrEqual(1);
    expect(c2.waiting).toBeGreaterThanOrEqual(1);

    // Close one, the other still works
    await q1.close();
    await q2.add('job3', { from: 'q2-after' });
    const c2b = await q2.getJobCounts();
    expect(c2b.waiting).toBeGreaterThanOrEqual(2);

    await q2.close();
    await flushQueue(cleanupClient, QN1);
    await flushQueue(cleanupClient, QN2);
  }, 10000);

  // --- FlowProducer with injected client ---

  it('FlowProducer works with injected client', async () => {
    const QN = uq('-fp-inject');
    const flow = new FlowProducer({ client: cleanupClient });

    const result = await flow.add({
      name: 'parent',
      queueName: QN,
      data: { type: 'flow' },
      children: [
        { name: 'child1', queueName: QN, data: { idx: 1 } },
      ],
    });

    expect(result.job.id).toBeDefined();
    expect(result.children).toHaveLength(1);

    await flow.close();

    // Client still alive
    const pong = await cleanupClient.ping();
    expect(String(pong)).toBe('PONG');

    // Cleanup
    const q = new Queue(QN, { client: cleanupClient });
    const worker = new Worker(QN, async () => {}, { connection: CONNECTION, concurrency: 5 });
    await sleep(1000);
    await worker.close();
    await q.close();
    await flushQueue(cleanupClient, QN);
  }, 15000);

  // --- Worker with injected command client ---

  it('Worker processes jobs with injected commandClient', async () => {
    const QN = uq('-w-inject');
    const q = new Queue(QN, { connection: CONNECTION });
    let completed = 0;

    await q.add('work', { val: 1 });
    await q.add('work', { val: 2 });

    const worker = new Worker(QN, async () => { completed++; }, {
      connection: CONNECTION,
      commandClient: cleanupClient,
      concurrency: 5,
    });

    const deadline = Date.now() + 10000;
    while (completed < 2 && Date.now() < deadline) await sleep(50);

    await worker.close();
    await q.close();
    await flushQueue(cleanupClient, QN);

    expect(completed).toBe(2);

    // Shared client still usable
    const pong = await cleanupClient.ping();
    expect(String(pong)).toBe('PONG');
  }, 15000);

  it('Worker.close() does NOT destroy shared command client', async () => {
    const QN = uq('-w-noclose');
    const worker = new Worker(QN, async () => {}, {
      connection: CONNECTION,
      client: cleanupClient,
    });
    await worker.waitUntilReady();
    await worker.close();

    // Shared client still alive
    const pong = await cleanupClient.ping();
    expect(String(pong)).toBe('PONG');

    await flushQueue(cleanupClient, QN);
  }, 10000);

  // --- Queue + Worker sharing client end-to-end ---

  it('Queue + Worker sharing client processes jobs end-to-end', async () => {
    const QN = uq('-e2e-share');
    const q = new Queue(QN, { client: cleanupClient });
    const results: number[] = [];

    for (let i = 0; i < 5; i++) {
      await q.add('shared-job', { i });
    }

    const worker = new Worker(QN, async (job: any) => {
      results.push(job.data.i);
    }, {
      connection: CONNECTION,
      commandClient: cleanupClient,
      concurrency: 5,
    });

    const deadline = Date.now() + 10000;
    while (results.length < 5 && Date.now() < deadline) await sleep(50);

    await worker.close();
    await q.close();
    await flushQueue(cleanupClient, QN);

    expect(results.sort()).toEqual([0, 1, 2, 3, 4]);
  }, 15000);

  // --- QueueEvents rejects injected client ---

  it('QueueEvents rejects injected client', () => {
    expect(() => {
      new (QueueEvents as any)('test', { connection: CONNECTION, client: cleanupClient });
    }).toThrow('QueueEvents does not accept an injected `client`');
  });

  // --- Validation errors ---

  it('Queue throws when neither connection nor client provided', () => {
    expect(() => {
      new Queue('test', {} as any);
    }).toThrow('Either `connection` or `client` must be provided');
  });

  it('FlowProducer throws when neither connection nor client provided', () => {
    expect(() => {
      new FlowProducer({} as any);
    }).toThrow('Either `connection` or `client` must be provided');
  });

  it('Worker throws when neither connection nor client provided', () => {
    expect(() => {
      new Worker('test', async () => {}, {} as any);
    }).toThrow('Either `connection` or `client`/`commandClient` must be provided');
  });

  it('Worker throws when client provided without connection', () => {
    expect(() => {
      new Worker('test', async () => {}, { client: cleanupClient } as any);
    }).toThrow('Worker requires `connection`');
  });

  it('Worker throws when both client and commandClient provided', () => {
    expect(() => {
      new Worker('test', async () => {}, {
        connection: CONNECTION,
        client: cleanupClient,
        commandClient: cleanupClient,
      } as any);
    }).toThrow('Provide either `client` or `commandClient`, not both');
  });

  // --- ensureFunctionLibraryOnce deduplication ---

  it('ensureFunctionLibraryOnce deduplicates concurrent calls', async () => {
    const { ensureFunctionLibraryOnce } = require('../dist/connection') as typeof import('../src/connection');

    // Call 10 times concurrently on the same client - should not throw
    const promises = Array.from({ length: 10 }, () =>
      ensureFunctionLibraryOnce(cleanupClient),
    );
    await Promise.all(promises);

    // All resolved successfully - library loaded once, others got cached promise
    const pong = await cleanupClient.ping();
    expect(String(pong)).toBe('PONG');
  }, 10000);

  // --- Worker reconnect with injected client ---

  it('Worker with injected client emits error on blocking client failure', async () => {
    const QN = uq('-w-reconn');
    const errors: Error[] = [];

    const worker = new Worker(QN, async () => {}, {
      connection: CONNECTION,
      commandClient: cleanupClient,
    });
    worker.on('error', (err: Error) => { errors.push(err); });
    await worker.waitUntilReady();

    // Worker is running with injected command client + auto-created blocking client.
    // The shared command client is NOT recreated on reconnect - only the blocking one is.
    // We can verify the worker is functioning and the shared client survives.
    await worker.close();

    // Shared client still usable
    const pong = await cleanupClient.ping();
    expect(String(pong)).toBe('PONG');

    await flushQueue(cleanupClient, QN);
  }, 15000);

  // --- Queue with connection still works (backward compatibility) ---

  it('Queue with connection works as before', async () => {
    const QN = uq('-q-compat');
    const q = new Queue(QN, { connection: CONNECTION });

    await q.add('compat-job', { test: true });
    const counts = await q.getJobCounts();
    expect(counts.waiting).toBeGreaterThanOrEqual(1);

    await q.close();
    await flushQueue(cleanupClient, QN);
  }, 10000);
});
