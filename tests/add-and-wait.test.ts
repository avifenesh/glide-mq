import { it, expect, beforeAll, afterAll } from 'vitest';
import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');

describeEachMode('Queue.addAndWait', (CONNECTION) => {
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    cleanupClient.close();
  });

  it('returns the worker result even when the job completes immediately', async () => {
    const qName = `test-add-and-wait-fast-${Date.now()}`;
    const queue = new Queue(qName, { connection: CONNECTION });
    const worker = new Worker(
      qName,
      async (job: any) => {
        return { echoed: job.data.value };
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 100 },
    );
    worker.on('error', () => {});

    await worker.waitUntilReady();
    const result = await queue.addAndWait('fast', { value: 42 }, { waitTimeout: 10000 });

    expect(result).toEqual({ echoed: 42 });

    await worker.close(true);
    await queue.close();
    await flushQueue(cleanupClient, qName);
  }, 15000);

  it('works correctly when the queue already has historical events', async () => {
    const qName = `test-add-and-wait-history-${Date.now()}`;
    const queue = new Queue(qName, { connection: CONNECTION });
    const worker = new Worker(
      qName,
      async (job: any) => {
        return { echoed: job.data.value };
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 100 },
    );
    worker.on('error', () => {});

    await worker.waitUntilReady();
    expect(await queue.addAndWait('first', { value: 1 }, { waitTimeout: 10000 })).toEqual({ echoed: 1 });
    expect(await queue.addAndWait('second', { value: 2 }, { waitTimeout: 10000 })).toEqual({ echoed: 2 });

    await worker.close(true);
    await queue.close();
    await flushQueue(cleanupClient, qName);
  }, 15000);

  it('rejects with the worker failure reason', async () => {
    const qName = `test-add-and-wait-fail-${Date.now()}`;
    const queue = new Queue(qName, { connection: CONNECTION });
    const worker = new Worker(
      qName,
      async () => {
        throw new Error('boom');
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 100 },
    );
    worker.on('error', () => {});
    worker.on('failed', () => {});

    await worker.waitUntilReady();
    await expect(queue.addAndWait('fail', { value: 1 }, { waitTimeout: 10000 })).rejects.toThrow('boom');

    await worker.close(true);
    await queue.close();
    await flushQueue(cleanupClient, qName);
  }, 15000);

  it('rejects when the wait timeout elapses before completion', async () => {
    const qName = `test-add-and-wait-timeout-${Date.now()}`;
    const queue = new Queue(qName, { connection: CONNECTION });

    await expect(queue.addAndWait('timeout', { value: 1 }, { waitTimeout: 200 })).rejects.toThrow(
      /did not finish within 200ms/,
    );

    await queue.close();
    await flushQueue(cleanupClient, qName);
  }, 15000);

  it('rejects when add() is deduplicated and returns null', async () => {
    const qName = `test-add-and-wait-dedup-${Date.now()}`;
    const queue = new Queue(qName, { connection: CONNECTION });

    await queue.add('seed', { value: 1 }, { deduplication: { id: 'same-id', mode: 'simple' } });
    await expect(
      queue.addAndWait('seed', { value: 2 }, { waitTimeout: 1000, deduplication: { id: 'same-id', mode: 'simple' } }),
    ).rejects.toThrow(/deduplicated\/skipped/);

    await queue.close();
    await flushQueue(cleanupClient, qName);
  }, 15000);

  it('rejects in-flight addAndWait calls when the queue closes', async () => {
    const qName = `test-add-and-wait-close-${Date.now()}`;
    const queue = new Queue(qName, { connection: CONNECTION });

    const pending = queue.addAndWait('close-me', { value: 1 }, { waitTimeout: 10000 });
    await new Promise((resolve) => setTimeout(resolve, 100));
    await queue.close();

    await expect(pending).rejects.toThrow(/Queue is closing/);
    await flushQueue(cleanupClient, qName);
  }, 15000);
});
