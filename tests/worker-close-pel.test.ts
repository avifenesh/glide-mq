/**
 * close() must not strand claims in the closing consumer's PEL. Entries
 * delivered to an in-flight XREADGROUP while close() runs are handed back
 * so another worker picks them up without waiting for stalled recovery.
 */
import { it, expect, afterAll } from 'vitest';
import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

describeEachMode('Worker close() and in-flight XREADGROUP', (CONNECTION) => {
  const names: string[] = [];
  afterAll(async () => {
    const cleanup = await createCleanupClient(CONNECTION);
    for (const n of names) await flushQueue(cleanup, n);
    cleanup.close();
  });

  for (const concurrency of [1, 3]) {
    for (const delayMs of [0, 1, 3]) {
      it(`c=${concurrency} add ${delayMs}ms after close(): no entry left in the closed consumer PEL`, async () => {
        const name = `close-pel-${concurrency}-${delayMs}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
        names.push(name);
        const queue = new Queue(name, { connection: CONNECTION });
        const worker = new Worker(name, async () => 'ok', {
          connection: CONNECTION,
          concurrency,
          blockTimeout: 10000,
          stalledInterval: 60000,
        });
        await worker.waitUntilReady();
        // Let the poll loop enter XREADGROUP BLOCK.
        await new Promise((r) => setTimeout(r, 200));

        const closing = worker.close();
        if (delayMs > 0) await new Promise((r) => setTimeout(r, delayMs));
        const jobs = await Promise.all([queue.add('a', { i: 1 }), queue.add('b', { i: 2 })]);
        const started = Date.now();
        await closing;
        const closeMs = Date.now() - started;

        const client = await createCleanupClient(CONNECTION);
        try {
          const k = buildKeys(name);
          const pending = await client.xpending(k.stream, 'workers');
          const consumers = (pending[3] ?? []).map(([c, n]) => [String(c), Number(n)]);
          expect(consumers.filter(([c]) => c === (worker as any).consumerId)).toEqual([]);
          for (const job of jobs) {
            const state = await job!.getState();
            expect(['waiting', 'completed']).toContain(state);
          }
          expect(closeMs).toBeLessThan(3000);
        } finally {
          client.close();
          await queue.close();
        }
      });
    }
  }

  it('idle close(): a job added after close() resolves is not claimed by the closed consumer', async () => {
    const name = `close-pel-idle-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    names.push(name);
    const queue = new Queue(name, { connection: CONNECTION });
    await queue.getJobCounts();
    const worker = new Worker(name, async () => 'ok', {
      connection: CONNECTION,
      concurrency: 2,
      blockTimeout: 1500,
      stalledInterval: 60000,
    });
    await worker.waitUntilReady();
    await new Promise((r) => setTimeout(r, 200));

    const started = Date.now();
    await worker.close();
    // Bounded by the in-flight read: at most blockTimeout plus slack.
    expect(Date.now() - started).toBeLessThan(2500);

    const job = await queue.add('late', {});
    await new Promise((r) => setTimeout(r, 200));
    const client = await createCleanupClient(CONNECTION);
    try {
      const pending = await client.xpending(buildKeys(name).stream, 'workers');
      expect(Number(pending[0])).toBe(0);
      expect(await job!.getState()).toBe('waiting');
    } finally {
      client.close();
      await queue.close();
    }
  });
});
