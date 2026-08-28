/**
 * moveToWaitingChildren() with an empty deps set must not park forever.
 *
 * Lua ACK/XDELs, sets waiting-children, then only self-unblocks when
 * SCARD(deps) > 0. Zero deps never XADD back, so the job is stuck.
 *
 * Run: npx vitest run tests/waiting-children-empty.test.ts
 */
import { afterAll, beforeAll, expect, it } from 'vitest';
import { createCleanupClient, describeEachMode, flushQueue, waitFor } from './helpers/fixture';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');

describeEachMode('moveToWaitingChildren with zero deps', (CONNECTION) => {
  let cleanupClient: any;
  const queues: string[] = [];

  function uniqueQueue(prefix: string): string {
    const name = `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    queues.push(name);
    return name;
  }

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await Promise.all(queues.map((q) => flushQueue(cleanupClient, q).catch(() => {})));
    cleanupClient.close();
  });

  it('requeues a job that waits on children before any child exists', async () => {
    const Q = uniqueQueue('wtc-empty');
    const queue = new Queue(Q, { connection: CONNECTION });
    const job = await queue.add('parent', { n: 1 });

    const parked = new Set<string>();
    const worker = new Worker(
      Q,
      async (active) => {
        if (!parked.has(active.id)) {
          parked.add(active.id);
          await active.moveToWaitingChildren();
        }
        return 'ok';
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 50, stalledInterval: 60_000 },
    );
    worker.on('error', () => {});

    try {
      await waitFor(
        async () => {
          const state = await job.getState();
          return state === 'completed' || state === 'failed' || state === 'waiting-children';
        },
        4000,
        50,
      );
      expect(await job.getState()).toBe('completed');
    } finally {
      await worker.close(true);
      await queue.close();
    }
  }, 15000);
});
