/**
 * Reconnect must not destroy the command client under in-flight jobs.
 *
 * concurrency>1 dispatches processJob then keeps polling. A later pollOnce throw
 * runs reconnectAndResume, which closes the owned command client. Heartbeats
 * captured that client and fail silently, so lastActive freezes and stalled
 * reclaim redelivers the job while the original processor is still running.
 *
 * Run: npx vitest run tests/reconnect-live-client.test.ts
 */
import { afterAll, beforeAll, expect, it } from 'vitest';
import { createCleanupClient, describeEachMode, flushQueue, waitFor } from './helpers/fixture';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');

describeEachMode('reconnect with in-flight jobs', (CONNECTION) => {
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

  it('does not redeliver an in-flight job after poll reconnect', async () => {
    const Q = uniqueQueue('reconnect-live');
    const queue = new Queue(Q, { connection: CONNECTION });
    await queue.add('task', { n: 1 });

    let release!: () => void;
    const hold = new Promise<void>((resolve) => {
      release = resolve;
    });
    let started = 0;

    const worker = new Worker(
      Q,
      async () => {
        started++;
        await hold;
        return 'ok';
      },
      {
        connection: CONNECTION,
        concurrency: 2,
        blockTimeout: 50,
        lockDuration: 1000,
        stalledInterval: 400,
      },
    );
    worker.on('error', () => {});

    await waitFor(() => started === 1);

    const origPollOnce = (worker as any).pollOnce.bind(worker);
    let failNext = true;
    (worker as any).pollOnce = async () => {
      if (failNext) {
        failNext = false;
        throw new Error('Connection lost');
      }
      return origPollOnce();
    };

    await waitFor(() => !failNext, 4000, 20);
    await new Promise((r) => setTimeout(r, 2500));

    expect(started).toBe(1);

    release();
    await waitFor(() => started >= 1 && (worker as any).activeCount === 0, 4000, 50);
    expect(started).toBe(1);

    await worker.close(true);
    await queue.close();
  }, 15000);
});
