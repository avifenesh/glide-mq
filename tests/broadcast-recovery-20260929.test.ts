/**
 * Broadcast stalled recovery and retention regressions (2026-09-29).
 *
 * Run: npx vitest run tests/broadcast-recovery-20260929.test.ts
 */
import { afterAll, beforeAll, expect, it } from 'vitest';
import { createCleanupClient, describeEachMode, flushQueue, waitFor } from './helpers/fixture';

const { Broadcast } = require('../dist/broadcast') as typeof import('../src/broadcast');
const { BroadcastWorker } = require('../dist/broadcast-worker') as typeof import('../src/broadcast-worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describeEachMode('Broadcast recovery 2026-09-29', (CONNECTION) => {
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

  const recoveryOpts = { stalledInterval: 400, lockDuration: 400, maxStalledCount: 1, blockTimeout: 200 };

  it('R2-1: a subscription re-processes a message whose worker was killed mid-job', async () => {
    const Q = uniqueQueue('bcr-kill');
    const k = buildKeys(Q);
    const broadcast = new Broadcast(Q, { connection: CONNECTION });
    const runs = { a1: 0, a2: 0, b: 0 };
    const failed: string[] = [];

    const a1 = new BroadcastWorker(
      Q,
      async (job) => {
        runs.a1++;
        await new Promise<void>((resolve) => job.abortSignal?.addEventListener('abort', () => resolve()));
        throw new Error('killed');
      },
      { connection: CONNECTION, subscription: 'a', ...recoveryOpts },
    );
    const b = new BroadcastWorker(
      Q,
      async () => {
        runs.b++;
        return 'b';
      },
      { connection: CONNECTION, subscription: 'b', ...recoveryOpts },
    );
    let a2: InstanceType<typeof BroadcastWorker> | null = null;
    try {
      await Promise.all([a1.waitUntilReady(), b.waitUntilReady()]);
      const id = await broadcast.publish('evt', { n: 1 });
      await waitFor(() => runs.a1 === 1 && runs.b === 1, 5000);
      await a1.close(true);

      a2 = new BroadcastWorker(
        Q,
        async () => {
          runs.a2++;
          return 'a2';
        },
        { connection: CONNECTION, subscription: 'a', ...recoveryOpts },
      );
      a2.on('failed', (job: any) => failed.push(job.id));
      await a2.waitUntilReady();
      await waitFor(() => runs.a2 === 1, 8000);
      // Several more reclaim cycles must neither re-run nor fail it.
      await sleep(1500);
      expect(runs).toEqual({ a1: 1, a2: 1, b: 1 });
      expect(failed).toEqual([]);
      expect(await cleanupClient.zscore(k.failed, id!)).toBeNull();
      const pending = (await cleanupClient.xpending(k.stream, 'a')) as any[];
      expect(Number(pending[0])).toBe(0);
    } finally {
      await a1.close(true);
      await b.close(true);
      if (a2) await a2.close(true);
      await broadcast.close();
    }
  });

  it('R2-1: an entry delivered to a closing worker is run by the next worker of the subscription', async () => {
    const Q = uniqueQueue('bcr-close');
    const k = buildKeys(Q);
    const broadcast = new Broadcast(Q, { connection: CONNECTION });
    const runs = { a1: 0, a2: 0 };
    const a1 = new BroadcastWorker(
      Q,
      async () => {
        runs.a1++;
      },
      { connection: CONNECTION, subscription: 'a', ...recoveryOpts, blockTimeout: 1500 },
    );
    let a2: InstanceType<typeof BroadcastWorker> | null = null;
    try {
      await a1.waitUntilReady();
      await sleep(300);
      // close() waits for the in-flight XREADGROUP BLOCK; the entry published
      // meanwhile is delivered to the closing worker, which does not run it.
      const closing = a1.close();
      await broadcast.publish('evt', { n: 1 });
      await closing;
      expect(runs.a1).toBe(0);
      const pending = (await cleanupClient.xpending(k.stream, 'a')) as any[];
      expect(Number(pending[0])).toBe(1);

      a2 = new BroadcastWorker(
        Q,
        async () => {
          runs.a2++;
        },
        { connection: CONNECTION, subscription: 'a', ...recoveryOpts },
      );
      await waitFor(() => runs.a2 === 1, 8000);
      await sleep(1000);
      expect(runs).toEqual({ a1: 0, a2: 1 });
      const after = (await cleanupClient.xpending(k.stream, 'a')) as any[];
      expect(Number(after[0])).toBe(0);
    } finally {
      await a1.close(true);
      if (a2) await a2.close(true);
      await broadcast.close();
    }
  });

  it('R2-1: a stall in one subscription does not count against another', async () => {
    const Q = uniqueQueue('bcr-count');
    const k = buildKeys(Q);
    const broadcast = new Broadcast(Q, { connection: CONNECTION });
    try {
      const id = await broadcast.publish('evt', { n: 1 });
      // Two subscriptions each claimed the entry and died without activating
      // it (the entry was delivered while their workers were closing).
      await cleanupClient.xgroupCreate(k.stream, 'a', '0');
      await cleanupClient.xgroupCreate(k.stream, 'b', '0');
      await cleanupClient.xreadgroup('a', 'dead-a', { [k.stream]: '>' }, { count: 1 });
      await cleanupClient.xreadgroup('b', 'dead-b', { [k.stream]: '>' }, { count: 1 });
      await sleep(500);

      const runs = { a: 0, b: 0 };
      const failed: string[] = [];
      const wa = new BroadcastWorker(
        Q,
        async () => {
          runs.a++;
        },
        { connection: CONNECTION, subscription: 'a', ...recoveryOpts },
      );
      const wb = new BroadcastWorker(
        Q,
        async () => {
          runs.b++;
        },
        { connection: CONNECTION, subscription: 'b', ...recoveryOpts },
      );
      wa.on('failed', (job: any) => failed.push(job.id));
      wb.on('failed', (job: any) => failed.push(job.id));
      try {
        await waitFor(() => runs.a === 1 && runs.b === 1, 8000);
        await sleep(1000);
        expect(runs).toEqual({ a: 1, b: 1 });
        expect(failed).toEqual([]);
        expect(await cleanupClient.zscore(k.failed, id!)).toBeNull();
      } finally {
        await wa.close(true);
        await wb.close(true);
      }
    } finally {
      await broadcast.close();
    }
  });

  it('R2-9: maxMessages trim deletes the job hashes, sub hashes and ZSET members of trimmed messages', async () => {
    const Q = uniqueQueue('bcr-trim');
    const k = buildKeys(Q);
    const broadcast = new Broadcast(Q, { connection: CONNECTION, maxMessages: 3 });
    const done: string[] = [];
    let first = true;
    const worker = new BroadcastWorker(
      Q,
      async () => {
        // One failed attempt creates the per-subscription hash.
        if (first) {
          first = false;
          throw new Error('once');
        }
      },
      { connection: CONNECTION, subscription: 'a', blockTimeout: 200, promotionInterval: 50 },
    );
    worker.on('completed', (job: any) => done.push(job.id));
    try {
      await worker.waitUntilReady();
      const ids: string[] = [];
      for (let i = 0; i < 8; i++) {
        const id = await broadcast.publish('evt', { i }, { attempts: 2, backoff: { type: 'fixed', delay: 10 } });
        ids.push(id!);
        await waitFor(() => done.includes(id!), 5000);
      }
      expect(await cleanupClient.xlen(k.stream)).toBe(3);
      const trimmed = ids.slice(0, 5);
      for (const id of trimmed) {
        expect(await cleanupClient.exists([k.job(id)])).toBe(0);
        expect(await cleanupClient.zscore(k.completed, id)).toBeNull();
      }
      expect(await cleanupClient.exists([`${k.job(ids[0])}:sub:a`])).toBe(0);
      for (const id of ids.slice(5)) {
        expect(await cleanupClient.exists([k.job(id)])).toBe(1);
      }
    } finally {
      await worker.close(true);
      await broadcast.close();
    }
  });

  it('R2-9: a trimmed message still claimed or scheduled for retry keeps its hash until settled', async () => {
    const Q = uniqueQueue('bcr-trim-held');
    const k = buildKeys(Q);
    const broadcast = new Broadcast(Q, { connection: CONNECTION, maxMessages: 1 });
    try {
      const held = await broadcast.publish('evt', { n: 1 });
      await cleanupClient.xgroupCreate(k.stream, 'a', '0');
      const read = await cleanupClient.xreadgroup('a', 'c1', { [k.stream]: '>' }, { count: 1 });
      const entryId = Object.keys(read[0].value)[0];
      const retried = await broadcast.publish('evt', { n: 2 });
      // A subscription scheduled a retry for the second message.
      await cleanupClient.zadd(k.scheduled, [{ element: `${retried}||a`, score: Date.now() + 3600000 }]);
      await broadcast.publish('evt', { n: 3 });

      expect(await cleanupClient.xlen(k.stream)).toBe(1);
      // The claim of the first message and the scheduled retry keep their hashes.
      expect(await cleanupClient.exists([k.job(held!)])).toBe(1);
      expect(await cleanupClient.exists([k.job(retried!)])).toBe(1);

      await cleanupClient.xack(k.stream, 'a', [entryId]);
      await broadcast.publish('evt', { n: 4 });
      expect(await cleanupClient.exists([k.job(held!)])).toBe(0);
      expect(await cleanupClient.exists([k.job(retried!)])).toBe(1);
    } finally {
      await broadcast.close();
    }
  });
});
