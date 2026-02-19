/**
 * Integration tests for per-key ordering.
 *
 * Run: npx vitest run tests/ordering.test.ts
 */
import { it, expect, beforeAll, afterAll } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

function createRng(seed = 0x1234abcd) {
  let state = seed >>> 0;
  return () => {
    state = (state * 1664525 + 1013904223) >>> 0;
    return state / 0x100000000;
  };
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describeEachMode('Per-key ordering', (CONNECTION) => {
  const Q = 'test-ordering-' + Date.now();
  let queue: InstanceType<typeof Queue>;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
    queue = new Queue(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('processes jobs sequentially for each ordering key under concurrency', async () => {
    const rng = createRng(0x44aa99);
    const accountCount = 10;
    const totalJobs = 240;
    const createdByAccount = new Map<string, number>();
    const events: { accountId: string; expectedSeq: number; payload: number }[] = [];

    for (let i = 0; i < totalJobs; i += 1) {
      const accountId = `acct-${Math.floor(rng() * accountCount)}`;
      const next = (createdByAccount.get(accountId) || 0) + 1;
      createdByAccount.set(accountId, next);
      events.push({ accountId, expectedSeq: next, payload: i });
    }

    await queue.addBulk(
      events.map((e) => ({
        name: 'ordered',
        data: e,
        opts: {
          ordering: { key: e.accountId },
          // Add heterogeneity to amplify out-of-order risk without ordering controls.
          priority: Math.floor(rng() * 6),
        },
      })),
    );

    const seenByAccount = new Map<string, number>();
    let violations = 0;
    let processed = 0;
    let worker: InstanceType<typeof Worker> | null = null;

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 20000);
      worker = new Worker(
        Q,
        async (job: any) => {
          const accountId = String(job.data.accountId);
          const expectedSeq = Number(job.data.expectedSeq);
          const last = seenByAccount.get(accountId) || 0;
          if (expectedSeq !== last + 1) {
            violations += 1;
          }
          seenByAccount.set(accountId, Math.max(last, expectedSeq));
          processed += 1;
          if (processed >= totalJobs) {
            clearTimeout(timeout);
            resolve();
          }
          await sleep(2 + Math.floor(rng() * 9));
          return { ok: true };
        },
        { connection: CONNECTION, concurrency: 10, blockTimeout: 250, promotionInterval: 75 },
      );
      worker.on('error', () => {});
    });

    await done;
    if (worker) await worker.close(true);

    expect(violations).toBe(0);
    for (const [accountId, expectedFinal] of createdByAccount.entries()) {
      expect(seenByAccount.get(accountId)).toBe(expectedFinal);
    }
  }, 25000);

  it('deferActive does not recreate removed job hashes', async () => {
    const localQueueName = `test-ordering-defer-${Date.now()}`;
    const k = buildKeys(localQueueName);
    const missingJobId = '99999';
    const entryId = await cleanupClient.xadd(k.stream, ['jobId', missingJobId]);

    const result = await cleanupClient.fcall(
      'glidemq_deferActive',
      [k.stream, k.job(missingJobId)],
      [missingJobId, String(entryId), 'workers'],
    );

    expect(Number(result)).toBe(0);

    const hash = await cleanupClient.hgetall(k.job(missingJobId));
    expect(hash).toEqual([]);

    const streamLen = await cleanupClient.xlen(k.stream);
    expect(Number(streamLen)).toBe(0);
  });
});
