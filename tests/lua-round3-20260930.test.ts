/**
 * Server-function and worker gap regressions, round 3 (2026-09-30).
 *
 * Run: flock /tmp/gmq-test.lock npx vitest run tests/lua-round3-20260930.test.ts
 */
import { afterAll, beforeAll, expect, it } from 'vitest';
import { createCleanupClient, describeEachMode, flushQueue, waitFor } from './helpers/fixture';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

describeEachMode('Lua round 3 2026-09-30', (CONNECTION) => {
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

  // Item 1: a job fired by an every scheduler is still running when the
  // scheduler is switched to repeatAfterComplete and the old nextRun passes.
  // The first repeatAfterComplete run must wait for that job.
  it('mode switch to repeatAfterComplete waits for the in-flight job of the old mode', async () => {
    const Q = uniqueQueue('r3-sched-switch');
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);
    const starts: { id: string; at: number }[] = [];
    const ends: { id: string; at: number }[] = [];
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const worker = new Worker(
      Q,
      async (job) => {
        starts.push({ id: job.id, at: Date.now() });
        if (starts.length === 1) await gate;
        ends.push({ id: job.id, at: Date.now() });
      },
      { connection: CONNECTION, concurrency: 4, promotionInterval: 100 },
    );
    try {
      await queue.upsertJobScheduler('sw', { every: 700 }, { name: 'sw' });
      await waitFor(() => starts.length === 1, 5000);
      const firstId = starts[0].id;
      const before = await queue.getJobScheduler('sw');
      expect(before!.inflightJobId).toBe(firstId);

      await queue.upsertJobScheduler('sw', { repeatAfterComplete: 100 }, { name: 'sw' });
      // Past the old nextRun: the tick must park the entry instead of firing.
      await waitFor(async () => (await queue.getJobScheduler('sw'))!.nextRun === 0, 5000);
      await new Promise((r) => setTimeout(r, 400));
      expect(starts.length).toBe(1);

      release();
      await waitFor(() => starts.length >= 2, 5000);
      expect(starts[1].at).toBeGreaterThanOrEqual(ends[0].at + 100 - 5);
      const after = await queue.getJobScheduler('sw');
      expect(after!.repeatAfterComplete).toBe(100);
      // The old-mode job carries the scheduler name so its completion advances the chain.
      expect(String(await cleanupClient.hget(`${k.id.slice(0, -2)}job:${firstId}`, 'schedulerName'))).toBe('sw');
    } finally {
      release();
      await queue.removeJobScheduler('sw').catch(() => {});
      await worker.close();
      await queue.close();
    }
  }, 20000);
});
