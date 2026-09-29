/**
 * Follow-up regressions for server functions and worker paths (2026-09-29).
 *
 * Run: npx vitest run tests/followups-20260929.test.ts
 */
import { afterAll, beforeAll, expect, it } from 'vitest';
import { createCleanupClient, describeEachMode, flushQueue } from './helpers/fixture';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
const { moveToActive, popLists } = require('../dist/functions') as typeof import('../src/functions');

describeEachMode('Follow-ups 2026-09-29', (CONNECTION) => {
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

  it('F1: obliterate without force refuses while a list job is active', async () => {
    const Q = uniqueQueue('fu-obl-list');
    const k = buildKeys(Q);
    const queue = new Queue(Q, { connection: CONNECTION });
    try {
      const job = await queue.add('a', {}, { lifo: true });
      expect(await popLists(cleanupClient, k, 1)).toEqual([job!.id]);
      await moveToActive(cleanupClient, k, job!.id, Date.now());
      await expect(queue.obliterate()).rejects.toThrow(/1 active jobs/);
      expect(await cleanupClient.exists([k.job(job!.id)])).toBe(1);
      await queue.obliterate({ force: true });
      expect(await cleanupClient.exists([k.job(job!.id)])).toBe(0);
    } finally {
      await queue.close();
    }
  });
});
