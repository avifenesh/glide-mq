/**
 * Producer-side API correctness regressions (Queue, Producer, FlowProducer).
 * Runs against both standalone (:6379) and cluster (:7000).
 */
import { it, expect, beforeAll, afterAll } from 'vitest';
import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { buildKeys, keyPrefix } = require('../dist/utils') as typeof import('../src/utils');

describeEachMode('Queue.addBulk cross-queue parents', (CONNECTION) => {
  const Q = 'test-apicorr-bulk-' + Date.now();
  const PQ = Q + '-parent';
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    await flushQueue(cleanupClient, PQ);
    cleanupClient.close();
  });

  it('registers each created child in its own parent deps when earlier entries are skipped', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    try {
      const first = await queue.add('seed', {}, { deduplication: { id: 'dup-a' } });
      expect(first).not.toBeNull();

      const jobs = await queue.addBulk([
        { name: 'skipped', data: {}, opts: { deduplication: { id: 'dup-a' } } },
        { name: 'child-a', data: {}, opts: { parent: { queue: PQ, id: 'pa' } } },
        { name: 'child-b', data: {}, opts: { parent: { queue: PQ, id: 'pb' } } },
      ]);

      expect(jobs.map((j) => j.name)).toEqual(['child-a', 'child-b']);
      expect(jobs[0].parentQueue).toBe(PQ);
      expect(jobs[1].parentQueue).toBe(PQ);

      const pk = buildKeys(PQ);
      const pfx = keyPrefix('glide', Q);
      const depsA = [...(await cleanupClient.smembers(pk.deps('pa')))].map(String);
      const depsB = [...(await cleanupClient.smembers(pk.deps('pb')))].map(String);
      expect(depsA).toEqual([`${pfx}:${jobs[0].id}`]);
      expect(depsB).toEqual([`${pfx}:${jobs[1].id}`]);
    } finally {
      await queue.close();
    }
  });

  it('rejects an invalid parent queue name', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    try {
      await expect(queue.add('bad', {}, { parent: { queue: 'bad{tag}', id: '1' } })).rejects.toThrow();
      await expect(
        queue.addBulk([{ name: 'bad', data: {}, opts: { parent: { queue: 'bad{tag}', id: '1' } } }]),
      ).rejects.toThrow();
    } finally {
      await queue.close();
    }
  });
});
