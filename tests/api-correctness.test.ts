/**
 * Producer-side API correctness regressions (Queue, Producer, FlowProducer).
 * Runs against both standalone (:6379) and cluster (:7000).
 */
import { it, expect, beforeAll, afterAll } from 'vitest';
import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Producer } = require('../dist/producer') as typeof import('../src/producer');
const { FlowProducer } = require('../dist/flow-producer') as typeof import('../src/flow-producer');
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

describeEachMode('Job option number validation', (CONNECTION) => {
  const Q = 'test-apicorr-opts-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    await flushQueue(cleanupClient, Q + '-ok');
    cleanupClient.close();
  });

  const invalid: [string, Record<string, unknown>][] = [
    ['negative priority', { priority: -1 }],
    ['fractional priority', { priority: 1.5 }],
    ['priority above 2048', { priority: 2049 }],
    ['negative priority with delay', { priority: -1, delay: 1000 }],
    ['infinite delay', { delay: Infinity }],
    ['negative delay', { delay: -5 }],
    ['NaN delay', { delay: NaN }],
    ['negative attempts', { attempts: -1 }],
    ['fractional attempts', { attempts: 1.5 }],
    ['infinite backoff delay', { attempts: 3, backoff: { type: 'fixed', delay: Infinity } }],
    ['negative backoff jitter', { attempts: 3, backoff: { type: 'fixed', delay: 10, jitter: -1 } }],
  ];

  it('rejects invalid options on every add path without writing jobs', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const producer = new Producer(Q, { connection: CONNECTION });
    const flow = new FlowProducer({ connection: CONNECTION });
    try {
      for (const [label, opts] of invalid) {
        const o = opts as any;
        await expect(queue.add('j', {}, o), label).rejects.toThrow();
        await expect(queue.addBulk([{ name: 'j', data: {}, opts: o }]), label).rejects.toThrow();
        await expect(producer.add('j', {}, o), label).rejects.toThrow();
        await expect(producer.addBulk([{ name: 'j', data: {}, opts: o }]), label).rejects.toThrow();
        await expect(
          flow.add({ name: 'p', queueName: Q, data: {}, children: [{ name: 'c', queueName: Q, data: {}, opts: o }] }),
          label,
        ).rejects.toThrow();
        await expect(
          flow.add({
            name: 'p',
            queueName: Q,
            data: {},
            opts: o,
            children: [{ name: 'mid', queueName: Q, data: {}, children: [{ name: 'c', queueName: Q, data: {} }] }],
          }),
          label,
        ).rejects.toThrow();
        await expect(flow.addBulk([{ name: 'p', queueName: Q, data: {}, opts: o }]), label).rejects.toThrow();
        await expect(
          flow.addDAG({
            nodes: [
              { name: 'a', queueName: Q, data: {} },
              { name: 'b', queueName: Q, data: {}, deps: ['a'], opts: o },
            ],
          }),
          label,
        ).rejects.toThrow();
      }
      expect(await cleanupClient.get(buildKeys(Q).id)).toBeNull();
    } finally {
      await flow.close();
      await producer.close();
      await queue.close();
    }
  });

  it('accepts boundary values', async () => {
    const queue = new Queue(Q + '-ok', { connection: CONNECTION });
    try {
      expect(await queue.add('j', {}, { priority: 0, delay: 0, attempts: 0 })).not.toBeNull();
      expect(
        await queue.add('j', {}, { priority: 2048, delay: 10, attempts: 3, backoff: { type: 'fixed', delay: 0 } }),
      ).not.toBeNull();
    } finally {
      await queue.close();
    }
  });
});
