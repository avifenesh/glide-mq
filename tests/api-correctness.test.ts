/**
 * Producer-side API correctness regressions (Queue, Producer, FlowProducer).
 * Runs against both standalone (:6379) and cluster (:7000).
 */
import { it, expect, beforeAll, afterAll, afterEach, describe, vi } from 'vitest';
import { describeEachMode, createCleanupClient, flushQueue, waitFor } from './helpers/fixture';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { Producer } = require('../dist/producer') as typeof import('../src/producer');
const { FlowProducer } = require('../dist/flow-producer') as typeof import('../src/flow-producer');
const { buildKeys, keyPrefix } = require('../dist/utils') as typeof import('../src/utils');
const connection = require('../dist/connection') as typeof import('../src/connection');

describe('client initialization lifecycle', () => {
  const CONN = { addresses: [{ host: 'localhost', port: 6379 }] };

  function deferred<T>() {
    let resolve!: (v: T) => void;
    const promise = new Promise<T>((r) => (resolve = r));
    return { promise, resolve };
  }

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('Queue closes a created client when library load fails', async () => {
    const fake = { close: vi.fn() };
    vi.spyOn(connection, 'createClient').mockResolvedValue(fake as any);
    vi.spyOn(connection, 'ensureFunctionLibrary').mockRejectedValue(new Error('load failed'));
    const queue = new Queue('apicorr-init-q1', { connection: CONN });
    queue.on('error', () => {});
    await expect(queue.getClient()).rejects.toThrow('load failed');
    expect(fake.close).toHaveBeenCalledTimes(1);
    await queue.close();
  });

  it('Queue closes a client whose init finishes after close()', async () => {
    const fake = { close: vi.fn() };
    const created = deferred<any>();
    vi.spyOn(connection, 'createClient').mockReturnValue(created.promise);
    vi.spyOn(connection, 'ensureFunctionLibrary').mockResolvedValue(undefined as any);
    const queue = new Queue('apicorr-init-q2', { connection: CONN });
    const pending = queue.getClient();
    await queue.close();
    created.resolve(fake);
    await expect(pending).rejects.toThrow('closing');
    expect(fake.close).toHaveBeenCalledTimes(1);
  });

  it('FlowProducer shares one init across concurrent callers', async () => {
    const fake = { close: vi.fn() };
    const created = deferred<any>();
    const createSpy = vi.spyOn(connection, 'createClient').mockReturnValue(created.promise);
    vi.spyOn(connection, 'ensureFunctionLibrary').mockResolvedValue(undefined as any);
    const flow = new FlowProducer({ connection: CONN });
    const a = (flow as any).getClient();
    const b = (flow as any).getClient();
    created.resolve(fake);
    expect(await a).toBe(fake);
    expect(await b).toBe(fake);
    expect(createSpy).toHaveBeenCalledTimes(1);
    await flow.close();
    expect(fake.close).toHaveBeenCalledTimes(1);
  });

  it('FlowProducer closes a client after a failed library load and retries', async () => {
    const first = { close: vi.fn() };
    const second = { close: vi.fn() };
    const createSpy = vi
      .spyOn(connection, 'createClient')
      .mockResolvedValueOnce(first as any)
      .mockResolvedValueOnce(second as any);
    vi.spyOn(connection, 'ensureFunctionLibrary')
      .mockRejectedValueOnce(new Error('load failed'))
      .mockResolvedValueOnce(undefined as any);
    const flow = new FlowProducer({ connection: CONN });
    await expect((flow as any).getClient()).rejects.toThrow('load failed');
    expect(first.close).toHaveBeenCalledTimes(1);
    expect(await (flow as any).getClient()).toBe(second);
    expect(createSpy).toHaveBeenCalledTimes(2);
    await flow.close();
  });

  it('FlowProducer closes a client whose init finishes after close()', async () => {
    const fake = { close: vi.fn() };
    const created = deferred<any>();
    vi.spyOn(connection, 'createClient').mockReturnValue(created.promise);
    vi.spyOn(connection, 'ensureFunctionLibrary').mockResolvedValue(undefined as any);
    const flow = new FlowProducer({ connection: CONN });
    const pending = (flow as any).getClient();
    await flow.close();
    created.resolve(fake);
    await expect(pending).rejects.toThrow('closing');
    expect(fake.close).toHaveBeenCalledTimes(1);
  });
});

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

  it('FlowProducer rejects invalid queue names anywhere in the tree before writing', async () => {
    const flow = new FlowProducer({ connection: CONNECTION });
    try {
      await expect(
        flow.add({ name: 'p', queueName: Q, data: {}, children: [{ name: 'c', queueName: 'bad{tag}', data: {} }] }),
      ).rejects.toThrow('Queue name must not contain curly braces or colons');
      await expect(flow.addBulk([{ name: 'p', queueName: 'bad:q', data: {} }])).rejects.toThrow(
        'Queue name must not contain curly braces or colons',
      );
    } finally {
      await flow.close();
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

describeEachMode('FlowProducer.add with leaf children in other queues', (CONNECTION) => {
  const PQ = 'test-apicorr-xflow-p-' + Date.now();
  const CQ = 'test-apicorr-xflow-c-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, PQ);
    await flushQueue(cleanupClient, CQ);
    cleanupClient.close();
  });

  it('creates the flow and completes the parent after every child', async () => {
    const flow = new FlowProducer({ connection: CONNECTION });
    const processed: string[] = [];
    const workerOpts = { connection: CONNECTION, blockTimeout: 200, stalledInterval: 60000 };
    const childWorker = new Worker(
      CQ,
      async (job: any) => {
        processed.push(job.name);
        return job.data.v;
      },
      workerOpts,
    );
    const parentWorker = new Worker(
      PQ,
      async (job: any) => {
        processed.push(job.name);
        return job.data.v ?? 'parent';
      },
      workerOpts,
    );
    childWorker.on('error', () => {});
    parentWorker.on('error', () => {});
    try {
      const node = await flow.add({
        name: 'parent',
        queueName: PQ,
        data: {},
        children: [
          { name: 'same-queue', queueName: PQ, data: { v: 'a' } },
          { name: 'other-1', queueName: CQ, data: { v: 'b' } },
          { name: 'other-2', queueName: CQ, data: { v: 'c' } },
        ],
      });
      expect(node.children!.map((c) => c.job.name)).toEqual(['same-queue', 'other-1', 'other-2']);
      for (const child of node.children!) {
        expect(child.job.parentId).toBe(node.job.id);
        expect(child.job.parentQueue).toBe(PQ);
      }
      const deps = [...(await cleanupClient.smembers(buildKeys(PQ).deps(node.job.id)))].map(String).sort();
      expect(deps).toEqual(
        [
          `${keyPrefix('glide', PQ)}:${node.children![0].job.id}`,
          `${keyPrefix('glide', CQ)}:${node.children![1].job.id}`,
          `${keyPrefix('glide', CQ)}:${node.children![2].job.id}`,
        ].sort(),
      );

      const parentKey = buildKeys(PQ).job(node.job.id);
      await waitFor(async () => String(await cleanupClient.hget(parentKey, 'state')) === 'completed', 15000);
      expect(processed.filter((n) => n === 'parent')).toHaveLength(1);
      expect(processed.indexOf('parent')).toBe(processed.length - 1);
    } finally {
      await childWorker.close(true);
      await parentWorker.close(true);
      await flow.close();
    }
  }, 30000);
});

describeEachMode('searchJobs without state', (CONNECTION) => {
  const TS = Date.now();
  const Q = `apicorr-glob-${TS}-*`;
  const OTHER = `apicorr-glob-${TS}-x`;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    await flushQueue(cleanupClient, OTHER);
    cleanupClient.close();
  });

  it('returns only real jobs of this queue', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const other = new Queue(OTHER, { connection: CONNECTION });
    try {
      const job = await queue.add('real', { v: 1 });
      await other.add('real', { v: 2 });
      await other.add('real', { v: 3 });
      const jobKey = buildKeys(Q).job(job!.id);
      await cleanupClient.hset(`${jobKey}:sub:grp`, { a: '1' });
      await cleanupClient.set(`${jobKey}:usage-lock`, 'x');

      const all = await queue.searchJobs({ limit: 100 });
      expect(all.map((j) => j.id)).toEqual([job!.id]);
      expect(all[0].data).toEqual({ v: 1 });

      const named = await queue.searchJobs({ name: 'real', limit: 100 });
      expect(named.map((j) => j.id)).toEqual([job!.id]);
    } finally {
      await queue.close();
      await other.close();
    }
  });
});
