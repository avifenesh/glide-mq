/**
 * Integration tests for Queue.getWorkers() - list active workers with metadata.
 * Requires: valkey-server running on localhost:6379 and cluster on :7000-7005
 *
 * Run: npx vitest run tests/get-workers.test.ts
 */
import { it, expect, beforeAll, afterAll } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');

import { describeEachMode, createCleanupClient, flushQueue, waitFor } from './helpers/fixture';

describeEachMode('Queue.getWorkers()', (CONNECTION) => {
  const Q = 'test-getworkers-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    if (!cleanupClient) return;
    const suffixes = ['', '-multi', '-close', '-meta', '-hb', '-active'];
    for (const s of suffixes) {
      await flushQueue(cleanupClient, Q + s);
    }
    cleanupClient.close();
  });

  it('returns empty array when no workers', async () => {
    const qName = Q;
    const queue = new Queue(qName, { connection: CONNECTION });
    const workers = await queue.getWorkers();
    expect(workers).toEqual([]);
    await queue.close();
  }, 10000);

  it('lists a single active worker', async () => {
    const qName = Q;
    const queue = new Queue(qName, { connection: CONNECTION });
    const worker = new Worker(qName, async () => 'ok', { connection: CONNECTION, blockTimeout: 1000 });
    await worker.waitUntilReady();

    // Worker registration happens on init - should appear quickly
    await waitFor(async () => {
      const w = await queue.getWorkers();
      return w.length === 1;
    });

    const workers = await queue.getWorkers();
    expect(workers).toHaveLength(1);
    expect(workers[0].id).toBeTruthy();
    expect(typeof workers[0].addr).toBe('string');
    expect(typeof workers[0].pid).toBe('number');
    expect(typeof workers[0].startedAt).toBe('number');
    expect(workers[0].age).toBeGreaterThanOrEqual(0);
    expect(typeof workers[0].activeJobs).toBe('number');

    await worker.close();
    await queue.close();
  }, 15000);

  it('lists multiple workers', async () => {
    const qName = Q + '-multi';
    const queue = new Queue(qName, { connection: CONNECTION });
    const w1 = new Worker(qName, async () => 'ok', { connection: CONNECTION, blockTimeout: 1000 });
    const w2 = new Worker(qName, async () => 'ok', { connection: CONNECTION, blockTimeout: 1000 });
    await w1.waitUntilReady();
    await w2.waitUntilReady();

    await waitFor(async () => {
      const w = await queue.getWorkers();
      return w.length === 2;
    });

    const workers = await queue.getWorkers();
    expect(workers).toHaveLength(2);

    // IDs should be distinct
    const ids = workers.map((w) => w.id);
    expect(new Set(ids).size).toBe(2);

    // Sorted by startedAt (oldest first)
    expect(workers[0].startedAt).toBeLessThanOrEqual(workers[1].startedAt);

    await w1.close();
    await w2.close();
    await queue.close();
  }, 15000);

  it('worker disappears after close', async () => {
    const qName = Q + '-close';
    const queue = new Queue(qName, { connection: CONNECTION });
    const worker = new Worker(qName, async () => 'ok', { connection: CONNECTION, blockTimeout: 1000 });
    await worker.waitUntilReady();

    await waitFor(async () => {
      const w = await queue.getWorkers();
      return w.length === 1;
    });

    await worker.close();

    // Worker should deregister on close
    await waitFor(async () => {
      const w = await queue.getWorkers();
      return w.length === 0;
    });

    const workers = await queue.getWorkers();
    expect(workers).toHaveLength(0);

    await queue.close();
  }, 15000);

  it('heartbeat keeps worker alive past initial TTL', async () => {
    const qName = Q + '-hb';
    const queue = new Queue(qName, { connection: CONNECTION });
    // Short stalledInterval so TTL is 2s, heartbeat refreshes at 1s
    const worker = new Worker(qName, async () => 'ok', {
      connection: CONNECTION,
      blockTimeout: 1000,
      stalledInterval: 2000,
    });
    await worker.waitUntilReady();

    await waitFor(async () => {
      const w = await queue.getWorkers();
      return w.length === 1;
    });

    // Wait 3s - past initial TTL (2s) but heartbeat should have refreshed at 1s
    await new Promise((r) => setTimeout(r, 3000));

    const workers = await queue.getWorkers();
    expect(workers).toHaveLength(1);

    await worker.close();
    await queue.close();
  }, 20000);

  it('worker remains registered during job processing', async () => {
    const qName = Q + '-active';
    const queue = new Queue(qName, { connection: CONNECTION });
    let jobStarted = false;
    let finishJob!: () => void;
    const jobPromise = new Promise<void>((r) => {
      finishJob = r;
    });

    const worker = new Worker(
      qName,
      async () => {
        jobStarted = true;
        await jobPromise;
        return 'ok';
      },
      { connection: CONNECTION, blockTimeout: 1000, stalledInterval: 5000 },
    );
    await worker.waitUntilReady();
    await queue.add('slow', {});

    await waitFor(() => jobStarted);

    const workers = await queue.getWorkers();
    expect(workers).toHaveLength(1);
    expect(workers[0].activeJobs).toBeGreaterThanOrEqual(0);

    finishJob();
    await worker.close();
    await queue.close();
  }, 20000);

  it('metadata fields are correct types', async () => {
    const qName = Q + '-meta';
    const queue = new Queue(qName, { connection: CONNECTION });
    const worker = new Worker(qName, async () => 'ok', { connection: CONNECTION, blockTimeout: 1000 });
    await worker.waitUntilReady();

    await waitFor(async () => {
      const w = await queue.getWorkers();
      return w.length === 1;
    });

    const [info] = await queue.getWorkers();
    expect(info.addr).toBeTruthy();
    expect(info.pid).toBeGreaterThan(0);
    expect(info.startedAt).toBeLessThanOrEqual(Date.now());
    expect(info.startedAt).toBeGreaterThan(Date.now() - 60000);
    expect(info.age).toBeGreaterThanOrEqual(0);
    expect(info.activeJobs).toBeGreaterThanOrEqual(0);

    await worker.close();
    await queue.close();
  }, 15000);
});
