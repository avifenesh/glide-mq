/**
 * Integration tests for addBulk batch pipelining.
 * Verifies that the Batch API produces correct results in both standalone and cluster modes.
 *
 * Run: npx vitest run tests/batch.test.ts
 */
import { it, expect, beforeAll, afterAll } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

describeEachMode('addBulk batch pipelining', (CONNECTION) => {
  const Q = 'test-batch-' + Date.now();
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

  it('addBulk with empty array returns empty', async () => {
    const jobs = await queue.addBulk([]);
    expect(jobs).toEqual([]);
  });

  it('addBulk with single job works', async () => {
    const jobs = await queue.addBulk([
      { name: 'single', data: { x: 1 } },
    ]);
    expect(jobs).toHaveLength(1);
    expect(jobs[0].name).toBe('single');
    expect(jobs[0].data).toEqual({ x: 1 });

    const fetched = await queue.getJob(jobs[0].id);
    expect(fetched).not.toBeNull();
    expect(fetched!.id).toBe(jobs[0].id);
  });

  it('addBulk with multiple jobs returns unique sequential IDs', async () => {
    const jobs = await queue.addBulk([
      { name: 'a', data: { i: 1 } },
      { name: 'b', data: { i: 2 } },
      { name: 'c', data: { i: 3 } },
      { name: 'd', data: { i: 4 } },
      { name: 'e', data: { i: 5 } },
    ]);
    expect(jobs).toHaveLength(5);

    // All IDs should be unique
    const ids = new Set(jobs.map(j => j.id));
    expect(ids.size).toBe(5);

    // IDs should be sequential (all created in same batch)
    const numericIds = jobs.map(j => Number(j.id));
    for (let i = 1; i < numericIds.length; i++) {
      expect(numericIds[i]).toBe(numericIds[i - 1] + 1);
    }
  });

  it('addBulk jobs are retrievable by getJob', async () => {
    const jobs = await queue.addBulk([
      { name: 'fetch-a', data: { val: 'alpha' } },
      { name: 'fetch-b', data: { val: 'beta' } },
    ]);

    for (const job of jobs) {
      const fetched = await queue.getJob(job.id);
      expect(fetched).not.toBeNull();
      expect(fetched!.name).toBe(job.name);
    }
  });

  it('addBulk preserves job names and data', async () => {
    const input = [
      { name: 'type-x', data: { key: 'value1', nested: { a: 1 } } },
      { name: 'type-y', data: { key: 'value2', list: [1, 2, 3] } },
    ];
    const jobs = await queue.addBulk(input);

    expect(jobs[0].name).toBe('type-x');
    expect(jobs[0].data).toEqual(input[0].data);
    expect(jobs[1].name).toBe('type-y');
    expect(jobs[1].data).toEqual(input[1].data);
  });

  it('addBulk with delay puts jobs in scheduled set', async () => {
    const jobs = await queue.addBulk([
      { name: 'delayed-a', data: {}, opts: { delay: 60000 } },
      { name: 'delayed-b', data: {}, opts: { delay: 120000 } },
    ]);

    const k = buildKeys(Q);
    for (const job of jobs) {
      const score = await cleanupClient.zscore(k.scheduled, job.id);
      expect(score).not.toBeNull();
    }
  });

  it('addBulk with priority puts jobs in scheduled set', async () => {
    const jobs = await queue.addBulk([
      { name: 'prio-a', data: {}, opts: { priority: 1 } },
      { name: 'prio-b', data: {}, opts: { priority: 10 } },
    ]);

    const k = buildKeys(Q);
    for (const job of jobs) {
      const score = await cleanupClient.zscore(k.scheduled, job.id);
      expect(score).not.toBeNull();
    }
  });

  it('addBulk with mixed options (some delayed, some immediate)', async () => {
    const jobs = await queue.addBulk([
      { name: 'immediate', data: { type: 'now' } },
      { name: 'delayed', data: { type: 'later' }, opts: { delay: 60000 } },
      { name: 'prioritized', data: { type: 'prio' }, opts: { priority: 5 } },
    ]);

    expect(jobs).toHaveLength(3);

    const k = buildKeys(Q);
    // Immediate job should be in the stream
    const streamLen = await cleanupClient.xlen(k.stream);
    expect(streamLen).toBeGreaterThanOrEqual(1);

    // Delayed + prioritized should be in scheduled set
    for (const job of [jobs[1], jobs[2]]) {
      const score = await cleanupClient.zscore(k.scheduled, job.id);
      expect(score).not.toBeNull();
    }
  });

  it('addBulk honors deduplication and skips duplicates consistently', async () => {
    const jobs = await queue.addBulk([
      { name: 'dedup-first', data: { i: 1 }, opts: { deduplication: { id: 'batch-dedup-1', mode: 'simple' } } },
      { name: 'dedup-duplicate', data: { i: 2 }, opts: { deduplication: { id: 'batch-dedup-1', mode: 'simple' } } },
      { name: 'dedup-second-key', data: { i: 3 }, opts: { deduplication: { id: 'batch-dedup-2', mode: 'simple' } } },
    ]);

    expect(jobs).toHaveLength(2);
    expect(jobs.map((j) => j.name).sort()).toEqual(['dedup-first', 'dedup-second-key']);
  });
});

describeEachMode('addBulk batch - worker processing', (CONNECTION) => {
  const Q = 'test-batch-worker-' + Date.now();
  let queue: InstanceType<typeof Queue>;
  let worker: InstanceType<typeof Worker>;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
    queue = new Queue(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    if (worker) await worker.close();
    await queue.close();
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('batch-added jobs are processed by worker', async () => {
    const processed: string[] = [];
    const done = new Promise<void>((resolve) => {
      let count = 0;
      worker = new Worker(Q, async (job) => {
        processed.push(job.name);
        count++;
        if (count === 3) resolve();
        return `done-${job.name}`;
      }, { connection: CONNECTION, concurrency: 1 });
    });

    await queue.addBulk([
      { name: 'w1', data: { v: 1 } },
      { name: 'w2', data: { v: 2 } },
      { name: 'w3', data: { v: 3 } },
    ]);

    await done;
    expect(processed).toHaveLength(3);
    expect(processed.sort()).toEqual(['w1', 'w2', 'w3']);
  });
});
