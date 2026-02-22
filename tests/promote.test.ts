/**
 * Integration tests for job.promote() - move a delayed job to waiting immediately.
 * Requires: valkey-server running on localhost:6379 and cluster on :7000-7005
 *
 * Run: npx vitest run tests/promote.test.ts
 */
import { it, expect, beforeAll, afterAll } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
const { promoteJob } = require('../dist/functions/index') as typeof import('../src/functions/index');

import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

describeEachMode('job.promote()', (CONNECTION) => {
  const Q = 'test-promote-' + Date.now();
  let queue: InstanceType<typeof Queue>;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
    queue = new Queue(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    const suffixes = [
      '', '-prio', '-worker', '-instance',
      '-evt', '-not-found', '-waiting', '-active',
      '-completed', '-failed',
    ];
    for (const s of suffixes) {
      await flushQueue(cleanupClient, Q + s);
    }
    cleanupClient.close();
  });

  it('delayed job (no priority) promoted to waiting', async () => {
    const job = await queue.add('task', { x: 1 }, { delay: 60000 });
    const k = buildKeys(Q);

    // Verify delayed state
    expect(String(await cleanupClient.hget(k.job(job.id), 'state'))).toBe('delayed');
    expect(await cleanupClient.zscore(k.scheduled, job.id)).not.toBeNull();

    const result = await promoteJob(cleanupClient, k, job.id);
    expect(result).toBe('ok');

    // ZSCORE should be null (removed from scheduled)
    expect(await cleanupClient.zscore(k.scheduled, job.id)).toBeNull();

    // State should be 'waiting', delay should be '0'
    expect(String(await cleanupClient.hget(k.job(job.id), 'state'))).toBe('waiting');
    expect(String(await cleanupClient.hget(k.job(job.id), 'delay'))).toBe('0');
  });

  it('delayed job (with priority) promoted to waiting', async () => {
    const qName = Q + '-prio';
    const localQueue = new Queue(qName, { connection: CONNECTION });
    const k = buildKeys(qName);

    const job = await localQueue.add('task', { x: 1 }, { priority: 5, delay: 60000 });
    expect(String(await cleanupClient.hget(k.job(job.id), 'state'))).toBe('delayed');
    expect(await cleanupClient.zscore(k.scheduled, job.id)).not.toBeNull();

    const result = await promoteJob(cleanupClient, k, job.id);
    expect(result).toBe('ok');

    expect(await cleanupClient.zscore(k.scheduled, job.id)).toBeNull();
    expect(String(await cleanupClient.hget(k.job(job.id), 'state'))).toBe('waiting');
    expect(String(await cleanupClient.hget(k.job(job.id), 'delay'))).toBe('0');

    await localQueue.close();
  });

  it('promoted job is processed by worker', async () => {
    const qName = Q + '-worker';
    const localQueue = new Queue(qName, { connection: CONNECTION });
    const k = buildKeys(qName);

    const job = await localQueue.add('task', { v: 42 }, { delay: 60000 });
    expect(String(await cleanupClient.hget(k.job(job.id), 'state'))).toBe('delayed');

    const result = await promoteJob(cleanupClient, k, job.id);
    expect(result).toBe('ok');

    const processed: string[] = [];
    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker = new Worker(
        qName,
        async (j: any) => {
          processed.push(j.id);
          return { result: 'done' };
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
      );
      worker.on('completed', () => {
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });
      worker.on('error', () => {});
    });

    await done;
    expect(processed).toContain(job.id);

    const finalState = await cleanupClient.hget(k.job(job.id), 'state');
    expect(String(finalState)).toBe('completed');

    await localQueue.close();
  }, 15000);

  it('non-existent job returns error:not_found', async () => {
    const k = buildKeys(Q);
    const result = await promoteJob(cleanupClient, k, 'nonexistent-999');
    expect(result).toBe('error:not_found');
  });

  it('waiting job returns error:not_delayed', async () => {
    const qName = Q + '-waiting';
    const localQueue = new Queue(qName, { connection: CONNECTION });
    const k = buildKeys(qName);

    const job = await localQueue.add('task', { x: 1 });
    expect(String(await cleanupClient.hget(k.job(job.id), 'state'))).toBe('waiting');

    const result = await promoteJob(cleanupClient, k, job.id);
    expect(result).toBe('error:not_delayed');

    await localQueue.close();
  });

  it('active job returns error:not_delayed', async () => {
    const qName = Q + '-active';
    const localQueue = new Queue(qName, { connection: CONNECTION });
    const k = buildKeys(qName);

    const job = await localQueue.add('task', { x: 1 });
    await cleanupClient.hset(k.job(job.id), { state: 'active' });

    const result = await promoteJob(cleanupClient, k, job.id);
    expect(result).toBe('error:not_delayed');

    await localQueue.close();
  });

  it('completed job returns error:not_delayed', async () => {
    const qName = Q + '-completed';
    const localQueue = new Queue(qName, { connection: CONNECTION });
    const k = buildKeys(qName);

    const job = await localQueue.add('task', { x: 1 });
    await cleanupClient.hset(k.job(job.id), { state: 'completed' });

    const result = await promoteJob(cleanupClient, k, job.id);
    expect(result).toBe('error:not_delayed');

    await localQueue.close();
  });

  it('failed job returns error:not_delayed', async () => {
    const qName = Q + '-failed';
    const localQueue = new Queue(qName, { connection: CONNECTION });
    const k = buildKeys(qName);

    const job = await localQueue.add('task', { x: 1 });
    await cleanupClient.hset(k.job(job.id), { state: 'failed' });

    const result = await promoteJob(cleanupClient, k, job.id);
    expect(result).toBe('error:not_delayed');

    await localQueue.close();
  });

  it('Job.promote() instance method works and throws on non-delayed', async () => {
    const qName = Q + '-instance';
    const localQueue = new Queue(qName, { connection: CONNECTION });
    const k = buildKeys(qName);

    const job = await localQueue.add('task', { x: 1 }, { delay: 60000 });
    expect(job!.opts.delay).toBe(60000);

    await job!.promote();
    expect(job!.opts.delay).toBe(0);

    expect(String(await cleanupClient.hget(k.job(job!.id), 'state'))).toBe('waiting');

    // Promoting again (now waiting) should throw
    await expect(job!.promote()).rejects.toThrow('Cannot promote');

    await localQueue.close();
  });

  it('promoted event is emitted in events stream', async () => {
    const qName = Q + '-evt';
    const localQueue = new Queue(qName, { connection: CONNECTION });
    const k = buildKeys(qName);

    const job = await localQueue.add('task', { x: 1 }, { delay: 60000 });

    const result = await promoteJob(cleanupClient, k, job.id);
    expect(result).toBe('ok');

    const entries = (await cleanupClient.xrange(k.events, '-', '+')) as Record<string, [string, string][]>;
    const entryIds = Object.keys(entries);
    expect(entryIds.length).toBeGreaterThan(0);

    let found = false;
    for (const entryId of entryIds) {
      const fields = entries[entryId];
      const map: Record<string, string> = {};
      for (const [f, v] of fields) {
        map[String(f)] = String(v);
      }
      if (map.event === 'promoted' && map.jobId === job.id) {
        found = true;
      }
    }
    expect(found).toBe(true);

    await localQueue.close();
  });
});
