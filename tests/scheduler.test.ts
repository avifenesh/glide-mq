/**
 * Integration tests for job schedulers (repeatable/cron jobs).
 * Runs against both standalone (:6379) and cluster (:7000).
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { describeEachMode, createCleanupClient, flushQueue, ConnectionConfig } from './helpers/fixture';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

describeEachMode('Job schedulers', (CONNECTION) => {
  let cleanupClient: any;
  const Q = 'test-scheduler-' + Date.now();
  let queue: InstanceType<typeof Queue>;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
    queue = new Queue(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('upsertJobScheduler stores config in schedulers hash', async () => {
    await queue.upsertJobScheduler('repeat-500', { every: 500 }, { name: 'my-repeat', data: { x: 1 } });

    const k = buildKeys(Q);
    const raw = await cleanupClient.hget(k.schedulers, 'repeat-500');
    expect(raw).not.toBeNull();

    const config = JSON.parse(String(raw));
    expect(config.every).toBe(500);
    expect(config.template.name).toBe('my-repeat');
    expect(config.template.data).toEqual({ x: 1 });
    expect(config.nextRun).toBeGreaterThan(Date.now() - 2000);
  });

  it('removeJobScheduler deletes the scheduler entry', async () => {
    await queue.upsertJobScheduler('to-remove', { every: 1000 });

    const k = buildKeys(Q);
    let raw = await cleanupClient.hget(k.schedulers, 'to-remove');
    expect(raw).not.toBeNull();

    await queue.removeJobScheduler('to-remove');

    raw = await cleanupClient.hget(k.schedulers, 'to-remove');
    expect(raw).toBeNull();
  });

  it('upsertJobScheduler updates existing scheduler (upsert)', async () => {
    await queue.upsertJobScheduler('updatable', { every: 1000 }, { name: 'v1' });
    await queue.upsertJobScheduler('updatable', { every: 2000 }, { name: 'v2' });

    const k = buildKeys(Q);
    const raw = await cleanupClient.hget(k.schedulers, 'updatable');
    const config = JSON.parse(String(raw));
    expect(config.every).toBe(2000);
    expect(config.template.name).toBe('v2');

    // Clean up
    await queue.removeJobScheduler('updatable');
  });

  it('repeatable scheduler (every: 500ms) fires 2+ jobs within 4s via worker', async () => {
    const qName = Q + '-repeat';
    const localQueue = new Queue(qName, { connection: CONNECTION });

    await localQueue.upsertJobScheduler(
      'fast-repeat',
      { every: 500 },
      {
        name: 'tick',
        data: { seq: true },
      },
    );

    const processed: string[] = [];
    let worker: InstanceType<typeof Worker>;
    const done = new Promise<void>((resolve) => {
      const timeout = setTimeout(() => {
        worker.close(true).then(() => resolve());
      }, 4000);

      worker = new Worker(
        qName,
        async (job: any) => {
          processed.push(job.id);
          return 'ok';
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 500,
          stalledInterval: 60000,
          promotionInterval: 500,
        },
      );
      worker.on('error', () => {});
    });

    await done;

    // With 500ms interval, 500ms promotionInterval, and 4s window, expect at least 2 jobs
    expect(processed.length).toBeGreaterThanOrEqual(2);

    await localQueue.close();
    await flushQueue(cleanupClient, qName);
  }, 15000);

  it('upsertJobScheduler rejects missing schedule', async () => {
    await expect(queue.upsertJobScheduler('bad', {} as any)).rejects.toThrow(
      'Schedule must have either pattern (cron) or every (ms interval)',
    );
  });

  it('getJobScheduler returns a single scheduler entry by name', async () => {
    await queue.upsertJobScheduler('single-lookup', { every: 750 }, { name: 'lookup-job', data: { key: 'val' } });

    const result = await queue.getJobScheduler('single-lookup');
    expect(result).not.toBeNull();
    expect(result!.every).toBe(750);
    expect(result!.template?.name).toBe('lookup-job');
    expect(result!.template?.data).toEqual({ key: 'val' });
    expect(result!.nextRun).toBeGreaterThan(0);

    await queue.removeJobScheduler('single-lookup');
  });

  it('getJobScheduler returns null for non-existent name', async () => {
    const result = await queue.getJobScheduler('does-not-exist');
    expect(result).toBeNull();
  });

  it('getJobScheduler returns scheduler with cron pattern', async () => {
    await queue.upsertJobScheduler('cron-lookup', { pattern: '*/10 * * * *' });

    const result = await queue.getJobScheduler('cron-lookup');
    expect(result).not.toBeNull();
    expect(result!.pattern).toBe('*/10 * * * *');
    expect(result!.every).toBeUndefined();
    expect(result!.template).toBeUndefined();

    await queue.removeJobScheduler('cron-lookup');
  });

  it('getJobScheduler returns null for malformed JSON data', async () => {
    const k = buildKeys(Q);
    await cleanupClient.hset(k.schedulers, { corrupt: 'not-valid-json{' });

    const result = await queue.getJobScheduler('corrupt');
    expect(result).toBeNull();

    await cleanupClient.hdel(k.schedulers, ['corrupt']);
  });
});
