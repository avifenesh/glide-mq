/**
 * Integration tests for job schedulers (repeatable/cron jobs).
 * Requires: valkey-server running on localhost:6379
 *
 * Run: npx vitest run tests/scheduler.test.ts
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';

const { GlideClient } = require('speedkey') as typeof import('speedkey');
const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
const { LIBRARY_SOURCE, CONSUMER_GROUP } = require('../dist/functions/index') as typeof import('../src/functions/index');
const { ensureFunctionLibrary } = require('../dist/connection') as typeof import('../src/connection');

const CONNECTION = {
  addresses: [{ host: 'localhost', port: 6379 }],
};

let cleanupClient: InstanceType<typeof GlideClient>;

async function flushQueue(queueName: string) {
  const k = buildKeys(queueName);
  const keysToDelete = [
    k.id, k.stream, k.scheduled, k.completed, k.failed,
    k.events, k.meta, k.dedup, k.rate, k.schedulers,
  ];
  for (const key of keysToDelete) {
    try { await cleanupClient.del([key]); } catch {}
  }
  const prefix = `glide:{${queueName}}:job:`;
  let cursor = '0';
  do {
    const result = await cleanupClient.scan(cursor, { match: `${prefix}*`, count: 100 });
    cursor = result[0] as string;
    const keys = result[1] as string[];
    if (keys.length > 0) {
      await cleanupClient.del(keys);
    }
  } while (cursor !== '0');
}

beforeAll(async () => {
  cleanupClient = await GlideClient.createClient({
    addresses: [{ host: 'localhost', port: 6379 }],
  });
  await ensureFunctionLibrary(cleanupClient, LIBRARY_SOURCE);
});

afterAll(async () => {
  cleanupClient.close();
});

describe('Job schedulers', () => {
  const Q = 'test-scheduler-' + Date.now();
  let queue: InstanceType<typeof Queue>;

  beforeAll(() => {
    queue = new Queue(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(Q);
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

    await localQueue.upsertJobScheduler('fast-repeat', { every: 500 }, {
      name: 'tick',
      data: { seq: true },
    });

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
    await flushQueue(qName);
  }, 15000);

  it('upsertJobScheduler rejects missing schedule', async () => {
    await expect(
      queue.upsertJobScheduler('bad', {} as any),
    ).rejects.toThrow('Schedule must have either pattern (cron) or every (ms interval)');
  });
});
