/**
 * Integration tests for job schedulers (repeatable/cron jobs).
 * Runs against both standalone (:6379) and cluster (:7000).
 */
import { it, expect, beforeAll, afterAll } from 'vitest';
import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

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
      setTimeout(() => {
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

  it('upsertJobScheduler stores bounds and seeds a future every scheduler from startDate', async () => {
    const startDate = Date.now() + 2000;
    const endDate = startDate + 5000;
    await queue.upsertJobScheduler(
      'bounded-every',
      { every: 500, startDate: new Date(startDate), endDate, limit: 3 },
      { name: 'bounded-job', data: { bounded: true } },
    );

    const result = await queue.getJobScheduler('bounded-every');
    expect(result).not.toBeNull();
    expect(result!.startDate).toBe(startDate);
    expect(result!.endDate).toBe(endDate);
    expect(result!.limit).toBe(3);
    expect(result!.iterationCount).toBe(0);
    expect(result!.nextRun).toBe(startDate);

    await queue.removeJobScheduler('bounded-every');
  });

  it('upsertJobScheduler preserves iteration state when the schedule itself is unchanged', async () => {
    const startDate = Date.now() + 4000;
    const k = buildKeys(Q);

    await queue.upsertJobScheduler('preserve-state', { every: 500, startDate, limit: 3 }, { name: 'preserve-job' });
    await cleanupClient.hset(k.schedulers, {
      'preserve-state': JSON.stringify({
        every: 500,
        startDate,
        limit: 3,
        iterationCount: 2,
        lastRun: startDate,
        nextRun: startDate + 500,
        template: { name: 'preserve-job' },
      }),
    });

    await queue.upsertJobScheduler('preserve-state', { every: 500, startDate, limit: 3 }, { name: 'preserve-job-v2' });

    const result = await queue.getJobScheduler('preserve-state');
    expect(result).not.toBeNull();
    expect(result!.iterationCount).toBe(2);
    expect(result!.lastRun).toBe(startDate);
    expect(result!.nextRun).toBe(startDate + 500);

    await queue.removeJobScheduler('preserve-state');
  });

  it('upsertJobScheduler resets iteration state when the schedule changes', async () => {
    const startDate = Date.now() + 4000;
    const k = buildKeys(Q);

    await queue.upsertJobScheduler('reset-state', { every: 500, startDate, limit: 3 }, { name: 'reset-job' });
    await cleanupClient.hset(k.schedulers, {
      'reset-state': JSON.stringify({
        every: 500,
        startDate,
        limit: 3,
        iterationCount: 2,
        lastRun: startDate,
        nextRun: startDate + 500,
        template: { name: 'reset-job' },
      }),
    });

    await queue.upsertJobScheduler('reset-state', { every: 250, startDate, limit: 3 }, { name: 'reset-job-v2' });

    const result = await queue.getJobScheduler('reset-state');
    expect(result).not.toBeNull();
    expect(result!.iterationCount).toBe(0);
    expect(result!.lastRun).toBeUndefined();
    expect(result!.nextRun).toBe(startDate);

    await queue.removeJobScheduler('reset-state');
  });

  it('upsertJobScheduler seeds cron schedulers from the first occurrence on or after startDate', async () => {
    const start = new Date();
    start.setUTCSeconds(0, 0);
    const minutesUntilBoundary = 5 - (start.getUTCMinutes() % 5 || 5);
    start.setUTCMinutes(start.getUTCMinutes() + minutesUntilBoundary + 5);
    const startDate = start.getTime();

    await queue.upsertJobScheduler(
      'bounded-cron',
      { pattern: '*/5 * * * *', startDate, tz: 'UTC' },
      { name: 'bounded-cron-job', data: { cron: true } },
    );

    const result = await queue.getJobScheduler('bounded-cron');
    expect(result).not.toBeNull();
    expect(result!.startDate).toBe(startDate);
    expect(result!.nextRun).toBe(startDate);

    await queue.removeJobScheduler('bounded-cron');
  });

  it('upsertJobScheduler rounds an off-boundary cron startDate up to the next matching slot', async () => {
    const start = new Date();
    start.setUTCSeconds(0, 0);
    const baseMinute = start.getUTCMinutes();
    const offset = (5 - (baseMinute % 5)) % 5;
    start.setUTCMinutes(baseMinute + offset + 5, 0, 0);
    const boundary = start.getTime();
    const offBoundary = boundary - 90_000;

    await queue.upsertJobScheduler(
      'bounded-cron-off',
      { pattern: '*/5 * * * *', startDate: offBoundary, tz: 'UTC' },
      { name: 'bounded-cron-off-job', data: { cron: true } },
    );

    const result = await queue.getJobScheduler('bounded-cron-off');
    expect(result).not.toBeNull();
    expect(result!.startDate).toBe(offBoundary);
    expect(result!.nextRun).toBe(boundary);

    await queue.removeJobScheduler('bounded-cron-off');
  });

  it('getJobScheduler returns null for malformed JSON data', async () => {
    const k = buildKeys(Q);
    await cleanupClient.hset(k.schedulers, { corrupt: 'not-valid-json{' });

    const result = await queue.getJobScheduler('corrupt');
    expect(result).toBeNull();

    await cleanupClient.hdel(k.schedulers, ['corrupt']);
  });

  // --- Timezone support (#74) ---

  it('upsertJobScheduler stores tz in scheduler entry', async () => {
    await queue.upsertJobScheduler('tz-cron', { pattern: '0 9 * * *', tz: 'America/New_York' });

    const k = buildKeys(Q);
    const raw = await cleanupClient.hget(k.schedulers, 'tz-cron');
    expect(raw).not.toBeNull();

    const config = JSON.parse(String(raw));
    expect(config.pattern).toBe('0 9 * * *');
    expect(config.tz).toBe('America/New_York');
    expect(config.nextRun).toBeGreaterThan(0);

    await queue.removeJobScheduler('tz-cron');
  });

  it('upsertJobScheduler without tz does not store tz field', async () => {
    await queue.upsertJobScheduler('no-tz-cron', { pattern: '0 9 * * *' });

    const k = buildKeys(Q);
    const raw = await cleanupClient.hget(k.schedulers, 'no-tz-cron');
    expect(raw).not.toBeNull();

    const config = JSON.parse(String(raw));
    expect(config.tz).toBeUndefined();

    await queue.removeJobScheduler('no-tz-cron');
  });

  it('upsertJobScheduler rejects invalid timezone', async () => {
    await expect(queue.upsertJobScheduler('bad-tz', { pattern: '0 9 * * *', tz: 'Fake/Zone' })).rejects.toThrow(
      'Invalid timezone',
    );
  });

  it('upsertJobScheduler rejects startDate after endDate', async () => {
    const startDate = Date.now() + 5000;
    const endDate = startDate - 1000;
    await expect(queue.upsertJobScheduler('bad-window', { every: 1000, startDate, endDate })).rejects.toThrow(
      'startDate must be less than or equal to endDate',
    );
  });

  it('upsertJobScheduler rejects non-positive limit', async () => {
    await expect(queue.upsertJobScheduler('bad-limit', { every: 1000, limit: 0 })).rejects.toThrow(
      'limit must be a positive integer',
    );
  });

  it('upsertJobScheduler rejects invalid every intervals', async () => {
    await expect(queue.upsertJobScheduler('bad-every-negative', { every: -100 })).rejects.toThrow(
      'every must be a positive safe integer',
    );
    await expect(queue.upsertJobScheduler('bad-every-zero', { every: 0 as any })).rejects.toThrow(
      'every must be a positive safe integer',
    );
    await expect(queue.upsertJobScheduler('bad-every-string', { every: '100' as any })).rejects.toThrow(
      'every must be a positive safe integer',
    );
    await expect(queue.upsertJobScheduler('bad-every-float', { every: 1.5 as any })).rejects.toThrow(
      'every must be a positive safe integer',
    );
  });

  it('upsertJobScheduler rejects schedules with no occurrences inside the configured bounds', async () => {
    const startDate = new Date('2024-01-02T00:00:00Z').getTime();
    const endDate = new Date('2024-01-02T00:00:00Z').getTime();
    await expect(
      queue.upsertJobScheduler('no-window', {
        pattern: '0 0 1 1 *',
        startDate,
        endDate,
      }),
    ).rejects.toThrow('Schedule has no occurrences within the configured bounds');
  });

  it('upsertJobScheduler rejects invalid dates', async () => {
    await expect(
      queue.upsertJobScheduler('bad-start-date', { every: 1000, startDate: new Date(Number.NaN) }),
    ).rejects.toThrow('startDate must be a valid Date or timestamp');
    await expect(queue.upsertJobScheduler('bad-end-date', { every: 1000, endDate: Number.NaN as any })).rejects.toThrow(
      'endDate must be a valid Date or timestamp',
    );
  });

  it('getJobScheduler returns tz for timezone-aware scheduler', async () => {
    await queue.upsertJobScheduler('tz-lookup', { pattern: '30 14 * * *', tz: 'Asia/Tokyo' });

    const result = await queue.getJobScheduler('tz-lookup');
    expect(result).not.toBeNull();
    expect(result!.pattern).toBe('30 14 * * *');
    expect(result!.tz).toBe('Asia/Tokyo');

    await queue.removeJobScheduler('tz-lookup');
  });

  it('tz is ignored for every-based schedulers (no effect on interval)', async () => {
    // tz should be stored but has no effect on interval-based schedulers
    const before = Date.now();
    await queue.upsertJobScheduler('tz-every', { every: 5000, tz: 'America/New_York' });

    const k = buildKeys(Q);
    const raw = await cleanupClient.hget(k.schedulers, 'tz-every');
    const config = JSON.parse(String(raw));
    // nextRun should be roughly now + 5000ms (interval), not affected by tz
    expect(config.nextRun).toBeGreaterThanOrEqual(before + 4000);
    expect(config.nextRun).toBeLessThanOrEqual(before + 7000);
    expect(config.tz).toBe('America/New_York');

    await queue.removeJobScheduler('tz-every');
  });

  it('scheduler removes itself after reaching the configured limit', async () => {
    const qName = Q + '-limit';
    const localQueue = new Queue(qName, { connection: CONNECTION });
    const keys = buildKeys(qName);

    await localQueue.upsertJobScheduler(
      'limited-repeat',
      { every: 200, limit: 2 },
      {
        name: 'limited-job',
        data: { limited: true },
      },
    );

    const processed: string[] = [];
    const worker = new Worker(
      qName,
      async (job: any) => {
        processed.push(job.id);
        return 'ok';
      },
      {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 500,
        promotionInterval: 100,
        stalledInterval: 60000,
      },
    );
    worker.on('error', () => {});

    const deadline = Date.now() + 5000;
    while (processed.length < 2 && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 100));
    }
    expect(processed).toHaveLength(2);

    await new Promise((r) => setTimeout(r, 500));
    expect(processed).toHaveLength(2);
    const counts = await localQueue.getJobCounts();
    expect(counts.waiting).toBe(0);
    expect(counts.delayed).toBe(0);
    const raw = await cleanupClient.hget(keys.schedulers, 'limited-repeat');
    expect(raw).toBeNull();

    await worker.close(true);
    await localQueue.close();
    await flushQueue(cleanupClient, qName);
  }, 15000);

  it('two workers do not overshoot the configured limit for the same scheduler', async () => {
    const qName = Q + '-dual-worker-limit';
    const localQueue = new Queue(qName, { connection: CONNECTION });
    const keys = buildKeys(qName);

    await localQueue.upsertJobScheduler(
      'single-shot',
      { every: 200, limit: 1 },
      {
        name: 'single-shot-job',
        data: { limit: true },
      },
    );

    const processed: string[] = [];
    const workerA = new Worker(
      qName,
      async (job: any) => {
        processed.push(job.id);
        return 'ok';
      },
      {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 500,
        promotionInterval: 100,
        stalledInterval: 60000,
      },
    );
    const workerB = new Worker(
      qName,
      async (job: any) => {
        processed.push(job.id);
        return 'ok';
      },
      {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 500,
        promotionInterval: 100,
        stalledInterval: 60000,
      },
    );
    workerA.on('error', () => {});
    workerB.on('error', () => {});

    const deadline = Date.now() + 4000;
    while (processed.length < 1 && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 100));
    }
    expect(processed).toHaveLength(1);

    await new Promise((r) => setTimeout(r, 500));
    expect(processed).toHaveLength(1);
    const raw = await cleanupClient.hget(keys.schedulers, 'single-shot');
    expect(raw).toBeNull();

    await workerA.close(true);
    await workerB.close(true);
    await localQueue.close();
    await flushQueue(cleanupClient, qName);
  }, 15000);

  it('scheduler removes itself once the next run would exceed endDate', async () => {
    const qName = Q + '-end-date';
    const localQueue = new Queue(qName, { connection: CONNECTION });
    const keys = buildKeys(qName);
    const startDate = Date.now() + 300;

    await localQueue.upsertJobScheduler(
      'bounded-repeat',
      { every: 200, startDate, endDate: startDate },
      {
        name: 'bounded-job',
        data: { bounded: true },
      },
    );

    const processed: string[] = [];
    const worker = new Worker(
      qName,
      async (job: any) => {
        processed.push(job.id);
        return 'ok';
      },
      {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 500,
        promotionInterval: 100,
        stalledInterval: 60000,
      },
    );
    worker.on('error', () => {});

    const deadline = Date.now() + 4000;
    while (processed.length < 1 && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 100));
    }
    expect(processed).toHaveLength(1);

    await new Promise((r) => setTimeout(r, 500));
    expect(processed).toHaveLength(1);
    const counts = await localQueue.getJobCounts();
    expect(counts.waiting).toBe(0);
    expect(counts.delayed).toBe(0);
    const raw = await cleanupClient.hget(keys.schedulers, 'bounded-repeat');
    expect(raw).toBeNull();

    await worker.close(true);
    await localQueue.close();
    await flushQueue(cleanupClient, qName);
  }, 15000);

  it('scheduler deletes an already-exhausted entry without creating new jobs', async () => {
    const qName = Q + '-stale-limit';
    const localQueue = new Queue(qName, { connection: CONNECTION });
    const keys = buildKeys(qName);

    await cleanupClient.hset(keys.schedulers, {
      stale: JSON.stringify({
        every: 200,
        limit: 1,
        iterationCount: 1,
        nextRun: Date.now() - 100,
        template: { name: 'stale-job', data: { stale: true } },
      }),
    });

    const processed: string[] = [];
    const worker = new Worker(
      qName,
      async (job: any) => {
        processed.push(job.id);
        return 'ok';
      },
      {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 500,
        promotionInterval: 100,
        stalledInterval: 60000,
      },
    );
    worker.on('error', () => {});

    await new Promise((r) => setTimeout(r, 500));
    expect(processed).toHaveLength(0);

    const raw = await cleanupClient.hget(keys.schedulers, 'stale');
    expect(raw).toBeNull();

    await worker.close(true);
    await localQueue.close();
    await flushQueue(cleanupClient, qName);
  }, 15000);

  it('scheduler deletes invalid negative-interval entries without creating jobs', async () => {
    const qName = Q + '-invalid-every';
    const localQueue = new Queue(qName, { connection: CONNECTION });
    const keys = buildKeys(qName);

    await cleanupClient.hset(keys.schedulers, {
      invalid: JSON.stringify({
        every: -100,
        nextRun: Date.now() - 100,
        template: { name: 'invalid-job', data: { invalid: true } },
      }),
    });

    const processed: string[] = [];
    const worker = new Worker(
      qName,
      async (job: any) => {
        processed.push(job.id);
        return 'ok';
      },
      {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 500,
        promotionInterval: 100,
        stalledInterval: 60000,
      },
    );
    worker.on('error', () => {});

    await new Promise((r) => setTimeout(r, 500));
    expect(processed).toHaveLength(0);

    const raw = await cleanupClient.hget(keys.schedulers, 'invalid');
    expect(raw).toBeNull();

    await worker.close(true);
    await localQueue.close();
    await flushQueue(cleanupClient, qName);
  }, 15000);

  it('invalid persisted cron scheduler does not block a valid scheduler tick', async () => {
    const qName = Q + '-invalid-cron';
    const localQueue = new Queue(qName, { connection: CONNECTION });
    const keys = buildKeys(qName);

    await cleanupClient.hset(keys.schedulers, {
      broken: JSON.stringify({
        pattern: 'invalid cron',
        nextRun: Date.now() - 100,
        template: { name: 'broken-job' },
      }),
    });
    await localQueue.upsertJobScheduler('healthy', { every: 200, limit: 1 }, { name: 'healthy-job' });

    const processed: string[] = [];
    const worker = new Worker(
      qName,
      async (job: any) => {
        processed.push(job.name);
        return 'ok';
      },
      {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 500,
        promotionInterval: 100,
        stalledInterval: 60000,
      },
    );
    worker.on('error', () => {});

    const deadline = Date.now() + 4000;
    while (processed.length < 1 && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 100));
    }

    expect(processed).toEqual(['healthy-job']);
    expect(await cleanupClient.hget(keys.schedulers, 'broken')).toBeNull();

    await worker.close(true);
    await localQueue.close();
    await flushQueue(cleanupClient, qName);
  }, 15000);

});
