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

  it.each(['__proto__', 'constructor', 'prototype'])('upserts a scheduler named %s', async (name) => {
    await queue.upsertJobScheduler(name, { every: 1000 });
    const stored = await queue.getJobScheduler(name);
    expect(stored?.every).toBe(1000);

    const raw = await cleanupClient.hget(buildKeys(Q).schedulers, name);
    expect(raw).not.toBeNull();

    await queue.removeJobScheduler(name);
    expect(await queue.getJobScheduler(name)).toBeNull();
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
      'Schedule must have pattern (cron), every (ms interval), or repeatAfterComplete (ms)',
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

  it('repeatAfterComplete scheduler fires next job only after completion', async () => {
    const qName = Q + '-rac';
    const localQueue = new Queue(qName, { connection: CONNECTION });

    await localQueue.upsertJobScheduler(
      'rac-test',
      { repeatAfterComplete: 500 },
      { name: 'rac-job', data: { seq: true } },
    );

    const timestamps: { start: number; end: number }[] = [];
    let worker: InstanceType<typeof Worker>;
    const done = new Promise<void>((resolve) => {
      worker = new Worker(
        qName,
        async () => {
          const start = Date.now();
          await new Promise((r) => setTimeout(r, 200));
          timestamps.push({ start, end: Date.now() });
          if (timestamps.length >= 3) {
            setTimeout(() => worker.close(true).then(resolve), 100);
          }
          return 'ok';
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 500,
          stalledInterval: 60000,
          promotionInterval: 300,
        },
      );
      worker.on('error', () => {});

      // Safety timeout
      setTimeout(() => worker.close(true).then(resolve), 12000);
    });

    await done;

    expect(timestamps.length).toBeGreaterThanOrEqual(2);

    // Verify non-overlapping: each job starts after previous completes
    for (let i = 1; i < timestamps.length; i++) {
      // Assert true non-overlap: next starts after previous ends
      expect(timestamps[i].start).toBeGreaterThanOrEqual(timestamps[i - 1].end);

      // Assert repeatAfterComplete delay is honored (500ms with tolerance for jitter)
      const gap = timestamps[i].start - timestamps[i - 1].end;
      expect(gap).toBeGreaterThanOrEqual(400); // 500ms - 100ms jitter tolerance
    }

    await localQueue.close();
    await flushQueue(cleanupClient, qName);
  }, 20000);

  it('repeatAfterComplete schedules next after terminal failure', async () => {
    const qName = Q + '-rac-fail';
    const localQueue = new Queue(qName, { connection: CONNECTION });
    const { UnrecoverableError } = require('../dist/errors') as typeof import('../src/errors');

    await localQueue.upsertJobScheduler('rac-fail-test', { repeatAfterComplete: 300 }, { name: 'rac-fail-job' });

    let jobCount = 0;
    let worker: InstanceType<typeof Worker>;
    const done = new Promise<void>((resolve) => {
      worker = new Worker(
        qName,
        async () => {
          jobCount++;
          if (jobCount === 1) {
            throw new UnrecoverableError('terminal failure');
          }
          return 'ok';
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 500,
          stalledInterval: 60000,
          promotionInterval: 300,
        },
      );
      worker.on('error', () => {});

      setTimeout(() => worker.close(true).then(resolve), 6000);
    });

    await done;

    // First job fails terminally, but scheduler should still create a second job
    expect(jobCount).toBeGreaterThanOrEqual(2);

    await localQueue.close();
    await flushQueue(cleanupClient, qName);
  }, 15000);

  it('repeatAfterComplete respects limit', async () => {
    const qName = Q + '-rac-limit';
    const localQueue = new Queue(qName, { connection: CONNECTION });

    await localQueue.upsertJobScheduler(
      'rac-limit-test',
      { repeatAfterComplete: 200, limit: 2 },
      { name: 'rac-limit-job' },
    );

    let jobCount = 0;
    let worker: InstanceType<typeof Worker>;
    const done = new Promise<void>((resolve) => {
      worker = new Worker(
        qName,
        async () => {
          jobCount++;
          return 'ok';
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 500,
          stalledInterval: 60000,
          promotionInterval: 300,
        },
      );
      worker.on('error', () => {});

      setTimeout(() => worker.close(true).then(resolve), 5000);
    });

    await done;

    // Scheduler limit is 2: first job fired by scheduler tick (iterationCount=1),
    // worker completion schedules second (iterationCount stays at 1 on scheduler side,
    // but limit is checked in runSchedulers after bump to 2). Expect exactly 2.
    expect(jobCount).toBe(2);

    // Scheduler entry should be deleted after reaching the limit
    const k = buildKeys(qName);
    const entry = await cleanupClient.hget(k.schedulers, 'rac-limit-test');
    expect(entry).toBeNull();

    await localQueue.close();
    await flushQueue(cleanupClient, qName);
  }, 15000);

  it('repeatAfterComplete is mutually exclusive with every/pattern', async () => {
    await expect(
      queue.upsertJobScheduler('bad-combo-1', { every: 1000, repeatAfterComplete: 500 } as any),
    ).rejects.toThrow('mutually exclusive');

    await expect(
      queue.upsertJobScheduler('bad-combo-2', { pattern: '* * * * *', repeatAfterComplete: 500 } as any),
    ).rejects.toThrow('mutually exclusive');
  });

  it('every scheduler nextRun stays on the interval grid after a late tick', async () => {
    const qName = Q + '-anchor-' + Date.now();
    const localQueue = new Queue(qName, { connection: CONNECTION });
    const every = 500;
    const processed: string[] = [];

    try {
      await localQueue.upsertJobScheduler('anchor', { every }, { name: 'anchored', data: { n: 1 } });
      const initial = await localQueue.getJobScheduler('anchor');
      expect(initial).not.toBeNull();
      const firstNext = initial!.nextRun;
      expect(firstNext).toBeGreaterThan(0);

      const worker = new Worker(
        qName,
        async () => {
          processed.push('ok');
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 200,
          promotionInterval: 200,
          stalledInterval: 60000,
        },
      );
      worker.on('error', () => {});

      try {
        await new Promise<void>((resolve, reject) => {
          const timeout = setTimeout(() => reject(new Error('timeout waiting for scheduler jobs')), 8000);
          const check = () => {
            if (processed.length >= 2) {
              clearTimeout(timeout);
              resolve();
            }
          };
          worker.on('completed', check);
        });

        const later = await localQueue.getJobScheduler('anchor');
        expect(later).not.toBeNull();
        expect(later!.nextRun).toBeGreaterThan(firstNext);
        expect((later!.nextRun - firstNext) % every).toBe(0);
      } finally {
        await worker.close(true);
      }
    } finally {
      await localQueue.removeJobScheduler('anchor').catch(() => {});
      await localQueue.close();
      await flushQueue(cleanupClient, qName);
    }
  }, 15000);

  it('re-upserting an in-flight repeatAfterComplete scheduler does not overwrite a completion that lands mid-upsert', async () => {
    const k = buildKeys(Q);
    const name = 'rac-inflight-race';
    await queue.upsertJobScheduler(name, { repeatAfterComplete: 500 }, { name: 'rac-race' });
    const inFlight = { repeatAfterComplete: 500, iterationCount: 1, nextRun: 0, template: { name: 'rac-race' } };
    await cleanupClient.hset(k.schedulers, { [name]: JSON.stringify(inFlight) });
    const advancedNextRun = Date.now() + 500;

    // Simulate the in-flight job completing right after upsert read the entry:
    // completion advances nextRun in Lua without the scheduler mutation lock.
    const client = await (queue as any).getClient();
    const originalHget = client.hget.bind(client);
    let raced = false;
    client.hget = async (key: string, field: string) => {
      const value = await originalHget(key, field);
      if (!raced && key === k.schedulers && field === name) {
        raced = true;
        await cleanupClient.hset(k.schedulers, {
          [name]: JSON.stringify({ ...inFlight, nextRun: advancedNextRun }),
        });
      }
      return value;
    };
    try {
      await queue.upsertJobScheduler(name, { repeatAfterComplete: 500 }, { name: 'rac-race' });
    } finally {
      client.hget = originalHget;
    }
    expect(raced).toBe(true);
    const entry = await queue.getJobScheduler(name);
    expect(entry!.nextRun).toBe(advancedNextRun);
    await queue.removeJobScheduler(name);
  });

  it('switching every/pattern to repeatAfterComplete does not fire before the old nextRun', async () => {
    for (const [name, schedule] of [
      ['switch-every-rac', { every: 60_000 }],
      ['switch-cron-rac', { pattern: '0 0 1 1 *' }],
    ] as const) {
      await queue.upsertJobScheduler(name, schedule, { name: 'switch' });
      const before = await queue.getJobScheduler(name);
      expect(before!.nextRun).toBeGreaterThan(Date.now() + 1000);

      await queue.upsertJobScheduler(name, { repeatAfterComplete: 500 }, { name: 'switch' });
      const after = await queue.getJobScheduler(name);
      expect(after!.repeatAfterComplete).toBe(500);
      expect(after!.nextRun).toBe(before!.nextRun);
      await queue.removeJobScheduler(name);
    }

    // A later startDate still wins over the held nextRun.
    await queue.upsertJobScheduler('switch-start-rac', { every: 60_000 }, { name: 'switch' });
    const startDate = Date.now() + 120_000;
    await queue.upsertJobScheduler('switch-start-rac', { repeatAfterComplete: 500, startDate }, { name: 'switch' });
    expect((await queue.getJobScheduler('switch-start-rac'))!.nextRun).toBe(startDate);
    await queue.removeJobScheduler('switch-start-rac');

    // An endDate before the held nextRun leaves no occurrence.
    await queue.upsertJobScheduler('switch-end-rac', { every: 60_000 }, { name: 'switch' });
    await expect(
      queue.upsertJobScheduler('switch-end-rac', { repeatAfterComplete: 500, endDate: Date.now() + 30_000 }),
    ).rejects.toThrow('Schedule has no occurrences within the configured bounds');
    await queue.removeJobScheduler('switch-end-rac');
  });

  it('re-upserting a repeatAfterComplete scheduler while its job is in flight keeps the awaiting state', async () => {
    const k = buildKeys(Q);
    const name = 'rac-inflight-state';
    await queue.upsertJobScheduler(name, { repeatAfterComplete: 500, limit: 10 }, { name: 'rac-inflight' });
    const lastRun = Date.now() - 1000;
    await cleanupClient.hset(k.schedulers, {
      [name]: JSON.stringify({
        repeatAfterComplete: 500,
        limit: 10,
        iterationCount: 3,
        lastRun,
        nextRun: 0,
        template: { name: 'rac-inflight' },
      }),
    });

    // Same schedule: nothing changes
    await queue.upsertJobScheduler(name, { repeatAfterComplete: 500, limit: 10 }, { name: 'rac-inflight' });
    let entry = await queue.getJobScheduler(name);
    expect(entry!.nextRun).toBe(0);
    expect(entry!.iterationCount).toBe(3);
    expect(entry!.lastRun).toBe(lastRun);

    // New interval: still awaiting the in-flight job, interval applies after it completes
    await queue.upsertJobScheduler(name, { repeatAfterComplete: 2000, limit: 10 }, { name: 'rac-inflight' });
    entry = await queue.getJobScheduler(name);
    expect(entry!.nextRun).toBe(0);
    expect(entry!.repeatAfterComplete).toBe(2000);
    expect(entry!.iterationCount).toBe(3);
    expect(entry!.lastRun).toBe(lastRun);

    // Changed bounds restart the count but never fire a second chain
    await queue.upsertJobScheduler(
      name,
      { repeatAfterComplete: 2000, limit: 10, endDate: Date.now() + 60_000 },
      { name: 'rac-inflight' },
    );
    entry = await queue.getJobScheduler(name);
    expect(entry!.nextRun).toBe(0);
    expect(entry!.iterationCount).toBe(0);

    // Switching to another mode schedules normally
    await queue.upsertJobScheduler(name, { every: 1000 }, { name: 'rac-inflight' });
    entry = await queue.getJobScheduler(name);
    expect(entry!.nextRun).toBeGreaterThan(0);

    await queue.removeJobScheduler(name);
  });

  it('repeatAfterComplete scheduler upserted from inside its processor never overlaps', async () => {
    const qName = Q + '-rac-adaptive';
    const localQueue = new Queue(qName, { connection: CONNECTION });
    await localQueue.upsertJobScheduler('adaptive', { repeatAfterComplete: 100 }, { name: 'adaptive-job' });

    let active = 0;
    let maxActive = 0;
    let runs = 0;
    const worker = new Worker(
      qName,
      async () => {
        active++;
        maxActive = Math.max(maxActive, active);
        runs++;
        // Adaptive interval: re-upsert while this job is running
        await localQueue.upsertJobScheduler(
          'adaptive',
          { repeatAfterComplete: 100 + runs * 50 },
          { name: 'adaptive-job' },
        );
        await new Promise((r) => setTimeout(r, 600));
        active--;
        return 'ok';
      },
      { connection: CONNECTION, concurrency: 5, blockTimeout: 200, promotionInterval: 100, stalledInterval: 60000 },
    );
    worker.on('error', () => {});

    try {
      const deadline = Date.now() + 8000;
      while (runs < 3 && Date.now() < deadline) {
        await new Promise((r) => setTimeout(r, 100));
      }
      expect(runs).toBeGreaterThanOrEqual(3);
      expect(maxActive).toBe(1);
      const entry = await localQueue.getJobScheduler('adaptive');
      expect(entry!.iterationCount).toBeGreaterThanOrEqual(3);
    } finally {
      await worker.close(true);
      await localQueue.close();
      await flushQueue(cleanupClient, qName);
    }
  }, 20000);

  it('scheduler jobs carry the template ordering key, group concurrency and cost', async () => {
    const qName = Q + '-tmpl-ordering';
    const localQueue = new Queue(qName, { connection: CONNECTION });
    const k = buildKeys(qName);
    await localQueue.upsertJobScheduler(
      'ordered',
      { every: 100, limit: 3 },
      {
        name: 'ordered-job',
        opts: { ordering: { key: 'tenant-a', concurrency: 1 }, cost: 2 },
      },
    );

    let active = 0;
    let maxActive = 0;
    const ids: string[] = [];
    const worker = new Worker(
      qName,
      async (job: any) => {
        active++;
        maxActive = Math.max(maxActive, active);
        ids.push(job.id);
        await new Promise((r) => setTimeout(r, 400));
        active--;
        return 'ok';
      },
      { connection: CONNECTION, concurrency: 5, blockTimeout: 200, promotionInterval: 100, stalledInterval: 60000 },
    );
    worker.on('error', () => {});

    try {
      const deadline = Date.now() + 8000;
      while (ids.length < 3 && Date.now() < deadline) {
        await new Promise((r) => setTimeout(r, 100));
      }
      expect(ids.length).toBe(3);
      // Group concurrency 1 serializes the scheduled jobs even with worker concurrency 5
      expect(maxActive).toBe(1);
      const fields = await cleanupClient.hmget(k.job(ids[0]), ['groupKey', 'cost']);
      expect(fields.map((f: any) => (f == null ? null : String(f)))).toEqual(['tenant-a', '2000']);
    } finally {
      await worker.close(true);
      await localQueue.close();
      await flushQueue(cleanupClient, qName);
    }
  }, 20000);

  it('upsertJobScheduler validates template options like Queue.add', async () => {
    await expect(
      queue.upsertJobScheduler('tmpl-jobid', { every: 1000 }, { name: 'j', opts: { jobId: 'fixed' } as any }),
    ).rejects.toThrow('Scheduler template: jobId is not supported');
    await expect(
      queue.upsertJobScheduler('tmpl-cost', { every: 1000 }, { name: 'j', opts: { cost: -1 } }),
    ).rejects.toThrow('Scheduler template: cost must be a non-negative finite number');
    await expect(
      queue.upsertJobScheduler(
        'tmpl-tb',
        { every: 1000 },
        { name: 'j', opts: { ordering: { key: 'g', tokenBucket: { capacity: 0, refillRate: 1 } } } },
      ),
    ).rejects.toThrow('Scheduler template: tokenBucket.capacity must be a positive finite number');
    await expect(
      queue.upsertJobScheduler(
        'tmpl-over-capacity',
        { every: 1000 },
        { name: 'j', opts: { cost: 5, ordering: { key: 'g', tokenBucket: { capacity: 2, refillRate: 1 } } } },
      ),
    ).rejects.toThrow('Scheduler template: Job cost exceeds token bucket capacity');
    await expect(
      queue.upsertJobScheduler('tmpl-ttl', { every: 1000 }, { name: 'j', opts: { ttl: -5 } }),
    ).rejects.toThrow('Scheduler template: ttl must be a non-negative finite number');
    for (const name of ['tmpl-jobid', 'tmpl-cost', 'tmpl-tb', 'tmpl-over-capacity', 'tmpl-ttl']) {
      expect(await queue.getJobScheduler(name)).toBeNull();
    }
  });

  it('upsertJobScheduler rejects template delay, deduplication and parent the tick would ignore', async () => {
    const cases: [string, Record<string, unknown>, string][] = [
      ['tmpl-delay', { delay: 1000 }, 'delay'],
      ['tmpl-dedup', { deduplication: { id: 'd' } }, 'deduplication'],
      ['tmpl-parent', { parent: { queue: Q, id: '1' } }, 'parent'],
    ];
    for (const [name, opts, field] of cases) {
      await expect(queue.upsertJobScheduler(name, { every: 1000 }, { name: 'j', opts: opts as any })).rejects.toThrow(
        `Scheduler template: ${field} is not supported`,
      );
      expect(await queue.getJobScheduler(name)).toBeNull();
    }
  });

  it('upsertJobScheduler rejects oversized, unserializable or out-of-range templates', async () => {
    const { MAX_JOB_DATA_SIZE } = require('../dist/utils') as typeof import('../src/utils');
    await expect(
      queue.upsertJobScheduler('tmpl-big', { every: 1000 }, { name: 'j', data: 'x'.repeat(MAX_JOB_DATA_SIZE + 1) }),
    ).rejects.toThrow('Scheduler template: Job data exceeds maximum size');
    await expect(
      queue.upsertJobScheduler('tmpl-bigint', { every: 1000 }, { name: 'j', data: { n: BigInt(1) } }),
    ).rejects.toThrow('Scheduler template:');
    await expect(
      queue.upsertJobScheduler('tmpl-priority', { every: 1000 }, { name: 'j', opts: { priority: 5000 } }),
    ).rejects.toThrow('Scheduler template: Priority must be <= 2048');
    for (const name of ['tmpl-big', 'tmpl-bigint', 'tmpl-priority']) {
      expect(await queue.getJobScheduler(name)).toBeNull();
    }
  });

  it('a stored template that cannot produce a job reports an error and advances nextRun', async () => {
    const qName = Q + '-bad-template';
    const localQueue = new Queue(qName, { connection: CONNECTION });
    const k = buildKeys(qName);
    const { MAX_JOB_DATA_SIZE } = require('../dist/utils') as typeof import('../src/utils');
    const firstNextRun = Date.now() - 100;
    await cleanupClient.hset(k.schedulers, {
      oversized: JSON.stringify({
        every: 60_000,
        iterationCount: 0,
        nextRun: firstNextRun,
        template: { name: 'big', data: 'x'.repeat(MAX_JOB_DATA_SIZE + 1) },
      }),
      badpriority: JSON.stringify({
        every: 60_000,
        iterationCount: 0,
        nextRun: firstNextRun,
        template: { name: 'prio', opts: { priority: 5000 } },
      }),
    });

    const errors: string[] = [];
    const processed: string[] = [];
    const worker = new Worker(
      qName,
      async (job: any) => {
        processed.push(job.name);
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 200, promotionInterval: 100, stalledInterval: 60000 },
    );
    worker.on('error', (err: Error) => errors.push(err.message));

    try {
      const deadline = Date.now() + 5000;
      while (errors.filter((m) => m.includes('skipped a run')).length < 2 && Date.now() < deadline) {
        await new Promise((r) => setTimeout(r, 100));
      }
      await new Promise((r) => setTimeout(r, 500));
      const skipped = errors.filter((m) => m.includes('skipped a run'));
      // One report per scheduler: nextRun moved a full interval ahead, so no retry on every tick
      expect(skipped).toHaveLength(2);
      expect(skipped.some((m) => m.includes('"oversized"') && m.includes('exceeds maximum size'))).toBe(true);
      expect(skipped.some((m) => m.includes('"badpriority"') && m.includes('Priority must be <= 2048'))).toBe(true);
      for (const name of ['oversized', 'badpriority']) {
        const entry = await localQueue.getJobScheduler(name);
        expect(entry!.nextRun).toBe(firstNextRun + 60_000);
        expect(entry!.iterationCount).toBe(0);
      }
      expect(processed).toEqual([]);
    } finally {
      await worker.close(true);
      await localQueue.close();
      await flushQueue(cleanupClient, qName);
    }
  }, 15000);
});
