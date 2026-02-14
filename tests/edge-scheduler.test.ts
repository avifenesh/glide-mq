/**
 * Edge-case tests for job schedulers (repeatable/cron jobs).
 * Requires: valkey-server running on localhost:6379
 *
 * Run: npx vitest run tests/edge-scheduler.test.ts
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';

const { GlideClient } = require('speedkey') as typeof import('speedkey');
const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
const { LIBRARY_SOURCE } = require('../dist/functions/index') as typeof import('../src/functions/index');
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
  const prefix = `glide:{${queueName}}:`;
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

describe('Edge: Cron scheduler fires on schedule', () => {
  const Q = 'edge-sched-cron-' + Date.now();

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('cron scheduler with every-minute pattern creates jobs via worker', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });

    // Use a cron pattern that matches every minute. Since we can't wait a full
    // minute in a test, we instead verify the scheduler entry is stored correctly
    // and has a valid nextRun within the next 60 seconds.
    await queue.upsertJobScheduler('every-min', { pattern: '* * * * *' }, {
      name: 'cron-tick',
      data: { cron: true },
    });

    const k = buildKeys(Q);
    const raw = await cleanupClient.hget(k.schedulers, 'every-min');
    expect(raw).not.toBeNull();

    const config = JSON.parse(String(raw));
    expect(config.pattern).toBe('* * * * *');
    expect(config.template.name).toBe('cron-tick');

    // nextRun should be within the next 60 seconds
    const now = Date.now();
    expect(config.nextRun).toBeGreaterThan(now - 1000);
    expect(config.nextRun).toBeLessThanOrEqual(now + 61000);

    // Now set a short interval scheduler and verify it actually fires via a worker
    await queue.upsertJobScheduler('fast-cron-test', { every: 300 }, {
      name: 'fast-tick',
      data: { fast: true },
    });

    const processed: string[] = [];
    const worker = new Worker(
      Q,
      async (job: any) => {
        processed.push(job.name);
        return 'ok';
      },
      {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 500,
        promotionInterval: 300,
        stalledInterval: 60000,
      },
    );
    worker.on('error', () => {});

    // Wait for scheduler to fire at least once
    await new Promise(r => setTimeout(r, 3000));

    await worker.close(true);

    expect(processed.filter(n => n === 'fast-tick').length).toBeGreaterThanOrEqual(1);

    await queue.removeJobScheduler('every-min');
    await queue.removeJobScheduler('fast-cron-test');
    await queue.close();
  }, 15000);
});

describe('Edge: Remove scheduler while running', () => {
  const Q = 'edge-sched-remove-' + Date.now();

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('stops creating jobs after scheduler is removed', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });

    await queue.upsertJobScheduler('removable', { every: 300 }, {
      name: 'will-stop',
      data: { x: 1 },
    });

    const processed: string[] = [];
    const worker = new Worker(
      Q,
      async (job: any) => {
        processed.push(job.id);
        return 'ok';
      },
      {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 500,
        promotionInterval: 300,
        stalledInterval: 60000,
      },
    );
    worker.on('error', () => {});

    // Let it fire a couple times
    await new Promise(r => setTimeout(r, 2000));
    const countBefore = processed.length;
    expect(countBefore).toBeGreaterThanOrEqual(1);

    // Remove the scheduler
    await queue.removeJobScheduler('removable');

    // Verify it's gone from Redis
    const k = buildKeys(Q);
    const raw = await cleanupClient.hget(k.schedulers, 'removable');
    expect(raw).toBeNull();

    // Wait and verify no new jobs are created
    const countAfterRemoval = processed.length;
    await new Promise(r => setTimeout(r, 2000));
    const countFinal = processed.length;

    // Should have at most 1 more job (from in-flight scheduler tick)
    expect(countFinal - countAfterRemoval).toBeLessThanOrEqual(1);

    await worker.close(true);
    await queue.close();
  }, 15000);
});

describe('Edge: Upsert scheduler with changed interval', () => {
  const Q = 'edge-sched-upsert-' + Date.now();

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('updates interval when upserting an existing scheduler', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    // Create with 1000ms interval
    await queue.upsertJobScheduler('changeable', { every: 1000 }, {
      name: 'v1-tick',
      data: { version: 1 },
    });

    let raw = await cleanupClient.hget(k.schedulers, 'changeable');
    let config = JSON.parse(String(raw));
    expect(config.every).toBe(1000);
    expect(config.template.name).toBe('v1-tick');
    const originalNextRun = config.nextRun;

    // Upsert with new interval
    await queue.upsertJobScheduler('changeable', { every: 200 }, {
      name: 'v2-tick',
      data: { version: 2 },
    });

    raw = await cleanupClient.hget(k.schedulers, 'changeable');
    config = JSON.parse(String(raw));
    expect(config.every).toBe(200);
    expect(config.template.name).toBe('v2-tick');
    expect(config.template.data).toEqual({ version: 2 });
    // nextRun should be updated (sooner, since interval is shorter)
    expect(config.nextRun).toBeLessThanOrEqual(originalNextRun);

    // Verify the new interval actually works by running a worker
    const processed: string[] = [];
    const worker = new Worker(
      Q,
      async (job: any) => {
        processed.push(job.name);
        return 'ok';
      },
      {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 500,
        promotionInterval: 200,
        stalledInterval: 60000,
      },
    );
    worker.on('error', () => {});

    await new Promise(r => setTimeout(r, 2500));

    await worker.close(true);

    // With 200ms interval, should fire multiple times in 2.5s
    const v2Ticks = processed.filter(n => n === 'v2-tick');
    expect(v2Ticks.length).toBeGreaterThanOrEqual(2);

    await queue.removeJobScheduler('changeable');
    await queue.close();
  }, 15000);
});

describe('Edge: Two schedulers on same queue', () => {
  const Q = 'edge-sched-two-' + Date.now();

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('both schedulers fire independently', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });

    await queue.upsertJobScheduler('sched-A', { every: 400 }, {
      name: 'tick-A',
      data: { sched: 'A' },
    });

    await queue.upsertJobScheduler('sched-B', { every: 400 }, {
      name: 'tick-B',
      data: { sched: 'B' },
    });

    const k = buildKeys(Q);
    // Verify both exist
    const rawA = await cleanupClient.hget(k.schedulers, 'sched-A');
    const rawB = await cleanupClient.hget(k.schedulers, 'sched-B');
    expect(rawA).not.toBeNull();
    expect(rawB).not.toBeNull();

    const processedA: string[] = [];
    const processedB: string[] = [];

    const worker = new Worker(
      Q,
      async (job: any) => {
        if (job.name === 'tick-A') processedA.push(job.id);
        if (job.name === 'tick-B') processedB.push(job.id);
        return 'ok';
      },
      {
        connection: CONNECTION,
        concurrency: 2,
        blockTimeout: 500,
        promotionInterval: 300,
        stalledInterval: 60000,
      },
    );
    worker.on('error', () => {});

    // Wait for both to fire multiple times
    await new Promise(r => setTimeout(r, 3500));

    await worker.close(true);

    // Both should have fired at least twice
    expect(processedA.length).toBeGreaterThanOrEqual(2);
    expect(processedB.length).toBeGreaterThanOrEqual(2);

    await queue.removeJobScheduler('sched-A');
    await queue.removeJobScheduler('sched-B');
    await queue.close();
  }, 15000);
});
