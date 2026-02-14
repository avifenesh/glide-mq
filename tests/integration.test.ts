/**
 * Integration tests against a real Valkey instance.
 * Requires: valkey-server running on localhost:6379
 *
 * Run: npx vitest run tests/integration.test.ts
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';

// Import from compiled output to avoid speedkey TS source resolution issues
const { GlideClient } = require('speedkey') as typeof import('speedkey');
const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { Job } = require('../dist/job') as typeof import('../src/job');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
const { LIBRARY_SOURCE, LIBRARY_VERSION, CONSUMER_GROUP } = require('../dist/functions/index') as typeof import('../src/functions/index');
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
  // Clean job hashes
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

describe('Function Library', () => {
  it('glidemq_version returns correct version', async () => {
    const result = await cleanupClient.fcall('glidemq_version', [], []);
    expect(String(result)).toBe(LIBRARY_VERSION);
  });
});

describe('Queue.add + getJob', () => {
  const Q = 'test-add-' + Date.now();
  let queue: InstanceType<typeof Queue>;

  beforeAll(() => {
    queue = new Queue(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(Q);
  });

  it('adds a job and retrieves it by ID', async () => {
    const job = await queue.add('send-email', { to: 'user@test.com', subject: 'hi' });

    expect(job.id).toBeTruthy();
    expect(job.name).toBe('send-email');

    const fetched = await queue.getJob(job.id);
    expect(fetched).not.toBeNull();
    expect(fetched!.id).toBe(job.id);
    expect(fetched!.name).toBe('send-email');
  });

  it('adds a delayed job to the scheduled ZSet', async () => {
    const job = await queue.add('delayed', { x: 1 }, { delay: 60000 });
    const k = buildKeys(Q);
    const score = await cleanupClient.zscore(k.scheduled, job.id);
    expect(score).not.toBeNull();
  });

  it('adds a prioritized job to the scheduled ZSet', async () => {
    const job = await queue.add('prio', { x: 1 }, { priority: 5 });
    const k = buildKeys(Q);
    const score = await cleanupClient.zscore(k.scheduled, job.id);
    expect(score).not.toBeNull();
    expect(Number(score)).toBeGreaterThan(5 * (2 ** 42) - 1);
  });

  it('addBulk creates multiple jobs with unique IDs', async () => {
    const jobs = await queue.addBulk([
      { name: 'b1', data: { i: 1 } },
      { name: 'b2', data: { i: 2 } },
      { name: 'b3', data: { i: 3 } },
    ]);
    expect(jobs).toHaveLength(3);
    const ids = new Set(jobs.map(j => j.id));
    expect(ids.size).toBe(3);
  });

  it('getJob returns null for non-existent job', async () => {
    const result = await queue.getJob('999999');
    expect(result).toBeNull();
  });
});

describe('Queue pause/resume', () => {
  const Q = 'test-pause-' + Date.now();
  let queue: InstanceType<typeof Queue>;

  beforeAll(() => {
    queue = new Queue(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(Q);
  });

  it('pause sets meta.paused=1, resume sets it to 0', async () => {
    const k = buildKeys(Q);
    await queue.pause();
    expect(String(await cleanupClient.hget(k.meta, 'paused'))).toBe('1');
    await queue.resume();
    expect(String(await cleanupClient.hget(k.meta, 'paused'))).toBe('0');
  });
});

describe('Worker processes jobs', () => {
  const Q = 'test-worker-' + Date.now();

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('worker picks up and completes a job', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const processed: string[] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker = new Worker(
        Q,
        async (job: any) => {
          processed.push(job.id);
          return { ok: true };
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
      );
      worker.on('completed', () => {
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });
      worker.on('error', () => {});
    });

    await new Promise(r => setTimeout(r, 500));
    const job = await queue.add('work', { v: 1 });
    await done;

    expect(processed).toContain(job.id);

    // Verify completed in Valkey
    const k = buildKeys(Q);
    const score = await cleanupClient.zscore(k.completed, job.id);
    expect(score).not.toBeNull();
    expect(String(await cleanupClient.hget(k.job(job.id), 'state'))).toBe('completed');

    await queue.close();
  }, 15000);

  it('worker handles 5 concurrent jobs', async () => {
    const qName = Q + '-conc';
    const queue = new Queue(qName, { connection: CONNECTION });
    const processed: string[] = [];
    let maxConcurrent = 0;
    let current = 0;

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 12000);
      const worker = new Worker(
        qName,
        async (job: any) => {
          current++;
          if (current > maxConcurrent) maxConcurrent = current;
          await new Promise(r => setTimeout(r, 50));
          processed.push(job.id);
          current--;
          if (processed.length >= 5) {
            clearTimeout(timeout);
            setTimeout(() => worker.close(true).then(resolve), 200);
          }
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 3, blockTimeout: 1000 },
      );
      worker.on('error', () => {});
    });

    await new Promise(r => setTimeout(r, 500));
    for (let i = 0; i < 5; i++) {
      await queue.add(`c-${i}`, { i });
    }
    await done;

    expect(processed.length).toBe(5);
    expect(maxConcurrent).toBeGreaterThanOrEqual(2);

    await queue.close();
    await flushQueue(qName);
  }, 15000);
});

describe('Job operations', () => {
  const Q = 'test-jobops-' + Date.now();
  let queue: InstanceType<typeof Queue>;

  beforeAll(() => {
    queue = new Queue(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(Q);
  });

  it('updateProgress persists to hash', async () => {
    const job = await queue.add('prog', { x: 1 });
    const live = await queue.getJob(job.id);
    await live!.updateProgress(75);

    const k = buildKeys(Q);
    const val = await cleanupClient.hget(k.job(job.id), 'progress');
    expect(String(val)).toBe('75');
  });

  it('updateProgress with object', async () => {
    const job = await queue.add('prog2', { x: 1 });
    const live = await queue.getJob(job.id);
    await live!.updateProgress({ step: 3, total: 10 });

    const k = buildKeys(Q);
    const val = await cleanupClient.hget(k.job(job.id), 'progress');
    expect(JSON.parse(String(val))).toEqual({ step: 3, total: 10 });
  });

  it('remove deletes the job', async () => {
    const job = await queue.add('rm', { x: 1 });
    const live = await queue.getJob(job.id);
    await live!.remove();

    const k = buildKeys(Q);
    const exists = await cleanupClient.exists([k.job(job.id)]);
    expect(exists).toBe(0);
  });
});

describe('Events stream', () => {
  const Q = 'test-events-' + Date.now();
  let queue: InstanceType<typeof Queue>;

  beforeAll(() => {
    queue = new Queue(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(Q);
  });

  it('adding a job emits an added event', async () => {
    const job = await queue.add('ev-test', { x: 1 });
    const k = buildKeys(Q);

    // xrange returns Record<entryId, [field, value][]>
    const entries = await cleanupClient.xrange(k.events, '-', '+') as Record<string, [string, string][]>;
    const entryIds = Object.keys(entries);
    expect(entryIds.length).toBeGreaterThan(0);

    let found = false;
    for (const entryId of entryIds) {
      const fields = entries[entryId];
      const map: Record<string, string> = {};
      for (const [f, v] of fields) {
        map[String(f)] = String(v);
      }
      if (map.event === 'added' && map.jobId === job.id) {
        found = true;
      }
    }
    expect(found).toBe(true);
  });
});
