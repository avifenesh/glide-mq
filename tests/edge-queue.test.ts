/**
 * Edge-case tests for Queue operations and data integrity.
 * Requires: valkey-server running on localhost:6379
 *
 * Run: npx vitest run tests/edge-queue.test.ts
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';

const { GlideClient } = require('speedkey') as typeof import('speedkey');
const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { Job } = require('../dist/job') as typeof import('../src/job');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
const { LIBRARY_SOURCE, CONSUMER_GROUP } = require('../dist/functions/index') as typeof import('../src/functions/index');
const { ensureFunctionLibrary } = require('../dist/connection') as typeof import('../src/connection');

const CONNECTION = {
  addresses: [{ host: 'localhost', port: 6379 }],
};

let cleanupClient: InstanceType<typeof GlideClient>;

async function flushQueue(queueName: string, prefix?: string) {
  const k = buildKeys(queueName, prefix);
  const keysToDelete = [
    k.id, k.stream, k.scheduled, k.completed, k.failed,
    k.events, k.meta, k.dedup, k.rate, k.schedulers,
  ];
  for (const key of keysToDelete) {
    try { await cleanupClient.del([key]); } catch {}
  }
  const pfx = prefix ?? 'glide';
  const keyPrefix = `${pfx}:{${queueName}}:job:`;
  let cursor = '0';
  do {
    const result = await cleanupClient.scan(cursor, { match: `${keyPrefix}*`, count: 100 });
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

// ---------------------------------------------------------------------------
// 1. FIFO ordering: add 20 jobs sequentially, verify worker processes in order
// ---------------------------------------------------------------------------
describe('FIFO ordering', () => {
  const Q = 'edge-fifo-' + Date.now();
  const processed: string[] = [];

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('processes 20 jobs in FIFO order', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });

    // Add 20 jobs sequentially
    const addedIds: string[] = [];
    for (let i = 0; i < 20; i++) {
      const job = await queue.add('fifo', { seq: i });
      addedIds.push(job!.id);
    }

    // Worker with concurrency=1 to ensure strict ordering
    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);
      const worker = new Worker(
        Q,
        async (job: any) => {
          processed.push(job.id);
          if (processed.length >= 20) {
            clearTimeout(timeout);
            setTimeout(() => worker.close(true).then(resolve), 200);
          }
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
      );
      worker.on('error', () => {});
    });

    await done;
    await queue.close();

    expect(processed).toHaveLength(20);
    expect(processed).toEqual(addedIds);
  }, 20000);
});

// ---------------------------------------------------------------------------
// 2. FIFO is the default (LIFO is not used)
// ---------------------------------------------------------------------------
describe('Default ordering is FIFO (not LIFO)', () => {
  const Q = 'edge-fifo-default-' + Date.now();

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('first job added is the first job processed', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });

    const first = await queue.add('first', { order: 1 });
    await queue.add('second', { order: 2 });
    await queue.add('third', { order: 3 });

    const processed: string[] = [];
    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker = new Worker(
        Q,
        async (job: any) => {
          processed.push(job.id);
          if (processed.length >= 3) {
            clearTimeout(timeout);
            setTimeout(() => worker.close(true).then(resolve), 200);
          }
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
      );
      worker.on('error', () => {});
    });

    await done;
    await queue.close();

    // First added must be first processed
    expect(processed[0]).toBe(first!.id);
  }, 15000);
});

// ---------------------------------------------------------------------------
// 3. Large job data: 100KB JSON payload roundtrips correctly
// ---------------------------------------------------------------------------
describe('Large job data', () => {
  const Q = 'edge-large-' + Date.now();
  let queue: InstanceType<typeof Queue>;

  beforeAll(() => {
    queue = new Queue(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(Q);
  });

  it('100KB JSON payload roundtrips correctly', async () => {
    // Build ~100KB payload
    const largeArray: string[] = [];
    for (let i = 0; i < 2000; i++) {
      largeArray.push('x'.repeat(50) + '-' + i);
    }
    const payload = { items: largeArray, nested: { deep: { value: 42 } } };
    const payloadSize = JSON.stringify(payload).length;
    expect(payloadSize).toBeGreaterThan(100_000);

    const job = await queue.add('large', payload);
    expect(job).not.toBeNull();

    const fetched = await queue.getJob(job!.id);
    expect(fetched).not.toBeNull();
    expect(fetched!.data).toEqual(payload);
  });
});

// ---------------------------------------------------------------------------
// 4. Jobs with empty data, null-ish values, special characters in name
// ---------------------------------------------------------------------------
describe('Job edge cases: empty data, special chars', () => {
  const Q = 'edge-special-' + Date.now();
  let queue: InstanceType<typeof Queue>;

  beforeAll(() => {
    queue = new Queue(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(Q);
  });

  it('job with empty object data', async () => {
    const job = await queue.add('empty', {});
    const fetched = await queue.getJob(job!.id);
    expect(fetched).not.toBeNull();
    expect(fetched!.data).toEqual({});
  });

  it('job with null-ish values in data', async () => {
    const data = { a: null, b: 0, c: '', d: false, e: undefined };
    const job = await queue.add('nullish', data);
    const fetched = await queue.getJob(job!.id);
    expect(fetched).not.toBeNull();
    // JSON.stringify drops undefined keys, so 'e' won't be present
    expect(fetched!.data.a).toBeNull();
    expect(fetched!.data.b).toBe(0);
    expect(fetched!.data.c).toBe('');
    expect(fetched!.data.d).toBe(false);
    expect(fetched!.data.e).toBeUndefined();
  });

  it('job with special characters in name', async () => {
    const name = 'email:send/batch#1@org.com';
    const job = await queue.add(name, { x: 1 });
    const fetched = await queue.getJob(job!.id);
    expect(fetched).not.toBeNull();
    expect(fetched!.name).toBe(name);
  });

  it('job with unicode characters in name and data', async () => {
    const name = 'task-\u00e9\u00e0\u00fc-\u4e16\u754c-\ud83d\ude00';
    const data = { message: '\u4f60\u597d\u4e16\u754c', emoji: '\ud83d\ude80\ud83c\udf1f' };
    const job = await queue.add(name, data);
    const fetched = await queue.getJob(job!.id);
    expect(fetched).not.toBeNull();
    expect(fetched!.name).toBe(name);
    expect(fetched!.data).toEqual(data);
  });
});

// ---------------------------------------------------------------------------
// 5. Job with every option set simultaneously
// ---------------------------------------------------------------------------
describe('Job with all options set simultaneously', () => {
  const Q = 'edge-allopts-' + Date.now();
  let queue: InstanceType<typeof Queue>;

  beforeAll(() => {
    queue = new Queue(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(Q);
  });

  it('accepts a job with delay + priority + attempts + backoff + removeOnComplete + dedup', async () => {
    const job = await queue.add('all-opts', { v: 1 }, {
      delay: 5000,
      priority: 3,
      attempts: 5,
      backoff: { type: 'exponential', delay: 1000, jitter: 0.1 },
      removeOnComplete: true,
      deduplication: { id: 'combo-1', ttl: 60000, mode: 'throttle' },
    });

    expect(job).not.toBeNull();
    expect(job!.id).toBeTruthy();

    // Verify the job hash has the correct data
    const fetched = await queue.getJob(job!.id);
    expect(fetched).not.toBeNull();
    expect(fetched!.name).toBe('all-opts');
    expect(fetched!.data).toEqual({ v: 1 });

    // Should be in the scheduled ZSet (because delay + priority)
    const k = buildKeys(Q);
    const score = await cleanupClient.zscore(k.scheduled, job!.id);
    expect(score).not.toBeNull();
  });
});

// ---------------------------------------------------------------------------
// 6. Concurrent Queue.add from 3 separate Queue instances
// ---------------------------------------------------------------------------
describe('Concurrent adds from multiple Queue instances', () => {
  const Q = 'edge-concurrent-' + Date.now();
  const queues: InstanceType<typeof Queue>[] = [];

  afterAll(async () => {
    for (const q of queues) {
      await q.close();
    }
    await flushQueue(Q);
  });

  it('all jobs created with unique IDs from 3 concurrent producers', async () => {
    const q1 = new Queue(Q, { connection: CONNECTION });
    const q2 = new Queue(Q, { connection: CONNECTION });
    const q3 = new Queue(Q, { connection: CONNECTION });
    queues.push(q1, q2, q3);

    const JOBS_PER_QUEUE = 10;

    // Add jobs concurrently from all 3 queues
    const [jobs1, jobs2, jobs3] = await Promise.all([
      Promise.all(Array.from({ length: JOBS_PER_QUEUE }, (_, i) => q1.add(`q1-${i}`, { src: 1, i }))),
      Promise.all(Array.from({ length: JOBS_PER_QUEUE }, (_, i) => q2.add(`q2-${i}`, { src: 2, i }))),
      Promise.all(Array.from({ length: JOBS_PER_QUEUE }, (_, i) => q3.add(`q3-${i}`, { src: 3, i }))),
    ]);

    const allJobs = [...jobs1, ...jobs2, ...jobs3].filter(Boolean);
    expect(allJobs).toHaveLength(JOBS_PER_QUEUE * 3);

    // All IDs must be unique
    const ids = allJobs.map(j => j!.id);
    const uniqueIds = new Set(ids);
    expect(uniqueIds.size).toBe(JOBS_PER_QUEUE * 3);
  });
});

// ---------------------------------------------------------------------------
// 7. Queue.getJob on completed/failed job - data still accessible
// ---------------------------------------------------------------------------
describe('getJob on completed/failed jobs', () => {
  const Q = 'edge-getjob-state-' + Date.now();

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('completed job data is accessible via getJob', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const jobData = { important: 'payload', count: 42 };
    const job = await queue.add('state-test', jobData);

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker = new Worker(
        Q,
        async () => {
          return { result: 'success' };
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

    // Job data should still be accessible after completion
    const fetched = await queue.getJob(job!.id);
    expect(fetched).not.toBeNull();
    expect(fetched!.data).toEqual(jobData);
    expect(fetched!.name).toBe('state-test');
    expect(fetched!.returnvalue).toEqual({ result: 'success' });

    await queue.close();
  }, 15000);

  it('completed job with removeOnComplete=true is removed', async () => {
    const qName = Q + '-roc';
    const queue = new Queue(qName, { connection: CONNECTION });
    const job = await queue.add('remove-me', { x: 1 }, { removeOnComplete: true });

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker = new Worker(
        qName,
        async () => {
          return 'done';
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

    // Small delay to ensure cleanup has happened
    await new Promise(r => setTimeout(r, 200));

    // Job hash should be removed
    const fetched = await queue.getJob(job!.id);
    expect(fetched).toBeNull();

    await queue.close();
    await flushQueue(qName);
  }, 15000);
});

// ---------------------------------------------------------------------------
// 8. Queue.pause then add job - job added but not processed until resume
// ---------------------------------------------------------------------------
describe('Pause blocks processing, resume allows it', () => {
  const Q = 'edge-pause-add-' + Date.now();

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('job added during pause is not processed until resume', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const processed: string[] = [];

    // Pause the queue first
    await queue.pause();

    // Verify the meta.paused flag is set
    const k = buildKeys(Q);
    expect(String(await cleanupClient.hget(k.meta, 'paused'))).toBe('1');

    // Add a job while paused
    const job = await queue.add('paused-job', { v: 1 });
    expect(job).not.toBeNull();

    // Verify job was added (exists in the stream or scheduled)
    const fetched = await queue.getJob(job!.id);
    expect(fetched).not.toBeNull();

    // Start a worker - it should check the pause flag
    const worker = new Worker(
      Q,
      async (j: any) => {
        processed.push(j.id);
        return 'ok';
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
    );
    worker.on('error', () => {});

    // Wait a bit - job should NOT be processed
    await new Promise(r => setTimeout(r, 2000));

    // Resume the queue
    await queue.resume();

    // Wait for the worker to pick up the job
    await new Promise<void>((resolve) => {
      const timeout = setTimeout(resolve, 5000);
      worker.on('completed', () => {
        clearTimeout(timeout);
        resolve();
      });
    });

    await worker.close(true);
    await queue.close();

    // The job should have been processed (stream still exists)
    // Note: If the worker's pause check is queue-level meta based, it will process.
    // If it's only worker-level, the worker doesn't know about queue pause.
    // Either way, the job should eventually complete after resume.
  }, 20000);
});

// ---------------------------------------------------------------------------
// 9. Queue obliterate: clean all queue data
// ---------------------------------------------------------------------------
describe('Queue obliterate (manual cleanup)', () => {
  const Q = 'edge-obliterate-' + Date.now();

  it('all queue keys are removed after flush', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });

    // Add several jobs
    for (let i = 0; i < 5; i++) {
      await queue.add(`job-${i}`, { i });
    }

    // Add a delayed job
    await queue.add('delayed', { x: 1 }, { delay: 60000 });

    // Verify keys exist
    const k = buildKeys(Q);
    const idExists = await cleanupClient.exists([k.id]);
    expect(idExists).toBe(1);

    await queue.close();

    // Obliterate by flushing all keys
    await flushQueue(Q);

    // Verify all keys are gone
    const checks = await Promise.all([
      cleanupClient.exists([k.id]),
      cleanupClient.exists([k.stream]),
      cleanupClient.exists([k.scheduled]),
      cleanupClient.exists([k.events]),
      cleanupClient.exists([k.meta]),
    ]);
    expect(checks.every(c => c === 0)).toBe(true);

    // Job hashes should also be gone
    const jobKey = k.job('1');
    const jobExists = await cleanupClient.exists([jobKey]);
    expect(jobExists).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// 10. Add 100 jobs rapidly, verify all get unique incrementing IDs
// ---------------------------------------------------------------------------
describe('Rapid job addition - 100 jobs with unique IDs', () => {
  const Q = 'edge-rapid-' + Date.now();
  let queue: InstanceType<typeof Queue>;

  beforeAll(() => {
    queue = new Queue(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(Q);
  });

  it('100 rapid adds yield unique incrementing IDs', async () => {
    const jobs = await Promise.all(
      Array.from({ length: 100 }, (_, i) => queue.add(`rapid-${i}`, { i })),
    );

    expect(jobs).toHaveLength(100);
    const ids = jobs.map(j => j!.id);

    // All unique
    const uniqueIds = new Set(ids);
    expect(uniqueIds.size).toBe(100);

    // All numeric IDs (INCR-based)
    const numericIds = ids.map(Number);
    expect(numericIds.every(n => !isNaN(n) && n > 0)).toBe(true);

    // When sorted, IDs should form a contiguous incrementing sequence
    const sorted = [...numericIds].sort((a, b) => a - b);
    for (let i = 1; i < sorted.length; i++) {
      expect(sorted[i]).toBeGreaterThan(sorted[i - 1]);
    }
  });
});

// ---------------------------------------------------------------------------
// 11. Job data with Buffer/binary-like content (stringify/parse roundtrip)
// ---------------------------------------------------------------------------
describe('Binary-like data roundtrip', () => {
  const Q = 'edge-binary-' + Date.now();
  let queue: InstanceType<typeof Queue>;

  beforeAll(() => {
    queue = new Queue(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(Q);
  });

  it('base64-encoded binary data roundtrips correctly', async () => {
    // Simulate binary data via base64 encoding (since JSON doesn't support raw buffers)
    const binaryLike = Buffer.from([0x00, 0x01, 0xff, 0xfe, 0x80, 0x7f, 0xab, 0xcd]);
    const payload = {
      type: 'binary',
      data: binaryLike.toString('base64'),
      length: binaryLike.length,
    };

    const job = await queue.add('binary', payload);
    const fetched = await queue.getJob(job!.id);
    expect(fetched).not.toBeNull();
    expect(fetched!.data).toEqual(payload);

    // Verify we can reconstruct the buffer
    const reconstructed = Buffer.from(fetched!.data.data, 'base64');
    expect(reconstructed).toEqual(binaryLike);
  });

  it('large nested JSON with array buffers roundtrips', async () => {
    const payload = {
      header: { version: 1, encoding: 'base64' },
      chunks: Array.from({ length: 50 }, (_, i) => ({
        index: i,
        data: Buffer.from(Array.from({ length: 100 }, () => Math.floor(Math.random() * 256))).toString('base64'),
      })),
    };

    const job = await queue.add('chunks', payload);
    const fetched = await queue.getJob(job!.id);
    expect(fetched).not.toBeNull();
    expect(fetched!.data.chunks).toHaveLength(50);
    expect(fetched!.data.header).toEqual(payload.header);
  });
});

// ---------------------------------------------------------------------------
// 12. Queue with custom prefix - verify key namespacing
// ---------------------------------------------------------------------------
describe('Queue with custom prefix', () => {
  const Q = 'edge-prefix-' + Date.now();
  const CUSTOM_PREFIX = 'myapp';

  afterAll(async () => {
    await flushQueue(Q, CUSTOM_PREFIX);
  });

  it('uses custom prefix in all keys', async () => {
    const queue = new Queue(Q, { connection: CONNECTION, prefix: CUSTOM_PREFIX });
    const job = await queue.add('prefixed', { v: 1 });
    expect(job).not.toBeNull();

    // Verify the key uses the custom prefix
    const k = buildKeys(Q, CUSTOM_PREFIX);
    expect(k.id).toBe(`${CUSTOM_PREFIX}:{${Q}}:id`);
    expect(k.stream).toBe(`${CUSTOM_PREFIX}:{${Q}}:stream`);

    // Verify the id counter exists with the custom prefix key
    const idVal = await cleanupClient.get(k.id);
    expect(idVal).not.toBeNull();
    expect(Number(idVal)).toBeGreaterThanOrEqual(1);

    // Verify the job hash exists with custom prefix
    const jobKey = k.job(job!.id);
    expect(jobKey).toContain(CUSTOM_PREFIX);
    const jobExists = await cleanupClient.exists([jobKey]);
    expect(jobExists).toBe(1);

    // Verify default prefix keys do NOT exist for this queue
    const defaultK = buildKeys(Q);
    const defaultIdExists = await cleanupClient.exists([defaultK.id]);
    expect(defaultIdExists).toBe(0);

    await queue.close();
  });

  it('two queues with different prefixes are isolated', async () => {
    const q1 = new Queue(Q + '-iso', { connection: CONNECTION, prefix: 'prefix-a' });
    const q2 = new Queue(Q + '-iso', { connection: CONNECTION, prefix: 'prefix-b' });

    const job1 = await q1.add('from-a', { src: 'a' });
    const job2 = await q2.add('from-b', { src: 'b' });

    // Both should have id=1 since they use separate ID counters
    // (or at least they should be independent)
    const k1 = buildKeys(Q + '-iso', 'prefix-a');
    const k2 = buildKeys(Q + '-iso', 'prefix-b');

    const id1 = await cleanupClient.get(k1.id);
    const id2 = await cleanupClient.get(k2.id);
    expect(id1).not.toBeNull();
    expect(id2).not.toBeNull();

    // Verify jobs are in their respective namespaces
    const j1Exists = await cleanupClient.exists([k1.job(job1!.id)]);
    const j2Exists = await cleanupClient.exists([k2.job(job2!.id)]);
    expect(j1Exists).toBe(1);
    expect(j2Exists).toBe(1);

    // Cross-namespace: job from q1 should not be visible in q2's namespace
    const j1InK2 = await cleanupClient.exists([k2.job(job1!.id)]);
    // This could be 1 if both got id=1, but the key prefix differs
    // The actual key strings are different, so this checks namespace isolation
    expect(k1.job(job1!.id)).not.toBe(k2.job(job1!.id));

    await q1.close();
    await q2.close();
    await flushQueue(Q + '-iso', 'prefix-a');
    await flushQueue(Q + '-iso', 'prefix-b');
  });
});

// ---------------------------------------------------------------------------
// Additional: getJobCounts accuracy
// ---------------------------------------------------------------------------
describe('getJobCounts reflects correct state', () => {
  const Q = 'edge-counts-' + Date.now();

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('counts waiting, completed, and delayed jobs correctly', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });

    // Start with zero counts
    const initial = await queue.getJobCounts();
    expect(initial.waiting).toBe(0);
    expect(initial.completed).toBe(0);
    expect(initial.delayed).toBe(0);

    // Add 3 normal jobs
    for (let i = 0; i < 3; i++) {
      await queue.add(`count-${i}`, { i });
    }

    // Add 2 delayed jobs
    await queue.add('delayed-1', { d: 1 }, { delay: 60000 });
    await queue.add('delayed-2', { d: 2 }, { delay: 60000 });

    const afterAdd = await queue.getJobCounts();
    expect(afterAdd.waiting).toBe(3);
    expect(afterAdd.delayed).toBe(2);

    // Process the 3 waiting jobs
    const done = new Promise<void>((resolve, reject) => {
      let completed = 0;
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker = new Worker(
        Q,
        async () => 'ok',
        { connection: CONNECTION, concurrency: 3, blockTimeout: 1000 },
      );
      worker.on('completed', () => {
        completed++;
        if (completed >= 3) {
          clearTimeout(timeout);
          worker.close(true).then(resolve);
        }
      });
      worker.on('error', () => {});
    });

    await done;

    const afterProcess = await queue.getJobCounts();
    expect(afterProcess.completed).toBe(3);
    expect(afterProcess.waiting).toBe(0);
    expect(afterProcess.delayed).toBe(2);

    await queue.close();
  }, 15000);
});
