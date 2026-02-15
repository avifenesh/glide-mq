/**
 * Compression integration tests.
 * Verifies gzip compression of job data via Queue.add / Worker processing / Job.fromHash.
 *
 * Requires: valkey-server on :6379 and cluster on :7000-7005
 */
import { it, expect, beforeAll, afterAll } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

describeEachMode('Compression - roundtrip', (CONNECTION) => {
  const Q = 'test-compress-rt-' + Date.now();
  let queue: InstanceType<typeof Queue>;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
    queue = new Queue(Q, { connection: CONNECTION, compression: 'gzip' });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('compressed job data roundtrips correctly through getJob', async () => {
    const payload = { user: 'alice', items: [1, 2, 3], nested: { deep: true } };
    const job = await queue.add('compress-test', payload);
    expect(job).not.toBeNull();

    // Verify the raw hash value in Valkey is compressed (gz: prefix)
    const keys = buildKeys(Q);
    const rawData = await cleanupClient.hget(keys.job(job!.id), 'data');
    expect(String(rawData).startsWith('gz:')).toBe(true);

    // Verify getJob decompresses correctly
    const fetched = await queue.getJob(job!.id);
    expect(fetched).not.toBeNull();
    expect(fetched!.data).toEqual(payload);
  });

  it('compressed job data roundtrips through Worker processing', async () => {
    const payload = { action: 'send-email', to: 'bob@test.com', body: 'hello world' };
    const job = await queue.add('worker-compress', payload);
    expect(job).not.toBeNull();

    let receivedData: any = null;
    const worker = new Worker(
      Q,
      async (j: any) => {
        receivedData = j.data;
        return 'done';
      },
      { connection: CONNECTION },
    );

    await worker.waitUntilReady();
    await job!.waitUntilFinished(200, 10000);
    await worker.close();

    expect(receivedData).toEqual(payload);
  });
});

describeEachMode('Compression - large payload', (CONNECTION) => {
  const Q = 'test-compress-large-' + Date.now();
  let queue: InstanceType<typeof Queue>;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
    queue = new Queue(Q, { connection: CONNECTION, compression: 'gzip' });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('100KB payload compresses significantly', async () => {
    // Generate ~100KB of repetitive JSON data (compresses well)
    const bigArray = Array.from({ length: 5000 }, (_, i) => ({
      index: i,
      label: `item-${i}`,
      value: Math.random(),
    }));
    const payload = { records: bigArray };
    const rawSize = JSON.stringify(payload).length;
    expect(rawSize).toBeGreaterThan(100_000);

    const job = await queue.add('big-job', payload);
    expect(job).not.toBeNull();

    // Check compressed size in Valkey
    const keys = buildKeys(Q);
    const rawData = String(await cleanupClient.hget(keys.job(job!.id), 'data'));
    expect(rawData.startsWith('gz:')).toBe(true);
    // gz: prefix (3) + base64 of gzipped data should be much smaller
    expect(rawData.length).toBeLessThan(rawSize * 0.5);

    // Verify roundtrip
    const fetched = await queue.getJob(job!.id);
    expect(fetched!.data).toEqual(payload);
  });
});

describeEachMode('Compression - backward compatibility', (CONNECTION) => {
  const Q = 'test-compress-compat-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('uncompressed jobs still work when compression is not set', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const payload = { plain: true, count: 42 };
    const job = await queue.add('no-compress', payload);

    // Raw data should be plain JSON, not gz: prefixed
    const keys = buildKeys(Q);
    const rawData = String(await cleanupClient.hget(keys.job(job!.id), 'data'));
    expect(rawData.startsWith('gz:')).toBe(false);
    expect(JSON.parse(rawData)).toEqual(payload);

    const fetched = await queue.getJob(job!.id);
    expect(fetched!.data).toEqual(payload);
    await queue.close();
  });

  it('worker reads uncompressed jobs correctly even when queue used compression=none', async () => {
    const queue = new Queue(Q + '-mixed', { connection: CONNECTION });
    const payload = { legacy: true };
    const job = await queue.add('legacy-job', payload);

    let receivedData: any = null;
    const worker = new Worker(
      Q + '-mixed',
      async (j: any) => {
        receivedData = j.data;
        return 'ok';
      },
      { connection: CONNECTION },
    );

    await worker.waitUntilReady();
    await job!.waitUntilFinished(200, 10000);
    await worker.close();
    await queue.close();
    await flushQueue(cleanupClient, Q + '-mixed');

    expect(receivedData).toEqual(payload);
  });
});

describeEachMode('Compression - mixed mode', (CONNECTION) => {
  const Q = 'test-compress-mixed-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('worker processes both compressed and uncompressed jobs in same queue', async () => {
    // Add an uncompressed job first
    const plainQueue = new Queue(Q, { connection: CONNECTION });
    await plainQueue.add('plain', { type: 'uncompressed' });
    await plainQueue.close();

    // Add a compressed job to the same queue
    const gzQueue = new Queue(Q, { connection: CONNECTION, compression: 'gzip' });
    await gzQueue.add('compressed', { type: 'compressed' });
    await gzQueue.close();

    // Worker should handle both - collect results via events
    const results: Record<string, any> = {};
    let completed = 0;
    const done = new Promise<void>((resolve) => {
      const worker = new Worker(
        Q,
        async (j: any) => {
          results[j.name] = j.data;
          return j.name;
        },
        { connection: CONNECTION },
      );
      worker.on('completed', () => {
        completed++;
        if (completed >= 2) {
          worker.close().then(resolve);
        }
      });
    });

    await done;

    expect(results['plain']).toEqual({ type: 'uncompressed' });
    expect(results['compressed']).toEqual({ type: 'compressed' });
  });
});

describeEachMode('Compression - addBulk', (CONNECTION) => {
  const Q = 'test-compress-bulk-' + Date.now();
  let queue: InstanceType<typeof Queue>;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
    queue = new Queue(Q, { connection: CONNECTION, compression: 'gzip' });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('addBulk compresses all jobs', async () => {
    const jobs = await queue.addBulk([
      { name: 'b1', data: { i: 1, msg: 'first' } },
      { name: 'b2', data: { i: 2, msg: 'second' } },
    ]);
    expect(jobs).toHaveLength(2);

    const keys = buildKeys(Q);
    for (const job of jobs) {
      const rawData = String(await cleanupClient.hget(keys.job(job.id), 'data'));
      expect(rawData.startsWith('gz:')).toBe(true);

      const fetched = await queue.getJob(job.id);
      expect(fetched).not.toBeNull();
    }

    // Verify data integrity
    const f1 = await queue.getJob(jobs[0].id);
    const f2 = await queue.getJob(jobs[1].id);
    expect(f1!.data).toEqual({ i: 1, msg: 'first' });
    expect(f2!.data).toEqual({ i: 2, msg: 'second' });
  });
});
