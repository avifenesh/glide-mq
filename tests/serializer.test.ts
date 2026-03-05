/**
 * Pluggable Serializer tests.
 *
 * Part 1: In-memory tests (TestQueue/TestWorker) - no Valkey needed.
 * Part 2: Integration tests (real Queue/Worker) - requires Valkey on :6379 and cluster on :7000-7005.
 *
 * Run: npx vitest run tests/serializer.test.ts
 */
import { describe, it, expect, afterEach, beforeAll, afterAll } from 'vitest';
import { TestQueue, TestWorker } from '../src/testing';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { FlowProducer } = require('../dist/flow-producer') as typeof import('../src/flow-producer');
const { JSON_SERIALIZER } = require('../dist/types') as typeof import('../src/types');
import type { Serializer } from '../src/types';

import { describeEachMode, createCleanupClient, flushQueue, waitFor } from './helpers/fixture';

// ---- Custom serializer for testing ----
// Prefixes serialized data with "REV:" and reverses the JSON string.
// This is intentionally weird to verify the serializer is actually used.
const reverseSerializer: Serializer = {
  serialize(data: unknown): string {
    const json = JSON.stringify(data);
    return 'REV:' + json.split('').reverse().join('');
  },
  deserialize(raw: string): unknown {
    if (!raw.startsWith('REV:')) {
      throw new Error(`Expected REV: prefix, got: ${raw.substring(0, 10)}`);
    }
    const reversed = raw.slice(4).split('').reverse().join('');
    return JSON.parse(reversed);
  },
};

// ---- Part 1: In-memory tests ----

describe('Serializer - TestQueue (in-memory)', () => {
  let queue: TestQueue;

  afterEach(async () => {
    if (queue) await queue.close();
  });

  it('default serializer roundtrips data like JSON', async () => {
    queue = new TestQueue('ser-default');
    const job = await queue.add('test', { hello: 'world', num: 42 });
    expect(job).not.toBeNull();
    expect(job!.data).toEqual({ hello: 'world', num: 42 });
  });

  it('custom serializer roundtrips data through serialize/deserialize', async () => {
    queue = new TestQueue('ser-custom', { serializer: reverseSerializer });
    const payload = { user: 'alice', items: [1, 2, 3] };
    const job = await queue.add('test', payload);
    expect(job).not.toBeNull();
    // Data should be equivalent after roundtrip
    expect(job!.data).toEqual(payload);
  });

  it('custom serializer strips non-serializable values (like undefined)', async () => {
    queue = new TestQueue('ser-strip', { serializer: reverseSerializer });
    const payload = { a: 1, b: undefined, c: 'yes' };
    const job = await queue.add('test', payload);
    expect(job).not.toBeNull();
    // undefined is dropped by JSON.stringify inside reverseSerializer
    expect(job!.data).toEqual({ a: 1, c: 'yes' });
  });

  it('custom serializer is used by TestWorker processing', async () => {
    queue = new TestQueue('ser-worker', { serializer: reverseSerializer });
    const worker = new TestWorker(queue, async (job) => {
      return { echo: job.data };
    });

    const payload = { x: 'hello' };
    await queue.add('test', payload);

    await waitFor(async () => {
      const counts = await queue.getJobCounts();
      return counts.completed === 1;
    });

    const completed = await queue.getJobs('completed');
    expect(completed).toHaveLength(1);
    expect(completed[0].data).toEqual(payload);
    expect(completed[0].returnvalue).toEqual({ echo: payload });
    await worker.close();
  });

  it('addBulk roundtrips all jobs through serializer', async () => {
    queue = new TestQueue('ser-bulk', { serializer: reverseSerializer });
    const jobs = await queue.addBulk([
      { name: 'a', data: { v: 1 } },
      { name: 'b', data: { v: 2 } },
    ]);
    expect(jobs).toHaveLength(2);
    expect(jobs[0].data).toEqual({ v: 1 });
    expect(jobs[1].data).toEqual({ v: 2 });
  });
});

// ---- Part 2: JSON_SERIALIZER constant ----

describe('JSON_SERIALIZER', () => {
  it('serializes and deserializes correctly', () => {
    const data = { hello: 'world', nested: { arr: [1, 2, 3] } };
    const serialized = JSON_SERIALIZER.serialize(data);
    expect(serialized).toBe(JSON.stringify(data));
    expect(JSON_SERIALIZER.deserialize(serialized)).toEqual(data);
  });

  it('handles null, numbers, strings, booleans', () => {
    expect(JSON_SERIALIZER.deserialize(JSON_SERIALIZER.serialize(null))).toBeNull();
    expect(JSON_SERIALIZER.deserialize(JSON_SERIALIZER.serialize(42))).toBe(42);
    expect(JSON_SERIALIZER.deserialize(JSON_SERIALIZER.serialize('hello'))).toBe('hello');
    expect(JSON_SERIALIZER.deserialize(JSON_SERIALIZER.serialize(true))).toBe(true);
  });
});

// ---- Part 3: Integration tests with real Valkey ----

describeEachMode('Serializer - Queue + Worker', (CONNECTION) => {
  const Q = 'test-serializer-' + Date.now();
  let queue: InstanceType<typeof Queue>;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
    queue = new Queue(Q, { connection: CONNECTION, serializer: reverseSerializer });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('custom serializer stores non-JSON data in Valkey', async () => {
    const payload = { user: 'alice', score: 99 };
    const job = await queue.add('ser-store', payload);
    expect(job).not.toBeNull();

    // Read raw hash from Valkey to verify the serializer was used
    const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
    const keys = buildKeys(Q);
    const rawData = await cleanupClient.hget(keys.job(job!.id), 'data');
    const rawStr = String(rawData);
    // Should NOT be plain JSON - should have REV: prefix
    expect(rawStr.startsWith('REV:')).toBe(true);
    // Should NOT parse as plain JSON
    expect(() => JSON.parse(rawStr)).toThrow();
  });

  it('getJob deserializes with custom serializer', async () => {
    const payload = { items: ['a', 'b', 'c'], nested: { deep: true } };
    const job = await queue.add('ser-getjob', payload);
    const fetched = await queue.getJob(job!.id);
    expect(fetched).not.toBeNull();
    expect(fetched!.data).toEqual(payload);
  });

  it('Worker processes and returns with custom serializer', async () => {
    const payload = { action: 'process', value: 42 };
    const job = await queue.add('ser-worker', payload);

    let receivedData: any = null;
    const worker = new Worker(
      Q,
      async (j: any) => {
        receivedData = j.data;
        return { processed: true, input: j.data.value };
      },
      { connection: CONNECTION, serializer: reverseSerializer },
    );

    await worker.waitUntilReady();
    await job!.waitUntilFinished(200, 10000);
    await worker.close();

    expect(receivedData).toEqual(payload);

    // Verify returnvalue was stored with serializer
    const fetched = await queue.getJob(job!.id);
    expect(fetched!.returnvalue).toEqual({ processed: true, input: 42 });
  });

  it('searchJobs works with custom serializer', async () => {
    const payload = { searchKey: 'findme-' + Date.now() };
    await queue.add('ser-search', payload);

    const results = await queue.searchJobs({ name: 'ser-search', data: { searchKey: payload.searchKey } });
    expect(results.length).toBeGreaterThanOrEqual(1);
    expect(results[0].data).toEqual(payload);
  });
});

describeEachMode('Serializer - FlowProducer', (CONNECTION) => {
  // Use the same queue name for parent and children to avoid CrossSlot errors in cluster mode.
  // All keys hash to the same slot because of the {queueName} hash tag.
  const Q = 'test-serializer-flow-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('FlowProducer serializes parent and child data with custom serializer', async () => {
    const flow = new FlowProducer({
      connection: CONNECTION,
      serializer: reverseSerializer,
    });

    const result = await flow.add({
      name: 'parent-job',
      queueName: Q,
      data: { role: 'parent' },
      children: [
        { name: 'child-1', queueName: Q, data: { role: 'child', idx: 1 } },
        { name: 'child-2', queueName: Q, data: { role: 'child', idx: 2 } },
      ],
    });

    expect(result.job).toBeDefined();
    expect(result.children).toHaveLength(2);

    // Verify raw data in Valkey uses custom serializer
    const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
    const keys = buildKeys(Q);
    const rawParentData = String(await cleanupClient.hget(keys.job(result.job.id), 'data'));
    expect(rawParentData.startsWith('REV:')).toBe(true);

    const rawChildData = String(await cleanupClient.hget(keys.job(result.children![0].job.id), 'data'));
    expect(rawChildData.startsWith('REV:')).toBe(true);

    await flow.close();
  });
});

describeEachMode('Serializer - updateData', (CONNECTION) => {
  const Q = 'test-ser-updatedata-' + Date.now();
  let queue: InstanceType<typeof Queue>;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
    queue = new Queue(Q, { connection: CONNECTION, serializer: reverseSerializer });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('updateData persists with custom serializer and roundtrips via getJob', async () => {
    const job = await queue.add('upd-test', { original: true });

    // Worker calls updateData during processing
    let updateDone = false;
    const worker = new Worker(
      Q,
      async (j: any) => {
        await j.updateData({ modified: true, value: 99 });
        updateDone = true;
        return 'ok';
      },
      { connection: CONNECTION, serializer: reverseSerializer },
    );

    await worker.waitUntilReady();
    await job!.waitUntilFinished(200, 10000);
    await worker.close();

    expect(updateDone).toBe(true);

    // Verify the raw data in Valkey uses custom serializer
    const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
    const keys = buildKeys(Q);
    const rawData = String(await cleanupClient.hget(keys.job(job!.id), 'data'));
    expect(rawData.startsWith('REV:')).toBe(true);

    // Verify getJob reads it back correctly
    const fetched = await queue.getJob(job!.id);
    expect(fetched!.data).toEqual({ modified: true, value: 99 });
  });
});

describeEachMode('Serializer - getChildrenValues', (CONNECTION) => {
  const Q = 'test-ser-children-' + Date.now();
  let queue: InstanceType<typeof Queue>;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
    queue = new Queue(Q, { connection: CONNECTION, serializer: reverseSerializer });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('parent getChildrenValues deserializes child return values with custom serializer', async () => {
    const flow = new FlowProducer({
      connection: CONNECTION,
      serializer: reverseSerializer,
    });

    const result = await flow.add({
      name: 'parent',
      queueName: Q,
      data: { role: 'parent' },
      children: [
        { name: 'child-a', queueName: Q, data: { role: 'child', idx: 1 } },
        { name: 'child-b', queueName: Q, data: { role: 'child', idx: 2 } },
      ],
    });

    let capturedChildValues: any = null;
    const worker = new Worker(
      Q,
      async (j: any) => {
        if (j.name === 'parent') {
          capturedChildValues = await j.getChildrenValues();
          return { parentDone: true };
        }
        return { childResult: j.data.idx * 10 };
      },
      { connection: CONNECTION, serializer: reverseSerializer },
    );

    await worker.waitUntilReady();

    // Wait for parent to complete (children complete first, then parent unblocks)
    await waitFor(async () => {
      const fetched = await queue.getJob(result.job.id);
      return fetched?.finishedOn != null;
    }, 15000);

    await worker.close();
    await flow.close();

    // capturedChildValues should have deserialized return values from both children
    expect(capturedChildValues).not.toBeNull();
    const values = Object.values(capturedChildValues) as any[];
    expect(values).toHaveLength(2);
    // Children returned { childResult: 10 } and { childResult: 20 }
    const sorted = values.sort((a: any, b: any) => a.childResult - b.childResult);
    expect(sorted[0]).toEqual({ childResult: 10 });
    expect(sorted[1]).toEqual({ childResult: 20 });
  });
});

describeEachMode('Serializer - addBulk integration', (CONNECTION) => {
  const Q = 'test-ser-addbulk-' + Date.now();
  let queue: InstanceType<typeof Queue>;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
    queue = new Queue(Q, { connection: CONNECTION, serializer: reverseSerializer });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('addBulk stores all jobs with custom serializer', async () => {
    const jobs = await queue.addBulk([
      { name: 'bulk-a', data: { v: 'first' } },
      { name: 'bulk-b', data: { v: 'second' } },
      { name: 'bulk-c', data: { v: 'third' } },
    ]);
    expect(jobs).toHaveLength(3);

    // Verify raw data in Valkey uses custom serializer for all jobs
    const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
    const keys = buildKeys(Q);
    for (const job of jobs) {
      const rawData = String(await cleanupClient.hget(keys.job(job.id), 'data'));
      expect(rawData.startsWith('REV:')).toBe(true);
    }

    // Verify getJob roundtrip for each
    for (let i = 0; i < jobs.length; i++) {
      const fetched = await queue.getJob(jobs[i].id);
      expect(fetched!.data).toEqual(jobs[i].data);
    }
  });
});

describeEachMode('Serializer - DLQ', (CONNECTION) => {
  const Q = 'test-ser-dlq-' + Date.now();
  const DLQ = Q + '-dlq';
  let queue: InstanceType<typeof Queue>;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
    queue = new Queue(Q, {
      connection: CONNECTION,
      serializer: reverseSerializer,
      deadLetterQueue: { name: DLQ },
    });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(cleanupClient, Q);
    await flushQueue(cleanupClient, DLQ);
    cleanupClient.close();
  });

  it('DLQ jobs are readable despite custom serializer on source queue', async () => {
    const payload = { critical: true, id: Date.now() };
    const job = await queue.add('dlq-test', payload, { attempts: 1 });

    const failedPromise = new Promise<void>((resolve) => {
      const worker = new Worker(
        Q,
        async () => {
          throw new Error('permanent failure');
        },
        {
          connection: CONNECTION,
          serializer: reverseSerializer,
          deadLetterQueue: { name: DLQ },
          stalledInterval: 60000,
        },
      );
      worker.on('failed', async (_job: any, _err: any) => {
        // Wait a tick for DLQ write to complete
        setTimeout(async () => {
          await worker.close();
          resolve();
        }, 200);
      });
    });

    await failedPromise;

    // Read DLQ jobs - these should be readable (DLQ envelope is always JSON)
    const dlqJobs = await queue.getDeadLetterJobs();
    expect(dlqJobs.length).toBeGreaterThanOrEqual(1);

    // DLQ envelope contains the original data as a plain object (JSON-serialized in the envelope)
    const dlqJob = dlqJobs.find((j: any) => j.data?.originalJobId === job!.id);
    expect(dlqJob).toBeDefined();
    const dlqData = dlqJob!.data as any;
    expect(dlqData.originalQueue).toBe(Q);
    expect(dlqData.data).toEqual(payload);
    expect(dlqData.failedReason).toBe('permanent failure');
  });
});

describeEachMode('Serializer - compression combo', (CONNECTION) => {
  const Q = 'test-ser-compress-' + Date.now();
  let queue: InstanceType<typeof Queue>;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
    queue = new Queue(Q, {
      connection: CONNECTION,
      serializer: reverseSerializer,
      compression: 'gzip',
    });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('compression wraps custom serializer output', async () => {
    const payload = { compressed: true, data: 'hello world'.repeat(50) };
    const job = await queue.add('compress-ser', payload);

    // Raw value should be compressed (gz: prefix wrapping the serialized data)
    const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
    const keys = buildKeys(Q);
    const rawData = String(await cleanupClient.hget(keys.job(job!.id), 'data'));
    expect(rawData.startsWith('gz:')).toBe(true);

    // getJob should decompress then deserialize correctly
    const fetched = await queue.getJob(job!.id);
    expect(fetched!.data).toEqual(payload);
  });

  it('Worker roundtrips compressed + serialized data', async () => {
    const payload = { action: 'compress-test', items: Array.from({ length: 100 }, (_, i) => i) };
    const job = await queue.add('compress-worker', payload);

    let receivedData: any = null;
    const worker = new Worker(
      Q,
      async (j: any) => {
        receivedData = j.data;
        return { ok: true };
      },
      { connection: CONNECTION, serializer: reverseSerializer, compression: 'gzip' } as any,
    );

    await worker.waitUntilReady();
    await job!.waitUntilFinished(200, 10000);
    await worker.close();

    expect(receivedData).toEqual(payload);
  });
});

describeEachMode('Serializer - backward compatibility', (CONNECTION) => {
  const Q = 'test-serializer-compat-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('no serializer option uses JSON (backward compatible)', async () => {
    const queue = new Queue(Q, { connection: CONNECTION });
    const payload = { compat: true, value: 123 };
    const job = await queue.add('compat-test', payload);

    const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
    const keys = buildKeys(Q);
    const rawData = String(await cleanupClient.hget(keys.job(job!.id), 'data'));
    // Should be plain JSON
    expect(JSON.parse(rawData)).toEqual(payload);

    const fetched = await queue.getJob(job!.id);
    expect(fetched!.data).toEqual(payload);
    await queue.close();
  });

  it('Worker without serializer processes JSON data', async () => {
    const queue = new Queue(Q + '-w', { connection: CONNECTION });
    const payload = { legacy: true };
    const job = await queue.add('legacy-test', payload);

    let receivedData: any = null;
    const worker = new Worker(
      Q + '-w',
      async (j: any) => {
        receivedData = j.data;
        return { ok: true };
      },
      { connection: CONNECTION },
    );

    await worker.waitUntilReady();
    await job!.waitUntilFinished(200, 10000);
    await worker.close();
    await queue.close();
    await flushQueue(cleanupClient, Q + '-w');

    expect(receivedData).toEqual(payload);
  });
});
