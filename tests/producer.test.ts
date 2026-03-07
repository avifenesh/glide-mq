/**
 * Producer integration tests.
 * Verifies the lightweight Producer class produces jobs identical to Queue.add().
 *
 * Requires: valkey-server on :6379 and cluster on :7000-7005
 */
import { it, expect, beforeAll, afterAll } from 'vitest';

const { Producer } = require('../dist/producer') as typeof import('../src/producer');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

import { describeEachMode, createCleanupClient, flushQueue, waitFor } from './helpers/fixture';

describeEachMode('Producer - add()', (CONNECTION) => {
  const Q = 'test-producer-add-' + Date.now();
  let producer: InstanceType<typeof Producer>;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
    producer = new Producer(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    await producer.close();
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('returns a string ID, not a Job instance', async () => {
    const id = await producer.add('test-job', { key: 'value' });
    expect(typeof id).toBe('string');
    expect(id).not.toBeNull();
    expect(Number(id)).toBeGreaterThan(0);
  });

  it('stores job data in Valkey', async () => {
    const payload = { user: 'alice', count: 42 };
    const id = await producer.add('data-check', payload);
    expect(id).not.toBeNull();

    const keys = buildKeys(Q);
    const rawData = await cleanupClient.hget(keys.job(id!), 'data');
    expect(JSON.parse(String(rawData))).toEqual(payload);
  });

  it('stores job name in Valkey', async () => {
    const id = await producer.add('named-job', { x: 1 });
    expect(id).not.toBeNull();

    const keys = buildKeys(Q);
    const rawName = await cleanupClient.hget(keys.job(id!), 'name');
    expect(String(rawName)).toBe('named-job');
  });

  it('supports delay option', async () => {
    const id = await producer.add('delayed-job', { d: 1 }, { delay: 60000 });
    expect(id).not.toBeNull();
    expect(typeof id).toBe('string');
  });

  it('supports priority option', async () => {
    const id = await producer.add('priority-job', { p: 1 }, { priority: 5 });
    expect(id).not.toBeNull();
    expect(typeof id).toBe('string');
  });

  it('supports custom jobId', async () => {
    const customId = 'custom-' + Date.now();
    const id = await producer.add('custom-id-job', { c: 1 }, { jobId: customId });
    expect(id).toBe(customId);
  });

  it('returns null for duplicate custom jobId', async () => {
    const customId = 'dup-custom-' + Date.now();
    const id1 = await producer.add('first', { a: 1 }, { jobId: customId });
    expect(id1).toBe(customId);

    const id2 = await producer.add('second', { b: 2 }, { jobId: customId });
    expect(id2).toBeNull();
  });

  it('validates jobId with invalid characters', async () => {
    await expect(producer.add('bad-id', {}, { jobId: 'has:colon' })).rejects.toThrow(
      'jobId must not contain control characters, curly braces, or colons',
    );
    await expect(producer.add('bad-id', {}, { jobId: 'has{brace' })).rejects.toThrow(
      'jobId must not contain control characters, curly braces, or colons',
    );
  });

  it('throws for oversized payload', async () => {
    const hugeData = { big: 'x'.repeat(1048577) };
    await expect(producer.add('huge', hugeData)).rejects.toThrow('Job data exceeds maximum size');
  });

  it('throws for invalid ttl', async () => {
    await expect(producer.add('bad-ttl', {}, { ttl: -1 })).rejects.toThrow(
      'ttl must be a non-negative finite number',
    );
    await expect(producer.add('bad-ttl', {}, { ttl: Infinity })).rejects.toThrow(
      'ttl must be a non-negative finite number',
    );
  });

  it('throws for invalid token bucket config', async () => {
    await expect(
      producer.add('bad-tb', {}, { ordering: { key: 'k', tokenBucket: { capacity: -1, refillRate: 1 } } }),
    ).rejects.toThrow('tokenBucket.capacity must be a positive finite number');
    await expect(
      producer.add('bad-tb', {}, { ordering: { key: 'k', tokenBucket: { capacity: 1, refillRate: 0 } } }),
    ).rejects.toThrow('tokenBucket.refillRate must be a positive finite number');
  });

  it('throws for invalid cost', async () => {
    await expect(producer.add('bad-cost', {}, { cost: -1 })).rejects.toThrow(
      'cost must be a non-negative finite number',
    );
  });

  it('validates ordering key length', async () => {
    const longKey = 'k'.repeat(257);
    await expect(producer.add('long-key', {}, { ordering: { key: longKey } })).rejects.toThrow(
      'Ordering key exceeds maximum length',
    );
  });

  it('throws after close()', async () => {
    const ephemeral = new Producer(Q, { connection: CONNECTION });
    await ephemeral.add('before-close', { ok: true });
    await ephemeral.close();
    await expect(ephemeral.add('after-close', {})).rejects.toThrow('Producer is closed');
  });
});

describeEachMode('Producer - deduplication', (CONNECTION) => {
  const Q = 'test-producer-dedup-' + Date.now();
  let producer: InstanceType<typeof Producer>;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
    producer = new Producer(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    await producer.close();
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('returns null for deduplicated job', async () => {
    const dedupId = 'dedup-' + Date.now();
    const id1 = await producer.add('dedup-first', { a: 1 }, { deduplication: { id: dedupId, ttl: 5000 } });
    expect(id1).not.toBeNull();

    const id2 = await producer.add('dedup-second', { b: 2 }, { deduplication: { id: dedupId, ttl: 5000 } });
    expect(id2).toBeNull();
  });
});

describeEachMode('Producer - compression', (CONNECTION) => {
  const Q = 'test-producer-compress-' + Date.now();
  let producer: InstanceType<typeof Producer>;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
    producer = new Producer(Q, { connection: CONNECTION, compression: 'gzip' });
  });

  afterAll(async () => {
    await producer.close();
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('stores compressed data with gz: prefix', async () => {
    const payload = { items: Array.from({ length: 100 }, (_, i) => i) };
    const id = await producer.add('compressed', payload);
    expect(id).not.toBeNull();

    const keys = buildKeys(Q);
    const rawData = await cleanupClient.hget(keys.job(id!), 'data');
    expect(String(rawData).startsWith('gz:')).toBe(true);
  });
});

describeEachMode('Producer - addBulk()', (CONNECTION) => {
  const Q = 'test-producer-bulk-' + Date.now();
  let producer: InstanceType<typeof Producer>;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
    producer = new Producer(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    await producer.close();
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('returns empty array for empty input', async () => {
    const result = await producer.addBulk([]);
    expect(result).toEqual([]);
  });

  it('returns array of string IDs', async () => {
    const ids = await producer.addBulk([
      { name: 'bulk-1', data: { i: 1 } },
      { name: 'bulk-2', data: { i: 2 } },
      { name: 'bulk-3', data: { i: 3 } },
    ]);
    expect(ids).toHaveLength(3);
    for (const id of ids) {
      expect(typeof id).toBe('string');
      expect(id).not.toBeNull();
    }
  });

  it('stores all jobs in Valkey', async () => {
    const ids = await producer.addBulk([
      { name: 'verify-1', data: { v: 'one' } },
      { name: 'verify-2', data: { v: 'two' } },
    ]);

    const keys = buildKeys(Q);
    for (let i = 0; i < ids.length; i++) {
      const rawName = await cleanupClient.hget(keys.job(ids[i]!), 'name');
      expect(String(rawName)).toBe(`verify-${i + 1}`);
    }
  });
});

describeEachMode('Producer - Worker interop', (CONNECTION) => {
  const Q = 'test-producer-interop-' + Date.now();
  let producer: InstanceType<typeof Producer>;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
    producer = new Producer(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    await producer.close();
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('jobs added via Producer are processed by Worker', async () => {
    const payload = { action: 'send-email', to: 'test@example.com' };
    const id = await producer.add('interop-job', payload);
    expect(id).not.toBeNull();

    let receivedData: any = null;
    let receivedName = '';
    const worker = new Worker(
      Q,
      async (job: any) => {
        receivedData = job.data;
        receivedName = job.name;
        return 'done';
      },
      { connection: CONNECTION },
    );

    try {
      await waitFor(() => receivedData !== null, 10000);
      expect(receivedData).toEqual(payload);
      expect(receivedName).toBe('interop-job');
    } finally {
      await worker.close();
    }
  });
});

describeEachMode('Producer - injected client', (CONNECTION) => {
  const Q = 'test-producer-injected-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('works with an injected client and does not close it', async () => {
    const producer = new Producer(Q, { client: cleanupClient, connection: CONNECTION });
    const id = await producer.add('injected-test', { x: 1 });
    expect(id).not.toBeNull();
    expect(typeof id).toBe('string');
    await producer.close();

    // The injected client should still be usable
    const keys = buildKeys(Q);
    const rawName = await cleanupClient.hget(keys.job(id!), 'name');
    expect(String(rawName)).toBe('injected-test');
  });
});

describeEachMode('Producer - constructor validation', (CONNECTION) => {
  it('throws without connection or client', () => {
    expect(() => new Producer('q', {} as any)).toThrow('Either `connection` or `client` must be provided.');
  });
});
