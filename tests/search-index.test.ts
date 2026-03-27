/**
 * Tests for Valkey Search / vector index integration.
 *
 * Structure:
 * 1. Unit tests (mocked): createJobIndex schema, storeVector buffer encoding
 * 2. Testing mode: TestQueue.createJobIndex + vectorSearch round-trip
 * 3. Integration tests: require valkey-bundle with search module loaded
 */
import { describe, it, expect, beforeAll, afterAll, vi, beforeEach, afterEach } from 'vitest';

const { GlideFt } = require('@glidemq/speedkey') as typeof import('@glidemq/speedkey');

// ---- Unit tests (mocked client) ----

describe('createJobIndex schema construction', () => {
  it('calls GlideFt.create with correct prefix and base fields', async () => {
    const createSpy = vi.spyOn(GlideFt, 'create').mockResolvedValue('OK');
    const listSpy = vi.spyOn(GlideFt, 'list').mockResolvedValue([]);

    const { Queue } = require('../dist/queue') as typeof import('../src/queue');
    const q = new Queue('idx-test-1', {
      connection: { addresses: [{ host: 'localhost', port: 6379 }] },
    });

    // Inject a mock client
    const mockClient = {} as any;
    (q as any).client = mockClient;
    (q as any).clientOwned = false;

    await q.createJobIndex({
      vectorField: { name: 'embedding', dimensions: 128, algorithm: 'HNSW', distanceMetric: 'COSINE' },
    });

    expect(createSpy).toHaveBeenCalledOnce();
    const [client, indexName, schema, options] = createSpy.mock.calls[0];
    expect(client).toBe(mockClient);
    expect(indexName).toBe('idx-test-1-idx');
    expect(options).toEqual({
      dataType: 'HASH',
      prefixes: ['glide:{idx-test-1'],
    });

    // Base fields: name, state, timestamp, priority + vector
    const fieldNames = schema.map((f: any) => f.name);
    expect(fieldNames).toContain('name');
    expect(fieldNames).toContain('state');
    expect(fieldNames).toContain('timestamp');
    expect(fieldNames).toContain('priority');
    expect(fieldNames).toContain('embedding');

    // Check vector field attributes
    const vecField = schema.find((f: any) => f.name === 'embedding');
    expect(vecField).toBeDefined();
    expect(vecField!.type).toBe('VECTOR');
    expect((vecField as any).attributes.algorithm).toBe('HNSW');
    expect((vecField as any).attributes.dimensions).toBe(128);
    expect((vecField as any).attributes.distanceMetric).toBe('COSINE');

    createSpy.mockRestore();
    listSpy.mockRestore();
    await q.close();
  });

  it('auto-adds dummy vector when no vectorField is provided', async () => {
    const createSpy = vi.spyOn(GlideFt, 'create').mockResolvedValue('OK');
    const listSpy = vi.spyOn(GlideFt, 'list').mockResolvedValue([]);

    const { Queue } = require('../dist/queue') as typeof import('../src/queue');
    const q = new Queue('idx-test-2', {
      connection: { addresses: [{ host: 'localhost', port: 6379 }] },
    });
    const mockClient = {} as any;
    (q as any).client = mockClient;
    (q as any).clientOwned = false;

    await q.createJobIndex();

    expect(createSpy).toHaveBeenCalledOnce();
    const schema = createSpy.mock.calls[0][2];
    const vecField = schema.find((f: any) => f.name === '_vec');
    expect(vecField).toBeDefined();
    expect(vecField!.type).toBe('VECTOR');
    expect((vecField as any).attributes.algorithm).toBe('FLAT');
    expect((vecField as any).attributes.dimensions).toBe(2);

    createSpy.mockRestore();
    listSpy.mockRestore();
    await q.close();
  });

  it('includes user-defined extra fields in schema', async () => {
    const createSpy = vi.spyOn(GlideFt, 'create').mockResolvedValue('OK');
    const listSpy = vi.spyOn(GlideFt, 'list').mockResolvedValue([]);

    const { Queue } = require('../dist/queue') as typeof import('../src/queue');
    const q = new Queue('idx-test-3', {
      connection: { addresses: [{ host: 'localhost', port: 6379 }] },
    });
    const mockClient = {} as any;
    (q as any).client = mockClient;
    (q as any).clientOwned = false;

    await q.createJobIndex({
      fields: [
        { type: 'TAG', name: 'category' },
        { type: 'NUMERIC', name: 'score' },
      ],
      vectorField: { name: 'vec', dimensions: 4 },
    });

    const schema = createSpy.mock.calls[0][2];
    const fieldNames = schema.map((f: any) => f.name);
    expect(fieldNames).toContain('category');
    expect(fieldNames).toContain('score');
    expect(fieldNames).toContain('vec');

    createSpy.mockRestore();
    listSpy.mockRestore();
    await q.close();
  });
});

describe('storeVector buffer encoding', () => {
  it('writes Float32Array buffer to job hash via hset', async () => {
    const { Job } = require('../dist/job') as typeof import('../src/job');
    const hsetCalls: any[] = [];
    const mockClient = {
      hset: async (key: string, fields: Record<string, any>) => {
        hsetCalls.push({ key, fields });
        return 1;
      },
    } as any;
    const mockKeys = {
      job: (id: string) => `glide:{test}:job:${id}`,
    } as any;

    const job = new Job(mockClient, mockKeys, '42', 'test-job', {}, {});

    const embedding = [1.0, 2.0, 3.0];
    await job.storeVector('vec', embedding);

    expect(hsetCalls).toHaveLength(1);
    expect(hsetCalls[0].key).toBe('glide:{test}:job:42');
    const buf = hsetCalls[0].fields.vec;
    expect(buf).toBeInstanceOf(Buffer);
    expect(buf.length).toBe(12); // 3 * 4 bytes

    // Verify the buffer content matches Float32Array
    const restored = new Float32Array(buf.buffer, buf.byteOffset, 3);
    expect(Array.from(restored)).toEqual([1.0, 2.0, 3.0]);
  });

  it('accepts Float32Array directly', async () => {
    const { Job } = require('../dist/job') as typeof import('../src/job');
    const hsetCalls: any[] = [];
    const mockClient = {
      hset: async (_key: string, fields: Record<string, any>) => {
        hsetCalls.push(fields);
        return 1;
      },
    } as any;
    const mockKeys = {
      job: (id: string) => `glide:{test}:job:${id}`,
    } as any;

    const job = new Job(mockClient, mockKeys, '99', 'test-job', {}, {});
    const f32 = new Float32Array([4.0, 5.0]);
    await job.storeVector('embedding', f32);

    expect(hsetCalls).toHaveLength(1);
    const buf = hsetCalls[0].embedding;
    expect(buf).toBeInstanceOf(Buffer);
    expect(buf.length).toBe(8); // 2 * 4 bytes
  });
});

// ---- Testing mode ----

describe('Testing mode - vector search', () => {
  const { TestQueue, TestJob } = require('../dist/testing') as typeof import('../src/testing');

  it('createJobIndex + vectorSearch round-trip', async () => {
    const q = new TestQueue('vec-test-1');
    await q.createJobIndex({
      vectorField: { name: 'vec', dimensions: 3 },
    });

    const job1 = await q.add('doc', { text: 'hello' });
    const job2 = await q.add('doc', { text: 'world' });
    const job3 = await q.add('doc', { text: 'foo' });

    await job1!.storeVector('vec', [1.0, 0.0, 0.0]);
    await job2!.storeVector('vec', [0.0, 1.0, 0.0]);
    await job3!.storeVector('vec', [0.9, 0.1, 0.0]);

    const results = await q.vectorSearch([1.0, 0.0, 0.0], { k: 2 });
    expect(results).toHaveLength(2);
    // job1 is most similar to [1,0,0], job3 is next
    expect(results[0].job.id).toBe(job1!.id);
    expect(results[1].job.id).toBe(job3!.id);
    expect(results[0].score).toBeLessThan(results[1].score);

    await q.close();
  });

  it('TestJob.storeVector + TestQueue.vectorSearch', async () => {
    const q = new TestQueue('vec-test-2');
    await q.createJobIndex({
      vectorField: { name: 'emb', dimensions: 2 },
    });

    const job = await q.add('item', { val: 1 });
    await job!.storeVector('emb', [0.5, 0.5]);

    const results = await q.vectorSearch([0.5, 0.5], { k: 5 });
    expect(results).toHaveLength(1);
    expect(results[0].job.id).toBe(job!.id);
    // Exact match should have distance ~0
    expect(results[0].score).toBeCloseTo(0, 5);

    await q.close();
  });

  it('vectorSearch with k limits results', async () => {
    const q = new TestQueue('vec-test-3');
    await q.createJobIndex({ vectorField: { name: 'v', dimensions: 2 } });

    for (let i = 0; i < 10; i++) {
      const job = await q.add('item', { i });
      await job!.storeVector('v', [Math.cos(i), Math.sin(i)]);
    }

    const results = await q.vectorSearch([1.0, 0.0], { k: 3 });
    expect(results).toHaveLength(3);

    await q.close();
  });

  it('dropJobIndex clears state and vectorSearch throws', async () => {
    const q = new TestQueue('vec-test-4');
    await q.createJobIndex({ vectorField: { name: 'v', dimensions: 2 } });

    const job = await q.add('item', {});
    await job!.storeVector('v', [1.0, 0.0]);

    // Search works before drop
    const before = await q.vectorSearch([1.0, 0.0]);
    expect(before).toHaveLength(1);

    await q.dropJobIndex();

    // Search throws after drop
    await expect(q.vectorSearch([1.0, 0.0])).rejects.toThrow('No index created');

    await q.close();
  });

  it('vectorSearch with pre-filter @state:{completed}', async () => {
    const q = new TestQueue('vec-test-5');
    await q.createJobIndex({ vectorField: { name: 'v', dimensions: 2 } });

    const j1 = await q.add('a', {});
    const j2 = await q.add('b', {});
    await j1!.storeVector('v', [1.0, 0.0]);
    await j2!.storeVector('v', [0.9, 0.1]);

    // Mark j2 as completed
    const r2 = q.jobs.get(j2!.id)!;
    r2.state = 'completed';

    const results = await q.vectorSearch([1.0, 0.0], { filter: '@state:{completed}' });
    expect(results).toHaveLength(1);
    expect(results[0].job.id).toBe(j2!.id);

    await q.close();
  });
});

// ---- Integration tests (require valkey-bundle with search module) ----

describe('Valkey Search integration', () => {
  const { GlideClient } = require('@glidemq/speedkey') as typeof import('@glidemq/speedkey');
  const { Queue } = require('../dist/queue') as typeof import('../src/queue');
  const { Worker } = require('../dist/worker') as typeof import('../src/worker');

  const CONNECTION = {
    addresses: [{ host: 'localhost', port: 6379 }],
    requestTimeout: 5000,
  };

  let cleanupClient: InstanceType<typeof GlideClient>;
  let searchAvailable = false;
  let searchVersion = 0; // e.g. 10000 = 1.0.0, 66048 = 1.2.0

  /** Drop ALL FT indexes to prevent cascading timeout issues from stale indexes. */
  async function dropAllIndexes() {
    if (!cleanupClient) return;
    try {
      const indexes = await GlideFt.list(cleanupClient);
      for (const idx of indexes) {
        try {
          await GlideFt.dropindex(cleanupClient, String(idx));
        } catch {}
      }
    } catch {}
  }

  beforeAll(async () => {
    try {
      cleanupClient = await GlideClient.createClient({
        addresses: CONNECTION.addresses,
        requestTimeout: 10000,
      });
      await GlideFt.list(cleanupClient);
      searchAvailable = true;
      // Detect search module version via MODULE LIST
      try {
        const modules = (await cleanupClient.customCommand(['MODULE', 'LIST'])) as any[];
        for (let i = 0; i < modules.length; i += 2) {
          const mod = modules[i] as any[];
          if (mod && String(mod[1]) === 'search') {
            searchVersion = Number(mod[3]) || 0;
            break;
          }
        }
      } catch {}
      // Clean up any stale indexes from previous runs
      await dropAllIndexes();
    } catch {
      searchAvailable = false;
    }
  });

  afterEach(async () => {
    // Drop all indexes after every test to prevent accumulation
    await dropAllIndexes();
  });

  afterAll(async () => {
    if (cleanupClient) {
      try {
        await dropAllIndexes();
        cleanupClient.close();
      } catch {}
    }
  });

  function requireSearch() {
    if (!searchAvailable) return false;
    return true;
  }

  /** Require search 1.1+ (version >= 65536). Skip test on older servers. */
  function requireSearch11() {
    if (!searchAvailable) return false;
    if (searchVersion < 65536) return false; // 65536 = 1.1.0
    return true;
  }

  /** Helper to flush queue keys + drop index */
  async function cleanup(qName: string, indexName?: string) {
    if (!cleanupClient) return;
    const { buildKeys, keyPrefix } = require('../dist/utils') as typeof import('../src/utils');
    const k = buildKeys(qName);
    try {
      await cleanupClient.del([
        k.id,
        k.stream,
        k.scheduled,
        k.completed,
        k.failed,
        k.events,
        k.meta,
        k.dedup,
        k.rate,
        k.schedulers,
      ]);
    } catch {}
    const pfx = keyPrefix('glide', qName);
    try {
      let cursor = '0';
      do {
        const result = await cleanupClient.scan(cursor, { match: `${pfx}:job:*`, count: 100 });
        cursor = result[0] as string;
        const keys = result[1] as string[];
        if (keys.length > 0) await cleanupClient.del(keys);
      } while (cursor !== '0');
    } catch {}
    if (indexName) {
      try {
        await GlideFt.dropindex(cleanupClient, indexName);
      } catch {}
    }
  }

  // Helper: store a vector using the cleanup client (avoids FCALL + search index conflict)
  function vecBuf(arr: number[] | Float32Array): Buffer {
    const f32 = arr instanceof Float32Array ? arr : new Float32Array(arr);
    return Buffer.from(f32.buffer, f32.byteOffset, f32.byteLength);
  }

  async function storeVecDirect(qName: string, jobId: string, field: string, embedding: number[] | Float32Array) {
    const key = `glide:{${qName}}:job:${jobId}`;
    await cleanupClient.hset(key, { [field]: vecBuf(embedding) });
  }

  it('add jobs + storeVector + createJobIndex + vectorSearch returns ranked results', async () => {
    if (!requireSearch()) return;

    const qName = 'search-int-1-' + Date.now();
    const idxName = `${qName}-idx`;
    const q = new Queue(qName, { connection: CONNECTION });

    try {
      // Add jobs and store vectors BEFORE creating the index
      // (avoids valkey-search FCALL + index interaction bug)
      const j1 = await q.add('doc', { title: 'close' });
      const j2 = await q.add('doc', { title: 'far' });
      const j3 = await q.add('doc', { title: 'closest' });

      await storeVecDirect(qName, j1!.id, 'vec', [1.0, 0.0, 0.0, 0.0]);
      await storeVecDirect(qName, j2!.id, 'vec', [0.0, 1.0, 0.0, 0.0]);
      await storeVecDirect(qName, j3!.id, 'vec', [0.99, 0.01, 0.0, 0.0]);

      // Create the index AFTER data is in place
      await q.createJobIndex({
        vectorField: { name: 'vec', dimensions: 4, algorithm: 'FLAT', distanceMetric: 'COSINE' },
      });

      // Give the index time to backfill
      await new Promise((r) => setTimeout(r, 1500));

      const results = await q.vectorSearch([1.0, 0.0, 0.0, 0.0], { k: 3 });
      expect(results.length).toBeGreaterThanOrEqual(2);

      const ids = results.map((r) => r.job.id);
      expect(ids).toContain(j1!.id);
      expect(ids).toContain(j3!.id);

      for (let i = 1; i < results.length; i++) {
        expect(results[i].score).toBeGreaterThanOrEqual(results[i - 1].score);
      }
    } finally {
      await q.close();
      await cleanup(qName, idxName);
    }
  }, 15000);

  it('vectorSearch with pre-filter (@state:{completed})', async () => {
    if (!requireSearch()) return;

    const qName = 'search-int-2-' + Date.now();
    const idxName = `${qName}-idx`;
    const q = new Queue(qName, { connection: CONNECTION });

    try {
      const j1 = await q.add('task', { cat: 'a' });
      const j2 = await q.add('task', { cat: 'b' });

      // Store vectors before index creation
      await storeVecDirect(qName, j1!.id, 'vec', [1.0, 0.0, 0.0, 0.0]);
      await storeVecDirect(qName, j2!.id, 'vec', [0.9, 0.1, 0.0, 0.0]);

      // Mark jobs as completed directly (avoids Worker FCALL + index conflict)
      await cleanupClient.hset(`glide:{${qName}}:job:${j1!.id}`, { state: 'completed' });
      await cleanupClient.hset(`glide:{${qName}}:job:${j2!.id}`, { state: 'completed' });

      // Create index after all state changes
      await q.createJobIndex({
        vectorField: { name: 'vec', dimensions: 4, algorithm: 'FLAT', distanceMetric: 'COSINE' },
      });
      await new Promise((r) => setTimeout(r, 1500));

      const results = await q.vectorSearch([1.0, 0.0, 0.0, 0.0], {
        filter: '@state:{completed}',
        k: 10,
      });
      expect(results.length).toBeGreaterThanOrEqual(1);
      for (const r of results) {
        const state = await r.job.getState();
        expect(state).toBe('completed');
      }
    } finally {
      await q.close();
      await cleanup(qName, idxName);
    }
  }, 15000);

  it('vectorSearch returns Job objects with correct data', async () => {
    if (!requireSearch()) return;

    const qName = 'search-int-3-' + Date.now();
    const idxName = `${qName}-idx`;
    const q = new Queue(qName, { connection: CONNECTION });

    try {
      const j1 = await q.add('embedding-job', { text: 'hello world', category: 'greeting' });
      await storeVecDirect(qName, j1!.id, 'vec', [1.0, 0.0]);

      await q.createJobIndex({
        vectorField: { name: 'vec', dimensions: 2, algorithm: 'FLAT', distanceMetric: 'L2' },
      });
      await new Promise((r) => setTimeout(r, 1500));

      const results = await q.vectorSearch([1.0, 0.0], { k: 1 });
      expect(results).toHaveLength(1);
      expect(results[0].job.id).toBe(j1!.id);
      expect(results[0].job.name).toBe('embedding-job');
      expect((results[0].job.data as any).text).toBe('hello world');
    } finally {
      await q.close();
      await cleanup(qName, idxName);
    }
  }, 15000);

  it('createJobIndex with custom TAG fields', async () => {
    if (!requireSearch()) return;

    const qName = 'search-int-5-' + Date.now();
    const idxName = `${qName}-idx`;
    const q = new Queue(qName, { connection: CONNECTION });

    try {
      await q.createJobIndex({
        fields: [{ type: 'TAG', name: 'category' }],
        vectorField: { name: 'vec', dimensions: 2, algorithm: 'FLAT', distanceMetric: 'COSINE' },
      });

      const info = await GlideFt.info(cleanupClient, idxName);
      expect(info).toBeDefined();
      // attributes format varies: v1.x uses array of arrays, later uses array of objects
      const attrs = ((info as any).attributes ?? (info as any).fields) as any[];
      expect(attrs).toBeDefined();
      const fieldNames: string[] = [];
      for (const attr of attrs) {
        if (Array.isArray(attr)) {
          const idIdx = attr.indexOf('identifier');
          if (idIdx >= 0) fieldNames.push(String(attr[idIdx + 1]));
        } else if (typeof attr === 'object') {
          fieldNames.push(String(attr.identifier || attr.field_name));
        }
      }
      expect(fieldNames).toContain('category');
      expect(fieldNames).toContain('name');
      expect(fieldNames).toContain('state');
    } finally {
      await q.close();
      await cleanup(qName, idxName);
    }
  }, 15000);

  it('dropJobIndex + vectorSearch throws', async () => {
    if (!requireSearch()) return;

    const qName = 'search-int-6-' + Date.now();
    const idxName = `${qName}-idx`;
    const q = new Queue(qName, { connection: CONNECTION });

    try {
      await q.createJobIndex({
        vectorField: { name: 'vec', dimensions: 2, algorithm: 'FLAT', distanceMetric: 'COSINE' },
      });

      await q.dropJobIndex();

      await expect(q.vectorSearch([1.0, 0.0])).rejects.toThrow();
    } finally {
      await q.close();
      await cleanup(qName);
    }
  }, 15000);

  it('multiple queues with separate indexes', async () => {
    if (!requireSearch()) return;

    const qName1 = 'search-int-7a-' + Date.now();
    const qName2 = 'search-int-7b-' + Date.now();
    const idx1 = `${qName1}-idx`;
    const idx2 = `${qName2}-idx`;
    const q1 = new Queue(qName1, { connection: CONNECTION });
    const q2 = new Queue(qName2, { connection: CONNECTION });

    try {
      const j1 = await q1.add('item', { from: 'q1' });
      const j2 = await q2.add('item', { from: 'q2' });
      await storeVecDirect(qName1, j1!.id, 'vec', [1.0, 0.0]);
      await storeVecDirect(qName2, j2!.id, 'vec', [1.0, 0.0]);

      await q1.createJobIndex({
        vectorField: { name: 'vec', dimensions: 2, algorithm: 'FLAT', distanceMetric: 'L2' },
      });
      await q2.createJobIndex({
        vectorField: { name: 'vec', dimensions: 2, algorithm: 'FLAT', distanceMetric: 'L2' },
      });
      await new Promise((r) => setTimeout(r, 1500));

      const r1 = await q1.vectorSearch([1.0, 0.0], { k: 10 });
      const r2 = await q2.vectorSearch([1.0, 0.0], { k: 10 });

      expect(r1).toHaveLength(1);
      expect(r2).toHaveLength(1);
      expect(r1[0].job.id).toBe(j1!.id);
      expect(r2[0].job.id).toBe(j2!.id);
    } finally {
      await q1.close();
      await q2.close();
      await cleanup(qName1, idx1);
      await cleanup(qName2, idx2);
    }
  }, 15000);

  it('vectorSearch with k > num jobs returns all available', async () => {
    if (!requireSearch()) return;

    const qName = 'search-int-8-' + Date.now();
    const idxName = `${qName}-idx`;
    const q = new Queue(qName, { connection: CONNECTION });

    try {
      const j1 = await q.add('item', {});
      const j2 = await q.add('item', {});
      await storeVecDirect(qName, j1!.id, 'vec', [1.0, 0.0]);
      await storeVecDirect(qName, j2!.id, 'vec', [0.0, 1.0]);

      await q.createJobIndex({
        vectorField: { name: 'vec', dimensions: 2, algorithm: 'FLAT', distanceMetric: 'COSINE' },
      });
      await new Promise((r) => setTimeout(r, 1500));

      const results = await q.vectorSearch([1.0, 0.0], { k: 100 });
      expect(results.length).toBe(2);
    } finally {
      await q.close();
      await cleanup(qName, idxName);
    }
  }, 15000);

  it('high-dimensional vectors (768d)', async () => {
    if (!requireSearch()) return;

    const qName = 'search-int-9-' + Date.now();
    const idxName = `${qName}-idx`;
    const q = new Queue(qName, { connection: CONNECTION });

    try {
      const makeVec = (seed: number) => {
        const v = new Float32Array(768);
        for (let i = 0; i < 768; i++) v[i] = Math.sin(seed * (i + 1));
        return v;
      };

      const j1 = await q.add('embed', { label: 'a' });
      const j2 = await q.add('embed', { label: 'b' });
      await storeVecDirect(qName, j1!.id, 'vec', makeVec(1));
      await storeVecDirect(qName, j2!.id, 'vec', makeVec(2));

      await q.createJobIndex({
        vectorField: { name: 'vec', dimensions: 768, algorithm: 'HNSW', distanceMetric: 'COSINE' },
      });
      await new Promise((r) => setTimeout(r, 2000));

      const results = await q.vectorSearch(makeVec(1), { k: 2 });
      expect(results.length).toBeGreaterThanOrEqual(1);
      expect(results[0].job.id).toBe(j1!.id);
    } finally {
      await q.close();
      await cleanup(qName, idxName);
    }
  }, 60000);

  // -- Search 1.1+ create options (requires valkey-search >= 1.1)

  it('createJobIndex with skipInitialScan option', async () => {
    if (!requireSearch11()) return;

    const qName = 'search-skipscan-' + Date.now();
    const idxName = `${qName}-idx`;
    const q = new Queue(qName, { connection: CONNECTION });

    try {
      // Add a job BEFORE creating the index
      const job = await q.add('pre-existing', { text: 'before index' });
      await storeVecDirect(qName, job!.id, 'vec', [1.0, 0.0]);

      // skipInitialScan: index won't backfill pre-existing keys
      await q.createJobIndex({
        vectorField: { name: 'vec', dimensions: 2, algorithm: 'FLAT', distanceMetric: 'COSINE' },
        createOptions: { skipInitialScan: true },
      });

      await new Promise((r) => setTimeout(r, 500));
      const results = await q.vectorSearch([1.0, 0.0], { k: 10 });
      expect(results).toHaveLength(0);

      // New job added AFTER index creation IS indexed
      const job2 = await q.add('post-index', {});
      await storeVecDirect(qName, job2!.id, 'vec', [0.0, 1.0]);
      await new Promise((r) => setTimeout(r, 500));

      const results2 = await q.vectorSearch([0.0, 1.0], { k: 10 });
      expect(results2.length).toBeGreaterThanOrEqual(1);
    } finally {
      await q.close();
      await cleanup(qName, idxName);
    }
  }, 30000);

  it('createJobIndex with noStopWords option', async () => {
    if (!requireSearch11()) return;

    const qName = 'search-nostop-' + Date.now();
    const idxName = `${qName}-idx`;
    const q = new Queue(qName, { connection: CONNECTION });

    try {
      await q.createJobIndex({
        fields: [{ type: 'TEXT', name: 'title' }],
        vectorField: { name: 'vec', dimensions: 2, algorithm: 'FLAT', distanceMetric: 'COSINE' },
        createOptions: { noStopWords: true },
      });

      const info = await GlideFt.info(cleanupClient, idxName);
      expect(info).toBeDefined();
    } finally {
      await q.close();
      await cleanup(qName, idxName);
    }
  }, 15000);

  it('createJobIndex with custom stopWords', async () => {
    if (!requireSearch11()) return;

    const qName = 'search-cstop-' + Date.now();
    const idxName = `${qName}-idx`;
    const q = new Queue(qName, { connection: CONNECTION });

    try {
      await q.createJobIndex({
        fields: [{ type: 'TEXT', name: 'title' }],
        vectorField: { name: 'vec', dimensions: 2, algorithm: 'FLAT', distanceMetric: 'COSINE' },
        createOptions: { stopWords: ['the', 'a', 'is'] },
      });

      const info = await GlideFt.info(cleanupClient, idxName);
      expect(info).toBeDefined();
    } finally {
      await q.close();
      await cleanup(qName, idxName);
    }
  }, 15000);

  // -- Non-vector index (search 1.1+ allows indexes without vector fields)

  it('createJobIndex without vectorField on search 1.1+', async () => {
    if (!requireSearch11()) return;

    const qName = 'search-novec-' + Date.now();
    const idxName = `${qName}-idx`;
    const q = new Queue(qName, { connection: CONNECTION });

    try {
      // No vectorField - on search 1.0 this adds a dummy vector, on 1.1+ it should work natively
      await q.createJobIndex({
        fields: [{ type: 'TAG', name: 'category' }],
      });

      const info = await GlideFt.info(cleanupClient, idxName);
      expect(info).toBeDefined();
    } finally {
      await q.close();
      await cleanup(qName, idxName);
    }
  }, 15000);

  // -- Distance metrics

  it('vectorSearch with L2 distance metric', async () => {
    if (!requireSearch()) return;

    const qName = 'search-l2-' + Date.now();
    const idxName = `${qName}-idx`;
    const q = new Queue(qName, { connection: CONNECTION });

    try {
      await q.createJobIndex({
        vectorField: { name: 'vec', dimensions: 2, algorithm: 'FLAT', distanceMetric: 'L2' },
      });

      const j1 = await q.add('near', {});
      await storeVecDirect(qName, j1!.id, 'vec', [1.0, 0.0]);
      const j2 = await q.add('far', {});
      await storeVecDirect(qName, j2!.id, 'vec', [0.0, 1.0]);

      await new Promise((r) => setTimeout(r, 500));

      const results = await q.vectorSearch([1.0, 0.0], { k: 2 });
      expect(results).toHaveLength(2);
      // L2: lower = more similar
      expect(results[0].job.id).toBe(j1!.id);
      expect(results[0].score).toBeLessThan(results[1].score);
    } finally {
      await q.close();
      await cleanup(qName, idxName);
    }
  }, 15000);

  it('vectorSearch with inner product distance metric', async () => {
    if (!requireSearch()) return;

    const qName = 'search-ip-' + Date.now();
    const idxName = `${qName}-idx`;
    const q = new Queue(qName, { connection: CONNECTION });

    try {
      await q.createJobIndex({
        vectorField: { name: 'vec', dimensions: 2, algorithm: 'FLAT', distanceMetric: 'IP' },
      });

      const j1 = await q.add('aligned', {});
      await storeVecDirect(qName, j1!.id, 'vec', [1.0, 0.0]);
      const j2 = await q.add('partial', {});
      await storeVecDirect(qName, j2!.id, 'vec', [0.5, 0.5]);

      await new Promise((r) => setTimeout(r, 500));

      const results = await q.vectorSearch([1.0, 0.0], { k: 2 });
      expect(results).toHaveLength(2);
    } finally {
      await q.close();
      await cleanup(qName, idxName);
    }
  }, 15000);

  it('createJobIndex throws clear error when search module not loaded', async () => {
    // This test forces ensureSearchModule to fail
    const { Queue: Q2 } = require('../dist/queue') as typeof import('../src/queue');
    const q = new Q2('no-search-mod-' + Date.now(), {
      connection: { addresses: [{ host: 'localhost', port: 6379 }] },
    });

    // Mock the search module check to fail
    const mockClient = {
      hset: async () => 1,
    } as any;
    (q as any).client = mockClient;
    (q as any).clientOwned = false;
    (q as any).searchModuleAvailable = null;

    const listSpy = vi.spyOn(GlideFt, 'list').mockRejectedValue(new Error('ERR unknown command'));

    try {
      await expect(q.createJobIndex()).rejects.toThrow('Valkey Search module is not available');
    } finally {
      listSpy.mockRestore();
      await q.close();
    }
  });
});
