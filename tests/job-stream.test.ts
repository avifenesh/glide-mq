/**
 * Tests for per-job streaming channel (Job.stream / Queue.readStream).
 *
 * Run: npx vitest run tests/job-stream.test.ts
 */
import { describe, it, expect, vi, beforeAll, beforeEach, afterAll } from 'vitest';
import { Job } from '../src/job';
import { buildKeys, MAX_JOB_DATA_SIZE } from '../src/utils';
import { TestQueue } from '../src/testing';

vi.mock('@glidemq/speedkey');

// ---- Unit tests (mocked client) ----

function makeMockClient(overrides: Record<string, unknown> = {}) {
  return {
    fcall: vi.fn(),
    hset: vi.fn().mockResolvedValue(1),
    hget: vi.fn().mockResolvedValue(null),
    hgetall: vi.fn().mockResolvedValue([]),
    hmget: vi.fn().mockResolvedValue([]),
    xadd: vi.fn().mockResolvedValue('1-0'),
    xrange: vi.fn().mockResolvedValue(null),
    smembers: vi.fn().mockResolvedValue(new Set()),
    rpush: vi.fn().mockResolvedValue(1),
    close: vi.fn(),
    exec: vi.fn().mockResolvedValue([]),
    ...overrides,
  };
}

const keys = buildKeys('test-queue');

describe('Job.stream (unit)', () => {
  let mockClient: ReturnType<typeof makeMockClient>;
  let callCount: number;

  beforeEach(() => {
    vi.clearAllMocks();
    callCount = 0;
    mockClient = makeMockClient({
      xadd: vi.fn().mockImplementation(() => {
        callCount++;
        return Promise.resolve(`${callCount}-0`);
      }),
    });
  });

  it('calls XADD on jstream key', async () => {
    const job = new Job(mockClient as any, keys, '42', 'llm-stream', {}, {});
    const entryId = await job.stream({ token: 'Hello' });

    expect(mockClient.xadd).toHaveBeenCalledWith(keys.jstream('42'), [['token', 'Hello']]);
    expect(entryId).toBe('1-0');
  });

  it('multiple chunks get different entry IDs', async () => {
    const job = new Job(mockClient as any, keys, '42', 'llm-stream', {}, {});
    const id1 = await job.stream({ token: 'Hello' });
    const id2 = await job.stream({ token: ' world' });

    expect(id1).toBe('1-0');
    expect(id2).toBe('2-0');
    expect(mockClient.xadd).toHaveBeenCalledTimes(2);
  });

  it('empty chunk object throws', async () => {
    const job = new Job(mockClient as any, keys, '42', 'llm-stream', {}, {});
    await expect(job.stream({})).rejects.toThrow('Stream chunk must not be empty');
  });

  it('large chunk near MAX_JOB_DATA_SIZE accepted', async () => {
    const job = new Job(mockClient as any, keys, '42', 'llm-stream', {}, {});
    const largeValue = 'x'.repeat(MAX_JOB_DATA_SIZE - 10);
    const entryId = await job.stream({ data: largeValue });
    expect(entryId).toBe('1-0');
  });

  it('chunk exceeding MAX_JOB_DATA_SIZE rejected', async () => {
    const job = new Job(mockClient as any, keys, '42', 'llm-stream', {}, {});
    const hugeValue = 'x'.repeat(MAX_JOB_DATA_SIZE + 1);
    await expect(job.stream({ data: hugeValue })).rejects.toThrow('exceeds maximum size');
  });

  it('works without entryId (external context)', async () => {
    const job = new Job(mockClient as any, keys, '42', 'llm-stream', {}, {});
    // entryId is undefined (not set by a Worker) - stream() should still work
    expect(job.entryId).toBeUndefined();
    const id = await job.stream({ token: 'test' });
    expect(id).toBe('1-0');
  });

  it('streamChunk reasoning calls xadd with type+content fields', async () => {
    const job = new Job(mockClient as any, keys, '42', 'llm-stream', {}, {});
    const entryId = await job.streamChunk('reasoning', 'thinking...');

    expect(mockClient.xadd).toHaveBeenCalledWith(keys.jstream('42'), [
      ['type', 'reasoning'],
      ['content', 'thinking...'],
    ]);
    expect(entryId).toBe('1-0');
  });

  it('streamChunk done calls xadd with type only (no content field)', async () => {
    const job = new Job(mockClient as any, keys, '42', 'llm-stream', {}, {});
    const entryId = await job.streamChunk('done');

    expect(mockClient.xadd).toHaveBeenCalledWith(keys.jstream('42'), [['type', 'done']]);
    expect(entryId).toBe('1-0');
  });

  it('streamChunk content produces correct fields', async () => {
    const job = new Job(mockClient as any, keys, '42', 'llm-stream', {}, {});
    await job.streamChunk('content', 'answer');

    expect(mockClient.xadd).toHaveBeenCalledWith(keys.jstream('42'), [
      ['type', 'content'],
      ['content', 'answer'],
    ]);
  });
});

// ---- Testing mode tests ----

describe('TestJob.stream + TestQueue.readStream', () => {
  let queue: InstanceType<typeof TestQueue>;

  afterAll(async () => {
    if (queue) await queue.close();
  });

  it('round-trip: stream chunks read back via readStream', async () => {
    queue = new TestQueue('test-stream-rt');
    const job = await queue.add('llm-call', { prompt: 'hello' });
    expect(job).not.toBeNull();

    await job!.stream({ token: 'Hello' });
    await job!.stream({ token: ' world' });

    const entries = await queue.readStream(job!.id);
    expect(entries).toHaveLength(2);
    expect(entries[0].fields.token).toBe('Hello');
    expect(entries[1].fields.token).toBe(' world');
    expect(entries[0].id).toBe('test-1');
    expect(entries[1].id).toBe('test-2');
  });

  it('readStream with lastId filters correctly', async () => {
    queue = new TestQueue('test-stream-lastid');
    const job = await queue.add('llm-call', {});
    expect(job).not.toBeNull();

    const id1 = await job!.stream({ token: 'a' });
    await job!.stream({ token: 'b' });
    await job!.stream({ token: 'c' });

    const entries = await queue.readStream(job!.id, { lastId: id1 });
    expect(entries).toHaveLength(2);
    expect(entries[0].fields.token).toBe('b');
    expect(entries[1].fields.token).toBe('c');
  });

  it('readStream with count limits results', async () => {
    queue = new TestQueue('test-stream-count');
    const job = await queue.add('llm-call', {});
    expect(job).not.toBeNull();

    await job!.stream({ token: 'a' });
    await job!.stream({ token: 'b' });
    await job!.stream({ token: 'c' });

    const entries = await queue.readStream(job!.id, { count: 2 });
    expect(entries).toHaveLength(2);
    expect(entries[0].fields.token).toBe('a');
    expect(entries[1].fields.token).toBe('b');
  });

  it('readStream returns empty for nonexistent job', async () => {
    queue = new TestQueue('test-stream-none');
    const entries = await queue.readStream('nonexistent');
    expect(entries).toEqual([]);
  });

  it('empty chunk throws', async () => {
    queue = new TestQueue('test-stream-empty');
    const job = await queue.add('llm-call', {});
    await expect(job!.stream({})).rejects.toThrow('Stream chunk must not be empty');
  });

  it('oversized chunk throws', async () => {
    queue = new TestQueue('test-stream-oversize');
    const job = await queue.add('llm-call', {});
    const huge = 'x'.repeat(MAX_JOB_DATA_SIZE + 1);
    await expect(job!.stream({ data: huge })).rejects.toThrow('exceeds maximum size');
  });

  it('streamChunk round-trip: reasoning + content + done read back correctly', async () => {
    queue = new TestQueue('test-streamchunk-rt');
    const job = await queue.add('llm-call', { prompt: 'hello' });
    expect(job).not.toBeNull();

    await job!.streamChunk('reasoning', 'Let me think...');
    await job!.streamChunk('content', 'The answer is 42.');
    await job!.streamChunk('done');

    const entries = await queue.readStream(job!.id);
    expect(entries).toHaveLength(3);

    expect(entries[0].fields.type).toBe('reasoning');
    expect(entries[0].fields.content).toBe('Let me think...');

    expect(entries[1].fields.type).toBe('content');
    expect(entries[1].fields.content).toBe('The answer is 42.');

    expect(entries[2].fields.type).toBe('done');
    expect(entries[2].fields.content).toBeUndefined();
  });
});

// ---- Integration tests (require Valkey) ----

import { describeEachMode, createCleanupClient, flushQueue, waitFor } from './helpers/fixture';

const QueueImpl = require('../dist/queue').Queue;
const WorkerImpl = require('../dist/worker').Worker;

import type { Server } from 'http';
import { STANDALONE } from './helpers/fixture';

const createProxyServer = require('../dist/proxy/index').createProxyServer;

describe('SSE proxy endpoint', () => {
  let server: Server;
  let baseUrl: string;
  let proxyClose: () => Promise<void>;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(STANDALONE);
    const proxy = createProxyServer({ connection: STANDALONE });
    proxyClose = proxy.close;
    await new Promise<void>((resolve) => {
      server = proxy.app.listen(0, () => {
        const addr = server.address();
        if (typeof addr === 'object' && addr) {
          baseUrl = `http://127.0.0.1:${addr.port}`;
        }
        resolve();
      });
    });
  });

  afterAll(async () => {
    await proxyClose();
    await new Promise<void>((resolve, reject) => server.close((err) => (err ? reject(err) : resolve())));
    await cleanupClient.close();
  });

  it('SSE proxy returns stream chunks for a job', async () => {
    const Q = 'test-jstream-sse-' + Date.now();
    const queue = new QueueImpl(Q, { connection: STANDALONE });
    const worker = new WorkerImpl(
      Q,
      async (job: any) => {
        await job.stream({ token: 'Hello' });
        await job.stream({ token: ' world' });
        return 'done';
      },
      { connection: STANDALONE },
    );

    try {
      const job = await queue.add('sse-test', {});
      await waitFor(async () => {
        const fetched = await queue.getJob(job.id);
        return fetched?.finishedOn !== undefined;
      });

      const res = await fetch(`${baseUrl}/queues/${Q}/jobs/${job.id}/stream`);
      expect(res.status).toBe(200);
      expect(res.headers.get('content-type')).toBe('text/event-stream');

      const text = await res.text();
      expect(text).toContain('data:');
      expect(text).toContain('Hello');
      expect(text).toContain(' world');
    } finally {
      await worker.close();
      await queue.close();
      await flushQueue(cleanupClient, Q);
    }
  });
});

describeEachMode('Job streaming integration', (CONNECTION) => {
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    cleanupClient.close();
  });

  it('stream() + readStream() round-trip on real Valkey', async () => {
    const Q = 'test-jstream-rt-' + Date.now();
    const queue = new QueueImpl(Q, { connection: CONNECTION });
    const worker = new WorkerImpl(
      Q,
      async (job: any) => {
        await job.stream({ token: 'Hello' });
        await job.stream({ token: ' world' });
        return 'done';
      },
      { connection: CONNECTION },
    );

    try {
      const job = await queue.add('stream-test', { prompt: 'hi' });
      await waitFor(async () => {
        const fetched = await queue.getJob(job.id);
        return fetched?.finishedOn !== undefined;
      });

      const entries = await queue.readStream(job.id);
      expect(entries.length).toBe(2);
      expect(entries[0].fields.token).toBe('Hello');
      expect(entries[1].fields.token).toBe(' world');
    } finally {
      await worker.close();
      await queue.close();
      await flushQueue(cleanupClient, Q);
    }
  });

  it('readStream with lastId resume', async () => {
    const Q = 'test-jstream-lid-' + Date.now();
    const queue = new QueueImpl(Q, { connection: CONNECTION });
    const worker = new WorkerImpl(
      Q,
      async (job: any) => {
        await job.stream({ token: 'a' });
        await job.stream({ token: 'b' });
        await job.stream({ token: 'c' });
        return 'done';
      },
      { connection: CONNECTION },
    );

    try {
      const job = await queue.add('stream-resume', {});
      await waitFor(async () => {
        const fetched = await queue.getJob(job.id);
        return fetched?.finishedOn !== undefined;
      });

      const all = await queue.readStream(job.id);
      expect(all.length).toBe(3);

      const rest = await queue.readStream(job.id, { lastId: all[0].id });
      expect(rest.length).toBe(2);
      expect(rest[0].fields.token).toBe('b');
      expect(rest[1].fields.token).toBe('c');
    } finally {
      await worker.close();
      await queue.close();
      await flushQueue(cleanupClient, Q);
    }
  });

  it('multiple chunks arrive in order', async () => {
    const Q = 'test-jstream-ord-' + Date.now();
    const queue = new QueueImpl(Q, { connection: CONNECTION });
    const CHUNK_COUNT = 10;
    const worker = new WorkerImpl(
      Q,
      async (job: any) => {
        for (let i = 0; i < CHUNK_COUNT; i++) {
          await job.stream({ index: String(i) });
        }
        return 'done';
      },
      { connection: CONNECTION },
    );

    try {
      const job = await queue.add('stream-order', {});
      await waitFor(async () => {
        const fetched = await queue.getJob(job.id);
        return fetched?.finishedOn !== undefined;
      });

      const entries = await queue.readStream(job.id, { count: 100 });
      expect(entries.length).toBe(CHUNK_COUNT);
      for (let i = 0; i < CHUNK_COUNT; i++) {
        expect(entries[i].fields.index).toBe(String(i));
      }
    } finally {
      await worker.close();
      await queue.close();
      await flushQueue(cleanupClient, Q);
    }
  });

  it('stream survives job completion', async () => {
    const Q = 'test-jstream-sv-' + Date.now();
    const queue = new QueueImpl(Q, { connection: CONNECTION });
    const worker = new WorkerImpl(
      Q,
      async (job: any) => {
        await job.stream({ token: 'persisted' });
        return 'done';
      },
      { connection: CONNECTION },
    );

    try {
      const job = await queue.add('stream-survive', {});
      await waitFor(async () => {
        const fetched = await queue.getJob(job.id);
        return fetched?.finishedOn !== undefined;
      });

      const entries = await queue.readStream(job.id);
      expect(entries.length).toBe(1);
      expect(entries[0].fields.token).toBe('persisted');
    } finally {
      await worker.close();
      await queue.close();
      await flushQueue(cleanupClient, Q);
    }
  });

  it('stream cleaned up on job.remove()', async () => {
    const Q = 'test-jstream-rm-' + Date.now();
    const queue = new QueueImpl(Q, { connection: CONNECTION });
    const worker = new WorkerImpl(
      Q,
      async (job: any) => {
        await job.stream({ token: 'to-be-removed' });
        return 'done';
      },
      { connection: CONNECTION },
    );

    try {
      const job = await queue.add('stream-cleanup', {});
      await waitFor(async () => {
        const fetched = await queue.getJob(job.id);
        return fetched?.finishedOn !== undefined;
      });

      const before = await queue.readStream(job.id);
      expect(before.length).toBe(1);

      const fetched = await queue.getJob(job.id);
      await fetched.remove();

      const after = await queue.readStream(job.id);
      expect(after).toEqual([]);
    } finally {
      await worker.close();
      await queue.close();
      await flushQueue(cleanupClient, Q);
    }
  });
});
