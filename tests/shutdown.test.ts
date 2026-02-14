import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { GlideClient } from 'speedkey';
import { Worker } from '../src/worker';
import { Queue } from '../src/queue';
import { QueueEvents } from '../src/queue-events';
import { FlowProducer } from '../src/flow-producer';
import { LIBRARY_VERSION } from '../src/functions/index';

// Mock speedkey module
vi.mock('speedkey', () => {
  const MockGlideClient = {
    createClient: vi.fn(),
  };
  const MockGlideClusterClient = {
    createClient: vi.fn(),
  };
  return {
    GlideClient: MockGlideClient,
    GlideClusterClient: MockGlideClusterClient,
  };
});

function makeMockClient(overrides: Record<string, unknown> = {}) {
  return {
    fcall: vi.fn().mockImplementation((func: string) => {
      if (func === 'glidemq_checkConcurrency') return Promise.resolve(-1);
      return Promise.resolve(LIBRARY_VERSION);
    }),
    functionLoad: vi.fn(),
    xgroupCreate: vi.fn().mockResolvedValue('OK'),
    xreadgroup: vi.fn().mockResolvedValue(null),
    xread: vi.fn().mockResolvedValue(null),
    hgetall: vi.fn().mockResolvedValue([]),
    hget: vi.fn().mockResolvedValue(null),
    close: vi.fn(),
    ...overrides,
  };
}

const connectionOpts = {
  addresses: [{ host: '127.0.0.1', port: 6379 }],
};

const defaultWorkerOpts = {
  connection: connectionOpts,
  concurrency: 1,
  blockTimeout: 100,
};

describe('Worker.close()', () => {
  let mockCommandClient: ReturnType<typeof makeMockClient>;
  let mockBlockingClient: ReturnType<typeof makeMockClient>;

  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers({ shouldAdvanceTime: true });

    mockCommandClient = makeMockClient();
    mockBlockingClient = makeMockClient();

    let callCount = 0;
    vi.mocked(GlideClient.createClient).mockImplementation(async () => {
      callCount++;
      return (callCount === 1 ? mockCommandClient : mockBlockingClient) as any;
    });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should emit closing and closed events', async () => {
    const processor = vi.fn().mockResolvedValue('done');
    const worker = new Worker('test-queue', processor, defaultWorkerOpts);
    await worker.waitUntilReady();

    const events: string[] = [];
    worker.on('closing', () => events.push('closing'));
    worker.on('closed', () => events.push('closed'));

    await worker.close();

    expect(events).toEqual(['closing', 'closed']);
  });

  it('should close both clients', async () => {
    const processor = vi.fn().mockResolvedValue('done');
    const worker = new Worker('test-queue', processor, defaultWorkerOpts);
    await worker.waitUntilReady();

    await worker.close();

    expect(mockCommandClient.close).toHaveBeenCalledTimes(1);
    expect(mockBlockingClient.close).toHaveBeenCalledTimes(1);
  });

  it('should be idempotent - second close is a no-op', async () => {
    const processor = vi.fn().mockResolvedValue('done');
    const worker = new Worker('test-queue', processor, defaultWorkerOpts);
    await worker.waitUntilReady();

    const events: string[] = [];
    worker.on('closing', () => events.push('closing'));
    worker.on('closed', () => events.push('closed'));

    await worker.close();
    await worker.close();

    expect(events).toEqual(['closing', 'closed']);
    expect(mockCommandClient.close).toHaveBeenCalledTimes(1);
    expect(mockBlockingClient.close).toHaveBeenCalledTimes(1);
  });

  it('should wait for active jobs before closing (force=false)', async () => {
    let resolveJob!: () => void;
    const jobPromise = new Promise<void>((r) => { resolveJob = r; });
    const processor = vi.fn().mockReturnValue(jobPromise);

    // Set up a mock that returns one job entry, then null
    let callIdx = 0;
    mockBlockingClient.xreadgroup.mockImplementation(async () => {
      callIdx++;
      if (callIdx === 1) {
        return [{
          key: 'glide:{test-queue}:stream',
          value: {
            '1-0': [['jobId', '1']],
          },
        }];
      }
      return null;
    });

    mockCommandClient.hgetall.mockResolvedValue([
      { field: 'id', value: '1' },
      { field: 'name', value: 'test' },
      { field: 'data', value: '{}' },
      { field: 'opts', value: '{}' },
      { field: 'timestamp', value: String(Date.now()) },
      { field: 'attemptsMade', value: '0' },
    ]);

    mockCommandClient.fcall.mockImplementation((func: string) => {
      if (func === 'glidemq_checkConcurrency') return Promise.resolve(-1);
      if (func === 'glidemq_completeJob') return Promise.resolve(1);
      return Promise.resolve(LIBRARY_VERSION);
    });

    const worker = new Worker('test-queue', processor, defaultWorkerOpts);
    await worker.waitUntilReady();

    // Wait for the processor to be called
    await vi.advanceTimersByTimeAsync(200);
    expect(processor).toHaveBeenCalledTimes(1);

    // Start close (should wait for active job)
    let closed = false;
    const closePromise = worker.close().then(() => { closed = true; });

    // Job still active - close should not have resolved
    await vi.advanceTimersByTimeAsync(50);
    expect(closed).toBe(false);

    // Resolve the job
    resolveJob();
    await closePromise;

    expect(closed).toBe(true);
  });

  it('should close immediately with force=true even with active jobs', async () => {
    let resolveJob!: (v: string) => void;
    const jobPromise = new Promise<string>((r) => { resolveJob = r; });
    const processor = vi.fn().mockReturnValue(jobPromise);

    let callIdx = 0;
    mockBlockingClient.xreadgroup.mockImplementation(async () => {
      callIdx++;
      if (callIdx === 1) {
        return [{
          key: 'glide:{test-queue}:stream',
          value: {
            '1-0': [['jobId', '1']],
          },
        }];
      }
      return null;
    });

    mockCommandClient.hgetall.mockResolvedValue([
      { field: 'id', value: '1' },
      { field: 'name', value: 'test' },
      { field: 'data', value: '{}' },
      { field: 'opts', value: '{}' },
      { field: 'timestamp', value: String(Date.now()) },
      { field: 'attemptsMade', value: '0' },
    ]);

    mockCommandClient.fcall.mockImplementation((func: string) => {
      if (func === 'glidemq_checkConcurrency') return Promise.resolve(-1);
      if (func === 'glidemq_completeJob') return Promise.resolve(1);
      return Promise.resolve(LIBRARY_VERSION);
    });

    const worker = new Worker('test-queue', processor, defaultWorkerOpts);
    // Suppress unhandled error from background processJob after force close
    worker.on('error', () => {});
    await worker.waitUntilReady();
    await vi.advanceTimersByTimeAsync(200);

    // Force close should not wait
    await worker.close(true);

    expect(mockCommandClient.close).toHaveBeenCalledTimes(1);

    // Cleanup: resolve the job so the background promise settles
    resolveJob('done');
    await vi.advanceTimersByTimeAsync(50);
  });

  it('should stop the scheduler on close', async () => {
    const processor = vi.fn().mockResolvedValue('done');
    const worker = new Worker('test-queue', processor, defaultWorkerOpts);
    await worker.waitUntilReady();

    // pollLoop runs on setInterval — close should stop it
    await worker.close();

    // After close, no more polling should happen
    const callCountBefore = mockBlockingClient.xreadgroup.mock.calls.length;
    await vi.advanceTimersByTimeAsync(10000);
    const callCountAfter = mockBlockingClient.xreadgroup.mock.calls.length;
    expect(callCountAfter).toBe(callCountBefore);
  });
});

describe('Queue.close()', () => {
  let mockClient: ReturnType<typeof makeMockClient>;

  beforeEach(() => {
    vi.clearAllMocks();
    mockClient = makeMockClient();
    vi.mocked(GlideClient.createClient).mockResolvedValue(mockClient as any);
  });

  it('should be idempotent', async () => {
    mockClient.fcall
      .mockResolvedValueOnce(LIBRARY_VERSION)
      .mockResolvedValueOnce('1');
    const queue = new Queue('test-queue', { connection: connectionOpts });

    // Force client creation
    await queue.add('job', {});
    await queue.close();
    await queue.close();

    expect(mockClient.close).toHaveBeenCalledTimes(1);
  });

  it('should reject getClient calls after close', async () => {
    const queue = new Queue('test-queue', { connection: connectionOpts });
    await queue.close();

    await expect(queue.getClient()).rejects.toThrow('Queue is closing');
  });
});

describe('Queue error event', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should emit error when client creation fails and not cache', async () => {
    vi.mocked(GlideClient.createClient).mockRejectedValueOnce(new Error('Connection refused'));

    const queue = new Queue('test-queue', { connection: connectionOpts });
    const errors: Error[] = [];
    queue.on('error', (err) => errors.push(err));

    await expect(queue.getClient()).rejects.toThrow('Connection refused');
    expect(errors).toHaveLength(1);
    expect(errors[0].message).toContain('Connection refused');

    // Next call should retry, not return cached failure
    const freshClient = makeMockClient();
    freshClient.fcall.mockResolvedValue(LIBRARY_VERSION);
    vi.mocked(GlideClient.createClient).mockResolvedValueOnce(freshClient as any);
    const client = await queue.getClient();
    expect(client).toBe(freshClient);
    await queue.close();
  });
});

describe('QueueEvents.close()', () => {
  let mockClient: ReturnType<typeof makeMockClient>;

  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers({ shouldAdvanceTime: true });
    mockClient = makeMockClient();
    vi.mocked(GlideClient.createClient).mockResolvedValue(mockClient as any);
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should be idempotent', async () => {
    const qe = new QueueEvents('test-queue', { connection: connectionOpts });
    await qe.waitUntilReady();

    await qe.close();
    await qe.close();

    expect(mockClient.close).toHaveBeenCalledTimes(1);
  });

  it('should stop polling after close', async () => {
    const qe = new QueueEvents('test-queue', { connection: connectionOpts });
    await qe.waitUntilReady();
    await qe.close();

    const callsBefore = mockClient.xread.mock.calls.length;
    await vi.advanceTimersByTimeAsync(10000);
    expect(mockClient.xread.mock.calls.length).toBe(callsBefore);
  });
});

describe('FlowProducer.close()', () => {
  let mockClient: ReturnType<typeof makeMockClient>;

  beforeEach(() => {
    vi.clearAllMocks();
    mockClient = makeMockClient();
    vi.mocked(GlideClient.createClient).mockResolvedValue(mockClient as any);
  });

  it('should be idempotent', async () => {
    mockClient.fcall
      .mockResolvedValueOnce(LIBRARY_VERSION)
      .mockResolvedValueOnce('1');
    const fp = new FlowProducer({ connection: connectionOpts });

    // Force client creation via add
    await fp.add({
      name: 'parent',
      queueName: 'flow-queue',
      data: {},
      children: [],
    });

    await fp.close();
    await fp.close();

    expect(mockClient.close).toHaveBeenCalledTimes(1);
  });
});
