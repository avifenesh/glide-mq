import { describe, it, expect, vi, beforeEach } from 'vitest';
import { GlideClient } from '@glidemq/speedkey';
import { Queue } from '../src/queue';
import { Worker } from '../src/worker';
import { Job } from '../src/job';
import { LIBRARY_VERSION, CONSUMER_GROUP } from '../src/functions/index';

// Mock speedkey module
vi.mock('@glidemq/speedkey', () => {
  const MockGlideClient = {
    createClient: vi.fn(),
  };
  const MockGlideClusterClient = {
    createClient: vi.fn(),
  };
  // Mock Batch: accumulate commands, exec returns results
  class MockBatch {
    commands: any[] = [];
    constructor(_isAtomic?: boolean) {}
    xrange(...args: any[]) {
      this.commands.push({ cmd: 'xrange', args });
      return this;
    }
    hgetall(...args: any[]) {
      this.commands.push({ cmd: 'hgetall', args });
      return this;
    }
    hget(...args: any[]) {
      this.commands.push({ cmd: 'hget', args });
      return this;
    }
    lrange(...args: any[]) {
      this.commands.push({ cmd: 'lrange', args });
      return this;
    }
    llen(...args: any[]) {
      this.commands.push({ cmd: 'llen', args });
      return this;
    }
  }
  class MockClusterScanCursor {
    private done = true;
    isFinished() {
      return this.done;
    }
    getCursor() {
      return '0';
    }
  }
  return {
    GlideClient: MockGlideClient,
    GlideClusterClient: MockGlideClusterClient,
    InfBoundary: {
      PositiveInfinity: '+',
      NegativeInfinity: '-',
    },
    Batch: MockBatch,
    ClusterBatch: MockBatch,
    ClusterScanCursor: MockClusterScanCursor,
  };
});

function makeMockClient(overrides: Record<string, unknown> = {}) {
  const client: any = {
    fcall: vi.fn().mockResolvedValue(LIBRARY_VERSION),
    functionLoad: vi.fn(),
    hset: vi.fn(),
    hget: vi.fn().mockResolvedValue(null),
    hgetall: vi.fn().mockResolvedValue([]),
    hdel: vi.fn(),
    xadd: vi.fn(),
    xlen: vi.fn().mockResolvedValue(0),
    xrange: vi.fn().mockResolvedValue(null),
    xpending: vi.fn().mockResolvedValue([0, null, null, []]),
    xpendingWithOptions: vi.fn().mockResolvedValue([]),
    xgroupCreate: vi.fn(),
    xreadgroup: vi.fn().mockResolvedValue(null),
    zadd: vi.fn(),
    zcard: vi.fn().mockResolvedValue(0),
    zrange: vi.fn().mockResolvedValue([]),
    del: vi.fn(),
    scan: vi.fn().mockResolvedValue(['0', []]),
    smembers: vi.fn().mockResolvedValue(new Set()),
    hmget: vi.fn().mockResolvedValue([null, null]),
    close: vi.fn(),
    ...overrides,
  };
  client.exec = vi.fn().mockImplementation(async (batch: any) => {
    const results: any[] = [];
    for (const cmd of batch.commands ?? []) {
      const method = client[cmd.cmd];
      if (method) {
        results.push(await method(...cmd.args));
      } else {
        results.push(null);
      }
    }
    return results;
  });
  return client;
}

const connOpts = {
  connection: { addresses: [{ host: '127.0.0.1', port: 6379 }] },
};

// Helper to build queue keys for a given queue name
function buildQueueKeys(queueName: string) {
  const p = `glide:{${queueName}}`;
  return {
    id: `${p}:id`,
    stream: `${p}:stream`,
    scheduled: `${p}:scheduled`,
    completed: `${p}:completed`,
    failed: `${p}:failed`,
    events: `${p}:events`,
    meta: `${p}:meta`,
    dedup: `${p}:dedup`,
    rate: `${p}:rate`,
    schedulers: `${p}:schedulers`,
    job: (id: string) => `${p}:job:${id}`,
    deps: (id: string) => `${p}:deps:${id}`,
    parent: (id: string) => `${p}:parent:${id}`,
  };
}

// ---- Job state methods ----

describe('Job state check methods', () => {
  let mockClient: ReturnType<typeof makeMockClient>;

  beforeEach(() => {
    vi.clearAllMocks();
    mockClient = makeMockClient();
  });

  function makeJob(id: string) {
    const keys = buildQueueKeys('q');
    return new (Job as any)(mockClient, keys, id, 'test-job', {}, {}) as Job;
  }

  it('isCompleted returns true when state is completed', async () => {
    mockClient.hget.mockResolvedValue('completed');
    const job = makeJob('1');
    expect(await job.isCompleted()).toBe(true);
    expect(await job.isFailed()).toBe(false);
  });

  it('isFailed returns true when state is failed', async () => {
    mockClient.hget.mockResolvedValue('failed');
    const job = makeJob('2');
    expect(await job.isFailed()).toBe(true);
    expect(await job.isCompleted()).toBe(false);
  });

  it('isDelayed returns true when state is delayed', async () => {
    mockClient.hget.mockResolvedValue('delayed');
    const job = makeJob('3');
    expect(await job.isDelayed()).toBe(true);
  });

  it('isActive returns true when state is active', async () => {
    mockClient.hget.mockResolvedValue('active');
    const job = makeJob('4');
    expect(await job.isActive()).toBe(true);
  });

  it('isWaiting returns true when state is waiting', async () => {
    mockClient.hget.mockResolvedValue('waiting');
    const job = makeJob('5');
    expect(await job.isWaiting()).toBe(true);
  });

  it('getState returns unknown when hash does not exist', async () => {
    mockClient.hget.mockResolvedValue(null);
    const job = makeJob('99');
    expect(await job.getState()).toBe('unknown');
  });

  it('getState reads from the correct hash key', async () => {
    mockClient.hget.mockResolvedValue('waiting');
    const job = makeJob('42');
    await job.getState();
    expect(mockClient.hget).toHaveBeenCalledWith('glide:{q}:job:42', 'state');
  });
});

// ---- Queue.obliterate ----

describe('Queue.obliterate', () => {
  let mockClient: ReturnType<typeof makeMockClient>;

  beforeEach(() => {
    vi.clearAllMocks();
    mockClient = makeMockClient();
    vi.mocked(GlideClient.createClient).mockResolvedValue(mockClient as any);
  });

  it('deletes all static keys', async () => {
    const queue = new Queue('obliterate-test', connOpts);
    await queue.obliterate({ force: true });

    expect(mockClient.del).toHaveBeenCalledWith(
      expect.arrayContaining([
        'glide:{obliterate-test}:id',
        'glide:{obliterate-test}:stream',
        'glide:{obliterate-test}:scheduled',
        'glide:{obliterate-test}:completed',
        'glide:{obliterate-test}:failed',
        'glide:{obliterate-test}:events',
        'glide:{obliterate-test}:meta',
        'glide:{obliterate-test}:dedup',
        'glide:{obliterate-test}:rate',
        'glide:{obliterate-test}:schedulers',
      ]),
    );

    await queue.close();
  });

  it('scans and deletes job hashes and deps sets', async () => {
    // First scan call (job pattern): returns keys, then empty
    // Second scan call (deps pattern): returns keys, then empty
    mockClient.scan
      .mockResolvedValueOnce(['123', ['glide:{obliterate-test}:job:1', 'glide:{obliterate-test}:job:2']])
      .mockResolvedValueOnce(['0', []])
      .mockResolvedValueOnce(['456', ['glide:{obliterate-test}:deps:1']])
      .mockResolvedValueOnce(['0', []]);

    const queue = new Queue('obliterate-test', connOpts);
    await queue.obliterate({ force: true });

    // del called: once for static keys, once for job batch, once for deps batch
    expect(mockClient.del).toHaveBeenCalledTimes(3);
    expect(mockClient.del).toHaveBeenCalledWith(['glide:{obliterate-test}:job:1', 'glide:{obliterate-test}:job:2']);
    expect(mockClient.del).toHaveBeenCalledWith(['glide:{obliterate-test}:deps:1']);

    await queue.close();
  });

  it('fails when there are active jobs and force is false', async () => {
    mockClient.xpending.mockResolvedValue([3, '1-0', '3-0', [['consumer1', '3']]]);

    const queue = new Queue('obliterate-test', connOpts);
    await expect(queue.obliterate()).rejects.toThrow('Cannot obliterate');

    await queue.close();
  });

  it('succeeds when there are active jobs and force is true', async () => {
    mockClient.xpending.mockResolvedValue([3, '1-0', '3-0', [['consumer1', '3']]]);

    const queue = new Queue('obliterate-test', connOpts);
    await expect(queue.obliterate({ force: true })).resolves.toBeUndefined();

    await queue.close();
  });

  it('succeeds when consumer group does not exist (no active jobs)', async () => {
    mockClient.xpending.mockRejectedValue(new Error('NOGROUP'));

    const queue = new Queue('obliterate-test', connOpts);
    await expect(queue.obliterate()).resolves.toBeUndefined();

    await queue.close();
  });
});

// ---- Queue.getJobs ----

describe('Queue.getJobs', () => {
  let mockClient: ReturnType<typeof makeMockClient>;

  beforeEach(() => {
    vi.clearAllMocks();
    mockClient = makeMockClient();
    vi.mocked(GlideClient.createClient).mockResolvedValue(mockClient as any);
  });

  it('returns waiting jobs from the stream', async () => {
    mockClient.xrange.mockResolvedValue({
      '1-0': [['jobId', '1']],
      '2-0': [['jobId', '2']],
    });
    mockClient.hgetall
      .mockResolvedValueOnce([
        { field: 'id', value: '1' },
        { field: 'name', value: 'j1' },
        { field: 'data', value: '{}' },
        { field: 'opts', value: '{}' },
        { field: 'state', value: 'waiting' },
      ])
      .mockResolvedValueOnce([
        { field: 'id', value: '2' },
        { field: 'name', value: 'j2' },
        { field: 'data', value: '{}' },
        { field: 'opts', value: '{}' },
        { field: 'state', value: 'waiting' },
      ]);

    const queue = new Queue('getjobs-test', connOpts);
    const jobs = await queue.getJobs('waiting');

    expect(jobs).toHaveLength(2);
    expect(jobs[0].id).toBe('1');
    expect(jobs[1].id).toBe('2');

    await queue.close();
  });

  it('returns delayed jobs from the scheduled ZSet', async () => {
    mockClient.zrange.mockResolvedValue(['5', '6']);
    mockClient.hgetall
      .mockResolvedValueOnce([
        { field: 'id', value: '5' },
        { field: 'name', value: 'j5' },
        { field: 'data', value: '{}' },
        { field: 'opts', value: '{}' },
        { field: 'state', value: 'delayed' },
      ])
      .mockResolvedValueOnce([
        { field: 'id', value: '6' },
        { field: 'name', value: 'j6' },
        { field: 'data', value: '{}' },
        { field: 'opts', value: '{}' },
        { field: 'state', value: 'delayed' },
      ]);

    const queue = new Queue('getjobs-test', connOpts);
    const jobs = await queue.getJobs('delayed');

    expect(jobs).toHaveLength(2);
    expect(jobs[0].id).toBe('5');
    expect(jobs[1].id).toBe('6');
    expect(mockClient.zrange).toHaveBeenCalledWith('glide:{getjobs-test}:scheduled', { start: 0, end: -1 });

    await queue.close();
  });

  it('returns completed jobs from the completed ZSet', async () => {
    mockClient.zrange.mockResolvedValue(['10']);
    mockClient.hgetall.mockResolvedValueOnce([
      { field: 'id', value: '10' },
      { field: 'name', value: 'j10' },
      { field: 'data', value: '{}' },
      { field: 'opts', value: '{}' },
      { field: 'state', value: 'completed' },
      { field: 'returnvalue', value: '"done"' },
    ]);

    const queue = new Queue('getjobs-test', connOpts);
    const jobs = await queue.getJobs('completed');

    expect(jobs).toHaveLength(1);
    expect(jobs[0].id).toBe('10');
    expect(mockClient.zrange).toHaveBeenCalledWith('glide:{getjobs-test}:completed', { start: 0, end: -1 });

    await queue.close();
  });

  it('returns failed jobs from the failed ZSet', async () => {
    mockClient.zrange.mockResolvedValue(['20']);
    mockClient.hgetall.mockResolvedValueOnce([
      { field: 'id', value: '20' },
      { field: 'name', value: 'j20' },
      { field: 'data', value: '{}' },
      { field: 'opts', value: '{}' },
      { field: 'state', value: 'failed' },
      { field: 'failedReason', value: 'error' },
    ]);

    const queue = new Queue('getjobs-test', connOpts);
    const jobs = await queue.getJobs('failed');

    expect(jobs).toHaveLength(1);
    expect(jobs[0].id).toBe('20');

    await queue.close();
  });

  it('returns active jobs via XPENDING + XRANGE', async () => {
    mockClient.xpendingWithOptions.mockResolvedValue([['1-0', 'consumer1', 5000, 1]]);
    // XRANGE for each pending entry to get the jobId
    mockClient.xrange.mockResolvedValue({
      '1-0': [['jobId', '7']],
    });
    mockClient.hgetall.mockResolvedValueOnce([
      { field: 'id', value: '7' },
      { field: 'name', value: 'j7' },
      { field: 'data', value: '{}' },
      { field: 'opts', value: '{}' },
      { field: 'state', value: 'active' },
    ]);

    const queue = new Queue('getjobs-test', connOpts);
    const jobs = await queue.getJobs('active');

    expect(jobs).toHaveLength(1);
    expect(jobs[0].id).toBe('7');

    await queue.close();
  });

  it('returns empty array when no active jobs and consumer group does not exist', async () => {
    mockClient.xpendingWithOptions.mockRejectedValue(new Error('NOGROUP'));

    const queue = new Queue('getjobs-test', connOpts);
    const jobs = await queue.getJobs('active');

    expect(jobs).toHaveLength(0);

    await queue.close();
  });

  it('applies pagination for delayed jobs', async () => {
    mockClient.zrange.mockResolvedValue(['3']);
    mockClient.hgetall.mockResolvedValueOnce([
      { field: 'id', value: '3' },
      { field: 'name', value: 'j3' },
      { field: 'data', value: '{}' },
      { field: 'opts', value: '{}' },
      { field: 'state', value: 'delayed' },
    ]);

    const queue = new Queue('getjobs-test', connOpts);
    const jobs = await queue.getJobs('delayed', 2, 5);

    expect(mockClient.zrange).toHaveBeenCalledWith('glide:{getjobs-test}:scheduled', { start: 2, end: 5 });

    await queue.close();
  });

  it('returns empty array when stream is empty for waiting type', async () => {
    mockClient.xrange.mockResolvedValue(null);

    const queue = new Queue('getjobs-test', connOpts);
    const jobs = await queue.getJobs('waiting');

    expect(jobs).toHaveLength(0);

    await queue.close();
  });
});

// ---- Queue.getJobCountByTypes ----

describe('Queue.getJobCountByTypes', () => {
  let mockClient: ReturnType<typeof makeMockClient>;

  beforeEach(() => {
    vi.clearAllMocks();
    mockClient = makeMockClient();
    vi.mocked(GlideClient.createClient).mockResolvedValue(mockClient as any);
  });

  it('returns the same result as getJobCounts', async () => {
    mockClient.xlen.mockResolvedValue(10);
    mockClient.zcard
      .mockResolvedValueOnce(5) // completed
      .mockResolvedValueOnce(2) // failed
      .mockResolvedValueOnce(3); // scheduled
    mockClient.xpending.mockResolvedValue([4, '1-0', '4-0', [['c1', '4']]]);

    const queue = new Queue('counts-test', connOpts);
    const counts = await queue.getJobCountByTypes();

    expect(counts).toEqual({
      waiting: 6, // 10 - 4
      active: 4,
      delayed: 3,
      completed: 5,
      failed: 2,
    });

    await queue.close();
  });
});

// ---- Worker.drain ----

describe('Worker.drain', () => {
  let mockClient: ReturnType<typeof makeMockClient>;

  beforeEach(() => {
    vi.clearAllMocks();
    mockClient = makeMockClient();
  });

  it('closes the worker when stream and scheduled set are empty', async () => {
    // Mock both command and blocking clients
    const mockBlockingClient = makeMockClient();
    vi.mocked(GlideClient.createClient)
      .mockResolvedValueOnce(mockClient as any) // command client
      .mockResolvedValueOnce(mockBlockingClient as any); // blocking client

    mockClient.xlen.mockResolvedValue(0);
    mockClient.zcard.mockResolvedValue(0);

    const processor = vi.fn().mockResolvedValue('done');
    const worker = new Worker('drain-test', processor, connOpts);
    await worker.waitUntilReady();
    await worker.drain();

    // Worker should be closed after drain
    expect(mockClient.close).toHaveBeenCalled();
  });
});
