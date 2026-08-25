import { describe, it, expect, vi, beforeEach } from 'vitest';
import { GlideClient, RequestError } from '@glidemq/speedkey';
import { Queue } from '../src/queue';
import { Worker } from '../src/worker';
import { Job } from '../src/job';
import { LIBRARY_VERSION, removeJob, revokeJob } from '../src/functions/index';
import { buildKeys } from '../src/utils';

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
  class MockRequestError extends Error {
    constructor(message?: string) {
      super(message);
      this.name = 'RequestError';
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
    RequestError: MockRequestError,
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
    rpop: vi.fn().mockResolvedValue(null),
    rpopCount: vi.fn().mockResolvedValue(null),
    zadd: vi.fn(),
    zcard: vi.fn().mockResolvedValue(0),
    zcount: vi.fn().mockResolvedValue(0),
    zrange: vi.fn().mockResolvedValue([]),
    lrange: vi.fn().mockResolvedValue([]),
    del: vi.fn(),
    unlink: vi.fn(),
    scan: vi.fn().mockResolvedValue(['0', []]),
    smembers: vi.fn().mockResolvedValue(new Set()),
    srem: vi.fn(),
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

  it('moveToDelayed rejects jobs that are not active in a worker', async () => {
    const job = makeJob('43') as any;
    await expect(job.moveToDelayed(Date.now() + 100)).rejects.toThrow('active in a Worker');
    expect(job.moveToDelayedRequest).toBeUndefined();
  });

  it('moveToDelayed(nextStep) rejects non-plain payloads', async () => {
    const keys = buildQueueKeys('q');
    const job = new (Job as any)(mockClient, keys, '44', 'test-job', Object.create(null), {}) as any;
    job.entryId = '1-0';
    await expect(job.moveToDelayed(Date.now() + 100, 'next')).rejects.toThrow('plain-object job data');
    expect(job.moveToDelayedRequest).toBeUndefined();
  });
});

// ---- Queue.pause suspended timeout sweep ----

describe('Queue.pause suspended timeout sweep', () => {
  let mockClient: ReturnType<typeof makeMockClient>;

  beforeEach(() => {
    vi.clearAllMocks();
    mockClient = makeMockClient();
    mockClient.zcount.mockResolvedValue(1);
    vi.mocked(GlideClient.createClient).mockResolvedValue(mockClient as any);
  });

  it('sweeps expired suspended jobs when pausing locally', async () => {
    mockClient.fcall.mockImplementation(async (name: string) => (name === 'glidemq_tryLock' ? 1 : 0));
    const queue = new Queue('pause-sweep-test', connOpts);

    await queue.pause();

    expect(mockClient.fcall).toHaveBeenCalledWith('glidemq_pause', expect.any(Array), []);
    expect(mockClient.fcall).toHaveBeenCalledWith('glidemq_sweepSuspended', expect.any(Array), expect.any(Array));
    expect(mockClient.fcall).toHaveBeenCalledWith('glidemq_unlock', expect.any(Array), expect.any(Array));

    await queue.close();
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

    // obliterate now uses UNLINK to defer memory reclaim off the main thread.
    expect(mockClient.unlink).toHaveBeenCalledWith(
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
    mockClient.scan.mockResolvedValueOnce([
      '0',
      ['glide:{obliterate-test}:job:1', 'glide:{obliterate-test}:job:2', 'glide:{obliterate-test}:deps:1'],
    ]);

    const queue = new Queue('obliterate-test', connOpts);
    await queue.obliterate({ force: true });

    expect(mockClient.unlink).toHaveBeenCalledTimes(2);
    expect(mockClient.unlink).toHaveBeenCalledWith([
      'glide:{obliterate-test}:job:1',
      'glide:{obliterate-test}:job:2',
      'glide:{obliterate-test}:deps:1',
    ]);

    await queue.close();
  });

  it('deletes the full queue namespace and removes its usage registry entry', async () => {
    const namespaceKeys = [
      'glide:{obliterate-test}:job:1',
      'glide:{obliterate-test}:job:1:usage-lock',
      'glide:{obliterate-test}:job:1:sub:subscriber',
      'glide:{obliterate-test}:log:1',
      'glide:{obliterate-test}:deps:1',
      'glide:{obliterate-test}:parents:1',
      'glide:{obliterate-test}:jstream:1',
      'glide:{obliterate-test}:signals:1',
      'glide:{obliterate-test}:group:group-a',
      'glide:{obliterate-test}:groupq:group-a',
      'glide:{obliterate-test}:orderdone:pending:group-a',
      'glide:{obliterate-test}:w:worker-a',
      'glide:{obliterate-test}:budget:flow-a',
      'glide:{obliterate-test}:usage:60000',
      'glide:{obliterate-test}:schedulers:lock:__tick__',
      'glide:{obliterate-test}:suspended:lock:__sweep__',
    ];
    mockClient.scan.mockResolvedValueOnce(['0', namespaceKeys]);

    const queue = new Queue('obliterate-test', connOpts);
    await queue.obliterate({ force: true });

    expect(mockClient.unlink).toHaveBeenCalledWith(
      expect.arrayContaining([
        'glide:{obliterate-test}:priority',
        'glide:{obliterate-test}:list-active',
        'glide:{obliterate-test}:suspended',
        'glide:{obliterate-test}:tpm',
      ]),
    );
    expect(mockClient.scan).toHaveBeenCalledWith('0', { match: 'glide:{obliterate-test}:*', count: 100 });
    expect(mockClient.unlink).toHaveBeenCalledWith(namespaceKeys);
    expect(mockClient.srem).toHaveBeenCalledWith('glide:usage:queues', ['obliterate-test']);

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

  it('keeps remove and revoke FCALL key signatures stable', async () => {
    const keys = buildKeys('getjobs-cleanup');
    mockClient.fcall.mockResolvedValueOnce(1).mockResolvedValueOnce('revoked');

    await removeJob(mockClient, keys, '1');
    expect(mockClient.fcall).toHaveBeenNthCalledWith(
      1,
      'glidemq_removeJob',
      [keys.job('1'), keys.stream, keys.scheduled, keys.completed, keys.failed, keys.events, keys.log('1')],
      ['1'],
    );

    await revokeJob(mockClient, keys, '2', 123, 'workers');
    expect(mockClient.fcall).toHaveBeenNthCalledWith(
      2,
      'glidemq_revoke',
      [keys.job('2'), keys.stream, keys.scheduled, keys.failed, keys.events],
      ['2', '123', 'workers'],
    );
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

  it('returns waiting jobs from every dispatch source and skips pending stream entries', async () => {
    const hashes: Record<string, { field: string; value: string }[]> = {};
    for (const id of ['prio-1', 'prio-2', 'lifo-2', 'lifo-1', 'fifo-1']) {
      hashes[`glide:{getjobs-test}:job:${id}`] = [
        { field: 'id', value: id },
        { field: 'name', value: id },
        { field: 'data', value: '{}' },
        { field: 'opts', value: '{}' },
        { field: 'state', value: 'waiting' },
      ];
    }
    mockClient.hgetall.mockImplementation(async (key: string) => hashes[key] ?? []);
    mockClient.lrange.mockImplementation(async (key: string) => {
      if (key.endsWith(':priority')) return ['prio-2', 'prio-1'];
      if (key.endsWith(':lifo')) return ['lifo-1', 'lifo-2'];
      return [];
    });
    mockClient.xrange.mockResolvedValueOnce({
      '1-0': [['jobId', 'active-fifo']],
      '2-0': [['jobId', 'fifo-1']],
    });
    mockClient.xpendingWithOptions.mockResolvedValueOnce([['1-0', 'consumer', 0, 1]]);

    const queue = new Queue('getjobs-test', connOpts);
    const jobs = await queue.getJobs('waiting', 1, 4);

    expect(jobs.map((job) => job.id)).toEqual(['prio-2', 'lifo-2', 'lifo-1', 'fifo-1']);
    expect(mockClient.lrange).toHaveBeenCalledWith('glide:{getjobs-test}:priority', -5, -1);
    expect(mockClient.lrange).toHaveBeenCalledWith('glide:{getjobs-test}:lifo', -3, -1);
    expect(mockClient.xrange).toHaveBeenCalledWith('glide:{getjobs-test}:stream', '-', '+', { count: 1000 });

    await queue.close();
  });

  it('propagates non-NOGROUP errors while checking pending waiting jobs', async () => {
    mockClient.xrange.mockResolvedValue({ '1-0': [['jobId', 'fifo-1']] });
    mockClient.xpendingWithOptions.mockRejectedValue(new RequestError('ERR ACL user lacks XPENDING permission'));

    const queue = new Queue('getjobs-test', connOpts);

    await expect(queue.getJobs('waiting')).rejects.toThrow('ERR ACL user lacks XPENDING permission');

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
    await queue.getJobs('delayed', 2, 5);

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
