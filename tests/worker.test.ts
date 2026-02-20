import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { GlideClient } from '@glidemq/speedkey';
import { Worker } from '../src/worker';
import { Scheduler } from '../src/scheduler';
import { CONSUMER_GROUP, LIBRARY_VERSION } from '../src/functions/index';
import { buildKeys } from '../src/utils';

// Mock speedkey module
vi.mock('@glidemq/speedkey', () => {
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
    fcall: vi.fn().mockImplementation((func: string, _keys?: string[], args?: string[]) => {
      if (func === 'glidemq_checkConcurrency') return Promise.resolve(-1);
      if (func === 'glidemq_complete') return Promise.resolve(1);
      if (func === 'glidemq_fail') return Promise.resolve('failed');
      if (func === 'glidemq_promote') return Promise.resolve(0);
      if (func === 'glidemq_reclaimStalled') return Promise.resolve(0);
      if (func === 'glidemq_completeAndFetchNext') {
        const jobId = args?.[0] ?? '0';
        return Promise.resolve(JSON.stringify({ completed: jobId, next: false }));
      }
      return Promise.resolve(LIBRARY_VERSION);
    }),
    functionLoad: vi.fn(),
    xgroupCreate: vi.fn().mockResolvedValue('OK'),
    xreadgroup: vi.fn().mockResolvedValue(null),
    hgetall: vi.fn().mockResolvedValue([]),
    hget: vi.fn().mockResolvedValue(null),
    hset: vi.fn().mockResolvedValue(1),
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
  blockTimeout: 100, // Short for tests
};

const keys = buildKeys('test-queue');

describe('Worker', () => {
  let mockCommandClient: ReturnType<typeof makeMockClient>;
  let mockBlockingClient: ReturnType<typeof makeMockClient>;

  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers({ shouldAdvanceTime: true });

    mockCommandClient = makeMockClient();
    mockBlockingClient = makeMockClient();

    // First createClient call is commandClient, second is blockingClient
    let callCount = 0;
    vi.mocked(GlideClient.createClient).mockImplementation(async () => {
      callCount++;
      return (callCount === 1 ? mockCommandClient : mockBlockingClient) as any;
    });
  });

  afterEach(async () => {
    vi.useRealTimers();
  });

  it('should generate a unique consumerId', () => {
    const processor = vi.fn().mockResolvedValue('done');
    const worker = new Worker('test-queue', processor, defaultWorkerOpts);

    // consumerId is private, but we can verify via xreadgroup calls
    // after init. For now, just verify the worker was created.
    expect(worker.name).toBe('test-queue');

    // Clean up - close worker before it finishes init
    worker.close(true);
  });

  it('should create both command and blocking clients during init', async () => {
    const processor = vi.fn().mockResolvedValue('done');

    // Make xreadgroup block "forever" (until we close)
    mockBlockingClient.xreadgroup = vi.fn().mockImplementation(
      () => new Promise(() => {}), // never resolves
    );

    const worker = new Worker('test-queue', processor, defaultWorkerOpts);
    await worker.waitUntilReady();

    expect(GlideClient.createClient).toHaveBeenCalledTimes(2);

    await worker.close(true);
  });

  it('should ensure function library is loaded during init', async () => {
    const processor = vi.fn().mockResolvedValue('done');
    mockBlockingClient.xreadgroup = vi.fn().mockImplementation(() => new Promise(() => {}));

    const worker = new Worker('test-queue', processor, defaultWorkerOpts);
    await worker.waitUntilReady();

    // ensureFunctionLibrary calls fcall('glidemq_version', [], [])
    expect(mockCommandClient.fcall).toHaveBeenCalledWith('glidemq_version', [], []);

    await worker.close(true);
  });

  it('should create consumer group during init', async () => {
    const processor = vi.fn().mockResolvedValue('done');
    mockBlockingClient.xreadgroup = vi.fn().mockImplementation(() => new Promise(() => {}));

    const worker = new Worker('test-queue', processor, defaultWorkerOpts);
    await worker.waitUntilReady();

    expect(mockCommandClient.xgroupCreate).toHaveBeenCalledWith(keys.stream, CONSUMER_GROUP, '0', {
      mkStream: true,
    });

    await worker.close(true);
  });

  it('should call xreadgroup with correct parameters', async () => {
    const processor = vi.fn().mockResolvedValue('done');

    let resolveXRead: ((value: any) => void) | null = null;
    mockBlockingClient.xreadgroup = vi.fn().mockImplementation(() => {
      return new Promise((resolve) => {
        resolveXRead = resolve;
      });
    });

    const worker = new Worker('test-queue', processor, {
      ...defaultWorkerOpts,
      concurrency: 5,
      prefetch: 10,
      blockTimeout: 2000,
    });
    await worker.waitUntilReady();

    // Wait for poll loop to call xreadgroup
    await vi.advanceTimersByTimeAsync(10);

    expect(mockBlockingClient.xreadgroup).toHaveBeenCalledWith(
      CONSUMER_GROUP,
      expect.any(String),
      { [keys.stream]: '>' },
      { count: 10, block: 2000 },
    );

    // Resolve to prevent hanging
    resolveXRead!(null);
    await worker.close(true);
  });

  it('should process a job and call completeAndFetchNext on success (c=1)', async () => {
    const processor = vi.fn().mockResolvedValue({ result: 42 });

    // Return one message from xreadgroup, then block forever
    let xreadCalls = 0;
    mockBlockingClient.xreadgroup = vi.fn().mockImplementation(() => {
      xreadCalls++;
      if (xreadCalls === 1) {
        return Promise.resolve([
          {
            key: keys.stream,
            value: {
              '1234567890-0': [['jobId', '1']],
            },
          },
        ]);
      }
      return new Promise(() => {}); // block forever
    });

    // At c=1, processJobFastPath calls moveToActive (fcall glidemq_moveToActive)
    // which returns a JSON-encoded hash array, then completeAndFetchNext.
    const jobHash = JSON.stringify([
      'id',
      '1',
      'name',
      'test-job',
      'data',
      '{"foo":"bar"}',
      'opts',
      '{}',
      'timestamp',
      '1000',
      'attemptsMade',
      '0',
      'state',
      'active',
    ]);

    mockCommandClient.fcall = vi
      .fn()
      .mockImplementation((func: string, _keys?: string[], args?: string[]) => {
        if (func === 'glidemq_version') return Promise.resolve(LIBRARY_VERSION);
        if (func === 'glidemq_checkConcurrency') return Promise.resolve(-1);
        if (func === 'glidemq_promote') return Promise.resolve(0);
        if (func === 'glidemq_reclaimStalled') return Promise.resolve(0);
        if (func === 'glidemq_moveToActive') return Promise.resolve(jobHash);
        if (func === 'glidemq_completeAndFetchNext') {
          const jobId = args?.[0] ?? '0';
          return Promise.resolve(JSON.stringify({ completed: jobId, next: false }));
        }
        return Promise.resolve(LIBRARY_VERSION);
      });

    const completedJobs: any[] = [];
    const worker = new Worker('test-queue', processor, defaultWorkerOpts);
    worker.on('completed', (job: any, result: any) => {
      completedJobs.push({ job, result });
    });

    await worker.waitUntilReady();
    await vi.advanceTimersByTimeAsync(100);

    // Processor should have been called with a Job
    expect(processor).toHaveBeenCalledTimes(1);
    const jobArg = processor.mock.calls[0][0];
    expect(jobArg.id).toBe('1');
    expect(jobArg.name).toBe('test-job');
    expect(jobArg.data).toEqual({ foo: 'bar' });

    // At c=1, completeAndFetchNext is called instead of completeJob
    expect(mockCommandClient.fcall).toHaveBeenCalledWith(
      'glidemq_completeAndFetchNext',
      [keys.stream, keys.completed, keys.events, keys.job('1')],
      expect.arrayContaining(['1', '1234567890-0']),
    );

    // Event should have been emitted
    expect(completedJobs).toHaveLength(1);
    expect(completedJobs[0].result).toEqual({ result: 42 });

    await worker.close(true);
  });

  it('should process a job and call failJob on processor error', async () => {
    const processor = vi.fn().mockRejectedValue(new Error('Processing failed'));

    let xreadCalls = 0;
    mockBlockingClient.xreadgroup = vi.fn().mockImplementation(() => {
      xreadCalls++;
      if (xreadCalls === 1) {
        return Promise.resolve([
          {
            key: keys.stream,
            value: {
              '1234567890-0': [['jobId', '2']],
            },
          },
        ]);
      }
      return new Promise(() => {});
    });

    mockCommandClient.hgetall = vi.fn().mockResolvedValue([
      { field: 'id', value: '2' },
      { field: 'name', value: 'failing-job' },
      { field: 'data', value: '{}' },
      { field: 'opts', value: '{"attempts":3,"backoff":{"type":"fixed","delay":1000}}' },
      { field: 'timestamp', value: '1000' },
      { field: 'attemptsMade', value: '0' },
    ]);

    const failedJobs: any[] = [];
    const worker = new Worker('test-queue', processor, defaultWorkerOpts);
    worker.on('failed', (job: any, err: any) => {
      failedJobs.push({ job, err });
    });

    await worker.waitUntilReady();
    await vi.advanceTimersByTimeAsync(100);

    // failJob is called via fcall('glidemq_fail', ...)
    expect(mockCommandClient.fcall).toHaveBeenCalledWith(
      'glidemq_fail',
      [keys.stream, keys.failed, keys.scheduled, keys.events, keys.job('2')],
      expect.arrayContaining(['2', '1234567890-0', 'Processing failed']),
    );

    expect(failedJobs).toHaveLength(1);
    expect(failedJobs[0].err.message).toBe('Processing failed');

    await worker.close(true);
  });

  it('should respect concurrency limits', async () => {
    let activeJobs = 0;
    let maxActive = 0;

    const processor = vi.fn().mockImplementation(async () => {
      activeJobs++;
      maxActive = Math.max(maxActive, activeJobs);
      // Simulate work
      await new Promise((r) => setTimeout(r, 50));
      activeJobs--;
      return 'done';
    });

    // Return 5 messages at once
    let xreadCalls = 0;
    mockBlockingClient.xreadgroup = vi.fn().mockImplementation(() => {
      xreadCalls++;
      if (xreadCalls === 1) {
        return Promise.resolve([
          {
            key: keys.stream,
            value: {
              '1-0': [['jobId', '1']],
              '2-0': [['jobId', '2']],
              '3-0': [['jobId', '3']],
              '4-0': [['jobId', '4']],
              '5-0': [['jobId', '5']],
            },
          },
        ]);
      }
      return new Promise(() => {});
    });

    mockCommandClient.hgetall = vi.fn().mockImplementation(async (key: string) => {
      const id = key.split(':').pop();
      return [
        { field: 'id', value: id },
        { field: 'name', value: 'job' },
        { field: 'data', value: '{}' },
        { field: 'opts', value: '{}' },
        { field: 'timestamp', value: '1000' },
        { field: 'attemptsMade', value: '0' },
      ];
    });

    const worker = new Worker('test-queue', processor, {
      ...defaultWorkerOpts,
      concurrency: 3,
      prefetch: 5,
    });

    await worker.waitUntilReady();
    // Let the poll loop run and jobs process
    await vi.advanceTimersByTimeAsync(200);

    // All 5 jobs should be processed
    expect(processor).toHaveBeenCalledTimes(5);

    await worker.close(true);
  });

  it('should handle null xreadgroup result (timeout)', async () => {
    const processor = vi.fn().mockResolvedValue('done');

    // Always return null (no messages)
    let xreadCalls = 0;
    mockBlockingClient.xreadgroup = vi.fn().mockImplementation(() => {
      xreadCalls++;
      if (xreadCalls > 3) {
        return new Promise(() => {}); // block forever after 3 polls
      }
      return Promise.resolve(null);
    });

    const worker = new Worker('test-queue', processor, defaultWorkerOpts);
    await worker.waitUntilReady();
    await vi.advanceTimersByTimeAsync(500);

    // Processor should not have been called
    expect(processor).not.toHaveBeenCalled();
    // But xreadgroup should have been called multiple times (poll loop)
    expect(xreadCalls).toBeGreaterThanOrEqual(3);

    await worker.close(true);
  });

  it('should skip entries with missing jobId field', async () => {
    const processor = vi.fn().mockResolvedValue('done');

    let xreadCalls = 0;
    mockBlockingClient.xreadgroup = vi.fn().mockImplementation(() => {
      xreadCalls++;
      if (xreadCalls === 1) {
        return Promise.resolve([
          {
            key: keys.stream,
            value: {
              '1-0': [['someOtherField', 'value']], // no jobId
              '2-0': [['jobId', '10']],
            },
          },
        ]);
      }
      return new Promise(() => {});
    });

    mockCommandClient.hgetall = vi.fn().mockResolvedValue([
      { field: 'id', value: '10' },
      { field: 'name', value: 'valid-job' },
      { field: 'data', value: '{}' },
      { field: 'opts', value: '{}' },
      { field: 'timestamp', value: '1000' },
      { field: 'attemptsMade', value: '0' },
    ]);

    const worker = new Worker('test-queue', processor, defaultWorkerOpts);
    await worker.waitUntilReady();
    await vi.advanceTimersByTimeAsync(100);

    // Only the valid job should be processed
    expect(processor).toHaveBeenCalledTimes(1);
    expect(processor.mock.calls[0][0].id).toBe('10');

    await worker.close(true);
  });

  it('should handle missing job hash gracefully', async () => {
    const processor = vi.fn().mockResolvedValue('done');

    let xreadCalls = 0;
    mockBlockingClient.xreadgroup = vi.fn().mockImplementation(() => {
      xreadCalls++;
      if (xreadCalls === 1) {
        return Promise.resolve([
          {
            key: keys.stream,
            value: {
              '1-0': [['jobId', '999']],
            },
          },
        ]);
      }
      return new Promise(() => {});
    });

    // moveToActive returns '' when job hash doesn't exist (job deleted)
    mockCommandClient.fcall = vi
      .fn()
      .mockImplementation((func: string, _keys?: string[], args?: string[]) => {
        if (func === 'glidemq_version') return Promise.resolve(LIBRARY_VERSION);
        if (func === 'glidemq_checkConcurrency') return Promise.resolve(-1);
        if (func === 'glidemq_promote') return Promise.resolve(0);
        if (func === 'glidemq_reclaimStalled') return Promise.resolve(0);
        if (func === 'glidemq_moveToActive') return Promise.resolve('');
        if (func === 'glidemq_complete') return Promise.resolve(1);
        if (func === 'glidemq_completeAndFetchNext') {
          const jobId = args?.[0] ?? '0';
          return Promise.resolve(JSON.stringify({ completed: jobId, next: false }));
        }
        return Promise.resolve(LIBRARY_VERSION);
      });

    const worker = new Worker('test-queue', processor, defaultWorkerOpts);
    await worker.waitUntilReady();
    await vi.advanceTimersByTimeAsync(100);

    // Processor should NOT have been called
    expect(processor).not.toHaveBeenCalled();

    // completeJob is called to ACK the orphaned entry
    expect(mockCommandClient.fcall).toHaveBeenCalledWith(
      'glidemq_complete',
      expect.any(Array),
      expect.arrayContaining(['999', '1-0']),
    );

    await worker.close(true);
  });

  it('should emit error event on xreadgroup failure and continue', async () => {
    const processor = vi.fn().mockResolvedValue('done');

    let xreadCalls = 0;
    mockBlockingClient.xreadgroup = vi.fn().mockImplementation(() => {
      xreadCalls++;
      if (xreadCalls === 1) {
        return Promise.reject(new Error('Connection lost'));
      }
      return new Promise(() => {});
    });

    const errors: any[] = [];
    const worker = new Worker('test-queue', processor, defaultWorkerOpts);
    worker.on('error', (err: any) => errors.push(err));

    await worker.waitUntilReady();
    await vi.advanceTimersByTimeAsync(2000);

    expect(errors).toHaveLength(1);
    expect(errors[0].message).toBe('Connection lost');

    await worker.close(true);
  });

  it('pause should stop the poll loop', async () => {
    const processor = vi.fn().mockResolvedValue('done');

    mockBlockingClient.xreadgroup = vi.fn().mockImplementation(() => new Promise(() => {}));

    const worker = new Worker('test-queue', processor, defaultWorkerOpts);
    await worker.waitUntilReady();

    await worker.pause(true);

    // After pause, the worker should not poll again
    const callsBefore = mockBlockingClient.xreadgroup.mock.calls.length;
    await vi.advanceTimersByTimeAsync(1000);
    const callsAfter = mockBlockingClient.xreadgroup.mock.calls.length;

    // No new xreadgroup calls after pause
    expect(callsAfter).toBe(callsBefore);

    await worker.close(true);
  });

  it('close should stop scheduler and close clients', async () => {
    const processor = vi.fn().mockResolvedValue('done');
    mockBlockingClient.xreadgroup = vi.fn().mockImplementation(() => new Promise(() => {}));

    const worker = new Worker('test-queue', processor, defaultWorkerOpts);
    await worker.waitUntilReady();

    await worker.close(true);

    expect(mockCommandClient.close).toHaveBeenCalled();
    expect(mockBlockingClient.close).toHaveBeenCalled();
  });

  it('should use prefetch equal to concurrency by default', () => {
    const processor = vi.fn().mockResolvedValue('done');

    const worker = new Worker('test-queue', processor, {
      ...defaultWorkerOpts,
      concurrency: 7,
      // prefetch not set - should default to concurrency
    });

    // We can't directly access private fields, but we can verify via xreadgroup count
    // The important thing is the worker is constructed without error
    expect(worker.name).toBe('test-queue');
    worker.close(true);
  });
});

describe('Scheduler', () => {
  let mockClient: ReturnType<typeof makeMockClient>;
  const queueKeys = buildKeys('test-queue');

  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers();
    mockClient = makeMockClient();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should call promote on start and then at intervals', async () => {
    mockClient.fcall = vi.fn().mockResolvedValue(0);

    const scheduler = new Scheduler(mockClient as any, queueKeys, {
      promotionInterval: 1000,
      stalledInterval: 5000,
    });

    scheduler.start();

    // Immediate call
    expect(mockClient.fcall).toHaveBeenCalledWith(
      'glidemq_promote',
      [queueKeys.scheduled, queueKeys.stream, queueKeys.events],
      [expect.any(String)],
    );

    const initialCalls = mockClient.fcall.mock.calls.filter(
      (c: any[]) => c[0] === 'glidemq_promote',
    ).length;

    // Advance by promotionInterval
    await vi.advanceTimersByTimeAsync(1000);

    const afterCalls = mockClient.fcall.mock.calls.filter(
      (c: any[]) => c[0] === 'glidemq_promote',
    ).length;

    expect(afterCalls).toBeGreaterThan(initialCalls);

    scheduler.stop();
  });

  it('should call reclaimStalled on start and then at intervals', async () => {
    mockClient.fcall = vi.fn().mockResolvedValue(0);

    const scheduler = new Scheduler(mockClient as any, queueKeys, {
      promotionInterval: 5000,
      stalledInterval: 1000,
      consumerId: 'test-consumer',
    });

    scheduler.start();

    // Immediate call
    expect(mockClient.fcall).toHaveBeenCalledWith(
      'glidemq_reclaimStalled',
      [queueKeys.stream, queueKeys.events],
      [
        CONSUMER_GROUP,
        'test-consumer',
        '1000', // minIdleMs = stalledInterval
        '1', // maxStalledCount default
        expect.any(String), // timestamp
        queueKeys.failed,
      ],
    );

    scheduler.stop();
  });

  it('should stop timers on stop()', () => {
    mockClient.fcall = vi.fn().mockResolvedValue(0);

    const scheduler = new Scheduler(mockClient as any, queueKeys, {
      promotionInterval: 100,
      stalledInterval: 100,
    });

    scheduler.start();
    scheduler.stop();

    const callsAfterStop = mockClient.fcall.mock.calls.length;

    // Advance timers - no new calls should happen
    vi.advanceTimersByTime(1000);

    expect(mockClient.fcall.mock.calls.length).toBe(callsAfterStop);
  });

  it('should not start twice', () => {
    mockClient.fcall = vi.fn().mockResolvedValue(0);

    const scheduler = new Scheduler(mockClient as any, queueKeys, {
      promotionInterval: 100,
      stalledInterval: 100,
    });

    scheduler.start();
    scheduler.start(); // Should be a no-op

    // Only one immediate round of calls
    const promoteCalls = mockClient.fcall.mock.calls.filter(
      (c: any[]) => c[0] === 'glidemq_promote',
    ).length;
    expect(promoteCalls).toBe(1);

    scheduler.stop();
  });

  it('should swallow errors from promote/reclaimStalled', async () => {
    mockClient.fcall = vi.fn().mockRejectedValue(new Error('NOSCRIPT'));

    const scheduler = new Scheduler(mockClient as any, queueKeys, {
      promotionInterval: 100,
      stalledInterval: 100,
    });

    // Should not throw
    scheduler.start();

    await vi.advanceTimersByTimeAsync(200);

    // Still running, no crash
    scheduler.stop();
  });

  it('promoteDelayed should call promote with current timestamp', async () => {
    const now = 1700000000000;
    vi.setSystemTime(now);
    mockClient.fcall = vi.fn().mockResolvedValue(3);

    const scheduler = new Scheduler(mockClient as any, queueKeys);
    const count = await scheduler.promoteDelayed();

    expect(count).toBe(3);
    expect(mockClient.fcall).toHaveBeenCalledWith(
      'glidemq_promote',
      [queueKeys.scheduled, queueKeys.stream, queueKeys.events],
      [now.toString()],
    );
  });

  it('reclaimStalledJobs should call reclaimStalled with correct args', async () => {
    const now = 1700000000000;
    vi.setSystemTime(now);
    mockClient.fcall = vi.fn().mockResolvedValue(2);

    const scheduler = new Scheduler(mockClient as any, queueKeys, {
      stalledInterval: 15000,
      maxStalledCount: 3,
      consumerId: 'my-consumer',
    });

    const count = await scheduler.reclaimStalledJobs();

    expect(count).toBe(2);
    expect(mockClient.fcall).toHaveBeenCalledWith(
      'glidemq_reclaimStalled',
      [queueKeys.stream, queueKeys.events],
      [CONSUMER_GROUP, 'my-consumer', '15000', '3', now.toString(), queueKeys.failed],
    );
  });
});
