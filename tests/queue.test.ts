import { describe, it, expect, vi, beforeEach } from 'vitest';
import { GlideClient } from '@glidemq/speedkey';
import { Queue } from '../src/queue';
import { LIBRARY_VERSION } from '../src/functions/index';
import { MAX_JOB_DATA_SIZE } from '../src/utils';

// Mock speedkey module
vi.mock('@glidemq/speedkey', () => {
  const MockGlideClient = {
    createClient: vi.fn(),
  };
  const MockGlideClusterClient = {
    createClient: vi.fn(),
  };
  const MockBatch = vi.fn().mockImplementation(() => ({
    fcall: vi.fn().mockReturnThis(),
  }));
  return {
    GlideClient: MockGlideClient,
    GlideClusterClient: MockGlideClusterClient,
    Batch: MockBatch,
    ClusterBatch: MockBatch,
    InfBoundary: { PositiveInfinity: '+', NegativeInfinity: '-' },
  };
});

function makeMockClient(overrides: Record<string, unknown> = {}) {
  return {
    fcall: vi.fn().mockResolvedValue(LIBRARY_VERSION),
    functionLoad: vi.fn(),
    hset: vi.fn(),
    hgetall: vi.fn().mockResolvedValue([]),
    xadd: vi.fn(),
    xread: vi.fn().mockResolvedValue(null),
    xrevrange: vi.fn().mockResolvedValue({}),
    xgroupCreate: vi.fn(),
    zadd: vi.fn(),
    smembers: vi.fn().mockResolvedValue(new Set()),
    sadd: vi.fn().mockResolvedValue(1),
    exec: vi.fn().mockResolvedValue(['1', '2', '3']),
    close: vi.fn(),
    ...overrides,
  };
}

const connOpts = {
  connection: { addresses: [{ host: '127.0.0.1', port: 6379 }] },
};

describe('Queue', () => {
  let mockClient: ReturnType<typeof makeMockClient>;

  beforeEach(() => {
    vi.clearAllMocks();
    mockClient = makeMockClient();
    vi.mocked(GlideClient.createClient).mockResolvedValue(mockClient as any);
  });

  describe('constructor', () => {
    it('should store the queue name and options', () => {
      const queue = new Queue('test-queue', connOpts);
      expect(queue.name).toBe('test-queue');
    });

    it('should not create a client until a method is called', () => {
      new Queue('test-queue', connOpts);
      expect(GlideClient.createClient).not.toHaveBeenCalled();
    });
  });

  describe('add', () => {
    it('should call glidemq_addJob FCALL and return a Job', async () => {
      mockClient.fcall
        .mockResolvedValueOnce(LIBRARY_VERSION) // version check
        .mockResolvedValueOnce('1'); // addJob returns job ID
      const queue = new Queue('test-queue', connOpts);

      const job = await queue.add('email', { to: 'user@test.com' });

      expect(job.id).toBe('1');
      expect(job.name).toBe('email');
      expect(job.data).toEqual({ to: 'user@test.com' });
    });

    it('should pass delay and priority to addJob', async () => {
      mockClient.fcall.mockResolvedValueOnce(LIBRARY_VERSION).mockResolvedValueOnce('2');
      const queue = new Queue('test-queue', connOpts);

      const job = await queue.add(
        'report',
        { type: 'daily' },
        {
          delay: 5000,
          priority: 3,
        },
      );

      expect(job.id).toBe('2');
      // Verify the fcall was called with correct args
      const addJobCall = mockClient.fcall.mock.calls[1];
      expect(addJobCall[0]).toBe('glidemq_addJob');
      // keys: [id, stream, scheduled, events]
      expect(addJobCall[1]).toHaveLength(4);
      // args include: jobName, data, opts, timestamp, delay, priority, parentId, maxAttempts
      const args = addJobCall[2];
      expect(args[0]).toBe('report'); // jobName
      expect(args[4]).toBe('5000'); // delay
      expect(args[5]).toBe('3'); // priority
    });

    it('should pass parent ID when parent option is set', async () => {
      mockClient.fcall.mockResolvedValueOnce(LIBRARY_VERSION).mockResolvedValueOnce('3');
      const queue = new Queue('test-queue', connOpts);

      const job = await queue.add(
        'child-task',
        { x: 1 },
        {
          parent: { queue: 'parent-queue', id: '42' },
        },
      );

      expect(job.parentId).toBe('42');
      const args = mockClient.fcall.mock.calls[1][2];
      expect(args[6]).toBe('42'); // parentId
    });

    it('should pass maxAttempts from opts.attempts', async () => {
      mockClient.fcall.mockResolvedValueOnce(LIBRARY_VERSION).mockResolvedValueOnce('4');
      const queue = new Queue('test-queue', connOpts);

      await queue.add('retryable', {}, { attempts: 5 });

      const args = mockClient.fcall.mock.calls[1][2];
      expect(args[7]).toBe('5'); // maxAttempts
    });

    it('should create client lazily on first add call', async () => {
      mockClient.fcall.mockResolvedValueOnce(LIBRARY_VERSION).mockResolvedValueOnce('1');
      const queue = new Queue('test-queue', connOpts);
      expect(GlideClient.createClient).not.toHaveBeenCalled();

      await queue.add('job', {});

      expect(GlideClient.createClient).toHaveBeenCalledTimes(1);
    });

    it('should reuse the same client across multiple add calls', async () => {
      mockClient.fcall
        .mockResolvedValueOnce(LIBRARY_VERSION) // version check (once)
        .mockResolvedValueOnce('1')
        .mockResolvedValueOnce('2');
      const queue = new Queue('test-queue', connOpts);

      await queue.add('job1', {});
      await queue.add('job2', {});

      expect(GlideClient.createClient).toHaveBeenCalledTimes(1);
    });

    it('should reject oversized ordering keys', async () => {
      mockClient.fcall.mockResolvedValueOnce(LIBRARY_VERSION);
      const queue = new Queue('test-queue', connOpts);
      const orderingKey = 'x'.repeat(257);

      await expect(queue.add('ordered', { x: 1 }, { ordering: { key: orderingKey } })).rejects.toThrow(
        'Ordering key exceeds maximum length',
      );
      expect(mockClient.fcall).toHaveBeenCalledTimes(1);
    });
  });

  describe('addAndWait', () => {
    it('should require a connection because it needs a blocking client', async () => {
      const queue = new Queue('test-queue', { client: mockClient as any } as any);
      await expect(queue.addAndWait('job', {}, { waitTimeout: 1000 })).rejects.toThrow('requires `connection`');
    });

    it('should reject removeOnComplete/removeOnFail because the fallback needs the job hash', async () => {
      const queue = new Queue('test-queue', connOpts);
      await expect(queue.addAndWait('job', {}, { waitTimeout: 1000, removeOnComplete: true })).rejects.toThrow(
        'does not support removeOnComplete/removeOnFail',
      );
      await expect(queue.addAndWait('job', {}, { waitTimeout: 1000, removeOnFail: true })).rejects.toThrow(
        'does not support removeOnComplete/removeOnFail',
      );
    });

    it('should fail before enqueue if the blocking wait client cannot be created', async () => {
      const commandClient = makeMockClient({
        fcall: vi.fn().mockResolvedValueOnce(LIBRARY_VERSION),
        xrevrange: vi.fn().mockResolvedValue({}),
      });

      let callCount = 0;
      vi.mocked(GlideClient.createClient).mockImplementation(async () => {
        callCount++;
        if (callCount === 1) return commandClient as any;
        throw new Error('blocking client failed');
      });

      const queue = new Queue('test-queue', connOpts);
      await expect(queue.addAndWait('job', {}, { waitTimeout: 1000 })).rejects.toThrow('blocking client failed');
      expect((commandClient.fcall as any).mock.calls.some((call: any[]) => call[0] === 'glidemq_addJob')).toBe(false);
    });

    it('should reconnect the blocking wait client after an xread error', async () => {
      vi.useFakeTimers({ shouldAdvanceTime: true });
      try {
        const commandClient = makeMockClient({
          fcall: vi
            .fn()
            .mockResolvedValueOnce(LIBRARY_VERSION) // ensureFunctionLibrary
            .mockResolvedValueOnce('1'), // addJob
          xrevrange: vi.fn().mockResolvedValue({}),
        });
        const blockingClient1 = makeMockClient({
          xread: vi.fn().mockRejectedValue(new Error('socket lost')),
        });
        const blockingClient2 = makeMockClient({
          xread: vi.fn().mockResolvedValue([
            {
              key: 'glide:{test-queue}:events',
              value: {
                '1-0': [
                  ['event', 'completed'],
                  ['jobId', '1'],
                  ['returnvalue', '"done"'],
                ],
              },
            },
          ]),
        });

        let callCount = 0;
        vi.mocked(GlideClient.createClient).mockImplementation(async () => {
          callCount++;
          if (callCount === 1) return commandClient as any;
          if (callCount === 2) return blockingClient1 as any;
          return blockingClient2 as any;
        });

        const queue = new Queue('test-queue', connOpts);
        const pending = queue.addAndWait('job', {}, { waitTimeout: 3000 });
        await vi.advanceTimersByTimeAsync(1100);

        await expect(pending).resolves.toBe('done');
        expect(blockingClient1.close).toHaveBeenCalled();
        expect(blockingClient2.xread).toHaveBeenCalled();

        await queue.close();
      } finally {
        vi.useRealTimers();
      }
    });

    it('should retry when reconnecting the blocking wait client also fails once', async () => {
      vi.useFakeTimers({ shouldAdvanceTime: true });
      try {
        const commandClient = makeMockClient({
          fcall: vi.fn().mockResolvedValueOnce(LIBRARY_VERSION).mockResolvedValueOnce('1'),
          xrevrange: vi.fn().mockResolvedValue({}),
        });
        const blockingClient1 = makeMockClient({
          xread: vi.fn().mockRejectedValue(new Error('socket lost')),
        });
        const blockingClient2 = makeMockClient({
          xread: vi.fn().mockResolvedValue([
            {
              key: 'glide:{test-queue}:events',
              value: {
                '1-0': [
                  ['event', 'completed'],
                  ['jobId', '1'],
                  ['returnvalue', '"done"'],
                ],
              },
            },
          ]),
        });

        let callCount = 0;
        vi.mocked(GlideClient.createClient).mockImplementation(async () => {
          callCount++;
          if (callCount === 1) return commandClient as any;
          if (callCount === 2) return blockingClient1 as any;
          if (callCount === 3) throw new Error('reconnect failed');
          return blockingClient2 as any;
        });

        const queue = new Queue('test-queue', connOpts);
        const pending = queue.addAndWait('job', {}, { waitTimeout: 5000 });
        await vi.advanceTimersByTimeAsync(2100);

        await expect(pending).resolves.toBe('done');
        expect(blockingClient1.close).toHaveBeenCalled();
        expect(blockingClient2.xread).toHaveBeenCalled();

        await queue.close();
      } finally {
        vi.useRealTimers();
      }
    });

    it('should fall back to the job hash if the terminal event is no longer in the stream', async () => {
      const commandClient = makeMockClient({
        fcall: vi.fn().mockResolvedValueOnce(LIBRARY_VERSION).mockResolvedValueOnce('1'),
        xrevrange: vi.fn().mockResolvedValue({}),
        hget: vi.fn().mockImplementation(async (_key: string, field: string) => {
          if (field === 'state') return 'completed';
          if (field === 'returnvalue') return '"done"';
          return null;
        }),
      });
      const blockingClient = makeMockClient({
        xread: vi.fn().mockResolvedValue(null),
      });

      let callCount = 0;
      vi.mocked(GlideClient.createClient).mockImplementation(async () => {
        callCount++;
        return (callCount === 1 ? commandClient : blockingClient) as any;
      });

      const queue = new Queue('test-queue', connOpts);
      await expect(queue.addAndWait('job', {}, { waitTimeout: 1 })).resolves.toBe('done');
    });
  });

  describe('addBulk', () => {
    it('should add multiple jobs and return an array of Job instances', async () => {
      mockClient.fcall.mockResolvedValueOnce(LIBRARY_VERSION);
      mockClient.exec.mockResolvedValueOnce(['1', '2', '3']);
      const queue = new Queue('test-queue', connOpts);

      const jobs = await queue.addBulk([
        { name: 'a', data: { x: 1 } },
        { name: 'b', data: { x: 2 } },
        { name: 'c', data: { x: 3 } },
      ]);

      expect(jobs).toHaveLength(3);
      expect(jobs[0].id).toBe('1');
      expect(jobs[0].name).toBe('a');
      expect(jobs[1].id).toBe('2');
      expect(jobs[2].id).toBe('3');
    });

    it('should return an empty array for empty input', async () => {
      mockClient.fcall.mockResolvedValueOnce(LIBRARY_VERSION);
      const queue = new Queue('test-queue', connOpts);

      const jobs = await queue.addBulk([]);
      expect(jobs).toEqual([]);
    });

    it('should reject oversized ordering keys in bulk entries', async () => {
      mockClient.fcall.mockResolvedValueOnce(LIBRARY_VERSION);
      const queue = new Queue('test-queue', connOpts);
      const orderingKey = 'x'.repeat(257);

      await expect(
        queue.addBulk([{ name: 'a', data: { x: 1 }, opts: { ordering: { key: orderingKey } } }]),
      ).rejects.toThrow('Ordering key exceeds maximum length');
      expect(mockClient.exec).not.toHaveBeenCalled();
    });
  });

  describe('getJob', () => {
    it('should return a Job when the hash exists', async () => {
      mockClient.fcall.mockResolvedValueOnce(LIBRARY_VERSION);
      mockClient.hgetall.mockResolvedValue([
        { field: 'id', value: '5' },
        { field: 'name', value: 'process' },
        { field: 'data', value: '{"key":"val"}' },
        { field: 'opts', value: '{}' },
        { field: 'timestamp', value: '1700000000000' },
        { field: 'attemptsMade', value: '2' },
        { field: 'state', value: 'active' },
      ]);
      const queue = new Queue('test-queue', connOpts);

      const job = await queue.getJob('5');

      expect(job).not.toBeNull();
      expect(job!.id).toBe('5');
      expect(job!.name).toBe('process');
      expect(job!.data).toEqual({ key: 'val' });
      expect(job!.attemptsMade).toBe(2);
      expect(job!.timestamp).toBe(1700000000000);
    });

    it('should return null when the hash does not exist', async () => {
      mockClient.fcall.mockResolvedValueOnce(LIBRARY_VERSION);
      mockClient.hgetall.mockResolvedValue([]);
      const queue = new Queue('test-queue', connOpts);

      const job = await queue.getJob('999');
      expect(job).toBeNull();
    });

    it('should populate finishedOn and returnvalue for completed jobs', async () => {
      mockClient.fcall.mockResolvedValueOnce(LIBRARY_VERSION);
      mockClient.hgetall.mockResolvedValue([
        { field: 'id', value: '6' },
        { field: 'name', value: 'calc' },
        { field: 'data', value: '{}' },
        { field: 'opts', value: '{}' },
        { field: 'timestamp', value: '1700000000000' },
        { field: 'attemptsMade', value: '0' },
        { field: 'finishedOn', value: '1700000001000' },
        { field: 'returnvalue', value: '"result-value"' },
        { field: 'state', value: 'completed' },
      ]);
      const queue = new Queue('test-queue', connOpts);

      const job = await queue.getJob('6');

      expect(job!.finishedOn).toBe(1700000001000);
      expect(job!.returnvalue).toBe('result-value');
    });

    it('should populate failedReason for failed jobs', async () => {
      mockClient.fcall.mockResolvedValueOnce(LIBRARY_VERSION);
      mockClient.hgetall.mockResolvedValue([
        { field: 'id', value: '7' },
        { field: 'name', value: 'broken' },
        { field: 'data', value: '{}' },
        { field: 'opts', value: '{}' },
        { field: 'timestamp', value: '1700000000000' },
        { field: 'attemptsMade', value: '3' },
        { field: 'failedReason', value: 'connection timeout' },
        { field: 'state', value: 'failed' },
      ]);
      const queue = new Queue('test-queue', connOpts);

      const job = await queue.getJob('7');

      expect(job!.failedReason).toBe('connection timeout');
      expect(job!.attemptsMade).toBe(3);
    });
  });

  describe('pause', () => {
    it('should call glidemq_pause FCALL with meta and events keys', async () => {
      mockClient.fcall
        .mockResolvedValueOnce(LIBRARY_VERSION) // version check
        .mockResolvedValueOnce(1); // pause returns 1
      const queue = new Queue('test-queue', connOpts);

      await queue.pause();

      const pauseCall = mockClient.fcall.mock.calls[1];
      expect(pauseCall[0]).toBe('glidemq_pause');
      expect(pauseCall[1]).toEqual(['glide:{test-queue}:meta', 'glide:{test-queue}:events']);
    });
  });

  describe('resume', () => {
    it('should call glidemq_resume FCALL with meta and events keys', async () => {
      mockClient.fcall.mockResolvedValueOnce(LIBRARY_VERSION).mockResolvedValueOnce(1);
      const queue = new Queue('test-queue', connOpts);

      await queue.resume();

      const resumeCall = mockClient.fcall.mock.calls[1];
      expect(resumeCall[0]).toBe('glidemq_resume');
      expect(resumeCall[1]).toEqual(['glide:{test-queue}:meta', 'glide:{test-queue}:events']);
    });
  });

  describe('close', () => {
    it('should close the client connection', async () => {
      mockClient.fcall.mockResolvedValueOnce(LIBRARY_VERSION).mockResolvedValueOnce('1');
      const queue = new Queue('test-queue', connOpts);

      // Force client creation
      await queue.add('job', {});
      await queue.close();

      expect(mockClient.close).toHaveBeenCalledTimes(1);
    });

    it('should be safe to call close when no client exists', async () => {
      const queue = new Queue('test-queue', connOpts);
      // Should not throw
      await queue.close();
    });

    it('should reject operations after close', async () => {
      mockClient.fcall.mockResolvedValueOnce(LIBRARY_VERSION).mockResolvedValueOnce('1');

      const queue = new Queue('test-queue', connOpts);

      await queue.add('job1', {});
      await queue.close();

      // After close, getClient should throw
      await expect(queue.add('job2', {})).rejects.toThrow('Queue is closing');
    });
  });

  describe('key generation', () => {
    it('should use default prefix "glide" when no prefix specified', async () => {
      mockClient.fcall.mockResolvedValueOnce(LIBRARY_VERSION).mockResolvedValueOnce('1');
      const queue = new Queue('my-queue', connOpts);
      await queue.add('test', {});

      // Check keys passed to addJob FCALL
      const keys = mockClient.fcall.mock.calls[1][1];
      expect(keys[0]).toBe('glide:{my-queue}:id');
      expect(keys[1]).toBe('glide:{my-queue}:stream');
      expect(keys[2]).toBe('glide:{my-queue}:scheduled');
      expect(keys[3]).toBe('glide:{my-queue}:events');
    });

    it('should use custom prefix when specified', async () => {
      mockClient.fcall.mockResolvedValueOnce(LIBRARY_VERSION).mockResolvedValueOnce('1');
      const queue = new Queue('orders', {
        ...connOpts,
        prefix: 'myapp',
      });
      await queue.add('test', {});

      const keys = mockClient.fcall.mock.calls[1][1];
      expect(keys[0]).toBe('myapp:{orders}:id');
    });
  });

  describe('size limits', () => {
    it('add rejects oversized job data', async () => {
      mockClient.fcall.mockResolvedValueOnce(LIBRARY_VERSION);
      const queue = new Queue('test-queue', connOpts);
      const oversized = { data: 'x'.repeat(MAX_JOB_DATA_SIZE) };
      await expect(queue.add('test', oversized)).rejects.toThrow('Job data exceeds maximum size');
    });

    it('addBulk rejects oversized job data', async () => {
      mockClient.fcall.mockResolvedValueOnce(LIBRARY_VERSION);
      const queue = new Queue('test-queue', connOpts);
      const oversized = { data: 'x'.repeat(MAX_JOB_DATA_SIZE) };
      await expect(queue.addBulk([{ name: 'test', data: oversized }])).rejects.toThrow('Job data exceeds maximum size');
    });

    it('add enforces byte length not character count', async () => {
      mockClient.fcall.mockResolvedValueOnce(LIBRARY_VERSION);
      const queue = new Queue('test-queue', connOpts);
      const multiByteChar = '\u4e16'; // 3 bytes in UTF-8
      const count = Math.ceil(MAX_JOB_DATA_SIZE / 3) + 1;
      const oversized = { data: multiByteChar.repeat(count) };
      await expect(queue.add('test', oversized)).rejects.toThrow('Job data exceeds maximum size');
    });
  });
});
