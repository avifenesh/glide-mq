import { describe, it, expect, vi, beforeEach } from 'vitest';
import { Job } from '../src/job';
import { buildKeys } from '../src/utils';

function makeMockClient(overrides: Record<string, unknown> = {}) {
  return {
    fcall: vi.fn(),
    functionLoad: vi.fn(),
    hset: vi.fn().mockResolvedValue(1),
    hget: vi.fn().mockResolvedValue(null),
    hgetall: vi.fn().mockResolvedValue([]),
    xadd: vi.fn().mockResolvedValue('1-0'),
    zadd: vi.fn().mockResolvedValue(1),
    smembers: vi.fn().mockResolvedValue(new Set()),
    close: vi.fn(),
    ...overrides,
  };
}

const keys = buildKeys('test-queue');

describe('Job', () => {
  let mockClient: ReturnType<typeof makeMockClient>;

  beforeEach(() => {
    vi.clearAllMocks();
    mockClient = makeMockClient();
  });

  describe('constructor', () => {
    it('should initialize all basic properties', () => {
      const job = new Job(
        mockClient as any,
        keys,
        '1',
        'send-email',
        { to: 'user@test.com' },
        {},
      );

      expect(job.id).toBe('1');
      expect(job.name).toBe('send-email');
      expect(job.data).toEqual({ to: 'user@test.com' });
      expect(job.opts).toEqual({});
      expect(job.attemptsMade).toBe(0);
      expect(job.progress).toBe(0);
      expect(job.returnvalue).toBeUndefined();
      expect(job.failedReason).toBeUndefined();
      expect(job.finishedOn).toBeUndefined();
      expect(job.processedOn).toBeUndefined();
    });
  });

  describe('updateProgress', () => {
    it('should update progress with a numeric value', async () => {
      const job = new Job(mockClient as any, keys, '1', 'job', {}, {});

      await job.updateProgress(50);

      expect(mockClient.hset).toHaveBeenCalledWith(
        'glide:{test-queue}:job:1',
        { progress: '50' },
      );
      expect(mockClient.xadd).toHaveBeenCalledWith(
        'glide:{test-queue}:events',
        [
          ['event', 'progress'],
          ['jobId', '1'],
          ['data', '50'],
        ],
      );
      expect(job.progress).toBe(50);
    });

    it('should update progress with an object value', async () => {
      const job = new Job(mockClient as any, keys, '2', 'job', {}, {});
      const progressObj = { step: 3, total: 10 };

      await job.updateProgress(progressObj);

      expect(mockClient.hset).toHaveBeenCalledWith(
        'glide:{test-queue}:job:2',
        { progress: JSON.stringify(progressObj) },
      );
      expect(job.progress).toEqual(progressObj);
    });
  });

  describe('updateData', () => {
    it('should update the data field in the hash and locally', async () => {
      const job = new Job(mockClient as any, keys, '1', 'job', { old: true }, {});

      const newData = { updated: true, count: 5 };
      await job.updateData(newData);

      expect(mockClient.hset).toHaveBeenCalledWith(
        'glide:{test-queue}:job:1',
        { data: JSON.stringify(newData) },
      );
      expect(job.data).toEqual(newData);
    });
  });

  describe('getChildrenValues', () => {
    it('should return empty object when no children exist', async () => {
      const job = new Job(mockClient as any, keys, '1', 'parent', {}, {});

      const values = await job.getChildrenValues();
      expect(values).toEqual({});
      expect(mockClient.smembers).toHaveBeenCalledWith('glide:{test-queue}:deps:1');
    });

    it('should fetch return values from child jobs', async () => {
      mockClient.smembers.mockResolvedValue(new Set(['glide:{test-queue}:10', 'glide:{test-queue}:11']));
      mockClient.hget
        .mockResolvedValueOnce('"result-a"')
        .mockResolvedValueOnce('"result-b"');
      const job = new Job(mockClient as any, keys, '1', 'parent', {}, {});

      const values = await job.getChildrenValues();

      expect(values['glide:{test-queue}:10']).toBe('result-a');
      expect(values['glide:{test-queue}:11']).toBe('result-b');
    });

    it('should skip children with null returnvalue', async () => {
      mockClient.smembers.mockResolvedValue(new Set(['glide:{test-queue}:10', 'glide:{test-queue}:11']));
      mockClient.hget
        .mockResolvedValueOnce('"done"')
        .mockResolvedValueOnce(null);
      const job = new Job(mockClient as any, keys, '1', 'parent', {}, {});

      const values = await job.getChildrenValues();

      expect(values['glide:{test-queue}:10']).toBe('done');
      expect(values['glide:{test-queue}:11']).toBeUndefined();
    });
  });

  describe('moveToFailed', () => {
    it('should call glidemq_fail FCALL with correct arguments', async () => {
      mockClient.fcall.mockResolvedValue('failed');
      const job = new Job(mockClient as any, keys, '5', 'job', {}, { attempts: 3 });
      job.entryId = '1700000000000-0';

      await job.moveToFailed(new Error('something broke'));

      expect(mockClient.fcall).toHaveBeenCalledTimes(1);
      const call = mockClient.fcall.mock.calls[0];
      expect(call[0]).toBe('glidemq_fail');
      // keys: [stream, failed, scheduled, events, job:5]
      expect(call[1]).toEqual([
        'glide:{test-queue}:stream',
        'glide:{test-queue}:failed',
        'glide:{test-queue}:scheduled',
        'glide:{test-queue}:events',
        'glide:{test-queue}:job:5',
      ]);
      // args: [jobId, entryId, failedReason, timestamp, maxAttempts, backoffDelay, group]
      const args = call[2];
      expect(args[0]).toBe('5'); // jobId
      expect(args[1]).toBe('1700000000000-0'); // entryId
      expect(args[2]).toBe('something broke'); // failedReason
      expect(args[4]).toBe('3'); // maxAttempts
    });

    it('should use fallback entryId when not set', async () => {
      mockClient.fcall.mockResolvedValue('failed');
      const job = new Job(mockClient as any, keys, '5', 'job', {}, {});

      await job.moveToFailed(new Error('fail'));

      const args = mockClient.fcall.mock.calls[0][2];
      expect(args[1]).toBe('0-0'); // fallback entryId
    });

    it('should calculate backoff delay when backoff option is set', async () => {
      mockClient.fcall.mockResolvedValue('retrying');
      const job = new Job(mockClient as any, keys, '5', 'job', {}, {
        attempts: 5,
        backoff: { type: 'fixed', delay: 1000 },
      });
      job.entryId = '1-0';
      job.attemptsMade = 1;

      await job.moveToFailed(new Error('transient'));

      const args = mockClient.fcall.mock.calls[0][2];
      expect(args[5]).toBe('1000'); // backoffDelay = fixed 1000
    });

    it('should increment attemptsMade when result is retrying', async () => {
      mockClient.fcall.mockResolvedValue('retrying');
      const job = new Job(mockClient as any, keys, '5', 'job', {}, {
        attempts: 3,
        backoff: { type: 'fixed', delay: 500 },
      });
      job.entryId = '1-0';
      job.attemptsMade = 0;

      await job.moveToFailed(new Error('err'));

      expect(job.attemptsMade).toBe(1);
      expect(job.failedReason).toBe('err');
    });

    it('should not increment attemptsMade when result is failed', async () => {
      mockClient.fcall.mockResolvedValue('failed');
      const job = new Job(mockClient as any, keys, '5', 'job', {}, {});
      job.entryId = '1-0';
      job.attemptsMade = 0;

      await job.moveToFailed(new Error('err'));

      expect(job.attemptsMade).toBe(0);
    });
  });

  describe('remove', () => {
    it('should call glidemq_removeJob FCALL', async () => {
      mockClient.fcall.mockResolvedValue(1);
      const job = new Job(mockClient as any, keys, '8', 'job', {}, {});

      await job.remove();

      expect(mockClient.fcall).toHaveBeenCalledTimes(1);
      const call = mockClient.fcall.mock.calls[0];
      expect(call[0]).toBe('glidemq_removeJob');
      expect(call[1]).toEqual([
        'glide:{test-queue}:job:8',
        'glide:{test-queue}:stream',
        'glide:{test-queue}:scheduled',
        'glide:{test-queue}:completed',
        'glide:{test-queue}:failed',
        'glide:{test-queue}:events',
      ]);
      expect(call[2]).toEqual(['8']);
    });
  });

  describe('retry', () => {
    it('should add job to scheduled ZSet and update state', async () => {
      const job = new Job(mockClient as any, keys, '3', 'job', {}, { priority: 0 });

      await job.retry();

      expect(mockClient.zadd).toHaveBeenCalledTimes(1);
      const zaddCall = mockClient.zadd.mock.calls[0];
      expect(zaddCall[0]).toBe('glide:{test-queue}:scheduled');
      // Score should be timestamp (priority=0, so score = 0 + now)
      const score = zaddCall[1][0].score;
      expect(score).toBeGreaterThan(0);
      expect(score).toBeLessThan(Date.now() + 1000); // should be roughly now

      expect(mockClient.hset).toHaveBeenCalledWith(
        'glide:{test-queue}:job:3',
        { state: 'delayed', failedReason: '' },
      );
    });

    it('should encode priority into the score', async () => {
      const job = new Job(mockClient as any, keys, '3', 'job', {}, { priority: 2 });

      await job.retry();

      const score = mockClient.zadd.mock.calls[0][1][0].score;
      const PRIORITY_SHIFT = 2 ** 42;
      // Score should include priority * PRIORITY_SHIFT
      expect(score).toBeGreaterThanOrEqual(2 * PRIORITY_SHIFT);
    });
  });

  describe('fromHash', () => {
    it('should parse all fields from a hash record', () => {
      const hash: Record<string, string> = {
        id: '10',
        name: 'compute',
        data: '{"input":42}',
        opts: '{"delay":1000}',
        timestamp: '1700000000000',
        attemptsMade: '3',
        processedOn: '1700000001000',
        finishedOn: '1700000002000',
        returnvalue: '{"output":84}',
        failedReason: '',
        parentId: '5',
        progress: '75',
      };

      const job = Job.fromHash(mockClient as any, keys, '10', hash);

      expect(job.id).toBe('10');
      expect(job.name).toBe('compute');
      expect(job.data).toEqual({ input: 42 });
      expect(job.opts).toEqual({ delay: 1000 });
      expect(job.timestamp).toBe(1700000000000);
      expect(job.attemptsMade).toBe(3);
      expect(job.processedOn).toBe(1700000001000);
      expect(job.finishedOn).toBe(1700000002000);
      expect(job.returnvalue).toEqual({ output: 84 });
      expect(job.failedReason).toBeUndefined(); // empty string becomes undefined
      expect(job.parentId).toBe('5');
      expect(job.progress).toBe(75);
    });

    it('should handle missing optional fields gracefully', () => {
      const hash: Record<string, string> = {
        id: '11',
        name: 'simple',
        data: '{}',
        opts: '{}',
        timestamp: '1700000000000',
        attemptsMade: '0',
      };

      const job = Job.fromHash(mockClient as any, keys, '11', hash);

      expect(job.processedOn).toBeUndefined();
      expect(job.finishedOn).toBeUndefined();
      expect(job.returnvalue).toBeUndefined();
      expect(job.failedReason).toBeUndefined();
      expect(job.parentId).toBeUndefined();
    });

    it('should parse object progress values', () => {
      const hash: Record<string, string> = {
        id: '12',
        name: 'upload',
        data: '{}',
        opts: '{}',
        timestamp: '1700000000000',
        attemptsMade: '0',
        progress: '{"step":3,"total":10}',
      };

      const job = Job.fromHash(mockClient as any, keys, '12', hash);

      expect(job.progress).toEqual({ step: 3, total: 10 });
    });

    it('should handle non-JSON progress as a number', () => {
      const hash: Record<string, string> = {
        id: '13',
        name: 'count',
        data: '{}',
        opts: '{}',
        timestamp: '1700000000000',
        attemptsMade: '0',
        progress: '50',
      };

      const job = Job.fromHash(mockClient as any, keys, '13', hash);

      expect(job.progress).toBe(50);
    });
  });
});
