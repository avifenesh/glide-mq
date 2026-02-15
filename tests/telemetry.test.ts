import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { GlideClient } from '@glidemq/speedkey';
import { LIBRARY_VERSION } from '../src/functions/index';

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

// Build a mock span that records all calls
function makeMockSpan() {
  const span = {
    setAttribute: vi.fn().mockReturnThis(),
    setStatus: vi.fn().mockReturnThis(),
    recordException: vi.fn(),
    end: vi.fn(),
  };
  return span;
}

function makeMockTracer(span: ReturnType<typeof makeMockSpan>) {
  return {
    startSpan: vi.fn().mockReturnValue(span),
  };
}

function makeMockClient(overrides: Record<string, unknown> = {}) {
  return {
    fcall: vi.fn().mockImplementation((func: string) => {
      if (func === 'glidemq_checkConcurrency') return Promise.resolve(-1);
      return Promise.resolve(LIBRARY_VERSION);
    }),
    functionLoad: vi.fn(),
    hset: vi.fn(),
    hgetall: vi.fn().mockResolvedValue([]),
    xadd: vi.fn(),
    xgroupCreate: vi.fn().mockResolvedValue('OK'),
    xreadgroup: vi.fn().mockResolvedValue(null),
    zadd: vi.fn(),
    zcard: vi.fn().mockResolvedValue(0),
    smembers: vi.fn().mockResolvedValue(new Set()),
    close: vi.fn(),
    ...overrides,
  };
}

const connOpts = {
  connection: { addresses: [{ host: '127.0.0.1', port: 6379 }] },
};

describe('Telemetry', () => {
  let mockClient: ReturnType<typeof makeMockClient>;

  beforeEach(() => {
    vi.clearAllMocks();
    mockClient = makeMockClient();
    vi.mocked(GlideClient.createClient).mockResolvedValue(mockClient as any);
  });

  describe('setTracer', () => {
    it('should use user-provided tracer for Queue.add spans', async () => {
      const span = makeMockSpan();
      const tracer = makeMockTracer(span);

      // Import and configure telemetry
      const { setTracer } = await import('../src/telemetry');
      setTracer(tracer);

      const { Queue } = await import('../src/queue');
      const queue = new Queue('test-queue', connOpts);

      // Mock addJob to return a jobId
      mockClient.fcall.mockImplementation(async (func: string) => {
        if (func === 'glidemq_addJob') return '42';
        return LIBRARY_VERSION;
      });

      const job = await queue.add('email', { to: 'user@example.com' });

      expect(tracer.startSpan).toHaveBeenCalledWith('glide-mq.queue.add');
      expect(span.setAttribute).toHaveBeenCalledWith('glide-mq.queue', 'test-queue');
      expect(span.setAttribute).toHaveBeenCalledWith('glide-mq.job.name', 'email');
      expect(span.setAttribute).toHaveBeenCalledWith('glide-mq.job.delay', 0);
      expect(span.setAttribute).toHaveBeenCalledWith('glide-mq.job.priority', 0);
      expect(span.setAttribute).toHaveBeenCalledWith('glide-mq.job.id', '42');
      expect(span.setStatus).toHaveBeenCalledWith({ code: 0 });
      expect(span.end).toHaveBeenCalled();
      expect(job).not.toBeNull();
      expect(job!.id).toBe('42');

      await queue.close();

      // Clean up: reset the tracer so other tests are not affected
      setTracer(null as any);
    });

    it('should record delay and priority attributes on Queue.add span', async () => {
      const span = makeMockSpan();
      const tracer = makeMockTracer(span);

      const { setTracer } = await import('../src/telemetry');
      setTracer(tracer);

      const { Queue } = await import('../src/queue');
      const queue = new Queue('test-queue', connOpts);

      mockClient.fcall.mockImplementation(async (func: string) => {
        if (func === 'glidemq_addJob') return '99';
        return LIBRARY_VERSION;
      });

      await queue.add('report', { type: 'daily' }, { delay: 5000, priority: 3 });

      expect(span.setAttribute).toHaveBeenCalledWith('glide-mq.job.delay', 5000);
      expect(span.setAttribute).toHaveBeenCalledWith('glide-mq.job.priority', 3);

      await queue.close();
      setTracer(null as any);
    });

    it('should record error on span when Queue.add fails', async () => {
      const span = makeMockSpan();
      const tracer = makeMockTracer(span);

      const { setTracer } = await import('../src/telemetry');
      setTracer(tracer);

      const { Queue } = await import('../src/queue');
      const queue = new Queue('test-queue', connOpts);

      mockClient.fcall.mockRejectedValue(new Error('connection lost'));

      await expect(queue.add('email', {})).rejects.toThrow('connection lost');

      expect(span.setStatus).toHaveBeenCalledWith(
        expect.objectContaining({ code: 1, message: 'connection lost' }),
      );
      expect(span.recordException).toHaveBeenCalled();
      expect(span.end).toHaveBeenCalled();

      await queue.close();
      setTracer(null as any);
    });
  });

  describe('FlowProducer', () => {
    it('should create a span for FlowProducer.add with child count', async () => {
      const span = makeMockSpan();
      const tracer = makeMockTracer(span);

      const { setTracer } = await import('../src/telemetry');
      setTracer(tracer);

      const { FlowProducer } = await import('../src/flow-producer');
      const flowProducer = new FlowProducer(connOpts);

      // Mock addFlow to return a JSON-stringified array of IDs
      mockClient.fcall.mockImplementation(async (func: string) => {
        if (func === 'glidemq_addFlow') return JSON.stringify(['p1', 'c1', 'c2']);
        return LIBRARY_VERSION;
      });

      const result = await flowProducer.add({
        name: 'parent',
        queueName: 'flow-queue',
        data: { batch: true },
        children: [
          { name: 'child1', queueName: 'flow-queue', data: { step: 1 } },
          { name: 'child2', queueName: 'flow-queue', data: { step: 2 } },
        ],
      });

      expect(tracer.startSpan).toHaveBeenCalledWith('glide-mq.flow.add');
      expect(span.setAttribute).toHaveBeenCalledWith('glide-mq.queue', 'flow-queue');
      expect(span.setAttribute).toHaveBeenCalledWith('glide-mq.flow.name', 'parent');
      expect(span.setAttribute).toHaveBeenCalledWith('glide-mq.flow.childCount', 2);
      expect(span.end).toHaveBeenCalled();
      expect(result.job.id).toBe('p1');

      await flowProducer.close();
      setTracer(null as any);
    });
  });

  describe('no-op when tracing disabled', () => {
    it('should work correctly when no tracer is set and @opentelemetry/api is unavailable', async () => {
      const { setTracer } = await import('../src/telemetry');
      // Explicitly clear user tracer
      setTracer(null as any);

      const { Queue } = await import('../src/queue');
      const queue = new Queue('test-queue', connOpts);

      mockClient.fcall.mockImplementation(async (func: string) => {
        if (func === 'glidemq_addJob') return '1';
        return LIBRARY_VERSION;
      });

      // Should not throw - the no-op span path works fine
      const job = await queue.add('email', { to: 'test@test.com' });
      expect(job).not.toBeNull();
      expect(job!.id).toBe('1');

      await queue.close();
    });
  });

  describe('withSpan', () => {
    it('should call fn even when tracing is disabled (no-op)', async () => {
      const { withSpan, setTracer } = await import('../src/telemetry');
      setTracer(null as any);

      const fn = vi.fn().mockResolvedValue('result');
      const result = await withSpan('test.span', { key: 'val' }, fn);

      expect(fn).toHaveBeenCalled();
      expect(result).toBe('result');
    });

    it('should create span, set attributes, and end on success', async () => {
      const span = makeMockSpan();
      const tracer = makeMockTracer(span);

      const { withSpan, setTracer } = await import('../src/telemetry');
      setTracer(tracer);

      const result = await withSpan(
        'test.operation',
        { attr1: 'value1', attr2: 42 },
        async () => 'done',
      );

      expect(tracer.startSpan).toHaveBeenCalledWith('test.operation');
      expect(span.setAttribute).toHaveBeenCalledWith('attr1', 'value1');
      expect(span.setAttribute).toHaveBeenCalledWith('attr2', 42);
      expect(span.setStatus).toHaveBeenCalledWith({ code: 0 });
      expect(span.end).toHaveBeenCalled();
      expect(result).toBe('done');

      setTracer(null as any);
    });

    it('should record exception and re-throw on failure', async () => {
      const span = makeMockSpan();
      const tracer = makeMockTracer(span);

      const { withSpan, setTracer } = await import('../src/telemetry');
      setTracer(tracer);

      const error = new Error('boom');
      await expect(
        withSpan('test.fail', {}, async () => { throw error; }),
      ).rejects.toThrow('boom');

      expect(span.setStatus).toHaveBeenCalledWith(
        expect.objectContaining({ code: 1, message: 'boom' }),
      );
      expect(span.recordException).toHaveBeenCalledWith(error);
      expect(span.end).toHaveBeenCalled();

      setTracer(null as any);
    });
  });

  describe('withChildSpan', () => {
    it('should create a child span and end it on success', async () => {
      const span = makeMockSpan();
      const tracer = makeMockTracer(span);

      const { withChildSpan, setTracer } = await import('../src/telemetry');
      setTracer(tracer);

      const result = await withChildSpan(
        'test.child',
        { childAttr: 'yes' },
        async () => 123,
      );

      expect(tracer.startSpan).toHaveBeenCalledWith('test.child');
      expect(span.setAttribute).toHaveBeenCalledWith('childAttr', 'yes');
      expect(span.end).toHaveBeenCalled();
      expect(result).toBe(123);

      setTracer(null as any);
    });
  });
});
