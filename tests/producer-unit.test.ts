import { describe, it, expect, vi, beforeEach } from 'vitest';
import { LIBRARY_VERSION } from '../src/functions/index';
import { Producer } from '../src/producer';

// Mock speedkey module
vi.mock('@glidemq/speedkey', () => {
  class MockGlideClient {
    static createClient = vi.fn();
  }
  class MockGlideClusterClient {
    static createClient = vi.fn();
  }
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
    functionList: vi.fn().mockResolvedValue([]),
    hset: vi.fn(),
    hgetall: vi.fn().mockResolvedValue([]),
    xadd: vi.fn(),
    xread: vi.fn().mockResolvedValue(null),
    zadd: vi.fn(),
    sadd: vi.fn().mockResolvedValue(1),
    exec: vi.fn().mockResolvedValue(['1', '2', '3']),
    close: vi.fn(),
    constructor: { name: 'GlideClient' },
    ...overrides,
  };
}

describe('Producer unit tests', () => {
  let mockClient: ReturnType<typeof makeMockClient>;

  beforeEach(() => {
    vi.clearAllMocks();
    mockClient = makeMockClient();
  });

  describe('skipEvents', () => {
    it('should pass skipEvents=1 when events:false is set', async () => {
      mockClient.fcall.mockResolvedValueOnce(LIBRARY_VERSION).mockResolvedValueOnce('1');
      const producer = new Producer('test-q', { client: mockClient as any, events: false });

      await producer.add('job', { x: 1 });

      const addJobCall = mockClient.fcall.mock.calls[1];
      expect(addJobCall[0]).toBe('glidemq_addJob');
      const args = addJobCall[2];
      expect(args[args.length - 1]).toBe('1');

      await producer.close();
    });

    it('should pass skipEvents=0 when events option is not set', async () => {
      mockClient.fcall.mockResolvedValueOnce(LIBRARY_VERSION).mockResolvedValueOnce('2');
      const producer = new Producer('test-q', { client: mockClient as any });

      await producer.add('job', { x: 1 });

      const addJobCall = mockClient.fcall.mock.calls[1];
      const args = addJobCall[2];
      expect(args[args.length - 1]).toBe('0');

      await producer.close();
    });

    it('should pass skipEvents=1 through dedup path', async () => {
      mockClient.fcall.mockResolvedValueOnce(LIBRARY_VERSION).mockResolvedValueOnce('3');
      const producer = new Producer('test-q', { client: mockClient as any, events: false });

      await producer.add('job', { x: 1 }, { deduplication: { id: 'dedup-1' } });

      const dedupCall = mockClient.fcall.mock.calls[1];
      expect(dedupCall[0]).toBe('glidemq_dedup');
      const args = dedupCall[2];
      expect(args[args.length - 1]).toBe('1');

      await producer.close();
    });
  });
});
