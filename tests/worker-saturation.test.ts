import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { GlideClient } from '@glidemq/speedkey';
import { Worker } from '../src/worker';
import { LIBRARY_VERSION } from '../src/functions/index';

// Mock speedkey module
vi.mock('@glidemq/speedkey', () => {
  const MockGlideClient = {
    createClient: vi.fn(),
  };
  return { GlideClient: MockGlideClient };
});

function makeMockClient(overrides: Record<string, unknown> = {}) {
  return {
    fcall: vi.fn().mockImplementation((func: string, _keys?: string[], args?: string[]) => {
      if (func === 'glidemq_checkConcurrency') return Promise.resolve(-1);
      if (func === 'glidemq_complete') return Promise.resolve(1);
      if (func === 'glidemq_fail') return Promise.resolve('failed');
      if (func === 'glidemq_promote') return Promise.resolve(0);
      if (func === 'glidemq_reclaimStalled') return Promise.resolve(0);
      if (func === 'glidemq_moveToActive') {
        return Promise.resolve(
          JSON.stringify([
            'id',
            args?.[0] ?? '1',
            'name',
            'test-job',
            'data',
            '{}',
            'opts',
            '{}',
            'timestamp',
            '1000',
            'attemptsMade',
            '0',
            'state',
            'active',
          ]),
        );
      }
      if (func === 'glidemq_completeAndFetchNext') {
        const jobId = args?.[0] ?? '0';
        return Promise.resolve(JSON.stringify({ completed: jobId, next: false }));
      }
      return Promise.resolve(LIBRARY_VERSION);
    }),
    xgroupCreate: vi.fn().mockResolvedValue('OK'),
    xreadgroup: vi.fn().mockResolvedValue(null),
    hgetall: vi.fn().mockResolvedValue([]),
    hget: vi.fn().mockResolvedValue(null),
    hset: vi.fn().mockResolvedValue(1),
    close: vi.fn(),
    ...overrides,
  };
}

const defaultWorkerOpts = {
  connection: { addresses: [{ host: '127.0.0.1', port: 6379 }] },
  concurrency: 2,
  prefetch: 2,
  blockTimeout: 100,
};

describe('Worker Saturation', () => {
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

  it('should NOT call pollOnce repeatedly when at capacity', async () => {
    const pollOnceSpy = vi.spyOn(Worker.prototype as any, 'pollOnce');

    let finishJob: () => void;
    const processingPromise = new Promise<void>((resolve) => {
      finishJob = resolve;
    });
    const processor = vi.fn().mockImplementation(() => processingPromise);

    // Provide 2 jobs to fill concurrency=2
    let xreadCalls = 0;
    mockBlockingClient.xreadgroup = vi.fn().mockImplementation(() => {
      xreadCalls++;
      if (xreadCalls === 1) {
        return Promise.resolve([
          {
            key: 'test:stream',
            value: {
              '1-0': [['jobId', '1']],
              '2-0': [['jobId', '2']],
            },
          },
        ]);
      }
      return Promise.resolve(null);
    });

    const worker = new Worker('test-queue', processor, defaultWorkerOpts);
    await worker.waitUntilReady();

    // Advance timers to allow the worker to pick up jobs and start looping
    await vi.advanceTimersByTimeAsync(100);

    const calls = pollOnceSpy.mock.calls.length;

    // With optimized wait, should only be called a few times (initial calls + maybe one wait call)
    // Definitely NOT 20+
    expect(calls).toBeLessThanOrEqual(5);

    finishJob!();
    await worker.close(true);
  });
});
