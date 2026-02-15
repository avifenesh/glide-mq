import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { GlideClient } from 'speedkey';
import { Worker } from '../src/worker';
import { QueueEvents } from '../src/queue-events';
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
    // Return a promise that never resolves (simulates blocking) until explicitly resolved
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

/**
 * Helper: creates a mock client whose xreadgroup/xread blocks
 * (returns a never-resolving promise). This prevents the poll loop from spinning.
 */
function makePendingMockClient(overrides: Record<string, unknown> = {}) {
  const neverResolve = new Promise(() => {});

  const client = makeMockClient({
    xreadgroup: vi.fn().mockReturnValue(neverResolve),
    xread: vi.fn().mockReturnValue(neverResolve),
    ...overrides,
  });

  return client;
}

describe('Worker connection error recovery', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should emit error on poll failure', async () => {
    // Use pending clients so the poll loop blocks instead of spinning
    const initCommand = makeMockClient();
    const initBlocking = makePendingMockClient();

    // Make xreadgroup reject once (simulating connection error)
    initBlocking.xreadgroup.mockRejectedValueOnce(new Error('Connection lost'));

    let createCount = 0;
    vi.mocked(GlideClient.createClient).mockImplementation(async () => {
      createCount++;
      if (createCount === 1) return initCommand as any;
      return initBlocking as any;
    });

    const processor = vi.fn().mockResolvedValue('done');
    const worker = new Worker('test-queue', processor, defaultWorkerOpts);
    const errors: Error[] = [];
    worker.on('error', (err: Error) => errors.push(err));

    await worker.waitUntilReady();

    // Let the first poll run and fail
    await vi.advanceTimersByTimeAsync(200);

    expect(errors.length).toBeGreaterThanOrEqual(1);
    expect(errors[0].message).toBe('Connection lost');

    await worker.close(true);
  });

  it('should attempt reconnection after connection error', async () => {
    const initCommand = makeMockClient();
    const initBlocking = makePendingMockClient();

    // First poll rejects immediately
    initBlocking.xreadgroup.mockRejectedValueOnce(new Error('Connection lost'));

    // After reconnect, return a pending (blocking) client so it doesn't spin
    const reconnectCommand = makeMockClient();
    const reconnectBlocking = makePendingMockClient();

    let createCount = 0;
    vi.mocked(GlideClient.createClient).mockImplementation(async () => {
      createCount++;
      switch (createCount) {
        case 1: return initCommand as any;
        case 2: return initBlocking as any;
        case 3: return reconnectCommand as any;
        case 4: return reconnectBlocking as any;
        default: return makePendingMockClient() as any;
      }
    });

    const processor = vi.fn().mockResolvedValue('done');
    const worker = new Worker('test-queue', processor, defaultWorkerOpts);
    worker.on('error', () => {}); // suppress

    await worker.waitUntilReady();

    // First poll fails
    await vi.advanceTimersByTimeAsync(200);

    // Advance past 1s backoff
    await vi.advanceTimersToNextTimerAsync();

    // Reconnect should have created new clients
    expect(createCount).toBe(4);

    await worker.close(true);
  });

  it('should use exponential backoff on repeated reconnect failures', async () => {
    const initCommand = makeMockClient();
    const initBlocking = makePendingMockClient();

    // First poll rejects
    initBlocking.xreadgroup.mockRejectedValueOnce(new Error('Connection lost'));

    let createCount = 0;
    const createTimes: number[] = [];
    vi.mocked(GlideClient.createClient).mockImplementation(async () => {
      createCount++;
      createTimes.push(Date.now());
      if (createCount <= 2) {
        return (createCount === 1 ? initCommand : initBlocking) as any;
      }
      // Reconnect attempts fail
      throw new Error('Connection refused');
    });

    const processor = vi.fn().mockResolvedValue('done');
    const worker = new Worker('test-queue', processor, defaultWorkerOpts);
    worker.on('error', () => {}); // suppress

    await worker.waitUntilReady();

    // First poll fails -> reconnectAndResume runs immediately and fails ->
    // schedules setTimeout(reconnectAndResume, 2000)
    await vi.advanceTimersByTimeAsync(200);
    const countAfterImmediate = createCount;
    expect(countAfterImmediate).toBeGreaterThan(2); // init(2) + immediate reconnect(1)

    // Advance 2s to fire the first backoff timer (reconnect attempt #2)
    await vi.advanceTimersByTimeAsync(2000);
    const countAfterFirstTimer = createCount;
    expect(countAfterFirstTimer).toBeGreaterThan(countAfterImmediate);

    // Advance 4s to fire the second backoff timer (reconnect attempt #3, exponential)
    await vi.advanceTimersByTimeAsync(4000);
    const countAfterSecondTimer = createCount;
    expect(countAfterSecondTimer).toBeGreaterThan(countAfterFirstTimer);

    // Verify exponential backoff via timestamps:
    // The gap between reconnect attempts should grow.
    // createTimes[2] = immediate reconnect (no timer delay)
    // createTimes[3] = after ~2s timer
    // createTimes[4] = after ~4s timer
    const gap1 = createTimes[3] - createTimes[2]; // should be ~2000
    const gap2 = createTimes[4] - createTimes[3]; // should be ~4000
    expect(gap2).toBeGreaterThanOrEqual(gap1);

    await worker.close(true);
  });

  it('should not reconnect after close()', async () => {
    const initCommand = makeMockClient();
    const initBlocking = makePendingMockClient();

    // First poll rejects
    initBlocking.xreadgroup.mockRejectedValueOnce(new Error('Connection lost'));

    let createCount = 0;
    vi.mocked(GlideClient.createClient).mockImplementation(async () => {
      createCount++;
      if (createCount === 1) return initCommand as any;
      return initBlocking as any;
    });

    const processor = vi.fn().mockResolvedValue('done');
    const worker = new Worker('test-queue', processor, defaultWorkerOpts);
    worker.on('error', () => {}); // suppress

    await worker.waitUntilReady();
    await vi.advanceTimersByTimeAsync(200);

    // Close before backoff fires
    await worker.close(true);
    const countAfterClose = createCount;

    // Advance timers - no new clients should be created
    for (let i = 0; i < 5; i++) {
      await vi.advanceTimersToNextTimerAsync();
    }
    expect(createCount).toBe(countAfterClose);
  });

  it('should re-ensure function library after reconnect', async () => {
    const initCommand = makeMockClient();
    const initBlocking = makePendingMockClient();

    // First poll rejects
    initBlocking.xreadgroup.mockRejectedValueOnce(new Error('Connection lost'));

    const reconnectCommand = makeMockClient();
    const reconnectBlocking = makePendingMockClient();

    let createCount = 0;
    vi.mocked(GlideClient.createClient).mockImplementation(async () => {
      createCount++;
      switch (createCount) {
        case 1: return initCommand as any;
        case 2: return initBlocking as any;
        case 3: return reconnectCommand as any;
        case 4: return reconnectBlocking as any;
        default: return makePendingMockClient() as any;
      }
    });

    const processor = vi.fn().mockResolvedValue('done');
    const worker = new Worker('test-queue', processor, defaultWorkerOpts);
    worker.on('error', () => {});

    await worker.waitUntilReady();
    await vi.advanceTimersByTimeAsync(200);

    // Trigger reconnect
    await vi.advanceTimersToNextTimerAsync();

    // The reconnect should have called fcall on the new command client
    // to ensure the function library (glidemq_version check)
    expect(reconnectCommand.fcall).toHaveBeenCalled();

    await worker.close(true);
  });
});

describe('QueueEvents connection error recovery', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should emit error on poll failure', async () => {
    const initClient = makePendingMockClient();

    // First poll rejects
    initClient.xread.mockRejectedValueOnce(new Error('Connection lost'));

    vi.mocked(GlideClient.createClient).mockResolvedValue(initClient as any);

    const qe = new QueueEvents('test-queue', {
      connection: connectionOpts,
      blockTimeout: 100,
    });

    const errors: Error[] = [];
    qe.on('error', (err: Error) => errors.push(err));

    await qe.waitUntilReady();
    await vi.advanceTimersByTimeAsync(200);

    expect(errors.length).toBeGreaterThanOrEqual(1);
    expect(errors[0].message).toBe('Connection lost');

    await qe.close();
  });

  it('should attempt reconnection after connection error', async () => {
    const initClient = makePendingMockClient();
    const reconnectClient = makePendingMockClient();

    // First poll rejects
    initClient.xread.mockRejectedValueOnce(new Error('Connection lost'));

    let createCount = 0;
    vi.mocked(GlideClient.createClient).mockImplementation(async () => {
      createCount++;
      if (createCount === 1) return initClient as any;
      return reconnectClient as any;
    });

    const qe = new QueueEvents('test-queue', {
      connection: connectionOpts,
      blockTimeout: 100,
    });
    qe.on('error', () => {});

    await qe.waitUntilReady();
    await vi.advanceTimersByTimeAsync(200);

    // Advance past 1s backoff
    await vi.advanceTimersToNextTimerAsync();

    expect(createCount).toBe(2);
    await qe.close();
  });

  it('should not reconnect after close()', async () => {
    const initClient = makePendingMockClient();

    // First poll rejects
    initClient.xread.mockRejectedValueOnce(new Error('Connection lost'));

    let createCount = 0;
    vi.mocked(GlideClient.createClient).mockImplementation(async () => {
      createCount++;
      return initClient as any;
    });

    const qe = new QueueEvents('test-queue', {
      connection: connectionOpts,
      blockTimeout: 100,
    });
    qe.on('error', () => {});

    await qe.waitUntilReady();
    await vi.advanceTimersByTimeAsync(200);

    await qe.close();
    const countAfterClose = createCount;

    for (let i = 0; i < 5; i++) {
      await vi.advanceTimersToNextTimerAsync();
    }
    expect(createCount).toBe(countAfterClose);
  });
});
