/**
 * Worker lifecycle unit tests: reconnect vs close races, heartbeat cleanup,
 * pause semantics and limiter waits. Mocked clients, no Valkey needed.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { GlideClient } from '@glidemq/speedkey';
import { Worker } from '../src/worker';
import { QueueEvents } from '../src/queue-events';
import { LIBRARY_VERSION } from '../src/functions/index';

vi.mock('@glidemq/speedkey', () => {
  const MockGlideClient = { createClient: vi.fn() };
  const MockGlideClusterClient = { createClient: vi.fn() };
  return {
    GlideClient: MockGlideClient,
    GlideClusterClient: MockGlideClusterClient,
    TimeUnit: { Milliseconds: 'PX' },
  };
});

const neverResolve = () => new Promise(() => {});

function makeMockClient(overrides: Record<string, unknown> = {}) {
  return {
    fcall: vi.fn().mockImplementation((func: string) => {
      if (func === 'glidemq_checkConcurrency') return Promise.resolve(-1);
      return Promise.resolve(LIBRARY_VERSION);
    }),
    functionLoad: vi.fn(),
    xgroupCreate: vi.fn().mockResolvedValue('OK'),
    xreadgroup: vi.fn().mockImplementation(neverResolve),
    xread: vi.fn().mockImplementation(neverResolve),
    hgetall: vi.fn().mockResolvedValue([]),
    hget: vi.fn().mockResolvedValue(null),
    hmget: vi.fn().mockResolvedValue([null, null, null, null]),
    hset: vi.fn().mockResolvedValue(1),
    set: vi.fn().mockResolvedValue('OK'),
    del: vi.fn().mockResolvedValue(1),
    ping: vi.fn().mockResolvedValue('PONG'),
    close: vi.fn(),
    ...overrides,
  };
}

function deferred<T>() {
  let resolve!: (v: T) => void;
  let reject!: (e: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

const connection = { addresses: [{ host: '127.0.0.1', port: 6379 }] };

describe('reconnect racing close()', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('Worker disposes a client created by a reconnect that finishes after close()', async () => {
    const initCommand = makeMockClient();
    const initBlocking = makeMockClient();
    initBlocking.xreadgroup.mockRejectedValueOnce(new Error('Connection lost'));
    const reconnectCommand = makeMockClient();
    const pendingCreate = deferred<any>();

    let createCount = 0;
    vi.mocked(GlideClient.createClient).mockImplementation(async () => {
      createCount++;
      if (createCount === 1) return initCommand as any;
      if (createCount === 2) return initBlocking as any;
      if (createCount === 3) return pendingCreate.promise;
      return makeMockClient() as any;
    });

    const worker = new Worker('lifecycle-w1', vi.fn(), { connection, blockTimeout: 100 });
    worker.on('error', () => {});
    await worker.waitUntilReady();

    // Poll fails, reconnect starts and blocks on createClient for the command client.
    await vi.advanceTimersByTimeAsync(5000);
    expect(createCount).toBe(3);

    await worker.close(true);
    pendingCreate.resolve(reconnectCommand);
    await vi.advanceTimersByTimeAsync(5000);

    expect(reconnectCommand.close).toHaveBeenCalledTimes(1);
    expect(createCount).toBe(3);
    expect((worker as any).scheduler).toBeNull();
    expect((worker as any).workerHeartbeatTimer).toBeNull();
    expect((worker as any).commandClient).toBeNull();
    expect((worker as any).blockingClient).toBeNull();
  });

  it('Worker disposes a blocking client created by a reconnect that finishes after close()', async () => {
    const initCommand = makeMockClient();
    const initBlocking = makeMockClient();
    initBlocking.xreadgroup.mockRejectedValueOnce(new Error('Connection lost'));
    const reconnectCommand = makeMockClient();
    const reconnectBlocking = makeMockClient();
    const pendingBlocking = deferred<any>();

    let createCount = 0;
    vi.mocked(GlideClient.createClient).mockImplementation(async () => {
      createCount++;
      if (createCount === 1) return initCommand as any;
      if (createCount === 2) return initBlocking as any;
      if (createCount === 3) return reconnectCommand as any;
      if (createCount === 4) return pendingBlocking.promise;
      return makeMockClient() as any;
    });

    const worker = new Worker('lifecycle-w1b', vi.fn(), { connection, blockTimeout: 100 });
    worker.on('error', () => {});
    await worker.waitUntilReady();
    await vi.advanceTimersByTimeAsync(5000);
    expect(createCount).toBe(4);

    await worker.close(true);
    pendingBlocking.resolve(reconnectBlocking);
    await vi.advanceTimersByTimeAsync(5000);

    expect(reconnectBlocking.close).toHaveBeenCalledTimes(1);
    expect(reconnectCommand.close).toHaveBeenCalledTimes(1);
    expect((worker as any).scheduler).toBeNull();
    expect((worker as any).workerHeartbeatTimer).toBeNull();
    expect((worker as any).blockingClient).toBeNull();
    expect(reconnectBlocking.xreadgroup).not.toHaveBeenCalled();
  });

  it('QueueEvents disposes a client created by a reconnect that finishes after close()', async () => {
    const initClient = makeMockClient();
    initClient.xread.mockRejectedValueOnce(new Error('Connection lost'));
    const reconnectClient = makeMockClient();
    const pendingCreate = deferred<any>();

    let createCount = 0;
    vi.mocked(GlideClient.createClient).mockImplementation(async () => {
      createCount++;
      if (createCount === 1) return initClient as any;
      if (createCount === 2) return pendingCreate.promise;
      return makeMockClient() as any;
    });

    const qe = new QueueEvents('lifecycle-w1-qe', { connection, blockTimeout: 100 });
    qe.on('error', () => {});
    await qe.waitUntilReady();
    await vi.advanceTimersByTimeAsync(5000);
    expect(createCount).toBe(2);

    await qe.close();
    pendingCreate.resolve(reconnectClient);
    await vi.advanceTimersByTimeAsync(5000);

    expect(reconnectClient.close).toHaveBeenCalledTimes(1);
    expect(reconnectClient.xread).not.toHaveBeenCalled();
    expect((qe as any).client).toBeNull();
  });

  it('QueueEvents close() clears a pending reconnect timer', async () => {
    const initClient = makeMockClient();
    initClient.xread.mockRejectedValueOnce(new Error('Connection lost'));
    vi.mocked(GlideClient.createClient).mockResolvedValue(initClient as any);

    const qe = new QueueEvents('lifecycle-w1-qe-timer', { connection, blockTimeout: 100 });
    qe.on('error', () => {});
    await qe.waitUntilReady();
    await vi.advanceTimersByTimeAsync(10);
    expect((qe as any).reconnectTimer).not.toBeNull();

    await qe.close();
    expect((qe as any).reconnectTimer).toBeNull();
  });
});
