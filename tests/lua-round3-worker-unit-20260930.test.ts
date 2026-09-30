/**
 * Worker unit tests (mocked clients, no Valkey) for the round 3 review fixes:
 * a GLOBAL_FULL stream claim is held in the PEL and re-checked, bounded.
 *
 * Run: npx vitest run tests/lua-round3-worker-unit-20260930.test.ts
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { GlideClient } from '@glidemq/speedkey';
import { Worker } from '../src/worker';
import { LIBRARY_VERSION } from '../src/functions/index';

vi.mock('@glidemq/speedkey', () => {
  const MockGlideClient = { createClient: vi.fn() };
  class MockGlideClusterClient {
    static createClient = vi.fn();
  }
  class MockBatch {
    commands: [string, unknown[]][] = [];
    constructor(readonly isAtomic: boolean) {
      return new Proxy(this, {
        get(target, prop, receiver) {
          if (prop in target) return Reflect.get(target, prop, receiver);
          return (...args: unknown[]) => {
            target.commands.push([String(prop), args]);
            return receiver;
          };
        },
      });
    }
  }
  return {
    GlideClient: MockGlideClient,
    GlideClusterClient: MockGlideClusterClient,
    Batch: MockBatch,
    ClusterBatch: MockBatch,
    TimeUnit: { Milliseconds: 'PX' },
    ExpireOptions: { HasNoExpiry: 'NX' },
  };
});

const neverResolve = () => new Promise(() => {});

function makeMockClient(overrides: Record<string, unknown> = {}) {
  const client: Record<string, any> = {
    fcall: vi.fn().mockResolvedValue(LIBRARY_VERSION),
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
    smembers: vi.fn().mockResolvedValue(new Set()),
    close: vi.fn(),
    ...overrides,
  };
  client.exec ??= vi.fn(async (batch: { commands: [string, unknown[]][] }) => {
    const out: unknown[] = [];
    for (const [cmd, args] of batch.commands) {
      try {
        out.push(await client[cmd](...args));
      } catch (err) {
        out.push(err);
      }
    }
    return out;
  });
  return client;
}

function jobHash(id: string) {
  return JSON.stringify(['id', id, 'name', 'job', 'data', '{}', 'opts', '{}', 'timestamp', '1000', 'state', 'active']);
}

function streamResult(entries: [string, string][]) {
  const value: Record<string, [string, string][]> = {};
  for (const [entryId, jobId] of entries) value[entryId] = [['jobId', jobId]];
  return [{ key: 'stream', value }];
}

/** fcall router: moveToActive replies GLOBAL_FULL `fullReplies` times, then the job hash. */
function routeFcall(fullReplies: number) {
  let moveCalls = 0;
  const fn = vi.fn().mockImplementation((func: string, keys: string[] = [], args: string[] = []) => {
    if (func === 'glidemq_checkConcurrency') return Promise.resolve(-1);
    if (func === 'glidemq_moveToActive') {
      moveCalls++;
      if (moveCalls <= fullReplies) return Promise.resolve('GLOBAL_FULL');
      return Promise.resolve(jobHash(String(keys[0]).split(':').pop()!));
    }
    if (func === 'glidemq_completeAndFetchNext') {
      return Promise.resolve(JSON.stringify({ completed: args[0], next: false }));
    }
    if (func === 'glidemq_deferActive') return Promise.resolve(1);
    if (func === 'glidemq_removeIdleConsumer') return Promise.resolve(1);
    return Promise.resolve(LIBRARY_VERSION);
  });
  return { fn, moveCalls: () => moveCalls };
}

function wireClients(command: ReturnType<typeof makeMockClient>, blocking: ReturnType<typeof makeMockClient>) {
  let n = 0;
  vi.mocked(GlideClient.createClient).mockImplementation(async () => {
    n++;
    return (n === 1 ? command : blocking) as any;
  });
}

const connection = { addresses: [{ host: '127.0.0.1', port: 6379 }] };

describe('GLOBAL_FULL stream claims are held and re-checked', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  function setup(fullReplies: number) {
    const routes = routeFcall(fullReplies);
    const command = makeMockClient({ fcall: routes.fn });
    const blocking = makeMockClient({
      xreadgroup: vi
        .fn()
        .mockResolvedValueOnce(streamResult([['1-0', '7']]))
        .mockImplementation(neverResolve),
    });
    wireClients(command, blocking);
    const processor = vi.fn().mockResolvedValue('ok');
    const worker = new Worker('r3-gc-hold', processor, {
      connection,
      blockTimeout: 100,
      lockDuration: 2000,
      stalledInterval: 2000,
    });
    worker.on('error', () => {});
    const deferCalls = () => command.fcall.mock.calls.filter((c: any[]) => c[0] === 'glidemq_deferActive');
    return { worker, processor, routes, deferCalls };
  }

  it('keeps the claim in the PEL, retries moveToActive with backoff and runs the job once admitted', async () => {
    const { worker, processor, routes, deferCalls } = setup(3);
    await worker.waitUntilReady();
    await vi.advanceTimersByTimeAsync(2000);

    expect(processor).toHaveBeenCalledTimes(1);
    expect(routes.moveCalls()).toBe(4);
    expect(deferCalls()).toHaveLength(0);
    await worker.close();
  });

  it('hands the claim back once the bounded hold (min(lockDuration, stalledInterval) / 2) expires', async () => {
    const { worker, processor, routes, deferCalls } = setup(Number.MAX_SAFE_INTEGER);
    await worker.waitUntilReady();
    await vi.advanceTimersByTimeAsync(400);
    expect(deferCalls()).toHaveLength(0);
    await vi.advanceTimersByTimeAsync(1600);

    expect(processor).not.toHaveBeenCalled();
    expect(deferCalls()).toHaveLength(1);
    expect(deferCalls()[0][2]).toEqual(['7', '1-0', 'workers', '0', '0', '0']);
    // Backoff is capped at 250 ms: at most ~1000 ms / 250 ms + the first fast retries.
    expect(routes.moveCalls()).toBeGreaterThanOrEqual(5);
    expect(routes.moveCalls()).toBeLessThanOrEqual(12);
    await worker.close();
  });

  it('hands a held claim back when close() lands during the hold', async () => {
    const { worker, processor, deferCalls } = setup(Number.MAX_SAFE_INTEGER);
    await worker.waitUntilReady();
    await vi.advanceTimersByTimeAsync(150);
    const closing = worker.close();
    await vi.advanceTimersByTimeAsync(500);
    await closing;

    expect(processor).not.toHaveBeenCalled();
    expect(deferCalls()).toHaveLength(1);
    expect(deferCalls()[0][2]).toEqual(['7', '1-0', 'workers', '0', '1', '0']);
  });
});
