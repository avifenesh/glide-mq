/**
 * Worker lifecycle unit tests: reconnect vs close races, heartbeat cleanup,
 * pause semantics and limiter waits. Mocked clients, no Valkey needed.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { GlideClient } from '@glidemq/speedkey';
import { Worker } from '../src/worker';
import { BroadcastWorker } from '../src/broadcast-worker';
import { QueueEvents } from '../src/queue-events';
import { LIBRARY_VERSION } from '../src/functions/index';

vi.mock('@glidemq/speedkey', () => {
  const MockGlideClient = { createClient: vi.fn() };
  // A class so isClusterClient's instanceof check works on mock clients.
  class MockGlideClusterClient {
    static createClient = vi.fn();
  }
  /** Records typed batch calls; the mock client's exec replays them. */
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
  };
});

const neverResolve = () => new Promise(() => {});

function makeMockClient(overrides: Record<string, unknown> = {}) {
  const client: Record<string, any> = {
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
    smembers: vi.fn().mockResolvedValue(new Set()),
    close: vi.fn(),
    ...overrides,
  };
  // One exec is one round trip. Each queued command runs through the client's
  // own mock; a rejection lands in its slot like raiseOnError=false.
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

function jobHash(id: string, extra: string[] = []) {
  return JSON.stringify([
    'id',
    id,
    'name',
    'job',
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
    ...extra,
  ]);
}

function streamResult(entries: [string, string][]) {
  const value: Record<string, [string, string][]> = {};
  for (const [entryId, jobId] of entries) value[entryId] = [['jobId', jobId]];
  return [{ key: 'stream', value }];
}

function jobIdFromKeys(keys?: string[]) {
  return String(keys?.[0] ?? '')
    .split(':')
    .pop()!;
}

/** fcall router: returns the per-function override, else the library version. */
function routeFcall(routes: Record<string, (keys: string[], args: string[]) => unknown>) {
  return vi.fn().mockImplementation((func: string, keys: string[] = [], args: string[] = []) => {
    if (routes[func]) return Promise.resolve().then(() => routes[func](keys, args));
    if (func === 'glidemq_checkConcurrency') return Promise.resolve(-1);
    if (func === 'glidemq_moveToActive') return Promise.resolve(jobHash(jobIdFromKeys(keys)));
    if (func === 'glidemq_completeAndFetchNext') {
      return Promise.resolve(JSON.stringify({ completed: args[0], next: false }));
    }
    if (func === 'glidemq_complete') return Promise.resolve(1);
    if (func === 'glidemq_fail') return Promise.resolve('retrying');
    return Promise.resolve(LIBRARY_VERSION);
  });
}

/** Wire createClient: first call is the command client, the rest are blocking clients. */
function wireClients(command: ReturnType<typeof makeMockClient>, blocking: ReturnType<typeof makeMockClient>) {
  let n = 0;
  vi.mocked(GlideClient.createClient).mockImplementation(async () => {
    n++;
    return (n === 1 ? command : blocking) as any;
  });
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

describe('poll error backoff', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('backs off before the first reconnect and grows the delay while polls keep failing', async () => {
    let createCount = 0;
    const createTimes: number[] = [];
    vi.mocked(GlideClient.createClient).mockImplementation(async () => {
      createCount++;
      createTimes.push(Date.now());
      // Every blocking client rejects its poll with a non-connection error.
      return makeMockClient({
        xreadgroup: vi.fn().mockRejectedValue(new Error('NOPERM this user has no permissions')),
      }) as any;
    });

    const worker = new Worker('lifecycle-w9', vi.fn(), { connection, blockTimeout: 100 });
    worker.on('error', () => {});
    await worker.waitUntilReady();
    const start = Date.now();

    await vi.advanceTimersByTimeAsync(500);
    // No reconnect before the first backoff elapses.
    expect(createCount).toBe(2);

    await vi.advanceTimersByTimeAsync(7000);
    // Reconnects at ~1s, ~3s (1+2), ~7s (1+2+4): 2 clients each.
    expect(createCount).toBeLessThanOrEqual(2 + 3 * 2);
    expect(createCount).toBeGreaterThanOrEqual(2 + 2 * 2);
    const reconnectStarts = createTimes.slice(2).filter((_, i) => i % 2 === 0);
    expect(reconnectStarts[0] - start).toBeGreaterThanOrEqual(900);
    expect(reconnectStarts[1] - reconnectStarts[0]).toBeGreaterThanOrEqual(1900);

    await worker.close(true);
  });
});

describe('heartbeat cleanup on pre-processor failures', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('stops the heartbeat and drops the abort controller when the rate limiter call throws', async () => {
    const command = makeMockClient({
      fcall: routeFcall({
        glidemq_rateLimit: () => {
          throw new Error('rate limiter unavailable');
        },
      }),
    });
    const blocking = makeMockClient({
      xreadgroup: vi
        .fn()
        .mockResolvedValueOnce(streamResult([['1-0', 'j1']]))
        .mockImplementation(neverResolve),
    });
    wireClients(command, blocking);

    const processor = vi.fn().mockResolvedValue('ok');
    const worker = new Worker('lifecycle-w2', processor, {
      connection,
      blockTimeout: 100,
      limiter: { max: 1, duration: 1000 },
    });
    worker.on('error', () => {});
    await worker.waitUntilReady();
    await vi.advanceTimersByTimeAsync(100);

    expect(processor).not.toHaveBeenCalled();
    expect((worker as any).heartbeatIntervals.size).toBe(0);
    expect((worker as any).activeAbortControllers.size).toBe(0);

    await worker.close(true);
  });

  it('stops heartbeats of already activated batch entries when a later activation throws', async () => {
    // j1 is activated and gets a heartbeat; j2's ordering check then throws.
    const command = makeMockClient({
      fcall: routeFcall({
        glidemq_moveToActive: (keys) => {
          const id = jobIdFromKeys(keys);
          return id === 'j2' ? jobHash(id, ['orderingKey', 'k', 'orderingSeq', '1']) : jobHash(id);
        },
      }),
      hget: vi.fn().mockRejectedValue(new Error('ordering check failed')),
    });
    const blocking = makeMockClient({
      xreadgroup: vi
        .fn()
        .mockResolvedValueOnce(
          streamResult([
            ['1-0', 'j1'],
            ['2-0', 'j2'],
          ]),
        )
        .mockImplementation(neverResolve),
    });
    wireClients(command, blocking);

    const processor = vi.fn().mockResolvedValue([]);
    const worker = new Worker('lifecycle-w2-batch', processor, {
      connection,
      blockTimeout: 100,
      batch: { size: 2 },
    });
    worker.on('error', () => {});
    await worker.waitUntilReady();
    await vi.advanceTimersByTimeAsync(100);

    expect(processor).not.toHaveBeenCalled();
    expect((worker as any).heartbeatIntervals.size).toBe(0);

    await worker.close(true);
  });

  it('startHeartbeat replaces an existing timer for the same job', async () => {
    wireClients(makeMockClient(), makeMockClient());
    const worker = new Worker('lifecycle-w2-dup', vi.fn(), { connection, blockTimeout: 100 });
    await worker.waitUntilReady();

    const clearSpy = vi.spyOn(globalThis, 'clearInterval');
    (worker as any).startHeartbeat('dup');
    const first = (worker as any).heartbeatIntervals.get('dup');
    (worker as any).startHeartbeat('dup');
    expect(clearSpy).toHaveBeenCalledWith(first);
    (worker as any).stopHeartbeat('dup');
    expect((worker as any).heartbeatIntervals.size).toBe(0);
    clearSpy.mockRestore();

    await worker.close(true);
  });
});

function nextHash(completed: string, nextId: string, nextEntryId: string) {
  return ['NEXT_HASH', completed, nextId, nextEntryId, ...JSON.parse(jobHash(nextId))];
}

describe('Worker.pause() under backlog', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('stops chaining completeAndFetchNext once paused', async () => {
    let cafCalls = 0;
    const command = makeMockClient({
      fcall: routeFcall({
        // Bounded backlog so the pre-fix behavior terminates.
        glidemq_completeAndFetchNext: (_keys, args) => {
          cafCalls++;
          if (cafCalls > 5) return JSON.stringify({ completed: args[0], next: false });
          return nextHash(args[0], `n${cafCalls}`, `${cafCalls + 1}-0`);
        },
      }),
    });
    const blocking = makeMockClient({
      xreadgroup: vi
        .fn()
        .mockResolvedValueOnce(streamResult([['1-0', 'j1']]))
        .mockImplementation(neverResolve),
    });
    wireClients(command, blocking);

    let pausePromise: Promise<void> | undefined;
    let pauseResolved = false;
    const worker = new Worker(
      'lifecycle-w3',
      async () => {
        if (!pausePromise) {
          pausePromise = worker.pause().then(() => {
            pauseResolved = true;
          });
        }
        return 'ok';
      },
      { connection, blockTimeout: 100 },
    );
    const processed: string[] = [];
    worker.on('completed', (job: any) => processed.push(job.id));
    await worker.waitUntilReady();
    await vi.advanceTimersByTimeAsync(200);

    expect(processed).toEqual(['j1']);
    expect(cafCalls).toBe(0);
    const completeCalls = command.fcall.mock.calls.filter((c: any[]) => c[0] === 'glidemq_complete');
    expect(completeCalls).toHaveLength(1);
    expect(pauseResolved).toBe(true);

    await worker.close(true);
  });

  it('defers the next job fetched by a completeAndFetchNext that raced pause()', async () => {
    const holder: { worker?: Worker } = {};
    const command = makeMockClient({
      fcall: routeFcall({
        glidemq_completeAndFetchNext: (_keys, args) => {
          void holder.worker!.pause(true);
          return nextHash(args[0], 'raced', '2-0');
        },
      }),
    });
    const blocking = makeMockClient({
      xreadgroup: vi
        .fn()
        .mockResolvedValueOnce(streamResult([['1-0', 'j1']]))
        .mockImplementation(neverResolve),
    });
    wireClients(command, blocking);

    const processed: string[] = [];
    const worker = new Worker(
      'lifecycle-w3-caf',
      async (job) => {
        processed.push(job.id!);
        return 'ok';
      },
      { connection, blockTimeout: 100 },
    );
    holder.worker = worker;
    await worker.waitUntilReady();
    await vi.advanceTimersByTimeAsync(100);

    expect(processed).toEqual(['j1']);
    const deferCalls = command.fcall.mock.calls.filter((c: any[]) => c[0] === 'glidemq_deferActive');
    expect(deferCalls).toHaveLength(1);
    // jobId, entryId, group, broadcast, pausedRestore, undoGroupClaim
    expect(deferCalls[0][2]).toEqual(['raced', '2-0', 'workers', '0', '1', '1']);

    await worker.close(true);
  });

  it('hands back entries delivered by an XREADGROUP in flight when pause() landed', async () => {
    const read = deferred<any>();
    const command = makeMockClient({ fcall: routeFcall({}) });
    const blocking = makeMockClient({
      xreadgroup: vi
        .fn()
        .mockImplementationOnce(() => read.promise)
        .mockImplementation(neverResolve),
    });
    wireClients(command, blocking);

    const processor = vi.fn().mockResolvedValue('ok');
    const worker = new Worker('lifecycle-w3-inflight', processor, { connection, blockTimeout: 100 });
    await worker.waitUntilReady();
    await vi.advanceTimersByTimeAsync(10);

    await worker.pause();
    read.resolve(streamResult([['1-0', 'late']]));
    await vi.advanceTimersByTimeAsync(50);

    expect(processor).not.toHaveBeenCalled();
    const moveCalls = command.fcall.mock.calls.filter((c: any[]) => c[0] === 'glidemq_moveToActive');
    expect(moveCalls).toHaveLength(0);
    const deferCalls = command.fcall.mock.calls.filter((c: any[]) => c[0] === 'glidemq_deferActive');
    expect(deferCalls).toHaveLength(1);
    expect(deferCalls[0][2][0]).toBe('late');
    expect(deferCalls[0][2][1]).toBe('1-0');

    await worker.close(true);
  });
});

describe('batch fetch count', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('BroadcastWorker caps the XREADGROUP count at batch.size', async () => {
    const blocking = makeMockClient();
    wireClients(makeMockClient(), blocking);

    const worker = new BroadcastWorker('lifecycle-w4', vi.fn().mockResolvedValue([]), {
      connection,
      subscription: 'sub-w4',
      concurrency: 3,
      batch: { size: 2 },
      blockTimeout: 100,
    });
    await worker.waitUntilReady();
    await vi.advanceTimersByTimeAsync(10);

    expect(blocking.xreadgroup).toHaveBeenCalled();
    expect(blocking.xreadgroup.mock.calls[0][3]).toEqual({ count: 2, block: 100 });

    await worker.close(true);
  });

  it.each([
    ['Worker', (opts: any) => new Worker('lifecycle-w4-gc', vi.fn().mockResolvedValue([]), opts)],
    [
      'BroadcastWorker',
      (opts: any) =>
        new BroadcastWorker('lifecycle-w4-gc-b', vi.fn().mockResolvedValue([]), { ...opts, subscription: 'sub-w4' }),
    ],
  ])('%s keeps the batch cap when global concurrency has more room', async (_name, make) => {
    const command = makeMockClient({
      fcall: routeFcall({ glidemq_checkConcurrency: () => 8 }),
      hmget: vi.fn().mockResolvedValue(['10', null, null, null]),
    });
    const blocking = makeMockClient();
    wireClients(command, blocking);

    const worker = make({ connection, concurrency: 3, batch: { size: 4 }, blockTimeout: 100 });
    await worker.waitUntilReady();
    await vi.advanceTimersByTimeAsync(10);

    expect(blocking.xreadgroup).toHaveBeenCalled();
    expect(blocking.xreadgroup.mock.calls[0][3]).toEqual({ count: 4, block: 100 });

    await worker.close(true);
  });
});

describe('QueueEvents init failure', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('routes a connection failure to error listeners instead of an unhandled rejection', async () => {
    vi.mocked(GlideClient.createClient).mockRejectedValue(new Error('ECONNREFUSED'));
    const unhandled: unknown[] = [];
    const onUnhandled = (reason: unknown) => unhandled.push(reason);
    process.on('unhandledRejection', onUnhandled);
    try {
      const qe = new QueueEvents('lifecycle-w6', { connection });
      const errors: Error[] = [];
      qe.on('error', (err: Error) => errors.push(err));
      // Never awaits waitUntilReady().
      await new Promise((resolve) => setTimeout(resolve, 20));

      expect(errors).toHaveLength(1);
      expect(errors[0].message).toContain('ECONNREFUSED');
      expect(unhandled).toEqual([]);
      await qe.close();
    } finally {
      process.off('unhandledRejection', onUnhandled);
    }
  });
});

describe('QueueEvents throwing listener', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  function eventsClient() {
    return makeMockClient({
      xread: vi
        .fn()
        .mockResolvedValueOnce([
          {
            key: 'events',
            value: {
              '1-0': [
                ['event', 'completed'],
                ['jobId', 'a'],
              ],
              '2-0': [
                ['event', 'completed'],
                ['jobId', 'b'],
              ],
            },
          },
        ])
        .mockImplementation(neverResolve),
    });
  }

  it('advances past an event whose listener throws and routes the error', async () => {
    const client = eventsClient();
    vi.mocked(GlideClient.createClient).mockResolvedValue(client as any);

    const qe = new QueueEvents('lifecycle-w7', { connection, blockTimeout: 100 });
    const seen: string[] = [];
    const errors: Error[] = [];
    qe.on('completed', (payload: any) => {
      seen.push(payload.jobId);
      if (payload.jobId === 'a') throw new Error('listener bug');
    });
    qe.on('error', (err: Error) => errors.push(err));
    await qe.waitUntilReady();
    await vi.advanceTimersByTimeAsync(50);

    expect(seen).toEqual(['a', 'b']);
    expect(errors.map((e) => e.message)).toEqual(['listener bug']);
    expect(client.xread).toHaveBeenCalledTimes(2);
    expect(Object.values(client.xread.mock.calls[1][0])).toEqual(['2-0']);

    await qe.close();
  });

  it('rethrows a listener error asynchronously when there is no error listener', async () => {
    const client = eventsClient();
    vi.mocked(GlideClient.createClient).mockResolvedValue(client as any);
    const ticks: (() => void)[] = [];
    const realNextTick = process.nextTick;
    const tickSpy = vi.spyOn(process, 'nextTick').mockImplementation(((fn: () => void, ...args: unknown[]) => {
      if (fn.toString().includes('throw err')) {
        ticks.push(fn);
        return;
      }
      return realNextTick(fn, ...args);
    }) as any);

    try {
      const qe = new QueueEvents('lifecycle-w7-noerr', { connection, blockTimeout: 100 });
      const seen: string[] = [];
      qe.on('completed', (payload: any) => {
        seen.push(payload.jobId);
        if (payload.jobId === 'a') throw new Error('listener bug');
      });
      await qe.waitUntilReady();
      await vi.advanceTimersByTimeAsync(50);

      expect(seen).toEqual(['a', 'b']);
      expect(ticks).toHaveLength(1);
      expect(() => ticks[0]()).toThrow('listener bug');
      await qe.close();
    } finally {
      tickSpy.mockRestore();
    }
  });
});

describe('prefetch above concurrency', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  function countedReads(total: number) {
    let next = 0;
    return vi.fn().mockImplementation((_g: string, _c: string, _s: unknown, opts: { count: number }) => {
      if (next >= total) return neverResolve();
      const entries: [string, string][] = [];
      for (let i = 0; i < opts.count && next < total; i++, next++) entries.push([`${next + 1}-0`, `j${next}`]);
      return Promise.resolve(streamResult(entries));
    });
  }

  it.each([1, 3])('never runs more than concurrency=%i jobs or claims beyond it', async (concurrency) => {
    const blocking = makeMockClient({ xreadgroup: countedReads(20) });
    wireClients(makeMockClient({ fcall: routeFcall({}) }), blocking);

    let running = 0;
    let maxRunning = 0;
    const release: (() => void)[] = [];
    const worker = new Worker(
      'lifecycle-w8',
      () =>
        new Promise<string>((resolve) => {
          running++;
          maxRunning = Math.max(maxRunning, running);
          release.push(() => {
            running--;
            resolve('ok');
          });
        }),
      { connection, concurrency, prefetch: 10, blockTimeout: 100 },
    );
    await worker.waitUntilReady();
    await vi.advanceTimersByTimeAsync(50);

    expect(maxRunning).toBe(concurrency);
    for (const call of blocking.xreadgroup.mock.calls) expect(call[3].count).toBeLessThanOrEqual(concurrency);

    for (let i = 0; i < 20 && release.length > 0; i++) {
      release.splice(0).forEach((r) => r());
      await vi.advanceTimersByTimeAsync(20);
    }
    expect(maxRunning).toBe(concurrency);

    await worker.close(true);
  });
});

describe('suspend continuations', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('evicts the oldest continuation beyond the cap', async () => {
    wireClients(makeMockClient(), makeMockClient());
    const worker = new Worker('lifecycle-w10', vi.fn(), { connection, blockTimeout: 100 });
    await worker.waitUntilReady();

    const max = (Worker as any).MAX_SUSPEND_CONTINUATIONS as number;
    const onResume = async () => 'x';
    for (let i = 0; i < max + 5; i++) {
      (worker as any).setSuspendContinuation(`s${i}`, { job: {}, onResume });
    }
    const map = (worker as any).suspendContinuations as Map<string, unknown>;
    expect(map.size).toBe(max);
    expect(map.has('s0')).toBe(false);
    expect(map.has('s4')).toBe(false);
    expect(map.has('s5')).toBe(true);
    expect(map.has(`s${max + 4}`)).toBe(true);

    await worker.close(true);
    expect(map.size).toBe(0);
  });

  it('drops the continuation when the suspend call fails', async () => {
    const command = makeMockClient({
      fcall: routeFcall({ glidemq_suspend: () => 'error:not_active' }),
    });
    const blocking = makeMockClient({
      xreadgroup: vi
        .fn()
        .mockResolvedValueOnce(streamResult([['1-0', 'susp']]))
        .mockImplementation(neverResolve),
    });
    wireClients(command, blocking);

    const worker = new Worker(
      'lifecycle-w10-fail',
      async (job) => {
        await job.suspend({ onResume: async () => 'resumed' });
      },
      { connection, blockTimeout: 100 },
    );
    worker.on('error', () => {});
    await worker.waitUntilReady();
    await vi.advanceTimersByTimeAsync(50);

    expect(command.fcall.mock.calls.some((c: any[]) => c[0] === 'glidemq_suspend')).toBe(true);
    expect((worker as any).suspendContinuations.size).toBe(0);

    await worker.close(true);
  });
});

describe('limiter waits during close()', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  async function closeDuringWait(opts: Record<string, unknown>, routes: Parameters<typeof routeFcall>[0]) {
    const command = makeMockClient({ fcall: routeFcall(routes) });
    const blocking = makeMockClient({
      xreadgroup: vi
        .fn()
        .mockResolvedValueOnce(streamResult([['1-0', 'limited']]))
        .mockImplementation(neverResolve),
    });
    wireClients(command, blocking);

    const processor = vi.fn().mockResolvedValue(opts.batch ? ['ok'] : 'ok');
    const worker = new Worker('lifecycle-w11-limiter', processor, { connection, blockTimeout: 100, ...opts });
    await worker.waitUntilReady();
    await vi.advanceTimersByTimeAsync(100);

    let closed = false;
    const closing = worker.close().then(() => {
      closed = true;
    });
    await vi.advanceTimersByTimeAsync(500);
    expect(closed).toBe(true);
    await closing;

    expect(processor).not.toHaveBeenCalled();
    const deferCalls = command.fcall.mock.calls.filter((c: any[]) => c[0] === 'glidemq_deferActive');
    expect(deferCalls).toHaveLength(1);
    expect(deferCalls[0][2]).toEqual(['limited', '1-0', 'workers', '0', '1', '1']);
  }

  it('wakes a rate limiter sleep and hands the job back', async () => {
    await closeDuringWait({ limiter: { max: 1, duration: 60000 } }, { glidemq_rateLimit: () => 60000 });
  });

  it('wakes a batch rate limiter sleep and hands the batch back', async () => {
    await closeDuringWait(
      { limiter: { max: 1, duration: 60000 }, batch: { size: 1 } },
      { glidemq_rateLimit: () => 60000 },
    );
  });

  it('wakes a token limiter sleep and hands the job back', async () => {
    const realNow = Date.now;
    // Local counter already at the cap for a window that just started. Anchor
    // the window at now: a minute-aligned start could expire mid-test near a
    // minute boundary and let the job run instead of sleeping.
    const spy = vi.spyOn(Worker.prototype as any, 'waitForTokenLimit');
    spy.mockImplementationOnce(async function (this: any) {
      this.tpmLocalCounter = 10;
      this.tpmWindowStart = realNow();
      spy.mockRestore();
      return this.waitForTokenLimit();
    });
    await closeDuringWait({ tokenLimiter: { maxTokens: 10, duration: 60000, scope: 'worker' } }, {});
  });
});

describe('close(true) aborts running processors', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it.each([false, true])('signals job.abortSignal without failing the job (batch=%s)', async (batch) => {
    const command = makeMockClient({ fcall: routeFcall({}) });
    const blocking = makeMockClient({
      xreadgroup: vi
        .fn()
        .mockResolvedValueOnce(streamResult([['1-0', 'running']]))
        .mockImplementation(neverResolve),
    });
    wireClients(command, blocking);

    let signal: AbortSignal | undefined;
    const started = deferred<void>();
    const waitForAbort = (s: AbortSignal) =>
      new Promise<never>((_, reject) => s.addEventListener('abort', () => reject(new Error('aborted by close'))));
    const processor = batch
      ? async (jobs: any[]) => {
          signal = jobs[0].abortSignal;
          started.resolve();
          return waitForAbort(signal!);
        }
      : async (job: any) => {
          signal = job.abortSignal;
          started.resolve();
          return waitForAbort(signal!);
        };
    const worker = new Worker('lifecycle-w11-force', processor as any, {
      connection,
      blockTimeout: 100,
      ...(batch ? { batch: { size: 1 } } : {}),
    });
    const failed: string[] = [];
    worker.on('failed', (job: any) => failed.push(job.id));
    worker.on('error', () => {});
    await worker.waitUntilReady();
    await started.promise;

    await worker.close(true);
    await vi.advanceTimersByTimeAsync(50);

    expect(signal!.aborted).toBe(true);
    expect(failed).toEqual([]);
    expect(command.fcall.mock.calls.filter((c: any[]) => c[0] === 'glidemq_fail')).toHaveLength(0);
  });

  it('escalates a graceful close that is waiting for a running job', async () => {
    const command = makeMockClient({ fcall: routeFcall({}) });
    const blocking = makeMockClient({
      xreadgroup: vi
        .fn()
        .mockResolvedValueOnce(streamResult([['1-0', 'slow']]))
        .mockImplementation(neverResolve),
    });
    wireClients(command, blocking);

    const started = deferred<void>();
    const worker = new Worker(
      'lifecycle-w11-escalate',
      (job: any) =>
        new Promise((_, reject) => {
          started.resolve();
          job.abortSignal.addEventListener('abort', () => reject(new Error('aborted by close')));
        }),
      { connection, blockTimeout: 100 },
    );
    worker.on('error', () => {});
    await worker.waitUntilReady();
    await started.promise;

    let gracefulDone = false;
    const graceful = worker.close().then(() => {
      gracefulDone = true;
    });
    await vi.advanceTimersByTimeAsync(50);
    expect(gracefulDone).toBe(false);

    await worker.close(true);
    await graceful;
    expect(gracefulDone).toBe(true);
    expect(command.fcall.mock.calls.filter((c: any[]) => c[0] === 'glidemq_fail')).toHaveLength(0);
  });
});

describe('batch return value size guard', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  async function runBatchOnce(result: unknown) {
    const command = makeMockClient({ fcall: routeFcall({}) });
    const blocking = makeMockClient({
      xreadgroup: vi
        .fn()
        .mockResolvedValueOnce(streamResult([['1-0', 'sized']]))
        .mockImplementation(neverResolve),
    });
    wireClients(command, blocking);
    const done = deferred<void>();
    const worker = new Worker(
      'lifecycle-size-guard',
      async () => {
        setTimeout(() => done.resolve(), 0);
        return [result];
      },
      { connection, blockTimeout: 100, batch: { size: 1 } },
    );
    worker.on('error', () => {});
    await worker.waitUntilReady();
    await done.promise;
    await vi.advanceTimersByTimeAsync(20);
    await worker.close(true);
    return command.fcall.mock.calls.map((c: any[]) => c[0]);
  }

  it('skips Buffer.byteLength for small results and still completes', async () => {
    const spy = vi.spyOn(Buffer, 'byteLength');
    const funcs = await runBatchOnce('small');
    const measured = spy.mock.calls.some((c) => c[0] === JSON.stringify('small'));
    spy.mockRestore();
    expect(measured).toBe(false);
    expect(funcs).toContain('glidemq_complete');
  });

  it('still fails a result over the byte limit', async () => {
    // 600k two-byte chars: length passes MAX/4, bytes exceed MAX.
    const funcs = await runBatchOnce('é'.repeat(600_000));
    expect(funcs).toContain('glidemq_fail');
    expect(funcs).not.toContain('glidemq_complete');
  });
});

describe('job heartbeat round trips', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('refreshes lastActive and checks revocation in one pipeline per tick', async () => {
    const command = makeMockClient();
    wireClients(command, makeMockClient());
    const worker = new Worker('lifecycle-hb-rtt', vi.fn(), { connection, blockTimeout: 100 });
    await worker.waitUntilReady();

    const ac = new AbortController();
    (worker as any).activeAbortControllers.set('hb', ac);
    (worker as any).startHeartbeat('hb', 2000);
    await vi.advanceTimersByTimeAsync(1000);

    expect(command.exec).toHaveBeenCalledTimes(1);
    const batch = command.exec.mock.calls[0][0];
    expect(batch.isAtomic).toBe(false);
    expect(batch.commands.map((c: [string, unknown[]]) => c[0])).toEqual(['hset', 'hget']);
    expect(batch.commands[0][1][0]).toMatch(/job:hb$/);
    expect(batch.commands[1][1]).toEqual([batch.commands[0][1][0], 'revoked']);
    expect(ac.signal.aborted).toBe(false);

    command.hget.mockResolvedValue('1');
    await vi.advanceTimersByTimeAsync(1000);
    expect(command.exec).toHaveBeenCalledTimes(2);
    expect(ac.signal.aborted).toBe(true);

    (worker as any).stopHeartbeat('hb');
    await worker.close(true);
  });
});

describe('batch activation and completion round trips', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  function batchRead(ids: string[]) {
    return makeMockClient({
      xreadgroup: vi
        .fn()
        .mockResolvedValueOnce(streamResult(ids.map((id, i) => [`${i + 1}-0`, id] as [string, string])))
        .mockImplementation(neverResolve),
    });
  }

  function fcallsIn(batch: { commands: [string, unknown[]][] }) {
    return batch.commands.map(([cmd, args]) => {
      expect(cmd).toBe('fcall');
      return [args[0], jobIdFromKeys((args[1] as string[]).slice(args[0] === 'glidemq_complete' ? 3 : 0))];
    });
  }

  it('activates and completes a batch of 3 in one pipeline each, in entry order', async () => {
    const command = makeMockClient({ fcall: routeFcall({}) });
    wireClients(command, batchRead(['a', 'b', 'c']));
    const completed: string[] = [];
    const processor = vi.fn(async (jobs: any[]) => jobs.map((j) => `r-${j.id}`));
    const worker = new Worker('lifecycle-batch-rtt', processor, { connection, blockTimeout: 100, batch: { size: 3 } });
    worker.on('completed', (job: any) => completed.push(job.id));
    await worker.waitUntilReady();
    await vi.advanceTimersByTimeAsync(50);

    expect(processor).toHaveBeenCalledTimes(1);
    expect(processor.mock.calls[0][0].map((j: any) => j.id)).toEqual(['a', 'b', 'c']);
    // 2 round trips for the batch instead of 6 sequential FCALLs.
    expect(command.exec).toHaveBeenCalledTimes(2);
    const [activation, completion] = command.exec.mock.calls.map((c: any[]) => c[0]);
    expect(activation.isAtomic).toBe(false);
    expect(completion.isAtomic).toBe(false);
    expect(fcallsIn(activation)).toEqual([
      ['glidemq_moveToActive', 'a'],
      ['glidemq_moveToActive', 'b'],
      ['glidemq_moveToActive', 'c'],
    ]);
    expect(fcallsIn(completion)).toEqual([
      ['glidemq_complete', 'a'],
      ['glidemq_complete', 'b'],
      ['glidemq_complete', 'c'],
    ]);
    expect(completion.commands.map((c: any) => c[1][2][2])).toEqual(['"r-a"', '"r-b"', '"r-c"']);
    expect(completed).toEqual(['a', 'b', 'c']);

    await worker.close(true);
  });

  it('runs the other entries when one pipelined activation errors', async () => {
    const command = makeMockClient({
      fcall: routeFcall({
        glidemq_moveToActive: (keys) => {
          const id = jobIdFromKeys(keys);
          if (id === 'b') throw new Error('activation failed');
          return jobHash(id);
        },
      }),
    });
    wireClients(command, batchRead(['a', 'b', 'c']));
    const errors: Error[] = [];
    const processor = vi.fn(async (jobs: any[]) => jobs.map(() => 'ok'));
    const worker = new Worker('lifecycle-batch-partial', processor, {
      connection,
      blockTimeout: 100,
      batch: { size: 3 },
    });
    worker.on('error', (err) => errors.push(err));
    await worker.waitUntilReady();
    await vi.advanceTimersByTimeAsync(50);

    expect(processor.mock.calls[0][0].map((j: any) => j.id)).toEqual(['a', 'c']);
    expect(errors.map((e) => e.message)).toContain('activation failed');
    expect((worker as any).heartbeatIntervals.size).toBe(0);

    await worker.close(true);
  });

  it('runs nothing and leaks no heartbeat when the activation pipeline rejects', async () => {
    const command = makeMockClient({ fcall: routeFcall({}) });
    command.exec = vi.fn().mockRejectedValueOnce(new Error('connection lost'));
    wireClients(command, batchRead(['a', 'b']));
    const processor = vi.fn(async (jobs: any[]) => jobs.map(() => 'ok'));
    const worker = new Worker('lifecycle-batch-reject', processor, {
      connection,
      blockTimeout: 100,
      batch: { size: 2 },
    });
    worker.on('error', () => {});
    await worker.waitUntilReady();
    await vi.advanceTimersByTimeAsync(50);

    expect(processor).not.toHaveBeenCalled();
    expect((worker as any).heartbeatIntervals.size).toBe(0);

    await worker.close(true);
  });

  it('fails a revoked completion and completes the rest of the pipeline', async () => {
    const command = makeMockClient({
      fcall: routeFcall({
        glidemq_complete: (_keys, args) => (args[0] === 'b' ? 'REVOKED' : 1),
      }),
    });
    wireClients(command, batchRead(['a', 'b', 'c']));
    const completed: string[] = [];
    const processor = vi.fn(async (jobs: any[]) => jobs.map(() => 'ok'));
    const worker = new Worker('lifecycle-batch-revoked', processor, {
      connection,
      blockTimeout: 100,
      batch: { size: 3 },
    });
    worker.on('completed', (job: any) => completed.push(job.id));
    worker.on('error', () => {});
    await worker.waitUntilReady();
    await vi.advanceTimersByTimeAsync(50);

    expect(completed).toEqual(['a', 'c']);
    const fails = command.fcall.mock.calls.filter((c: any[]) => c[0] === 'glidemq_fail');
    expect(fails).toHaveLength(1);
    expect(fails[0][2][0]).toBe('b');

    await worker.close(true);
  });
});

describe('batch refill during close', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it.each([
    ['Worker', (opts: any, p: any) => new Worker('lifecycle-refill-close', p, opts)],
    [
      'BroadcastWorker',
      (opts: any, p: any) => new BroadcastWorker('lifecycle-refill-close-b', p, { ...opts, subscription: 'sub-rc' }),
    ],
  ])('%s swallows a refill read torn down by close(true) and hands collected entries back', async (_name, make) => {
    const command = makeMockClient({ fcall: routeFcall({}) });
    const refill = deferred<any>();
    const blocking = makeMockClient({
      xreadgroup: vi
        .fn()
        .mockResolvedValueOnce(streamResult([['1-0', 'first']]))
        .mockImplementationOnce(() => refill.promise)
        .mockImplementation(neverResolve),
    });
    wireClients(command, blocking);

    const processor = vi.fn().mockResolvedValue(['ok']);
    const errors: unknown[] = [];
    const worker = make({ connection, batch: { size: 3, timeout: 5000 }, blockTimeout: 100 }, processor);
    worker.on('error', (err: unknown) => errors.push(err));
    await worker.waitUntilReady();
    await vi.advanceTimersByTimeAsync(20);
    expect(blocking.xreadgroup).toHaveBeenCalledTimes(2);

    const closing = worker.close(true);
    refill.reject(new Error('client closed'));
    await vi.advanceTimersByTimeAsync(20);
    await closing;

    expect(errors).toEqual([]);
    expect(processor).not.toHaveBeenCalled();
    expect(blocking.xreadgroup).toHaveBeenCalledTimes(2);
  });
});

describe('eager cross-queue parent notifications', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('keeps the xq-pending entry unless the parent took the completion or is gone', async () => {
    const results: Record<string, () => unknown> = {
      thrown: () => {
        throw new Error('connection lost');
      },
      counted: () => 0,
      gone: () => -1,
      odd: () => null,
    };
    const command = makeMockClient({
      fcall: routeFcall({ glidemq_completeChild: (_keys, args) => results[args[1]]() }),
      srem: vi.fn().mockResolvedValue(1),
    });
    wireClients(command, makeMockClient());
    const worker = new Worker('lifecycle-xq', vi.fn(), { connection, blockTimeout: 100 });
    const errors: Error[] = [];
    worker.on('error', (err) => errors.push(err));
    await worker.waitUntilReady();

    const member = (parentId: string) => JSON.stringify(['parent-q', parentId, `glide:{lifecycle-xq}:${parentId}c`]);
    await expect(
      (worker as any).notifyCrossQueueParents([member('thrown'), member('counted'), member('gone'), member('odd')]),
    ).resolves.toBeUndefined();

    const removed = command.srem.mock.calls.flatMap((c: any[]) => c[1]);
    expect(removed).toEqual([member('counted'), member('gone')]);
    expect(errors.map((e) => e.message)).toEqual(['connection lost']);

    await worker.close(true);
  });
});
