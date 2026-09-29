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
    smembers: vi.fn().mockResolvedValue(new Set()),
    close: vi.fn(),
    ...overrides,
  };
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
    const command = makeMockClient({
      fcall: routeFcall({
        glidemq_moveToActive: (keys) => {
          const id = jobIdFromKeys(keys);
          if (id === 'j2') throw new Error('activation failed');
          return jobHash(id);
        },
      }),
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
