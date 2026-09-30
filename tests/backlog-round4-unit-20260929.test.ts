/**
 * Backlog round 4 unit tests (2026-09-29): rolling-upgrade fallbacks and
 * round-trip counts on mocked clients. No Valkey needed.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { GlideClient } from '@glidemq/speedkey';
import { Queue } from '../src/queue';
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
  };
});

function makeMockClient(overrides: Record<string, unknown> = {}) {
  const client: Record<string, any> = {
    fcall: vi.fn().mockResolvedValue(LIBRARY_VERSION),
    functionLoad: vi.fn(),
    hset: vi.fn().mockResolvedValue(1),
    hdel: vi.fn().mockResolvedValue(1),
    hget: vi.fn().mockResolvedValue(null),
    hgetall: vi.fn().mockResolvedValue([]),
    hmget: vi.fn().mockResolvedValue([null, null, null]),
    exists: vi.fn().mockResolvedValue(1),
    ping: vi.fn().mockResolvedValue('PONG'),
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

const connection = { addresses: [{ host: '127.0.0.1', port: 6379 }] };

function hashReply(fields: Record<string, string>) {
  return Object.entries(fields).map(([field, value]) => ({ field, value }));
}

describe('Queue.updateFlowBudget rolling upgrade', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('uses glidemq_updateFlowBudget when the library has it', async () => {
    const client = makeMockClient();
    client.fcall.mockImplementation((func: string) => {
      if (func === 'glidemq_updateFlowBudget') return Promise.resolve('ok');
      return Promise.resolve(LIBRARY_VERSION);
    });
    client.hgetall.mockResolvedValue(
      hashReply({ maxTotalCost: '5', usedTokens: '0', usedCost: '2', onExceeded: 'pause' }),
    );
    vi.mocked(GlideClient.createClient).mockResolvedValue(client as any);
    const queue = new Queue('r4-unit-budget', { connection });
    const state = await queue.updateFlowBudget('f1', { maxTotalCost: 5, maxTokens: null });
    expect(state).toMatchObject({ maxTotalCost: 5, usedCost: 2, exceeded: false, onExceeded: 'pause' });
    const call = client.fcall.mock.calls.find((c: unknown[]) => c[0] === 'glidemq_updateFlowBudget');
    expect(call).toEqual([
      'glidemq_updateFlowBudget',
      ['glide:{r4-unit-budget}:budget:f1'],
      ['maxTotalCost', '5', 'maxTokens', ''],
    ]);
    expect(client.hset).not.toHaveBeenCalled();
    await queue.close();
  });

  it('falls back to plain hash writes and re-evaluates exceeded on an older library', async () => {
    const client = makeMockClient();
    client.fcall.mockImplementation((func: string) => {
      if (func === 'glidemq_updateFlowBudget') return Promise.reject(new Error('ERR Function not found'));
      return Promise.resolve(LIBRARY_VERSION);
    });
    // usedCost 2 fits the raised cap of 5; the per-category input cap of 10 is over.
    client.hgetall.mockResolvedValue(
      hashReply({
        maxTotalCost: '5',
        maxTokens: '{"input":10}',
        usedTokens: '30',
        usedCost: '2',
        'usedTokens:input': '30',
        exceeded: '1',
        onExceeded: 'fail',
      }),
    );
    vi.mocked(GlideClient.createClient).mockResolvedValue(client as any);
    const queue = new Queue('r4-unit-budget-old', { connection });
    await queue.updateFlowBudget('f1', { maxTotalCost: 5, maxTotalTokens: null });
    expect(client.hset).toHaveBeenCalledWith('glide:{r4-unit-budget-old}:budget:f1', { maxTotalCost: '5' });
    expect(client.hdel).toHaveBeenCalledWith('glide:{r4-unit-budget-old}:budget:f1', ['maxTotalTokens']);
    // Still over the per-category cap: the flag stays set.
    expect(client.hset).toHaveBeenCalledWith('glide:{r4-unit-budget-old}:budget:f1', { exceeded: '1' });
    expect(client.hdel).not.toHaveBeenCalledWith('glide:{r4-unit-budget-old}:budget:f1', ['exceeded']);

    client.hgetall.mockResolvedValue(hashReply({ maxTotalCost: '5', usedTokens: '0', usedCost: '2', exceeded: '1' }));
    await queue.updateFlowBudget('f1', { maxTokens: null });
    expect(client.hdel).toHaveBeenCalledWith('glide:{r4-unit-budget-old}:budget:f1', ['exceeded']);
    await queue.close();
  });

  it('returns null for a flow without a budget and rejects invalid limits', async () => {
    const client = makeMockClient({ exists: vi.fn().mockResolvedValue(0) });
    client.fcall.mockImplementation((func: string) => {
      if (func === 'glidemq_updateFlowBudget') return Promise.resolve('no_budget');
      return Promise.resolve(LIBRARY_VERSION);
    });
    vi.mocked(GlideClient.createClient).mockResolvedValue(client as any);
    const queue = new Queue('r4-unit-budget-none', { connection });
    expect(await queue.updateFlowBudget('missing', { maxTotalCost: 1 })).toBeNull();
    await expect(queue.updateFlowBudget('f', { maxTotalTokens: Number.NaN })).rejects.toThrow(/finite number/);
    await expect(queue.updateFlowBudget('f', { maxCosts: { a: -1 } })).rejects.toThrow(/maxCosts.a/);
    await expect(queue.updateFlowBudget('f', { onExceeded: 'stop' as any })).rejects.toThrow(/onExceeded/);
    await queue.close();
  });
});

const neverResolve = () => new Promise(() => {});

function jobHashFields(id: string): string[] {
  return [
    'id',
    id,
    'name',
    'job',
    'data',
    '{}',
    'opts',
    '{"attempts":3}',
    'timestamp',
    '1000',
    'attemptsMade',
    '0',
    'state',
    'active',
  ];
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

/** fcall router with worker defaults: version, no global concurrency, empty lists, activation hash. */
function routeFcall(routes: Record<string, (keys: string[], args: string[]) => unknown>) {
  return vi.fn().mockImplementation((func: string, keys: string[] = [], args: string[] = []) => {
    if (routes[func]) return Promise.resolve().then(() => routes[func](keys, args));
    if (func === 'glidemq_checkConcurrency') return Promise.resolve(-1);
    if (func === 'glidemq_popLists') return Promise.resolve([]);
    if (func === 'glidemq_moveToActive') return Promise.resolve(JSON.stringify(jobHashFields(jobIdFromKeys(keys))));
    return Promise.resolve(LIBRARY_VERSION);
  });
}

function wireClients(command: ReturnType<typeof makeMockClient>, blocking: ReturnType<typeof makeMockClient>) {
  let n = 0;
  vi.mocked(GlideClient.createClient).mockImplementation(async () => {
    n++;
    return (n === 1 ? command : blocking) as any;
  });
}

function fcallNames(client: ReturnType<typeof makeMockClient>, name: string) {
  return client.fcall.mock.calls.filter((c: unknown[]) => c[0] === name);
}

async function until(pred: () => boolean, timeoutMs = 3000) {
  const deadline = Date.now() + timeoutMs;
  while (!pred()) {
    if (Date.now() > deadline) throw new Error('until timed out');
    await new Promise((r) => setTimeout(r, 5));
  }
}

describe('failAndFetchNext chain', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('fails and fetches the next job in one FCALL per failure, never falling back to a poll', async () => {
    const chain = ['1', '2', '3', '4'];
    const command = makeMockClient({
      fcall: routeFcall({
        glidemq_failAndFetchNext: (_keys, args) => {
          const idx = chain.indexOf(args[0]);
          const nextId = chain[idx + 1];
          if (!nextId) return ['failed', 'NEXT_NONE', args[0]];
          return ['retrying', 'NEXT_HASH', args[0], nextId, `${nextId}-0`, ...jobHashFields(nextId)];
        },
      }),
      xgroupCreate: vi.fn().mockResolvedValue('OK'),
    });
    const blocking = makeMockClient({
      xgroupCreate: vi.fn().mockResolvedValue('OK'),
      xreadgroup: vi
        .fn()
        .mockResolvedValueOnce(streamResult([['1-0', '1']]))
        .mockImplementation(neverResolve),
    });
    wireClients(command, blocking);

    const seen: string[] = [];
    const failed: [string, string][] = [];
    const worker = new Worker(
      'r4-unit-faf',
      async (job) => {
        seen.push(job.id);
        throw new Error(`boom ${job.id}`);
      },
      { connection, blockTimeout: 100 },
    );
    worker.on('error', () => {});
    worker.on('failed', (job, err) => failed.push([job.id, err.message]));
    await worker.waitUntilReady();
    await until(() => failed.length === 4);

    expect(seen).toEqual(chain);
    expect(failed).toEqual(chain.map((id) => [id, `boom ${id}`]));
    const fafCalls = fcallNames(command, 'glidemq_failAndFetchNext');
    expect(fafCalls).toHaveLength(4);
    // KEYS/ARGS: the glidemq_fail layout plus the consumer name.
    expect(fafCalls[0][1]).toEqual([
      'glide:{r4-unit-faf}:stream',
      'glide:{r4-unit-faf}:failed',
      'glide:{r4-unit-faf}:scheduled',
      'glide:{r4-unit-faf}:events',
      'glide:{r4-unit-faf}:job:1',
      'glide:{r4-unit-faf}:metrics:failed',
    ]);
    expect(fafCalls[0][2].slice(0, 3)).toEqual(['1', '1-0', 'boom 1']);
    expect(fafCalls[0][2][4]).toBe('3');
    expect(fafCalls[0][2].slice(10, 13)).toEqual(['0', '0', '0']);
    expect(fafCalls[0][2][13]).toBe((worker as any).consumerId);
    expect(fcallNames(command, 'glidemq_fail')).toHaveLength(0);
    expect(fcallNames(command, 'glidemq_completeAndFetchNext')).toHaveLength(0);
    // Jobs 2..4 came from the chain, not from a poll.
    expect(fcallNames(command, 'glidemq_moveToActive')).toHaveLength(1);
    expect(blocking.xreadgroup.mock.calls.length).toBeLessThanOrEqual(2);

    await worker.close(true);
  });

  it('falls back to glidemq_fail when the library lacks failAndFetchNext and stops retrying it', async () => {
    const command = makeMockClient({
      fcall: routeFcall({
        glidemq_failAndFetchNext: () => {
          throw new Error('ERR Function not found');
        },
        glidemq_fail: () => 'retrying',
      }),
      xgroupCreate: vi.fn().mockResolvedValue('OK'),
    });
    const blocking = makeMockClient({
      xgroupCreate: vi.fn().mockResolvedValue('OK'),
      xreadgroup: vi
        .fn()
        .mockResolvedValueOnce(streamResult([['1-0', '1']]))
        .mockResolvedValueOnce(streamResult([['2-0', '2']]))
        .mockImplementation(neverResolve),
    });
    wireClients(command, blocking);

    const failed: string[] = [];
    const worker = new Worker(
      'r4-unit-faf-old',
      async () => {
        throw new Error('boom');
      },
      { connection, blockTimeout: 100 },
    );
    worker.on('error', () => {});
    worker.on('failed', (job) => failed.push(job.id));
    await worker.waitUntilReady();
    await until(() => failed.length === 2);

    expect(failed).toEqual(['1', '2']);
    expect(fcallNames(command, 'glidemq_failAndFetchNext')).toHaveLength(1);
    const failCalls = fcallNames(command, 'glidemq_fail');
    expect(failCalls).toHaveLength(2);
    expect(failCalls[0][2].slice(0, 3)).toEqual(['1', '1-0', 'boom']);
    expect((worker as any).failAndFetchNextUnavailable).toBe(true);

    await worker.close(true);
  });
});
