/**
 * Backlog round 4 unit tests (2026-09-29): rolling-upgrade fallbacks and
 * round-trip counts on mocked clients. No Valkey needed.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { GlideClient } from '@glidemq/speedkey';
import { Queue } from '../src/queue';
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
