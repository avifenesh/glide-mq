/**
 * Tests for AI-specific job metadata (reportUsage / getFlowUsage).
 *
 * Run: npx vitest run tests/ai-metadata.test.ts
 */
import { describe, it, expect, vi, beforeEach, beforeAll, afterAll } from 'vitest';
import { Batch, ClusterBatch } from '@glidemq/speedkey';
import { Job } from '../src/job';
import { buildKeys, MAX_JOB_DATA_SIZE } from '../src/utils';
import { TestQueue, TestWorker } from '../src/testing';
import type { JobUsage } from '../src/types';

vi.mock('@glidemq/speedkey');

// ---- Unit tests (mocked client) ----

function makeMockClient(overrides: Record<string, unknown> = {}) {
  return {
    fcall: vi.fn(),
    hset: vi.fn().mockResolvedValue(1),
    hget: vi.fn().mockResolvedValue(null),
    hgetall: vi.fn().mockResolvedValue([]),
    hmget: vi.fn().mockResolvedValue([]),
    xadd: vi.fn().mockResolvedValue('1-0'),
    smembers: vi.fn().mockResolvedValue(new Set()),
    rpush: vi.fn().mockResolvedValue(1),
    close: vi.fn(),
    exec: vi.fn().mockResolvedValue([]),
    ...overrides,
  };
}

const keys = buildKeys('test-queue');

describe('Job.reportUsage (unit)', () => {
  let mockClient: ReturnType<typeof makeMockClient>;
  let mockBatch: any;

  beforeEach(() => {
    vi.clearAllMocks();
    mockClient = makeMockClient();
    mockBatch = {
      hset: vi.fn(),
      xadd: vi.fn(),
    };
    (Batch as unknown as ReturnType<typeof vi.fn>).mockReturnValue(mockBatch);
    (ClusterBatch as unknown as ReturnType<typeof vi.fn>).mockReturnValue(mockBatch);
  });

  it('persists usage to job hash via HSET', async () => {
    const job = new Job(mockClient as any, keys, '1', 'llm-call', {}, {});
    await job.reportUsage({ model: 'gpt-4o', tokens: { input: 100, output: 50 } });

    expect(mockBatch.hset).toHaveBeenCalledWith(
      keys.job('1'),
      expect.objectContaining({
        'usage:model': 'gpt-4o',
        'usage:tokens': JSON.stringify({ input: 100, output: 50 }),
        'usage:totalTokens': '150',
      }),
    );
  });

  it('auto-computes totalTokens when not provided', async () => {
    const job = new Job(mockClient as any, keys, '1', 'llm-call', {}, {});
    await job.reportUsage({ tokens: { input: 200, output: 80 } });

    expect(job.usage?.totalTokens).toBe(280);
    expect(mockBatch.hset).toHaveBeenCalledWith(
      keys.job('1'),
      expect.objectContaining({ 'usage:totalTokens': '280' }),
    );
  });

  it('preserves explicit totalTokens when provided', async () => {
    const job = new Job(mockClient as any, keys, '1', 'llm-call', {}, {});
    await job.reportUsage({ tokens: { input: 200, output: 80 }, totalTokens: 999 });

    expect(job.usage?.totalTokens).toBe(999);
  });

  it('stores all fields when fully populated', async () => {
    const job = new Job(mockClient as any, keys, '1', 'llm-call', {}, {});
    const usage: JobUsage = {
      model: 'claude-sonnet-4-20250514',
      provider: 'anthropic',
      tokens: { input: 500, output: 200 },
      totalTokens: 700,
      costs: { total: 0.0042 },
      totalCost: 0.0042,
      latencyMs: 1250,
      cached: false,
    };
    await job.reportUsage(usage);

    expect(mockBatch.hset).toHaveBeenCalledWith(
      keys.job('1'),
      expect.objectContaining({
        'usage:model': 'claude-sonnet-4-20250514',
        'usage:provider': 'anthropic',
        'usage:tokens': JSON.stringify({ input: 500, output: 200 }),
        'usage:totalTokens': '700',
        'usage:costs': JSON.stringify({ total: 0.0042 }),
        'usage:totalCost': '0.0042',
        'usage:latencyMs': '1250',
        'usage:cached': '0',
      }),
    );
  });

  it('accepts minimal fields (only model)', async () => {
    const job = new Job(mockClient as any, keys, '1', 'llm-call', {}, {});
    await job.reportUsage({ model: 'gpt-4o-mini' });

    expect(job.usage?.model).toBe('gpt-4o-mini');
    expect(job.usage?.tokens).toBeUndefined();
  });

  it('overwrites previous usage on repeated calls', async () => {
    const job = new Job(mockClient as any, keys, '1', 'llm-call', {}, {});
    await job.reportUsage({ model: 'gpt-4o', tokens: { input: 100 } });
    await job.reportUsage({ model: 'claude-sonnet-4-20250514', tokens: { input: 200 } });

    expect(job.usage?.model).toBe('claude-sonnet-4-20250514');
    expect(job.usage?.tokens?.input).toBe(200);
  });

  it('rejects negative value in tokens map', async () => {
    const job = new Job(mockClient as any, keys, '1', 'llm-call', {}, {});
    await expect(job.reportUsage({ tokens: { input: -1 } })).rejects.toThrow("Token count for 'input' must not be negative");
  });

  it('rejects negative output in tokens map', async () => {
    const job = new Job(mockClient as any, keys, '1', 'llm-call', {}, {});
    await expect(job.reportUsage({ tokens: { output: -5 } })).rejects.toThrow("Token count for 'output' must not be negative");
  });

  it('rejects negative totalTokens', async () => {
    const job = new Job(mockClient as any, keys, '1', 'llm-call', {}, {});
    await expect(job.reportUsage({ totalTokens: -10 })).rejects.toThrow('totalTokens must not be negative');
  });

  it('auto-computes totalTokens with reasoning tokens', async () => {
    const job = new Job(mockClient as any, keys, '1', 'llm-call', {}, {});
    await job.reportUsage({ tokens: { input: 100, output: 50, reasoning: 3000 } });

    expect(job.usage?.totalTokens).toBe(3150);
    expect(mockBatch.hset).toHaveBeenCalledWith(
      keys.job('1'),
      expect.objectContaining({ 'usage:totalTokens': '3150' }),
    );
  });

  it('auto-computes totalCost from costs breakdown', async () => {
    const job = new Job(mockClient as any, keys, '1', 'llm-call', {}, {});
    await job.reportUsage({ costs: { input: 0.01, output: 0.03, reasoning: 0.15 } });

    expect(job.usage?.totalCost).toBeCloseTo(0.19, 10);
    expect(mockBatch.hset).toHaveBeenCalledWith(
      keys.job('1'),
      expect.objectContaining({
        'usage:costs': JSON.stringify({ input: 0.01, output: 0.03, reasoning: 0.15 }),
      }),
    );
  });

  it('persists costUnit field', async () => {
    const job = new Job(mockClient as any, keys, '1', 'llm-call', {}, {});
    await job.reportUsage({ costs: { total: 0.05 }, costUnit: 'usd' });

    expect(job.usage?.costUnit).toBe('usd');
    expect(mockBatch.hset).toHaveBeenCalledWith(
      keys.job('1'),
      expect.objectContaining({
        'usage:costUnit': 'usd',
      }),
    );
  });

  it('rejects negative value for arbitrary key in tokens map', async () => {
    const job = new Job(mockClient as any, keys, '1', 'llm-call', {}, {});
    await expect(job.reportUsage({ tokens: { reasoning: -100 } })).rejects.toThrow("Token count for 'reasoning' must not be negative");
  });

  it('emits usage event to events stream', async () => {
    const job = new Job(mockClient as any, keys, '1', 'llm-call', {}, {});
    await job.reportUsage({ model: 'gpt-4o', tokens: { input: 100, output: 50 } });

    expect(mockBatch.xadd).toHaveBeenCalledWith(
      keys.events,
      expect.arrayContaining([
        ['event', 'usage'],
        ['jobId', '1'],
      ]),
    );
  });

  it('does not require entryId (callable from any context)', async () => {
    const job = new Job(mockClient as any, keys, '1', 'llm-call', {}, {});
    // No entryId set - should still work
    await expect(job.reportUsage({ model: 'gpt-4o' })).resolves.not.toThrow();
  });
});

describe('Job.fromHash usage parsing', () => {
  let mockClient: ReturnType<typeof makeMockClient>;

  beforeEach(() => {
    vi.clearAllMocks();
    mockClient = makeMockClient();
  });

  it('parses usage fields from hash', () => {
    const hash: Record<string, string> = {
      name: 'llm-call',
      data: '{}',
      opts: '{}',
      'usage:model': 'gpt-4o',
      'usage:provider': 'openai',
      'usage:tokens': JSON.stringify({ input: 100, output: 50 }),
      'usage:totalTokens': '150',
      'usage:costs': JSON.stringify({ total: 0.003 }),
      'usage:totalCost': '0.003',
      'usage:latencyMs': '800',
      'usage:cached': '0',
    };

    const job = Job.fromHash(mockClient as any, keys, '1', hash);
    expect(job.usage).toEqual({
      model: 'gpt-4o',
      provider: 'openai',
      tokens: { input: 100, output: 50 },
      totalTokens: 150,
      costs: { total: 0.003 },
      totalCost: 0.003,
      costUnit: undefined,
      latencyMs: 800,
      cached: false,
    });
  });

  it('parses cached: true correctly', () => {
    const hash: Record<string, string> = {
      name: 'llm-call',
      data: '{}',
      opts: '{}',
      'usage:model': 'gpt-4o',
      'usage:cached': '1',
    };

    const job = Job.fromHash(mockClient as any, keys, '1', hash);
    expect(job.usage?.cached).toBe(true);
  });

  it('returns undefined usage when no usage fields present', () => {
    const hash: Record<string, string> = {
      name: 'llm-call',
      data: '{}',
      opts: '{}',
    };

    const job = Job.fromHash(mockClient as any, keys, '1', hash);
    expect(job.usage).toBeUndefined();
  });
});

// ---- Testing mode tests ----

describe('TestJob.reportUsage', () => {
  let queue: InstanceType<typeof TestQueue>;

  afterAll(async () => {
    if (queue) await queue.close();
  });

  it('stores usage in-memory and auto-computes totalTokens', async () => {
    queue = new TestQueue('test-usage');
    const job = await queue.add('llm-call', { prompt: 'hello' });
    expect(job).not.toBeNull();

    await job!.reportUsage({ model: 'gpt-4o', tokens: { input: 100, output: 50 } });
    expect(job!.usage).toEqual({
      model: 'gpt-4o',
      tokens: { input: 100, output: 50 },
      totalTokens: 150,
    });
  });

  it('rejects negative token counts', async () => {
    queue = new TestQueue('test-usage-neg');
    const job = await queue.add('llm-call', {});
    await expect(job!.reportUsage({ tokens: { input: -1 } })).rejects.toThrow("Token count for 'input' must not be negative");
  });
});

// ---- Integration tests (require Valkey) ----

import { describeEachMode, createCleanupClient, flushQueue, waitFor } from './helpers/fixture';

const QueueImpl = require('../dist/queue').Queue;
const WorkerImpl = require('../dist/worker').Worker;
const FlowProducerImpl = require('../dist/flow-producer').FlowProducer;

describeEachMode('AI Metadata integration', (CONNECTION) => {
  const Q = 'test-ai-meta-' + Date.now();
  let queue: any;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
    queue = new QueueImpl(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('reportUsage persists and survives getJob round-trip', async () => {
    const worker = new WorkerImpl(Q, async (job: any) => {
      await job.reportUsage({
        model: 'gpt-4o',
        provider: 'openai',
        tokens: { input: 500, output: 200 },
        costs: { total: 0.012 },
        latencyMs: 850,
        cached: false,
      });
      return 'done';
    }, { connection: CONNECTION });

    const job = await queue.add('test-usage-persist', { prompt: 'hello world' });
    await waitFor(async () => {
      const fetched = await queue.getJob(job.id);
      return fetched?.finishedOn !== undefined;
    });

    const fetched = await queue.getJob(job.id);
    expect(fetched.usage).toBeDefined();
    expect(fetched.usage.model).toBe('gpt-4o');
    expect(fetched.usage.provider).toBe('openai');
    expect(fetched.usage.tokens?.input).toBe(500);
    expect(fetched.usage.tokens?.output).toBe(200);
    expect(fetched.usage.totalTokens).toBe(700);
    expect(fetched.usage.costs?.total).toBe(0.012);
    expect(fetched.usage.totalCost).toBe(0.012);
    expect(fetched.usage.latencyMs).toBe(850);
    expect(fetched.usage.cached).toBe(false);

    await worker.close(true);
  });

  it('reportUsage works from external context (post-hoc annotation)', async () => {
    const added = await queue.add('external-annotate', { data: 'test' });
    const fetched = await queue.getJob(added.id);
    expect(fetched).not.toBeNull();

    await fetched.reportUsage({ model: 'claude-sonnet-4-20250514', tokens: { input: 300 } });

    const re = await queue.getJob(added.id);
    expect(re.usage?.model).toBe('claude-sonnet-4-20250514');
    expect(re.usage?.tokens?.input).toBe(300);
    expect(re.usage?.totalTokens).toBe(300);
  });

  it('getFlowUsage aggregates parent + children', async () => {
    const fp = new FlowProducerImpl({ connection: CONNECTION });
    const flow = await fp.add({
      name: 'parent',
      queueName: Q,
      data: { step: 'aggregate' },
      children: [
        { name: 'child-1', queueName: Q, data: { step: 'embed' } },
        { name: 'child-2', queueName: Q, data: { step: 'generate' } },
      ],
    });

    const worker = new WorkerImpl(Q, async (job: any) => {
      if (job.name === 'child-1') {
        await job.reportUsage({ model: 'text-embedding-3-small', provider: 'openai', tokens: { input: 200 }, costs: { total: 0.001 } });
      } else if (job.name === 'child-2') {
        await job.reportUsage({ model: 'gpt-4o', provider: 'openai', tokens: { input: 1000, output: 500 }, costs: { total: 0.025 } });
      } else if (job.name === 'parent') {
        await job.reportUsage({ model: 'gpt-4o', provider: 'openai', tokens: { input: 50, output: 20 }, costs: { total: 0.001 } });
      }
      return 'ok';
    }, { connection: CONNECTION });

    // Wait for all jobs to complete
    await waitFor(async () => {
      const parent = await queue.getJob(flow.job.id);
      return parent?.finishedOn !== undefined;
    }, 10000);

    const usage = await queue.getFlowUsage(flow.job.id);
    expect(usage.jobCount).toBe(3);
    expect(usage.tokens.input).toBe(1250); // 200 + 1000 + 50
    expect(usage.tokens.output).toBe(520);  // 0 + 500 + 20
    expect(usage.totalTokens).toBe(1770);  // 1250 + 520
    expect(usage.costs.total).toBeCloseTo(0.027, 4); // 0.001 + 0.025 + 0.001
    expect(usage.totalCost).toBeCloseTo(0.027, 4);
    expect(usage.models['gpt-4o']).toBe(2);
    expect(usage.models['text-embedding-3-small']).toBe(1);

    await worker.close(true);
    await fp.close();
  });
});
