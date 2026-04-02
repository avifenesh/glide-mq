/**
 * Tests for dual-axis rate limiting (RPM + TPM).
 *
 * Run: npx vitest run tests/dual-rate-limit.test.ts
 */
import { describe, it, expect, vi, beforeEach, beforeAll, afterAll } from 'vitest';
import { Batch, ClusterBatch } from '@glidemq/speedkey';
import { Job } from '../src/job';
import { buildKeys } from '../src/utils';
import { TestQueue, TestWorker } from '../src/testing';

vi.mock('@glidemq/speedkey');

async function alignToTokenWindowStart(windowMs: number, bufferMs = 25): Promise<void> {
  const offset = Date.now() % windowMs;
  if (offset <= bufferMs) return;
  await new Promise<void>((resolve) => setTimeout(resolve, windowMs - offset + bufferMs));
}

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
    incrBy: vi.fn().mockResolvedValue(0),
    pexpire: vi.fn().mockResolvedValue(true),
    ...overrides,
  };
}

const keys = buildKeys('test-queue');

describe('Job.reportTokens (unit)', () => {
  let mockClient: ReturnType<typeof makeMockClient>;

  beforeEach(() => {
    vi.clearAllMocks();
    mockClient = makeMockClient();
  });

  it('stores tpmTokens in the job hash', async () => {
    const job = new Job(mockClient as any, keys, '1', 'llm-call', {}, {});
    await job.reportTokens(500);

    expect(mockClient.hset).toHaveBeenCalledWith(keys.job('1'), { tpmTokens: '500' });
    expect(job.tpmTokens).toBe(500);
  });

  it('rejects negative token count', async () => {
    const job = new Job(mockClient as any, keys, '1', 'llm-call', {}, {});
    await expect(job.reportTokens(-1)).rejects.toThrow('Token count must not be negative');
  });

  it('overwrites previous value on second call', async () => {
    const job = new Job(mockClient as any, keys, '1', 'llm-call', {}, {});
    await job.reportTokens(100);
    await job.reportTokens(200);

    expect(job.tpmTokens).toBe(200);
    expect(mockClient.hset).toHaveBeenCalledTimes(2);
    expect(mockClient.hset).toHaveBeenLastCalledWith(keys.job('1'), { tpmTokens: '200' });
  });

  it('allows zero tokens', async () => {
    const job = new Job(mockClient as any, keys, '1', 'llm-call', {}, {});
    await job.reportTokens(0);

    expect(mockClient.hset).toHaveBeenCalledWith(keys.job('1'), { tpmTokens: '0' });
    expect(job.tpmTokens).toBe(0);
  });
});

describe('Job.fromHash parses tpmTokens', () => {
  let mockClient: ReturnType<typeof makeMockClient>;

  beforeEach(() => {
    vi.clearAllMocks();
    mockClient = makeMockClient();
  });

  it('parses tpmTokens from hash', () => {
    const hash: Record<string, string> = {
      name: 'test',
      data: '{}',
      opts: '{}',
      timestamp: '1000',
      attemptsMade: '0',
      tpmTokens: '750',
    };
    const job = Job.fromHash(mockClient as any, keys, '1', hash);
    expect(job.tpmTokens).toBe(750);
  });

  it('tpmTokens is undefined when not present', () => {
    const hash: Record<string, string> = {
      name: 'test',
      data: '{}',
      opts: '{}',
      timestamp: '1000',
      attemptsMade: '0',
    };
    const job = Job.fromHash(mockClient as any, keys, '1', hash);
    expect(job.tpmTokens).toBeUndefined();
  });
});

// ---- Testing mode tests ----

describe('TestJob.reportTokens', () => {
  let queue: TestQueue;

  beforeEach(() => {
    queue = new TestQueue('test-tpm');
  });

  it('stores tpmTokens on the TestJob', async () => {
    const job = await queue.add('test', { prompt: 'hello' });
    expect(job).not.toBeNull();
    await job!.reportTokens(300);
    expect(job!.tpmTokens).toBe(300);
  });

  it('rejects negative token count', async () => {
    const job = await queue.add('test', { prompt: 'hello' });
    await expect(job!.reportTokens(-1)).rejects.toThrow('Token count must not be negative');
  });
});

describe('TestWorker TPM enforcement', () => {
  it('delays processing when TPM limit is exceeded', async () => {
    const queue = new TestQueue('test-tpm-delay');
    const completionTimes: number[] = [];
    const WINDOW_MS = 2000;

    const worker = new TestWorker(
      queue,
      async (job: any) => {
        await job.reportTokens(600);
        completionTimes.push(Date.now());
        return 'done';
      },
      {
        tokenLimiter: { maxTokens: 1000, duration: WINDOW_MS },
      },
    );

    await alignToTokenWindowStart(WINDOW_MS);

    // Add 3 jobs - first two consume 1200 tokens total (exceeds 1000 limit).
    // Third job should be delayed until the window resets.
    await queue.add('job1', {});
    await queue.add('job2', {});
    await queue.add('job3', {});

    let completedCount = 0;
    worker.on('completed', () => {
      completedCount++;
    });

    const start = Date.now();
    while (completedCount < 3 && Date.now() - start < 8000) {
      await new Promise((r) => setTimeout(r, 50));
    }

    expect(completedCount).toBe(3);
    expect(completionTimes.length).toBe(3);
    // Jobs 1 and 2 complete fast. After job 2, counter = 1200 > 1000.
    // Job 3 must wait for window reset. The wait depends on window alignment,
    // so we just verify there IS a meaningful delay (> 200ms).
    const gap = completionTimes[2] - completionTimes[1];
    expect(gap).toBeGreaterThanOrEqual(200);

    await worker.close();
    await queue.close();
  });

  it('reportUsage auto-feeds TPM when tokenLimiter is active', async () => {
    const queue = new TestQueue('test-tpm-usage');
    const completionTimes: number[] = [];
    const WINDOW_MS = 2000;

    const worker = new TestWorker(
      queue,
      async (job: any) => {
        await job.reportUsage({ tokens: { input: 400, output: 200 } });
        completionTimes.push(Date.now());
        return 'done';
      },
      {
        tokenLimiter: { maxTokens: 1000, duration: WINDOW_MS },
      },
    );

    await alignToTokenWindowStart(WINDOW_MS);

    await queue.add('job1', {});
    await queue.add('job2', {});
    await queue.add('job3', {});

    let completedCount = 0;
    worker.on('completed', () => {
      completedCount++;
    });

    const start = Date.now();
    while (completedCount < 3 && Date.now() - start < 8000) {
      await new Promise((r) => setTimeout(r, 50));
    }

    expect(completedCount).toBe(3);
    // Jobs 1 + 2 = 1200 tokens, exceeds 1000. Job 3 must wait for window reset.
    const gap = completionTimes[2] - completionTimes[1];
    expect(gap).toBeGreaterThanOrEqual(200);

    await worker.close();
    await queue.close();
  });

  it('allows processing when under the TPM limit', async () => {
    const queue = new TestQueue('test-tpm-under');
    const completionTimes: number[] = [];

    const worker = new TestWorker(
      queue,
      async (job: any) => {
        await job.reportTokens(100);
        completionTimes.push(Date.now());
        return 'done';
      },
      {
        tokenLimiter: { maxTokens: 10000, duration: 5000 },
      },
    );

    // 5 jobs x 100 tokens = 500 tokens - well under 10000 limit
    for (let i = 0; i < 5; i++) {
      await queue.add(`job${i}`, {});
    }

    let completedCount = 0;
    worker.on('completed', () => {
      completedCount++;
    });

    const start = Date.now();
    while (completedCount < 5 && Date.now() - start < 3000) {
      await new Promise((r) => setTimeout(r, 50));
    }

    expect(completedCount).toBe(5);
    // All should complete quickly - no significant delay
    const totalTime = completionTimes[4] - completionTimes[0];
    expect(totalTime).toBeLessThan(500);

    await worker.close();
    await queue.close();
  });

  it('uses max of reportTokens and reportUsage totalTokens', async () => {
    const queue = new TestQueue('test-tpm-max');
    const completionTimes: number[] = [];
    const WINDOW_MS = 2000;

    const worker = new TestWorker(
      queue,
      async (job: any) => {
        // reportUsage gives 200 totalTokens, reportTokens gives 600
        // Worker should use max(200, 600) = 600 for TPM tracking
        await job.reportUsage({ tokens: { input: 100, output: 100 } });
        await job.reportTokens(600);
        completionTimes.push(Date.now());
        return 'done';
      },
      {
        tokenLimiter: { maxTokens: 1000, duration: WINDOW_MS },
      },
    );

    await alignToTokenWindowStart(WINDOW_MS);

    await queue.add('job1', {});
    await queue.add('job2', {});
    await queue.add('job3', {});

    let completedCount = 0;
    worker.on('completed', () => {
      completedCount++;
    });

    const start = Date.now();
    while (completedCount < 3 && Date.now() - start < 8000) {
      await new Promise((r) => setTimeout(r, 50));
    }

    expect(completedCount).toBe(3);
    // 600 tokens per job (from tpmTokens, since it's larger than 200).
    // 2 jobs = 1200 > 1000. Third must wait.
    const gap = completionTimes[2] - completionTimes[1];
    expect(gap).toBeGreaterThanOrEqual(200);

    await worker.close();
    await queue.close();
  });
});

describe('buildKeys includes tpm key', () => {
  it('generates correct tpm key', () => {
    const k = buildKeys('my-queue');
    expect(k.tpm).toBe('glide:{my-queue}:tpm');
  });

  it('respects custom prefix', () => {
    const k = buildKeys('my-queue', 'custom');
    expect(k.tpm).toBe('custom:{my-queue}:tpm');
  });
});

// ---- Integration tests (require Valkey) ----

import { describeEachMode, createCleanupClient, flushQueue, waitFor } from './helpers/fixture';

const QueueImpl = require('../dist/queue').Queue;
const WorkerImpl = require('../dist/worker').Worker;

describeEachMode('Dual-axis rate limiting (TPM)', (CONNECTION) => {
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    cleanupClient.close();
  });

  async function cleanupTpmKeys(queueName: string) {
    const tpmPattern = `glide:{${queueName}}:tpm:*`;
    try {
      if (cleanupClient.constructor.name === 'GlideClusterClient') {
        const { ClusterScanCursor } = require('@glidemq/speedkey');
        let cursor = new ClusterScanCursor();
        while (!cursor.isFinished()) {
          const [nextCursor, found] = await cleanupClient.scan(cursor, { match: tpmPattern, count: 100 });
          cursor = nextCursor;
          if (found.length > 0) await cleanupClient.del(found.map((k: any) => String(k)));
        }
      } else {
        let cursor = '0';
        do {
          const result = await cleanupClient.scan(cursor, { match: tpmPattern, count: 100 });
          cursor = result[0] as string;
          const found = result[1] as string[];
          if (found.length > 0) await cleanupClient.del(found);
        } while (cursor !== '0');
      }
    } catch {
      /* ignore */
    }
  }

  it('reportTokens persists tpmTokens in the job hash', async () => {
    const Q = `tpm-persist-${Date.now()}`;
    const queue = new QueueImpl(Q, { connection: CONNECTION });

    const worker = new WorkerImpl(
      Q,
      async (job: any) => {
        await job.reportTokens(1500);
        return 'done';
      },
      { connection: CONNECTION },
    );

    const added = await queue.add('test-report-tokens', { prompt: 'test' });
    await waitFor(async () => {
      const fetched = await queue.getJob(added.id);
      return fetched?.finishedOn !== undefined;
    });

    const fetched = await queue.getJob(added.id);
    expect(fetched.tpmTokens).toBe(1500);

    await worker.close(true);
    await queue.close();
    await flushQueue(cleanupClient, Q);
  });

  it('worker pauses when TPM limit is exceeded (scope: worker)', async () => {
    const Q = `tpm-wscope-${Date.now()}`;
    const queue = new QueueImpl(Q, { connection: CONNECTION });
    const completionTimes: number[] = [];
    // Use a 2s window for reliability
    const WINDOW_MS = 2000;

    const worker = new WorkerImpl(
      Q,
      async (job: any) => {
        await job.reportTokens(600);
        completionTimes.push(Date.now());
        return 'done';
      },
      {
        connection: CONNECTION,
        tokenLimiter: { maxTokens: 1000, duration: WINDOW_MS, scope: 'worker' },
      },
    );

    await queue.add('tpm-w1', {});
    await queue.add('tpm-w2', {});
    await queue.add('tpm-w3', {});

    await waitFor(async () => completionTimes.length >= 3, 10000);

    expect(completionTimes.length).toBe(3);
    // First two: 1200 tokens > 1000 limit. Third must wait for window reset.
    // Window alignment means the gap can be anywhere from ~0ms to ~2000ms.
    // We just verify the gap exceeds some minimum (> 200ms).
    const gap = completionTimes[2] - completionTimes[1];
    expect(gap).toBeGreaterThanOrEqual(200);

    await worker.close(true);
    await queue.close();
    await flushQueue(cleanupClient, Q);
    await cleanupTpmKeys(Q);
  });

  it('worker pauses when TPM limit is exceeded (scope: queue)', async () => {
    const Q = `tpm-qscope-${Date.now()}`;
    const queue = new QueueImpl(Q, { connection: CONNECTION });
    const completionTimes: number[] = [];
    const WINDOW_MS = 2000;

    const worker = new WorkerImpl(
      Q,
      async (job: any) => {
        await job.reportTokens(600);
        completionTimes.push(Date.now());
        return 'done';
      },
      {
        connection: CONNECTION,
        tokenLimiter: { maxTokens: 1000, duration: WINDOW_MS, scope: 'queue' },
      },
    );

    await queue.add('tpm-q1', {});
    await queue.add('tpm-q2', {});
    await queue.add('tpm-q3', {});

    await waitFor(async () => completionTimes.length >= 3, 10000);

    expect(completionTimes.length).toBe(3);
    const gap = completionTimes[2] - completionTimes[1];
    expect(gap).toBeGreaterThanOrEqual(200);

    await worker.close(true);
    await queue.close();
    await flushQueue(cleanupClient, Q);
    await cleanupTpmKeys(Q);
  });

  it('worker pauses when TPM limit is exceeded (scope: both)', async () => {
    const Q = `tpm-bscope-${Date.now()}`;
    const queue = new QueueImpl(Q, { connection: CONNECTION });
    const completionTimes: number[] = [];
    const WINDOW_MS = 2000;

    const worker = new WorkerImpl(
      Q,
      async (job: any) => {
        await job.reportTokens(600);
        completionTimes.push(Date.now());
        return 'done';
      },
      {
        connection: CONNECTION,
        tokenLimiter: { maxTokens: 1000, duration: WINDOW_MS, scope: 'both' },
      },
    );

    await queue.add('tpm-b1', {});
    await queue.add('tpm-b2', {});
    await queue.add('tpm-b3', {});

    await waitFor(async () => completionTimes.length >= 3, 10000);

    expect(completionTimes.length).toBe(3);
    const gap = completionTimes[2] - completionTimes[1];
    expect(gap).toBeGreaterThanOrEqual(200);

    await worker.close(true);
    await queue.close();
    await flushQueue(cleanupClient, Q);
    await cleanupTpmKeys(Q);
  });

  it('worker resumes after TPM window expires', async () => {
    const Q = `tpm-resume-${Date.now()}`;
    const queue = new QueueImpl(Q, { connection: CONNECTION });
    const completionTimes: number[] = [];
    const WINDOW_MS = 2000;

    const worker = new WorkerImpl(
      Q,
      async (job: any) => {
        await job.reportTokens(1100);
        completionTimes.push(Date.now());
        return 'done';
      },
      {
        connection: CONNECTION,
        tokenLimiter: { maxTokens: 1000, duration: WINDOW_MS, scope: 'worker' },
      },
    );

    await queue.add('tpm-resume-1', {});
    await queue.add('tpm-resume-2', {});

    await waitFor(async () => completionTimes.length >= 2, 10000);

    expect(completionTimes.length).toBe(2);
    // Single job of 1100 tokens exceeds 1000. Second job waits for new window.
    const gap = completionTimes[1] - completionTimes[0];
    expect(gap).toBeGreaterThanOrEqual(200);

    await worker.close(true);
    await queue.close();
    await flushQueue(cleanupClient, Q);
    await cleanupTpmKeys(Q);
  });

  it('RPM and TPM enforced independently', async () => {
    const Q = `tpm-indep-${Date.now()}`;
    const queue = new QueueImpl(Q, { connection: CONNECTION });
    const completedNames: string[] = [];

    const worker = new WorkerImpl(
      Q,
      async (job: any) => {
        // Report minimal tokens - TPM should not block
        await job.reportTokens(10);
        completedNames.push(job.name);
        return 'done';
      },
      {
        connection: CONNECTION,
        limiter: { max: 100, duration: 5000 },
        tokenLimiter: { maxTokens: 100000, duration: 5000, scope: 'worker' },
      },
    );

    await queue.add('rpm-tpm-1', {});
    await queue.add('rpm-tpm-2', {});
    await queue.add('rpm-tpm-3', {});

    await waitFor(async () => completedNames.length >= 3, 8000);

    expect(completedNames.length).toBe(3);

    await worker.close(true);
    await queue.close();
    await flushQueue(cleanupClient, Q);
    await cleanupTpmKeys(Q);
  });

  it('one large-token job blocks until window resets', async () => {
    const Q = `tpm-large-${Date.now()}`;
    const queue = new QueueImpl(Q, { connection: CONNECTION });
    const completionTimes: number[] = [];
    const WINDOW_MS = 2000;

    const worker = new WorkerImpl(
      Q,
      async (job: any) => {
        await job.reportTokens(5000);
        completionTimes.push(Date.now());
        return 'done';
      },
      {
        connection: CONNECTION,
        tokenLimiter: { maxTokens: 5000, duration: WINDOW_MS, scope: 'worker' },
      },
    );

    await queue.add('big-token-1', {});
    await queue.add('big-token-2', {});

    await waitFor(async () => completionTimes.length >= 2, 10000);

    expect(completionTimes.length).toBe(2);
    const gap = completionTimes[1] - completionTimes[0];
    // The delay depends on window alignment, but must be non-trivial.
    expect(gap).toBeGreaterThanOrEqual(200);

    await worker.close(true);
    await queue.close();
    await flushQueue(cleanupClient, Q);
    await cleanupTpmKeys(Q);
  });

  it('reportUsage auto-feeds TPM counter', async () => {
    const Q = `tpm-auto-${Date.now()}`;
    const queue = new QueueImpl(Q, { connection: CONNECTION });
    const completionTimes: number[] = [];
    const WINDOW_MS = 2000;

    const worker = new WorkerImpl(
      Q,
      async (job: any) => {
        await job.reportUsage({ tokens: { input: 400, output: 200 } });
        completionTimes.push(Date.now());
        return 'done';
      },
      {
        connection: CONNECTION,
        tokenLimiter: { maxTokens: 1000, duration: WINDOW_MS, scope: 'worker' },
      },
    );

    await queue.add('auto-tpm-1', {});
    await queue.add('auto-tpm-2', {});
    await queue.add('auto-tpm-3', {});

    await waitFor(async () => completionTimes.length >= 3, 10000);

    expect(completionTimes.length).toBe(3);
    // 600 tokens per job via usage. 2 jobs = 1200 > 1000. Third must wait.
    const gap = completionTimes[2] - completionTimes[1];
    expect(gap).toBeGreaterThanOrEqual(200);

    await worker.close(true);
    await queue.close();
    await flushQueue(cleanupClient, Q);
    await cleanupTpmKeys(Q);
  });

  it('queue-scope TPM uses shared Valkey counter', async () => {
    // Verify the Valkey counter is actually incremented and readable.
    // We don't test multi-worker blocking (too flaky with timing) but verify the
    // shared counter mechanics directly.
    const Q = `tpm-shared-${Date.now()}`;
    const queue = new QueueImpl(Q, { connection: CONNECTION });
    const { buildKeys: buildK } = require('../dist/utils') as typeof import('../src/utils');
    const k = buildK(Q);
    const WINDOW_MS = 5000;

    const worker = new WorkerImpl(
      Q,
      async (job: any) => {
        await job.reportTokens(500);
        return 'done';
      },
      {
        connection: CONNECTION,
        tokenLimiter: { maxTokens: 10000, duration: WINDOW_MS, scope: 'queue' },
      },
    );

    // Add 3 jobs - each reports 500 tokens
    await queue.add('shared-1', {});
    await queue.add('shared-2', {});
    await queue.add('shared-3', {});

    await waitFor(async () => {
      const counts = await queue.getJobCounts();
      return counts.completed >= 3;
    }, 8000);

    // Check the Valkey TPM counter - should have 1500 tokens
    const windowId = Math.floor(Date.now() / WINDOW_MS);
    const tpmKey = `${k.tpm}:${windowId}`;
    const counterVal = await cleanupClient.incrBy(tpmKey, 0);
    expect(Number(counterVal)).toBe(1500);

    await worker.close(true);
    await queue.close();
    await flushQueue(cleanupClient, Q);
    await cleanupTpmKeys(Q);
  });
});
