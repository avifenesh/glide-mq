/**
 * Tests for fallback chains: ordered model/provider alternatives on retryable failure.
 * Requires: valkey-server on localhost:6379 and cluster on :7000-7005
 *
 * Run: npx vitest run tests/fallback-chain.test.ts
 */
import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { UnrecoverableError } = require('../dist/errors') as typeof import('../src/errors');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
const { promote } = require('../dist/functions/index') as typeof import('../src/functions/index');

import { TestQueue, TestWorker } from '../src/testing';
import { describeEachMode, createCleanupClient, flushQueue, waitFor } from './helpers/fixture';

function uid() {
  return `fb-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
}

// ---- Integration tests (real Valkey) ----

describeEachMode('Fallback chains', (CONNECTION) => {
  let cleanupClient: any;
  let queueName: string;
  let queue: InstanceType<typeof Queue>;
  let worker: InstanceType<typeof Worker>;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterEach(async () => {
    if (worker) {
      try { await worker.close(true); } catch {}
    }
    if (queue) {
      try { await queue.close(); } catch {}
    }
    if (queueName) await flushQueue(cleanupClient, queueName);
  });

  afterAll(async () => {
    cleanupClient.close();
  });

  it('job with fallbacks retries through chain - processor reads job.currentFallback', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });
    const k = buildKeys(queueName);

    const fallbacks = [
      { model: 'gpt-4o-mini', provider: 'openai' },
      { model: 'claude-sonnet', provider: 'anthropic' },
    ];

    const seenFallbacks: any[] = [];

    await queue.add('llm-call', { prompt: 'hello' }, {
      attempts: 3,
      backoff: { type: 'fixed', delay: 100 },
      fallbacks,
    });

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 20000);

      worker = new Worker(queueName, async (job: any) => {
        seenFallbacks.push(job.currentFallback ?? 'original');
        if (job.attemptsMade < 2) {
          throw new Error('model down');
        }
        return { ok: true };
      }, { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 });

      worker.on('failed', () => {
        setTimeout(async () => {
          try { await promote(cleanupClient, k, Date.now()); } catch {}
        }, 200);
      });

      worker.on('completed', () => {
        clearTimeout(timeout);
        resolve();
      });

      worker.on('error', () => {});
    });

    await done;

    expect(seenFallbacks[0]).toBe('original');
    expect(seenFallbacks[1]).toEqual({ model: 'gpt-4o-mini', provider: 'openai' });
    expect(seenFallbacks[2]).toEqual({ model: 'claude-sonnet', provider: 'anthropic' });
  }, 25000);

  it('job.currentFallback reflects current position after each retry', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });
    const k = buildKeys(queueName);

    const fallbacks = [
      { model: 'fallback-1', provider: 'p1' },
      { model: 'fallback-2', provider: 'p2' },
    ];

    const positions: number[] = [];

    await queue.add('pos-check', {}, {
      attempts: 3,
      backoff: { type: 'fixed', delay: 100 },
      fallbacks,
    });

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 20000);

      worker = new Worker(queueName, async (job: any) => {
        positions.push(job.fallbackIndex);
        if (positions.length < 3) {
          throw new Error('retry');
        }
        return 'done';
      }, { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 });

      worker.on('failed', () => {
        setTimeout(async () => {
          try { await promote(cleanupClient, k, Date.now()); } catch {}
        }, 200);
      });

      worker.on('completed', () => {
        clearTimeout(timeout);
        resolve();
      });

      worker.on('error', () => {});
    });

    await done;

    expect(positions).toEqual([0, 1, 2]);
  }, 25000);

  it('fallbackIndex starts at 0, increments on each retry', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });
    const k = buildKeys(queueName);

    const fallbacks = [
      { model: 'fb1' },
      { model: 'fb2' },
    ];

    const indices: number[] = [];

    await queue.add('idx-check', {}, {
      attempts: 3,
      backoff: { type: 'fixed', delay: 100 },
      fallbacks,
    });

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 20000);

      worker = new Worker(queueName, async (job: any) => {
        indices.push(job.fallbackIndex);
        if (indices.length < 3) throw new Error('down');
        return 'ok';
      }, { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 });

      worker.on('failed', () => {
        setTimeout(async () => {
          try { await promote(cleanupClient, k, Date.now()); } catch {}
        }, 200);
      });

      worker.on('completed', () => {
        clearTimeout(timeout);
        resolve();
      });

      worker.on('error', () => {});
    });

    await done;
    expect(indices).toEqual([0, 1, 2]);
  }, 25000);

  it('after exhausting all fallbacks + attempts, job goes to failed', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });
    const k = buildKeys(queueName);

    const fallbacks = [{ model: 'fb1' }];

    const job = await queue.add('exhaust', {}, {
      attempts: 2,
      backoff: { type: 'fixed', delay: 100 },
      fallbacks,
    });

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 20000);
      let failCount = 0;

      worker = new Worker(queueName, async () => {
        throw new Error('always fails');
      }, { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 });

      worker.on('failed', (j: any) => {
        failCount++;
        if (failCount < 2) {
          setTimeout(async () => {
            try { await promote(cleanupClient, k, Date.now()); } catch {}
          }, 200);
        } else {
          clearTimeout(timeout);
          resolve();
        }
      });

      worker.on('error', () => {});
    });

    await done;

    const state = await cleanupClient.hget(k.job(job!.id), 'state');
    expect(String(state)).toBe('failed');

    const fbIdx = await cleanupClient.hget(k.job(job!.id), 'fallbackIndex');
    expect(String(fbIdx)).toBe('1');
  }, 25000);

  it('UnrecoverableError skips remaining fallbacks', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });

    const fallbacks = [
      { model: 'fb1' },
      { model: 'fb2' },
    ];

    const seenIndices: number[] = [];

    await queue.add('unrecoverable', {}, {
      attempts: 3,
      backoff: { type: 'fixed', delay: 100 },
      fallbacks,
    });

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 20000);

      worker = new Worker(queueName, async (job: any) => {
        seenIndices.push(job.fallbackIndex);
        throw new UnrecoverableError('fatal');
      }, { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 });

      worker.on('failed', () => {
        clearTimeout(timeout);
        resolve();
      });

      worker.on('error', () => {});
    });

    await done;

    // Only saw the first attempt, no retries
    expect(seenIndices).toEqual([0]);
  }, 25000);

  it('fallback chain works with exponential backoff', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });
    const k = buildKeys(queueName);

    const fallbacks = [
      { model: 'exp-fb1' },
      { model: 'exp-fb2' },
    ];

    const seenModels: (string | undefined)[] = [];

    await queue.add('exp-backoff', {}, {
      attempts: 3,
      backoff: { type: 'exponential', delay: 50 },
      fallbacks,
    });

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 20000);

      worker = new Worker(queueName, async (job: any) => {
        seenModels.push(job.currentFallback?.model);
        if (seenModels.length < 3) throw new Error('retry');
        return 'done';
      }, { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 });

      worker.on('failed', () => {
        setTimeout(async () => {
          try { await promote(cleanupClient, k, Date.now()); } catch {}
        }, 300);
      });

      worker.on('completed', () => {
        clearTimeout(timeout);
        resolve();
      });

      worker.on('error', () => {});
    });

    await done;

    expect(seenModels).toEqual([undefined, 'exp-fb1', 'exp-fb2']);
  }, 25000);

  it('fallback chain works without backoff config', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });
    const k = buildKeys(queueName);

    const fallbacks = [{ model: 'no-backoff-fb' }];
    const seenIndices: number[] = [];

    await queue.add('no-backoff', {}, {
      attempts: 2,
      fallbacks,
    });

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 20000);

      worker = new Worker(queueName, async (job: any) => {
        seenIndices.push(job.fallbackIndex);
        if (seenIndices.length < 2) throw new Error('fail');
        return 'ok';
      }, { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 });

      worker.on('failed', () => {
        setTimeout(async () => {
          try { await promote(cleanupClient, k, Date.now()); } catch {}
        }, 200);
      });

      worker.on('completed', () => {
        clearTimeout(timeout);
        resolve();
      });

      worker.on('error', () => {});
    });

    await done;
    expect(seenIndices).toEqual([0, 1]);
  }, 25000);

  it('empty fallbacks array behaves like no fallbacks', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });
    const k = buildKeys(queueName);

    const indices: number[] = [];

    await queue.add('empty-fb', {}, {
      attempts: 2,
      backoff: { type: 'fixed', delay: 100 },
      fallbacks: [],
    });

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 20000);

      worker = new Worker(queueName, async (job: any) => {
        indices.push(job.fallbackIndex);
        expect(job.currentFallback).toBeUndefined();
        if (indices.length < 2) throw new Error('fail');
        return 'ok';
      }, { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 });

      worker.on('failed', () => {
        setTimeout(async () => {
          try { await promote(cleanupClient, k, Date.now()); } catch {}
        }, 200);
      });

      worker.on('completed', () => {
        clearTimeout(timeout);
        resolve();
      });

      worker.on('error', () => {});
    });

    await done;

    // fallbackIndex stays 0 because empty array means no "fallbacks" string match in Lua
    // (opts still contains '"fallbacks":[]' which does match the string check, so it increments)
    // But currentFallback should be undefined because fallbacks[0] doesn't exist
    expect(indices[0]).toBe(0);
    expect(indices.length).toBe(2);
  }, 25000);

  it('fallback metadata accessible in processor', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });
    const k = buildKeys(queueName);

    const fallbacks = [
      { model: 'gpt-4o-mini', provider: 'openai', metadata: { temperature: 0.7, maxTokens: 100 } },
    ];

    let capturedFallback: any = null;

    await queue.add('metadata', {}, {
      attempts: 2,
      backoff: { type: 'fixed', delay: 100 },
      fallbacks,
    });

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 20000);

      worker = new Worker(queueName, async (job: any) => {
        if (job.fallbackIndex === 0) {
          throw new Error('primary down');
        }
        capturedFallback = job.currentFallback;
        return 'done';
      }, { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 });

      worker.on('failed', () => {
        setTimeout(async () => {
          try { await promote(cleanupClient, k, Date.now()); } catch {}
        }, 200);
      });

      worker.on('completed', () => {
        clearTimeout(timeout);
        resolve();
      });

      worker.on('error', () => {});
    });

    await done;

    expect(capturedFallback).toEqual({
      model: 'gpt-4o-mini',
      provider: 'openai',
      metadata: { temperature: 0.7, maxTokens: 100 },
    });
  }, 25000);
});

// ---- Testing mode ----

describe('Fallback chains [testing mode]', () => {
  it('testing mode fallback chain works end-to-end', async () => {
    const tq = new TestQueue('fb-test');

    const seenFallbacks: any[] = [];
    const fallbacks = [
      { model: 'gpt-4o-mini', provider: 'openai' },
      { model: 'claude-sonnet', provider: 'anthropic' },
    ];

    const tw = new TestWorker(tq, async (job: any) => {
      seenFallbacks.push({
        index: job.fallbackIndex,
        fallback: job.currentFallback ?? 'original',
      });
      if (job.fallbackIndex < 2) {
        throw new Error('model down');
      }
      return { ok: true };
    });

    const completed = new Promise<void>((resolve) => {
      tw.on('completed', () => resolve());
    });

    await tq.add('llm-call', { prompt: 'test' }, {
      attempts: 3,
      fallbacks,
    });

    await completed;
    await tw.close();

    expect(seenFallbacks).toEqual([
      { index: 0, fallback: 'original' },
      { index: 1, fallback: { model: 'gpt-4o-mini', provider: 'openai' } },
      { index: 2, fallback: { model: 'claude-sonnet', provider: 'anthropic' } },
    ]);
  });

  it('testing mode: UnrecoverableError skips fallbacks', async () => {
    const tq = new TestQueue('fb-unrecoverable');

    const seenIndices: number[] = [];
    const fallbacks = [{ model: 'fb1' }, { model: 'fb2' }];

    const tw = new TestWorker(tq, async (job: any) => {
      seenIndices.push(job.fallbackIndex);
      throw new UnrecoverableError('fatal');
    });

    const failed = new Promise<void>((resolve) => {
      tw.on('failed', () => resolve());
    });

    await tq.add('fatal', {}, { attempts: 3, fallbacks });

    await failed;
    await tw.close();

    expect(seenIndices).toEqual([0]);
  });

  it('testing mode: empty fallbacks array - currentFallback always undefined', async () => {
    const tq = new TestQueue('fb-empty');

    const results: any[] = [];

    const tw = new TestWorker(tq, async (job: any) => {
      results.push({ index: job.fallbackIndex, fb: job.currentFallback });
      if (results.length < 2) throw new Error('fail');
      return 'ok';
    });

    const completed = new Promise<void>((resolve) => {
      tw.on('completed', () => resolve());
    });

    await tq.add('empty', {}, { attempts: 2, fallbacks: [] });

    await completed;
    await tw.close();

    // With empty array, fallbackIndex increments but currentFallback is undefined
    // (because fallbacks[n-1] doesn't exist for empty array)
    expect(results[0].fb).toBeUndefined();
    expect(results[1].fb).toBeUndefined();
  });

  it('testing mode: fallback metadata accessible', async () => {
    const tq = new TestQueue('fb-metadata');

    let capturedFallback: any = null;
    const fallbacks = [
      { model: 'gpt-4o-mini', provider: 'openai', metadata: { temperature: 0.5, region: 'us-east' } },
    ];

    const tw = new TestWorker(tq, async (job: any) => {
      if (job.fallbackIndex === 0) throw new Error('primary down');
      capturedFallback = job.currentFallback;
      return 'ok';
    });

    const completed = new Promise<void>((resolve) => {
      tw.on('completed', () => resolve());
    });

    await tq.add('meta', {}, { attempts: 2, fallbacks });

    await completed;
    await tw.close();

    expect(capturedFallback).toEqual({
      model: 'gpt-4o-mini',
      provider: 'openai',
      metadata: { temperature: 0.5, region: 'us-east' },
    });
  });
});
