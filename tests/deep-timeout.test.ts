/**
 * Deep tests: per-job timeout edge cases.
 * Requires: valkey-server running on localhost:6379
 *
 * Run: npx vitest run tests/deep-timeout.test.ts
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';

const { GlideClient } = require('speedkey') as typeof import('speedkey');
const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { FlowProducer } = require('../dist/flow-producer') as typeof import('../src/flow-producer');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
const { LIBRARY_SOURCE, CONSUMER_GROUP } = require('../dist/functions/index') as typeof import('../src/functions/index');
const { ensureFunctionLibrary } = require('../dist/connection') as typeof import('../src/connection');
const { promote } = require('../dist/functions/index') as typeof import('../src/functions/index');

const CONNECTION = {
  addresses: [{ host: 'localhost', port: 6379 }],
};

let cleanupClient: InstanceType<typeof GlideClient>;

async function flushQueue(queueName: string) {
  const k = buildKeys(queueName);
  const keysToDelete = [
    k.id, k.stream, k.scheduled, k.completed, k.failed,
    k.events, k.meta, k.dedup, k.rate, k.schedulers,
  ];
  for (const key of keysToDelete) {
    try { await cleanupClient.del([key]); } catch {}
  }
  const prefix = `glide:{${queueName}}:`;
  let cursor = '0';
  do {
    const result = await cleanupClient.scan(cursor, { match: `${prefix}*`, count: 100 });
    cursor = result[0] as string;
    const keys = result[1] as string[];
    if (keys.length > 0) {
      await cleanupClient.del(keys);
    }
  } while (cursor !== '0');
}

beforeAll(async () => {
  cleanupClient = await GlideClient.createClient({
    addresses: [{ host: 'localhost', port: 6379 }],
  });
  await ensureFunctionLibrary(cleanupClient, LIBRARY_SOURCE);
});

afterAll(async () => {
  cleanupClient.close();
});

describe('Per-job timeout - deep edge cases', () => {
  // 1. Job completes before timeout - normal path
  it('job with timeout=1000ms completes normally when processor is fast', async () => {
    const Q = 'deep-to-ok-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    await queue.add('fast', { val: 1 }, { timeout: 1000 });

    const done = new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('test timeout')), 10000);
      const worker = new Worker(
        Q,
        async () => {
          await new Promise(r => setTimeout(r, 50));
          return 'done';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});
      worker.on('completed', (j: any, result: any) => {
        expect(result).toBe('done');
        clearTimeout(timer);
        worker.close(true).then(resolve);
      });
    });

    await done;
    await queue.close();
    await flushQueue(Q);
  }, 15000);

  // 2. Job exceeds timeout - fails with correct message
  it('job with timeout=200ms fails when processor takes 2s', async () => {
    const Q = 'deep-to-fail-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    const job = await queue.add('slow', { val: 1 }, { timeout: 200 });

    const done = new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('test timeout')), 10000);
      const worker = new Worker(
        Q,
        async () => {
          await new Promise(r => setTimeout(r, 2000));
          return 'should-not-reach';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});
      worker.on('failed', (j: any, err: Error) => {
        expect(j.id).toBe(job.id);
        expect(err.message).toBe('Job timeout exceeded');
        clearTimeout(timer);
        worker.close(true).then(resolve);
      });
    });

    await done;
    await queue.close();
    await flushQueue(Q);
  }, 15000);

  // 3. Job timeout + retry: times out first attempt, succeeds on 2nd
  it('timed-out job retries and succeeds on 2nd attempt', async () => {
    const Q = 'deep-to-retry-ok-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    let attempt = 0;
    const job = await queue.add('retry-to', { val: 1 }, {
      timeout: 200,
      attempts: 2,
      backoff: { type: 'fixed', delay: 100 },
    });

    const done = new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('test timeout')), 15000);
      const worker = new Worker(
        Q,
        async () => {
          attempt++;
          if (attempt === 1) {
            // Exceed timeout
            await new Promise(r => setTimeout(r, 2000));
            return 'unreachable';
          }
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});
      worker.on('failed', () => {
        // Promote so the retry is picked up
        setTimeout(async () => {
          try { await promote(cleanupClient, k, Date.now()); } catch {}
        }, 200);
      });
      worker.on('completed', (j: any) => {
        expect(j.id).toBe(job.id);
        clearTimeout(timer);
        worker.close(true).then(resolve);
      });
    });

    await done;

    const state = await cleanupClient.hget(k.job(job.id), 'state');
    expect(String(state)).toBe('completed');
    const attemptsMade = await cleanupClient.hget(k.job(job.id), 'attemptsMade');
    expect(String(attemptsMade)).toBe('1');

    await queue.close();
    await flushQueue(Q);
  }, 20000);

  // 4. Job timeout + retry: times out all attempts, ends in failed
  it('timed-out job exhausts all retries and ends in failed state', async () => {
    const Q = 'deep-to-retry-exhaust-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    let failCount = 0;
    const job = await queue.add('retry-exhaust', { val: 1 }, {
      timeout: 150,
      attempts: 2,
      backoff: { type: 'fixed', delay: 100 },
    });

    const done = new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('test timeout')), 15000);
      const worker = new Worker(
        Q,
        async () => {
          // Always exceed timeout
          await new Promise(r => setTimeout(r, 2000));
          return 'unreachable';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});
      worker.on('failed', () => {
        failCount++;
        if (failCount < 2) {
          setTimeout(async () => {
            try { await promote(cleanupClient, k, Date.now()); } catch {}
          }, 200);
        } else {
          clearTimeout(timer);
          setTimeout(() => worker.close(true).then(resolve), 200);
        }
      });
    });

    await done;

    expect(failCount).toBe(2);
    const state = await cleanupClient.hget(k.job(job.id), 'state');
    expect(String(state)).toBe('failed');
    const failedReason = await cleanupClient.hget(k.job(job.id), 'failedReason');
    expect(String(failedReason)).toBe('Job timeout exceeded');
    const failedScore = await cleanupClient.zscore(k.failed, job.id);
    expect(failedScore).not.toBeNull();

    await queue.close();
    await flushQueue(Q);
  }, 20000);

  // 5. Job with timeout=0 - no timeout applied
  it('timeout=0 means no timeout, slow processor completes normally', async () => {
    const Q = 'deep-to-zero-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    await queue.add('no-to', { val: 1 }, { timeout: 0 });

    const done = new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('test timeout')), 10000);
      const worker = new Worker(
        Q,
        async () => {
          await new Promise(r => setTimeout(r, 500));
          return 'slow-ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});
      worker.on('completed', (_j: any, result: any) => {
        expect(result).toBe('slow-ok');
        clearTimeout(timer);
        worker.close(true).then(resolve);
      });
    });

    await done;
    await queue.close();
    await flushQueue(Q);
  }, 15000);

  // 6. Job without timeout option - slow processor completes normally
  it('no timeout option at all means no timeout, processor completes', async () => {
    const Q = 'deep-to-none-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    await queue.add('no-to-opt', { val: 1 });

    const done = new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('test timeout')), 10000);
      const worker = new Worker(
        Q,
        async () => {
          await new Promise(r => setTimeout(r, 500));
          return 'no-to-ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});
      worker.on('completed', (_j: any, result: any) => {
        expect(result).toBe('no-to-ok');
        clearTimeout(timer);
        worker.close(true).then(resolve);
      });
    });

    await done;
    await queue.close();
    await flushQueue(Q);
  }, 15000);

  // 7. Edge: timeout=1ms - likely times out
  it('timeout=1ms is an extreme edge case that triggers timeout', async () => {
    const Q = 'deep-to-1ms-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    await queue.add('tiny-to', { val: 1 }, { timeout: 1 });

    const done = new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('test timeout')), 10000);
      const worker = new Worker(
        Q,
        async () => {
          // Even a trivial async delay should exceed 1ms
          await new Promise(r => setTimeout(r, 50));
          return 'should-not-reach';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});
      worker.on('failed', (_j: any, err: Error) => {
        expect(err.message).toBe('Job timeout exceeded');
        clearTimeout(timer);
        worker.close(true).then(resolve);
      });
    });

    await done;
    await queue.close();
    await flushQueue(Q);
  }, 15000);

  // 8. Timeout error message is exactly 'Job timeout exceeded'
  it('timeout error message is exactly "Job timeout exceeded"', async () => {
    const Q = 'deep-to-msg-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    await queue.add('msg-check', { val: 1 }, { timeout: 100 });

    const capturedError = await new Promise<Error>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('test timeout')), 10000);
      const worker = new Worker(
        Q,
        async () => {
          await new Promise(r => setTimeout(r, 2000));
          return 'unreachable';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});
      worker.on('failed', (_j: any, err: Error) => {
        clearTimeout(timer);
        worker.close(true).then(() => resolve(err));
      });
    });

    expect(capturedError).toBeInstanceOf(Error);
    expect(capturedError.message).toBe('Job timeout exceeded');
    expect(capturedError.message).not.toContain('timeout exceeded:');
    expect(capturedError.message).not.toContain('Timeout');

    await queue.close();
    await flushQueue(Q);
  }, 15000);

  // 9. Timed-out job has correct state, failedReason, attemptsMade in hash
  it('timed-out job hash has state=failed, correct failedReason and attemptsMade', async () => {
    const Q = 'deep-to-hash-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    const job = await queue.add('hash-check', { val: 1 }, { timeout: 150 });

    const done = new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('test timeout')), 10000);
      const worker = new Worker(
        Q,
        async () => {
          await new Promise(r => setTimeout(r, 2000));
          return 'unreachable';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});
      worker.on('failed', () => {
        clearTimeout(timer);
        setTimeout(() => worker.close(true).then(resolve), 200);
      });
    });

    await done;

    const state = await cleanupClient.hget(k.job(job.id), 'state');
    expect(String(state)).toBe('failed');

    const failedReason = await cleanupClient.hget(k.job(job.id), 'failedReason');
    expect(String(failedReason)).toBe('Job timeout exceeded');

    const attemptsMade = await cleanupClient.hget(k.job(job.id), 'attemptsMade');
    expect(Number(String(attemptsMade))).toBeGreaterThanOrEqual(1);

    await queue.close();
    await flushQueue(Q);
  }, 15000);

  // 10. Timed-out job emits 'failed' event with timeout error
  it('timed-out job emits failed event with Error object', async () => {
    const Q = 'deep-to-event-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    await queue.add('event-check', { val: 1 }, { timeout: 100 });

    let emittedJob: any = null;
    let emittedErr: any = null;

    const done = new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('test timeout')), 10000);
      const worker = new Worker(
        Q,
        async () => {
          await new Promise(r => setTimeout(r, 2000));
          return 'unreachable';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});
      worker.on('failed', (j: any, err: Error) => {
        emittedJob = j;
        emittedErr = err;
        clearTimeout(timer);
        worker.close(true).then(resolve);
      });
    });

    await done;

    expect(emittedJob).not.toBeNull();
    expect(emittedJob.name).toBe('event-check');
    expect(emittedErr).toBeInstanceOf(Error);
    expect(emittedErr.message).toBe('Job timeout exceeded');

    await queue.close();
    await flushQueue(Q);
  }, 15000);

  // 11. Multiple concurrent jobs, one times out, others complete normally
  it('one job times out while other concurrent jobs complete normally', async () => {
    const Q = 'deep-to-concurrent-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    // Add 3 jobs: fast, slow (will timeout), fast
    const fast1 = await queue.add('fast1', { idx: 1 }, { timeout: 5000 });
    const slow = await queue.add('slow', { idx: 2 }, { timeout: 200 });
    const fast2 = await queue.add('fast2', { idx: 3 }, { timeout: 5000 });

    const completedIds: string[] = [];
    const failedIds: string[] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('test timeout')), 15000);
      const worker = new Worker(
        Q,
        async (j: any) => {
          if (j.data.idx === 2) {
            // Slow job exceeds timeout
            await new Promise(r => setTimeout(r, 2000));
            return 'unreachable';
          }
          await new Promise(r => setTimeout(r, 50));
          return `result-${j.data.idx}`;
        },
        { connection: CONNECTION, concurrency: 3, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});
      worker.on('completed', (j: any) => {
        completedIds.push(j.id);
        if (completedIds.length + failedIds.length >= 3) {
          clearTimeout(timer);
          worker.close(true).then(resolve);
        }
      });
      worker.on('failed', (j: any) => {
        failedIds.push(j.id);
        if (completedIds.length + failedIds.length >= 3) {
          clearTimeout(timer);
          worker.close(true).then(resolve);
        }
      });
    });

    await done;

    expect(completedIds).toContain(fast1.id);
    expect(completedIds).toContain(fast2.id);
    expect(failedIds).toContain(slow.id);

    await queue.close();
    await flushQueue(Q);
  }, 20000);

  // 12. CPU-blocking processor that yields mid-way: timeout can fire during await
  it('processor that yields to event loop mid-work allows timeout to fire', async () => {
    const Q = 'deep-to-yield-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    // Timeout is 100ms. Processor does 50ms sync work, yields, then 200ms more.
    // The timeout fires during the yield point.
    await queue.add('yield-block', { val: 1 }, { timeout: 100 });

    const done = new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('test timeout')), 10000);
      const worker = new Worker(
        Q,
        async () => {
          // 50ms sync work
          const start = Date.now();
          while (Date.now() - start < 50) { /* busy-wait */ }
          // Yield to event loop - timeout can fire here
          await new Promise(r => setTimeout(r, 200));
          return 'should-not-complete';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});
      worker.on('failed', (_j: any, err: Error) => {
        expect(err.message).toBe('Job timeout exceeded');
        clearTimeout(timer);
        worker.close(true).then(resolve);
      });
    });

    await done;
    await queue.close();
    await flushQueue(Q);
  }, 15000);

  // 13. Job with timeout + removeOnFail=true - hash deleted after timeout
  it('timed-out job with removeOnFail=true has its hash deleted', async () => {
    const Q = 'deep-to-rmfail-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    const job = await queue.add('rm-fail', { val: 1 }, {
      timeout: 100,
      removeOnFail: true,
    });

    const done = new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('test timeout')), 10000);
      const worker = new Worker(
        Q,
        async () => {
          await new Promise(r => setTimeout(r, 2000));
          return 'unreachable';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});
      worker.on('failed', () => {
        clearTimeout(timer);
        setTimeout(() => worker.close(true).then(resolve), 300);
      });
    });

    await done;

    // Job hash should be deleted when removeOnFail=true
    const exists = await cleanupClient.exists([k.job(job.id)]);
    expect(exists).toBe(0);

    await queue.close();
    await flushQueue(Q);
  }, 15000);

  // 14. Job with timeout in a flow - child times out, parent stays waiting-children
  it('child timeout in flow leaves parent in waiting-children state', async () => {
    const Q = 'deep-to-flow-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    const flow = new FlowProducer({ connection: CONNECTION });

    const node = await flow.add({
      name: 'parent',
      queueName: Q,
      data: { type: 'parent' },
      children: [
        { name: 'child-ok', queueName: Q, data: { idx: 1 } },
        { name: 'child-timeout', queueName: Q, data: { idx: 2 }, opts: { timeout: 100 } },
      ],
    });

    const completedIds: string[] = [];
    const failedIds: string[] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('test timeout')), 15000);
      const worker = new Worker(
        Q,
        async (j: any) => {
          if (j.data.idx === 2) {
            // Exceed timeout
            await new Promise(r => setTimeout(r, 2000));
            return 'unreachable';
          }
          return `done-${j.data.idx}`;
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});
      worker.on('completed', (j: any) => {
        completedIds.push(j.id);
        // After both children are processed (1 completed, 1 failed)
        if (completedIds.length + failedIds.length >= 2) {
          clearTimeout(timer);
          setTimeout(() => worker.close(true).then(resolve), 300);
        }
      });
      worker.on('failed', (j: any) => {
        failedIds.push(j.id);
        if (completedIds.length + failedIds.length >= 2) {
          clearTimeout(timer);
          setTimeout(() => worker.close(true).then(resolve), 300);
        }
      });
    });

    await done;

    // The parent should still be in waiting-children because one child failed
    const parentState = await cleanupClient.hget(k.job(node.job.id), 'state');
    expect(String(parentState)).toBe('waiting-children');

    await flow.close();
    await queue.close();
    await flushQueue(Q);
  }, 20000);

  // 15. Timeout fires for one job, next job in same worker processes fine
  it('after a timeout failure, next job in the same worker processes normally', async () => {
    const Q = 'deep-to-next-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    const job1 = await queue.add('will-timeout', { idx: 1 }, { timeout: 150 });

    let processedCount = 0;
    const completedIds: string[] = [];
    const failedIds: string[] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('test timeout')), 15000);
      const worker = new Worker(
        Q,
        async (j: any) => {
          processedCount++;
          if (j.data.idx === 1) {
            await new Promise(r => setTimeout(r, 2000));
            return 'unreachable';
          }
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});
      worker.on('failed', (j: any) => {
        failedIds.push(j.id);
        // Add second job after first fails
        queue.add('will-succeed', { idx: 2 }).catch(() => {});
      });
      worker.on('completed', (j: any) => {
        completedIds.push(j.id);
        clearTimeout(timer);
        worker.close(true).then(resolve);
      });
    });

    await done;

    expect(failedIds).toContain(job1.id);
    expect(completedIds).toHaveLength(1);
    expect(processedCount).toBe(2);

    await queue.close();
    await flushQueue(Q);
  }, 20000);

  // 16. Timeout value is read from job opts, not worker opts
  it('timeout is per-job: two jobs with different timeouts behave independently', async () => {
    const Q = 'deep-to-perjob-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    // Job1: 200ms timeout, processor takes 100ms - should complete
    // Job2: 50ms timeout, processor takes 100ms - should fail
    const job1 = await queue.add('long-to', { idx: 1 }, { timeout: 2000 });
    const job2 = await queue.add('short-to', { idx: 2 }, { timeout: 50 });

    const completedIds: string[] = [];
    const failedIds: string[] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('test timeout')), 15000);
      const worker = new Worker(
        Q,
        async () => {
          // Both take 200ms
          await new Promise(r => setTimeout(r, 200));
          return 'result';
        },
        { connection: CONNECTION, concurrency: 2, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});
      worker.on('completed', (j: any) => {
        completedIds.push(j.id);
        if (completedIds.length + failedIds.length >= 2) {
          clearTimeout(timer);
          worker.close(true).then(resolve);
        }
      });
      worker.on('failed', (j: any) => {
        failedIds.push(j.id);
        if (completedIds.length + failedIds.length >= 2) {
          clearTimeout(timer);
          worker.close(true).then(resolve);
        }
      });
    });

    await done;

    expect(completedIds).toContain(job1.id);
    expect(failedIds).toContain(job2.id);

    await queue.close();
    await flushQueue(Q);
  }, 20000);

  // 17. Timeout cleanup: no dangling timers after worker close
  it('worker.close() after timeout does not leak timers', async () => {
    const Q = 'deep-to-cleanup-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    await queue.add('cleanup', { val: 1 }, { timeout: 100 });

    const done = new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('test timeout')), 10000);
      const worker = new Worker(
        Q,
        async () => {
          await new Promise(r => setTimeout(r, 2000));
          return 'unreachable';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});
      worker.on('failed', () => {
        clearTimeout(timer);
        worker.close(true).then(resolve);
      });
    });

    await done;

    // If timers leaked, this would hang or cause unhandled errors.
    // Wait a bit to ensure no unhandled rejection / timer firing after close.
    await new Promise(r => setTimeout(r, 500));

    await queue.close();
    await flushQueue(Q);
  }, 15000);

  // 18. Timed-out job is in failed ZSet
  it('timed-out job appears in the failed ZSet', async () => {
    const Q = 'deep-to-failzset-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    const job = await queue.add('fail-zset', { val: 1 }, { timeout: 100 });

    const done = new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('test timeout')), 10000);
      const worker = new Worker(
        Q,
        async () => {
          await new Promise(r => setTimeout(r, 2000));
          return 'unreachable';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});
      worker.on('failed', () => {
        clearTimeout(timer);
        setTimeout(() => worker.close(true).then(resolve), 200);
      });
    });

    await done;

    const failedScore = await cleanupClient.zscore(k.failed, job.id);
    expect(failedScore).not.toBeNull();

    const completedScore = await cleanupClient.zscore(k.completed, job.id);
    expect(completedScore).toBeNull();

    await queue.close();
    await flushQueue(Q);
  }, 15000);

  // 19. Timeout with negative value - treated same as no timeout
  it('timeout with negative value is treated as no timeout', async () => {
    const Q = 'deep-to-neg-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    await queue.add('neg-to', { val: 1 }, { timeout: -1 });

    const done = new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('test timeout')), 10000);
      const worker = new Worker(
        Q,
        async () => {
          await new Promise(r => setTimeout(r, 300));
          return 'neg-ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});
      worker.on('completed', (_j: any, result: any) => {
        expect(result).toBe('neg-ok');
        clearTimeout(timer);
        worker.close(true).then(resolve);
      });
    });

    await done;
    await queue.close();
    await flushQueue(Q);
  }, 15000);

  // 20. Timeout with processor that rejects on its own - processor error takes precedence when faster
  it('processor error fires before timeout when processor throws sooner', async () => {
    const Q = 'deep-to-throw-first-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    await queue.add('throw-first', { val: 1 }, { timeout: 5000 });

    const done = new Promise<Error>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('test timeout')), 10000);
      const worker = new Worker(
        Q,
        async () => {
          await new Promise(r => setTimeout(r, 50));
          throw new Error('processor-error');
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});
      worker.on('failed', (_j: any, err: Error) => {
        clearTimeout(timer);
        worker.close(true).then(() => resolve(err));
      });
    });

    const err = await done;
    // Processor error should be the one reported, not timeout
    expect(err.message).toBe('processor-error');

    await queue.close();
    await flushQueue(Q);
  }, 15000);

  // 21. Timeout with processor that both rejects late and timeout fires first
  it('timeout fires before late processor error', async () => {
    const Q = 'deep-to-race-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    await queue.add('race', { val: 1 }, { timeout: 100 });

    const done = new Promise<Error>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('test timeout')), 10000);
      const worker = new Worker(
        Q,
        async () => {
          await new Promise(r => setTimeout(r, 500));
          throw new Error('late-processor-error');
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});
      worker.on('failed', (_j: any, err: Error) => {
        clearTimeout(timer);
        worker.close(true).then(() => resolve(err));
      });
    });

    const err = await done;
    // Timeout should win the race
    expect(err.message).toBe('Job timeout exceeded');

    await queue.close();
    await flushQueue(Q);
  }, 15000);
});
