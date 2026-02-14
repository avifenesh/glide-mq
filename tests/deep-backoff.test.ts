/**
 * Deep tests: Custom backoff strategies.
 * Requires: valkey-server on localhost:6379 and cluster on :7000-7005
 *
 * Run: npx vitest run tests/deep-backoff.test.ts
 */
import { it, expect, beforeAll, afterAll } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
const { promote } = require('../dist/functions/index') as typeof import('../src/functions/index');

import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

describeEachMode('Custom backoff strategies', (CONNECTION) => {
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    cleanupClient.close();
  });

  it('custom strategy is invoked with attemptsMade and error', async () => {
    const Q = 'deep-backoff-invoke-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    const strategyArgs: { attempt: number; message: string }[] = [];

    const job = await queue.add('task', { v: 1 }, {
      attempts: 3,
      backoff: { type: 'custom-linear', delay: 100 },
    });

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 20000);
      let attemptCount = 0;

      const worker = new Worker(
        Q,
        async () => {
          attemptCount++;
          if (attemptCount <= 2) {
            throw new Error(`fail-${attemptCount}`);
          }
          return 'ok';
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 500,
          promotionInterval: 500,
          backoffStrategies: {
            'custom-linear': (attempt: number, err: Error) => {
              strategyArgs.push({ attempt, message: err.message });
              return attempt * 100;
            },
          },
        },
      );

      worker.on('failed', () => {
        setTimeout(async () => {
          try { await promote(cleanupClient, k, Date.now()); } catch {}
        }, 200);
      });

      worker.on('completed', () => {
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });
      worker.on('error', () => {});
    });

    await done;

    expect(strategyArgs).toHaveLength(2);
    expect(strategyArgs[0]).toEqual({ attempt: 1, message: 'fail-1' });
    expect(strategyArgs[1]).toEqual({ attempt: 2, message: 'fail-2' });

    await queue.close();
    await flushQueue(cleanupClient, Q);
  }, 25000);

  it('custom strategy controls actual backoff delay', async () => {
    const Q = 'deep-backoff-delay-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    const job = await queue.add('task', {}, {
      attempts: 2,
      backoff: { type: 'custom-fixed-500', delay: 0 },
    });

    let failCount = 0;

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);

      const worker = new Worker(
        Q,
        async () => {
          failCount++;
          if (failCount <= 1) {
            throw new Error('retry-me');
          }
          return 'done';
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 500,
          promotionInterval: 500,
          backoffStrategies: {
            'custom-fixed-500': () => 500,
          },
        },
      );

      worker.on('failed', () => {
        setTimeout(async () => {
          try { await promote(cleanupClient, k, Date.now()); } catch {}
        }, 600);
      });

      worker.on('completed', () => {
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });
      worker.on('error', () => {});
    });

    await done;

    const state = await cleanupClient.hget(k.job(job.id), 'state');
    expect(String(state)).toBe('completed');

    await queue.close();
    await flushQueue(cleanupClient, Q);
  }, 20000);

  it('custom strategy returning 0 retries immediately (no backoff delay)', async () => {
    const Q = 'deep-backoff-zero-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    const timestamps: number[] = [];

    const job = await queue.add('task', {}, {
      attempts: 3,
      backoff: { type: 'instant-retry', delay: 0 },
    });

    let attemptCount = 0;

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);

      const worker = new Worker(
        Q,
        async () => {
          attemptCount++;
          timestamps.push(Date.now());
          if (attemptCount <= 2) {
            throw new Error('nope');
          }
          return 'success';
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 500,
          promotionInterval: 500,
          backoffStrategies: {
            'instant-retry': () => 0,
          },
        },
      );

      worker.on('failed', () => {
        setTimeout(async () => {
          try { await promote(cleanupClient, k, Date.now()); } catch {}
        }, 100);
      });

      worker.on('completed', () => {
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });
      worker.on('error', () => {});
    });

    await done;

    expect(attemptCount).toBe(3);
    const finalState = await cleanupClient.hget(k.job(job.id), 'state');
    expect(String(finalState)).toBe('completed');

    await queue.close();
    await flushQueue(cleanupClient, Q);
  }, 20000);

  it('falls back to built-in calculateBackoff when strategy name is unregistered', async () => {
    const Q = 'deep-backoff-fallback-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    const job = await queue.add('task', {}, {
      attempts: 2,
      backoff: { type: 'fixed', delay: 200 },
    });

    let attemptCount = 0;

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);

      const worker = new Worker(
        Q,
        async () => {
          attemptCount++;
          if (attemptCount <= 1) throw new Error('fail');
          return 'ok';
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 500,
          promotionInterval: 500,
          backoffStrategies: {
            'my-custom': () => 9999,
          },
        },
      );

      worker.on('failed', () => {
        setTimeout(async () => {
          try { await promote(cleanupClient, k, Date.now()); } catch {}
        }, 300);
      });

      worker.on('completed', () => {
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });
      worker.on('error', () => {});
    });

    await done;

    expect(attemptCount).toBe(2);
    const state = await cleanupClient.hget(k.job(job.id), 'state');
    expect(String(state)).toBe('completed');

    await queue.close();
    await flushQueue(cleanupClient, Q);
  }, 20000);

  it('custom strategy that depends on error type', async () => {
    const Q = 'deep-backoff-errtype-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    const delays: number[] = [];

    const job = await queue.add('task', {}, {
      attempts: 3,
      backoff: { type: 'error-aware', delay: 100 },
    });

    let attemptCount = 0;

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 20000);

      const worker = new Worker(
        Q,
        async () => {
          attemptCount++;
          if (attemptCount === 1) throw new Error('TRANSIENT: connection reset');
          if (attemptCount === 2) throw new Error('FATAL: data corrupt');
          return 'recovered';
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 500,
          promotionInterval: 500,
          backoffStrategies: {
            'error-aware': (_attempt: number, err: Error) => {
              const delay = err.message.includes('TRANSIENT') ? 200 : 2000;
              delays.push(delay);
              return delay;
            },
          },
        },
      );

      worker.on('failed', () => {
        setTimeout(async () => {
          try { await promote(cleanupClient, k, Date.now()); } catch {}
        }, 300);
      });

      worker.on('completed', () => {
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });
      worker.on('error', () => {});
    });

    await done;

    expect(delays).toEqual([200, 2000]);

    await queue.close();
    await flushQueue(cleanupClient, Q);
  }, 25000);

  it('custom strategy with exponential-like formula', async () => {
    const Q = 'deep-backoff-custom-exp-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    const computedDelays: number[] = [];

    const job = await queue.add('task', {}, {
      attempts: 4,
      backoff: { type: 'custom-exp', delay: 50 },
    });

    let attemptCount = 0;

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 20000);

      const worker = new Worker(
        Q,
        async () => {
          attemptCount++;
          if (attemptCount <= 3) throw new Error('retry');
          return 'ok';
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 500,
          promotionInterval: 500,
          backoffStrategies: {
            'custom-exp': (attempt: number) => {
              const delay = Math.pow(3, attempt - 1) * 50;
              computedDelays.push(delay);
              return delay;
            },
          },
        },
      );

      worker.on('failed', () => {
        setTimeout(async () => {
          try { await promote(cleanupClient, k, Date.now()); } catch {}
        }, 300);
      });

      worker.on('completed', () => {
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });
      worker.on('error', () => {});
    });

    await done;

    expect(computedDelays).toEqual([50, 150, 450]);

    await queue.close();
    await flushQueue(cleanupClient, Q);
  }, 25000);

  it('custom strategy is not called when no attempts configured', async () => {
    const Q = 'deep-backoff-noattempts-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    let strategyCalled = false;

    const job = await queue.add('task', {}, {
      backoff: { type: 'should-not-run', delay: 100 },
    });

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);

      const worker = new Worker(
        Q,
        async () => { throw new Error('fail'); },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 500,
          promotionInterval: 500,
          backoffStrategies: {
            'should-not-run': () => {
              strategyCalled = true;
              return 100;
            },
          },
        },
      );

      worker.on('failed', () => {
        clearTimeout(timeout);
        setTimeout(() => worker.close(true).then(resolve), 200);
      });
      worker.on('error', () => {});
    });

    await done;

    expect(strategyCalled).toBe(false);
    const state = await cleanupClient.hget(k.job(job.id), 'state');
    expect(String(state)).toBe('failed');

    await queue.close();
    await flushQueue(cleanupClient, Q);
  }, 15000);

  it('custom strategy is not called when no backoff option on job', async () => {
    const Q = 'deep-backoff-nobackoff-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    let strategyCalled = false;

    const job = await queue.add('task', {}, {
      attempts: 3,
    });

    let failCount = 0;

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);

      const worker = new Worker(
        Q,
        async () => {
          failCount++;
          if (failCount <= 1) throw new Error('fail');
          return 'ok';
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 500,
          promotionInterval: 500,
          backoffStrategies: {
            'anything': () => {
              strategyCalled = true;
              return 100;
            },
          },
        },
      );

      worker.on('failed', () => {
        setTimeout(async () => {
          try { await promote(cleanupClient, k, Date.now()); } catch {}
        }, 200);
      });

      worker.on('completed', () => {
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });
      worker.on('error', () => {});
    });

    await done;

    expect(strategyCalled).toBe(false);

    await queue.close();
    await flushQueue(cleanupClient, Q);
  }, 20000);

  it('multiple custom strategies can coexist on one worker', async () => {
    const Q = 'deep-backoff-multi-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    const usedStrategies: string[] = [];

    const jobA = await queue.add('jobA', { strategy: 'alpha' }, {
      attempts: 2,
      backoff: { type: 'alpha', delay: 100 },
    });
    const jobB = await queue.add('jobB', { strategy: 'beta' }, {
      attempts: 2,
      backoff: { type: 'beta', delay: 100 },
    });

    const failCounts: Record<string, number> = {};
    let completedCount = 0;

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 20000);

      const worker = new Worker(
        Q,
        async (job: any) => {
          failCounts[job.name] = (failCounts[job.name] || 0) + 1;
          if (failCounts[job.name] <= 1) {
            throw new Error(`${job.name}-fail`);
          }
          return 'ok';
        },
        {
          connection: CONNECTION,
          concurrency: 2,
          blockTimeout: 500,
          promotionInterval: 500,
          backoffStrategies: {
            alpha: () => { usedStrategies.push('alpha'); return 100; },
            beta: () => { usedStrategies.push('beta'); return 200; },
          },
        },
      );

      worker.on('failed', () => {
        setTimeout(async () => {
          try { await promote(cleanupClient, k, Date.now()); } catch {}
        }, 300);
      });

      worker.on('completed', () => {
        completedCount++;
        if (completedCount >= 2) {
          clearTimeout(timeout);
          setTimeout(() => worker.close(true).then(resolve), 200);
        }
      });
      worker.on('error', () => {});
    });

    await done;

    expect(usedStrategies).toContain('alpha');
    expect(usedStrategies).toContain('beta');

    await queue.close();
    await flushQueue(cleanupClient, Q);
  }, 25000);

  it('custom strategy returning large delay still retries after promote', async () => {
    const Q = 'deep-backoff-large-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    const job = await queue.add('task', {}, {
      attempts: 2,
      backoff: { type: 'very-slow', delay: 0 },
    });

    let attemptCount = 0;

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);

      const worker = new Worker(
        Q,
        async () => {
          attemptCount++;
          if (attemptCount <= 1) throw new Error('fail');
          return 'ok';
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 500,
          promotionInterval: 500,
          backoffStrategies: {
            'very-slow': () => 60000,
          },
        },
      );

      worker.on('failed', () => {
        setTimeout(async () => {
          try {
            await promote(cleanupClient, k, Date.now() + 120000);
          } catch {}
        }, 300);
      });

      worker.on('completed', () => {
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });
      worker.on('error', () => {});
    });

    await done;

    expect(attemptCount).toBe(2);
    const state = await cleanupClient.hget(k.job(job.id), 'state');
    expect(String(state)).toBe('completed');

    await queue.close();
    await flushQueue(cleanupClient, Q);
  }, 20000);

  it('exhausted retries with custom strategy moves to failed', async () => {
    const Q = 'deep-backoff-exhaust-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);

    const job = await queue.add('task', {}, {
      attempts: 2,
      backoff: { type: 'custom-fail', delay: 100 },
    });

    let attemptCount = 0;

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);
      let failCount = 0;

      const worker = new Worker(
        Q,
        async () => {
          attemptCount++;
          throw new Error('always-fail');
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 500,
          promotionInterval: 500,
          backoffStrategies: {
            'custom-fail': () => 100,
          },
        },
      );

      worker.on('failed', () => {
        failCount++;
        if (failCount < 2) {
          setTimeout(async () => {
            try { await promote(cleanupClient, k, Date.now()); } catch {}
          }, 200);
        } else {
          clearTimeout(timeout);
          setTimeout(() => worker.close(true).then(resolve), 200);
        }
      });
      worker.on('error', () => {});
    });

    await done;

    expect(attemptCount).toBe(2);
    const state = await cleanupClient.hget(k.job(job.id), 'state');
    expect(String(state)).toBe('failed');

    const failedReason = await cleanupClient.hget(k.job(job.id), 'failedReason');
    expect(String(failedReason)).toBe('always-fail');

    await queue.close();
    await flushQueue(cleanupClient, Q);
  }, 20000);
});
