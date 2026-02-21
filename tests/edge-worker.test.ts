/**
 * Edge-case integration tests for Worker resilience and stalled recovery.
 * Runs against both standalone (:6379) and cluster (:7000).
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { describeEachMode, createCleanupClient, flushQueue, ConnectionConfig } from './helpers/fixture';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
const { CONSUMER_GROUP } = require('../dist/functions/index') as typeof import('../src/functions/index');

describeEachMode('Edge: Worker', (CONNECTION) => {
  let cleanupClient: any;
  const TS = Date.now();

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    cleanupClient.close();
  });

  describe('close(force=true) leaves jobs in PEL', () => {
    const Q = `ew-force-close-${TS}`;

    afterAll(async () => {
      await flushQueue(cleanupClient, Q);
    });

    it('force-closing a worker while a job is active does not ACK the job', async () => {
      const queue = new Queue(Q, { connection: CONNECTION });
      let processorStarted = false;

      const workerReady = new Promise<InstanceType<typeof Worker>>((resolve) => {
        const w = new Worker(
          Q,
          async () => {
            processorStarted = true;
            await new Promise((r) => setTimeout(r, 30000));
            return 'done';
          },
          { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
        );
        w.on('error', () => {});
        w.waitUntilReady().then(() => resolve(w));
      });

      const worker = await workerReady;

      await queue.add('long-task', { x: 1 });
      while (!processorStarted) {
        await new Promise((r) => setTimeout(r, 50));
      }

      await worker.close(true);

      const k = buildKeys(Q);
      const pending = (await cleanupClient.customCommand(['XPENDING', k.stream, CONSUMER_GROUP])) as any[];
      expect(Number(pending[0])).toBeGreaterThanOrEqual(1);

      await queue.close();
    }, 15000);
  });

  describe('close(force=false) waits for active jobs', () => {
    const Q = `ew-graceful-close-${TS}`;

    afterAll(async () => {
      await flushQueue(cleanupClient, Q);
    });

    it('graceful close waits for active job to complete', async () => {
      const queue = new Queue(Q, { connection: CONNECTION });
      const completed: string[] = [];

      const worker = new Worker(
        Q,
        async (job: any) => {
          await new Promise((r) => setTimeout(r, 500));
          completed.push(job.id);
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});
      await worker.waitUntilReady();

      const job = await queue.add('task', { x: 1 });

      await new Promise((r) => setTimeout(r, 300));

      await worker.close(false);

      expect(completed).toContain(job.id);

      await queue.close();
    }, 15000);
  });

  describe('pause and resume', () => {
    const Q = `ew-pause-${TS}`;

    afterAll(async () => {
      await flushQueue(cleanupClient, Q);
    });

    it('pause allows current job to finish but blocks new jobs', async () => {
      const queue = new Queue(Q, { connection: CONNECTION });
      const processed: string[] = [];

      const worker = new Worker(
        Q,
        async (job: any) => {
          await new Promise((r) => setTimeout(r, 200));
          processed.push(job.id);
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', () => {});
      await worker.waitUntilReady();

      const job1 = await queue.add('task1', { i: 1 });

      await new Promise((r) => setTimeout(r, 100));
      await worker.pause();

      expect(processed).toContain(job1.id);

      const countBefore = processed.length;

      const job2 = await queue.add('task2', { i: 2 });
      await new Promise((r) => setTimeout(r, 1500));

      expect(processed.length).toBe(countBefore);

      await worker.resume();

      await new Promise((r) => setTimeout(r, 2000));
      expect(processed).toContain(job2.id);

      await worker.close(true);
      await queue.close();
    }, 15000);
  });

  describe('concurrency=1 processes sequentially', () => {
    const Q = `ew-seq-${TS}`;

    afterAll(async () => {
      await flushQueue(cleanupClient, Q);
    });

    it('jobs are processed in order with concurrency=1', async () => {
      const queue = new Queue(Q, { connection: CONNECTION });
      const order: string[] = [];

      const done = new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
        const worker = new Worker(
          Q,
          async (job: any) => {
            order.push(job.name);
            if (order.length >= 3) {
              clearTimeout(timeout);
              setTimeout(() => worker.close(true).then(resolve), 200);
            }
            return 'ok';
          },
          { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
        );
        worker.on('error', () => {});
      });

      await new Promise((r) => setTimeout(r, 500));
      await queue.add('first', {});
      await queue.add('second', {});
      await queue.add('third', {});

      await done;

      expect(order).toEqual(['first', 'second', 'third']);
      await queue.close();
    }, 15000);
  });

  describe('error handler: processor throws', () => {
    const Q = `ew-throw-${TS}`;

    afterAll(async () => {
      await flushQueue(cleanupClient, Q);
    });

    it('emits failed event with the error when processor throws', async () => {
      const queue = new Queue(Q, { connection: CONNECTION });
      const failedEvents: { jobId: string; message: string }[] = [];

      const done = new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
        const worker = new Worker(
          Q,
          async () => {
            throw new Error('processor exploded');
          },
          { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
        );
        worker.on('error', () => {});
        worker.on('failed', (job: any, err: Error) => {
          failedEvents.push({ jobId: job.id, message: err.message });
          clearTimeout(timeout);
          worker.close(true).then(resolve);
        });
      });

      await new Promise((r) => setTimeout(r, 500));
      const job = await queue.add('bad-job', { x: 1 });

      await done;

      expect(failedEvents).toHaveLength(1);
      expect(failedEvents[0].jobId).toBe(job.id);
      expect(failedEvents[0].message).toBe('processor exploded');

      const k = buildKeys(Q);
      const score = await cleanupClient.zscore(k.failed, job.id);
      expect(score).not.toBeNull();
      expect(String(await cleanupClient.hget(k.job(job.id), 'state'))).toBe('failed');

      await queue.close();
    }, 15000);
  });

  describe('processor returns undefined', () => {
    const Q = `ew-undef-${TS}`;

    afterAll(async () => {
      await flushQueue(cleanupClient, Q);
    });

    it('job completes with null returnvalue when processor returns undefined', async () => {
      const queue = new Queue(Q, { connection: CONNECTION });

      const done = new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
        const worker = new Worker(
          Q,
          async () => {
            return undefined;
          },
          { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
        );
        worker.on('error', () => {});
        worker.on('completed', () => {
          clearTimeout(timeout);
          worker.close(true).then(resolve);
        });
      });

      await new Promise((r) => setTimeout(r, 500));
      const job = await queue.add('undef-job', { x: 1 });

      await done;

      const k = buildKeys(Q);
      const rv = await cleanupClient.hget(k.job(job.id), 'returnvalue');
      expect(String(rv)).toBe('null');
      expect(String(await cleanupClient.hget(k.job(job.id), 'state'))).toBe('completed');

      await queue.close();
    }, 15000);
  });

  describe('very short blockTimeout', () => {
    const Q = `ew-shorttimeout-${TS}`;

    afterAll(async () => {
      await flushQueue(cleanupClient, Q);
    });

    it('does not error with blockTimeout=100 on an empty queue, and still processes when a job arrives', async () => {
      const queue = new Queue(Q, { connection: CONNECTION });
      const errors: Error[] = [];
      const completed: string[] = [];

      const worker = new Worker(
        Q,
        async (job: any) => {
          completed.push(job.id);
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 100, stalledInterval: 60000 },
      );
      worker.on('error', (err: Error) => errors.push(err));
      await worker.waitUntilReady();

      await new Promise((r) => setTimeout(r, 1000));
      expect(errors).toHaveLength(0);

      const job = await queue.add('late-job', { x: 1 });
      await new Promise((r) => setTimeout(r, 1500));

      expect(completed).toContain(job.id);
      expect(errors).toHaveLength(0);

      await worker.close(true);
      await queue.close();
    }, 15000);
  });

  describe('Multiple workers on same queue', () => {
    const Q = `ew-multi-${TS}`;

    afterAll(async () => {
      await flushQueue(cleanupClient, Q);
    });

    it('distributes jobs across workers with no duplicates', async () => {
      const queue = new Queue(Q, { connection: CONNECTION });
      const processedByWorker1: string[] = [];
      const processedByWorker2: string[] = [];

      const worker1 = new Worker(
        Q,
        async (job: any) => {
          processedByWorker1.push(job.id);
          await new Promise((r) => setTimeout(r, 100));
          return 'w1';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker1.on('error', () => {});

      const worker2 = new Worker(
        Q,
        async (job: any) => {
          processedByWorker2.push(job.id);
          await new Promise((r) => setTimeout(r, 100));
          return 'w2';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker2.on('error', () => {});

      await worker1.waitUntilReady();
      await worker2.waitUntilReady();

      const NUM_JOBS = 6;
      const jobIds: string[] = [];
      for (let i = 0; i < NUM_JOBS; i++) {
        const job = await queue.add(`multi-${i}`, { i });
        jobIds.push(job.id);
      }

      await new Promise((r) => setTimeout(r, 3000));

      const allProcessed = [...processedByWorker1, ...processedByWorker2];
      for (const id of jobIds) {
        expect(allProcessed).toContain(id);
      }
      expect(new Set(allProcessed).size).toBe(allProcessed.length);

      await worker1.close(true);
      await worker2.close(true);
      await queue.close();
    }, 15000);
  });

  describe('on empty queue', () => {
    const Q = `ew-empty-${TS}`;

    afterAll(async () => {
      await flushQueue(cleanupClient, Q);
    });

    it('does not error when no jobs are available', async () => {
      const errors: Error[] = [];

      const worker = new Worker(
        Q,
        async () => {
          return 'should not run';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker.on('error', (err: Error) => errors.push(err));
      await worker.waitUntilReady();

      await new Promise((r) => setTimeout(r, 2000));

      expect(errors).toHaveLength(0);

      await worker.close(true);
    }, 10000);
  });

  describe('slow processor', () => {
    const Q = `ew-slow-${TS}`;

    afterAll(async () => {
      await flushQueue(cleanupClient, Q);
    });

    it('completes a job even when the processor takes a while', async () => {
      const queue = new Queue(Q, { connection: CONNECTION });

      const done = new Promise<string>((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
        const worker = new Worker(
          Q,
          async () => {
            await new Promise((r) => setTimeout(r, 2000));
            return 'slow-result';
          },
          { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
        );
        worker.on('error', () => {});
        worker.on('completed', (job: any, result: any) => {
          clearTimeout(timeout);
          worker.close(true).then(() => resolve(result));
        });
      });

      await new Promise((r) => setTimeout(r, 500));
      const job = await queue.add('slow-job', { x: 1 });

      const result = await done;
      expect(result).toBe('slow-result');

      const k = buildKeys(Q);
      expect(String(await cleanupClient.hget(k.job(job.id), 'state'))).toBe('completed');
      expect(String(await cleanupClient.hget(k.job(job.id), 'returnvalue'))).toBe('"slow-result"');

      await queue.close();
    }, 15000);
  });

  describe('Stalled job recovery', () => {
    const Q = `ew-stalled-${TS}`;

    afterAll(async () => {
      await flushQueue(cleanupClient, Q);
    });

    it('reclaims a stalled job and moves it to failed after exceeding maxStalledCount', async () => {
      const queue = new Queue(Q, { connection: CONNECTION });
      const k = buildKeys(Q);

      let w1Started = false;
      const worker1 = new Worker(
        Q,
        async () => {
          w1Started = true;
          await new Promise((r) => setTimeout(r, 60000));
          return 'w1-done';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker1.on('error', () => {});
      await worker1.waitUntilReady();

      const job = await queue.add('stall-test', { recover: true });
      while (!w1Started) {
        await new Promise((r) => setTimeout(r, 50));
      }

      await worker1.close(true);

      const pending = (await cleanupClient.customCommand(['XPENDING', k.stream, CONSUMER_GROUP])) as any[];
      expect(Number(pending[0])).toBeGreaterThanOrEqual(1);

      const worker2 = new Worker(
        Q,
        async () => {
          return 'w2-done';
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 500,
          stalledInterval: 1000,
          maxStalledCount: 1,
        },
      );
      worker2.on('error', () => {});
      worker2.on('stalled', (jobId: string) => {});
      await worker2.waitUntilReady();

      await new Promise((r) => setTimeout(r, 3500));

      await worker2.close(true);

      const state = String(await cleanupClient.hget(k.job(job.id), 'state'));
      expect(state).toBe('failed');
      const failedReason = String(await cleanupClient.hget(k.job(job.id), 'failedReason'));
      expect(failedReason).toContain('stalled');

      const failedScore = await cleanupClient.zscore(k.failed, job.id);
      expect(failedScore).not.toBeNull();

      await queue.close();
    }, 20000);

    it('increments stalledCount on the job hash during reclaim', async () => {
      const Q2 = `ew-stalled-count-${TS}`;
      const queue = new Queue(Q2, { connection: CONNECTION });
      const k = buildKeys(Q2);

      let w1Started = false;
      const worker1 = new Worker(
        Q2,
        async () => {
          w1Started = true;
          await new Promise((r) => setTimeout(r, 60000));
          return 'never';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500, stalledInterval: 60000 },
      );
      worker1.on('error', () => {});
      await worker1.waitUntilReady();

      const job = await queue.add('count-stall', { x: 1 });
      while (!w1Started) {
        await new Promise((r) => setTimeout(r, 50));
      }
      await worker1.close(true);

      const worker2 = new Worker(
        Q2,
        async () => {
          return 'w2';
        },
        {
          connection: CONNECTION,
          concurrency: 1,
          blockTimeout: 500,
          stalledInterval: 1000,
          maxStalledCount: 10,
        },
      );
      worker2.on('error', () => {});
      await worker2.waitUntilReady();

      await new Promise((r) => setTimeout(r, 2000));

      await worker2.close(true);

      const stalledCount = await cleanupClient.hget(k.job(job.id), 'stalledCount');
      expect(Number(stalledCount)).toBeGreaterThanOrEqual(1);

      const state = String(await cleanupClient.hget(k.job(job.id), 'state'));
      expect(state).toBe('active');

      await queue.close();
      await flushQueue(cleanupClient, Q2);
    }, 15000);
  });
});
