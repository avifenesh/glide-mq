/**
 * LIFO (Last-In-First-Out) job processing order tests.
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');

describeEachMode('LIFO: Basic ordering', (CONNECTION) => {
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    cleanupClient.close();
  });

  it('processes 10 jobs in LIFO order (reverse of enqueue)', async () => {
    const Q = 'lifo-basic-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    const addedIds: string[] = [];
    for (let i = 0; i < 10; i++) {
      const job = await queue.add('lifo-job', { seq: i }, { lifo: true });
      expect(job).not.toBeNull();
      addedIds.push(job!.id);
    }

    const processed: string[] = [];
    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);
      const worker = new Worker(
        Q,
        async (job: any) => {
          processed.push(job.id);
          if (processed.length >= 10) {
            clearTimeout(timeout);
            setTimeout(() => worker.close(true).then(resolve), 200);
          }
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
      );
      worker.on('error', () => {});
    });

    await done;
    await queue.close();
    await flushQueue(cleanupClient, Q);

    expect(processed).toHaveLength(10);
    // LIFO: last added = first processed
    expect(processed).toEqual(addedIds.reverse());
  }, 20000);

  it('rejects lifo + ordering.key combination', async () => {
    const Q = 'lifo-ordering-reject-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    await expect(queue.add('bad', { x: 1 }, { lifo: true, ordering: { key: 'test' } })).rejects.toThrow(
      'lifo and ordering.key cannot be used together',
    );

    await queue.close();
    await flushQueue(cleanupClient, Q);
  });

  it('FIFO is still default (no lifo option)', async () => {
    const Q = 'lifo-fifo-default-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    const addedIds: string[] = [];
    for (let i = 0; i < 5; i++) {
      const job = await queue.add('fifo-job', { seq: i });
      addedIds.push(job!.id);
    }

    const processed: string[] = [];
    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker = new Worker(
        Q,
        async (job: any) => {
          processed.push(job.id);
          if (processed.length >= 5) {
            clearTimeout(timeout);
            setTimeout(() => worker.close(true).then(resolve), 200);
          }
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
      );
      worker.on('error', () => {});
    });

    await done;
    await queue.close();
    await flushQueue(cleanupClient, Q);

    expect(processed).toHaveLength(5);
    // FIFO: first added = first processed (NOT reversed)
    expect(processed).toEqual(addedIds);
  }, 15000);
});

describeEachMode('LIFO: Priority interactions', (CONNECTION) => {
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    cleanupClient.close();
  });

  it('priority takes precedence over LIFO', async () => {
    const Q = 'lifo-priority-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    // Add 3 LIFO jobs
    await queue.add('lifo1', { seq: 1 }, { lifo: true });
    await queue.add('lifo2', { seq: 2 }, { lifo: true });
    await queue.add('lifo3', { seq: 3 }, { lifo: true });

    // Add a priority job (should process first)
    const priorityJob = await queue.add('priority', { seq: 0 }, { priority: 1 });

    const processed: string[] = [];
    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker = new Worker(
        Q,
        async (job: any) => {
          processed.push(job.id);
          if (processed.length >= 4) {
            clearTimeout(timeout);
            setTimeout(() => worker.close(true).then(resolve), 200);
          }
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
      );
      worker.on('error', () => {});
    });

    await done;
    await queue.close();
    await flushQueue(cleanupClient, Q);

    expect(processed).toHaveLength(4);
    // Priority job should be first
    expect(processed[0]).toBe(priorityJob!.id);
  }, 15000);

  it('delayed LIFO jobs are promoted to LIFO list', async () => {
    const Q = 'lifo-delayed-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    // Add LIFO job with short delay
    const delayedJob = await queue.add('delayed-lifo', { seq: 1 }, { lifo: true, delay: 500 });

    // Add immediate LIFO job
    const immediateJob = await queue.add('immediate-lifo', { seq: 2 }, { lifo: true });

    // Wait for delayed job to be promoted
    await new Promise((r) => setTimeout(r, 700));

    const processed: string[] = [];
    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker = new Worker(
        Q,
        async (job: any) => {
          processed.push(job.id);
          if (processed.length >= 2) {
            clearTimeout(timeout);
            setTimeout(() => worker.close(true).then(resolve), 200);
          }
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
      );
      worker.on('error', () => {});
    });

    await done;
    await queue.close();
    await flushQueue(cleanupClient, Q);

    expect(processed).toHaveLength(2);
    // Delayed job promoted to LIFO list, should process before immediate job (both LIFO)
    expect(processed[0]).toBe(delayedJob!.id);
    expect(processed[1]).toBe(immediateJob!.id);
  }, 15000);
});

describeEachMode('LIFO: Mixed with FIFO', (CONNECTION) => {
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    cleanupClient.close();
  });

  it('LIFO jobs process before FIFO jobs', async () => {
    const Q = 'lifo-fifo-mix-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    // Add FIFO jobs first
    const fifo1 = await queue.add('fifo1', { seq: 1 });
    const fifo2 = await queue.add('fifo2', { seq: 2 });

    // Add LIFO jobs
    const lifo1 = await queue.add('lifo1', { seq: 3 }, { lifo: true });
    const lifo2 = await queue.add('lifo2', { seq: 4 }, { lifo: true });

    const processed: string[] = [];
    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker = new Worker(
        Q,
        async (job: any) => {
          processed.push(job.id);
          if (processed.length >= 4) {
            clearTimeout(timeout);
            setTimeout(() => worker.close(true).then(resolve), 200);
          }
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
      );
      worker.on('error', () => {});
    });

    await done;
    await queue.close();
    await flushQueue(cleanupClient, Q);

    expect(processed).toHaveLength(4);
    // LIFO jobs should process before FIFO jobs
    expect(processed[0]).toBe(lifo2!.id);
    expect(processed[1]).toBe(lifo1!.id);
    expect(processed[2]).toBe(fifo1!.id);
    expect(processed[3]).toBe(fifo2!.id);
  }, 15000);
});

describeEachMode('LIFO: Global concurrency enforcement', (CONNECTION) => {
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    cleanupClient.close();
  });

  it('globalConcurrency=1 prevents concurrent LIFO job processing across workers', async () => {
    const Q = 'lifo-gc-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });
    await queue.setGlobalConcurrency(1);

    // Add 4 LIFO jobs
    for (let i = 0; i < 4; i++) {
      await queue.add('job', { i }, { lifo: true });
    }

    let maxConcurrent = 0;
    let concurrent = 0;
    const processed: number[] = [];

    const makeWorker = () =>
      new Worker(
        Q,
        async (job: any) => {
          concurrent++;
          maxConcurrent = Math.max(maxConcurrent, concurrent);
          await new Promise((r) => setTimeout(r, 50));
          concurrent--;
          processed.push(job.data.i);
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 500 },
      );

    const w1 = makeWorker();
    const w2 = makeWorker();
    w1.on('error', () => {});
    w2.on('error', () => {});

    // Wait for all 4 jobs to complete
    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (processed.length >= 4) {
          clearInterval(check);
          resolve();
        }
      }, 100);
    });

    await w1.close(true);
    await w2.close(true);
    await queue.close();
    await flushQueue(cleanupClient, Q);

    expect(maxConcurrent).toBe(1);
    expect(processed).toHaveLength(4);
  }, 20000);
});
