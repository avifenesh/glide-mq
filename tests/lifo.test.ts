/**
 * LIFO (Last-In-First-Out) job processing order tests.
 */
import { it, expect, beforeAll, afterAll } from 'vitest';
import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

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

describeEachMode('LIFO: Scheduler template', (CONNECTION) => {
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    cleanupClient.close();
  });

  it('scheduler with lifo:true enqueues jobs to the LIFO list', async () => {
    const Q = 'lifo-scheduler-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    // Set up scheduler with lifo: true
    await queue.upsertJobScheduler('lifo-sched', { every: 100 }, { name: 'report', data: {}, opts: { lifo: true } });

    const processed: string[] = [];
    let completed = 0;

    const worker = new Worker(
      Q,
      async (job: any) => {
        processed.push(job.id);
        completed++;
        return 'ok';
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 500, promotionInterval: 200 },
    );
    worker.on('error', () => {});

    // Wait for 3 scheduler firings
    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (completed >= 3) {
          clearInterval(check);
          resolve();
        }
      }, 100);
    });

    await worker.close(true);

    // Verify jobs had lifo flag set (each processed job should be from LIFO path)
    const jobs = await Promise.all(processed.map((id) => queue.getJob(id)));
    for (const job of jobs) {
      expect(job?.opts).toMatchObject({ lifo: true });
    }

    await queue.removeJobScheduler('lifo-sched');
    await queue.close();
    await flushQueue(cleanupClient, Q);
  }, 15000);

  it('rejects scheduler template with lifo + ordering.key', async () => {
    const Q = 'lifo-sched-val-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    await expect(
      queue.upsertJobScheduler(
        'bad-sched',
        { every: 1000 },
        { name: 'job', data: {}, opts: { lifo: true, ordering: { key: 'grp' } } },
      ),
    ).rejects.toThrow('lifo and ordering.key cannot be used together');

    await queue.close();
    await flushQueue(cleanupClient, Q);
  }, 10000);
});

describeEachMode('LIFO: FlowProducer child jobs', (CONNECTION) => {
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    cleanupClient.close();
  });

  it('FlowProducer child jobs with lifo:true are processed in LIFO order', async () => {
    const { FlowProducer } = require('../dist/flow-producer') as typeof import('../src/flow-producer');
    const Q = 'lifo-flow-children-' + Date.now();
    const flow = new FlowProducer({ connection: CONNECTION });

    await flow.add({
      name: 'parent',
      queueName: Q,
      data: { step: 'root' },
      children: [
        { name: 'child-a', queueName: Q, data: { seq: 1 }, opts: { lifo: true } },
        { name: 'child-b', queueName: Q, data: { seq: 2 }, opts: { lifo: true } },
        { name: 'child-c', queueName: Q, data: { seq: 3 }, opts: { lifo: true } },
      ],
    });

    const processedNames: string[] = [];
    const worker = new Worker(
      Q,
      async (job: any) => {
        processedNames.push(job.name);
        return 'ok';
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
    );
    worker.on('error', () => {});

    // Wait for all 3 children + parent to complete
    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (processedNames.length >= 4) {
          clearInterval(check);
          resolve();
        }
      }, 100);
    });

    await worker.close(true);
    await flow.close();

    // Children should be processed LIFO: child-c first, then child-b, then child-a
    const childOrder = processedNames.filter((n) => n.startsWith('child-'));
    expect(childOrder[0]).toBe('child-c');
    expect(childOrder[1]).toBe('child-b');
    expect(childOrder[2]).toBe('child-a');

    const queue = new (require('../dist/queue') as any).Queue(Q, { connection: CONNECTION });
    await flushQueue(cleanupClient, Q);
    await queue.close();
  }, 20000);

  it('rejects FlowProducer child with lifo + ordering.key', async () => {
    const { FlowProducer } = require('../dist/flow-producer') as typeof import('../src/flow-producer');
    const Q = 'lifo-flow-val-' + Date.now();
    const flow = new FlowProducer({ connection: CONNECTION });

    await expect(
      flow.add({
        name: 'parent',
        queueName: Q,
        data: {},
        children: [{ name: 'child', queueName: Q, data: {}, opts: { lifo: true, ordering: { key: 'grp' } } }],
      }),
    ).rejects.toThrow('lifo and ordering.key cannot be used together');

    await flow.close();
    await flushQueue(cleanupClient, Q);
  }, 10000);
});

describeEachMode('LIFO: Global concurrency enforcement for priority jobs', (CONNECTION) => {
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    cleanupClient.close();
  });

  it('globalConcurrency=1 prevents concurrent priority job processing across workers', async () => {
    const Q = 'priority-gc-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });
    await queue.setGlobalConcurrency(1);

    // Add 4 priority jobs
    for (let i = 0; i < 4; i++) {
      await queue.add('job', { i }, { priority: 1 });
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

describeEachMode('LIFO: Batch pop throughput', (CONNECTION) => {
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    cleanupClient.close();
  });

  it('worker with concurrency=5 dispatches multiple LIFO jobs concurrently via rpopCount', async () => {
    const Q = 'lifo-batch-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });

    // Add 10 LIFO jobs with a fixed delay to confirm concurrency
    for (let i = 0; i < 10; i++) {
      await queue.add('job', { i }, { lifo: true });
    }

    let maxConcurrent = 0;
    let concurrent = 0;
    const processedIds: number[] = [];

    const worker = new Worker(
      Q,
      async (job: any) => {
        concurrent++;
        maxConcurrent = Math.max(maxConcurrent, concurrent);
        await new Promise((r) => setTimeout(r, 30));
        concurrent--;
        processedIds.push(job.data.i);
        return 'ok';
      },
      { connection: CONNECTION, concurrency: 5, blockTimeout: 500 },
    );
    worker.on('error', () => {});

    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (processedIds.length >= 10) {
          clearInterval(check);
          resolve();
        }
      }, 50);
    });

    await worker.close(true);
    await queue.close();
    await flushQueue(cleanupClient, Q);

    expect(processedIds).toHaveLength(10);
    // With concurrency=5 and 30ms processing time, multiple jobs must run in parallel
    // If sequential: 10 × 30ms = 300ms minimum. True concurrency should allow ≥2 concurrent.
    expect(maxConcurrent).toBeGreaterThanOrEqual(2);
    // Note: LIFO ordering under concurrency>1 is non-deterministic; we only verify throughput.
  }, 15000);
});

describeEachMode('LIFO: list-active counter on failure', (CONNECTION) => {
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    try {
      cleanupClient.close();
    } catch {
      /* ignore ClosingError if client was already closed */
    }
  });

  it('list-active counter is decremented when a LIFO job fails permanently', async () => {
    const Q = 'lifo-fail-counter-' + Date.now();
    const queue = new Queue(Q, { connection: CONNECTION });
    await queue.setGlobalConcurrency(1);

    // Add a LIFO job that will fail (1 attempt max)
    await queue.add('fail-job', { fail: true }, { lifo: true, attempts: 1 });

    let failedCount = 0;
    const w1 = new Worker(
      Q,
      async () => {
        throw new Error('intentional failure');
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 500 },
    );
    w1.on('failed', () => {
      failedCount++;
    });
    w1.on('error', () => {});

    // Wait for the job to fail permanently
    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (failedCount >= 1) {
          clearInterval(check);
          resolve();
        }
      }, 100);
    });
    await w1.close(true);

    // Now add a second LIFO job under gc=1.
    // If list-active was decremented correctly, a new worker can pop it.
    // If list-active is stuck at 1, rpopAndReserve returns null and the job never runs.
    await queue.add('second-job', { ok: true }, { lifo: true });

    let secondProcessed = false;
    const w2 = new Worker(
      Q,
      async () => {
        secondProcessed = true;
        return 'ok';
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 500 },
    );
    w2.on('error', () => {});

    await new Promise<void>((resolve, reject) => {
      const check = setInterval(() => {
        if (secondProcessed) {
          clearInterval(check);
          resolve();
        }
      }, 100);
      setTimeout(() => {
        clearInterval(check);
        reject(new Error('second job not processed - list-active counter may be stuck'));
      }, 5000);
    });

    await w2.close(true);
    await queue.close();
    await flushQueue(cleanupClient, Q);

    expect(secondProcessed).toBe(true);
  }, 15000);

  it('LIFO processes jobs in last-in-first-out order within a batch', async () => {
    const qName = 'lifo-batch-order-' + Date.now();
    const queue = new Queue(qName, { connection: CONNECTION });

    // Add 5 LIFO jobs sequentially so they have a defined insertion order
    for (let i = 0; i < 5; i++) {
      await queue.add('task', { seq: i }, { lifo: true });
    }

    const processedOrder: number[] = [];

    await new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);

      const worker = new Worker(
        qName,
        async (job: any) => {
          processedOrder.push(job.data.seq);
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1 },
      );

      worker.on('drained', async () => {
        if (processedOrder.length === 5) {
          clearTimeout(timeout);
          await worker.close(true);
          await queue.close();
          try {
            // Last added (seq=4) should be processed first in LIFO order
            expect(processedOrder[0]).toBe(4);
            expect(processedOrder[1]).toBe(3);
            expect(processedOrder[2]).toBe(2);
            expect(processedOrder[3]).toBe(1);
            expect(processedOrder[4]).toBe(0);
            resolve();
          } catch (e) {
            reject(e);
          }
        }
      });

      worker.on('error', (err: Error) => {
        reject(err);
      });
    });
  }, 15000);
});

describeEachMode('LIFO: Stalled list-sourced job recovery', (CONNECTION) => {
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
    // Ensure version 66 library is loaded
    const tmpQ = new Queue(`stall-lib-load-${Date.now()}`, { connection: CONNECTION });
    await tmpQ.close();
  });

  afterAll(async () => {
    cleanupClient.close();
  });

  it('reclaimStalledListJobs detects stalled LIFO job and moves to failed with list-active DECR', async () => {
    const Q = `stall-lifo-reclaim-${Date.now()}`;
    const k = buildKeys(Q);

    // Simulate a stalled list-sourced job (library already loaded in beforeAll): create job hash with state=active, lifo=1, stale lastActive
    const jobId = 'stalled-lifo-1';
    const staleTime = Date.now() - 60000; // 60s ago
    await cleanupClient.hset(k.job(jobId), {
      name: 'stalled-task',
      data: '{}',
      opts: '{}',
      state: 'active',
      lifo: '1',
      processedOn: staleTime.toString(),
      lastActive: staleTime.toString(),
      stalledCount: '0',
      attemptsMade: '0',
    });

    // Set list-active to 1 (simulating the INCR from rpopAndReserve)
    await cleanupClient.set(k.listActive, '1');

    // Call reclaimStalledListJobs directly via FCALL
    // minIdleMs=5000, maxStalledCount=1, timestamp=now
    const now = Date.now();
    const count = await cleanupClient.fcall(
      'glidemq_reclaimStalledListJobs',
      [k.stream, k.events],
      ['5000', '1', now.toString(), k.failed],
    );

    // First call: stalledCount goes from 0 to 1 (== maxStalledCount), job stays active (stalled event)
    expect(Number(count)).toBe(1);
    const state1 = await cleanupClient.hget(k.job(jobId), 'state');
    expect(state1).toBe('active');
    const sc1 = await cleanupClient.hget(k.job(jobId), 'stalledCount');
    expect(sc1).toBe('1');
    // list-active should NOT be decremented yet (job is re-activated, not failed)
    const la1 = await cleanupClient.get(k.listActive);
    expect(la1).toBe('1');

    // Second call: stalledCount goes from 1 to 2 (> maxStalledCount), job moves to failed
    const count2 = await cleanupClient.fcall(
      'glidemq_reclaimStalledListJobs',
      [k.stream, k.events],
      ['5000', '1', (now + 1).toString(), k.failed],
    );
    expect(Number(count2)).toBe(1);
    const state2 = await cleanupClient.hget(k.job(jobId), 'state');
    expect(state2).toBe('failed');
    const reason = await cleanupClient.hget(k.job(jobId), 'failedReason');
    expect(reason).toBe('job stalled more than maxStalledCount');

    // list-active should be decremented to 0
    const la2 = await cleanupClient.get(k.listActive);
    expect(Number(la2)).toBe(0);

    // Job should be in the failed ZSet
    const failedScore = await cleanupClient.zscore(k.failed, jobId);
    expect(failedScore).not.toBeNull();

    await flushQueue(cleanupClient, Q);
  });

  it('reclaimStalledListJobs detects stalled priority-sourced job and moves to failed with list-active DECR', async () => {
    const Q = `stall-priority-reclaim-${Date.now()}`;
    const k = buildKeys(Q);

    // Simulate a stalled priority-sourced job: state=active, priority='5', NO lifo field, stale lastActive
    const jobId = 'stalled-priority-1';
    const staleTime = Date.now() - 60000; // 60s ago
    await cleanupClient.hset(k.job(jobId), {
      name: 'priority-task',
      data: '{}',
      opts: '{}',
      state: 'active',
      priority: '5',
      processedOn: staleTime.toString(),
      lastActive: staleTime.toString(),
      stalledCount: '0',
      attemptsMade: '0',
    });

    // Set list-active to 1 (simulating the INCR from rpopAndReserve)
    await cleanupClient.set(k.listActive, '1');

    const now = Date.now();

    // First call: stalledCount 0 -> 1 (== maxStalledCount), job stays active (stalled event)
    const count1 = await cleanupClient.fcall(
      'glidemq_reclaimStalledListJobs',
      [k.stream, k.events],
      ['5000', '1', now.toString(), k.failed],
    );
    expect(Number(count1)).toBe(1);
    const state1 = await cleanupClient.hget(k.job(jobId), 'state');
    expect(state1).toBe('active');
    const sc1 = await cleanupClient.hget(k.job(jobId), 'stalledCount');
    expect(sc1).toBe('1');
    // list-active should NOT be decremented yet
    const la1 = await cleanupClient.get(k.listActive);
    expect(la1).toBe('1');

    // Second call: stalledCount 1 -> 2 (> maxStalledCount), job moves to failed
    const count2 = await cleanupClient.fcall(
      'glidemq_reclaimStalledListJobs',
      [k.stream, k.events],
      ['5000', '1', (now + 1).toString(), k.failed],
    );
    expect(Number(count2)).toBe(1);
    const state2 = await cleanupClient.hget(k.job(jobId), 'state');
    expect(state2).toBe('failed');
    const reason = await cleanupClient.hget(k.job(jobId), 'failedReason');
    expect(reason).toBe('job stalled more than maxStalledCount');

    // list-active should be decremented to 0
    const la2 = await cleanupClient.get(k.listActive);
    expect(Number(la2)).toBe(0);

    // Job should be in the failed ZSet
    const failedScore = await cleanupClient.zscore(k.failed, jobId);
    expect(failedScore).not.toBeNull();

    await flushQueue(cleanupClient, Q);
  });

  it('deferActive DECRs list-active when entryId is empty (list-sourced job)', async () => {
    const Q = `defer-active-decr-${Date.now()}`;
    const k = buildKeys(Q);

    const jobId = 'defer-lifo-1';
    await cleanupClient.hset(k.job(jobId), {
      name: 'defer-task',
      data: '{}',
      opts: '{}',
      state: 'active',
      lifo: '1',
    });

    // Set list-active to 1
    await cleanupClient.set(k.listActive, '1');

    // Call glidemq_deferActive with entryId='' (list-sourced job, no stream entry)
    await cleanupClient.fcall('glidemq_deferActive', [k.stream, k.job(jobId), k.listActive], [jobId, '', 'workers']);

    // list-active should be decremented to 0
    const la = await cleanupClient.get(k.listActive);
    expect(Number(la)).toBe(0);

    // Job should be moved back to waiting
    const state = await cleanupClient.hget(k.job(jobId), 'state');
    expect(state).toBe('waiting');

    await flushQueue(cleanupClient, Q);
  });

  it('reclaimStalledListJobs does not touch non-stalled list jobs', async () => {
    const Q = `stall-lifo-notouch-${Date.now()}`;
    const k = buildKeys(Q);

    // Create a fresh active list-sourced job (lastActive is recent)
    const jobId = 'fresh-lifo-1';
    const recentTime = Date.now();
    await cleanupClient.hset(k.job(jobId), {
      name: 'fresh-task',
      data: '{}',
      opts: '{}',
      state: 'active',
      lifo: '1',
      processedOn: recentTime.toString(),
      lastActive: recentTime.toString(),
      stalledCount: '0',
      attemptsMade: '0',
    });

    await cleanupClient.set(k.listActive, '1');

    // Call with minIdleMs=30000 - job is fresh, should not be touched
    const count = await cleanupClient.fcall(
      'glidemq_reclaimStalledListJobs',
      [k.stream, k.events],
      ['30000', '1', recentTime.toString(), k.failed],
    );

    expect(Number(count)).toBe(0);
    const state = await cleanupClient.hget(k.job(jobId), 'state');
    expect(state).toBe('active');
    const la = await cleanupClient.get(k.listActive);
    expect(la).toBe('1');

    await flushQueue(cleanupClient, Q);
  });
});

describeEachMode('LIFO: rpopAndReserve batch', (CONNECTION) => {
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
    // Ensure function library is loaded
    const tmpQ = new Queue(`rpop-batch-lib-load-${Date.now()}`, { connection: CONNECTION });
    await tmpQ.close();
  });

  afterAll(async () => {
    cleanupClient.close();
  });

  it('rpopAndReserve pops multiple LIFO jobs in a single FCALL when count > 1', async () => {
    const Q = `rpop-batch-${Date.now()}`;
    const k = buildKeys(Q);
    const queue = new Queue(Q, { connection: CONNECTION });

    // Set globalConcurrency=5 so the function does not clamp below 3
    await queue.setGlobalConcurrency(5);

    // Push 3 LIFO jobs
    const j1 = await queue.add('batch1', { seq: 1 }, { lifo: true });
    const j2 = await queue.add('batch2', { seq: 2 }, { lifo: true });
    const j3 = await queue.add('batch3', { seq: 3 }, { lifo: true });

    // Call rpopAndReserve with count=3
    const result = await cleanupClient.fcall(
      'glidemq_rpopAndReserve',
      [k.meta, k.stream, k.listActive, k.lifo],
      ['workers', '3'],
    );

    expect(Array.isArray(result)).toBe(true);
    expect(result).toHaveLength(3);

    // All 3 job IDs should be returned (LIFO order: j3, j2, j1)
    const ids = result.map((v: any) => String(v));
    expect(ids).toContain(j1!.id);
    expect(ids).toContain(j2!.id);
    expect(ids).toContain(j3!.id);

    // list-active should be incremented to 3
    const la = await cleanupClient.get(k.listActive);
    expect(Number(la)).toBe(3);

    await queue.close();
    await flushQueue(cleanupClient, Q);
  });

  it('rpopAndReserve clamps to globalConcurrency minus active', async () => {
    const Q = `rpop-gc-clamp-${Date.now()}`;
    const k = buildKeys(Q);
    const queue = new Queue(Q, { connection: CONNECTION });

    // Set globalConcurrency=2
    await queue.setGlobalConcurrency(2);

    // Push 3 LIFO jobs
    await queue.add('clamp1', { seq: 1 }, { lifo: true });
    await queue.add('clamp2', { seq: 2 }, { lifo: true });
    await queue.add('clamp3', { seq: 3 }, { lifo: true });

    // Call rpopAndReserve with count=5 (more than gc allows)
    const result = await cleanupClient.fcall(
      'glidemq_rpopAndReserve',
      [k.meta, k.stream, k.listActive, k.lifo],
      ['workers', '5'],
    );

    expect(Array.isArray(result)).toBe(true);
    // gc=2, no pending stream jobs, no list-active -> available=2, so only 2 returned
    expect(result).toHaveLength(2);

    // list-active should be 2
    const la = await cleanupClient.get(k.listActive);
    expect(Number(la)).toBe(2);

    await queue.close();
    await flushQueue(cleanupClient, Q);
  });
});
