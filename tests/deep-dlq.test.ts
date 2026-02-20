/**
 * Deep tests: Dead Letter Queue (DLQ) functionality
 * Requires: valkey-server on localhost:6379 and cluster on :7000-7005
 *
 * Run: npx vitest run tests/deep-dlq.test.ts
 */
import { it, expect, beforeAll, afterAll, afterEach } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { Job } = require('../dist/job') as typeof import('../src/job');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

function uid() {
  return `dlq-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
}

describeEachMode('Dead Letter Queue', (CONNECTION) => {
  let cleanupClient: any;
  let queueName: string;
  let dlqName: string;
  let queue: InstanceType<typeof Queue>;
  let dlqQueue: InstanceType<typeof Queue>;
  let worker: InstanceType<typeof Worker>;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterEach(async () => {
    if (worker) {
      try {
        await worker.close(true);
      } catch {}
    }
    if (queue) {
      try {
        await queue.close();
      } catch {}
    }
    if (dlqQueue) {
      try {
        await dlqQueue.close();
      } catch {}
    }
    if (queueName) await flushQueue(cleanupClient, queueName);
    if (dlqName) await flushQueue(cleanupClient, dlqName);
  });

  afterAll(async () => {
    cleanupClient.close();
  });

  it('moves job to DLQ after exhausting retries', async () => {
    queueName = uid();
    dlqName = `${queueName}-dlq`;
    queue = new Queue(queueName, {
      connection: CONNECTION,
      deadLetterQueue: { name: dlqName },
    });

    const job = await queue.add(
      'fail-task',
      { value: 42 },
      { attempts: 2, backoff: { type: 'fixed', delay: 50 } },
    );

    const failedPromise = new Promise<void>((resolve) => {
      let failCount = 0;
      worker = new Worker(
        queueName,
        async () => {
          throw new Error('intentional failure');
        },
        {
          connection: CONNECTION,
          deadLetterQueue: { name: dlqName },
          stalledInterval: 60000,
        },
      );
      worker.on('failed', () => {
        failCount++;
        if (failCount >= 2) {
          setTimeout(resolve, 200);
        }
      });
    });

    await failedPromise;

    dlqQueue = new Queue(dlqName, { connection: CONNECTION });
    const dlqJobs = await dlqQueue.getJobs('waiting');
    expect(dlqJobs.length).toBeGreaterThanOrEqual(1);
    const dlqJob = dlqJobs[0];
    const dlqData = dlqJob.data as any;
    expect(dlqData.originalQueue).toBe(queueName);
    expect(dlqData.originalJobId).toBe(job!.id);
    expect(dlqData.failedReason).toBe('intentional failure');
  });

  it('DLQ job preserves original data payload', async () => {
    queueName = uid();
    dlqName = `${queueName}-dlq`;
    queue = new Queue(queueName, {
      connection: CONNECTION,
      deadLetterQueue: { name: dlqName },
    });

    const originalData = { complex: { nested: true }, array: [1, 2, 3] };
    await queue.add('data-test', originalData, { attempts: 1 });

    const failedPromise = new Promise<void>((resolve) => {
      worker = new Worker(
        queueName,
        async () => {
          throw new Error('data preservation test');
        },
        {
          connection: CONNECTION,
          deadLetterQueue: { name: dlqName },
          stalledInterval: 60000,
        },
      );
      worker.on('failed', () => setTimeout(resolve, 200));
    });

    await failedPromise;

    dlqQueue = new Queue(dlqName, { connection: CONNECTION });
    const dlqJobs = await dlqQueue.getJobs('waiting');
    expect(dlqJobs.length).toBeGreaterThanOrEqual(1);
    const dlqData = dlqJobs[0].data as any;
    expect(dlqData.data).toEqual(originalData);
  });

  it('DLQ job records attemptsMade count', async () => {
    queueName = uid();
    dlqName = `${queueName}-dlq`;
    queue = new Queue(queueName, {
      connection: CONNECTION,
      deadLetterQueue: { name: dlqName },
    });

    await queue.add(
      'attempts-track',
      { x: 1 },
      { attempts: 3, backoff: { type: 'fixed', delay: 50 } },
    );

    const failedPromise = new Promise<void>((resolve) => {
      let failCount = 0;
      worker = new Worker(
        queueName,
        async () => {
          throw new Error('attempt tracking');
        },
        {
          connection: CONNECTION,
          deadLetterQueue: { name: dlqName },
          stalledInterval: 60000,
        },
      );
      worker.on('failed', () => {
        failCount++;
        if (failCount >= 3) setTimeout(resolve, 200);
      });
    });

    await failedPromise;

    dlqQueue = new Queue(dlqName, { connection: CONNECTION });
    const dlqJobs = await dlqQueue.getJobs('waiting');
    expect(dlqJobs.length).toBeGreaterThanOrEqual(1);
    const dlqData = dlqJobs[0].data as any;
    expect(dlqData.attemptsMade).toBeGreaterThanOrEqual(2);
  });

  it('does not move to DLQ if retries not exhausted', async () => {
    queueName = uid();
    dlqName = `${queueName}-dlq`;
    queue = new Queue(queueName, {
      connection: CONNECTION,
      deadLetterQueue: { name: dlqName },
    });

    let callCount = 0;
    await queue.add(
      'retry-succeed',
      { x: 1 },
      { attempts: 3, backoff: { type: 'fixed', delay: 50 } },
    );

    const completedPromise = new Promise<void>((resolve) => {
      worker = new Worker(
        queueName,
        async () => {
          callCount++;
          if (callCount < 2) throw new Error('fail once');
          return 'ok';
        },
        {
          connection: CONNECTION,
          deadLetterQueue: { name: dlqName },
          stalledInterval: 60000,
        },
      );
      worker.on('completed', () => resolve());
    });

    await completedPromise;
    await new Promise((r) => setTimeout(r, 100));

    dlqQueue = new Queue(dlqName, { connection: CONNECTION });
    const dlqJobs = await dlqQueue.getJobs('waiting');
    expect(dlqJobs.length).toBe(0);
  });

  it('no DLQ movement when deadLetterQueue not configured', async () => {
    queueName = uid();
    dlqName = `${queueName}-dlq`;
    queue = new Queue(queueName, {
      connection: CONNECTION,
    });

    await queue.add('no-dlq', { x: 1 }, { attempts: 1 });

    const failedPromise = new Promise<void>((resolve) => {
      worker = new Worker(
        queueName,
        async () => {
          throw new Error('no dlq configured');
        },
        {
          connection: CONNECTION,
          stalledInterval: 60000,
        },
      );
      worker.on('failed', () => setTimeout(resolve, 200));
    });

    await failedPromise;

    dlqQueue = new Queue(dlqName, { connection: CONNECTION });
    const dlqK = buildKeys(dlqName);
    const streamLen = await cleanupClient.xlen(dlqK.stream);
    expect(streamLen).toBe(0);
  });

  it('getDeadLetterJobs returns empty when no DLQ configured', async () => {
    queueName = uid();
    dlqName = `${queueName}-dlq`;
    queue = new Queue(queueName, { connection: CONNECTION });
    const dlqJobs = await queue.getDeadLetterJobs();
    expect(dlqJobs).toEqual([]);
  });

  it('getDeadLetterJobs returns DLQ jobs', async () => {
    queueName = uid();
    dlqName = `${queueName}-dlq`;
    queue = new Queue(queueName, {
      connection: CONNECTION,
      deadLetterQueue: { name: dlqName },
    });

    await queue.add('dlq-get', { v: 1 }, { attempts: 1 });
    await queue.add('dlq-get', { v: 2 }, { attempts: 1 });

    let failCount = 0;
    const allFailed = new Promise<void>((resolve) => {
      worker = new Worker(
        queueName,
        async () => {
          throw new Error('dlq get test');
        },
        {
          connection: CONNECTION,
          deadLetterQueue: { name: dlqName },
          stalledInterval: 60000,
        },
      );
      worker.on('failed', () => {
        failCount++;
        if (failCount >= 2) setTimeout(resolve, 200);
      });
    });

    await allFailed;

    const dlqJobs = await queue.getDeadLetterJobs();
    expect(dlqJobs.length).toBe(2);
  });

  it('getDeadLetterJobs supports pagination (start/end)', async () => {
    queueName = uid();
    dlqName = `${queueName}-dlq`;
    queue = new Queue(queueName, {
      connection: CONNECTION,
      deadLetterQueue: { name: dlqName },
    });

    for (let i = 0; i < 3; i++) {
      await queue.add('dlq-page', { i }, { attempts: 1 });
    }

    let failCount = 0;
    const allFailed = new Promise<void>((resolve) => {
      worker = new Worker(
        queueName,
        async () => {
          throw new Error('pagination test');
        },
        {
          connection: CONNECTION,
          deadLetterQueue: { name: dlqName },
          stalledInterval: 60000,
        },
      );
      worker.on('failed', () => {
        failCount++;
        if (failCount >= 3) setTimeout(resolve, 200);
      });
    });

    await allFailed;

    const page = await queue.getDeadLetterJobs(0, 1);
    expect(page.length).toBe(2);
  });

  it('multiple jobs go to same DLQ', async () => {
    queueName = uid();
    dlqName = `${queueName}-dlq`;
    queue = new Queue(queueName, {
      connection: CONNECTION,
      deadLetterQueue: { name: dlqName },
    });

    for (let i = 0; i < 5; i++) {
      await queue.add('multi-fail', { idx: i }, { attempts: 1 });
    }

    let failCount = 0;
    const allFailed = new Promise<void>((resolve) => {
      worker = new Worker(
        queueName,
        async () => {
          throw new Error('multi fail');
        },
        {
          connection: CONNECTION,
          deadLetterQueue: { name: dlqName },
          stalledInterval: 60000,
        },
      );
      worker.on('failed', () => {
        failCount++;
        if (failCount >= 5) setTimeout(resolve, 200);
      });
    });

    await allFailed;

    dlqQueue = new Queue(dlqName, { connection: CONNECTION });
    const dlqJobs = await dlqQueue.getJobs('waiting');
    expect(dlqJobs.length).toBe(5);
  });

  it('DLQ job has correct name from original job', async () => {
    queueName = uid();
    dlqName = `${queueName}-dlq`;
    queue = new Queue(queueName, {
      connection: CONNECTION,
      deadLetterQueue: { name: dlqName },
    });

    await queue.add('special-name', { x: 1 }, { attempts: 1 });

    const failedPromise = new Promise<void>((resolve) => {
      worker = new Worker(
        queueName,
        async () => {
          throw new Error('name test');
        },
        {
          connection: CONNECTION,
          deadLetterQueue: { name: dlqName },
          stalledInterval: 60000,
        },
      );
      worker.on('failed', () => setTimeout(resolve, 200));
    });

    await failedPromise;

    dlqQueue = new Queue(dlqName, { connection: CONNECTION });
    const dlqJobs = await dlqQueue.getJobs('waiting');
    expect(dlqJobs.length).toBeGreaterThanOrEqual(1);
    expect(dlqJobs[0].name).toBe('special-name');
  });

  it('original job is in failed state after DLQ move', async () => {
    queueName = uid();
    dlqName = `${queueName}-dlq`;
    queue = new Queue(queueName, {
      connection: CONNECTION,
      deadLetterQueue: { name: dlqName },
    });

    const job = await queue.add('state-check', { x: 1 }, { attempts: 1 });

    const failedPromise = new Promise<void>((resolve) => {
      worker = new Worker(
        queueName,
        async () => {
          throw new Error('state check');
        },
        {
          connection: CONNECTION,
          deadLetterQueue: { name: dlqName },
          stalledInterval: 60000,
        },
      );
      worker.on('failed', () => setTimeout(resolve, 200));
    });

    await failedPromise;

    const fetched = await queue.getJob(job!.id);
    expect(fetched).not.toBeNull();
    const state = await fetched!.getState();
    expect(state).toBe('failed');
  });

  it('DLQ job data includes failedReason string', async () => {
    queueName = uid();
    dlqName = `${queueName}-dlq`;
    queue = new Queue(queueName, {
      connection: CONNECTION,
      deadLetterQueue: { name: dlqName },
    });

    await queue.add('reason-check', { x: 1 }, { attempts: 1 });

    const failedPromise = new Promise<void>((resolve) => {
      worker = new Worker(
        queueName,
        async () => {
          throw new Error('specific failure reason XYZ');
        },
        {
          connection: CONNECTION,
          deadLetterQueue: { name: dlqName },
          stalledInterval: 60000,
        },
      );
      worker.on('failed', () => setTimeout(resolve, 200));
    });

    await failedPromise;

    dlqQueue = new Queue(dlqName, { connection: CONNECTION });
    const dlqJobs = await dlqQueue.getJobs('waiting');
    const dlqData = dlqJobs[0].data as any;
    expect(dlqData.failedReason).toBe('specific failure reason XYZ');
  });

  it('DLQ works with delayed jobs that fail after promotion', async () => {
    queueName = uid();
    dlqName = `${queueName}-dlq`;
    queue = new Queue(queueName, {
      connection: CONNECTION,
      deadLetterQueue: { name: dlqName },
    });

    await queue.add('delayed-dlq', { x: 1 }, { delay: 100, attempts: 1 });

    const failedPromise = new Promise<void>((resolve) => {
      worker = new Worker(
        queueName,
        async () => {
          throw new Error('delayed fail');
        },
        {
          connection: CONNECTION,
          deadLetterQueue: { name: dlqName },
          promotionInterval: 100,
          stalledInterval: 60000,
        },
      );
      worker.on('failed', () => setTimeout(resolve, 300));
    });

    await failedPromise;

    dlqQueue = new Queue(dlqName, { connection: CONNECTION });
    const dlqJobs = await dlqQueue.getJobs('waiting');
    expect(dlqJobs.length).toBeGreaterThanOrEqual(1);
    const dlqData = dlqJobs[0].data as any;
    expect(dlqData.failedReason).toBe('delayed fail');
  });

  it('DLQ works with backoff: all retries exhausted then moved', async () => {
    queueName = uid();
    dlqName = `${queueName}-dlq`;
    queue = new Queue(queueName, {
      connection: CONNECTION,
      deadLetterQueue: { name: dlqName },
    });

    await queue.add(
      'backoff-dlq',
      { x: 1 },
      {
        attempts: 3,
        backoff: { type: 'fixed', delay: 50 },
      },
    );

    let failCount = 0;
    const allFailed = new Promise<void>((resolve) => {
      worker = new Worker(
        queueName,
        async () => {
          throw new Error('backoff exhaust');
        },
        {
          connection: CONNECTION,
          deadLetterQueue: { name: dlqName },
          promotionInterval: 50,
          stalledInterval: 60000,
        },
      );
      worker.on('failed', () => {
        failCount++;
        if (failCount >= 3) setTimeout(resolve, 200);
      });
    });

    await allFailed;

    dlqQueue = new Queue(dlqName, { connection: CONNECTION });
    const dlqJobs = await dlqQueue.getJobs('waiting');
    expect(dlqJobs.length).toBeGreaterThanOrEqual(1);
  });

  it('failed event emits for job that goes to DLQ', async () => {
    queueName = uid();
    dlqName = `${queueName}-dlq`;
    queue = new Queue(queueName, {
      connection: CONNECTION,
      deadLetterQueue: { name: dlqName },
    });

    await queue.add('event-check', { x: 1 }, { attempts: 1 });

    const failedJobs: any[] = [];
    const done = new Promise<void>((resolve) => {
      worker = new Worker(
        queueName,
        async () => {
          throw new Error('event test');
        },
        {
          connection: CONNECTION,
          deadLetterQueue: { name: dlqName },
          stalledInterval: 60000,
        },
      );
      worker.on('failed', (job: any, err: Error) => {
        failedJobs.push({ job, err });
        setTimeout(resolve, 200);
      });
    });

    await done;

    expect(failedJobs.length).toBe(1);
    expect(failedJobs[0].err.message).toBe('event test');
  });

  it('job counts show failed count after DLQ move', async () => {
    queueName = uid();
    dlqName = `${queueName}-dlq`;
    queue = new Queue(queueName, {
      connection: CONNECTION,
      deadLetterQueue: { name: dlqName },
    });

    await queue.add('counts-check', { x: 1 }, { attempts: 1 });

    const failedPromise = new Promise<void>((resolve) => {
      worker = new Worker(
        queueName,
        async () => {
          throw new Error('counts');
        },
        {
          connection: CONNECTION,
          deadLetterQueue: { name: dlqName },
          stalledInterval: 60000,
        },
      );
      worker.on('failed', () => setTimeout(resolve, 200));
    });

    await failedPromise;

    const counts = await queue.getJobCounts();
    expect(counts.failed).toBeGreaterThanOrEqual(1);
  });

  it('DLQ job originalQueue field is correct', async () => {
    queueName = uid();
    dlqName = `${queueName}-dlq`;
    queue = new Queue(queueName, {
      connection: CONNECTION,
      deadLetterQueue: { name: dlqName },
    });

    await queue.add('origin-check', { x: 1 }, { attempts: 1 });

    const failedPromise = new Promise<void>((resolve) => {
      worker = new Worker(
        queueName,
        async () => {
          throw new Error('origin');
        },
        {
          connection: CONNECTION,
          deadLetterQueue: { name: dlqName },
          stalledInterval: 60000,
        },
      );
      worker.on('failed', () => setTimeout(resolve, 200));
    });

    await failedPromise;

    dlqQueue = new Queue(dlqName, { connection: CONNECTION });
    const dlqJobs = await dlqQueue.getJobs('waiting');
    expect(dlqJobs.length).toBeGreaterThanOrEqual(1);
    const dlqData = dlqJobs[0].data as any;
    expect(dlqData.originalQueue).toBe(queueName);
  });
});
