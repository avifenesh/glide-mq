/**
 * Deep tests: Dead Letter Queue (DLQ) functionality
 * Requires: valkey-server running on localhost:6379
 *
 * Run: npx vitest run tests/deep-dlq.test.ts
 */
import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest';

const { GlideClient } = require('speedkey') as typeof import('speedkey');
const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { Job } = require('../dist/job') as typeof import('../src/job');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
const { LIBRARY_SOURCE, CONSUMER_GROUP } = require('../dist/functions/index') as typeof import('../src/functions/index');
const { ensureFunctionLibrary } = require('../dist/connection') as typeof import('../src/connection');

const CONNECTION = {
  addresses: [{ host: 'localhost', port: 6379 }],
};

let cleanupClient: InstanceType<typeof GlideClient>;

async function flushQueue(queueName: string, prefix = 'glide') {
  const k = buildKeys(queueName, prefix);
  const keysToDelete = [
    k.id, k.stream, k.scheduled, k.completed, k.failed,
    k.events, k.meta, k.dedup, k.rate, k.schedulers,
  ];
  for (const key of keysToDelete) {
    try { await cleanupClient.del([key]); } catch {}
  }
  const pfx = `${prefix}:{${queueName}}:`;
  for (const pattern of [`${pfx}job:*`, `${pfx}log:*`, `${pfx}deps:*`]) {
    let cursor = '0';
    do {
      const result = await cleanupClient.scan(cursor, { match: pattern, count: 100 });
      cursor = result[0] as string;
      const keys = result[1] as string[];
      if (keys.length > 0) await cleanupClient.del(keys);
    } while (cursor !== '0');
  }
}

function uid() {
  return `dlq-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
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

describe('Dead Letter Queue', () => {
  let queueName: string;
  let dlqName: string;
  let queue: InstanceType<typeof Queue>;
  let dlqQueue: InstanceType<typeof Queue>;
  let worker: InstanceType<typeof Worker>;

  afterEach(async () => {
    if (worker) {
      try { await worker.close(true); } catch {}
    }
    if (queue) {
      try { await queue.close(); } catch {}
    }
    if (dlqQueue) {
      try { await dlqQueue.close(); } catch {}
    }
    if (queueName) await flushQueue(queueName);
    if (dlqName) await flushQueue(dlqName);
  });

  it('moves job to DLQ after exhausting retries', async () => {
    queueName = uid();
    dlqName = `${queueName}-dlq`;
    queue = new Queue(queueName, {
      connection: CONNECTION,
      deadLetterQueue: { name: dlqName },
    });

    const job = await queue.add('fail-task', { value: 42 }, { attempts: 2, backoff: { type: 'fixed', delay: 50 } });

    const failedPromise = new Promise<void>((resolve) => {
      let failCount = 0;
      worker = new Worker(queueName, async () => {
        throw new Error('intentional failure');
      }, {
        connection: CONNECTION,
        deadLetterQueue: { name: dlqName },
        stalledInterval: 60000,
      });
      worker.on('failed', () => {
        failCount++;
        if (failCount >= 2) {
          setTimeout(resolve, 200);
        }
      });
    });

    await failedPromise;

    // Check the DLQ has the job
    dlqQueue = new Queue(dlqName, { connection: CONNECTION });
    const dlqJobs = await dlqQueue.getJobs('waiting');
    expect(dlqJobs.length).toBeGreaterThanOrEqual(1);
    const dlqJob = dlqJobs[0];
    // data is already parsed by Job.fromHash
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
      worker = new Worker(queueName, async () => {
        throw new Error('data preservation test');
      }, {
        connection: CONNECTION,
        deadLetterQueue: { name: dlqName },
        stalledInterval: 60000,
      });
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

    await queue.add('attempts-track', { x: 1 }, { attempts: 3, backoff: { type: 'fixed', delay: 50 } });

    const failedPromise = new Promise<void>((resolve) => {
      let failCount = 0;
      worker = new Worker(queueName, async () => {
        throw new Error('attempt tracking');
      }, {
        connection: CONNECTION,
        deadLetterQueue: { name: dlqName },
        stalledInterval: 60000,
      });
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
    // attemptsMade should be the number of attempts the worker saw (at least 2 since the last fail increments it)
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
    await queue.add('retry-succeed', { x: 1 }, { attempts: 3, backoff: { type: 'fixed', delay: 50 } });

    const completedPromise = new Promise<void>((resolve) => {
      worker = new Worker(queueName, async () => {
        callCount++;
        if (callCount < 2) throw new Error('fail once');
        return 'ok';
      }, {
        connection: CONNECTION,
        deadLetterQueue: { name: dlqName },
        stalledInterval: 60000,
      });
      worker.on('completed', () => resolve());
    });

    await completedPromise;
    await new Promise(r => setTimeout(r, 100));

    dlqQueue = new Queue(dlqName, { connection: CONNECTION });
    const dlqJobs = await dlqQueue.getJobs('waiting');
    expect(dlqJobs.length).toBe(0);
  });

  it('no DLQ movement when deadLetterQueue not configured', async () => {
    queueName = uid();
    dlqName = `${queueName}-dlq`;
    queue = new Queue(queueName, {
      connection: CONNECTION,
      // no deadLetterQueue
    });

    await queue.add('no-dlq', { x: 1 }, { attempts: 1 });

    const failedPromise = new Promise<void>((resolve) => {
      worker = new Worker(queueName, async () => {
        throw new Error('no dlq configured');
      }, {
        connection: CONNECTION,
        // no deadLetterQueue
        stalledInterval: 60000,
      });
      worker.on('failed', () => setTimeout(resolve, 200));
    });

    await failedPromise;

    // Verify that the DLQ stream does not exist
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
      worker = new Worker(queueName, async () => {
        throw new Error('dlq get test');
      }, {
        connection: CONNECTION,
        deadLetterQueue: { name: dlqName },
        stalledInterval: 60000,
      });
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

    // Add 3 jobs that will all fail
    for (let i = 0; i < 3; i++) {
      await queue.add('dlq-page', { i }, { attempts: 1 });
    }

    let failCount = 0;
    const allFailed = new Promise<void>((resolve) => {
      worker = new Worker(queueName, async () => {
        throw new Error('pagination test');
      }, {
        connection: CONNECTION,
        deadLetterQueue: { name: dlqName },
        stalledInterval: 60000,
      });
      worker.on('failed', () => {
        failCount++;
        if (failCount >= 3) setTimeout(resolve, 200);
      });
    });

    await allFailed;

    // Get only first 2
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
      worker = new Worker(queueName, async () => {
        throw new Error('multi fail');
      }, {
        connection: CONNECTION,
        deadLetterQueue: { name: dlqName },
        stalledInterval: 60000,
      });
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
      worker = new Worker(queueName, async () => {
        throw new Error('name test');
      }, {
        connection: CONNECTION,
        deadLetterQueue: { name: dlqName },
        stalledInterval: 60000,
      });
      worker.on('failed', () => setTimeout(resolve, 200));
    });

    await failedPromise;

    dlqQueue = new Queue(dlqName, { connection: CONNECTION });
    const dlqJobs = await dlqQueue.getJobs('waiting');
    expect(dlqJobs.length).toBeGreaterThanOrEqual(1);
    // The DLQ job preserves the original job's name
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
      worker = new Worker(queueName, async () => {
        throw new Error('state check');
      }, {
        connection: CONNECTION,
        deadLetterQueue: { name: dlqName },
        stalledInterval: 60000,
      });
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
      worker = new Worker(queueName, async () => {
        throw new Error('specific failure reason XYZ');
      }, {
        connection: CONNECTION,
        deadLetterQueue: { name: dlqName },
        stalledInterval: 60000,
      });
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
      worker = new Worker(queueName, async () => {
        throw new Error('delayed fail');
      }, {
        connection: CONNECTION,
        deadLetterQueue: { name: dlqName },
        promotionInterval: 100,
        stalledInterval: 60000,
      });
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

    await queue.add('backoff-dlq', { x: 1 }, {
      attempts: 3,
      backoff: { type: 'fixed', delay: 50 },
    });

    let failCount = 0;
    const allFailed = new Promise<void>((resolve) => {
      worker = new Worker(queueName, async () => {
        throw new Error('backoff exhaust');
      }, {
        connection: CONNECTION,
        deadLetterQueue: { name: dlqName },
        promotionInterval: 50,
        stalledInterval: 60000,
      });
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
      worker = new Worker(queueName, async () => {
        throw new Error('event test');
      }, {
        connection: CONNECTION,
        deadLetterQueue: { name: dlqName },
        stalledInterval: 60000,
      });
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
      worker = new Worker(queueName, async () => {
        throw new Error('counts');
      }, {
        connection: CONNECTION,
        deadLetterQueue: { name: dlqName },
        stalledInterval: 60000,
      });
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
      worker = new Worker(queueName, async () => {
        throw new Error('origin');
      }, {
        connection: CONNECTION,
        deadLetterQueue: { name: dlqName },
        stalledInterval: 60000,
      });
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
