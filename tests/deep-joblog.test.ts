/**
 * Deep tests: Job logging functionality
 * Requires: valkey-server on localhost:6379 and cluster on :7000-7005
 *
 * Run: npx vitest run tests/deep-joblog.test.ts
 */
import { it, expect, beforeAll, afterAll, afterEach } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { Job } = require('../dist/job') as typeof import('../src/job');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

function uid() {
  return `log-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
}

describeEachMode('Job Logging', (CONNECTION) => {
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

  it('job.log appends a single log entry', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });

    const completed = new Promise<void>((resolve) => {
      worker = new Worker(queueName, async (job: any) => {
        await job.log('hello from processor');
        return 'done';
      }, {
        connection: CONNECTION,
        stalledInterval: 60000,
      });
      worker.on('completed', () => resolve());
    });

    const job = await queue.add('log-single', { x: 1 });
    await completed;

    const { logs, count } = await queue.getJobLogs(job!.id);
    expect(count).toBe(1);
    expect(logs).toEqual(['hello from processor']);
  });

  it('job.log appends multiple entries in order', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });

    const completed = new Promise<void>((resolve) => {
      worker = new Worker(queueName, async (job: any) => {
        await job.log('step 1');
        await job.log('step 2');
        await job.log('step 3');
        return 'done';
      }, {
        connection: CONNECTION,
        stalledInterval: 60000,
      });
      worker.on('completed', () => resolve());
    });

    const job = await queue.add('log-multi', { x: 1 });
    await completed;

    const { logs, count } = await queue.getJobLogs(job!.id);
    expect(count).toBe(3);
    expect(logs).toEqual(['step 1', 'step 2', 'step 3']);
  });

  it('getJobLogs returns empty for job with no logs', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });

    const completed = new Promise<void>((resolve) => {
      worker = new Worker(queueName, async () => {
        return 'no logs';
      }, {
        connection: CONNECTION,
        stalledInterval: 60000,
      });
      worker.on('completed', () => resolve());
    });

    const job = await queue.add('no-logs', { x: 1 });
    await completed;

    const { logs, count } = await queue.getJobLogs(job!.id);
    expect(count).toBe(0);
    expect(logs).toEqual([]);
  });

  it('getJobLogs returns empty for non-existent job', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });
    const { logs, count } = await queue.getJobLogs('nonexistent-999');
    expect(count).toBe(0);
    expect(logs).toEqual([]);
  });

  it('log entries persist after job completes', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });

    const completed = new Promise<void>((resolve) => {
      worker = new Worker(queueName, async (job: any) => {
        await job.log('persistent entry');
        return 'done';
      }, {
        connection: CONNECTION,
        stalledInterval: 60000,
      });
      worker.on('completed', () => resolve());
    });

    const job = await queue.add('persist-log', { x: 1 });
    await completed;

    await worker.close(true);
    worker = null as any;

    const { logs, count } = await queue.getJobLogs(job!.id);
    expect(count).toBe(1);
    expect(logs[0]).toBe('persistent entry');
  });

  it('log entries persist after job fails', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });

    const failed = new Promise<void>((resolve) => {
      worker = new Worker(queueName, async (job: any) => {
        await job.log('log before failure');
        throw new Error('intentional');
      }, {
        connection: CONNECTION,
        stalledInterval: 60000,
      });
      worker.on('failed', () => resolve());
    });

    const job = await queue.add('fail-log', { x: 1 });
    await failed;

    const { logs, count } = await queue.getJobLogs(job!.id);
    expect(count).toBe(1);
    expect(logs[0]).toBe('log before failure');
  });

  it('getJobLogs supports start/end pagination', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });

    const completed = new Promise<void>((resolve) => {
      worker = new Worker(queueName, async (job: any) => {
        for (let i = 0; i < 10; i++) {
          await job.log(`entry-${i}`);
        }
        return 'done';
      }, {
        connection: CONNECTION,
        stalledInterval: 60000,
      });
      worker.on('completed', () => resolve());
    });

    const job = await queue.add('paginate', { x: 1 });
    await completed;

    const { logs, count } = await queue.getJobLogs(job!.id, 2, 4);
    expect(count).toBe(10);
    expect(logs).toEqual(['entry-2', 'entry-3', 'entry-4']);
  });

  it('getJobLogs with start=0, end=-1 returns all', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });

    const completed = new Promise<void>((resolve) => {
      worker = new Worker(queueName, async (job: any) => {
        await job.log('a');
        await job.log('b');
        await job.log('c');
        return 'done';
      }, {
        connection: CONNECTION,
        stalledInterval: 60000,
      });
      worker.on('completed', () => resolve());
    });

    const job = await queue.add('all-logs', { x: 1 });
    await completed;

    const { logs } = await queue.getJobLogs(job!.id, 0, -1);
    expect(logs).toEqual(['a', 'b', 'c']);
  });

  it('log entries support special characters', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });

    const specialMsg = 'Error: "value" has <html> & newlines\nline2\ttab';
    const completed = new Promise<void>((resolve) => {
      worker = new Worker(queueName, async (job: any) => {
        await job.log(specialMsg);
        return 'done';
      }, {
        connection: CONNECTION,
        stalledInterval: 60000,
      });
      worker.on('completed', () => resolve());
    });

    const job = await queue.add('special-chars', { x: 1 });
    await completed;

    const { logs } = await queue.getJobLogs(job!.id);
    expect(logs[0]).toBe(specialMsg);
  });

  it('log entries support long messages', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });

    const longMsg = 'x'.repeat(10000);
    const completed = new Promise<void>((resolve) => {
      worker = new Worker(queueName, async (job: any) => {
        await job.log(longMsg);
        return 'done';
      }, {
        connection: CONNECTION,
        stalledInterval: 60000,
      });
      worker.on('completed', () => resolve());
    });

    const job = await queue.add('long-log', { x: 1 });
    await completed;

    const { logs } = await queue.getJobLogs(job!.id);
    expect(logs[0].length).toBe(10000);
  });

  it('multiple jobs have independent log lists', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });

    let completedCount = 0;
    const allDone = new Promise<void>((resolve) => {
      worker = new Worker(queueName, async (job: any) => {
        await job.log(`log for ${job.id}`);
        return 'done';
      }, {
        connection: CONNECTION,
        stalledInterval: 60000,
      });
      worker.on('completed', () => {
        completedCount++;
        if (completedCount >= 2) resolve();
      });
    });

    const job1 = await queue.add('independent-1', { x: 1 });
    const job2 = await queue.add('independent-2', { x: 2 });
    await allDone;

    const logs1 = await queue.getJobLogs(job1!.id);
    const logs2 = await queue.getJobLogs(job2!.id);

    expect(logs1.count).toBe(1);
    expect(logs2.count).toBe(1);
    expect(logs1.logs[0]).toBe(`log for ${job1!.id}`);
    expect(logs2.logs[0]).toBe(`log for ${job2!.id}`);
  });

  it('log list is stored at the correct key', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });

    const completed = new Promise<void>((resolve) => {
      worker = new Worker(queueName, async (job: any) => {
        await job.log('key check');
        return 'done';
      }, {
        connection: CONNECTION,
        stalledInterval: 60000,
      });
      worker.on('completed', () => resolve());
    });

    const job = await queue.add('key-check', { x: 1 });
    await completed;

    const k = buildKeys(queueName);
    const expectedKey = k.log(job!.id);
    const len = await cleanupClient.llen(expectedKey);
    expect(len).toBe(1);
  });

  it('removeJob also removes log list', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });

    const completed = new Promise<void>((resolve) => {
      worker = new Worker(queueName, async (job: any) => {
        await job.log('will be deleted');
        return 'done';
      }, {
        connection: CONNECTION,
        stalledInterval: 60000,
      });
      worker.on('completed', () => resolve());
    });

    const job = await queue.add('remove-log', { x: 1 });
    await completed;

    const before = await queue.getJobLogs(job!.id);
    expect(before.count).toBe(1);

    const fetched = await queue.getJob(job!.id);
    await fetched!.remove();

    const k = buildKeys(queueName);
    const len = await cleanupClient.llen(k.log(job!.id));
    expect(len).toBe(0);
  });

  it('many log entries (50+) are correctly stored and retrieved', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });

    const N = 50;
    const completed = new Promise<void>((resolve) => {
      worker = new Worker(queueName, async (job: any) => {
        for (let i = 0; i < N; i++) {
          await job.log(`line-${i}`);
        }
        return 'done';
      }, {
        connection: CONNECTION,
        stalledInterval: 60000,
      });
      worker.on('completed', () => resolve());
    });

    const job = await queue.add('many-logs', { x: 1 });
    await completed;

    const { logs, count } = await queue.getJobLogs(job!.id);
    expect(count).toBe(N);
    expect(logs.length).toBe(N);
    expect(logs[0]).toBe('line-0');
    expect(logs[N - 1]).toBe(`line-${N - 1}`);
  });

  it('log during retries accumulates across attempts', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });

    let attempt = 0;
    const done = new Promise<void>((resolve) => {
      worker = new Worker(queueName, async (job: any) => {
        attempt++;
        await job.log(`attempt ${attempt}`);
        if (attempt < 3) throw new Error('retry');
        return 'ok';
      }, {
        connection: CONNECTION,
        stalledInterval: 60000,
        promotionInterval: 50,
      });
      worker.on('completed', () => resolve());
    });

    const job = await queue.add('retry-log', { x: 1 }, {
      attempts: 3,
      backoff: { type: 'fixed', delay: 50 },
    });
    await done;

    const { logs, count } = await queue.getJobLogs(job!.id);
    expect(count).toBe(3);
    expect(logs).toEqual(['attempt 1', 'attempt 2', 'attempt 3']);
  });

  it('getJobLogs tail: negative start index', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });

    const completed = new Promise<void>((resolve) => {
      worker = new Worker(queueName, async (job: any) => {
        for (let i = 0; i < 5; i++) {
          await job.log(`msg-${i}`);
        }
        return 'done';
      }, {
        connection: CONNECTION,
        stalledInterval: 60000,
      });
      worker.on('completed', () => resolve());
    });

    const job = await queue.add('tail-log', { x: 1 });
    await completed;

    const { logs } = await queue.getJobLogs(job!.id, -2, -1);
    expect(logs).toEqual(['msg-3', 'msg-4']);
  });

  it('log with empty string', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });

    const completed = new Promise<void>((resolve) => {
      worker = new Worker(queueName, async (job: any) => {
        await job.log('');
        return 'done';
      }, {
        connection: CONNECTION,
        stalledInterval: 60000,
      });
      worker.on('completed', () => resolve());
    });

    const job = await queue.add('empty-log', { x: 1 });
    await completed;

    const { logs, count } = await queue.getJobLogs(job!.id);
    expect(count).toBe(1);
    expect(logs[0]).toBe('');
  });

  it('log with JSON string content', async () => {
    queueName = uid();
    queue = new Queue(queueName, { connection: CONNECTION });

    const jsonMsg = JSON.stringify({ level: 'info', message: 'test', data: [1, 2] });
    const completed = new Promise<void>((resolve) => {
      worker = new Worker(queueName, async (job: any) => {
        await job.log(jsonMsg);
        return 'done';
      }, {
        connection: CONNECTION,
        stalledInterval: 60000,
      });
      worker.on('completed', () => resolve());
    });

    const job = await queue.add('json-log', { x: 1 });
    await completed;

    const { logs } = await queue.getJobLogs(job!.id);
    expect(logs[0]).toBe(jsonMsg);
    const parsed = JSON.parse(logs[0]);
    expect(parsed.level).toBe('info');
  });
});
