/**
 * Suspend/Resume with Signals tests.
 * Tests the human-in-the-loop workflow where jobs can be suspended
 * and resumed via external signals.
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { describeEachMode, createCleanupClient, flushQueue, waitFor } from './helpers/fixture';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { QueueEvents } = require('../dist/queue-events') as typeof import('../src/queue-events');
const { SuspendError } = require('../dist/errors') as typeof import('../src/errors');
const { Job } = require('../dist/job') as typeof import('../src/job');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

// ---- Unit tests (no Valkey needed) ----

describe('SuspendError (unit)', () => {
  it('suspend() throws SuspendError', async () => {
    const mockClient = {} as any;
    const keys = buildKeys('test-suspend-unit');
    const job = Job.fromHash(mockClient, keys, '1', {
      name: 'test',
      data: '{}',
      opts: '{}',
      state: 'active',
      attemptsMade: '0',
      timestamp: Date.now().toString(),
    });
    job.entryId = '1234-0';

    let thrownError: Error | undefined;
    try {
      await job.suspend({ reason: 'needs-approval' });
    } catch (err: any) {
      thrownError = err;
    }
    expect(thrownError).toBeInstanceOf(SuspendError);
    expect(thrownError!.message).toBe('Job suspended');
  });

  it('suspend() stores request with reason and timeout', async () => {
    const mockClient = {} as any;
    const keys = buildKeys('test-suspend-unit');
    const job = Job.fromHash(mockClient, keys, '2', {
      name: 'test',
      data: '{}',
      opts: '{}',
      state: 'active',
      attemptsMade: '0',
      timestamp: Date.now().toString(),
    });
    job.entryId = '1234-1';

    const onResume = async () => 'resumed';
    try {
      await job.suspend({ reason: 'approval', timeout: 5000, onResume });
    } catch {
      // expected SuspendError
    }

    const req = job.consumeSuspendRequest();
    expect(req).toBeDefined();
    expect(req!.reason).toBe('approval');
    expect(req!.timeout).toBe(5000);
    expect(req!.onResume).toBe(onResume);
  });

  it('suspend() requires entryId', async () => {
    const mockClient = {} as any;
    const keys = buildKeys('test-suspend-unit');
    const job = Job.fromHash(mockClient, keys, '3', {
      name: 'test',
      data: '{}',
      opts: '{}',
      state: 'active',
      attemptsMade: '0',
      timestamp: Date.now().toString(),
    });

    await expect(job.suspend()).rejects.toThrow('suspend() can only be used while the job is active in a Worker');
  });

  it('consumeSuspendRequest() returns and clears request', async () => {
    const mockClient = {} as any;
    const keys = buildKeys('test-suspend-unit');
    const job = Job.fromHash(mockClient, keys, '4', {
      name: 'test',
      data: '{}',
      opts: '{}',
      state: 'active',
      attemptsMade: '0',
      timestamp: Date.now().toString(),
    });
    job.entryId = '1234-2';

    try {
      await job.suspend({ reason: 'test' });
    } catch {
      // expected
    }

    const first = job.consumeSuspendRequest();
    expect(first).toBeDefined();
    expect(first!.reason).toBe('test');

    const second = job.consumeSuspendRequest();
    expect(second).toBeUndefined();
  });

  it('fromHash parses signals field', () => {
    const mockClient = {} as any;
    const keys = buildKeys('test-suspend-unit');
    const signals = [{ name: 'approve', data: 'ok', receivedAt: 1000 }];
    const job = Job.fromHash(mockClient, keys, '5', {
      name: 'test',
      data: '{}',
      opts: '{}',
      state: 'waiting',
      attemptsMade: '0',
      timestamp: Date.now().toString(),
      signals: JSON.stringify(signals),
    });

    expect(job.signals).toEqual(signals);
  });
});

// ---- Testing mode (in-memory, no Valkey) ----

describe('Suspend/Signal (testing mode)', () => {
  let TestQueue: typeof import('../src/testing').TestQueue;
  let TestWorker: typeof import('../src/testing').TestWorker;

  beforeAll(async () => {
    const testing = require('../dist/testing');
    TestQueue = testing.TestQueue;
    TestWorker = testing.TestWorker;
  });

  it('TestJob.suspend() + TestQueue.signal() round-trip', async () => {
    const queue = new TestQueue('test-suspend-rt');
    let suspendCount = 0;
    let resumedWithSignals = false;

    const worker = new TestWorker(queue, async (job: any) => {
      if (suspendCount === 0) {
        suspendCount++;
        await job.suspend({ reason: 'waiting-for-approval' });
      }
      if (job.signals && job.signals.length > 0) {
        resumedWithSignals = true;
        return { approved: true };
      }
      return { approved: false };
    });

    const added = await queue.add('request', { type: 'approval' });

    await waitFor(() => {
      const record = queue.jobs.get(added!.id);
      return record?.state === 'suspended';
    }, 3000);

    const resumed = await queue.signal(added!.id, 'approve', { by: 'admin' });
    expect(resumed).toBe(true);

    await waitFor(() => {
      const record = queue.jobs.get(added!.id);
      return record?.state === 'completed';
    }, 3000);

    expect(resumedWithSignals).toBe(true);
    await worker.close();
    await queue.close();
  });

  it('multiple signals accumulated on record', async () => {
    const queue = new TestQueue('test-suspend-multi');
    const receivedSignals: any[] = [];

    const worker = new TestWorker(queue, async (job: any) => {
      if (job.signals.length === 0) {
        await job.suspend();
      }
      receivedSignals.push(...job.signals);
      return { count: job.signals.length };
    });

    const added = await queue.add('multi-sig', {});

    await waitFor(() => {
      const record = queue.jobs.get(added!.id);
      return record?.state === 'suspended';
    }, 3000);

    await queue.signal(added!.id, 'sig1', { a: 1 });

    await waitFor(() => {
      const record = queue.jobs.get(added!.id);
      return record?.state === 'completed';
    }, 3000);

    expect(receivedSignals.length).toBeGreaterThanOrEqual(1);
    expect(receivedSignals[0].name).toBe('sig1');
    await worker.close();
    await queue.close();
  });

  it('getSuspendInfo() returns metadata', async () => {
    const queue = new TestQueue('test-suspend-info');

    const worker = new TestWorker(queue, async (job: any) => {
      await job.suspend({ reason: 'human-review', timeout: 60000 });
    });

    const added = await queue.add('review', { doc: 'abc' });

    await waitFor(() => {
      const record = queue.jobs.get(added!.id);
      return record?.state === 'suspended';
    }, 3000);

    const info = await queue.getSuspendInfo(added!.id);
    expect(info).not.toBeNull();
    expect(info!.reason).toBe('human-review');
    expect(info!.timeout).toBe(60000);
    expect(info!.suspendedAt).toBeGreaterThan(0);
    expect(info!.signals).toEqual([]);

    await worker.close();
    await queue.close();
  });

  it('signal on non-suspended job returns false', async () => {
    const queue = new TestQueue('test-suspend-nosig');

    const worker = new TestWorker(queue, async () => ({ done: true }));
    const added = await queue.add('normal', {});

    await waitFor(() => {
      const record = queue.jobs.get(added!.id);
      return record?.state === 'completed';
    }, 3000);

    const result = await queue.signal(added!.id, 'test');
    expect(result).toBe(false);

    await worker.close();
    await queue.close();
  });

  it('suspend({ timeout }) fails even after the worker closes', async () => {
    const queue = new TestQueue('test-suspend-timeout-no-worker');

    const worker = new TestWorker(queue, async (job: any) => {
      await job.suspend({ timeout: 250 });
    });

    const added = await queue.add('timeout-no-worker', {});

    await waitFor(() => {
      const record = queue.jobs.get(added!.id);
      return record?.state === 'suspended';
    }, 3000);

    await worker.close();

    await waitFor(() => {
      const record = queue.jobs.get(added!.id);
      return record?.state === 'failed';
    }, 3000);

    const failed = queue.jobs.get(added!.id);
    expect(failed?.failedReason).toBe('Suspend timeout exceeded');

    await queue.close();
  });
});

// ---- Integration tests (against Valkey) ----

// Helper to create a unique queue name per test
let testCounter = 0;
function uniqueQ(): string {
  return `test-susp-${Date.now()}-${++testCounter}`;
}

describeEachMode('Suspend/Signal', (CONNECTION) => {
  const queuesToFlush: string[] = [];
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    for (const q of queuesToFlush) {
      try {
        await flushQueue(cleanupClient, q);
      } catch {
        // best effort
      }
    }
    cleanupClient.close();
  });

  it('basic suspend + signal resumes job', async () => {
    const qName = uniqueQ();
    queuesToFlush.push(qName);
    const queue = new Queue(qName, { connection: CONNECTION });
    let suspendCount = 0;

    const worker = new Worker(
      qName,
      async (job: any) => {
        if (suspendCount === 0) {
          suspendCount++;
          await job.suspend({ reason: 'waiting' });
        }
        return { resumed: true };
      },
      { connection: CONNECTION },
    );
    await worker.waitUntilReady();

    const added = await queue.add('suspend-basic', { val: 1 });

    await waitFor(async () => {
      const info = await queue.getSuspendInfo(added!.id);
      return info !== null;
    }, 8000);

    const info = await queue.getSuspendInfo(added!.id);
    expect(info).not.toBeNull();
    expect(info!.reason).toBe('waiting');

    const resumed = await queue.signal(added!.id, 'go', { approved: true });
    expect(resumed).toBe(true);

    const completed = await new Promise<any>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('Timeout waiting for completion')), 10000);
      worker.on('completed', (_job: any, result: any) => {
        clearTimeout(timer);
        resolve(result);
      });
    });

    expect(completed.resumed).toBe(true);

    await worker.close();
    await queue.close();
  }, 20000);

  it('processor re-invoked with job.signals populated', async () => {
    const qName = uniqueQ();
    queuesToFlush.push(qName);
    const queue = new Queue(qName, { connection: CONNECTION });
    const invocations: any[] = [];

    const worker = new Worker(
      qName,
      async (job: any) => {
        invocations.push({ signals: [...job.signals] });
        if (job.signals.length === 0) {
          await job.suspend();
        }
        return { signalCount: job.signals.length };
      },
      { connection: CONNECTION },
    );
    await worker.waitUntilReady();

    const added = await queue.add('suspend-signals', {});

    await waitFor(async () => {
      const info = await queue.getSuspendInfo(added!.id);
      return info !== null;
    }, 8000);

    await queue.signal(added!.id, 'resume', { data: 'hello' });

    const completed = await new Promise<any>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('Timeout')), 10000);
      worker.on('completed', (_job: any, result: any) => {
        clearTimeout(timer);
        resolve(result);
      });
    });

    expect(completed.signalCount).toBe(1);
    expect(invocations.length).toBe(2);
    expect(invocations[0].signals).toEqual([]);
    expect(invocations[1].signals.length).toBe(1);
    expect(invocations[1].signals[0].name).toBe('resume');

    await worker.close();
    await queue.close();
  }, 20000);

  it('signal() on non-suspended job returns false', async () => {
    const qName = uniqueQ();
    queuesToFlush.push(qName);
    const queue = new Queue(qName, { connection: CONNECTION });

    const worker = new Worker(qName, async () => ({ done: true }), { connection: CONNECTION });
    await worker.waitUntilReady();

    const added = await queue.add('no-suspend', {});

    await waitFor(async () => {
      const job = await queue.getJob(added!.id);
      if (!job) return false;
      return (await job.getState()) === 'completed';
    }, 8000);

    const result = await queue.signal(added!.id, 'late-signal');
    expect(result).toBe(false);

    await worker.close();
    await queue.close();
  }, 15000);

  it('signal() on non-existent job returns false', async () => {
    const qName = uniqueQ();
    queuesToFlush.push(qName);
    const queue = new Queue(qName, { connection: CONNECTION });
    await queue.add('dummy', {});

    const result = await queue.signal('non-existent-job-id-999', 'test');
    expect(result).toBe(false);

    await queue.close();
  }, 10000);

  it('suspend({ timeout }) auto-fails after timeout', async () => {
    const qName = uniqueQ();
    queuesToFlush.push(qName);
    const queue = new Queue(qName, { connection: CONNECTION });

    const worker = new Worker(
      qName,
      async (job: any) => {
        if (job.signals.length === 0) {
          await job.suspend({ timeout: 2000 });
        }
        return { ok: true };
      },
      {
        connection: CONNECTION,
        stalledInterval: 1500,
      },
    );
    await worker.waitUntilReady();

    const added = await queue.add('suspend-timeout', {});

    await waitFor(async () => {
      const info = await queue.getSuspendInfo(added!.id);
      return info !== null;
    }, 8000);

    // Wait for the timeout sweep to fail the job
    await waitFor(async () => {
      const job = await queue.getJob(added!.id);
      if (!job) return false;
      return (await job.getState()) === 'failed';
    }, 15000);

    const job = await queue.getJob(added!.id);
    const state = await job!.getState();
    expect(state).toBe('failed');

    await worker.close();
    await queue.close();
  }, 25000);

  it('suspend({ timeout }) fails without any worker still running', async () => {
    const qName = uniqueQ();
    queuesToFlush.push(qName);
    const queue = new Queue(qName, { connection: CONNECTION });

    const worker = new Worker(
      qName,
      async (job: any) => {
        await job.suspend({ timeout: 2000 });
      },
      {
        connection: CONNECTION,
        stalledInterval: 60000,
      },
    );
    await worker.waitUntilReady();

    const added = await queue.add('suspend-timeout-no-worker', {});

    await waitFor(async () => {
      const info = await queue.getSuspendInfo(added!.id);
      return info !== null;
    }, 8000);

    await worker.close();

    await waitFor(async () => {
      const job = await queue.getJob(added!.id);
      if (!job) return false;
      return (await job.getState()) === 'failed';
    }, 12000);

    const job = await queue.getJob(added!.id);
    expect(await job!.getState()).toBe('failed');
    expect(job!.failedReason).toBe('Suspend timeout exceeded');

    await queue.close();
  }, 20000);

  it('suspend() without timeout waits indefinitely (signal after 2s works)', async () => {
    const qName = uniqueQ();
    queuesToFlush.push(qName);
    const queue = new Queue(qName, { connection: CONNECTION });

    const worker = new Worker(
      qName,
      async (job: any) => {
        if (job.signals.length === 0) {
          await job.suspend();
        }
        return { signalCount: job.signals.length };
      },
      { connection: CONNECTION },
    );
    await worker.waitUntilReady();

    const added = await queue.add('suspend-infinite', {});

    await waitFor(async () => {
      const info = await queue.getSuspendInfo(added!.id);
      return info !== null;
    }, 8000);

    await new Promise((r) => setTimeout(r, 2000));

    const resumed = await queue.signal(added!.id, 'wake', { msg: 'hello' });
    expect(resumed).toBe(true);

    const completed = await new Promise<any>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('Timeout')), 10000);
      worker.on('completed', (_job: any, result: any) => {
        clearTimeout(timer);
        resolve(result);
      });
    });

    expect(completed.signalCount).toBe(1);

    await worker.close();
    await queue.close();
  }, 25000);

  it('getSuspendInfo returns null for non-suspended job', async () => {
    const qName = uniqueQ();
    queuesToFlush.push(qName);
    const queue = new Queue(qName, { connection: CONNECTION });
    const worker = new Worker(qName, async () => 'done', { connection: CONNECTION });
    await worker.waitUntilReady();

    const added = await queue.add('not-suspended', {});

    await waitFor(async () => {
      const job = await queue.getJob(added!.id);
      if (!job) return false;
      return (await job.getState()) === 'completed';
    }, 8000);

    const info = await queue.getSuspendInfo(added!.id);
    expect(info).toBeNull();

    await worker.close();
    await queue.close();
  }, 15000);

  it('QueueEvents emits suspended and resumed events', async () => {
    const qName = uniqueQ();
    queuesToFlush.push(qName);
    const queue = new Queue(qName, { connection: CONNECTION });
    const events: any[] = [];

    const queueEvents = new QueueEvents(qName, { connection: CONNECTION });
    await queueEvents.waitUntilReady();

    queueEvents.on('suspended', (data: any) => {
      events.push({ type: 'suspended', jobId: data.jobId });
    });
    queueEvents.on('resumed', (data: any) => {
      events.push({ type: 'resumed', jobId: data.jobId });
    });

    const worker = new Worker(
      qName,
      async (job: any) => {
        if (job.signals.length === 0) {
          await job.suspend({ reason: 'event-test' });
        }
        return { ok: true };
      },
      { connection: CONNECTION },
    );
    await worker.waitUntilReady();

    const added = await queue.add('event-test', {});

    await waitFor(async () => {
      const info = await queue.getSuspendInfo(added!.id);
      return info !== null;
    }, 8000);

    await new Promise((r) => setTimeout(r, 500));

    await queue.signal(added!.id, 'approve');

    await waitFor(() => events.length >= 2, 8000);

    const suspendedEvent = events.find((e) => e.type === 'suspended');
    const resumedEvent = events.find((e) => e.type === 'resumed');
    expect(suspendedEvent).toBeDefined();
    expect(resumedEvent).toBeDefined();
    expect(suspendedEvent.jobId).toBe(added!.id);
    expect(resumedEvent.jobId).toBe(added!.id);

    await queueEvents.close();
    await worker.close();
    await queue.close();
  }, 25000);

  it('suspend({ onResume }) callback runs on same worker', async () => {
    const qName = uniqueQ();
    queuesToFlush.push(qName);
    const queue = new Queue(qName, { connection: CONNECTION });
    let onResumeCalled = false;

    const worker = new Worker(
      qName,
      async (job: any) => {
        if (job.signals.length === 0) {
          await job.suspend({
            reason: 'with-callback',
            onResume: async (signals: any[]) => {
              onResumeCalled = true;
              return { fromCallback: true, signalName: signals[0].name };
            },
          });
        }
        return { fromProcessor: true };
      },
      { connection: CONNECTION },
    );
    await worker.waitUntilReady();

    const added = await queue.add('suspend-callback', {});

    await waitFor(async () => {
      const info = await queue.getSuspendInfo(added!.id);
      return info !== null;
    }, 8000);

    await queue.signal(added!.id, 'continue', { data: 'test' });

    const completed = await new Promise<any>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('Timeout')), 10000);
      worker.on('completed', (_job: any, result: any) => {
        clearTimeout(timer);
        resolve(result);
      });
    });

    expect(onResumeCalled).toBe(true);
    expect(completed.fromCallback).toBe(true);
    expect(completed.signalName).toBe('continue');

    await worker.close();
    await queue.close();
  }, 20000);

  it('onResume throw triggers failure handling', async () => {
    const qName = uniqueQ();
    queuesToFlush.push(qName);
    const queue = new Queue(qName, { connection: CONNECTION });

    const worker = new Worker(
      qName,
      async (job: any) => {
        if (job.signals.length === 0) {
          await job.suspend({
            onResume: async () => {
              throw new Error('callback-error');
            },
          });
        }
        return { ok: true };
      },
      { connection: CONNECTION },
    );
    await worker.waitUntilReady();

    const added = await queue.add('suspend-callback-fail', {});

    await waitFor(async () => {
      const info = await queue.getSuspendInfo(added!.id);
      return info !== null;
    }, 8000);

    await queue.signal(added!.id, 'resume');

    await waitFor(async () => {
      const job = await queue.getJob(added!.id);
      if (!job) return false;
      const s = await job.getState();
      return s === 'failed' || s === 'completed';
    }, 10000);

    const job = await queue.getJob(added!.id);
    const state = await job!.getState();
    expect(state).toBe('failed');

    await worker.close();
    await queue.close();
  }, 20000);

  it('signal with large payload', async () => {
    const qName = uniqueQ();
    queuesToFlush.push(qName);
    const queue = new Queue(qName, { connection: CONNECTION });

    const worker = new Worker(
      qName,
      async (job: any) => {
        if (job.signals.length === 0) {
          await job.suspend();
        }
        return { payloadLength: JSON.stringify(job.signals[0].data).length };
      },
      { connection: CONNECTION },
    );
    await worker.waitUntilReady();

    const added = await queue.add('large-signal', {});

    await waitFor(async () => {
      const info = await queue.getSuspendInfo(added!.id);
      return info !== null;
    }, 8000);

    const largeData = { items: Array.from({ length: 1000 }, (_, i) => ({ id: i, value: 'x'.repeat(40) })) };
    const resumed = await queue.signal(added!.id, 'large', largeData);
    expect(resumed).toBe(true);

    const completed = await new Promise<any>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('Timeout')), 10000);
      worker.on('completed', (_job: any, result: any) => {
        clearTimeout(timer);
        resolve(result);
      });
    });

    expect(completed.payloadLength).toBeGreaterThan(1000);

    await worker.close();
    await queue.close();
  }, 20000);
});
