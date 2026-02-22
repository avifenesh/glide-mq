import { describe, it, expect, vi, beforeEach, beforeAll } from 'vitest';
import path from 'path';
import { GlideClient } from '@glidemq/speedkey';
import { Worker } from '../src/worker';
import { SandboxPool } from '../src/sandbox/pool';
import { SandboxJob } from '../src/sandbox/sandbox-job';
import { createSandboxedProcessor } from '../src/sandbox/index';
import { Job } from '../src/job';
import { LIBRARY_VERSION } from '../src/functions/index';

vi.mock('@glidemq/speedkey', () => {
  const MockGlideClient = {
    createClient: vi.fn(),
  };
  const MockGlideClusterClient = {
    createClient: vi.fn(),
  };
  return {
    GlideClient: MockGlideClient,
    GlideClusterClient: MockGlideClusterClient,
  };
});

const PROCESSORS = path.resolve(__dirname, 'fixtures/processors');
const ECHO_PROCESSOR = path.join(PROCESSORS, 'echo.js');
const ECHO_ESM_PROCESSOR = path.join(PROCESSORS, 'echo.mjs');
const THROW_PROCESSOR = path.join(PROCESSORS, 'throw.js');
const CRASH_PROCESSOR = path.join(PROCESSORS, 'crash.js');
const SLOW_PROCESSOR = path.join(PROCESSORS, 'slow.js');
const PROGRESS_PROCESSOR = path.join(PROCESSORS, 'progress.js');
const FLOOD_PROGRESS_PROCESSOR = path.join(PROCESSORS, 'flood-progress.js');
const CRASH_AFTER_PROXY_PROCESSOR = path.join(PROCESSORS, 'crash-after-proxy.js');
const CONDITIONAL_CRASH_PROCESSOR = path.join(PROCESSORS, 'conditional-crash.js');
const CONDITIONAL_PROXY_CRASH_PROCESSOR = path.join(PROCESSORS, 'conditional-proxy-crash.js');

// The compiled runner.js lives in dist/sandbox/
const RUNNER_PATH = path.resolve(__dirname, '..', 'dist', 'sandbox', 'runner.js');

// Verify build artifacts and fixtures exist before running tests
beforeAll(async () => {
  const fs = await import('fs');
  expect(fs.existsSync(ECHO_PROCESSOR)).toBe(true);
  expect(fs.existsSync(RUNNER_PATH)).toBe(true);
});

describe('SandboxJob', () => {
  it('should populate all fields from serialized data', () => {
    const serialized = {
      id: '42',
      name: 'test-job',
      data: { foo: 'bar' },
      opts: { attempts: 3 },
      attemptsMade: 1,
      timestamp: 1000,
      progress: 50,
      processedOn: 2000,
      parentId: 'p1',
      parentQueue: 'parent-q',
      orderingKey: 'ok1',
      orderingSeq: 5,
      groupKey: 'g1',
      cost: 10,
    };

    const sendMessage = vi.fn();
    const job = new SandboxJob(serialized, sendMessage);

    expect(job.id).toBe('42');
    expect(job.name).toBe('test-job');
    expect(job.data).toEqual({ foo: 'bar' });
    expect(job.opts).toEqual({ attempts: 3 });
    expect(job.attemptsMade).toBe(1);
    expect(job.timestamp).toBe(1000);
    expect(job.progress).toBe(50);
    expect(job.processedOn).toBe(2000);
    expect(job.parentId).toBe('p1');
    expect(job.parentQueue).toBe('parent-q');
    expect(job.orderingKey).toBe('ok1');
    expect(job.orderingSeq).toBe(5);
    expect(job.groupKey).toBe('g1');
    expect(job.cost).toBe(10);
    expect(job.abortSignal.aborted).toBe(false);
  });

  it('should send proxy-request for log()', async () => {
    const sendMessage = vi.fn();
    const job = new SandboxJob(
      { id: '1', name: 'j', data: {}, opts: {}, attemptsMade: 0, timestamp: 0, progress: 0 },
      sendMessage,
    );

    const logPromise = job.log('hello');

    expect(sendMessage).toHaveBeenCalledTimes(1);
    const msg = sendMessage.mock.calls[0][0];
    expect(msg.type).toBe('proxy-request');
    expect(msg.method).toBe('log');
    expect(msg.args).toEqual(['hello']);

    // Simulate proxy response
    job.handleProxyResponse({ type: 'proxy-response', id: msg.id });
    await logPromise;
  });

  it('should send proxy-request for updateProgress()', async () => {
    const sendMessage = vi.fn();
    const job = new SandboxJob(
      { id: '1', name: 'j', data: {}, opts: {}, attemptsMade: 0, timestamp: 0, progress: 0 },
      sendMessage,
    );

    const progressPromise = job.updateProgress(75);
    const msg = sendMessage.mock.calls[0][0];

    expect(msg.method).toBe('updateProgress');
    expect(msg.args).toEqual([75]);

    job.handleProxyResponse({ type: 'proxy-response', id: msg.id });
    await progressPromise;
    expect(job.progress).toBe(75);
  });

  it('should send proxy-request for updateData()', async () => {
    const sendMessage = vi.fn();
    const job = new SandboxJob(
      { id: '1', name: 'j', data: { old: true }, opts: {}, attemptsMade: 0, timestamp: 0, progress: 0 },
      sendMessage,
    );

    const updatePromise = job.updateData({ new: true });
    const msg = sendMessage.mock.calls[0][0];

    expect(msg.method).toBe('updateData');
    expect(msg.args).toEqual([{ new: true }]);

    job.handleProxyResponse({ type: 'proxy-response', id: msg.id });
    await updatePromise;
    expect(job.data).toEqual({ new: true });
  });

  it('should reject proxy call on error response', async () => {
    const sendMessage = vi.fn();
    const job = new SandboxJob(
      { id: '1', name: 'j', data: {}, opts: {}, attemptsMade: 0, timestamp: 0, progress: 0 },
      sendMessage,
    );

    const logPromise = job.log('hello');
    const msg = sendMessage.mock.calls[0][0];

    job.handleProxyResponse({ type: 'proxy-response', id: msg.id, error: 'connection lost' });
    await expect(logPromise).rejects.toThrow('connection lost');
  });

  it('should trigger abort signal on _abort()', () => {
    const job = new SandboxJob(
      { id: '1', name: 'j', data: {}, opts: {}, attemptsMade: 0, timestamp: 0, progress: 0 },
      vi.fn(),
    );

    expect(job.abortSignal.aborted).toBe(false);
    job._abort();
    expect(job.abortSignal.aborted).toBe(true);
  });

  it('should throw GlideMQError for unsupported methods', async () => {
    const job = new SandboxJob(
      { id: '1', name: 'j', data: {}, opts: {}, attemptsMade: 0, timestamp: 0, progress: 0 },
      vi.fn(),
    );

    await expect(job.getState()).rejects.toThrow('Method not available in sandboxed processor');
    await expect(job.remove()).rejects.toThrow('Method not available in sandboxed processor');
    await expect(job.retry()).rejects.toThrow('Method not available in sandboxed processor');
    await expect(job.isCompleted()).rejects.toThrow('Method not available in sandboxed processor');
    await expect(job.isFailed()).rejects.toThrow('Method not available in sandboxed processor');
    await expect(job.isDelayed()).rejects.toThrow('Method not available in sandboxed processor');
    await expect(job.isActive()).rejects.toThrow('Method not available in sandboxed processor');
    await expect(job.isWaiting()).rejects.toThrow('Method not available in sandboxed processor');
    await expect(job.isRevoked()).rejects.toThrow('Method not available in sandboxed processor');
    await expect(job.moveToFailed(new Error('x'))).rejects.toThrow('Method not available in sandboxed processor');
    await expect(job.getChildrenValues()).rejects.toThrow('Method not available in sandboxed processor');
    await expect(job.waitUntilFinished()).rejects.toThrow('Method not available in sandboxed processor');
  });
});

describe('SandboxPool', () => {
  it('should process a job via worker thread and return result', async () => {
    const pool = new SandboxPool(ECHO_PROCESSOR, true, 2, RUNNER_PATH);

    // Create a minimal mock Job with the fields needed by toSerializedJob
    const fakeJob = {
      id: 'job-1',
      name: 'test',
      data: { hello: 'world' },
      opts: {},
      attemptsMade: 0,
      timestamp: Date.now(),
      progress: 0,
      processedOn: undefined,
      parentId: undefined,
      parentQueue: undefined,
      orderingKey: undefined,
      orderingSeq: undefined,
      groupKey: undefined,
      cost: undefined,
      // Mock the methods that pool.run proxy calls
      log: vi.fn(),
      updateProgress: vi.fn(),
      updateData: vi.fn(),
    } as unknown as Job;

    const result = await pool.run(fakeJob);
    expect(result).toEqual({ hello: 'world' });

    await pool.close();
  });

  it('should handle processor errors', async () => {
    const pool = new SandboxPool(THROW_PROCESSOR, true, 1, RUNNER_PATH);

    const fakeJob = {
      id: 'job-err',
      name: 'test',
      data: {},
      opts: {},
      attemptsMade: 0,
      timestamp: Date.now(),
      progress: 0,
      log: vi.fn(),
      updateProgress: vi.fn(),
      updateData: vi.fn(),
    } as unknown as Job;

    await expect(pool.run(fakeJob)).rejects.toThrow('processor error');

    await pool.close();
  });

  it('should handle processor crash (process.exit)', async () => {
    const pool = new SandboxPool(CRASH_PROCESSOR, true, 1, RUNNER_PATH);

    const fakeJob = {
      id: 'job-crash',
      name: 'test',
      data: {},
      opts: {},
      attemptsMade: 0,
      timestamp: Date.now(),
      progress: 0,
      log: vi.fn(),
      updateProgress: vi.fn(),
      updateData: vi.fn(),
    } as unknown as Job;

    await expect(pool.run(fakeJob)).rejects.toThrow(/exited with code/);

    await pool.close();
  });

  it('should process concurrent jobs', async () => {
    const pool = new SandboxPool(ECHO_PROCESSOR, true, 3, RUNNER_PATH);

    const makeJob = (id: string, data: any) =>
      ({
        id,
        name: 'test',
        data,
        opts: {},
        attemptsMade: 0,
        timestamp: Date.now(),
        progress: 0,
        log: vi.fn(),
        updateProgress: vi.fn(),
        updateData: vi.fn(),
      }) as unknown as Job;

    const results = await Promise.all([
      pool.run(makeJob('j1', { v: 1 })),
      pool.run(makeJob('j2', { v: 2 })),
      pool.run(makeJob('j3', { v: 3 })),
    ]);

    expect(results).toEqual([{ v: 1 }, { v: 2 }, { v: 3 }]);

    await pool.close();
  });

  it('should proxy updateProgress and log calls', async () => {
    const pool = new SandboxPool(PROGRESS_PROCESSOR, true, 1, RUNNER_PATH);

    const logFn = vi.fn();
    const progressFn = vi.fn();
    const fakeJob = {
      id: 'job-proxy',
      name: 'test',
      data: { value: 'ok' },
      opts: {},
      attemptsMade: 0,
      timestamp: Date.now(),
      progress: 0,
      log: logFn,
      updateProgress: progressFn,
      updateData: vi.fn(),
    } as unknown as Job;

    const result = await pool.run(fakeJob);

    expect(result).toEqual({ value: 'ok' });
    expect(progressFn).toHaveBeenCalledWith(50);
    expect(progressFn).toHaveBeenCalledWith(100);
    expect(logFn).toHaveBeenCalledWith('halfway');

    await pool.close();
  });

  it('should reject after close', async () => {
    const pool = new SandboxPool(ECHO_PROCESSOR, true, 1, RUNNER_PATH);
    await pool.close();

    const fakeJob = {
      id: 'j',
      name: 'test',
      data: {},
      opts: {},
      attemptsMade: 0,
      timestamp: Date.now(),
      progress: 0,
      log: vi.fn(),
      updateProgress: vi.fn(),
      updateData: vi.fn(),
    } as unknown as Job;

    await expect(pool.run(fakeJob)).rejects.toThrow('SandboxPool is closed');
  });

  it('should load ESM processor (.mjs)', async () => {
    const pool = new SandboxPool(ECHO_ESM_PROCESSOR, true, 1, RUNNER_PATH);

    try {
      const fakeJob = {
        id: 'job-esm',
        name: 'test',
        data: { esm: true },
        opts: {},
        attemptsMade: 0,
        timestamp: Date.now(),
        progress: 0,
        log: vi.fn(),
        updateProgress: vi.fn(),
        updateData: vi.fn(),
      } as unknown as Job;

      const result = await pool.run(fakeJob);
      expect(result).toEqual({ esm: true });
    } finally {
      await pool.close();
    }
  });

  it('should process job via child process (fork mode)', async () => {
    const pool = new SandboxPool(ECHO_PROCESSOR, false, 1, RUNNER_PATH);

    try {
      const fakeJob = {
        id: 'job-fork',
        name: 'test',
        data: { mode: 'fork' },
        opts: {},
        attemptsMade: 0,
        timestamp: Date.now(),
        progress: 0,
        log: vi.fn(),
        updateProgress: vi.fn(),
        updateData: vi.fn(),
      } as unknown as Job;

      const result = await pool.run(fakeJob);
      expect(result).toEqual({ mode: 'fork' });
    } finally {
      await pool.close();
    }
  });

  it('should handle processor crash in fork mode', async () => {
    const pool = new SandboxPool(CRASH_PROCESSOR, false, 1, RUNNER_PATH);

    try {
      const fakeJob = {
        id: 'job-fork-crash',
        name: 'test',
        data: {},
        opts: {},
        attemptsMade: 0,
        timestamp: Date.now(),
        progress: 0,
        log: vi.fn(),
        updateProgress: vi.fn(),
        updateData: vi.fn(),
      } as unknown as Job;

      await expect(pool.run(fakeJob)).rejects.toThrow(/exited with code/);
    } finally {
      await pool.close();
    }
  });

  it('should queue jobs when all workers are busy', async () => {
    const pool = new SandboxPool(SLOW_PROCESSOR, true, 1, RUNNER_PATH);

    const makeJob = (id: string) =>
      ({
        id,
        name: 'test',
        data: { delay: 100 },
        opts: {},
        attemptsMade: 0,
        timestamp: Date.now(),
        progress: 0,
        log: vi.fn(),
        updateProgress: vi.fn(),
        updateData: vi.fn(),
      }) as unknown as Job;

    // Both jobs should complete, but the second waits for the first
    const [r1, r2] = await Promise.all([pool.run(makeJob('j1')), pool.run(makeJob('j2'))]);

    expect(r1).toEqual({ delay: 100 });
    expect(r2).toEqual({ delay: 100 });

    await pool.close();
  });
});

describe('createSandboxedProcessor', () => {
  it('should throw for non-existent processor file', () => {
    expect(() => createSandboxedProcessor('/nonexistent/path.js', undefined, 1)).toThrow(
      'Processor file not found or not readable',
    );
  });

  it('should create a working processor and close handle', async () => {
    const { processor, close } = createSandboxedProcessor(ECHO_PROCESSOR, undefined, 1);

    expect(typeof processor).toBe('function');
    expect(typeof close).toBe('function');

    await close();
  });
});

describe('Worker with string processor', () => {
  function makeMockClient(overrides: Record<string, unknown> = {}) {
    return {
      fcall: vi.fn().mockImplementation((func: string) => {
        if (func === 'glidemq_checkConcurrency') return Promise.resolve(-1);
        if (func === 'glidemq_complete') return Promise.resolve(1);
        if (func === 'glidemq_fail') return Promise.resolve('failed');
        if (func === 'glidemq_promote') return Promise.resolve(0);
        if (func === 'glidemq_reclaimStalled') return Promise.resolve(0);
        if (func === 'glidemq_completeAndFetchNext') {
          return Promise.resolve(JSON.stringify({ completed: '0', next: false }));
        }
        return Promise.resolve(LIBRARY_VERSION);
      }),
      functionLoad: vi.fn(),
      xgroupCreate: vi.fn().mockResolvedValue('OK'),
      xreadgroup: vi.fn().mockResolvedValue(null),
      hgetall: vi.fn().mockResolvedValue([]),
      hget: vi.fn().mockResolvedValue(null),
      hmget: vi.fn().mockResolvedValue([null, null, null]),
      hset: vi.fn().mockResolvedValue(1),
      close: vi.fn(),
      ...overrides,
    };
  }

  const connectionOpts = {
    addresses: [{ host: '127.0.0.1', port: 6379 }],
  };

  beforeEach(() => {
    vi.clearAllMocks();

    const mockCommandClient = makeMockClient();
    const mockBlockingClient = makeMockClient({
      xreadgroup: vi.fn().mockImplementation(() => new Promise(() => {})),
    });

    let callCount = 0;
    vi.mocked(GlideClient.createClient).mockImplementation(async () => {
      callCount++;
      return (callCount === 1 ? mockCommandClient : mockBlockingClient) as any;
    });
  });

  it('should accept a file path string as processor', async () => {
    const worker = new Worker('sandbox-test', ECHO_PROCESSOR, {
      connection: connectionOpts,
      concurrency: 1,
      blockTimeout: 100,
    });

    expect(worker.name).toBe('sandbox-test');

    await worker.close(true);
  });

  it('should accept sandbox options', async () => {
    const worker = new Worker('sandbox-test-opts', ECHO_PROCESSOR, {
      connection: connectionOpts,
      concurrency: 2,
      blockTimeout: 100,
      sandbox: {
        useWorkerThreads: true,
        maxWorkers: 4,
      },
    });

    expect(worker.name).toBe('sandbox-test-opts');

    await worker.close(true);
  });
});

describe('Stress tests', () => {
  const makeJob = (id: string, data: any = {}) =>
    ({
      id,
      name: 'stress',
      data,
      opts: {},
      attemptsMade: 0,
      timestamp: Date.now(),
      progress: 0,
      log: vi.fn().mockResolvedValue(undefined),
      updateProgress: vi.fn().mockResolvedValue(undefined),
      updateData: vi.fn().mockResolvedValue(undefined),
    }) as unknown as Job;

  it('should handle pool exhaustion: 20 jobs through 2-worker pool', async () => {
    const pool = new SandboxPool(SLOW_PROCESSOR, true, 2, RUNNER_PATH);

    try {
      const jobs = Array.from({ length: 20 }, (_, i) => pool.run(makeJob(`exhaust-${i}`, { delay: 50 })));

      const results = await Promise.all(jobs);

      expect(results).toHaveLength(20);
      for (const r of results) {
        expect(r).toEqual({ delay: 50 });
      }
    } finally {
      await pool.close();
    }
  }, 30_000);

  it('should recover from 5 consecutive crashes within the same pool', async () => {
    const pool = new SandboxPool(CONDITIONAL_CRASH_PROCESSOR, true, 2, RUNNER_PATH);

    try {
      // Crash 5 workers sequentially - pool spawns replacements each time
      for (let i = 0; i < 5; i++) {
        await expect(pool.run(makeJob(`crash-${i}`, { crash: true }))).rejects.toThrow(/exited with code/);
      }

      // Same pool should still process jobs after repeated crashes
      const result = await pool.run(makeJob('post-crash', { ok: true }));
      expect(result).toEqual({ ok: true });
    } finally {
      await pool.close();
    }
  }, 30_000);

  it('should handle 50 rapid proxy calls (flood)', async () => {
    const pool = new SandboxPool(FLOOD_PROGRESS_PROCESSOR, true, 1, RUNNER_PATH);

    try {
      const progressFn = vi.fn().mockResolvedValue(undefined);
      const job = {
        ...makeJob('flood-1', { flooded: true }),
        updateProgress: progressFn,
      } as unknown as Job;

      const result = await pool.run(job);

      expect(result).toEqual({ flooded: true });
      expect(progressFn).toHaveBeenCalledTimes(50);
      // Verify ordering: IPC messages arrive in send order
      expect(progressFn).toHaveBeenNthCalledWith(1, 2);
      expect(progressFn).toHaveBeenNthCalledWith(50, 100);
    } finally {
      await pool.close();
    }
  }, 30_000);

  it('should recover after crash during proxy call', async () => {
    const pool = new SandboxPool(CONDITIONAL_PROXY_CRASH_PROCESSOR, true, 1, RUNNER_PATH);

    try {
      // Crash with in-flight proxy call
      await expect(pool.run(makeJob('crash-proxy-1', { crash: true }))).rejects.toThrow(/exited with code/);

      // Same pool should recover and process a normal job
      const result = await pool.run(makeJob('post-crash-proxy', { recovered: true }));
      expect(result).toEqual({ recovered: true });
    } finally {
      await pool.close();
    }
  }, 30_000);

  it('should close() during active and queued jobs', async () => {
    const pool = new SandboxPool(SLOW_PROCESSOR, true, 2, RUNNER_PATH);

    try {
      // Start 4 jobs: 2 active (occupy both workers) + 2 queued
      const promises = Array.from({ length: 4 }, (_, i) => pool.run(makeJob(`close-active-${i}`, { delay: 5000 })));

      // Attach rejection handlers BEFORE close() to prevent unhandled rejection warnings
      const settled = Promise.allSettled(promises);

      // Give workers time to start processing
      await new Promise((r) => setTimeout(r, 200));

      const start = Date.now();
      await pool.close(false);
      const elapsed = Date.now() - start;

      // close() should resolve within 10s (KILL_TIMEOUT=5s + buffer)
      expect(elapsed).toBeLessThan(10_000);

      // All 4 promises should reject
      const results = await settled;
      for (const r of results) {
        expect(r.status).toBe('rejected');
      }

      // Verify error types: queued jobs get "closed", active jobs get "exited"
      const reasons = results.map((r) => (r as PromiseRejectedResult).reason?.message ?? '');
      const closedCount = reasons.filter((m) => m.includes('closed')).length;
      const exitedCount = reasons.filter((m) => m.includes('exited')).length;
      expect(closedCount + exitedCount).toBe(4);
      expect(closedCount).toBeGreaterThan(0);
      expect(exitedCount).toBeGreaterThan(0);
    } finally {
      await pool.close();
    }
  }, 15_000);

  it('should force-close within 3s', async () => {
    const pool = new SandboxPool(SLOW_PROCESSOR, true, 2, RUNNER_PATH);

    try {
      const promises = [
        pool.run(makeJob('force-1', { delay: 10_000 })),
        pool.run(makeJob('force-2', { delay: 10_000 })),
      ];

      // Attach rejection handlers BEFORE close() to prevent unhandled rejection warnings
      const settled = Promise.allSettled(promises);

      // Give workers time to start
      await new Promise((r) => setTimeout(r, 200));

      const start = Date.now();
      await pool.close(true);
      const elapsed = Date.now() - start;

      // FORCE_TIMEOUT=2s + generous CI buffer
      expect(elapsed).toBeLessThan(5_000);

      const results = await settled;
      for (const r of results) {
        expect(r.status).toBe('rejected');
      }
    } finally {
      await pool.close();
    }
  }, 10_000);
});
