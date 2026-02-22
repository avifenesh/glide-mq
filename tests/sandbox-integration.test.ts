/**
 * Integration tests for sandboxed processor.
 * Requires: valkey-server running on localhost:6379 and cluster on :7000-7005
 *
 * Run: npx vitest run tests/sandbox-integration.test.ts
 */
import path from 'path';
import { it, expect, beforeAll, afterAll, afterEach } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');

import { describeEachMode, createCleanupClient, flushQueue, waitFor } from './helpers/fixture';

const ECHO_PROCESSOR = path.resolve(__dirname, 'fixtures/processors/echo.js');
const PROGRESS_PROCESSOR = path.resolve(__dirname, 'fixtures/processors/progress.js');
const CRASH_PROCESSOR = path.resolve(__dirname, 'fixtures/processors/crash.js');
const ABORT_PROCESSOR = path.resolve(__dirname, 'fixtures/processors/abort-aware.js');

describeEachMode('Sandboxed Processor', (CONNECTION) => {
  let cleanupClient: any;
  const workers: any[] = [];
  const queues: any[] = [];

  function makeQueue() {
    const q = new Queue('test-sandbox-' + Date.now() + '-' + Math.random().toString(36).slice(2), {
      connection: CONNECTION,
    });
    queues.push(q);
    return q;
  }

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterEach(async () => {
    for (const w of workers.splice(0)) {
      try {
        await w.close();
      } catch {
        // ignore close errors in cleanup
      }
    }
  });

  afterAll(async () => {
    for (const q of queues) {
      try {
        await q.close();
        await flushQueue(cleanupClient, q.name);
      } catch {
        // ignore
      }
    }
    cleanupClient.close();
  });

  it('processes a job with a file-path processor', async () => {
    const queue = makeQueue();
    const worker = new Worker(queue.name, ECHO_PROCESSOR, {
      connection: CONNECTION,
      concurrency: 1,
    });
    workers.push(worker);

    const completedPromise = new Promise<any>((resolve) => {
      worker.on('completed', (_job: any, result: any) => resolve(result));
    });

    const job = await queue.add('echo-test', { message: 'hello sandbox' });
    expect(job.id).toBeTruthy();

    const result = await completedPromise;
    expect(result).toEqual({ message: 'hello sandbox' });
  }, 60_000);

  it('proxies updateProgress and log from sandbox', async () => {
    const queue = makeQueue();
    const worker = new Worker(queue.name, PROGRESS_PROCESSOR, {
      connection: CONNECTION,
      concurrency: 1,
    });
    workers.push(worker);

    const completedPromise = new Promise<any>((resolve) => {
      worker.on('completed', (job: any) => resolve(job));
    });

    await queue.add('progress-test', { v: 1 });

    const job = await completedPromise;
    expect(job.returnvalue).toEqual({ v: 1 });

    // Verify progress was written to Valkey
    const fetched = await queue.getJob(job.id);
    expect(fetched).not.toBeNull();
    expect(fetched!.progress).toBe(100);
  }, 60_000);

  it('handles processor crash and continues processing', async () => {
    const queue = makeQueue();
    const worker = new Worker(queue.name, CRASH_PROCESSOR, {
      connection: CONNECTION,
      concurrency: 1,
    });
    workers.push(worker);

    const failedPromise = new Promise<any>((resolve) => {
      worker.on('failed', (job: any, err: any) => resolve({ job, err }));
    });

    // Also listen for errors to aid debugging
    worker.on('error', (err: any) => {
      // Expected: pool.run() rejection bubbles here after crash
    });

    await queue.add('crash-test', { x: 1 });

    const { err } = await failedPromise;
    expect(err.message).toMatch(/exited with code/);
  }, 60_000);

  it('processes concurrent sandboxed jobs', async () => {
    const queue = makeQueue();
    const worker = new Worker(queue.name, ECHO_PROCESSOR, {
      connection: CONNECTION,
      concurrency: 3,
      sandbox: { maxWorkers: 3 },
    });
    workers.push(worker);

    const results: any[] = [];
    const completedPromise = new Promise<void>((resolve) => {
      worker.on('completed', (_job: any, result: any) => {
        results.push(result);
        if (results.length === 3) resolve();
      });
    });

    await queue.add('c1', { n: 1 });
    await queue.add('c2', { n: 2 });
    await queue.add('c3', { n: 3 });

    await completedPromise;
    expect(results).toHaveLength(3);

    const values = results.map((r) => r.n).sort();
    expect(values).toEqual([1, 2, 3]);
  }, 60_000);

  it('Worker.close(false) waits for sandboxed jobs', async () => {
    const queue = makeQueue();
    const worker = new Worker(queue.name, ECHO_PROCESSOR, {
      connection: CONNECTION,
      concurrency: 1,
    });
    workers.push(worker);

    let completed = false;
    worker.on('completed', () => {
      completed = true;
    });

    await queue.add('close-test', { graceful: true });

    await waitFor(() => completed, 15_000);

    // Graceful close should not throw
    await worker.close(false);
    expect(completed).toBe(true);
  }, 60_000);

  it('abort signal propagates to sandboxed processor', async () => {
    const queue = makeQueue();
    const worker = new Worker(queue.name, ABORT_PROCESSOR, {
      connection: CONNECTION,
      concurrency: 1,
    });
    workers.push(worker);

    const failedPromise = new Promise<any>((resolve) => {
      worker.on('failed', (_job: any, err: any) => resolve(err));
    });

    const job = await queue.add('abort-test', {});
    const jobId = job.id!;

    // Poll until worker has registered the abort controller for this job
    await waitFor(() => worker.abortJob(jobId), 15_000);

    const err = await failedPromise;
    expect(err.message).toMatch(/revoked/);
  }, 60_000);
});
