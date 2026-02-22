/**
 * Integration tests for sandboxed processor.
 * Requires: valkey-server running on localhost:6379 and cluster on :7000-7005
 *
 * Run: npx vitest run tests/sandbox-integration.test.ts
 */
import path from 'path';
import { it, expect, beforeAll, afterAll } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');

import { describeEachMode, createCleanupClient, flushQueue, waitFor } from './helpers/fixture';

const ECHO_PROCESSOR = path.resolve(__dirname, 'fixtures/processors/echo.js');
const PROGRESS_PROCESSOR = path.resolve(__dirname, 'fixtures/processors/progress.js');
const CRASH_PROCESSOR = path.resolve(__dirname, 'fixtures/processors/crash.js');

describeEachMode('Sandboxed Processor', (CONNECTION) => {
  const Q = 'test-sandbox-' + Date.now();
  let queue: InstanceType<typeof Queue>;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
    queue = new Queue(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('processes a job with a file-path processor', async () => {
    const worker = new Worker(Q, ECHO_PROCESSOR, {
      connection: CONNECTION,
      concurrency: 1,
    });

    const completedPromise = new Promise<any>((resolve) => {
      worker.on('completed', (job: any, result: any) => resolve(result));
    });

    const job = await queue.add('echo-test', { message: 'hello sandbox' });
    expect(job.id).toBeTruthy();

    const result = await completedPromise;
    expect(result).toEqual({ message: 'hello sandbox' });

    await worker.close();
  });

  it('proxies updateProgress and log from sandbox', async () => {
    const worker = new Worker(Q, PROGRESS_PROCESSOR, {
      connection: CONNECTION,
      concurrency: 1,
    });

    const completedPromise = new Promise<any>((resolve) => {
      worker.on('completed', (job: any) => resolve(job));
    });

    await queue.add('progress-test', { v: 1 });

    const job = await completedPromise;
    expect(job.returnvalue).toEqual({ v: 1 });

    // Verify progress was written to Valkey
    const fetched = await queue.getJob(job.id);
    expect(fetched).not.toBeNull();
    // Progress should be 100 (the last update)
    expect(fetched!.progress).toBe(100);

    await worker.close();
  });

  it('handles processor crash and continues processing', async () => {
    const worker = new Worker(Q, CRASH_PROCESSOR, {
      connection: CONNECTION,
      concurrency: 1,
    });

    const failedPromise = new Promise<any>((resolve) => {
      worker.on('failed', (job: any, err: any) => resolve({ job, err }));
    });

    await queue.add('crash-test', { x: 1 });

    const { err } = await failedPromise;
    expect(err.message).toMatch(/exited with code/);

    await worker.close();
  });

  it('processes concurrent sandboxed jobs', async () => {
    const worker = new Worker(Q, ECHO_PROCESSOR, {
      connection: CONNECTION,
      concurrency: 3,
      sandbox: { maxWorkers: 3 },
    });

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

    await worker.close();
  });

  it('Worker.close(false) waits for sandboxed jobs', async () => {
    const worker = new Worker(Q, ECHO_PROCESSOR, {
      connection: CONNECTION,
      concurrency: 1,
    });

    let completed = false;
    worker.on('completed', () => {
      completed = true;
    });

    await queue.add('close-test', { graceful: true });

    // Wait a tiny bit for the job to be picked up
    await waitFor(() => completed, 5000);

    // Graceful close should not throw
    await worker.close(false);
    expect(completed).toBe(true);
  });
});
