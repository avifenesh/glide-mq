/**
 * Scenario: sandbox (Valkey only)
 * Create a simple processor file at a temp path that returns { ok: true }.
 * Create worker with sandbox option and the file path as processor string.
 * Add 10 jobs. Verify all complete. Clean up temp file.
 */

import type { ScenarioContext, ScenarioResult } from '../types';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

function waitForCount(target: number, counter: { value: number }, timeoutMs = 25000): Promise<void> {
  return new Promise((resolve) => {
    const start = Date.now();
    const check = () => {
      if (counter.value >= target) return resolve();
      if (Date.now() - start > timeoutMs) return resolve();
      setTimeout(check, 50);
    };
    check();
  });
}

export async function sandbox(ctx: ScenarioContext): Promise<ScenarioResult> {
  const queueName = ctx.uid();
  const violations: string[] = [];
  const totalJobs = 10;

  // Write a temporary processor file
  const tmpFile = path.join(os.tmpdir(), `fuzz-processor-${Date.now()}.cjs`);
  fs.writeFileSync(tmpFile, 'module.exports = async (job) => ({ processed: job.name, ok: true });\n');

  try {
    const queue = ctx.createQueue(queueName);

    const completedIds = new Set<string>();
    const counter = { value: 0 };

    // Create worker with sandbox processor (file path as string)
    // The Worker constructor accepts a string as processor, which triggers sandbox mode
    const connection =
      ctx.mode === 'cluster'
        ? { addresses: [{ host: '127.0.0.1', port: 7000 }], clusterMode: true }
        : { addresses: [{ host: 'localhost', port: 6379 }] };

    // We need to use the real Worker directly since ctx.createWorker
    // expects a function processor. Import Worker from dist.
    const { Worker } = require('../../../dist/worker') as typeof import('../../../src/worker');

    const worker = new Worker(queueName, tmpFile, {
      connection,
      concurrency: 3,
      sandbox: {},
    });

    worker.on('completed', (job: any) => {
      completedIds.add(job.id);
      counter.value++;
    });

    worker.on('error', (err: any) => {
      // Log but don't fail - sandbox errors can be transient
      violations.push(`Worker error: ${err.message}`);
    });

    // Add jobs
    for (let i = 0; i < totalJobs; i++) {
      await queue.add(`sandbox-job-${i}`, { index: i });
    }

    // Wait for all jobs to complete
    await waitForCount(totalJobs, counter);

    if (completedIds.size < totalJobs) {
      violations.push(`Expected ${totalJobs} completed jobs, got ${completedIds.size}`);
    }

    // Close worker (force to avoid hanging on sandbox pool shutdown)
    await worker.close(true);

    return {
      added: totalJobs,
      processed: completedIds.size,
      failed: 0,
      violations,
    };
  } finally {
    // Clean up temp file
    try {
      fs.unlinkSync(tmpFile);
    } catch {
      // Ignore cleanup errors
    }
  }
}
