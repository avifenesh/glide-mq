/**
 * Scenario: graceful-shutdown (both modes)
 * Create queue + worker. Add 30 jobs. Start processing.
 * Close worker gracefully via worker.close(false). Verify it completes
 * within 10s and no errors thrown.
 */

import type { ScenarioContext, ScenarioResult } from '../types';

export async function gracefulShutdown(ctx: ScenarioContext): Promise<ScenarioResult> {
  const queueName = ctx.uid();
  const queue = ctx.createQueue(queueName);

  const violations: string[] = [];
  const totalJobs = 30;
  const processedIds = new Set<string>();
  let errorOccurred = false;

  // Create worker
  const worker = ctx.createWorker(
    queueName,
    async (job: any) => {
      processedIds.add(job.id);
      // Simulate some work
      await new Promise((r) => setTimeout(r, 10));
      return 'done';
    },
    { concurrency: 5 },
  );

  worker.on('error', () => {
    errorOccurred = true;
  });

  // Add jobs
  for (let i = 0; i < totalJobs; i++) {
    await queue.add(`shutdown-job-${i}`, { index: i });
  }

  // Settle delay for test mode
  if (ctx.mode === 'test') {
    await new Promise((r) => setTimeout(r, 100));
  }

  // Let some jobs start processing
  await new Promise((r) => setTimeout(r, 500));

  // Gracefully close worker - should wait for active jobs to finish
  const closeStart = Date.now();
  try {
    await Promise.race([
      worker.close(false),
      new Promise<void>((_, reject) => setTimeout(() => reject(new Error('close() timed out after 10s')), 10000)),
    ]);
  } catch (err: any) {
    if (err.message.includes('timed out')) {
      violations.push('worker.close(false) did not complete within 10 seconds');
    } else {
      violations.push(`worker.close() threw: ${err.message}`);
    }
  }
  const closeElapsed = Date.now() - closeStart;

  if (closeElapsed > 10000) {
    violations.push(`Graceful shutdown took ${closeElapsed}ms (expected < 10000ms)`);
  }

  if (errorOccurred) {
    violations.push('Unhandled error occurred during graceful shutdown');
  }

  // Some jobs should have been processed before shutdown
  if (processedIds.size === 0) {
    violations.push('No jobs were processed before shutdown');
  }

  return {
    added: totalJobs,
    processed: processedIds.size,
    failed: 0,
    violations,
  };
}
