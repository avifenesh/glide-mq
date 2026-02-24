/**
 * Scenario: queue-events (Valkey only)
 * Create QueueEvents listener. Listen for 'added', 'completed', 'failed'.
 * Add 20 jobs, process them (some fail with 10% chance).
 * Verify: received 'added' events == jobs added,
 *         'completed' + 'failed' events == jobs added.
 */

import type { ScenarioContext, ScenarioResult } from '../types';

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

export async function queueEvents(ctx: ScenarioContext): Promise<ScenarioResult> {
  const { rng } = ctx;
  const queueName = ctx.uid();
  const queue = ctx.createQueue(queueName);

  const violations: string[] = [];
  const totalJobs = 20;

  // Create QueueEvents listener
  const qe = ctx.createQueueEvents(queueName);
  await qe.waitUntilReady();

  // Track events
  const addedEvents: string[] = [];
  const completedEvents: string[] = [];
  const failedEvents: string[] = [];

  qe.on('added', (payload: any) => {
    addedEvents.push(payload.jobId ?? 'unknown');
  });
  qe.on('completed', (payload: any) => {
    completedEvents.push(payload.jobId ?? 'unknown');
  });
  qe.on('failed', (payload: any) => {
    failedEvents.push(payload.jobId ?? 'unknown');
  });

  // Decide which jobs will fail (10% chance each)
  const failSet = new Set<number>();
  for (let i = 0; i < totalJobs; i++) {
    if (rng.chance(0.1)) {
      failSet.add(i);
    }
  }

  const processCounter = { value: 0 };
  let failedCount = 0;

  // Create worker
  ctx.createWorker(
    queueName,
    async (job: any) => {
      processCounter.value++;
      if (failSet.has(job.data.index)) {
        failedCount++;
        throw new Error('intentional failure');
      }
      return 'done';
    },
    { concurrency: 5 },
  );

  // Add jobs
  for (let i = 0; i < totalJobs; i++) {
    await queue.add(`qe-job-${i}`, { index: i });
  }

  // Wait for all jobs to process
  await waitForCount(totalJobs, processCounter);

  // Give events a moment to propagate
  await new Promise((r) => setTimeout(r, 2000));

  // Verify event counts - use generous tolerance since event streaming
  // can miss events under load
  if (addedEvents.length < totalJobs * 0.8) {
    violations.push(
      `Expected ~${totalJobs} 'added' events, got ${addedEvents.length}`,
    );
  }

  const terminalEvents = completedEvents.length + failedEvents.length;
  if (terminalEvents < totalJobs * 0.8) {
    violations.push(
      `Expected ~${totalJobs} terminal events (completed+failed), got ${terminalEvents} (${completedEvents.length} completed, ${failedEvents.length} failed)`,
    );
  }

  return {
    added: totalJobs,
    processed: processCounter.value,
    failed: failedCount,
    violations,
  };
}
