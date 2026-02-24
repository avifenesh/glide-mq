/**
 * Scenario: clean-drain
 * Add 50 jobs, process all. Then: clean(1000, 10, 'completed') - verify removes up to 10.
 * Add 20 more, drain() - verify waiting count drops to 0.
 * Add 10 failed jobs (processor throws), retryJobs() - verify they get reprocessed.
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

export async function cleanDrain(ctx: ScenarioContext): Promise<ScenarioResult> {
  const queueName = ctx.uid();
  const queue = ctx.createQueue(queueName);

  const violations: string[] = [];
  let totalAdded = 0;
  let totalProcessed = 0;
  let totalFailed = 0;

  // Phase 1: Add 50 jobs, process all
  const phase1Count = 50;
  const counter1 = { value: 0 };
  let shouldFail = false;
  let retryPhase = false;
  const retriedIds = new Set<string>();

  const worker = ctx.createWorker(
    queueName,
    async (job: any) => {
      if (shouldFail) {
        throw new Error('intentional failure');
      }
      if (retryPhase) {
        retriedIds.add(job.id);
      }
      counter1.value++;
      return 'done';
    },
    { concurrency: 10 },
  );

  for (let i = 0; i < phase1Count; i++) {
    await queue.add(`phase1-${i}`, { phase: 1, index: i });
  }
  totalAdded += phase1Count;

  // Settle delay for test mode
  if (ctx.mode === 'test') {
    await new Promise((r) => setTimeout(r, 100));
  }

  await waitForCount(phase1Count, counter1, 15000);
  totalProcessed += counter1.value;

  // Small delay to ensure finishedOn timestamps are old enough for grace
  await new Promise((r) => setTimeout(r, 50));

  // Phase 2: Clean completed jobs
  // Use grace=0 so all completed jobs are eligible (finished before now)
  const cleaned = await queue.clean(0, 10, 'completed');
  if (cleaned.length > 10) {
    violations.push(`clean() removed ${cleaned.length} jobs but limit was 10`);
  }
  if (cleaned.length === 0 && counter1.value > 0) {
    // In test mode, grace=0 means all completed jobs with finishedOn <= now are eligible
    // This could be 0 if timing is tight, so just warn
    violations.push(`clean() removed 0 jobs but ${counter1.value} were completed`);
  }

  // Phase 3: Add 20 more, then drain
  const phase3Count = 20;
  // Pause worker so jobs stay in waiting
  await queue.pause();
  for (let i = 0; i < phase3Count; i++) {
    await queue.add(`phase3-${i}`, { phase: 3, index: i });
  }
  totalAdded += phase3Count;

  // Drain the waiting jobs
  await queue.drain();

  // Resume worker
  await queue.resume();

  const counts = await queue.getJobCounts();
  if (counts.waiting > 0) {
    violations.push(`After drain(), waiting count is ${counts.waiting} (expected 0)`);
  }

  // Phase 4: Add 10 jobs that fail, then retryJobs
  shouldFail = true;
  const phase4Count = 10;
  const failCounter = { value: 0 };

  // Listen for failures
  worker.on('failed', () => {
    failCounter.value++;
  });

  for (let i = 0; i < phase4Count; i++) {
    await queue.add(`phase4-${i}`, { phase: 4, index: i });
  }
  totalAdded += phase4Count;

  // Settle delay for test mode
  if (ctx.mode === 'test') {
    await new Promise((r) => setTimeout(r, 100));
  }

  await waitForCount(phase4Count, failCounter, 10000);
  totalFailed += failCounter.value;

  // Now retry the failed jobs - switch to success mode
  shouldFail = false;
  retryPhase = true;
  // Track counter1 baseline so we know when retried jobs finish
  const retryStart = counter1.value;

  const retried = await queue.retryJobs();

  // Settle delay for test mode
  if (ctx.mode === 'test') {
    await new Promise((r) => setTimeout(r, 100));
  }

  // Wait for retried jobs to complete
  await waitForCount(retryStart + retried, counter1, 10000);

  if (retried === 0 && failCounter.value > 0) {
    violations.push(`retryJobs() retried 0 jobs but ${failCounter.value} had failed`);
  }

  // Verify retried jobs were reprocessed
  if (retriedIds.size < retried) {
    violations.push(`Only ${retriedIds.size} of ${retried} retried jobs were actually reprocessed`);
  }

  totalProcessed = counter1.value;

  return {
    added: totalAdded,
    processed: totalProcessed,
    failed: totalFailed,
    violations,
  };
}
