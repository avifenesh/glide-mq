/**
 * Scenario: delayed-promote
 * Add 20 jobs: half with delay=5000ms, half immediate.
 * For delayed jobs, randomly promote() some and changeDelay(0) others.
 * Verify: promoted/changeDelay(0) jobs complete quickly (within 5s),
 * truly delayed ones haven't completed yet (or completed after their delay).
 * For test mode, TestJob has promote() and changeDelay().
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

export async function delayedPromote(ctx: ScenarioContext): Promise<ScenarioResult> {
  const { rng } = ctx;
  const queueName = ctx.uid();
  const queue = ctx.createQueue(queueName);

  const totalJobs = 20;
  const halfPoint = totalJobs / 2;
  const completedIds = new Set<string>();
  const completionTimes = new Map<string, number>();
  const counter = { value: 0 };
  const startTime = Date.now();

  // Create worker
  ctx.createWorker(
    queueName,
    async (job: any) => {
      completedIds.add(job.id);
      completionTimes.set(job.id, Date.now() - startTime);
      counter.value++;
      return 'done';
    },
    { concurrency: 5 },
  );

  const immediateJobs: any[] = [];
  const delayedJobs: any[] = [];
  const promotedJobIds = new Set<string>();
  const keepDelayedJobIds = new Set<string>();

  // Add immediate jobs (first half)
  for (let i = 0; i < halfPoint; i++) {
    const job = await queue.add(`immediate-${i}`, { index: i });
    if (job) immediateJobs.push(job);
  }

  // Add delayed jobs (second half) with 5s delay
  for (let i = 0; i < halfPoint; i++) {
    const job = await queue.add(`delayed-${i}`, { index: i }, { delay: 5000 });
    if (job) delayedJobs.push(job);
  }

  // For delayed jobs, promote some, changeDelay(0) others, leave some delayed
  for (const job of delayedJobs) {
    const action = rng.nextInt(0, 2);
    if (action === 0) {
      // Promote
      try {
        await job.promote();
        promotedJobIds.add(job.id);
      } catch {
        // May fail if already processed or state changed
      }
    } else if (action === 1) {
      // changeDelay(0)
      try {
        await job.changeDelay(0);
        promotedJobIds.add(job.id);
      } catch {
        // May fail if already processed or state changed
      }
    } else {
      // Keep delayed
      keepDelayedJobIds.add(job.id);
    }
  }

  // Settle delay for test mode
  if (ctx.mode === 'test') {
    await new Promise((r) => setTimeout(r, 100));
  }

  // Wait for immediate + promoted jobs to complete
  const expectedQuick = immediateJobs.length + promotedJobIds.size;
  await waitForCount(expectedQuick, counter, 5000);

  const violations: string[] = [];

  // Verify immediate jobs all completed
  for (const job of immediateJobs) {
    if (!completedIds.has(job.id)) {
      violations.push(`Immediate job ${job.id} was not completed`);
    }
  }

  // Verify promoted/changeDelay(0) jobs completed within 5s
  for (const jobId of promotedJobIds) {
    const elapsed = completionTimes.get(jobId);
    if (elapsed !== undefined && elapsed > 5000) {
      violations.push(`Promoted job ${jobId} took ${elapsed}ms to complete (expected < 5000ms)`);
    }
  }

  // Verify truly delayed jobs have NOT yet completed (they have 5s delay, we only waited ~5s total)
  // This is a soft check - in test mode delayed jobs may process immediately
  // since TestQueue doesn't enforce actual delays
  if (ctx.mode !== 'test') {
    for (const jobId of keepDelayedJobIds) {
      const elapsed = completionTimes.get(jobId);
      if (elapsed !== undefined && elapsed < 3000) {
        violations.push(`Delayed job ${jobId} completed in ${elapsed}ms but should have been delayed ~5000ms`);
      }
    }
  }

  return {
    added: totalJobs,
    processed: counter.value,
    failed: 0,
    violations,
  };
}
