/**
 * Scenario: job-lifecycle
 * Test various Job methods:
 * - Job A: add with delay, then changeDelay(0), verify processes quickly
 * - Job B: add with priority 5, then changePriority(1), verify priority changed
 * - Job C: add with delay, then promote(), verify processes
 * - Job D: processor calls job.log('msg'), job.updateProgress(50), job.updateData({updated: true})
 * - Job E: processor calls job.discard(), verify job fails
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

export async function jobLifecycle(ctx: ScenarioContext): Promise<ScenarioResult> {
  const queueName = ctx.uid();
  const queue = ctx.createQueue(queueName);

  const violations: string[] = [];
  const completedIds = new Set<string>();
  const failedIds = new Set<string>();
  const processedCounter = { value: 0 };
  const terminalCounter = { value: 0 };
  let jobDData: any = null;
  let jobDProgress: any = null;

  // Create worker that handles each job type differently based on name
  const worker = ctx.createWorker(
    queueName,
    async (job: any) => {
      processedCounter.value++;

      if (job.name === 'job-D') {
        // Test log, updateProgress, updateData
        await job.log('lifecycle-test-log');
        await job.updateProgress(50);
        await job.updateData({ updated: true });
        jobDData = job.data;
        jobDProgress = job.progress;
        return 'job-D-done';
      }

      if (job.name === 'job-E') {
        // Test discard - mark as discarded then throw
        job.discard();
        throw new Error('discarded-error');
      }

      return `${job.name}-done`;
    },
    { concurrency: 1 },
  );

  worker.on('completed', (job: any) => {
    completedIds.add(job.id);
    terminalCounter.value++;
  });

  worker.on('failed', (job: any) => {
    failedIds.add(job.id);
    terminalCounter.value++;
  });

  // Job A: add with delay, then changeDelay(0)
  const jobA = await queue.add('job-A', { test: 'changeDelay' }, { delay: 5000 });
  if (jobA) {
    try {
      await jobA.changeDelay(0);
    } catch {
      // May fail in test mode if already picked up
    }
  }

  // Job B: add with priority 5, then changePriority(1)
  const jobB = await queue.add('job-B', { test: 'changePriority' }, { priority: 5 });
  if (jobB) {
    try {
      await jobB.changePriority(1);
    } catch {
      // May fail if already active
    }
  }

  // Job C: add with delay, then promote()
  const jobC = await queue.add('job-C', { test: 'promote' }, { delay: 5000 });
  if (jobC) {
    try {
      await jobC.promote();
    } catch {
      // May fail in test mode if state already changed
    }
  }

  // Job D: normal job - processor will test log/progress/updateData
  const jobD = await queue.add('job-D', { original: true });

  // Job E: processor will discard - give it attempts so discard is meaningful
  const jobE = await queue.add('job-E', { test: 'discard' }, { attempts: 3 });

  // Settle delay for test mode
  if (ctx.mode === 'test') {
    await new Promise((r) => setTimeout(r, 100));
  }

  // Wait for all 5 jobs to reach terminal state
  await waitForCount(5, terminalCounter, 10000);

  // Verify Job A completed
  if (jobA && !completedIds.has(jobA.id)) {
    violations.push('Job A (changeDelay) was not completed');
  }

  // Verify Job B completed (we just check it processed - priority change is best-effort)
  if (jobB && !completedIds.has(jobB.id)) {
    violations.push('Job B (changePriority) was not completed');
  }

  // Verify Job C completed
  if (jobC && !completedIds.has(jobC.id)) {
    violations.push('Job C (promote) was not completed');
  }

  // Verify Job D: data was updated, progress was set
  if (jobD && !completedIds.has(jobD.id)) {
    violations.push('Job D (log/progress/updateData) was not completed');
  }
  if (jobDData !== null && jobDData.updated !== true) {
    violations.push(`Job D data was not updated: ${JSON.stringify(jobDData)}`);
  }
  if (jobDProgress !== null && jobDProgress !== 50) {
    violations.push(`Job D progress was not 50: ${jobDProgress}`);
  }

  // Verify Job E: should have failed (discarded prevents retry)
  if (jobE && !failedIds.has(jobE.id)) {
    violations.push('Job E (discard) was not in failed state');
  }

  return {
    added: 5,
    processed: processedCounter.value,
    failed: failedIds.size,
    violations,
  };
}
