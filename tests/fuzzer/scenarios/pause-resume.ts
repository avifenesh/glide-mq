/**
 * Scenario: pause-resume
 * Add 30 jobs. Start worker. Run 15 rapid pause/resume cycles
 * (100ms pause, 50ms gap). Verify all jobs eventually complete.
 */

import type { ScenarioContext, ScenarioResult } from '../types';
import { checkNoJobLost } from '../invariants';

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

export async function pauseResume(ctx: ScenarioContext): Promise<ScenarioResult> {
  const { rng } = ctx;
  const queueName = ctx.uid();
  const queue = ctx.createQueue(queueName);

  const totalJobs = 30;
  const addedIds = new Set<string>();
  const processedIds = new Set<string>();
  const counter = { value: 0 };

  // Create worker with random concurrency
  const concurrency = rng.nextInt(1, 5);
  ctx.createWorker(
    queueName,
    async (job: any) => {
      processedIds.add(job.id);
      counter.value++;
      // Small random work time
      await new Promise((r) => setTimeout(r, rng.nextInt(1, 10)));
      return 'done';
    },
    { concurrency },
  );

  // Add jobs
  for (let i = 0; i < totalJobs; i++) {
    const job = await queue.add(`pause-test-${i}`, { index: i });
    if (job) {
      addedIds.add(job.id);
    }
  }

  // Settle delay for test mode
  if (ctx.mode === 'test') {
    await new Promise((r) => setTimeout(r, 100));
  }

  // Run 15 rapid pause/resume cycles
  const cycles = 15;
  for (let c = 0; c < cycles; c++) {
    try {
      await queue.pause();
      await new Promise((r) => setTimeout(r, 100));
      await queue.resume();
      await new Promise((r) => setTimeout(r, 50));
    } catch {
      // Ignore errors from racing with queue operations
    }
  }

  // Ensure queue is resumed at the end
  try {
    await queue.resume();
  } catch {
    // Ignore
  }

  // Wait for all jobs to complete
  await waitForCount(totalJobs, counter);

  const violations: string[] = [];
  violations.push(...checkNoJobLost(addedIds, processedIds));

  return {
    added: addedIds.size,
    processed: processedIds.size,
    failed: 0,
    violations,
  };
}
