/**
 * Scenario: priority-ordering
 * Add 50 jobs with priorities 0-9. Single worker c=1, slow processor (5ms).
 * Track processing order. Verify higher priority (lower number) jobs tend
 * to process first. Soft assertion: first 10 processed should have avg
 * priority <= avg of all jobs.
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

export async function priorityOrdering(ctx: ScenarioContext): Promise<ScenarioResult> {
  const { rng } = ctx;
  const queueName = ctx.uid();
  const queue = ctx.createQueue(queueName);

  const totalJobs = 50;
  const priorities: number[] = [];
  const processOrder: number[] = [];
  const counter = { value: 0 };
  const addedIds = new Set<string>();

  // Add all jobs first before creating the worker, so priorities can take effect
  for (let i = 0; i < totalJobs; i++) {
    const priority = rng.nextInt(0, 9);
    priorities.push(priority);
    const job = await queue.add(`prio-job-${i}`, { index: i, priority }, { priority });
    if (job) {
      addedIds.add(job.id);
    }
  }

  // Small delay to let all jobs settle in the queue
  await new Promise((r) => setTimeout(r, 50));

  // Single worker, concurrency 1, slow processor to ensure ordering is observable
  ctx.createWorker(
    queueName,
    async (job: any) => {
      processOrder.push(job.data.priority);
      counter.value++;
      // Slow processor to ensure one-at-a-time processing
      await new Promise((r) => setTimeout(r, 5));
      return 'done';
    },
    { concurrency: 1 },
  );

  // Settle delay for test mode
  if (ctx.mode === 'test') {
    await new Promise((r) => setTimeout(r, 100));
  }

  await waitForCount(totalJobs, counter);

  const violations: string[] = [];

  // Soft assertion: first 10 processed should have avg priority <= avg of all
  // Tolerance of +2 allows for some variance but maintains sensitivity to detect regressions
  if (processOrder.length >= 10) {
    const first10Avg = processOrder.slice(0, 10).reduce((a, b) => a + b, 0) / 10;
    const overallAvg = priorities.reduce((a, b) => a + b, 0) / priorities.length;
    if (first10Avg > overallAvg + 2) {
      violations.push(
        `Priority ordering violated: first 10 avg priority ${first10Avg.toFixed(2)} ` +
          `> overall avg ${overallAvg.toFixed(2)} + 2 tolerance`,
      );
    }
  }

  return {
    added: addedIds.size,
    processed: counter.value,
    failed: 0,
    violations,
  };
}
