/**
 * Scenario: rate-limiting (Valkey only)
 * Set global rate limit: { max: 5, duration: 1000 }. Add 20 jobs.
 * Start worker. Measure elapsed time. Verify took at least 3 seconds
 * (20 jobs at 5/sec = 4 seconds minimum, but allow generous tolerance).
 * Then removeGlobalRateLimit().
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

export async function rateLimiting(ctx: ScenarioContext): Promise<ScenarioResult> {
  const queueName = ctx.uid();
  const queue = ctx.createQueue(queueName);

  const violations: string[] = [];
  const totalJobs = 20;
  const counter = { value: 0 };

  // Set global rate limit before adding jobs: 5 jobs per second
  await queue.setGlobalRateLimit({ max: 5, duration: 1000 });

  // Create worker - it will respect the global rate limit
  ctx.createWorker(
    queueName,
    async () => {
      counter.value++;
      return 'done';
    },
    { concurrency: 10 },
  );

  // Add jobs
  for (let i = 0; i < totalJobs; i++) {
    await queue.add(`rate-job-${i}`, { index: i });
  }

  const startTime = Date.now();

  // Wait for all jobs to complete
  await waitForCount(totalJobs, counter);

  const elapsed = Date.now() - startTime;

  // 20 jobs at 5/sec = 4 seconds minimum.
  // Use generous tolerance: at least 3 seconds (allows for timing variance).
  // This is a soft assertion - timing can be flaky in CI.
  if (elapsed < 3000) {
    violations.push(
      `Rate limiting too fast: ${totalJobs} jobs at 5/sec completed in ${elapsed}ms (expected >= 3000ms)`,
    );
  }

  // Clean up: remove the global rate limit
  try {
    await queue.removeGlobalRateLimit();
  } catch {
    // Ignore cleanup errors
  }

  return {
    added: totalJobs,
    processed: counter.value,
    failed: 0,
    violations,
  };
}
