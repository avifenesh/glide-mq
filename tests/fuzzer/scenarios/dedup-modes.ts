/**
 * Scenario: dedup-modes
 * Add 50 jobs with 5 dedup IDs (10 per ID). Mode: 'simple'.
 * Verify only 5 unique jobs are processed (one per dedup ID).
 * For test mode, only 'simple' dedup is supported.
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

export async function dedupModes(ctx: ScenarioContext): Promise<ScenarioResult> {
  const { rng } = ctx;
  const queueName = ctx.uid();
  // Enable dedup support for test mode
  const queue = ctx.createQueue(queueName, ctx.mode === 'test' ? { dedup: true } : undefined);

  const numDedupIds = 5;
  const jobsPerDedupId = 10;
  const totalJobs = numDedupIds * jobsPerDedupId;

  const processedDedupIds = new Set<string>();
  const processedJobIds = new Set<string>();
  const counter = { value: 0 };
  let addedCount = 0;
  let skippedCount = 0;

  // Add all jobs BEFORE creating worker to avoid dedup keys being
  // cleared by completed jobs between sequential adds

  // Add jobs with dedup IDs - shuffle order for randomness
  const jobSpecs: { dedupId: string; index: number }[] = [];
  for (let d = 0; d < numDedupIds; d++) {
    for (let i = 0; i < jobsPerDedupId; i++) {
      jobSpecs.push({ dedupId: `dedup-group-${d}`, index: d * jobsPerDedupId + i });
    }
  }
  rng.shuffle(jobSpecs);

  for (const spec of jobSpecs) {
    const job = await queue.add(
      `dedup-job-${spec.index}`,
      { dedupId: spec.dedupId, index: spec.index },
      {
        deduplication: {
          id: spec.dedupId,
          mode: 'simple',
        },
      },
    );
    if (job) {
      addedCount++;
    } else {
      skippedCount++;
    }
  }

  // Now create worker to process the queued jobs
  ctx.createWorker(
    queueName,
    async (job: any) => {
      processedDedupIds.add(job.data.dedupId);
      processedJobIds.add(job.id);
      counter.value++;
      return 'done';
    },
    { concurrency: 1 },
  );

  // Settle delay for test mode
  if (ctx.mode === 'test') {
    await new Promise((r) => setTimeout(r, 100));
  }

  // Only the non-skipped jobs should be processed
  await waitForCount(addedCount, counter, 10000);

  const violations: string[] = [];

  // Verify exactly 5 unique dedup IDs were processed
  if (processedDedupIds.size !== numDedupIds) {
    violations.push(`Expected ${numDedupIds} unique dedup IDs processed, got ${processedDedupIds.size}`);
  }

  // Verify total processed matches added (non-skipped)
  if (addedCount !== numDedupIds) {
    violations.push(
      `Expected ${numDedupIds} jobs added (rest skipped by dedup), got ${addedCount} added and ${skippedCount} skipped`,
    );
  }

  // Verify skipped count is as expected
  const expectedSkipped = totalJobs - numDedupIds;
  if (skippedCount !== expectedSkipped) {
    violations.push(`Expected ${expectedSkipped} jobs skipped by dedup, got ${skippedCount}`);
  }

  return {
    added: addedCount,
    processed: counter.value,
    failed: 0,
    violations,
  };
}
