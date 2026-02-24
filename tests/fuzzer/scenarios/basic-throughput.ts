/**
 * Scenario: basic-throughput
 * N producers (1-5), M workers (1-4), random concurrency.
 * Each producer adds 20-80 jobs. Verify no job lost and data integrity.
 */

import type { ScenarioContext, ScenarioResult } from '../types';
import { randomJobOptions } from '../generators/job-options';
import { randomPayload } from '../generators/data';
import { checkNoJobLost, checkNoDuplicates, checkDataIntegrity } from '../invariants';

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

export async function basicThroughput(ctx: ScenarioContext): Promise<ScenarioResult> {
  const { rng } = ctx;
  const queueName = ctx.uid();
  const queue = ctx.createQueue(queueName);

  const numProducers = rng.nextInt(1, 5);
  const numWorkers = rng.nextInt(1, 4);

  const addedIds = new Set<string>();
  const addedData = new Map<string, any>();
  const processedIds = new Set<string>();
  const processedData = new Map<string, any>();
  const processLog: { jobId: string }[] = [];
  const counter = { value: 0 };
  let failedCount = 0;

  // Create workers first so they are ready when jobs arrive
  for (let w = 0; w < numWorkers; w++) {
    const concurrency = rng.nextInt(1, 8);
    ctx.createWorker(
      queueName,
      async (job: any) => {
        processedIds.add(job.id);
        processedData.set(job.id, job.data);
        processLog.push({ jobId: job.id });
        counter.value++;
        return 'done';
      },
      { concurrency },
    );
  }

  // Producers add jobs
  let totalAdded = 0;
  for (let p = 0; p < numProducers; p++) {
    const jobCount = rng.nextInt(20, 80);
    for (let i = 0; i < jobCount; i++) {
      const data = randomPayload(rng);
      const opts = randomJobOptions(rng);
      // Strip dedup to avoid reducing effective count
      delete opts.deduplication;
      // Strip delay so jobs process promptly
      delete opts.delay;
      // Strip ordering to avoid rate-limit stalls
      delete opts.ordering;
      // Strip cost/tokenBucket to avoid capacity errors
      delete opts.cost;

      try {
        const job = await queue.add(`job-${p}-${i}`, data, opts);
        if (job) {
          addedIds.add(job.id);
          addedData.set(job.id, data);
          totalAdded++;
        }
      } catch {
        // Ignore add errors (e.g. queue closing race)
      }
    }
  }

  // Settle delay for test mode
  if (ctx.mode === 'test') {
    await new Promise((r) => setTimeout(r, 100));
  }

  await waitForCount(totalAdded, counter);

  const violations: string[] = [];

  // Check no job lost
  const terminalIds = new Set([...processedIds]);
  // Also count failed jobs as terminal
  violations.push(...checkNoJobLost(addedIds, terminalIds));
  violations.push(...checkNoDuplicates(processLog));
  violations.push(...checkDataIntegrity(addedData, processedData));

  return {
    added: totalAdded,
    processed: processedIds.size,
    failed: failedCount,
    violations,
  };
}
