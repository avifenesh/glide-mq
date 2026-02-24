/**
 * Scenario: add-bulk
 * Use queue.addBulk with 10-100 jobs, random opts.
 * Verify returned array length matches input. Process all, verify no loss.
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

export async function addBulk(ctx: ScenarioContext): Promise<ScenarioResult> {
  const { rng } = ctx;
  const queueName = ctx.uid();
  const queue = ctx.createQueue(queueName);

  const jobCount = rng.nextInt(10, 100);
  const bulkJobs: { name: string; data: any; opts?: any }[] = [];

  for (let i = 0; i < jobCount; i++) {
    const data = randomPayload(rng);
    const opts = randomJobOptions(rng);
    // Strip features that reduce effective count or cause timeouts
    delete opts.deduplication;
    delete opts.delay;
    delete opts.ordering;
    delete opts.cost;
    bulkJobs.push({ name: `bulk-${i}`, data, opts });
  }

  const addedJobs = await queue.addBulk(bulkJobs);

  const violations: string[] = [];

  // Verify returned array length matches input
  if (addedJobs.length !== jobCount) {
    violations.push(
      `addBulk returned ${addedJobs.length} jobs but expected ${jobCount}`,
    );
  }

  const addedIds = new Set<string>();
  const addedData = new Map<string, any>();
  for (let i = 0; i < addedJobs.length; i++) {
    addedIds.add(addedJobs[i].id);
    addedData.set(addedJobs[i].id, bulkJobs[i].data);
  }

  const processedIds = new Set<string>();
  const processedData = new Map<string, any>();
  const processLog: { jobId: string }[] = [];
  const counter = { value: 0 };

  const concurrency = rng.nextInt(1, 10);
  ctx.createWorker(
    queueName,
    async (job: any) => {
      processedIds.add(job.id);
      processedData.set(job.id, job.data);
      processLog.push({ jobId: job.id });
      counter.value++;
      return 'ok';
    },
    { concurrency },
  );

  // Settle delay for test mode
  if (ctx.mode === 'test') {
    await new Promise((r) => setTimeout(r, 100));
  }

  await waitForCount(addedJobs.length, counter);

  const terminalIds = new Set([...processedIds]);
  violations.push(...checkNoJobLost(addedIds, terminalIds));
  violations.push(...checkNoDuplicates(processLog));
  violations.push(...checkDataIntegrity(addedData, processedData));

  return {
    added: addedJobs.length,
    processed: processedIds.size,
    failed: 0,
    violations,
  };
}
