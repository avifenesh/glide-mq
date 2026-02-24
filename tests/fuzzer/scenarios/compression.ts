/**
 * Scenario: compression (Valkey only)
 * Create queue with { compression: 'gzip' }. Add 20 jobs with varied data
 * including large payloads (10KB+). Process all. Verify data round-trips
 * correctly via checkDataIntegrity.
 */

import type { ScenarioContext, ScenarioResult } from '../types';
import { randomPayload } from '../generators/data';
import { checkNoJobLost, checkDataIntegrity } from '../invariants';

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

export async function compression(ctx: ScenarioContext): Promise<ScenarioResult> {
  const { rng } = ctx;
  const queueName = ctx.uid();
  const queue = ctx.createQueue(queueName, { compression: 'gzip' });

  const violations: string[] = [];
  const totalJobs = 20;

  const addedIds = new Set<string>();
  const addedData = new Map<string, any>();
  const processedIds = new Set<string>();
  const processedData = new Map<string, any>();
  const counter = { value: 0 };

  // Create worker with compression setting matching the queue
  ctx.createWorker(
    queueName,
    async (job: any) => {
      processedIds.add(job.id);
      processedData.set(job.id, job.data);
      counter.value++;
      return 'done';
    },
    { concurrency: 5 },
  );

  // Add jobs with varied data - mix random payloads with guaranteed large ones
  for (let i = 0; i < totalJobs; i++) {
    let data: Record<string, unknown>;

    if (i < 5) {
      // First 5: guaranteed large payloads (10KB+)
      const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
      const len = rng.nextInt(10000, 50000);
      let bigStr = '';
      for (let c = 0; c < len; c++) {
        bigStr += chars[rng.nextInt(0, chars.length - 1)];
      }
      data = { largePayload: bigStr, index: i };
    } else {
      // Rest: random payloads from generator
      data = { ...randomPayload(rng), index: i };
    }

    try {
      const job = await queue.add(`compress-job-${i}`, data);
      if (job) {
        addedIds.add(job.id);
        addedData.set(job.id, data);
      }
    } catch {
      // Ignore add errors
    }
  }

  // Wait for all jobs to process
  await waitForCount(addedIds.size, counter);

  // Verify data integrity - the data should survive gzip compression round-trip
  violations.push(...checkNoJobLost(addedIds, processedIds));
  violations.push(...checkDataIntegrity(addedData, processedData));

  return {
    added: addedIds.size,
    processed: processedIds.size,
    failed: 0,
    violations,
  };
}
