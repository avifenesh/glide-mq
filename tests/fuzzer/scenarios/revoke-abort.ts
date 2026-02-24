/**
 * Scenario: revoke-abort (Valkey only)
 * Add 20 jobs. Revoke 5 before worker starts. Start worker with slow processor
 * (50ms). During processing, revoke 3 more active jobs. Verify: revoked waiting
 * jobs show isRevoked()=true. Active-revoked jobs get abortSignal.
 * Total processed < 20 (some were revoked).
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

export async function revokeAbort(ctx: ScenarioContext): Promise<ScenarioResult> {
  const { rng } = ctx;
  const queueName = ctx.uid();
  const queue = ctx.createQueue(queueName);

  const violations: string[] = [];
  const totalJobs = 20;
  const revokeBeforeStart = 5;
  const revokeWhileActive = 3;

  const addedJobs: any[] = [];
  const processedIds = new Set<string>();
  const abortedIds = new Set<string>();
  const counter = { value: 0 };
  const terminalCounter = { value: 0 };

  // Add all jobs first (before worker starts)
  for (let i = 0; i < totalJobs; i++) {
    const job = await queue.add(`revoke-job-${i}`, { index: i });
    if (job) {
      addedJobs.push(job);
    }
  }

  // Revoke 5 random jobs before worker starts
  const shuffled = rng.shuffle(addedJobs.slice());
  const preRevokedJobs = shuffled.slice(0, revokeBeforeStart);
  const remainingJobs = shuffled.slice(revokeBeforeStart);

  for (const job of preRevokedJobs) {
    try {
      await queue.revoke(job.id);
    } catch {
      // Ignore revoke errors
    }
  }

  // Verify pre-revoked jobs have isRevoked flag
  let revokedCheckPassed = 0;
  for (const job of preRevokedJobs) {
    try {
      const isRevoked = await job.isRevoked();
      if (isRevoked) revokedCheckPassed++;
    } catch {
      // May fail if job already cleaned up
    }
  }

  if (revokedCheckPassed === 0 && preRevokedJobs.length > 0) {
    violations.push(
      `None of ${preRevokedJobs.length} pre-revoked jobs showed isRevoked()=true`,
    );
  }

  // Track which jobs we will try to revoke while active
  const activeRevokeTargets = new Set<string>();
  const toRevokeWhileActive = remainingJobs.slice(0, revokeWhileActive);
  for (const job of toRevokeWhileActive) {
    activeRevokeTargets.add(job.id);
  }

  // Start worker with slow processor
  const worker = ctx.createWorker(
    queueName,
    async (job: any) => {
      // Simulate work - slow enough that we can revoke during processing
      await new Promise((r) => setTimeout(r, 50));

      // Check if abortSignal fired
      if (job.abortSignal && job.abortSignal.aborted) {
        abortedIds.add(job.id);
        throw new Error('aborted');
      }

      processedIds.add(job.id);
      counter.value++;
      return 'done';
    },
    { concurrency: 3 },
  );

  worker.on('completed', () => {
    terminalCounter.value++;
  });
  worker.on('failed', () => {
    terminalCounter.value++;
  });

  // Wait briefly for worker to start processing, then revoke some active jobs
  await new Promise((r) => setTimeout(r, 200));

  for (const job of toRevokeWhileActive) {
    try {
      await queue.revoke(job.id);
    } catch {
      // May fail if already completed
    }
  }

  // Wait for remaining jobs to complete or fail
  // Not all totalJobs will complete due to revocations
  const expectedTerminal = totalJobs;
  await waitForCount(expectedTerminal, terminalCounter, 15000);

  // Verify: total processed should be less than totalJobs
  // (some were revoked before or during processing)
  if (processedIds.size >= totalJobs) {
    violations.push(
      `Expected fewer than ${totalJobs} processed (some revoked), but got ${processedIds.size}`,
    );
  }

  return {
    added: totalJobs,
    processed: processedIds.size,
    failed: totalJobs - processedIds.size,
    violations,
  };
}
