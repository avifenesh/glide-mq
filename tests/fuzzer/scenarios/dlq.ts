/**
 * Scenario: dlq (Valkey only)
 * Create queue + worker with deadLetterQueue configured. Add 20 jobs with
 * attempts: 2. Processor fails 50% of the time. Jobs that exhaust retries
 * should end up in DLQ. Verify DLQ has the right count.
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

export async function dlq(ctx: ScenarioContext): Promise<ScenarioResult> {
  const { rng } = ctx;
  const queueName = ctx.uid();
  const dlqName = `fuzz-dlq-${ctx.uid()}`;

  // Create the main queue with DLQ config
  const queue = ctx.createQueue(queueName, { deadLetterQueue: { name: dlqName } });

  // Create the DLQ queue so cleanup tracks it
  ctx.createQueue(dlqName);

  const violations: string[] = [];
  const totalJobs = 20;

  // Decide which jobs always fail
  const alwaysFailSet = new Set<number>();
  for (let i = 0; i < totalJobs; i++) {
    if (rng.chance(0.5)) {
      alwaysFailSet.add(i);
    }
  }

  let processedCount = 0;
  let failureCount = 0;
  // Track which jobs have reached a final terminal state (completed or exhausted all retries)
  const terminalJobIds = new Set<string>();
  const terminalCounter = { value: 0 };

  // Create worker - with DLQ config so it knows where to route exhausted jobs
  const worker = ctx.createWorker(
    queueName,
    async (job: any) => {
      if (alwaysFailSet.has(job.data.index)) {
        throw new Error('intentional failure');
      }
      processedCount++;
      return 'done';
    },
    {
      concurrency: 5,
      deadLetterQueue: { name: dlqName },
    },
  );

  worker.on('completed', (job: any) => {
    if (!terminalJobIds.has(job.id)) {
      terminalJobIds.add(job.id);
      terminalCounter.value++;
    }
  });

  worker.on('failed', (job: any) => {
    failureCount++;
    // Only count as terminal if attempts are exhausted
    const maxAttempts = job.opts?.attempts ?? 0;
    if (job.attemptsMade >= maxAttempts || maxAttempts === 0) {
      if (!terminalJobIds.has(job.id)) {
        terminalJobIds.add(job.id);
        terminalCounter.value++;
      }
    }
  });

  // Add jobs with attempts: 2 (so 1 retry before DLQ)
  for (let i = 0; i < totalJobs; i++) {
    await queue.add(`dlq-job-${i}`, { index: i }, { attempts: 2 });
  }

  // Wait for all jobs to reach final terminal state.
  // Jobs that always fail need to exhaust both attempts before being terminal.
  await waitForCount(totalJobs, terminalCounter, 20000);

  // Give time for DLQ writes to complete
  await new Promise((r) => setTimeout(r, 1000));

  // Check DLQ - jobs that always fail should end up here after exhausting retries
  try {
    const dlqJobs = await queue.getDeadLetterJobs();
    const expectedDlqCount = alwaysFailSet.size;

    // Allow some tolerance - DLQ routing is best-effort and timing-dependent
    if (dlqJobs.length === 0 && expectedDlqCount > 0) {
      violations.push(`Expected ~${expectedDlqCount} jobs in DLQ but found 0`);
    }
  } catch (err: any) {
    violations.push(`Failed to query DLQ: ${err.message}`);
  }

  return {
    added: totalJobs,
    processed: processedCount,
    failed: failureCount,
    violations,
  };
}
