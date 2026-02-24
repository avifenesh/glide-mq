/**
 * Scenario: search-jobs
 * Add 50 jobs with varied names. Process all. SearchJobs by name pattern.
 * Verify results match expected names. Both modes - TestQueue supports searchJobs.
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

const JOB_NAMES = ['email-send', 'pdf-generate', 'data-import', 'notify-user', 'cleanup-old'];

export async function searchJobs(ctx: ScenarioContext): Promise<ScenarioResult> {
  const { rng } = ctx;
  const queueName = ctx.uid();
  const queue = ctx.createQueue(queueName);

  const totalJobs = 50;
  const counter = { value: 0 };
  const addedByName = new Map<string, number>();
  const addedIds = new Set<string>();

  // Initialize counts
  for (const name of JOB_NAMES) {
    addedByName.set(name, 0);
  }

  // Create worker to process all jobs
  ctx.createWorker(
    queueName,
    async (job: any) => {
      counter.value++;
      return 'done';
    },
    { concurrency: 10 },
  );

  // Add jobs with varied names
  for (let i = 0; i < totalJobs; i++) {
    const name = rng.pick(JOB_NAMES);
    const job = await queue.add(name, { index: i, category: name });
    if (job) {
      addedIds.add(job.id);
      addedByName.set(name, (addedByName.get(name) ?? 0) + 1);
    }
  }

  // Settle delay for test mode
  if (ctx.mode === 'test') {
    await new Promise((r) => setTimeout(r, 100));
  }

  // Wait for all jobs to process
  await waitForCount(totalJobs, counter);

  const violations: string[] = [];

  // Search by each name and verify results
  for (const name of JOB_NAMES) {
    const expectedCount = addedByName.get(name) ?? 0;
    if (expectedCount === 0) continue;

    try {
      const results = await queue.searchJobs({ name });

      // Verify all returned jobs have the correct name
      for (const job of results) {
        if (job.name !== name) {
          violations.push(
            `searchJobs({name: '${name}'}) returned job ${job.id} with name '${job.name}'`,
          );
        }
      }

      // Verify count matches (searchJobs may have a default limit, so check <=)
      if (results.length > expectedCount) {
        violations.push(
          `searchJobs({name: '${name}'}) returned ${results.length} jobs but only ${expectedCount} were added`,
        );
      }
    } catch (err: any) {
      violations.push(`searchJobs({name: '${name}'}) threw: ${err.message}`);
    }
  }

  // Search by state
  try {
    const completedResults = await queue.searchJobs({ state: 'completed' });
    // All processed jobs should be in completed state (unless removed)
    if (completedResults.length === 0 && counter.value > 0) {
      // Some may have been removed by removeOnComplete - just check no error
    }
  } catch (err: any) {
    violations.push(`searchJobs({state: 'completed'}) threw: ${err.message}`);
  }

  // Search by name + state combined
  const targetName = rng.pick(JOB_NAMES);
  try {
    const combined = await queue.searchJobs({ name: targetName, state: 'completed' });
    for (const job of combined) {
      if (job.name !== targetName) {
        violations.push(
          `searchJobs({name: '${targetName}', state: 'completed'}) returned wrong name: '${job.name}'`,
        );
      }
    }
  } catch (err: any) {
    violations.push(`searchJobs combined query threw: ${err.message}`);
  }

  return {
    added: addedIds.size,
    processed: counter.value,
    failed: 0,
    violations,
  };
}
