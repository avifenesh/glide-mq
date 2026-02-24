/**
 * Scenario: scheduler
 * CRUD operations on job schedulers:
 * - upsertJobScheduler('test-sched', { every: 60000 }, { name: 'scheduled-job', data: {} })
 * - getJobScheduler('test-sched') - verify non-null
 * - getRepeatableJobs() - verify includes it
 * - removeJobScheduler('test-sched')
 * - getJobScheduler('test-sched') - verify null
 */

import type { ScenarioContext, ScenarioResult } from '../types';

export async function scheduler(ctx: ScenarioContext): Promise<ScenarioResult> {
  const { rng } = ctx;
  const queueName = ctx.uid();
  const queue = ctx.createQueue(queueName);

  const violations: string[] = [];
  const schedName = `test-sched-${rng.nextInt(1, 10000)}`;

  // Step 1: Upsert a job scheduler
  try {
    await queue.upsertJobScheduler(
      schedName,
      { every: 60000 },
      { name: 'scheduled-job', data: {} },
    );
  } catch (err: any) {
    violations.push(`upsertJobScheduler threw: ${err.message}`);
    return { added: 0, processed: 0, failed: 0, violations };
  }

  // Step 2: getJobScheduler - verify non-null
  try {
    const entry = await queue.getJobScheduler(schedName);
    if (!entry) {
      violations.push(`getJobScheduler('${schedName}') returned null after upsert`);
    } else {
      // Verify the entry has expected fields
      if (entry.every !== 60000) {
        violations.push(`Scheduler every is ${entry.every}, expected 60000`);
      }
      if (!entry.nextRun || entry.nextRun <= 0) {
        violations.push(`Scheduler nextRun is invalid: ${entry.nextRun}`);
      }
      if (entry.template) {
        if (entry.template.name !== 'scheduled-job') {
          violations.push(`Scheduler template name is '${entry.template.name}', expected 'scheduled-job'`);
        }
      }
    }
  } catch (err: any) {
    violations.push(`getJobScheduler threw: ${err.message}`);
  }

  // Step 3: getRepeatableJobs - verify includes it
  try {
    const repeatables = await queue.getRepeatableJobs();
    const found = repeatables.find((r: any) => r.name === schedName);
    if (!found) {
      violations.push(
        `getRepeatableJobs() did not include '${schedName}'. ` +
        `Found: [${repeatables.map((r: any) => r.name).join(', ')}]`,
      );
    } else {
      if (found.entry.every !== 60000) {
        violations.push(`Repeatable entry every is ${found.entry.every}, expected 60000`);
      }
    }
  } catch (err: any) {
    violations.push(`getRepeatableJobs threw: ${err.message}`);
  }

  // Step 4: Upsert again (update) - change interval
  try {
    await queue.upsertJobScheduler(
      schedName,
      { every: 120000 },
      { name: 'scheduled-job-v2', data: { version: 2 } },
    );

    const updated = await queue.getJobScheduler(schedName);
    if (!updated) {
      violations.push('getJobScheduler returned null after update-upsert');
    } else if (updated.every !== 120000) {
      violations.push(`Updated scheduler every is ${updated.every}, expected 120000`);
    }
  } catch (err: any) {
    violations.push(`upsert update threw: ${err.message}`);
  }

  // Step 5: Add a second scheduler to test multiple entries
  const schedName2 = `test-sched-2-${rng.nextInt(1, 10000)}`;
  try {
    await queue.upsertJobScheduler(
      schedName2,
      { every: 30000 },
      { name: 'second-scheduled', data: {} },
    );

    const repeatables = await queue.getRepeatableJobs();
    if (repeatables.length < 2) {
      violations.push(
        `Expected at least 2 repeatable jobs, got ${repeatables.length}`,
      );
    }
  } catch (err: any) {
    violations.push(`Second scheduler operations threw: ${err.message}`);
  }

  // Step 6: Remove the first scheduler
  try {
    await queue.removeJobScheduler(schedName);

    const afterRemove = await queue.getJobScheduler(schedName);
    if (afterRemove !== null) {
      violations.push(
        `getJobScheduler('${schedName}') returned non-null after removeJobScheduler`,
      );
    }
  } catch (err: any) {
    violations.push(`removeJobScheduler threw: ${err.message}`);
  }

  // Step 7: Verify second scheduler still exists
  try {
    const second = await queue.getJobScheduler(schedName2);
    if (!second) {
      violations.push('Second scheduler was removed when first was deleted');
    }
  } catch (err: any) {
    violations.push(`Checking second scheduler threw: ${err.message}`);
  }

  // Step 8: Remove second scheduler and verify empty
  try {
    await queue.removeJobScheduler(schedName2);
    const finalRepeatables = await queue.getRepeatableJobs();
    if (finalRepeatables.length !== 0) {
      violations.push(
        `After removing all schedulers, getRepeatableJobs() returned ${finalRepeatables.length} entries`,
      );
    }
  } catch (err: any) {
    violations.push(`Final cleanup threw: ${err.message}`);
  }

  return {
    added: 0,
    processed: 0,
    failed: 0,
    violations,
  };
}
