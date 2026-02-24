/**
 * Scenario: mixed-storm (both modes)
 * Chaos scenario. Run 100-200 random operations picked with weighted probability.
 * Track added count. Each op wrapped in try/catch. After all ops, verify no
 * unhandled rejections. Use a worker running in background the whole time.
 */

import type { ScenarioContext, ScenarioResult } from '../types';
import { randomPayload } from '../generators/data';

export async function mixedStorm(ctx: ScenarioContext): Promise<ScenarioResult> {
  const { rng } = ctx;
  const queueName = ctx.uid();
  const queue = ctx.createQueue(queueName);

  const violations: string[] = [];
  let addedCount = 0;
  let processedCount = 0;
  let failedCount = 0;

  // Track unhandled rejections during this scenario
  let unhandledRejection = false;
  const rejectionHandler = () => {
    unhandledRejection = true;
  };
  process.on('unhandledRejection', rejectionHandler);

  // Start a background worker
  const worker = ctx.createWorker(
    queueName,
    async () => {
      processedCount++;
      return 'done';
    },
    { concurrency: 10 },
  );

  worker.on('failed', () => {
    failedCount++;
  });

  // Keep track of added job IDs for getJob calls
  const addedJobIds: string[] = [];

  // Define weighted operations
  type OpName =
    | 'add'
    | 'addBulk'
    | 'getJobCounts'
    | 'getJob'
    | 'pause'
    | 'resume'
    | 'searchJobs'
    | 'clean'
    | 'drain'
    | 'retryJobs'
    | 'getWorkers'
    | 'getMetrics';

  const ops: [OpName, number][] = [
    ['add', 20],
    ['addBulk', 5],
    ['getJobCounts', 10],
    ['getJob', 8],
    ['pause', 3],
    ['resume', 3],
    ['searchJobs', 4],
    ['clean', 3],
    ['drain', 1],
    ['retryJobs', 2],
    ['getWorkers', 3],
    ['getMetrics', 3],
  ];

  const numOps = rng.nextInt(100, 200);

  for (let i = 0; i < numOps; i++) {
    const op = rng.weighted(ops);

    try {
      switch (op) {
        case 'add': {
          const data = randomPayload(rng);
          const job = await queue.add(`storm-${i}`, data);
          if (job) {
            addedCount++;
            addedJobIds.push(job.id);
          }
          break;
        }

        case 'addBulk': {
          const count = rng.nextInt(2, 10);
          const jobs = Array.from({ length: count }, (_, j) => ({
            name: `storm-bulk-${i}-${j}`,
            data: randomPayload(rng),
          }));
          const result = await queue.addBulk(jobs);
          addedCount += result.length;
          for (const j of result) {
            addedJobIds.push(j.id);
          }
          break;
        }

        case 'getJobCounts': {
          await queue.getJobCounts();
          break;
        }

        case 'getJob': {
          if (addedJobIds.length > 0) {
            const id = rng.pick(addedJobIds);
            await queue.getJob(id);
          }
          break;
        }

        case 'pause': {
          await queue.pause();
          break;
        }

        case 'resume': {
          await queue.resume();
          break;
        }

        case 'searchJobs': {
          await queue.searchJobs({ name: `storm-${rng.nextInt(0, numOps)}`, limit: 10 });
          break;
        }

        case 'clean': {
          await queue.clean(0, 5, 'completed');
          break;
        }

        case 'drain': {
          await queue.drain();
          break;
        }

        case 'retryJobs': {
          await queue.retryJobs({ count: 5 });
          break;
        }

        case 'getWorkers': {
          await queue.getWorkers();
          break;
        }

        case 'getMetrics': {
          const type = rng.pick(['completed', 'failed'] as const);
          await queue.getMetrics(type);
          break;
        }
      }
    } catch {
      // Expected errors like "queue is closing", "Queue is paused", etc. are OK
    }

    // Small yield every 10 ops to let the worker process
    if (i % 10 === 0) {
      await new Promise((r) => setTimeout(r, 10));
    }
  }

  // Ensure queue is resumed at the end so worker can finish
  try {
    await queue.resume();
  } catch {
    // Ignore
  }

  // Settle delay for test mode
  if (ctx.mode === 'test') {
    await new Promise((r) => setTimeout(r, 200));
  }

  // Wait a moment for background processing to settle
  await new Promise((r) => setTimeout(r, 1000));

  // Clean up unhandled rejection listener
  process.off('unhandledRejection', rejectionHandler);

  if (unhandledRejection) {
    violations.push('Unhandled promise rejection occurred during mixed-storm');
  }

  return {
    added: addedCount,
    processed: processedCount,
    failed: failedCount,
    violations,
  };
}
