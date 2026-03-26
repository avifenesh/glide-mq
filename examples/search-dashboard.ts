/**
 * Search Dashboard - Job search and monitoring
 *
 * Real scenario: Enqueue 20 AI inference jobs with varied metadata, process
 * most of them (some succeed, some fail), leave a few waiting, then demonstrate
 * the search and monitoring APIs: getJobCounts, searchJobs, getMetrics, getJobs.
 *
 * Run: npx tsx examples/search-dashboard.ts
 */
import { Queue, Worker } from '../dist/index';
import { chat, MODELS, CONNECTION, type Message } from './llm';

const QUEUE = `search-dashboard-${Date.now()}`;

const TASKS = [
  { userId: 'user-1', model: 'gpt-4', taskType: 'inference', priority: 1 },
  { userId: 'user-1', model: 'gpt-4', taskType: 'inference', priority: 2 },
  { userId: 'user-1', model: 'gpt-3.5', taskType: 'summarize', priority: 3 },
  { userId: 'user-2', model: 'gpt-4', taskType: 'inference', priority: 1 },
  { userId: 'user-2', model: 'gpt-4', taskType: 'translate', priority: 2 },
  { userId: 'user-2', model: 'gpt-3.5', taskType: 'inference', priority: 3 },
  { userId: 'user-3', model: 'gpt-4', taskType: 'inference', priority: 1 },
  { userId: 'user-3', model: 'gpt-3.5', taskType: 'summarize', priority: 2 },
  { userId: 'user-1', model: 'gpt-4', taskType: 'inference', priority: 1 },
  { userId: 'user-2', model: 'gpt-4', taskType: 'inference', priority: 2 },
  { userId: 'user-3', model: 'gpt-3.5', taskType: 'translate', priority: 3 },
  { userId: 'user-1', model: 'gpt-4', taskType: 'inference', priority: 1 },
  { userId: 'user-2', model: 'gpt-3.5', taskType: 'summarize', priority: 2 },
  { userId: 'user-3', model: 'gpt-4', taskType: 'inference', priority: 1 },
  { userId: 'user-1', model: 'gpt-4', taskType: 'inference', priority: 2 },
  { userId: 'user-2', model: 'gpt-3.5', taskType: 'translate', priority: 3 },
  { userId: 'user-3', model: 'gpt-4', taskType: 'inference', priority: 1 },
  { userId: 'user-1', model: 'gpt-3.5', taskType: 'summarize', priority: 2 },
  { userId: 'user-2', model: 'gpt-4', taskType: 'inference', priority: 1 },
  { userId: 'user-3', model: 'gpt-4', taskType: 'inference', priority: 2 },
];

async function main() {
  const queue = new Queue(QUEUE, { connection: CONNECTION });

  // Enqueue all 20 jobs
  console.log('Enqueueing 20 AI jobs...');
  const jobs = await queue.addBulk(
    TASKS.map((t, i) => ({
      name: t.taskType,
      data: { ...t, prompt: `Task ${i}: ${t.taskType} for ${t.userId}`, index: i },
      opts: { priority: t.priority },
    })),
  );
  console.log(`  Added ${jobs.length} jobs\n`);

  // Process 15: 12 succeed, 3 fail deliberately
  let processed = 0;
  const failIndices = new Set([3, 7, 11]);

  const worker = new Worker(QUEUE, async (job) => {
    processed++;
    if (processed > 15) {
      // Leave remaining 5 as waiting - skip by delaying
      throw new Error('skip');
    }

    if (failIndices.has(job.data.index)) {
      throw new Error(`Simulated failure for task ${job.data.index}`);
    }

    // Lightweight call to keep the example fast
    const messages: Message[] = [{ role: 'user', content: 'Say "ok" in one word.' }];
    const result = await chat(MODELS.fast, messages, 10);

    await job.reportUsage({
      model: result.model,
      inputTokens: result.inputTokens,
      outputTokens: result.outputTokens,
    });

    return { response: result.content.slice(0, 20) };
  }, { connection: CONNECTION, concurrency: 3, stalledInterval: 30_000 });

  // Wait for processing to settle
  const completedIds: string[] = [];
  const failedIds: string[] = [];
  worker.on('completed', (job) => completedIds.push(job.id));
  worker.on('failed', (job) => failedIds.push(job.id));

  const deadline = Date.now() + 90_000;
  while (completedIds.length + failedIds.length < 15 && Date.now() < deadline) {
    await new Promise(r => setTimeout(r, 500));
  }

  // Pause worker so remaining 5 stay waiting
  await worker.close(true);

  console.log('=== Dashboard ===\n');

  // 1. Job counts overview
  console.log('--- Job Counts ---');
  const counts = await queue.getJobCounts();
  console.log(`  waiting=${counts.waiting} active=${counts.active} completed=${counts.completed} failed=${counts.failed} delayed=${counts.delayed}\n`);

  // 2. Search completed inference jobs
  console.log('--- Search: completed inference jobs ---');
  const completedInf = await queue.searchJobs({ state: 'completed', name: 'inference' });
  console.log(`  Found ${completedInf.length} completed inference job(s)`);
  for (const j of completedInf.slice(0, 3)) {
    console.log(`    id=${j.id} user=${j.data.userId} model=${j.data.model}`);
  }
  if (completedInf.length > 3) console.log(`    ... and ${completedInf.length - 3} more`);

  // 3. Search by user
  console.log('\n--- Search: all jobs from user-1 ---');
  const user1Jobs = await queue.searchJobs({ data: { userId: 'user-1' } });
  console.log(`  Found ${user1Jobs.length} job(s) for user-1`);

  // 4. Metrics for completed jobs
  console.log('\n--- Metrics: completed ---');
  const metrics = await queue.getMetrics('completed');
  console.log(`  count=${metrics.count}, data points=${metrics.data?.length ?? 0}`);

  // 5. Failed jobs - show and retry one
  console.log('\n--- Failed Jobs ---');
  const failedJobs = await queue.getJobs('failed');
  console.log(`  ${failedJobs.length} failed job(s)`);
  for (const j of failedJobs) {
    console.log(`    id=${j.id} name=${j.name} reason="${j.failedReason?.slice(0, 40)}"`);
  }

  if (failedJobs.length > 0) {
    console.log(`\n  Retrying job ${failedJobs[0].id}...`);
    await failedJobs[0].retry();
    console.log('  [OK] Job moved back to waiting');
  }

  // Final counts
  console.log('\n--- Final Counts ---');
  const finalCounts = await queue.getJobCounts();
  console.log(`  waiting=${finalCounts.waiting} completed=${finalCounts.completed} failed=${finalCounts.failed}`);

  await queue.obliterate({ force: true });
  await queue.close();

  console.log('\n[OK] Search dashboard example complete');
  process.exit(0);
}

main().catch((err) => {
  console.error('[ERROR]', err);
  process.exit(1);
});
