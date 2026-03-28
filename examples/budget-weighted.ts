/**
 * Weighted Budget - per-category token limits with weights
 *
 * Demonstrates budget enforcement with weighted token counting.
 * Reasoning tokens cost 4x their count toward the total budget.
 * Per-category limits (e.g. max 500 reasoning tokens) enforce independently.
 *
 * No Valkey needed - uses TestQueue/TestWorker.
 *
 * Run: npx tsx examples/budget-weighted.ts
 */
import { TestQueue, TestWorker } from '../dist/testing';

async function main() {
  const queue = new TestQueue('budget-weighted');
  const PARENT_ID = 'flow-parent';

  // Set up budget: 1000 weighted total, reasoning weighs 4x, cached 0.25x
  // Also per-category cap: max 500 reasoning tokens
  queue.setBudget(PARENT_ID, {
    maxTotalTokens: 1000,
    tokenWeights: { reasoning: 4, cachedInput: 0.25 },
    maxTokens: { reasoning: 500 },
    onExceeded: 'fail',
  });

  // Simulated workloads - different token mixes
  const workloads = [
    { name: 'cached-lookup', tokens: { input: 50, cachedInput: 400, output: 100 } },
    { name: 'reasoning-task', tokens: { input: 80, output: 60, reasoning: 200 } },
    { name: 'heavy-reasoning', tokens: { input: 40, output: 30, reasoning: 350 } },
  ];

  const completed: string[] = [];
  const failed: string[] = [];

  const worker = new TestWorker(queue, async (job) => {
    if (job.name === 'parent') return { status: 'done' };

    await job.reportUsage({
      model: 'test-model',
      tokens: job.data.tokens,
    });

    return { task: job.name };
  });

  worker.on('completed', (job) => {
    if (job.name !== 'parent') completed.push(job.name);
  });
  worker.on('failed', (job, err) => {
    failed.push(`${job.name}: ${err.message}`);
  });

  // Create parent
  await queue.add('parent', {}, { jobId: PARENT_ID });

  // Create children with budget key
  for (const w of workloads) {
    const job = await queue.add(
      w.name,
      { tokens: w.tokens },
      {
        parent: { queue: queue.name, id: PARENT_ID },
      },
    );
    // Set budgetKey on the underlying record
    if (job) {
      const record = (queue as any).jobs.get(job.id);
      if (record) record.budgetKey = PARENT_ID;
    }
  }

  await new Promise((r) => setTimeout(r, 500));

  // Print what happened
  console.log('--- Workloads ---');
  for (const w of workloads) {
    const raw = Object.values(w.tokens).reduce((s, v) => s + v, 0);
    const weighted =
      (w.tokens.input ?? 0) * 1 +
      (w.tokens.cachedInput ?? 0) * 0.25 +
      (w.tokens.output ?? 0) * 1 +
      (w.tokens.reasoning ?? 0) * 4;
    console.log(`  ${w.name}: raw=${raw}, weighted=${weighted}`);
    console.log(`    tokens: ${JSON.stringify(w.tokens)}`);
  }

  console.log('\n--- Results ---');
  console.log(`  Completed: ${completed.join(', ') || '(none)'}`);
  console.log(`  Failed: ${failed.join(', ') || '(none)'}`);

  const budget = await queue.getFlowBudget(PARENT_ID);
  if (budget) {
    console.log(`\n--- Budget State ---`);
    console.log(`  Weighted used: ${budget.usedTokens} / ${budget.maxTotalTokens}`);
    console.log(`  Exceeded: ${budget.exceeded}`);
    console.log(`  Weights: reasoning=4x, cachedInput=0.25x, others=1x`);
    console.log(`  Per-category cap: reasoning max ${budget.maxTokens?.reasoning}`);
  }

  console.log('\n--- Explanation ---');
  console.log('  cached-lookup: weighted = 50 + 400*0.25 + 100 = 250 [OK]');
  console.log('  reasoning-task: weighted = 80 + 60 + 200*4 = 940 (cumulative: 1190 > 1000) [EXCEEDED]');
  console.log('  heavy-reasoning: budget already exceeded, fails pre-check');

  await worker.close();
  await queue.close();
  console.log('\n[OK] Weighted budget example complete');
  process.exit(0);
}

main().catch((err) => {
  console.error('[ERROR]', err);
  process.exit(1);
});
