/**
 * Cost Breakdown - per-category cost tracking
 *
 * Demonstrates currency-agnostic cost tracking with per-category breakdowns.
 * Each job reports costs by category (input, output, reasoning, etc.) with
 * a costUnit. Flow-level aggregation sums all categories across children.
 *
 * No Valkey needed - uses TestQueue/TestWorker.
 *
 * Run: npx tsx examples/cost-breakdown.ts
 */
import { TestQueue, TestWorker } from '../dist/testing';

async function main() {
  const queue = new TestQueue('cost-breakdown');

  // Simulated cost data for 3 different model calls
  const costData: Record<string, { model: string; tokens: Record<string, number>; costs: Record<string, number> }> = {
    summarize: {
      model: 'gpt-5.4',
      tokens: { input: 800, output: 200, reasoning: 0 },
      costs: { input: 0.004, output: 0.006, reasoning: 0 },
    },
    analyze: {
      model: 'claude-sonnet-4-20250514',
      tokens: { input: 1200, output: 400, reasoning: 3200 },
      costs: { input: 0.0036, output: 0.006, reasoning: 0.12 },
    },
    draft: {
      model: 'gpt-5.4',
      tokens: { input: 600, output: 800, reasoning: 0 },
      costs: { input: 0.003, output: 0.024, reasoning: 0 },
    },
  };

  const worker = new TestWorker(queue, async (job) => {
    if (job.name === 'pipeline') {
      return { status: 'aggregated' };
    }

    const sim = costData[job.data.task];
    await job.reportUsage({
      model: sim.model,
      provider: 'simulated',
      tokens: sim.tokens,
      costs: sim.costs,
      costUnit: 'usd',
    });

    return { task: job.data.task, model: sim.model };
  });

  // Create parent job
  const parent = await queue.add('pipeline', { task: 'aggregate' });

  // Create children with parent reference
  const tasks = ['summarize', 'analyze', 'draft'];
  for (const task of tasks) {
    await queue.add(
      task,
      { task },
      {
        parent: { queue: queue.name, id: parent!.id },
      },
    );
  }

  await new Promise((r) => setTimeout(r, 200));

  // Print per-job costs
  console.log('--- Per-Job Costs ---');
  for (const task of tasks) {
    const jobs = await queue.getJobs('completed');
    const job = jobs.find((j) => j.name === task);
    if (job?.usage) {
      const u = job.usage;
      console.log(`  ${task} (${u.model}):`);
      console.log(`    tokens: input=${u.tokens?.input}, output=${u.tokens?.output}, reasoning=${u.tokens?.reasoning}`);
      console.log(`    costs:  input=$${u.costs?.input}, output=$${u.costs?.output}, reasoning=$${u.costs?.reasoning}`);
      console.log(`    total:  ${u.totalTokens} tokens, $${u.totalCost?.toFixed(4)} ${u.costUnit}`);
    }
  }

  // Aggregate flow usage
  const flowUsage = await queue.getFlowUsage(parent!.id);
  console.log('\n--- Flow Usage (aggregated) ---');
  console.log(`  Jobs with usage: ${flowUsage.jobCount}`);
  console.log(`  Tokens by category:`);
  for (const [cat, val] of Object.entries(flowUsage.tokens)) {
    console.log(`    ${cat}: ${val}`);
  }
  console.log(`  Total tokens: ${flowUsage.totalTokens}`);
  console.log(`  Costs by category:`);
  for (const [cat, val] of Object.entries(flowUsage.costs)) {
    console.log(`    ${cat}: $${val.toFixed(4)}`);
  }
  console.log(`  Total cost: $${flowUsage.totalCost.toFixed(4)} ${flowUsage.costUnit}`);
  console.log(`  Models: ${JSON.stringify(flowUsage.models)}`);

  await worker.close();
  await queue.close();
  console.log('\n[OK] Cost breakdown example complete');
  process.exit(0);
}

main().catch((err) => {
  console.error('[ERROR]', err);
  process.exit(1);
});
