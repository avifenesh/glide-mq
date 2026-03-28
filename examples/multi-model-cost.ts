/**
 * Multi-Model Cost Comparison - compare costs across providers
 *
 * Runs the same task on 3 models with different pricing.
 * Flow-level aggregation shows per-model cost breakdown.
 * Shows: per-category costs, costUnit, model distribution, flow aggregation.
 *
 * No Valkey needed - uses TestQueue/TestWorker.
 * Run: npx tsx examples/multi-model-cost.ts
 */
import { TestQueue, TestWorker } from '../dist/testing';

// Pricing per million tokens (USD)
const PRICING: Record<string, { input: number; output: number; reasoning?: number }> = {
  'gpt-5.4': { input: 5.0, output: 15.0 },
  'claude-sonnet': { input: 3.0, output: 15.0, reasoning: 3.75 },
  'llama-3': { input: 0.2, output: 0.2 },
};

function computeCosts(model: string, tokens: Record<string, number>): Record<string, number> {
  const rates = PRICING[model];
  if (!rates) return {};
  const costs: Record<string, number> = {};
  for (const [cat, count] of Object.entries(tokens)) {
    const rate = (rates as Record<string, number>)[cat] ?? 0;
    costs[cat] = (count / 1_000_000) * rate;
  }
  return costs;
}

async function main() {
  const queue = new TestQueue('multi-model-cost');

  // Simulated outputs - same task, different models
  const models = [
    {
      name: 'gpt-5.4',
      tokens: { input: 850, output: 420 },
    },
    {
      name: 'claude-sonnet',
      tokens: { input: 850, output: 380, reasoning: 1200 },
    },
    {
      name: 'llama-3',
      tokens: { input: 850, output: 620 },
    },
  ];

  const worker = new TestWorker(queue, async (job) => {
    if (job.name === 'parent') return { status: 'aggregated' };

    const model = job.data.model as string;
    const tokens = job.data.tokens as Record<string, number>;
    const costs = computeCosts(model, tokens);

    await job.reportUsage({
      model,
      tokens,
      costs,
      costUnit: 'usd',
    });

    return { model, tokens, costs };
  });

  // Create parent
  const parent = await queue.add('parent', {}, { jobId: 'compare-flow' });

  // Create one child per model
  for (const m of models) {
    await queue.add(
      m.name,
      { model: m.name, tokens: m.tokens },
      {
        parent: { queue: queue.name, id: parent!.id },
      },
    );
  }

  await new Promise((r) => setTimeout(r, 300));

  // Print per-model breakdown
  console.log('--- Per-Model Results ---\n');
  console.log('Model            Input Tok  Output Tok  Reasoning Tok  Total Cost');
  console.log('---------------  ---------  ----------  -------------  ----------');

  const completed = await queue.getJobs('completed');
  const childJobs = completed.filter((j) => j.name !== 'parent');

  for (const job of childJobs) {
    const u = job.usage;
    if (!u) continue;
    const model = (u.model ?? '').padEnd(15);
    const inp = String(u.tokens?.input ?? 0).padStart(9);
    const out = String(u.tokens?.output ?? 0).padStart(10);
    const reas = String(u.tokens?.reasoning ?? 0).padStart(13);
    const cost = `$${(u.totalCost ?? 0).toFixed(6)}`.padStart(10);
    console.log(`${model}  ${inp}  ${out}  ${reas}  ${cost}`);
  }

  // Aggregated flow usage
  const flow = await queue.getFlowUsage(parent!.id);
  console.log(`\n--- Flow Aggregate ---`);
  console.log(`  Jobs:         ${flow.jobCount}`);
  console.log(`  Total tokens: ${flow.totalTokens}`);
  console.log(`  Total cost:   $${flow.totalCost.toFixed(6)} ${flow.costUnit ?? ''}`);
  console.log(`  Models used:  ${JSON.stringify(flow.models)}`);

  // Comparison insights
  const sorted = childJobs.filter((j) => j.usage).sort((a, b) => (a.usage!.totalCost ?? 0) - (b.usage!.totalCost ?? 0));
  const cheapest = sorted[0];
  const mostTokens = childJobs
    .filter((j) => j.usage)
    .sort((a, b) => (b.usage!.totalTokens ?? 0) - (a.usage!.totalTokens ?? 0))[0];

  console.log(`\n--- Comparison ---`);
  console.log(`  Cheapest:            ${cheapest?.usage?.model} ($${(cheapest?.usage?.totalCost ?? 0).toFixed(6)})`);
  console.log(`  Most tokens:         ${mostTokens?.usage?.model} (${mostTokens?.usage?.totalTokens} tokens)`);

  if (cheapest?.usage && sorted[sorted.length - 1]?.usage) {
    const ratio = (sorted[sorted.length - 1].usage!.totalCost ?? 1) / (cheapest.usage.totalCost ?? 1);
    console.log(`  Cost ratio max/min:  ${ratio.toFixed(1)}x`);
  }

  // Cost per output token
  console.log(`\n--- Cost per Output Token ---`);
  for (const job of childJobs) {
    const u = job.usage;
    if (!u || !u.tokens?.output) continue;
    const costPerOut = (u.totalCost ?? 0) / u.tokens.output;
    console.log(`  ${(u.model ?? '').padEnd(15)} $${costPerOut.toFixed(8)} / output token`);
  }

  await worker.close();
  await queue.close();
  console.log('\n[OK] Multi-model cost comparison complete');
  process.exit(0);
}

main().catch((err) => {
  console.error('[ERROR]', err);
  process.exit(1);
});
