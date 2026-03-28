/**
 * Agent Budget Loop - autonomous agent with budget guard
 *
 * An AI agent iterates on a research task: each step reasons about
 * what to do next, executes, and reports usage. The budget tracks
 * weighted reasoning tokens (4x) and stops the agent when spent.
 * Shows: reasoning tokens, weighted budgets, iterative processing.
 *
 * No Valkey needed - uses TestQueue/TestWorker.
 * Run: npx tsx examples/agent-budget-loop.ts
 */
import { TestQueue, TestWorker } from '../dist/testing';

async function main() {
  const queue = new TestQueue('agent-loop');
  const BUDGET_KEY = 'agent-flow';

  // Budget: 2000 weighted tokens, reasoning weighs 4x
  queue.setBudget(BUDGET_KEY, {
    maxTotalTokens: 2000,
    tokenWeights: { reasoning: 4 },
    onExceeded: 'fail',
  });

  // Simulated per-step usage - realistic for a reasoning model
  const stepProfiles = [
    { input: 45, output: 32, reasoning: 80 }, // light reasoning
    { input: 62, output: 28, reasoning: 120 }, // moderate
    { input: 55, output: 41, reasoning: 150 }, // heavier
    { input: 70, output: 35, reasoning: 200 }, // heavy
    { input: 48, output: 30, reasoning: 180 }, // heavy
    { input: 60, output: 25, reasoning: 160 }, // won't reach this
  ];

  let stepsCompleted = 0;
  let cumulativeWeighted = 0;
  const log: { step: number; tokens: Record<string, number>; weighted: number; cumulative: number }[] = [];

  const worker = new TestWorker(queue, async (job) => {
    if (job.name === 'agent-parent') return { step: -1, action: 'parent' };

    const step = job.data.step as number;
    const profile = stepProfiles[step] ?? stepProfiles[stepProfiles.length - 1];

    await job.reportUsage({
      model: 'o3-mini',
      provider: 'openai',
      tokens: { input: profile.input, output: profile.output, reasoning: profile.reasoning },
    });

    const weighted = profile.input * 1 + profile.output * 1 + profile.reasoning * 4;
    cumulativeWeighted += weighted;
    stepsCompleted++;
    log.push({ step, tokens: profile, weighted, cumulative: cumulativeWeighted });

    // Enqueue next step if there's more work
    if (step + 1 < stepProfiles.length) {
      const nextJob = await queue.add(
        `step-${step + 1}`,
        { step: step + 1 },
        {
          parent: { queue: queue.name, id: BUDGET_KEY },
        },
      );
      if (nextJob) {
        const record = (queue as any).jobs.get(nextJob.id);
        if (record) record.budgetKey = BUDGET_KEY;
      }
    }

    return { step, action: `Completed research step ${step + 1}` };
  });

  // Create the budget parent
  await queue.add('agent-parent', {}, { jobId: BUDGET_KEY });

  // Seed the first step
  const firstJob = await queue.add(
    'step-0',
    { step: 0 },
    {
      parent: { queue: queue.name, id: BUDGET_KEY },
    },
  );
  if (firstJob) {
    const record = (queue as any).jobs.get(firstJob.id);
    if (record) record.budgetKey = BUDGET_KEY;
  }

  // Let the agent loop run
  await new Promise((r) => setTimeout(r, 1000));

  // Print results
  console.log('--- Agent Budget Loop ---');
  console.log(`Budget: 2000 weighted tokens (reasoning = 4x)\n`);
  console.log('Step  Input  Output  Reasoning  Weighted  Cumulative');
  console.log('----  -----  ------  ---------  --------  ----------');
  for (const entry of log) {
    const t = entry.tokens;
    console.log(
      `  ${entry.step + 1}` +
        `     ${String(t.input).padStart(4)}` +
        `    ${String(t.output).padStart(4)}` +
        `       ${String(t.reasoning).padStart(4)}` +
        `      ${String(entry.weighted).padStart(4)}` +
        `        ${String(entry.cumulative).padStart(4)}`,
    );
  }

  const budget = await queue.getFlowBudget(BUDGET_KEY);
  console.log(`\n--- Budget State ---`);
  console.log(`  Steps completed: ${stepsCompleted} / ${stepProfiles.length}`);
  console.log(`  Weighted used:   ${budget?.usedTokens ?? 0} / 2000`);
  console.log(`  Exceeded:        ${budget?.exceeded ?? false}`);

  if (stepsCompleted < stepProfiles.length) {
    console.log(`\n  Agent stopped at step ${stepsCompleted} - budget exhausted before finishing.`);
    console.log(`  Remaining ${stepProfiles.length - stepsCompleted} steps were blocked by the budget guard.`);
  }

  await worker.close();
  await queue.close();
  console.log('\n[OK] Agent budget loop example complete');
  process.exit(0);
}

main().catch((err) => {
  console.error('[ERROR]', err);
  process.exit(1);
});
