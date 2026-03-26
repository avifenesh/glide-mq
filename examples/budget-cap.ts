/**
 * Budget Cap - budget middleware
 *
 * Real scenario: Prevent a runaway AI agent from burning through token budget.
 * A flow with 5 child jobs has a budget of 200 total tokens. After 3-4 jobs,
 * the budget is exceeded and remaining jobs fail with "Budget exceeded".
 *
 * Run: npx tsx examples/budget-cap.ts
 */
import { Queue, Worker, FlowProducer } from '../dist/index';
import { chat, MODELS, CONNECTION, type Message } from './llm';

const QUEUE = `budget-cap-${Date.now()}`;

async function main() {
  const queue = new Queue(QUEUE, { connection: CONNECTION });
  const flow = new FlowProducer({ connection: CONNECTION });
  const completedJobs: { name: string; tokens: number }[] = [];
  const failedJobs: { name: string; reason: string }[] = [];

  const worker = new Worker(QUEUE, async (job) => {
    if (job.name === 'budget-parent') {
      const children = await job.getChildrenValues();
      return { childCount: Object.keys(children).length };
    }

    const messages: Message[] = [
      { role: 'user', content: job.data.prompt },
    ];

    const result = await chat(MODELS.fast, messages, 40);

    await job.reportUsage({
      model: result.model,
      inputTokens: result.inputTokens,
      outputTokens: result.outputTokens,
    });

    // Use parentId to check flow-level budget state
    const parentId = job.parentId;
    if (parentId) {
      const budget = await queue.getFlowBudget(parentId);
      console.log(`  [${job.name}] ${result.totalTokens} tokens | Budget: ${budget?.usedTokens ?? '?'}/${budget?.maxTotalTokens ?? '?'} tokens, exceeded=${budget?.exceeded}`);
    }

    return { tokens: result.totalTokens, content: result.content.slice(0, 50) };
  }, { connection: CONNECTION, concurrency: 1 });

  worker.on('completed', (job, result) => {
    if (job.name !== 'budget-parent') {
      completedJobs.push({ name: job.name, tokens: result.tokens });
    }
  });
  worker.on('failed', (job, err) => {
    failedJobs.push({ name: job.name, reason: err.message });
  });

  const prompts = [
    'What is 1+1?',
    'Name a color.',
    'What is the sun?',
    'Name an ocean.',
    'What is code?',
  ];

  const node = await flow.add(
    {
      name: 'budget-parent',
      queueName: QUEUE,
      data: { task: 'aggregate' },
      children: prompts.map((prompt, i) => ({
        name: `step-${i + 1}`,
        queueName: QUEUE,
        data: { prompt },
      })),
    },
    { budget: { maxTotalTokens: 200, onExceeded: 'fail' } },
  );

  const flowId = node.job.id;
  console.log(`Flow created: parent=${flowId}, budget=200 tokens, children=${node.children!.length}`);
  console.log('');

  // Wait for jobs to process
  const deadline = Date.now() + 60_000;
  while (completedJobs.length + failedJobs.length < 5 && Date.now() < deadline) {
    await new Promise(r => setTimeout(r, 500));
  }

  console.log('\n--- Summary ---');
  console.log(`  Completed: ${completedJobs.length}`);
  for (const j of completedJobs) {
    console.log(`    ${j.name}: ${j.tokens} tokens`);
  }
  console.log(`  Failed: ${failedJobs.length}`);
  for (const j of failedJobs) {
    console.log(`    ${j.name}: ${j.reason}`);
  }

  const finalBudget = await queue.getFlowBudget(flowId);
  if (finalBudget) {
    console.log(`  Final budget state: ${finalBudget.usedTokens}/${finalBudget.maxTotalTokens} tokens, exceeded=${finalBudget.exceeded}`);
  }

  await worker.close(true);
  await queue.obliterate({ force: true });
  await queue.close();
  await flow.close();

  console.log('\n[OK] Budget cap example complete');
  process.exit(0);
}

main().catch((err) => {
  console.error('[ERROR]', err);
  process.exit(1);
});
