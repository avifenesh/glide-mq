/**
 * Usage Tracking - reportUsage / getFlowUsage
 *
 * Real scenario: Track token costs across an AI content pipeline.
 * A flow with 3 steps (research -> draft -> edit) each calls an LLM,
 * reports usage, and the parent aggregates results with getFlowUsage().
 *
 * Run: npx tsx examples/usage-tracking.ts
 */
import { Queue, Worker, FlowProducer } from '../dist/index';
import { chat, MODELS, CONNECTION, type Message } from './llm';

const QUEUE = `usage-tracking-${Date.now()}`;

async function main() {
  const queue = new Queue(QUEUE, { connection: CONNECTION });
  const flow = new FlowProducer({ connection: CONNECTION });

  const worker = new Worker(QUEUE, async (job) => {
    const { task, topic } = job.data;
    const model = task === 'research' ? MODELS.fast
      : task === 'draft' ? MODELS.nano
      : MODELS.fast;

    const messages: Message[] = [
      { role: 'system', content: `You are a helpful ${task} assistant.` },
      { role: 'user', content: topic },
    ];

    const start = Date.now();
    const result = await chat(model, messages, 100);
    const latency = Date.now() - start;

    await job.reportUsage({
      model: result.model,
      provider: 'openrouter',
      inputTokens: result.inputTokens,
      outputTokens: result.outputTokens,
      latencyMs: latency,
    });

    console.log(`[${task}] ${result.model} - ${result.totalTokens} tokens, ${latency}ms`);
    return { content: result.content, tokens: result.totalTokens };
  }, { connection: CONNECTION, concurrency: 3 });

  const completedJobs: string[] = [];
  worker.on('completed', (job) => { completedJobs.push(job.id); });

  const node = await flow.add({
    name: 'content-pipeline',
    queueName: QUEUE,
    data: { task: 'aggregate', topic: 'AI in healthcare' },
    children: [
      { name: 'research', queueName: QUEUE, data: { task: 'research', topic: 'List 3 key facts about AI in healthcare' } },
      { name: 'draft', queueName: QUEUE, data: { task: 'draft', topic: 'Write a one-paragraph draft about AI in healthcare' } },
      { name: 'edit', queueName: QUEUE, data: { task: 'edit', topic: 'Improve this text: AI helps doctors diagnose diseases faster' } },
    ],
  });

  console.log(`Flow created: parent=${node.job.id}, children=${node.children!.map(c => c.job.id).join(', ')}`);

  // Wait for all children to complete
  const deadline = Date.now() + 60_000;
  while (completedJobs.length < 3 && Date.now() < deadline) {
    await new Promise(r => setTimeout(r, 500));
  }

  if (completedJobs.length < 3) {
    console.error('Timed out waiting for children');
    await cleanup(worker, queue, flow);
    process.exit(1);
  }

  // Print per-job usage
  console.log('\n--- Per-Job Usage ---');
  for (const child of node.children!) {
    const job = await queue.getJob(child.job.id);
    if (job?.usage) {
      console.log(`  ${job.name}: model=${job.usage.model}, in=${job.usage.inputTokens}, out=${job.usage.outputTokens}, latency=${job.usage.latencyMs}ms`);
    }
  }

  // Aggregate flow usage
  const flowUsage = await queue.getFlowUsage(node.job.id);
  console.log('\n--- Flow Usage (aggregated) ---');
  console.log(`  Jobs with usage: ${flowUsage.jobCount}`);
  console.log(`  Total input tokens: ${flowUsage.totalInputTokens}`);
  console.log(`  Total output tokens: ${flowUsage.totalOutputTokens}`);
  console.log(`  Total tokens: ${flowUsage.totalInputTokens + flowUsage.totalOutputTokens}`);
  console.log(`  Model distribution:`, flowUsage.models);

  await cleanup(worker, queue, flow);
  console.log('\n[OK] Usage tracking example complete');
  process.exit(0);
}

async function cleanup(worker: Worker, queue: Queue, flow: FlowProducer) {
  await worker.close(true);
  await queue.obliterate({ force: true });
  await queue.close();
  await flow.close();
}

main().catch((err) => {
  console.error('[ERROR]', err);
  process.exit(1);
});
