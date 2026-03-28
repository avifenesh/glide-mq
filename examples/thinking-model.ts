/**
 * Thinking Model - reasoning token tracking
 *
 * Demonstrates how glide-mq tracks reasoning/thinking tokens separately
 * from input/output tokens. Thinking models (o1, o3, Claude extended thinking,
 * DeepSeek R1) generate internal reasoning that consumes tokens but isn't
 * part of the visible response.
 *
 * Run: npx tsx examples/thinking-model.ts
 */
import { Queue, Worker } from '../dist/index';
import { chat, MODELS, CONNECTION, type Message } from './llm';

const QUEUE = `thinking-model-${Date.now()}`;

async function main() {
  const queue = new Queue(QUEUE, { connection: CONNECTION });

  const worker = new Worker(
    QUEUE,
    async (job) => {
      const messages: Message[] = [{ role: 'user', content: job.data.prompt }];

      const start = Date.now();
      const result = await chat(MODELS.reasoning, messages, 200);
      const latency = Date.now() - start;

      await job.reportUsage({
        model: result.model,
        provider: 'openrouter',
        tokens: {
          input: result.inputTokens,
          output: result.outputTokens,
          reasoning: result.reasoningTokens,
        },
        latencyMs: latency,
      });

      return {
        content: result.content,
        reasoningContent: result.reasoningContent,
      };
    },
    { connection: CONNECTION, concurrency: 1 },
  );

  const job = await queue.add('reason', {
    prompt: 'What is the sum of all prime numbers less than 20?',
  });

  if (!job) {
    console.error('[ERROR] Failed to add job');
    await cleanup(worker, queue);
    process.exit(1);
  }

  console.log(`Job ${job.id} added. Waiting for completion...`);

  await job.waitUntilFinished(500, 60_000);
  const completed = await queue.getJob(job.id);

  if (!completed?.usage) {
    console.error('[ERROR] No usage reported');
    await cleanup(worker, queue);
    process.exit(1);
  }

  const u = completed.usage;
  console.log('\n--- Token Breakdown ---');
  console.log(`  Model:      ${u.model}`);
  console.log(`  Input:      ${u.tokens?.input ?? 0}`);
  console.log(`  Output:     ${u.tokens?.output ?? 0}`);
  console.log(`  Reasoning:  ${u.tokens?.reasoning ?? 0}`);
  console.log(`  Total:      ${u.totalTokens}`);
  console.log(`  Latency:    ${u.latencyMs}ms`);

  if (completed.returnvalue?.reasoningContent) {
    console.log(`\n--- Reasoning ---`);
    console.log(`  ${completed.returnvalue.reasoningContent.slice(0, 200)}`);
  }

  console.log(`\n--- Answer ---`);
  console.log(`  ${completed.returnvalue?.content?.slice(0, 200)}`);

  await cleanup(worker, queue);
  console.log('\n[OK] Thinking model example complete');
  process.exit(0);
}

async function cleanup(worker: Worker, queue: Queue) {
  await worker.close(true);
  await queue.obliterate({ force: true });
  await queue.close();
}

main().catch((err) => {
  console.error('[ERROR]', err);
  process.exit(1);
});
