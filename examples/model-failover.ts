/**
 * Model Failover - fallback chains
 *
 * Real scenario: Primary model is unavailable, falls back through a chain
 * of alternatives. The processor reads job.currentFallback to know which
 * model to use on each retry.
 *
 * Run: npx tsx examples/model-failover.ts
 */
import { Queue, Worker } from '../dist/index';
import { chat, MODELS, CONNECTION, type Message } from './llm';

const QUEUE = `model-failover-${Date.now()}`;

async function main() {
  const queue = new Queue(QUEUE, { connection: CONNECTION });
  let completedResult: any = null;
  let attempts: { model: string; fallbackIndex: number; error?: string }[] = [];

  const worker = new Worker(QUEUE, async (job) => {
    const { prompt } = job.data;
    const fallback = job.currentFallback;

    // On first attempt (no fallback), use the "primary" model from job data.
    // On retries, use whatever the fallback chain provides.
    const model = fallback ? fallback.model : job.data.primaryModel;

    console.log(`[Attempt ${job.attemptsMade + 1}] model="${model}", fallbackIndex=${job.fallbackIndex}`);

    const messages: Message[] = [
      { role: 'system', content: 'Be concise.' },
      { role: 'user', content: prompt },
    ];

    try {
      const result = await chat(model, messages, 80);
      console.log(`  [OK] Response from ${result.model}: "${result.content.slice(0, 60)}..."`);

      await job.reportUsage({
        model: result.model,
        inputTokens: result.inputTokens,
        outputTokens: result.outputTokens,
      });

      return { model: result.model, content: result.content };
    } catch (err: any) {
      console.log(`  [FAIL] ${err.message.slice(0, 80)}`);
      throw err;
    }
  }, {
    connection: CONNECTION,
    concurrency: 1,
    stalledInterval: 30_000,
  });

  worker.on('completed', (_job, result) => { completedResult = result; });

  // Primary model is bogus - will fail. Fallbacks are real models.
  const job = await queue.add('llm-query', {
    prompt: 'Explain message queues in one sentence.',
    primaryModel: 'nonexistent/bogus-model-v99',
  }, {
    attempts: 3,
    backoff: { type: 'fixed', delay: 500 },
    fallbacks: [
      { model: MODELS.nano, provider: 'nvidia' },
      { model: MODELS.fast, provider: 'liquid' },
    ],
  });

  if (!job) {
    console.error('Failed to add job');
    await cleanup(worker, queue);
    process.exit(1);
  }

  console.log(`Job ${job.id} added with fallback chain: bogus -> nano -> fast\n`);

  // Wait for completion or final failure
  const deadline = Date.now() + 60_000;
  while (!completedResult && Date.now() < deadline) {
    await new Promise(r => setTimeout(r, 500));
  }

  if (completedResult) {
    console.log(`\n--- Final Result ---`);
    console.log(`  Served by: ${completedResult.model}`);
    console.log(`  Content: ${completedResult.content.slice(0, 120)}`);
  } else {
    console.log('\n[WARN] Job did not complete in time');
  }

  await cleanup(worker, queue);
  console.log('\n[OK] Model failover example complete');
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
