/**
 * TPM Throttle - tokenLimiter (dual-axis rate limiting)
 *
 * Real scenario: Batch AI processing with provider rate limit compliance.
 * The worker is configured with a token limiter (300 tokens per 10s window).
 * After the first few jobs consume the budget, processing pauses until
 * the window resets.
 *
 * Run: npx tsx examples/tpm-throttle.ts
 */
import { Queue, Worker } from '../dist/index';
import { chat, MODELS, CONNECTION, type Message } from './llm';

const QUEUE = `tpm-throttle-${Date.now()}`;

async function main() {
  const queue = new Queue(QUEUE, { connection: CONNECTION });
  const timeline: { job: string; tokens: number; elapsed: number }[] = [];
  const start = Date.now();
  let completed = 0;

  const worker = new Worker(QUEUE, async (job) => {
    const messages: Message[] = [
      { role: 'user', content: job.data.prompt },
    ];

    const result = await chat(MODELS.fast, messages, 30);
    const tokens = result.totalTokens;

    // Report tokens so the limiter tracks them
    await job.reportTokens(tokens);

    const elapsed = Date.now() - start;
    timeline.push({ job: job.name, tokens, elapsed });
    console.log(`  [${(elapsed / 1000).toFixed(1)}s] ${job.name}: ${tokens} tokens`);

    return { tokens, content: result.content.slice(0, 40) };
  }, {
    connection: CONNECTION,
    concurrency: 1,
    tokenLimiter: {
      maxTokens: 300,
      duration: 10_000,
    },
  });

  worker.on('completed', () => { completed++; });

  // Enqueue 10 short tasks
  const prompts = [
    'What is 2+2?', 'Name one planet.', 'Say hello.', 'What color is the sky?',
    'Name a fruit.', 'What is gravity?', 'Count to 3.', 'Name a city.',
    'What is water?', 'Name an animal.',
  ];

  console.log(`Enqueuing ${prompts.length} jobs with tokenLimiter: 300 tokens per 10s\n`);

  for (let i = 0; i < prompts.length; i++) {
    await queue.add(`task-${i + 1}`, { prompt: prompts[i] });
  }

  // Wait for all to complete (with generous timeout for throttling)
  const deadline = Date.now() + 120_000;
  while (completed < prompts.length && Date.now() < deadline) {
    await new Promise(r => setTimeout(r, 500));
  }

  console.log(`\n--- Timeline ---`);
  console.log(`  Total jobs: ${completed}/${prompts.length}`);
  const totalTokens = timeline.reduce((s, t) => s + t.tokens, 0);
  console.log(`  Total tokens: ${totalTokens}`);
  const totalTime = (Date.now() - start) / 1000;
  console.log(`  Total time: ${totalTime.toFixed(1)}s`);

  if (timeline.length >= 2) {
    const firstBatch = timeline.filter(t => t.elapsed < 10_000);
    const secondBatch = timeline.filter(t => t.elapsed >= 10_000);
    console.log(`  First window (<10s): ${firstBatch.length} jobs, ${firstBatch.reduce((s, t) => s + t.tokens, 0)} tokens`);
    if (secondBatch.length > 0) {
      console.log(`  After throttle (>=10s): ${secondBatch.length} jobs`);
    }
  }

  await worker.close(true);
  await queue.obliterate({ force: true });
  await queue.close();

  console.log('\n[OK] TPM throttle example complete');
  process.exit(0);
}

main().catch((err) => {
  console.error('[ERROR]', err);
  process.exit(1);
});
