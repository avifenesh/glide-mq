/**
 * Adaptive Timeout - per-job lockDuration
 *
 * Real scenario: Mixed AI workload with fast classification and slow generation.
 * Each job type gets a different lockDuration so heartbeats and stall detection
 * match the expected processing time.
 *
 * Run: npx tsx examples/adaptive-timeout.ts
 */
import { Queue, Worker } from '../dist/index';
import { chat, MODELS, CONNECTION, type Message } from './llm';

const QUEUE = `adaptive-timeout-${Date.now()}`;

interface TaskResult {
  name: string;
  lockDuration: number;
  processingTime: number;
  result: string;
}

async function main() {
  const queue = new Queue(QUEUE, { connection: CONNECTION });
  const results: TaskResult[] = [];

  const worker = new Worker(QUEUE, async (job) => {
    const { task, prompt, model } = job.data;
    const lock = job.opts.lockDuration ?? 30_000;
    const start = Date.now();

    console.log(`[${task}] Started (lockDuration=${lock}ms, heartbeat every ${lock / 2}ms)`);

    const messages: Message[] = [
      { role: 'system', content: 'Be concise.' },
      { role: 'user', content: prompt },
    ];

    // maxTokens varies by task type
    const maxTokens = task === 'classify' ? 20 : task === 'summarize' ? 80 : 150;
    const result = await chat(model, messages, maxTokens);
    const elapsed = Date.now() - start;

    console.log(`[${task}] Done in ${elapsed}ms - "${result.content.slice(0, 60)}..."`);

    return { name: task, lockDuration: lock, processingTime: elapsed, result: result.content };
  }, {
    connection: CONNECTION,
    concurrency: 1,
    lockDuration: 30_000,
  });

  worker.on('completed', (_job, result) => { results.push(result); });

  // Enqueue 3 jobs with different lockDurations
  const jobs = [
    {
      name: 'classify',
      data: {
        task: 'classify',
        prompt: 'Classify this as positive/negative/neutral: "The product works great!"',
        model: MODELS.fast,
      },
      opts: { lockDuration: 5_000 },
    },
    {
      name: 'summarize',
      data: {
        task: 'summarize',
        prompt: 'Summarize in one sentence: Message queues decouple producers from consumers, enabling async processing.',
        model: MODELS.nano,
      },
      opts: { lockDuration: 30_000 },
    },
    {
      name: 'generate',
      data: {
        task: 'generate',
        prompt: 'Write a product description for a wireless noise-canceling headphone.',
        model: MODELS.fast,
      },
      opts: { lockDuration: 120_000 },
    },
  ];

  for (const j of jobs) {
    await queue.add(j.name, j.data, j.opts);
  }
  console.log(`Enqueued ${jobs.length} jobs with varying lockDurations\n`);

  // Wait for all to complete
  const deadline = Date.now() + 60_000;
  while (results.length < 3 && Date.now() < deadline) {
    await new Promise(r => setTimeout(r, 500));
  }

  console.log('\n--- Results ---');
  for (const r of results) {
    console.log(`  ${r.name}: lock=${r.lockDuration}ms, processed in ${r.processingTime}ms`);
  }

  if (results.length < 3) {
    console.log('[WARN] Not all jobs completed within timeout');
  }

  await worker.close(true);
  await queue.obliterate({ force: true });
  await queue.close();

  console.log('\n[OK] Adaptive timeout example complete');
  process.exit(0);
}

main().catch((err) => {
  console.error('[ERROR]', err);
  process.exit(1);
});
