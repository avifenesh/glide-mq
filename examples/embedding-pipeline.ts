/**
 * Embedding Pipeline - Batch document ingestion with queue
 *
 * Real scenario: Process document chunks through a queue with rate limiting.
 * Each chunk is "embedded" (summarized via LLM as a proxy), with token usage
 * tracked per chunk. A flow budget caps total tokens at 2000.
 *
 * Run: npx tsx examples/embedding-pipeline.ts
 */
import { Queue, Worker, FlowProducer } from '../dist/index';
import { chat, MODELS, CONNECTION, type Message } from './llm';

const QUEUE = `embedding-pipeline-${Date.now()}`;

const CHUNKS = [
  'Machine learning enables computers to learn from data without explicit programming.',
  'Neural networks are inspired by the structure of biological brains.',
  'Transformers use self-attention mechanisms for sequence-to-sequence tasks.',
  'Vector databases store embeddings for fast similarity search.',
  'Retrieval-augmented generation grounds LLM responses in documents.',
  'Fine-tuning adapts pre-trained models to specific domains.',
  'Prompt engineering crafts inputs to guide model outputs effectively.',
  'Token limits constrain the context window of language models.',
];

async function main() {
  const queue = new Queue(QUEUE, { connection: CONNECTION });
  const flow = new FlowProducer({ connection: CONNECTION });

  const timings: { chunk: number; tokens: number; ms: number }[] = [];
  const failedJobs: { name: string; reason: string }[] = [];

  const worker = new Worker(QUEUE, async (job) => {
    if (job.name === 'pipeline-root') {
      const children = await job.getChildrenValues();
      return { processed: Object.keys(children).length };
    }

    const { text, index } = job.data;
    const messages: Message[] = [
      { role: 'system', content: 'Summarize this text in one short sentence.' },
      { role: 'user', content: text },
    ];

    const start = Date.now();
    const result = await chat(MODELS.fast, messages, 50);
    const elapsed = Date.now() - start;

    await job.reportUsage({
      model: result.model,
      provider: 'openrouter',
      inputTokens: result.inputTokens,
      outputTokens: result.outputTokens,
      latencyMs: elapsed,
    });

    timings.push({ chunk: index, tokens: result.totalTokens, ms: elapsed });
    return { summary: result.content, tokens: result.totalTokens };
  }, {
    connection: CONNECTION,
    concurrency: 2,
    tokenLimiter: { maxTokens: 500, duration: 10_000 },
  });

  const completedCount = { n: 0 };
  worker.on('completed', (job) => {
    if (job.name !== 'pipeline-root') completedCount.n++;
  });
  worker.on('failed', (job, err) => {
    failedJobs.push({ name: job.name, reason: err.message });
  });

  const node = await flow.add({
    name: 'pipeline-root',
    queueName: QUEUE,
    data: { task: 'aggregate' },
    children: CHUNKS.map((text, i) => ({
      name: `chunk-${i}`,
      queueName: QUEUE,
      data: { text, index: i },
    })),
  }, { budget: { maxTotalTokens: 2000, onExceeded: 'fail' } });

  console.log(`Pipeline created: ${CHUNKS.length} chunks, budget=2000 tokens\n`);

  // Wait for processing
  const deadline = Date.now() + 90_000;
  while (completedCount.n + failedJobs.length < CHUNKS.length && Date.now() < deadline) {
    await new Promise(r => setTimeout(r, 500));
  }

  // Per-chunk timings
  console.log('--- Per-Chunk Results ---');
  timings.sort((a, b) => a.chunk - b.chunk);
  for (const t of timings) {
    console.log(`  chunk-${t.chunk}: ${t.tokens} tokens, ${t.ms}ms`);
  }

  if (failedJobs.length > 0) {
    console.log('\n--- Failed (budget exceeded) ---');
    for (const f of failedJobs) {
      console.log(`  ${f.name}: ${f.reason.slice(0, 80)}`);
    }
  }

  // Flow usage
  const flowUsage = await queue.getFlowUsage(node.job.id);
  console.log('\n--- Flow Usage ---');
  console.log(`  Jobs with usage: ${flowUsage.jobCount}`);
  console.log(`  Total input tokens: ${flowUsage.totalInputTokens}`);
  console.log(`  Total output tokens: ${flowUsage.totalOutputTokens}`);
  console.log(`  Total: ${flowUsage.totalInputTokens + flowUsage.totalOutputTokens}`);

  const budget = await queue.getFlowBudget(node.job.id);
  if (budget) {
    console.log(`  Budget: ${budget.usedTokens}/${budget.maxTotalTokens} tokens, exceeded=${budget.exceeded}`);
  }

  await worker.close(true);
  await queue.obliterate({ force: true });
  await queue.close();
  await flow.close();

  console.log('\n[OK] Embedding pipeline example complete');
  process.exit(0);
}

main().catch((err) => {
  console.error('[ERROR]', err);
  process.exit(1);
});
