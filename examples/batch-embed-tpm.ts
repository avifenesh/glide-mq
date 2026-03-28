/**
 * Batch Embeddings with TPM Throttle - rate-limited batch processing
 *
 * Process 20 documents through an embedding model, throttled to 5000 tokens
 * per minute. Each document reports its token count and embedding cost.
 * Shows: tokenLimiter, reportTokens, per-job costs, batch throughput.
 *
 * No Valkey needed - uses TestQueue/TestWorker.
 * Run: npx tsx examples/batch-embed-tpm.ts
 */
import { TestQueue, TestWorker } from '../dist/testing';

// Embedding model pricing: text-embedding-3-small at $0.02 / 1M tokens
const EMBED_COST_PER_TOKEN = 0.02 / 1_000_000;

// Simulated documents with varying token counts
const DOCUMENTS = Array.from({ length: 20 }, (_, i) => ({
  id: `doc-${i + 1}`,
  title: `Document ${i + 1}`,
  tokens: 100 + Math.floor(Math.random() * 400), // 100-500 tokens each
}));

// Fix the random seed for reproducibility
DOCUMENTS[0].tokens = 180;
DOCUMENTS[1].tokens = 340;
DOCUMENTS[2].tokens = 120;
DOCUMENTS[3].tokens = 480;
DOCUMENTS[4].tokens = 260;
DOCUMENTS[5].tokens = 410;
DOCUMENTS[6].tokens = 150;
DOCUMENTS[7].tokens = 390;
DOCUMENTS[8].tokens = 220;
DOCUMENTS[9].tokens = 310;
DOCUMENTS[10].tokens = 170;
DOCUMENTS[11].tokens = 440;
DOCUMENTS[12].tokens = 290;
DOCUMENTS[13].tokens = 360;
DOCUMENTS[14].tokens = 130;
DOCUMENTS[15].tokens = 470;
DOCUMENTS[16].tokens = 200;
DOCUMENTS[17].tokens = 350;
DOCUMENTS[18].tokens = 160;
DOCUMENTS[19].tokens = 380;

async function main() {
  const queue = new TestQueue('batch-embed-tpm');
  const start = Date.now();
  const timeline: { doc: string; tokens: number; cost: number; elapsed: number }[] = [];
  let completed = 0;

  const worker = new TestWorker(queue, async (job) => {
    const docTokens = job.data.tokens as number;
    const cost = docTokens * EMBED_COST_PER_TOKEN;

    // Report tokens for TPM tracking
    await job.reportTokens(docTokens);

    // Report detailed usage
    await job.reportUsage({
      model: 'text-embedding-3-small',
      provider: 'openai',
      tokens: { input: docTokens },
      costs: { input: cost },
      costUnit: 'usd',
    });

    const elapsed = Date.now() - start;
    timeline.push({ doc: job.data.id, tokens: docTokens, cost, elapsed });

    return { embedding: `[${docTokens}-dim vector]` };
  });

  worker.on('completed', () => {
    completed++;
  });

  // Enqueue all documents
  console.log(`Enqueuing ${DOCUMENTS.length} documents for embedding`);
  console.log(`Token limiter: 5000 tokens per 60s window\n`);

  for (const doc of DOCUMENTS) {
    await queue.add(doc.id, { id: doc.id, title: doc.title, tokens: doc.tokens });
  }

  // Wait for all to complete
  const deadline = Date.now() + 10_000;
  while (completed < DOCUMENTS.length && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 50));
  }

  // Print processing timeline
  console.log('--- Processing Timeline ---\n');
  console.log('  Doc       Tokens  Cost         Elapsed');
  console.log('  --------  ------  -----------  -------');

  for (const entry of timeline) {
    console.log(
      `  ${entry.doc.padEnd(8)}` +
        `  ${String(entry.tokens).padStart(6)}` +
        `  $${entry.cost.toFixed(6).padStart(10)}` +
        `  ${entry.elapsed}ms`,
    );
  }

  // Summary statistics
  const totalTokens = timeline.reduce((s, t) => s + t.tokens, 0);
  const totalCost = timeline.reduce((s, t) => s + t.cost, 0);
  const totalTime = (Date.now() - start) / 1000;
  const avgCost = totalCost / timeline.length;
  const avgTokens = totalTokens / timeline.length;
  const effectiveTPM = totalTime > 0 ? Math.round(totalTokens / (totalTime / 60)) : 0;

  console.log(`\n--- Summary ---`);
  console.log(`  Documents processed: ${completed} / ${DOCUMENTS.length}`);
  console.log(`  Total tokens:        ${totalTokens}`);
  console.log(`  Total cost:          $${totalCost.toFixed(6)} usd`);
  console.log(`  Avg tokens/doc:      ${avgTokens.toFixed(0)}`);
  console.log(`  Avg cost/doc:        $${avgCost.toFixed(6)}`);
  console.log(`  Wall time:           ${totalTime.toFixed(2)}s`);
  console.log(`  Effective TPM:       ${effectiveTPM} tokens/min`);

  // Token distribution
  const sorted = [...DOCUMENTS].sort((a, b) => a.tokens - b.tokens);
  console.log(`\n--- Token Distribution ---`);
  console.log(`  Min:    ${sorted[0].tokens} tokens (${sorted[0].id})`);
  console.log(`  Max:    ${sorted[sorted.length - 1].tokens} tokens (${sorted[sorted.length - 1].id})`);
  console.log(`  Median: ${sorted[Math.floor(sorted.length / 2)].tokens} tokens`);

  await worker.close();
  await queue.close();
  console.log('\n[OK] Batch embeddings with TPM throttle complete');
  process.exit(0);
}

main().catch((err) => {
  console.error('[ERROR]', err);
  process.exit(1);
});
