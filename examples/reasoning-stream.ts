/**
 * Reasoning Stream - typed streaming with reasoning vs content
 *
 * Demonstrates how to stream thinking model output with separate
 * reasoning and content chunks. Frontends can render reasoning
 * in a collapsible section while showing content directly.
 *
 * No Valkey needed - uses TestQueue/TestWorker.
 *
 * Run: npx tsx examples/reasoning-stream.ts
 */
import { TestQueue, TestWorker } from '../dist/testing';

async function main() {
  const queue = new TestQueue('reasoning-stream');

  const worker = new TestWorker(queue, async (job) => {
    // Simulate a thinking model: first reasoning, then content
    const reasoningSteps = [
      'The user asks for prime factors of 84.',
      'Start dividing: 84 / 2 = 42.',
      '42 / 2 = 21.',
      '21 / 3 = 7.',
      '7 is prime. Done.',
    ];

    const contentParts = ['The prime factorization of 84 is ', '2 x 2 x 3 x 7', ' (or 2^2 x 3 x 7).'];

    // Stream reasoning chunks
    for (const step of reasoningSteps) {
      await job.streamChunk('reasoning', step);
    }

    // Stream content chunks
    for (const part of contentParts) {
      await job.streamChunk('content', part);
    }

    // Signal completion
    await job.streamChunk('done');

    await job.reportUsage({
      model: 'test-thinking-model',
      tokens: { input: 20, output: 30, reasoning: 85 },
    });

    return { answer: '2^2 x 3 x 7' };
  });

  const job = await queue.add('factorize', { prompt: 'Find the prime factors of 84' });

  await new Promise((r) => setTimeout(r, 200));

  // Read and render the stream
  const entries = await queue.readStream(job!.id);

  console.log('--- Streamed Output ---');
  for (const entry of entries) {
    const { type, content } = entry.fields;
    if (type === 'reasoning') {
      console.log(`  [thinking] ${content}`);
    } else if (type === 'content') {
      process.stdout.write(content ?? '');
    } else if (type === 'done') {
      console.log('\n');
    }
  }

  // Show token breakdown
  const completed = await queue.getJob(job!.id);
  if (completed?.usage) {
    const u = completed.usage;
    console.log('--- Token Breakdown ---');
    console.log(`  Input:     ${u.tokens?.input}`);
    console.log(`  Output:    ${u.tokens?.output}`);
    console.log(`  Reasoning: ${u.tokens?.reasoning}`);
    console.log(`  Total:     ${u.totalTokens}`);
  }

  // Show stream stats
  const reasoning = entries.filter((e) => e.fields.type === 'reasoning').length;
  const content = entries.filter((e) => e.fields.type === 'content').length;
  console.log(`\n--- Stream Stats ---`);
  console.log(`  Reasoning chunks: ${reasoning}`);
  console.log(`  Content chunks:   ${content}`);
  console.log(`  Total chunks:     ${entries.length}`);

  await worker.close();
  await queue.close();
  console.log('\n[OK] Reasoning stream example complete');
  process.exit(0);
}

main().catch((err) => {
  console.error('[ERROR]', err);
  process.exit(1);
});
