/**
 * Token Streaming - job.stream / queue.readStream
 *
 * Real scenario: Stream a blog post generation to the terminal in real-time.
 * The worker generates a response using streamChat, publishing each token
 * chunk via job.stream(). A consumer polls readStream() and prints tokens
 * as they arrive. Demonstrates resume from a known lastId.
 *
 * Run: npx tsx examples/token-streaming.ts
 */
import { Queue, Worker } from '../dist/index';
import { streamChat, MODELS, CONNECTION } from './llm';

const QUEUE = `token-streaming-${Date.now()}`;

async function main() {
  const queue = new Queue(QUEUE, { connection: CONNECTION });

  const worker = new Worker(QUEUE, async (job) => {
    const { prompt, model } = job.data;
    let full = '';
    let totalIn = 0;
    let totalOut = 0;

    for await (const chunk of streamChat(model, [{ role: 'user', content: prompt }], 200)) {
      if (chunk.type === 'token') {
        await job.stream({ t: chunk.content });
        full += chunk.content;
      } else {
        totalIn = chunk.inputTokens ?? 0;
        totalOut = chunk.outputTokens ?? 0;
        await job.stream({ t: '', done: '1' });
      }
    }

    return { content: full, inputTokens: totalIn, outputTokens: totalOut };
  }, { connection: CONNECTION, concurrency: 1 });

  const job = await queue.add('blog-post', {
    prompt: 'Write a short paragraph about why message queues matter in AI applications.',
    model: MODELS.fast,
  });

  if (!job) {
    console.error('Failed to add job');
    await cleanup(worker, queue);
    process.exit(1);
  }

  console.log(`Job ${job.id} added. Streaming output:\n`);

  // Poll readStream for tokens
  let lastId: string | undefined;
  let done = false;
  let tokenCount = 0;

  while (!done) {
    const entries = await queue.readStream(job.id, { lastId, count: 50 });

    for (const entry of entries) {
      lastId = entry.id;
      if (entry.fields.done === '1') {
        done = true;
        break;
      }
      process.stdout.write(entry.fields.t);
      tokenCount++;
    }

    if (!done && entries.length === 0) {
      await new Promise(r => setTimeout(r, 100));
    }
  }

  console.log(`\n\n--- Stream complete: ${tokenCount} chunks received ---`);

  // Demonstrate resume: re-read from midpoint
  const allEntries = await queue.readStream(job.id);
  const midpoint = allEntries[Math.floor(allEntries.length / 2)]?.id;
  if (midpoint) {
    const resumed = await queue.readStream(job.id, { lastId: midpoint });
    console.log(`Resume from midpoint (${midpoint}): got ${resumed.length} remaining chunks`);
  }

  // Wait for job completion
  const state = await job.waitUntilFinished(500, 30_000);
  console.log(`Job state: ${state}`);

  await cleanup(worker, queue);
  console.log('[OK] Token streaming example complete');
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
