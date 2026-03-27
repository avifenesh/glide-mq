/**
 * Vercel AI SDK Integration - Durable LLM execution via glide-mq
 *
 * Real scenario: Use Vercel AI SDK (generateText, streamText) inside a
 * glide-mq worker for durable, retryable AI inference. OpenRouter provides
 * the model backend. Token usage is reported from the AI SDK response.
 *
 * Run: npx tsx examples/with-vercel-ai-sdk.ts
 */
import { createOpenAI } from '@ai-sdk/openai';
import { generateText, streamText } from 'ai';
import { Queue, Worker } from '../dist/index';
import { CONNECTION, MODELS } from './llm';

const QUEUE = `vercel-ai-${Date.now()}`;

const openrouter = createOpenAI({
  baseURL: 'https://openrouter.ai/api/v1',
  apiKey: process.env.OPENROUTER_API_KEY,
  name: 'openrouter',
  compatibility: 'compatible',
});

async function main() {
  const queue = new Queue(QUEUE, { connection: CONNECTION });

  // --- Example 1: generateText ---
  console.log('=== Example 1: generateText ===\n');

  const genWorker = new Worker(QUEUE, async (job) => {
    const { prompt, model } = job.data;

    if (job.data.mode === 'stream') {
      // Example 2 handler below
      const result = streamText({
        model: openrouter.chat(model),
        prompt,
        maxTokens: 150,
      });

      for await (const chunk of result.textStream) {
        await job.stream({ t: chunk });
      }
      await job.stream({ t: '', done: '1' });

      const usage = await result.usage;
      await job.reportUsage({
        model,
        provider: 'openrouter',
        inputTokens: usage.inputTokens ?? 0,
        outputTokens: usage.outputTokens ?? 0,
      });

      const text = await result.text;
      return { content: text, inputTokens: usage.inputTokens, outputTokens: usage.outputTokens };
    }

    // Default: generateText
    const result = await generateText({
      model: openrouter.chat(model),
      prompt,
      maxTokens: 150,
    });

    await job.reportUsage({
      model,
      provider: 'openrouter',
      inputTokens: result.usage.inputTokens ?? 0,
      outputTokens: result.usage.outputTokens ?? 0,
    });

    return {
      content: result.text,
      inputTokens: result.usage.inputTokens,
      outputTokens: result.usage.outputTokens,
      finishReason: result.finishReason,
    };
  }, { connection: CONNECTION, concurrency: 1 });

  let genResult: any = null;
  genWorker.on('completed', (_job, result) => { genResult = result; });

  const genJob = await queue.add('generate', {
    prompt: 'Explain why message queues are important for AI applications in 2-3 sentences.',
    model: MODELS.fast,
    mode: 'generate',
  });

  // Wait for generation
  const deadline = Date.now() + 60_000;
  while (!genResult && Date.now() < deadline) {
    await new Promise(r => setTimeout(r, 300));
  }

  if (genResult) {
    console.log(`Response: ${genResult.content}`);
    console.log(`Tokens: in=${genResult.inputTokens}, out=${genResult.outputTokens}`);
    console.log(`Finish reason: ${genResult.finishReason}\n`);
  }

  // --- Example 2: streamText with job.stream ---
  console.log('=== Example 2: streamText + job.stream ===\n');

  let streamDone = false;
  genWorker.on('completed', () => { streamDone = true; });

  const streamJob = await queue.add('stream', {
    prompt: 'Write a haiku about distributed systems.',
    model: MODELS.fast,
    mode: 'stream',
  });

  if (!streamJob) {
    console.error('Failed to add stream job');
    await cleanup(genWorker, queue);
    process.exit(1);
  }

  // Read stream from consumer side
  let lastId: string | undefined;
  let done = false;
  process.stdout.write('Streaming: ');

  while (!done) {
    const entries = await queue.readStream(streamJob.id, { lastId, count: 50 });
    for (const entry of entries) {
      lastId = entry.id;
      if (entry.fields.done === '1') {
        done = true;
        break;
      }
      process.stdout.write(entry.fields.t);
    }
    if (!done && entries.length === 0) {
      await new Promise(r => setTimeout(r, 100));
    }
  }

  console.log('\n');

  // Wait for stream job to fully complete
  while (!streamDone && Date.now() < deadline) {
    await new Promise(r => setTimeout(r, 200));
  }

  // Check usage on completed job
  const finishedJob = await queue.getJob(streamJob.id);
  if (finishedJob?.usage) {
    console.log(`Stream job usage: model=${finishedJob.usage.model}, in=${finishedJob.usage.inputTokens}, out=${finishedJob.usage.outputTokens}`);
  }

  await cleanup(genWorker, queue);
  console.log('\n[OK] Vercel AI SDK example complete');
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
