/**
 * Streaming SSE Backend - typed events for frontend integration
 *
 * Simulates the backend pattern for streaming AI responses to a web client.
 * Worker generates reasoning and content chunks via streamChunk().
 * A consumer formats them as Server-Sent Events with typed event names.
 * Shows: streamChunk types, readStream, SSE event formatting.
 *
 * No Valkey needed - uses TestQueue/TestWorker.
 * Run: npx tsx examples/streaming-sse.ts
 */
import { TestQueue, TestWorker } from '../dist/testing';

async function main() {
  const queue = new TestQueue('streaming-sse');

  const worker = new TestWorker(queue, async (job) => {
    // Simulate a thinking model: reasoning first, then content

    const reasoningSteps = [
      'The user wants a concise explanation of event-driven architecture.',
      'I should cover: definition, key components, benefits, and a real-world example.',
      'Keep it under 4 sentences for the explanation, then give one example.',
    ];

    const contentChunks = [
      'Event-driven architecture ',
      'is a design pattern where ',
      'system components communicate ',
      'through asynchronous events. ',
      'Producers emit events without knowing ',
      'who consumes them, enabling loose coupling. ',
      'This pattern excels at handling ',
      'variable workloads and real-time data flows. ',
      'Example: an e-commerce platform uses events ',
      'to trigger inventory updates, notifications, ',
      'and analytics in parallel after each order.',
    ];

    // Stream reasoning chunks
    for (const step of reasoningSteps) {
      await job.streamChunk('reasoning', step);
    }

    // Stream content token by token
    for (const chunk of contentChunks) {
      await job.streamChunk('content', chunk);
    }

    // Signal completion with usage metadata
    await job.streamChunk(
      'done',
      JSON.stringify({
        model: 'o3-mini',
        inputTokens: 85,
        outputTokens: 220,
        reasoningTokens: 340,
      }),
    );

    await job.reportUsage({
      model: 'o3-mini',
      tokens: { input: 85, output: 220, reasoning: 340 },
    });

    return { status: 'streamed' };
  });

  const job = await queue.add('explain', {
    prompt: 'Explain event-driven architecture in 4 sentences with an example.',
    model: 'o3-mini',
  });

  await new Promise((r) => setTimeout(r, 300));

  // Read the full stream - this is what your HTTP handler would do
  const entries = await queue.readStream(job!.id);

  // Format as SSE output
  console.log('--- SSE Output (as sent to frontend) ---\n');

  let sseOutput = '';
  for (const entry of entries) {
    const { type, content } = entry.fields;

    if (type === 'done') {
      const line = `id: ${entry.id}\nevent: done\ndata: ${content ?? '{}'}\n\n`;
      sseOutput += line;
      process.stdout.write(line);
    } else {
      const payload = JSON.stringify({ content: content ?? '' });
      const line = `id: ${entry.id}\nevent: ${type}\ndata: ${payload}\n\n`;
      sseOutput += line;
      process.stdout.write(line);
    }
  }

  // Demonstrate reconnection with Last-Event-ID
  console.log('--- Reconnection Demo ---\n');

  // Simulate a client that disconnected after the 5th event
  const midpointId = entries[4]?.id;
  if (midpointId) {
    const resumed = await queue.readStream(job!.id, { lastId: midpointId });
    console.log(`Client reconnects with Last-Event-ID: ${midpointId}`);
    console.log(`Server sends ${resumed.length} remaining events (of ${entries.length} total)`);

    // Show what the client would receive
    console.log(`\nResumed events:`);
    for (const entry of resumed.slice(0, 3)) {
      console.log(`  ${entry.fields.type}: ${(entry.fields.content ?? '').slice(0, 50)}...`);
    }
    if (resumed.length > 3) {
      console.log(`  ... and ${resumed.length - 3} more`);
    }
  }

  // Stream stats
  const reasoning = entries.filter((e) => e.fields.type === 'reasoning').length;
  const content = entries.filter((e) => e.fields.type === 'content').length;
  console.log(`\n--- Stream Stats ---`);
  console.log(`  Reasoning events: ${reasoning}`);
  console.log(`  Content events:   ${content}`);
  console.log(`  Total events:     ${entries.length}`);
  console.log(`  SSE bytes:        ${Buffer.byteLength(sseOutput, 'utf8')}`);

  await worker.close();
  await queue.close();
  console.log('\n[OK] Streaming SSE example complete');
  process.exit(0);
}

main().catch((err) => {
  console.error('[ERROR]', err);
  process.exit(1);
});
