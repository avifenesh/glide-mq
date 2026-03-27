/**
 * RAG Pipeline - streaming + usage tracking + budget + per-job lock
 *
 * Combines multiple AI primitives in a retrieval-augmented generation flow:
 * 1. Embed query (fast model, short lock)
 * 2. Vector search (simulated - returns fake docs)
 * 3. Generate response (large model, streaming, long lock)
 * Parent aggregates results. Budget caps the entire flow.
 *
 * Run: npx tsx examples/rag-pipeline.ts
 */
import { Queue, Worker, FlowProducer } from '../dist/index';
import { chat, streamChat, MODELS, CONNECTION, type Message } from './llm';

const QUEUE = `rag-pipeline-${Date.now()}`;

async function main() {
  const queue = new Queue(QUEUE, { connection: CONNECTION });
  const flow = new FlowProducer({ connection: CONNECTION });
  let parentDone = false;

  const worker = new Worker(QUEUE, async (job) => {
    const { step } = job.data;

    if (step === 'embed') {
      console.log('[embed] Creating query embedding...');
      const result = await chat(MODELS.fast, [
        { role: 'user', content: `Represent this for retrieval: "${job.data.query}"` },
      ], 30);
      await job.reportUsage({ model: result.model, tokens: { input: result.inputTokens, output: result.outputTokens } });
      console.log(`[embed] Done (${result.totalTokens} tokens)`);
      return { embedding: '[0.12, -0.45, 0.78, ...]', tokens: result.totalTokens };
    }

    if (step === 'search') {
      console.log('[search] Searching vector store (simulated)...');
      await new Promise(r => setTimeout(r, 200));
      const docs = [
        'Message queues enable async processing by decoupling producers from consumers.',
        'Valkey is a high-performance key-value store forked from Redis.',
        'glide-mq uses Valkey server functions for atomic queue operations.',
      ];
      return { docs, tokens: 0 };
    }

    if (step === 'generate') {
      const docs = job.data.context || [];
      const query = job.data.query;
      console.log(`[generate] Streaming response (${docs.length} context docs)...`);
      console.log('');

      const messages: Message[] = [
        { role: 'system', content: `Answer based on these documents:\n${docs.join('\n')}` },
        { role: 'user', content: query },
      ];

      let full = '';
      let inTok = 0, outTok = 0;
      for await (const chunk of streamChat(MODELS.fast, messages, 150)) {
        if (chunk.type === 'token') {
          process.stdout.write(chunk.content);
          await job.stream({ t: chunk.content });
          full += chunk.content;
        } else {
          inTok = chunk.inputTokens ?? 0;
          outTok = chunk.outputTokens ?? 0;
          await job.stream({ t: '', done: '1' });
        }
      }
      console.log('\n');

      await job.reportUsage({ model: MODELS.fast, tokens: { input: inTok, output: outTok } });
      console.log(`[generate] Done (in=${inTok}, out=${outTok})`);
      return { content: full, tokens: { input: inTok, output: outTok } };
    }

    if (step === 'aggregate') {
      const children = await job.getChildrenValues();
      parentDone = true;
      return { childCount: Object.keys(children).length };
    }

    return null;
  }, { connection: CONNECTION, concurrency: 2 });

  const query = 'How does glide-mq handle queue operations?';

  const node = await flow.add(
    {
      name: 'rag',
      queueName: QUEUE,
      data: { step: 'aggregate', query },
      children: [
        { name: 'embed', queueName: QUEUE, data: { step: 'embed', query }, opts: { lockDuration: 5_000 } },
        { name: 'search', queueName: QUEUE, data: { step: 'search', query }, opts: { lockDuration: 5_000 } },
        {
          name: 'generate', queueName: QUEUE,
          data: { step: 'generate', query, context: [
            'Message queues enable async processing by decoupling producers from consumers.',
            'glide-mq uses Valkey server functions for atomic queue operations.',
          ]},
          opts: { lockDuration: 60_000 },
        },
      ],
    },
    { budget: { maxTotalTokens: 1000 } },
  );

  console.log(`RAG flow created: parent=${node.job.id}, budget=1000 tokens`);
  console.log(`Query: "${query}"\n`);

  // Also poll the generate job's stream for live output
  const genJobId = node.children!.find(c => c.job.name === 'generate')!.job.id;
  let streamLastId: string | undefined;
  let streamDone = false;

  const deadline = Date.now() + 60_000;
  while (!parentDone && Date.now() < deadline) {
    if (!streamDone) {
      const entries = await queue.readStream(genJobId, { lastId: streamLastId, count: 50 });
      for (const e of entries) {
        streamLastId = e.id;
        if (e.fields.done === '1') streamDone = true;
      }
    }
    await new Promise(r => setTimeout(r, 300));
  }

  // Print flow usage
  const usage = await queue.getFlowUsage(node.job.id);
  console.log('--- Flow Usage ---');
  console.log(`  Jobs: ${usage.jobCount}, Input: ${usage.totalInputTokens}, Output: ${usage.totalOutputTokens}`);
  console.log(`  Models: ${JSON.stringify(usage.models)}`);

  const budget = await queue.getFlowBudget(node.job.id);
  if (budget) {
    console.log(`  Budget: ${budget.usedTokens}/${budget.maxTotalTokens} tokens`);
  }

  await worker.close(true);
  await queue.obliterate({ force: true });
  await queue.close();
  await flow.close();

  console.log('\n[OK] RAG pipeline example complete');
  process.exit(0);
}

main().catch((err) => {
  console.error('[ERROR]', err);
  process.exit(1);
});
