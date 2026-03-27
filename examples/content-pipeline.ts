/**
 * Content Pipeline - content moderation with streaming + human approval + fallbacks + budget
 *
 * Flow:
 * 1. AI classifies user-submitted content (fast model)
 * 2. If borderline, suspends for human review
 * 3. If approved, AI generates a polished version (streaming, with fallback models)
 * Budget cap prevents excessive generation costs.
 *
 * Run: npx tsx examples/content-pipeline.ts
 */
import { Queue, Worker, FlowProducer } from '../dist/index';
import { chat, streamChat, MODELS, CONNECTION, type Message } from './llm';

const QUEUE = `content-pipeline-${Date.now()}`;

async function main() {
  const queue = new Queue(QUEUE, { connection: CONNECTION });
  const flow = new FlowProducer({ connection: CONNECTION });
  let pipelineDone = false;
  let pipelineResult: any = null;

  const worker = new Worker(QUEUE, async (job) => {
    const { step } = job.data;

    if (step === 'classify') {
      console.log('[classify] Analyzing content...');
      const result = await chat(MODELS.fast, [
        { role: 'system', content: 'Classify the following content as: safe, borderline, or unsafe. Respond with exactly one word.' },
        { role: 'user', content: job.data.content },
      ], 10);

      await job.reportUsage({ model: result.model, tokens: { input: result.inputTokens, output: result.outputTokens } });

      const classification = result.content.trim().toLowerCase();
      console.log(`[classify] Result: ${classification}`);
      return { classification, tokens: result.totalTokens };
    }

    if (step === 'moderate') {
      // Check classification from sibling job
      const children = await job.getChildrenValues();
      const classifyResult = Object.values(children).find((v: any) => v?.classification);
      const classification = (classifyResult as any)?.classification ?? 'borderline';

      if (classification === 'unsafe') {
        return { action: 'rejected', reason: 'Content classified as unsafe' };
      }

      if (classification === 'borderline') {
        // Check if we have a signal from human review
        if (job.signals.length > 0) {
          const sig = job.signals[0];
          const data = typeof sig.data === 'string' ? JSON.parse(sig.data) : sig.data;
          if (data.action === 'approve') {
            console.log('[moderate] Human approved. Proceeding to polish.');
            return { action: 'approved' };
          }
          return { action: 'rejected', reason: data.reason || 'Human rejected' };
        }

        console.log('[moderate] Content is borderline. Suspending for human review...');
        console.log(`\n  Content: "${job.data.content}"\n`);
        await job.suspend({ reason: 'borderline-content-review' });
        return; // unreachable
      }

      return { action: 'approved' };
    }

    if (step === 'polish') {
      console.log('[polish] Generating polished version (streaming)...');

      // Use fallback model from chain if primary fails
      const fallback = job.currentFallback;
      const model = fallback ? fallback.model : MODELS.fast;

      const messages: Message[] = [
        { role: 'system', content: 'Rewrite the following content to be more professional and engaging. Keep it concise (2-3 sentences).' },
        { role: 'user', content: job.data.content },
      ];

      let full = '';
      let inTok = 0, outTok = 0;
      process.stdout.write('\n  ');
      for await (const chunk of streamChat(model, messages, 150)) {
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

      await job.reportUsage({ model, tokens: { input: inTok, output: outTok } });
      return { polished: full, tokens: inTok + outTok };
    }

    if (step === 'aggregate') {
      const children = await job.getChildrenValues();
      pipelineDone = true;
      pipelineResult = children;
      return children;
    }

    return null;
  }, { connection: CONNECTION, concurrency: 2 });

  const content = 'Our new product might help people lose weight rapidly with minimal effort, results may vary.';

  const node = await flow.add(
    {
      name: 'pipeline',
      queueName: QUEUE,
      data: { step: 'aggregate' },
      children: [
        { name: 'classify', queueName: QUEUE, data: { step: 'classify', content } },
        {
          name: 'moderate', queueName: QUEUE,
          data: { step: 'moderate', content },
          children: [
            { name: 'classify-for-moderate', queueName: QUEUE, data: { step: 'classify', content } },
          ],
        },
        {
          name: 'polish', queueName: QUEUE,
          data: { step: 'polish', content },
          opts: {
            lockDuration: 60_000,
            fallbacks: [
              { model: MODELS.nano },
              { model: MODELS.fast },
            ],
          },
        },
      ],
    },
    { budget: { maxTotalTokens: 800 } },
  );

  console.log(`Content pipeline started: "${content.slice(0, 50)}..."`);
  console.log(`Flow: parent=${node.job.id}, budget=800 tokens\n`);

  // Wait a bit, then check if moderation is suspended
  await new Promise(r => setTimeout(r, 5_000));

  const moderateJob = node.children!.find(c => c.job.name === 'moderate');
  if (moderateJob) {
    const info = await queue.getSuspendInfo(moderateJob.job.id);
    if (info) {
      console.log('[Human] Content flagged as borderline. Auto-approving for demo...');
      await queue.signal(moderateJob.job.id, 'review', { action: 'approve' });
    }
  }

  // Wait for pipeline completion
  const deadline = Date.now() + 60_000;
  while (!pipelineDone && Date.now() < deadline) {
    await new Promise(r => setTimeout(r, 500));
  }

  // Print usage summary
  const usage = await queue.getFlowUsage(node.job.id);
  console.log('--- Pipeline Usage ---');
  console.log(`  Jobs: ${usage.jobCount}, Tokens: ${usage.totalInputTokens + usage.totalOutputTokens}`);

  const budget = await queue.getFlowBudget(node.job.id);
  if (budget) {
    console.log(`  Budget: ${budget.usedTokens}/${budget.maxTotalTokens} tokens`);
  }

  await worker.close(true);
  await queue.obliterate({ force: true });
  await queue.close();
  await flow.close();

  console.log('\n[OK] Content pipeline example complete');
  process.exit(0);
}

main().catch((err) => {
  console.error('[ERROR]', err);
  process.exit(1);
});
