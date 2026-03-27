/**
 * LangChain Integration - Durable chain execution via glide-mq
 *
 * Real scenario: LangChain chains (prompt | model | parser) run inside
 * glide-mq workers for durable, retryable execution. Each pipeline step
 * (research -> summarize -> format) uses a different chain. Token usage
 * is reported from LangChain's response metadata.
 *
 * Run: npx tsx examples/with-langchain.ts
 */
import { ChatOpenAI } from '@langchain/openai';
import { ChatPromptTemplate } from '@langchain/core/prompts';
import { StringOutputParser } from '@langchain/core/output_parsers';
import { Queue, Worker, FlowProducer } from '../dist/index';
import { CONNECTION } from './llm';

const QUEUE = `langchain-${Date.now()}`;
const OPENROUTER_API_KEY = process.env.OPENROUTER_API_KEY;
if (!OPENROUTER_API_KEY) {
  console.error('Set OPENROUTER_API_KEY env var');
  process.exit(1);
}

const llm = new ChatOpenAI({
  model: 'arcee-ai/trinity-large-preview:free',
  configuration: { baseURL: 'https://openrouter.ai/api/v1' },
  apiKey: OPENROUTER_API_KEY,
  maxTokens: 150,
  temperature: 0.7,
});

const parser = new StringOutputParser();

// Three chains for the pipeline steps
const researchChain = ChatPromptTemplate.fromMessages([
  ['system', 'You are a research assistant. List 3 key facts about the topic.'],
  ['user', '{topic}'],
]).pipe(llm).pipe(parser);

const summarizeChain = ChatPromptTemplate.fromMessages([
  ['system', 'Summarize the following research into one concise paragraph.'],
  ['user', '{research}'],
]).pipe(llm).pipe(parser);

const formatChain = ChatPromptTemplate.fromMessages([
  ['system', 'Format this text as a professional brief with a title and bullet points.'],
  ['user', '{summary}'],
]).pipe(llm).pipe(parser);

async function invokeWithUsage(chain: any, input: Record<string, string>) {
  // Invoke the model directly to capture usage metadata, then run through parser
  const prompt = chain.first; // ChatPromptTemplate
  const messages = await prompt.formatMessages(input);
  const response = await llm.invoke(messages);
  const text = String(response.content);
  const usage = (response.response_metadata as any)?.tokenUsage;
  return { text, usage };
}

async function main() {
  const queue = new Queue(QUEUE, { connection: CONNECTION });
  const flow = new FlowProducer({ connection: CONNECTION });
  const results: Record<string, any> = {};

  const worker = new Worker(QUEUE, async (job) => {
    const { step, topic, research, summary } = job.data;

    if (step === 'aggregate') {
      const children = await job.getChildrenValues();
      return { children: Object.keys(children).length };
    }

    let output: string;

    if (step === 'research') {
      const r = await invokeWithUsage(researchChain, { topic });
      output = r.text;
      if (r.usage) {
        await job.reportUsage({
          model: 'arcee-ai/trinity-large-preview:free',
          provider: 'openrouter',
          tokens: { input: r.usage.promptTokens ?? 0, output: r.usage.completionTokens ?? 0 },
        });
      }
    } else if (step === 'summarize') {
      const r = await invokeWithUsage(summarizeChain, { research });
      output = r.text;
      if (r.usage) {
        await job.reportUsage({
          model: 'arcee-ai/trinity-large-preview:free',
          provider: 'openrouter',
          tokens: { input: r.usage.promptTokens ?? 0, output: r.usage.completionTokens ?? 0 },
        });
      }
    } else {
      const r = await invokeWithUsage(formatChain, { summary });
      output = r.text;
      if (r.usage) {
        await job.reportUsage({
          model: 'arcee-ai/trinity-large-preview:free',
          provider: 'openrouter',
          tokens: { input: r.usage.promptTokens ?? 0, output: r.usage.completionTokens ?? 0 },
        });
      }
    }

    console.log(`[${step}] ${output.slice(0, 80)}...`);
    results[step] = output;
    return { output };
  }, { connection: CONNECTION, concurrency: 1 });

  const completedCount = { n: 0 };
  worker.on('completed', (job) => {
    if (job.data.step !== 'aggregate') completedCount.n++;
  });

  console.log('Running 3-step LangChain pipeline: research -> summarize -> format\n');

  // Step 1: Research
  const researchJob = await queue.add('research', {
    step: 'research',
    topic: 'The impact of AI on healthcare',
  });
  const deadline = Date.now() + 90_000;
  while (completedCount.n < 1 && Date.now() < deadline) {
    await new Promise(r => setTimeout(r, 300));
  }

  // Step 2: Summarize using research output
  const sumJob = await queue.add('summarize', {
    step: 'summarize',
    research: results.research ?? '',
  });
  while (completedCount.n < 2 && Date.now() < deadline) {
    await new Promise(r => setTimeout(r, 300));
  }

  // Step 3: Format using summary
  const fmtJob = await queue.add('format', {
    step: 'format',
    summary: results.summarize ?? '',
  });
  while (completedCount.n < 3 && Date.now() < deadline) {
    await new Promise(r => setTimeout(r, 300));
  }

  // Final output
  console.log('\n--- Final Brief ---');
  console.log(results.format ?? '(not available)');

  // Per-job usage
  console.log('\n--- Per-Job Usage ---');
  for (const id of [researchJob?.id, sumJob?.id, fmtJob?.id]) {
    if (!id) continue;
    const j = await queue.getJob(id);
    if (j?.usage) {
      console.log(`  ${j.name}: in=${j.usage.tokens?.input ?? 0}, out=${j.usage.tokens?.output ?? 0}`);
    }
  }

  await worker.close(true);
  await queue.obliterate({ force: true });
  await queue.close();
  await flow.close();

  console.log('\n[OK] LangChain example complete');
  process.exit(0);
}

main().catch((err) => {
  console.error('[ERROR]', err);
  process.exit(1);
});
