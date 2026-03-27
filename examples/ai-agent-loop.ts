/**
 * AI Agent Loop - ReAct-style agent
 *
 * Combines: suspend/resume + fallbacks + usage tracking + TPM limiter
 *
 * The agent receives a task and iterates through a plan/execute/observe loop.
 * Each step makes an LLM call, tracks usage, and decides whether to continue
 * or ask for human input. TPM limiter prevents overwhelming the API.
 *
 * Run: npx tsx examples/ai-agent-loop.ts
 */
import { Queue, Worker } from '../dist/index';
import { chat, MODELS, CONNECTION, type Message } from './llm';

const QUEUE = `ai-agent-${Date.now()}`;

const TOOLS: Record<string, (input: string) => string> = {
  search: (q) => `Search results for "${q}": Found 3 articles about ${q}.`,
  calculate: (expr) => `Calculation: ${expr} = 42`,
  lookup: (key) => `Database lookup for "${key}": Record found with status=active.`,
};

async function main() {
  const queue = new Queue(QUEUE, { connection: CONNECTION });
  let finalResult: any = null;

  const worker = new Worker(QUEUE, async (job) => {
    const { task, history = [], iteration = 0 } = job.data;

    // Check for human signal on resume
    if (job.signals.length > 0) {
      const sig = job.signals[0];
      const data = typeof sig.data === 'string' ? JSON.parse(sig.data) : sig.data;
      console.log(`[Agent] Received human input: "${data.input}"`);
      // Continue with human's answer as context
      history.push({ role: 'user' as const, content: `Human provided: ${data.input}` });
    }

    if (iteration >= 4) {
      console.log('[Agent] Max iterations reached. Summarizing...');
      const result = await chat(MODELS.fast, [
        ...history,
        { role: 'user', content: 'Summarize what you accomplished in one sentence.' },
      ], 60);
      await job.reportUsage({ model: result.model, tokens: { input: result.inputTokens, output: result.outputTokens } });
      return { status: 'done', summary: result.content, iterations: iteration };
    }

    // Step 1: Plan
    console.log(`\n[Agent] Iteration ${iteration + 1} - Planning...`);
    const planMessages: Message[] = [
      { role: 'system', content: `You are an AI agent. Available tools: search, calculate, lookup. Task: ${task}\nRespond with EXACTLY one line in format: ACTION: tool_name(input) OR DONE: summary OR ASK: question for human` },
      ...history,
      { role: 'user', content: iteration === 0 ? 'What is your first action?' : 'What is your next action?' },
    ];

    const plan = await chat(MODELS.fast, planMessages, 50);
    await job.reportUsage({ model: plan.model, tokens: { input: plan.inputTokens, output: plan.outputTokens } });
    console.log(`[Agent] Plan: ${plan.content.trim()}`);

    const action = plan.content.trim();
    history.push({ role: 'assistant' as const, content: action });

    // Step 2: Check if done
    if (action.startsWith('DONE:')) {
      return { status: 'done', summary: action.slice(5).trim(), iterations: iteration + 1 };
    }

    // Step 3: Check if needs human input
    if (action.startsWith('ASK:')) {
      console.log(`[Agent] Needs human input: ${action.slice(4).trim()}`);
      await job.suspend({ reason: 'needs-human-input' });
      return; // unreachable
    }

    // Step 4: Execute tool
    const toolMatch = action.match(/ACTION:\s*(\w+)\((.+?)\)/);
    let observation: string;
    if (toolMatch) {
      const [, toolName, toolInput] = toolMatch;
      const tool = TOOLS[toolName];
      observation = tool ? tool(toolInput) : `Unknown tool: ${toolName}`;
      console.log(`[Agent] Tool result: ${observation}`);
    } else {
      observation = 'Could not parse action. Try again with format: ACTION: tool_name(input)';
    }

    history.push({ role: 'user' as const, content: `Observation: ${observation}` });

    // Step 5: Update data then re-enqueue (moveToDelayed throws, so updateData must come first)
    await job.updateData({ task, history, iteration: iteration + 1 });
    await job.moveToDelayed(Date.now() + 100);
  }, {
    connection: CONNECTION,
    concurrency: 1,
    tokenLimiter: { maxTokens: 500, duration: 10_000 },
    stalledInterval: 30_000,
  });

  worker.on('completed', (_job, result) => { finalResult = result; });

  await queue.add('agent-task', {
    task: 'Find information about message queues and calculate the total number of articles found.',
    history: [],
    iteration: 0,
  }, {
    fallbacks: [
      { model: MODELS.nano },
      { model: MODELS.fast },
    ],
  });

  console.log('Agent started. Processing task...');

  // Wait for completion or suspension
  const deadline = Date.now() + 60_000;
  while (!finalResult && Date.now() < deadline) {
    await new Promise(r => setTimeout(r, 500));
  }

  if (finalResult) {
    console.log('\n--- Agent Result ---');
    console.log(`  Status: ${finalResult.status}`);
    console.log(`  Iterations: ${finalResult.iterations}`);
    console.log(`  Summary: ${finalResult.summary}`);
  } else {
    console.log('\n[WARN] Agent did not complete in time (may be waiting for human input)');
  }

  await worker.close(true);
  await queue.obliterate({ force: true });
  await queue.close();

  console.log('\n[OK] AI agent loop example complete');
  process.exit(0);
}

main().catch((err) => {
  console.error('[ERROR]', err);
  process.exit(1);
});
