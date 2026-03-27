/**
 * Agent Memory - Chatbot with persistent memory
 *
 * Real scenario: Multi-turn chatbot where each turn is a queued job. Long-term
 * memory is stored in a Valkey hash. The worker loads prior context from memory,
 * calls the LLM with conversation history, and stores new entries. Finally,
 * searchJobs finds past interactions by user.
 *
 * Run: npx tsx examples/agent-memory.ts
 */
import { GlideClient } from '@glidemq/speedkey';
import { Queue, Worker } from '../dist/index';
import { chat, MODELS, CONNECTION, type Message } from './llm';

const QUEUE = `agent-memory-${Date.now()}`;
const MEMORY_KEY = `memory:${Date.now()}`;

async function main() {
  const queue = new Queue(QUEUE, { connection: CONNECTION });
  const valkey = await GlideClient.createClient({ addresses: CONNECTION.addresses });

  const completedResults: { turn: number; reply: string }[] = [];

  const worker = new Worker(QUEUE, async (job) => {
    const { userId, userMessage, turn } = job.data;

    // Load memory (prior conversation turns)
    const rawMemory = await valkey.hgetall(MEMORY_KEY);
    const history: Message[] = [];

    if (rawMemory && rawMemory.length > 0) {
      // rawMemory is HashDataType: { field, value }[]
      const sorted = [...rawMemory].sort((a, b) =>
        String(a.field).localeCompare(String(b.field))
      );
      for (const entry of sorted) {
        const parsed = JSON.parse(String(entry.value));
        history.push({ role: 'user', content: parsed.user });
        history.push({ role: 'assistant', content: parsed.assistant });
      }
    }

    // Build messages with memory
    const messages: Message[] = [
      { role: 'system', content: `You are a helpful assistant. User: ${userId}. Answer concisely.` },
      ...history,
      { role: 'user', content: userMessage },
    ];

    const result = await chat(MODELS.fast, messages, 100);
    console.log(`[Turn ${turn}] User: ${userMessage}`);
    console.log(`[Turn ${turn}] Bot: ${result.content}\n`);

    // Store this turn in memory
    await valkey.hset(MEMORY_KEY, [
      { field: `turn-${turn}`, value: JSON.stringify({ user: userMessage, assistant: result.content }) },
    ]);

    await job.reportUsage({
      model: result.model,
      tokens: { input: result.inputTokens, output: result.outputTokens },
    });

    return { reply: result.content, tokens: result.totalTokens };
  }, { connection: CONNECTION, concurrency: 1 });

  worker.on('completed', (job, result) => {
    completedResults.push({ turn: job.data.turn, reply: result.reply });
  });

  // Turn 1: initial question
  const job1 = await queue.add('chat-turn', {
    userId: 'user-42',
    userMessage: 'My name is Alice and I am building an AI chatbot. What framework should I use?',
    turn: 1,
  });

  // Wait for turn 1
  await waitForJobs(completedResults, 1);

  // Turn 2: follow-up that tests memory
  const job2 = await queue.add('chat-turn', {
    userId: 'user-42',
    userMessage: 'Can you remind me what I said my name was and what I am building?',
    turn: 2,
  });

  await waitForJobs(completedResults, 2);

  // Turn 3: different user
  const job3 = await queue.add('chat-turn', {
    userId: 'user-99',
    userMessage: 'What is a message queue?',
    turn: 3,
  });

  await waitForJobs(completedResults, 3);

  // Search jobs by user
  console.log('--- Search: jobs from user-42 ---');
  const user42Jobs = await queue.searchJobs({ data: { userId: 'user-42' } });
  console.log(`  Found ${user42Jobs.length} job(s) for user-42`);
  for (const j of user42Jobs) {
    console.log(`  id=${j.id} turn=${j.data.turn} message="${j.data.userMessage.slice(0, 50)}..."`);
  }

  console.log('\n--- Search: jobs from user-99 ---');
  const user99Jobs = await queue.searchJobs({ data: { userId: 'user-99' } });
  console.log(`  Found ${user99Jobs.length} job(s) for user-99`);

  // Verify memory was stored
  console.log('\n--- Memory contents ---');
  const mem = await valkey.hgetall(MEMORY_KEY);
  if (mem) {
    for (const entry of mem) {
      const parsed = JSON.parse(String(entry.value));
      console.log(`  ${entry.field}: user="${parsed.user.slice(0, 40)}..." assistant="${parsed.assistant.slice(0, 40)}..."`);
    }
  }

  // Cleanup
  await valkey.del([MEMORY_KEY]);
  valkey.close();
  await worker.close(true);
  await queue.obliterate({ force: true });
  await queue.close();

  console.log('\n[OK] Agent memory example complete');
  process.exit(0);
}

async function waitForJobs(results: any[], target: number) {
  const deadline = Date.now() + 45_000;
  while (results.length < target && Date.now() < deadline) {
    await new Promise(r => setTimeout(r, 300));
  }
}

main().catch((err) => {
  console.error('[ERROR]', err);
  process.exit(1);
});
