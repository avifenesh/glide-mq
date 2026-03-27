/**
 * Fallback Usage Tracking - cumulative usage across retries
 *
 * Primary model fails after consuming tokens. Fallback model succeeds.
 * Both attempts' usage is tracked - the failed attempt's tokens still count
 * toward budget and flow totals. Shows: fallbacks, cumulative usage, budget awareness.
 *
 * No Valkey needed - uses TestQueue/TestWorker.
 * Run: npx tsx examples/fallback-usage.ts
 */
import { TestQueue, TestWorker } from '../dist/testing';

interface AttemptLog {
  attempt: number;
  model: string;
  tokens: Record<string, number>;
  cost: number;
  status: 'failed' | 'success';
}

async function main() {
  const queue = new TestQueue('fallback-usage');
  const attempts: AttemptLog[] = [];

  const worker = new TestWorker(queue, async (job) => {
    const fallback = job.currentFallback;
    const attemptNum = job.attemptsMade + 1;

    if (!fallback) {
      // First attempt - primary model (claude-sonnet). Consumes tokens then fails.
      const tokens = { input: 680, output: 120, reasoning: 450 };
      const cost = (680 / 1e6) * 3.0 + (120 / 1e6) * 15.0 + (450 / 1e6) * 3.75;

      await job.reportUsage({
        model: 'claude-sonnet',
        provider: 'anthropic',
        tokens,
        costs: { input: (680 / 1e6) * 3.0, output: (120 / 1e6) * 15.0, reasoning: (450 / 1e6) * 3.75 },
        costUnit: 'usd',
        latencyMs: 2400,
      });

      attempts.push({ attempt: attemptNum, model: 'claude-sonnet', tokens, cost, status: 'failed' });
      console.log(
        `  Attempt ${attemptNum}: claude-sonnet - consumed ${Object.values(tokens).reduce((a, b) => a + b, 0)} tokens, then FAILED (rate limit)`,
      );

      throw new Error('429 Too Many Requests - rate limit exceeded');
    }

    // Fallback attempt - gpt-5.4. Succeeds.
    const tokens = { input: 680, output: 340 };
    const cost = (680 / 1e6) * 5.0 + (340 / 1e6) * 15.0;

    await job.reportUsage({
      model: fallback.model,
      provider: fallback.provider,
      tokens,
      costs: { input: (680 / 1e6) * 5.0, output: (340 / 1e6) * 15.0 },
      costUnit: 'usd',
      latencyMs: 1800,
    });

    attempts.push({ attempt: attemptNum, model: fallback.model, tokens, cost, status: 'success' });
    console.log(
      `  Attempt ${attemptNum}: ${fallback.model} - consumed ${Object.values(tokens).reduce((a, b) => a + b, 0)} tokens, SUCCESS`,
    );

    return { model: fallback.model, content: 'Generated response via fallback model' };
  });

  console.log('--- Fallback Chain Execution ---\n');

  await queue.add(
    'llm-task',
    {
      prompt: 'Explain distributed message queues in 3 paragraphs.',
    },
    {
      attempts: 3,
      backoff: { type: 'fixed', delay: 100 },
      fallbacks: [{ model: 'gpt-5.4', provider: 'openai' }],
    },
  );

  await new Promise((r) => setTimeout(r, 800));

  // Show attempt-by-attempt breakdown
  console.log('\n--- Attempt Breakdown ---\n');
  console.log('Attempt  Model           Tokens  Cost        Status');
  console.log('-------  --------------  ------  ----------  ------');

  let totalTokens = 0;
  let totalCost = 0;

  for (const a of attempts) {
    const tokSum = Object.values(a.tokens).reduce((s, v) => s + v, 0);
    totalTokens += tokSum;
    totalCost += a.cost;
    console.log(
      `  ${String(a.attempt).padStart(3)}    ` +
        `${a.model.padEnd(14)}  ` +
        `${String(tokSum).padStart(6)}  ` +
        `$${a.cost.toFixed(6).padStart(9)}  ` +
        `${a.status === 'failed' ? '[ERROR]' : '[OK]'}`,
    );
  }

  console.log(`\n--- Cumulative Usage ---`);
  console.log(`  Total attempts:  ${attempts.length}`);
  console.log(
    `  Wasted tokens:   ${Object.values(attempts[0]?.tokens ?? {}).reduce((s, v) => s + v, 0)} (from failed attempt)`,
  );
  console.log(
    `  Useful tokens:   ${Object.values(attempts[1]?.tokens ?? {}).reduce((s, v) => s + v, 0)} (from successful attempt)`,
  );
  console.log(`  Total tokens:    ${totalTokens}`);
  console.log(`  Total cost:      $${totalCost.toFixed(6)} usd`);

  const wastedPct = attempts.length > 1 ? ((attempts[0].cost / totalCost) * 100).toFixed(1) : '0';
  console.log(`  Wasted spend:    ${wastedPct}% of total cost was from the failed attempt`);

  console.log(`\n  Takeaway: fallback retries protect uptime, but failed attempts`);
  console.log(`  still consume tokens. Budget tracking captures the true cost.`);

  await worker.close();
  await queue.close();
  console.log('\n[OK] Fallback usage tracking example complete');
  process.exit(0);
}

main().catch((err) => {
  console.error('[ERROR]', err);
  process.exit(1);
});
