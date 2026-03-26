/**
 * Testing Mode - Unit testing without Valkey
 *
 * Real scenario: Validate glide-mq AI features using in-memory TestQueue
 * and TestWorker. No Valkey connection, no API keys needed. Each test uses
 * mock processors to verify: reportUsage, streaming, suspend/signal, and
 * fallback chains.
 *
 * Run: npx tsx examples/testing-mode.ts
 */
import { TestQueue, TestWorker } from 'glide-mq/testing';

let passed = 0;
let failed = 0;

function assert(condition: boolean, msg: string) {
  if (condition) {
    console.log(`  [PASS] ${msg}`);
    passed++;
  } else {
    console.error(`  [FAIL] ${msg}`);
    failed++;
  }
}

// --- Test 1: reportUsage ---
async function testReportUsage() {
  console.log('\nTest 1: reportUsage');
  const queue = new TestQueue('usage-test');

  let capturedUsage: any = null;
  const worker = new TestWorker(queue, async (job) => {
    await job.reportUsage({
      model: 'gpt-4',
      provider: 'openai',
      inputTokens: 100,
      outputTokens: 50,
      latencyMs: 200,
    });
    capturedUsage = job.usage;
    return { answer: 42 };
  });

  const job = await queue.add('inference', { prompt: 'What is 6*7?' });
  assert(job !== null, 'Job added');

  // Wait for processing
  await new Promise(r => setTimeout(r, 100));

  const fetched = await queue.getJob(job!.id);
  assert(fetched !== null, 'Job fetched');
  assert(fetched!.returnvalue?.answer === 42, 'Return value correct');

  // Usage is captured during processing (on the processor's job instance)
  assert(capturedUsage?.model === 'gpt-4', 'Usage model is gpt-4');
  assert(capturedUsage?.inputTokens === 100, 'Input tokens = 100');
  assert(capturedUsage?.outputTokens === 50, 'Output tokens = 50');
  assert(capturedUsage?.latencyMs === 200, 'Latency = 200ms');

  await worker.close();
  await queue.close();
}

// --- Test 2: Streaming ---
async function testStreaming() {
  console.log('\nTest 2: stream + readStream');
  const queue = new TestQueue('stream-test');

  const worker = new TestWorker(queue, async (job) => {
    const words = ['Hello', ' ', 'world', '!'];
    for (const w of words) {
      await job.stream({ t: w });
    }
    await job.stream({ t: '', done: '1' });
    return { length: words.length };
  });

  const job = await queue.add('generate', { prompt: 'Say hello' });
  await new Promise(r => setTimeout(r, 100));

  const entries = await queue.readStream(job!.id);
  assert(entries.length === 5, `Stream has 5 entries (got ${entries.length})`);
  assert(entries[0].fields.t === 'Hello', 'First chunk is "Hello"');
  assert(entries[entries.length - 1].fields.done === '1', 'Last chunk has done flag');

  // Test resume from midpoint
  if (entries.length >= 3) {
    const resumed = await queue.readStream(job!.id, { lastId: entries[1].id });
    assert(resumed.length === entries.length - 2, `Resume skips first 2 entries (got ${resumed.length})`);
  }

  await worker.close();
  await queue.close();
}

// --- Test 3: Suspend and Signal ---
async function testSuspendSignal() {
  console.log('\nTest 3: suspend + signal');
  const queue = new TestQueue('suspend-test');

  let invocations = 0;
  const worker = new TestWorker(queue, async (job) => {
    invocations++;

    if (job.signals.length > 0) {
      const sig = job.signals[0];
      const data = typeof sig.data === 'string' ? JSON.parse(sig.data) : sig.data;
      return { approved: true, reviewer: data.reviewer };
    }

    await job.suspend({ reason: 'awaiting-approval' });
  });

  const job = await queue.add('review', { doc: 'quarterly report' });
  await new Promise(r => setTimeout(r, 100));

  // Job should be suspended
  const info = await queue.getSuspendInfo(job!.id);
  assert(info !== null, 'Job is suspended');
  assert(info?.reason === 'awaiting-approval', 'Suspend reason matches');

  // Send approval signal
  await queue.signal(job!.id, 'approval', { reviewer: 'alice' });
  await new Promise(r => setTimeout(r, 200));

  // Job should now be completed
  const fetched = await queue.getJob(job!.id);
  assert(fetched!.returnvalue?.approved === true, 'Job approved after signal');
  assert(fetched!.returnvalue?.reviewer === 'alice', 'Reviewer is alice');
  assert(invocations === 2, `Processor invoked twice (got ${invocations})`);

  await worker.close();
  await queue.close();
}

// --- Test 4: Fallback Chain ---
async function testFallbackChain() {
  console.log('\nTest 4: fallback chain on failure');
  const queue = new TestQueue('fallback-test');

  const attempts: { model: string; index: number }[] = [];

  const worker = new TestWorker(queue, async (job) => {
    const fallback = job.currentFallback;
    const model = fallback ? fallback.model : 'primary-model';
    const idx = job.fallbackIndex;
    attempts.push({ model, index: idx });

    if (model === 'primary-model') {
      throw new Error('Primary model unavailable');
    }
    if (model === 'fallback-1') {
      throw new Error('Fallback 1 also down');
    }
    return { model, content: 'Success from fallback-2' };
  });

  const job = await queue.add('inference', { prompt: 'test' }, {
    attempts: 3,
    backoff: { type: 'fixed', delay: 50 },
    fallbacks: [
      { model: 'fallback-1', provider: 'provider-a' },
      { model: 'fallback-2', provider: 'provider-b' },
    ],
  });

  // Wait for retries to complete
  await new Promise(r => setTimeout(r, 1000));

  const fetched = await queue.getJob(job!.id);
  assert(attempts.length >= 2, `Multiple attempts made (got ${attempts.length})`);
  assert(fetched!.returnvalue?.model === 'fallback-2', 'Final model is fallback-2');
  assert(fetched!.returnvalue?.content === 'Success from fallback-2', 'Content from fallback-2');

  await worker.close();
  await queue.close();
}

// --- Run all tests ---
async function main() {
  console.log('=== glide-mq Testing Mode ===');
  console.log('No Valkey, no API keys - pure in-memory tests\n');

  await testReportUsage();
  await testStreaming();
  await testSuspendSignal();
  await testFallbackChain();

  console.log(`\n=== Results: ${passed} passed, ${failed} failed ===`);
  process.exit(failed > 0 ? 1 : 0);
}

main().catch((err) => {
  console.error('[ERROR]', err);
  process.exit(1);
});
