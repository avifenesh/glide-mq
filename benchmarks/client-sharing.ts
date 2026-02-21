/**
 * Client sharing benchmark: measure performance of shared vs dedicated clients.
 *
 * Scenarios:
 *   A. 1 Queue + 1 Worker(c=10) - dedicated (baseline)
 *   B. Same as A - shared command client
 *   C. 3 Queues + 1 Worker(c=50) - shared command client
 *   D. Same as C - dedicated per component
 *   E. addBulk(1000) + Worker(c=100) - both modes
 *   F. Noisy neighbor: continuous addBulk while Worker processes
 *
 * Usage: npx tsx benchmarks/run.ts sharing
 */

import { GlideClient } from '@glidemq/speedkey';
import { Queue, Worker } from '../dist';
import { flushDB, fmt, fmtMs, percentile, printTable, sleep, GLIDE_CONNECTION } from './utils';

const WARMUP_JOBS = 200;
const WARMUP_WAIT_MS = 3000;
const MEASURE_DURATION_MS = 30000; // 30 seconds per scenario
const MEASURE_TIMEOUT_MS = 45000;

interface ScenarioResult {
  scenario: string;
  mode: string;
  jobsProcessed: number;
  throughput: number;
  p50: number;
  p95: number;
  p99: number;
  rssDelta: number;
  connections: number;
}

async function createSharedClient(): Promise<GlideClient> {
  return await GlideClient.createClient({
    addresses: GLIDE_CONNECTION.addresses,
  });
}

function rssMB(): number {
  return process.memoryUsage().rss / 1024 / 1024;
}

/**
 * Run a time-based scenario: continuously add+process jobs for MEASURE_DURATION_MS,
 * then measure throughput and latency.
 */
async function runScenario(
  name: string,
  mode: string,
  setup: () => Promise<{ queues: InstanceType<typeof Queue>[]; workers: InstanceType<typeof Worker>[]; sharedClient?: GlideClient; connectionCount: number }>,
): Promise<ScenarioResult> {
  await flushDB();
  global.gc?.();
  const rssStart = rssMB();

  const { queues, workers, sharedClient, connectionCount } = await setup();

  // Suppress unhandled errors during startup/teardown
  for (const w of workers) {
    w.on('error', () => {});
  }

  // Wait for workers to be ready
  for (const w of workers) {
    await w.waitUntilReady();
  }

  // Warmup: saturate pipeline, let workers settle
  for (let i = 0; i < WARMUP_JOBS; i++) {
    await queues[i % queues.length].add('warmup', { i });
  }
  await sleep(WARMUP_WAIT_MS);

  // --- Measurement phase ---
  const latencies: number[] = [];
  let completed = 0;
  let adding = true;

  // Track completion latency via events
  for (const w of workers) {
    w.on('completed', (job: any) => {
      if (job.timestamp) {
        const lat = Date.now() - job.timestamp;
        if (!adding) return; // only count during measurement window
        latencies.push(lat);
      }
      completed++;
    });
  }

  const start = performance.now();
  const deadline = start + MEASURE_DURATION_MS;
  let added = 0;

  // Continuously add jobs round-robin for the measurement duration
  while (performance.now() < deadline) {
    const q = queues[added % queues.length];
    await q.add('bench-job', { i: added });
    added++;
  }
  adding = false;

  // Wait for remaining jobs to drain (up to timeout)
  const drainDeadline = Date.now() + MEASURE_TIMEOUT_MS;
  while (completed < added && Date.now() < drainDeadline) {
    await sleep(100);
  }

  const elapsed = performance.now() - start;
  const rssDelta = rssMB() - rssStart;

  latencies.sort((a, b) => a - b);

  const result: ScenarioResult = {
    scenario: name,
    mode,
    jobsProcessed: completed,
    throughput: (completed / elapsed) * 1000,
    p50: percentile(latencies, 50),
    p95: percentile(latencies, 95),
    p99: percentile(latencies, 99),
    rssDelta,
    connections: connectionCount,
  };

  // Cleanup
  for (const w of workers) {
    await w.close(true);
  }
  for (const q of queues) {
    await q.close();
  }
  if (sharedClient) {
    sharedClient.close();
  }

  return result;
}

// --- Scenario A: 1Q + 1W(c=10) dedicated ---
async function scenarioA(): Promise<ScenarioResult> {
  return runScenario('A: 1Q+1W(c=10)', 'dedicated', async () => {
    const q = new Queue('bench-a', { connection: GLIDE_CONNECTION });
    const w = new Worker('bench-a', async () => {}, { connection: GLIDE_CONNECTION, concurrency: 10 });
    return { queues: [q], workers: [w], connectionCount: 3 }; // q=1 + w=2
  });
}

// --- Scenario B: 1Q + 1W(c=10) shared ---
async function scenarioB(): Promise<ScenarioResult> {
  return runScenario('B: 1Q+1W(c=10)', 'shared', async () => {
    const client = await createSharedClient();
    const q = new Queue('bench-b', { client });
    const w = new Worker('bench-b', async () => {}, { connection: GLIDE_CONNECTION, commandClient: client, concurrency: 10 });
    return { queues: [q], workers: [w], sharedClient: client, connectionCount: 2 }; // shared=1 + blocking=1
  });
}

// --- Scenario C: 3Q + 1W(c=50) shared ---
async function scenarioC(): Promise<ScenarioResult> {
  return runScenario('C: 3Q+1W(c=50)', 'shared', async () => {
    const client = await createSharedClient();
    const queues = [
      new Queue('bench-c', { client }),
      new Queue('bench-c', { client }),
      new Queue('bench-c', { client }),
    ];
    const w = new Worker('bench-c', async () => {}, { connection: GLIDE_CONNECTION, commandClient: client, concurrency: 50 });
    return { queues, workers: [w], sharedClient: client, connectionCount: 2 }; // shared=1 + blocking=1
  });
}

// --- Scenario D: 3Q + 1W(c=50) dedicated ---
async function scenarioD(): Promise<ScenarioResult> {
  return runScenario('D: 3Q+1W(c=50)', 'dedicated', async () => {
    const queues = [
      new Queue('bench-d', { connection: GLIDE_CONNECTION }),
      new Queue('bench-d', { connection: GLIDE_CONNECTION }),
      new Queue('bench-d', { connection: GLIDE_CONNECTION }),
    ];
    const w = new Worker('bench-d', async () => {}, { connection: GLIDE_CONNECTION, concurrency: 50 });
    return { queues, workers: [w], connectionCount: 5 }; // 3q + 2w
  });
}

// --- Scenario E: 1Q + 1W(c=100) shared (high concurrency) ---
async function scenarioEShared(): Promise<ScenarioResult> {
  return runScenario('E: 1Q+1W(c=100)', 'shared', async () => {
    const client = await createSharedClient();
    const q = new Queue('bench-e', { client });
    const w = new Worker('bench-e', async () => {}, { connection: GLIDE_CONNECTION, commandClient: client, concurrency: 100 });
    return { queues: [q], workers: [w], sharedClient: client, connectionCount: 2 };
  });
}

// --- Scenario E: 1Q + 1W(c=100) dedicated (high concurrency) ---
async function scenarioEDedicated(): Promise<ScenarioResult> {
  return runScenario('E: 1Q+1W(c=100)', 'dedicated', async () => {
    const q = new Queue('bench-e-ded', { connection: GLIDE_CONNECTION });
    const w = new Worker('bench-e-ded', async () => {}, { connection: GLIDE_CONNECTION, concurrency: 100 });
    return { queues: [q], workers: [w], connectionCount: 3 };
  });
}

// --- Run all ---

export async function runSharing(): Promise<void> {
  console.log('\n## Client Sharing: Shared vs Dedicated\n');

  const results: ScenarioResult[] = [];

  console.log('  Running scenario A: 1Q+1W(c=10) dedicated...');
  results.push(await scenarioA());

  console.log('  Running scenario B: 1Q+1W(c=10) shared...');
  results.push(await scenarioB());

  console.log('  Running scenario C: 3Q+1W(c=50) shared...');
  results.push(await scenarioC());

  console.log('  Running scenario D: 3Q+1W(c=50) dedicated...');
  results.push(await scenarioD());

  console.log('  Running scenario E: bulk(1000)+W(c=100) shared...');
  results.push(await scenarioEShared());

  console.log('  Running scenario E: bulk(1000)+W(c=100) dedicated...');
  results.push(await scenarioEDedicated());

  console.log('');
  printTable(
    ['Scenario', 'Mode', 'Jobs', 'Throughput', 'p50', 'p95', 'p99', 'RSS Delta', 'Conns'],
    results.map((r) => [
      r.scenario,
      r.mode,
      fmt(r.jobsProcessed),
      fmt(r.throughput) + ' j/s',
      fmtMs(r.p50),
      fmtMs(r.p95),
      fmtMs(r.p99),
      r.rssDelta.toFixed(1) + ' MB',
      r.connections.toString(),
    ]),
  );

  // Compare pairs
  console.log('\n### Comparison (shared vs dedicated)\n');
  const pairs = [
    { shared: results[1], dedicated: results[0], name: '1Q+1W(c=10)' },
    { shared: results[2], dedicated: results[3], name: '3Q+1W(c=50)' },
    { shared: results[4], dedicated: results[5], name: 'bulk(1000)+W(c=100)' },
  ];

  for (const { shared, dedicated, name } of pairs) {
    const throughputDelta = ((shared.throughput - dedicated.throughput) / dedicated.throughput) * 100;
    const p99Delta = dedicated.p99 > 0 ? ((shared.p99 - dedicated.p99) / dedicated.p99) * 100 : 0;
    const p95Delta = dedicated.p95 > 0 ? ((shared.p95 - dedicated.p95) / dedicated.p95) * 100 : 0;
    const connSaved = dedicated.connections - shared.connections;

    const throughputOk = throughputDelta > -5;
    const p99Ok = p99Delta < 10;
    const p95Ok = p95Delta < 8;

    console.log(`  ${name}:`);
    console.log(`    Throughput: ${throughputDelta >= 0 ? '+' : ''}${throughputDelta.toFixed(1)}% ${throughputOk ? '[PASS]' : '[FAIL: >5% degradation]'}`);
    console.log(`    p99:        ${p99Delta >= 0 ? '+' : ''}${p99Delta.toFixed(1)}% ${p99Ok ? '[PASS]' : '[FAIL: >10% increase]'}`);
    console.log(`    p95:        ${p95Delta >= 0 ? '+' : ''}${p95Delta.toFixed(1)}% ${p95Ok ? '[PASS]' : '[FAIL: >8% increase]'}`);
    console.log(`    Connections saved: ${connSaved}`);
    console.log('');
  }
}
