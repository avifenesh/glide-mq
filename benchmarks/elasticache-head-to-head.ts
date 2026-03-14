/**
 * Standalone head-to-head benchmark: glide-mq vs BullMQ on ElastiCache with TLS.
 *
 * Usage (from EC2):
 *   ELASTICACHE_HOST=your-cluster.xxxxx.use1.cache.amazonaws.com \
 *   ELASTICACHE_PORT=6379 \
 *   npx tsx benchmarks/elasticache-head-to-head.ts
 *
 * Optional env vars:
 *   ELASTICACHE_HOST   - ElastiCache endpoint (required)
 *   ELASTICACHE_PORT   - port (default: 6379)
 *   USE_TLS            - enable TLS (default: true)
 *   USE_IAM            - use IAM auth (default: false)
 *   IAM_USER_ID        - IAM user ID (required if USE_IAM=true)
 *   IAM_CLUSTER_NAME   - ElastiCache cluster name (required if USE_IAM=true)
 *   AWS_REGION         - AWS region (default: us-east-1)
 *   CLUSTER_MODE       - connect as cluster client (default: false)
 *   N                  - jobs per run (default: 10000)
 *   CONCURRENCIES      - comma-separated concurrency levels (default: 1,10)
 *   WARMUP_N           - warmup jobs before each measured run (default: 500)
 */

import { Queue, Worker } from '../dist';
import { Queue as BullQueue, Worker as BullWorker } from 'bullmq';
import type { ConnectionOptions } from '../dist';
import Redis from 'ioredis';

// ---- Config ----

const HOST = process.env.ELASTICACHE_HOST;
if (!HOST) {
  console.error('ERROR: ELASTICACHE_HOST is required');
  process.exit(1);
}

const PORT = Number(process.env.ELASTICACHE_PORT) || 6379;
const USE_TLS = process.env.USE_TLS !== 'false'; // default true
const USE_IAM = process.env.USE_IAM === 'true';
const CLUSTER_MODE = process.env.CLUSTER_MODE === 'true';
const N = Number(process.env.N) || 10000;
const CONCURRENCIES = (process.env.CONCURRENCIES || '1,10').split(',').map(Number);
const WARMUP_N = Number(process.env.WARMUP_N) || 500;
const AWS_REGION = process.env.AWS_REGION || 'us-east-1';

// glide-mq connection
function buildGlideConnection(): ConnectionOptions {
  const conn: ConnectionOptions = {
    addresses: [{ host: HOST!, port: PORT }],
    useTLS: USE_TLS,
    clusterMode: CLUSTER_MODE,
  };

  if (USE_IAM) {
    conn.credentials = {
      type: 'iam' as const,
      serviceType: 'elasticache',
      region: AWS_REGION,
      userId: process.env.IAM_USER_ID!,
      clusterName: process.env.IAM_CLUSTER_NAME!,
    };
  }

  return conn;
}

// ioredis connection (used by BullMQ)
function buildIoredisConnection(): Record<string, any> {
  const conn: Record<string, any> = {
    host: HOST,
    port: PORT,
    maxRetriesPerRequest: null, // required by BullMQ
  };

  if (USE_TLS) {
    conn.tls = {};
  }

  return conn;
}

const GLIDE_CONN = buildGlideConnection();
const IOREDIS_CONN = buildIoredisConnection();

// ---- Helpers ----

function fmt(n: number): string {
  return n.toLocaleString('en-US', { maximumFractionDigits: 0 });
}

function pct(a: number, b: number): string {
  if (b === 0) return 'N/A';
  const diff = ((a - b) / b) * 100;
  return (diff >= 0 ? '+' : '') + diff.toFixed(1) + '%';
}

async function flushDB(): Promise<void> {
  const client = new Redis({ ...IOREDIS_CONN, lazyConnect: true });
  await client.connect();
  await client.flushdb();
  await client.quit();
}

// ---- Benchmark runners ----

interface RunResult {
  library: string;
  n: number;
  concurrency: number;
  elapsed: number;
  rate: number;
  warmupElapsed: number;
}

async function runGlideMQ(n: number, concurrency: number): Promise<RunResult> {
  const queueName = `ec-bench-glide-c${concurrency}`;
  const queue = new Queue(queueName, { connection: GLIDE_CONN });

  // Add warmup jobs
  console.log(`    Adding ${WARMUP_N} warmup + ${n} measured jobs...`);
  const totalJobs = WARMUP_N + n;
  for (let i = 0; i < totalJobs; i += 100) {
    const batch = [];
    for (let j = i; j < Math.min(i + 100, totalJobs); j++) {
      batch.push(queue.add('job', { i: j }));
    }
    await Promise.all(batch);
  }

  let completed = 0;
  let warmupDone = false;
  let warmupElapsed = 0;
  let measureStart = 0;

  const result = await new Promise<RunResult>((resolve) => {
    const worker = new Worker(
      queueName,
      async () => {},
      { connection: GLIDE_CONN, concurrency, events: false, metrics: false },
    );

    const start = performance.now();

    worker.on('completed', () => {
      completed++;

      if (!warmupDone && completed >= WARMUP_N) {
        warmupDone = true;
        warmupElapsed = performance.now() - start;
        measureStart = performance.now();
      }

      if (completed >= totalJobs) {
        const elapsed = performance.now() - measureStart;
        worker.close(true).then(() => queue.close()).then(() =>
          resolve({
            library: 'glide-mq',
            n,
            concurrency,
            elapsed,
            rate: (n / elapsed) * 1000,
            warmupElapsed,
          }),
        );
      }
    });

    worker.on('error', () => {});
  });

  return result;
}

async function runBullMQ(n: number, concurrency: number): Promise<RunResult> {
  const queueName = `ec-bench-bull-c${concurrency}`;
  const queue = new BullQueue(queueName, { connection: IOREDIS_CONN });

  console.log(`    Adding ${WARMUP_N} warmup + ${n} measured jobs...`);
  const totalJobs = WARMUP_N + n;
  for (let i = 0; i < totalJobs; i += 100) {
    const batch = [];
    for (let j = i; j < Math.min(i + 100, totalJobs); j++) {
      batch.push(queue.add('job', { i: j }));
    }
    await Promise.all(batch);
  }

  let completed = 0;
  let warmupDone = false;
  let warmupElapsed = 0;
  let measureStart = 0;

  const result = await new Promise<RunResult>((resolve) => {
    const worker = new BullWorker(
      queueName,
      async () => {},
      { connection: IOREDIS_CONN, concurrency },
    );

    const start = performance.now();

    worker.on('completed', () => {
      completed++;

      if (!warmupDone && completed >= WARMUP_N) {
        warmupDone = true;
        warmupElapsed = performance.now() - start;
        measureStart = performance.now();
      }

      if (completed >= totalJobs) {
        const elapsed = performance.now() - measureStart;
        worker.close(true).then(() => queue.close()).then(() =>
          resolve({
            library: 'bullmq',
            n,
            concurrency,
            elapsed,
            rate: (n / elapsed) * 1000,
            warmupElapsed,
          }),
        );
      }
    });

    worker.on('error', () => {});
  });

  return result;
}

// ---- Main ----

async function main() {
  console.log('ElastiCache Head-to-Head Benchmark');
  console.log('==================================');
  console.log(`Host:     ${HOST}:${PORT}`);
  console.log(`TLS:      ${USE_TLS}`);
  console.log(`IAM:      ${USE_IAM}`);
  console.log(`Cluster:  ${CLUSTER_MODE}`);
  console.log(`Jobs:     ${fmt(N)} (+ ${WARMUP_N} warmup)`);
  console.log(`Levels:   c=${CONCURRENCIES.join(', c=')}`);
  console.log('');

  // Connectivity check
  console.log('Testing connectivity...');
  try {
    const test = new Redis({ ...IOREDIS_CONN, lazyConnect: true });
    await test.connect();
    const pong = await test.ping();
    console.log(`  ioredis PING: ${pong}`);
    await test.quit();
  } catch (err) {
    console.error(`  ioredis connection failed: ${err instanceof Error ? err.message : err}`);
    process.exit(1);
  }

  const results: RunResult[] = [];

  for (const c of CONCURRENCIES) {
    console.log(`\n--- Concurrency = ${c} ---\n`);

    // glide-mq
    await flushDB();
    console.log(`  [glide-mq] Processing ${fmt(N)} jobs (c=${c})...`);
    const glide = await runGlideMQ(N, c);
    console.log(`  [glide-mq] ${fmt(glide.rate)} jobs/s (warmup: ${glide.warmupElapsed.toFixed(0)}ms)`);
    results.push(glide);

    // BullMQ
    await flushDB();
    console.log(`  [bullmq]   Processing ${fmt(N)} jobs (c=${c})...`);
    const bull = await runBullMQ(N, c);
    console.log(`  [bullmq]   ${fmt(bull.rate)} jobs/s (warmup: ${bull.warmupElapsed.toFixed(0)}ms)`);
    results.push(bull);
  }

  // Summary table
  console.log('\n\n=== RESULTS ===\n');
  console.log(
    '| Library   | c  | Jobs   | Elapsed (ms) | Rate (j/s) | vs other   |',
  );
  console.log(
    '|-----------|----:|-------:|-------------:|-----------:|------------|',
  );

  for (const c of CONCURRENCIES) {
    const glide = results.find((r) => r.library === 'glide-mq' && r.concurrency === c)!;
    const bull = results.find((r) => r.library === 'bullmq' && r.concurrency === c)!;

    console.log(
      `| glide-mq  | ${c.toString().padStart(2)} | ${fmt(glide.n).padStart(6)} | ${glide.elapsed.toFixed(0).padStart(12)} | ${fmt(glide.rate).padStart(10)} | ${pct(glide.rate, bull.rate).padStart(10)} |`,
    );
    console.log(
      `| bullmq    | ${c.toString().padStart(2)} | ${fmt(bull.n).padStart(6)} | ${bull.elapsed.toFixed(0).padStart(12)} | ${fmt(bull.rate).padStart(10)} | ${pct(bull.rate, glide.rate).padStart(10)} |`,
    );
  }

  console.log('\nNote: "vs other" shows % difference in rate (positive = faster).');
  console.log('Warmup jobs are excluded from measured throughput.\n');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
