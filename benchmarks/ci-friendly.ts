/**
 * CI-friendly benchmark suite.
 *
 * Measures short, deterministic workloads that are stable in CI:
 * - throughput and tail latency
 * - Redis keyspace hit/miss counters
 * - command volume and CPU deltas from INFO stats/cpu
 */

import { Queue, Worker } from '../dist';
import { Queue as BullQueue, Worker as BullWorker } from 'bullmq';
import {
  flushDB,
  fmt,
  fmtMs,
  percentile,
  printTable,
  sleep,
  GLIDE_CONNECTION,
  BULL_CONNECTION,
  readRedisStats,
  diffRedisStats,
} from './utils';

type LibraryName = 'glide-mq' | 'bullmq';

interface Adapter {
  name: LibraryName;
  createQueue: (queueName: string) => any;
  createWorker: (queueName: string, processor: (job: any) => Promise<void>) => any;
  add: (queue: any, name: string, data: unknown) => Promise<string>;
  waitUntilReady: (worker: any) => Promise<void>;
  closeWorker: (worker: any) => Promise<void>;
  closeQueue: (queue: any) => Promise<void>;
}

interface RoundResult {
  library: LibraryName;
  round: number;
  produced: number;
  completed: number;
  drained: boolean;
  throughput: number;
  addP95: number;
  e2eP95: number;
  keyspaceHits: number;
  keyspaceMisses: number;
  missRatePct: number;
  totalCommands: number;
  evictedKeys: number;
  expiredKeys: number;
  cpuUserSec: number;
  cpuSysSec: number;
}

function envInt(name: string, fallback: number): number {
  const raw = process.env[name];
  if (!raw) return fallback;
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.floor(n);
}

const ROUNDS = envInt('BENCH_CI_ROUNDS', 3);
const JOBS = envInt('BENCH_CI_JOBS', 1200);
const CONCURRENCY = envInt('BENCH_CI_CONCURRENCY', 24);
const TIMEOUT_MS = envInt('BENCH_CI_TIMEOUT_MS', 20000);
const PAYLOAD_BYTES = envInt('BENCH_CI_PAYLOAD_BYTES', 256);

const glideAdapter: Adapter = {
  name: 'glide-mq',
  createQueue(queueName: string) {
    return new Queue(queueName, { connection: GLIDE_CONNECTION });
  },
  createWorker(queueName: string, processor: (job: any) => Promise<void>) {
    return new Worker(queueName, processor, {
      connection: GLIDE_CONNECTION,
      concurrency: CONCURRENCY,
    });
  },
  async add(queue: any, name: string, data: unknown): Promise<string> {
    const job = await queue.add(name, data);
    if (!job || job.id == null) {
      throw new Error('glide-mq add returned no job id');
    }
    return String(job.id);
  },
  waitUntilReady(worker: any): Promise<void> {
    return worker.waitUntilReady();
  },
  closeWorker(worker: any): Promise<void> {
    return worker.close(true);
  },
  closeQueue(queue: any): Promise<void> {
    return queue.close();
  },
};

const bullAdapter: Adapter = {
  name: 'bullmq',
  createQueue(queueName: string) {
    return new BullQueue(queueName, { connection: BULL_CONNECTION });
  },
  createWorker(queueName: string, processor: (job: any) => Promise<void>) {
    return new BullWorker(queueName, processor, {
      connection: BULL_CONNECTION,
      concurrency: CONCURRENCY,
    });
  },
  async add(queue: any, name: string, data: unknown): Promise<string> {
    const job = await queue.add(name, data);
    if (!job || job.id == null) {
      throw new Error('bullmq add returned no job id');
    }
    return String(job.id);
  },
  waitUntilReady(worker: any): Promise<void> {
    return worker.waitUntilReady();
  },
  closeWorker(worker: any): Promise<void> {
    return worker.close(true);
  },
  closeQueue(queue: any): Promise<void> {
    return queue.close();
  },
};

const ADAPTERS: Adapter[] = [glideAdapter, bullAdapter];
const LIBRARIES = ADAPTERS.map((adapter) => adapter.name) as LibraryName[];

function median(values: number[]): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 0) {
    return (sorted[mid - 1] + sorted[mid]) / 2;
  }
  return sorted[mid];
}

function p95(values: number[]): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  return percentile(sorted, 95);
}

async function waitFor(predicate: () => boolean, timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return true;
    await sleep(20);
  }
  return predicate();
}

function subtractOverhead(
  rawDelta: ReturnType<typeof diffRedisStats>,
  overheadDelta: ReturnType<typeof diffRedisStats>,
) {
  return {
    keyspaceHits: Math.max(0, rawDelta.keyspaceHits - overheadDelta.keyspaceHits),
    keyspaceMisses: Math.max(0, rawDelta.keyspaceMisses - overheadDelta.keyspaceMisses),
    totalCommands: Math.max(0, rawDelta.totalCommands - overheadDelta.totalCommands),
    evictedKeys: Math.max(0, rawDelta.evictedKeys - overheadDelta.evictedKeys),
    expiredKeys: Math.max(0, rawDelta.expiredKeys - overheadDelta.expiredKeys),
    usedCpuUser: Math.max(0, rawDelta.usedCpuUser - overheadDelta.usedCpuUser),
    usedCpuSys: Math.max(0, rawDelta.usedCpuSys - overheadDelta.usedCpuSys),
  };
}

async function runRound(adapter: Adapter, round: number): Promise<RoundResult> {
  const queueName = `bench-ci-${adapter.name}-r${round}-${Date.now()}`;
  const payload = 'x'.repeat(PAYLOAD_BYTES);

  const addLatenciesMs: number[] = [];
  const e2eLatenciesMs: number[] = [];
  const enqueuedAt = new Map<number, number>();
  let completed = 0;
  let workerErrorMessage: string | null = null;

  let queue: any;
  let worker: any;

  await flushDB();
  const beforeStats = await readRedisStats();

  try {
    queue = adapter.createQueue(queueName);
    worker = adapter.createWorker(queueName, async () => {});

    worker.on('completed', (job: any) => {
      completed++;
      const token = Number(job?.data?.token);
      const queuedAt = Number.isFinite(token) ? enqueuedAt.get(token) : undefined;
      if (queuedAt != null) {
        e2eLatenciesMs.push(performance.now() - queuedAt);
        enqueuedAt.delete(token);
      }
    });
    worker.on('error', (err: unknown) => {
      if (workerErrorMessage) return;
      workerErrorMessage = err instanceof Error ? err.message : String(err);
    });

    await adapter.waitUntilReady(worker);
    const startedAt = performance.now();

    for (let i = 0; i < JOBS; i++) {
      const addStarted = performance.now();
      enqueuedAt.set(i, addStarted);
      await adapter.add(queue, 'ci', { i, token: i, payload });
      addLatenciesMs.push(performance.now() - addStarted);
    }

    const drained = await waitFor(() => completed >= JOBS || workerErrorMessage != null, TIMEOUT_MS);
    if (workerErrorMessage) {
      throw new Error(`[${adapter.name}] worker error: ${workerErrorMessage}`);
    }

    const elapsedMs = performance.now() - startedAt;
    const afterStatsWorkload = await readRedisStats();
    const afterStatsOverhead = await readRedisStats();
    const rawRedisDelta = diffRedisStats(beforeStats, afterStatsWorkload);
    const overheadDelta = diffRedisStats(afterStatsWorkload, afterStatsOverhead);
    const redisDelta = subtractOverhead(rawRedisDelta, overheadDelta);

    const keyspaceTotal = redisDelta.keyspaceHits + redisDelta.keyspaceMisses;
    const missRatePct = keyspaceTotal === 0 ? 0 : (redisDelta.keyspaceMisses / keyspaceTotal) * 100;

    return {
      library: adapter.name,
      round,
      produced: JOBS,
      completed,
      drained,
      throughput: elapsedMs > 0 ? (completed / elapsedMs) * 1000 : 0,
      addP95: p95(addLatenciesMs),
      e2eP95: p95(e2eLatenciesMs),
      keyspaceHits: redisDelta.keyspaceHits,
      keyspaceMisses: redisDelta.keyspaceMisses,
      missRatePct,
      totalCommands: redisDelta.totalCommands,
      evictedKeys: redisDelta.evictedKeys,
      expiredKeys: redisDelta.expiredKeys,
      cpuUserSec: redisDelta.usedCpuUser,
      cpuSysSec: redisDelta.usedCpuSys,
    };
  } finally {
    if (worker) {
      try {
        await adapter.closeWorker(worker);
      } catch {
        // ignore
      }
    }
    if (queue) {
      try {
        await adapter.closeQueue(queue);
      } catch {
        // ignore
      }
    }
  }
}

function flattenRows(rows: Record<LibraryName, RoundResult[]>): RoundResult[] {
  return Object.values(rows)
    .flat()
    .sort((a, b) => {
      if (a.round !== b.round) return a.round - b.round;
      return a.library.localeCompare(b.library);
    });
}

function summarize(rows: Record<LibraryName, RoundResult[]>): Record<LibraryName, RoundResult> {
  const out = {} as Record<LibraryName, RoundResult>;

  for (const lib of LIBRARIES) {
    const runRows = rows[lib];
    if (runRows.length === 0) {
      throw new Error(`No benchmark rows for ${lib}`);
    }

    out[lib] = {
      library: lib,
      round: 0,
      produced: Math.round(median(runRows.map((r) => r.produced))),
      completed: Math.round(median(runRows.map((r) => r.completed))),
      drained: runRows.every((r) => r.drained),
      throughput: median(runRows.map((r) => r.throughput)),
      addP95: median(runRows.map((r) => r.addP95)),
      e2eP95: median(runRows.map((r) => r.e2eP95)),
      keyspaceHits: Math.round(median(runRows.map((r) => r.keyspaceHits))),
      keyspaceMisses: Math.round(median(runRows.map((r) => r.keyspaceMisses))),
      missRatePct: median(runRows.map((r) => r.missRatePct)),
      totalCommands: Math.round(median(runRows.map((r) => r.totalCommands))),
      evictedKeys: Math.round(median(runRows.map((r) => r.evictedKeys))),
      expiredKeys: Math.round(median(runRows.map((r) => r.expiredKeys))),
      cpuUserSec: median(runRows.map((r) => r.cpuUserSec)),
      cpuSysSec: median(runRows.map((r) => r.cpuSysSec)),
    };
  }

  return out;
}

export async function runCIFriendly(): Promise<void> {
  console.log('\n## CI-Friendly Benchmark\n');
  console.log('Short deterministic run for CI with Redis cache and CPU counters.');
  console.log(
    `Config: rounds=${ROUNDS}, jobs=${fmt(JOBS)}, concurrency=${CONCURRENCY}, payload=${PAYLOAD_BYTES} bytes, timeout=${TIMEOUT_MS}ms.`,
  );

  const rows: Record<LibraryName, RoundResult[]> = {
    'glide-mq': [],
    bullmq: [],
  };

  for (let round = 1; round <= ROUNDS; round++) {
    const order = round % 2 === 1 ? ADAPTERS : [...ADAPTERS].reverse();
    for (const adapter of order) {
      console.log(`  [ci] round ${round}/${ROUNDS} - ${adapter.name}`);
      rows[adapter.name].push(await runRound(adapter, round));
    }
  }

  const allRows = flattenRows(rows);
  const stuck = allRows.filter((row) => !row.drained);
  if (stuck.length > 0) {
    throw new Error(
      `Timed out waiting for completion: ${stuck.map((row) => `${row.library} round ${row.round}`).join(', ')}`,
    );
  }

  printTable(
    ['Library', 'Round', 'Throughput', 'Add p95', 'E2E p95', 'Cache Hits', 'Cache Misses', 'Miss Rate', 'Commands'],
    allRows.map((row) => [
      row.library,
      row.round.toString(),
      `${fmt(row.throughput)} j/s`,
      fmtMs(row.addP95),
      fmtMs(row.e2eP95),
      fmt(row.keyspaceHits),
      fmt(row.keyspaceMisses),
      `${row.missRatePct.toFixed(2)}%`,
      fmt(row.totalCommands),
    ]),
  );

  const baseline = summarize(rows);
  console.log('\nMedian baseline across rounds:\n');
  printTable(
    [
      'Library',
      'Throughput',
      'Add p95',
      'E2E p95',
      'Cache Misses',
      'Miss Rate',
      'Commands',
      'CPU user',
      'CPU sys',
      'Evicted',
      'Expired',
    ],
    LIBRARIES.map((lib) => [
      lib,
      `${fmt(baseline[lib].throughput)} j/s`,
      fmtMs(baseline[lib].addP95),
      fmtMs(baseline[lib].e2eP95),
      fmt(baseline[lib].keyspaceMisses),
      `${baseline[lib].missRatePct.toFixed(2)}%`,
      fmt(baseline[lib].totalCommands),
      `${baseline[lib].cpuUserSec.toFixed(3)} s`,
      `${baseline[lib].cpuSysSec.toFixed(3)} s`,
      fmt(baseline[lib].evictedKeys),
      fmt(baseline[lib].expiredKeys),
    ]),
  );
}
