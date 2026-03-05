/**
 * User-centric benchmark.
 *
 * Captures the metrics queue users care about most:
 * - sustained ingest/processing behavior (throughput + tail latency)
 * - backlog drain speed after a large burst
 * - retry behavior under transient failures
 * - delayed-job start precision
 *
 * Methodology for fairness:
 * - same Valkey instance and same host/port
 * - same payloads, worker concurrency, and processor behavior
 * - alternating library execution order per round
 * - multiple rounds, median used as baseline
 */

import { mkdirSync, writeFileSync } from 'fs';
import path from 'path';
import { Queue, Worker } from '../dist';
import { Queue as BullQueue, Worker as BullWorker } from 'bullmq';
import { flushDB, fmt, fmtMs, percentile, printTable, sleep, GLIDE_CONNECTION, BULL_CONNECTION } from './utils';

type LibraryName = 'glide-mq' | 'bullmq';

interface Adapter {
  name: LibraryName;
  createQueue: (queueName: string) => any;
  createWorker: (
    queueName: string,
    processor: (job: any) => Promise<void>,
    opts: { concurrency: number; promotionInterval?: number },
  ) => any;
  add: (queue: any, name: string, data: unknown, opts?: Record<string, unknown>) => Promise<string>;
  addBulk: (queue: any, jobs: { name: string; data: unknown; opts?: Record<string, unknown> }[]) => Promise<string[]>;
  waitUntilReady: (worker: any) => Promise<void>;
  closeWorker: (worker: any) => Promise<void>;
  closeQueue: (queue: any) => Promise<void>;
  jobId: (job: any) => string;
  attemptsMade: (job: any) => number;
}

interface Quantiles {
  p50: number;
  p95: number;
  p99: number;
  max: number;
}

interface SteadyRound {
  library: LibraryName;
  round: number;
  produced: number;
  completed: number;
  backlog: number;
  drained: boolean;
  throughput: number;
  addP50: number;
  addP95: number;
  addP99: number;
  e2eP50: number;
  e2eP95: number;
  e2eP99: number;
}

interface BurstRound {
  library: LibraryName;
  round: number;
  completed: number;
  drained: boolean;
  drainMs: number;
  throughput: number;
}

interface RetryRound {
  library: LibraryName;
  round: number;
  completed: number;
  successRate: number;
  elapsedMs: number;
  throughput: number;
  failedEvents: number;
  retryExecutions: number;
  e2eP50: number;
  e2eP95: number;
  e2eP99: number;
}

interface DelayRound {
  library: LibraryName;
  round: number;
  completed: number;
  activeSeen: number;
  earlyStarts: number;
  lateP50: number;
  lateP95: number;
  lateP99: number;
  lateMax: number;
}

const glideAdapter: Adapter = {
  name: 'glide-mq',
  createQueue(queueName: string) {
    return new Queue(queueName, { connection: GLIDE_CONNECTION });
  },
  createWorker(
    queueName: string,
    processor: (job: any) => Promise<void>,
    opts: { concurrency: number; promotionInterval?: number },
  ) {
    const workerOpts: any = {
      connection: GLIDE_CONNECTION,
      concurrency: opts.concurrency,
    };
    if (opts.promotionInterval != null) {
      workerOpts.promotionInterval = opts.promotionInterval;
    }
    return new Worker(queueName, processor, workerOpts);
  },
  async add(queue: any, name: string, data: unknown, opts?: Record<string, unknown>): Promise<string> {
    const job = await queue.add(name, data, opts);
    if (!job || job.id == null) throw new Error('glide-mq add returned no job id');
    return String(job.id);
  },
  async addBulk(
    queue: any,
    jobs: { name: string; data: unknown; opts?: Record<string, unknown> }[],
  ): Promise<string[]> {
    const created = await queue.addBulk(jobs);
    const ids: string[] = [];
    for (const job of created) {
      if (!job || job.id == null) throw new Error('glide-mq addBulk returned no job id');
      ids.push(String(job.id));
    }
    return ids;
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
  jobId(job: any): string {
    return String(job.id);
  },
  attemptsMade(job: any): number {
    return Number(job.attemptsMade ?? 0);
  },
};

const bullAdapter: Adapter = {
  name: 'bullmq',
  createQueue(queueName: string) {
    return new BullQueue(queueName, { connection: BULL_CONNECTION });
  },
  createWorker(queueName: string, processor: (job: any) => Promise<void>, opts: { concurrency: number }): any {
    return new BullWorker(queueName, processor, {
      connection: BULL_CONNECTION,
      concurrency: opts.concurrency,
    });
  },
  async add(queue: any, name: string, data: unknown, opts?: Record<string, unknown>): Promise<string> {
    const job = await queue.add(name, data, opts);
    if (!job || job.id == null) throw new Error('bullmq add returned no job id');
    return String(job.id);
  },
  async addBulk(
    queue: any,
    jobs: { name: string; data: unknown; opts?: Record<string, unknown> }[],
  ): Promise<string[]> {
    const created = await queue.addBulk(jobs);
    const ids: string[] = [];
    for (const job of created) {
      if (!job || job.id == null) throw new Error('bullmq addBulk returned no job id');
      ids.push(String(job.id));
    }
    return ids;
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
  jobId(job: any): string {
    return String(job.id);
  },
  attemptsMade(job: any): number {
    return Number(job.attemptsMade ?? 0);
  },
};

const ADAPTERS: Adapter[] = [glideAdapter, bullAdapter];

function envInt(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw == null || raw === '') return fallback;
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.floor(n);
}

const ROUNDS = envInt('BENCH_ROUNDS', 3);

const WARMUP_JOBS = envInt('BENCH_WARMUP_JOBS', 300);
const WARMUP_TIMEOUT_MS = envInt('BENCH_WARMUP_TIMEOUT_MS', 20000);

const STEADY_DURATION_MS = envInt('BENCH_STEADY_DURATION_MS', 15000);
const STEADY_DRAIN_TIMEOUT_MS = envInt('BENCH_STEADY_DRAIN_TIMEOUT_MS', 30000);
const STEADY_PRODUCERS = envInt('BENCH_STEADY_PRODUCERS', 4);
const STEADY_CONCURRENCY = envInt('BENCH_STEADY_CONCURRENCY', 40);
const STEADY_PAYLOAD_BYTES = envInt('BENCH_STEADY_PAYLOAD_BYTES', 512);
const STEADY_PROCESSOR_MS = envInt('BENCH_STEADY_PROCESSOR_MS', 0);

const BURST_JOBS = envInt('BENCH_BURST_JOBS', 20000);
const BURST_BULK_SIZE = envInt('BENCH_BURST_BULK_SIZE', 500);
const BURST_DRAIN_TIMEOUT_MS = envInt('BENCH_BURST_DRAIN_TIMEOUT_MS', 60000);
const BURST_CONCURRENCY = envInt('BENCH_BURST_CONCURRENCY', 80);
const BURST_PAYLOAD_BYTES = envInt('BENCH_BURST_PAYLOAD_BYTES', 128);
const BURST_PROCESSOR_MS = envInt('BENCH_BURST_PROCESSOR_MS', 0);

const RETRY_JOBS = envInt('BENCH_RETRY_JOBS', 5000);
const RETRY_CONCURRENCY = envInt('BENCH_RETRY_CONCURRENCY', 40);
const RETRY_BACKOFF_MS = envInt('BENCH_RETRY_BACKOFF_MS', 50);
const RETRY_TIMEOUT_MS = envInt('BENCH_RETRY_TIMEOUT_MS', 60000);
const RETRY_FAIL_EVERY = envInt('BENCH_RETRY_FAIL_EVERY', 5);
const RETRY_PROCESSOR_MS = envInt('BENCH_RETRY_PROCESSOR_MS', 0);

const DELAY_JOBS = envInt('BENCH_DELAY_JOBS', 1200);
const DELAY_CONCURRENCY = envInt('BENCH_DELAY_CONCURRENCY', 30);
const DELAY_TIMEOUT_MS = envInt('BENCH_DELAY_TIMEOUT_MS', 30000);
const DELAY_PROMOTION_INTERVAL_MS = envInt('BENCH_DELAY_PROMOTION_INTERVAL_MS', 100);
const DELAY_BUCKETS = [100, 250, 500, 1000];

function median(values: number[]): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 0) {
    return (sorted[mid - 1] + sorted[mid]) / 2;
  }
  return sorted[mid];
}

function quantiles(values: number[]): Quantiles {
  if (values.length === 0) {
    return { p50: 0, p95: 0, p99: 0, max: 0 };
  }
  const sorted = [...values].sort((a, b) => a - b);
  return {
    p50: percentile(sorted, 50),
    p95: percentile(sorted, 95),
    p99: percentile(sorted, 99),
    max: sorted[sorted.length - 1],
  };
}

async function waitFor(predicate: () => boolean, timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return true;
    await sleep(25);
  }
  return predicate();
}

async function warmup(adapter: Adapter): Promise<void> {
  const queueName = `bench-warmup-${adapter.name}-${Date.now()}`;
  let queue: any;
  let worker: any;
  let completed = 0;

  try {
    queue = adapter.createQueue(queueName);
    worker = adapter.createWorker(queueName, async () => {}, {
      concurrency: 16,
    });

    worker.on('completed', () => {
      completed++;
    });
    worker.on('error', () => {});

    await adapter.waitUntilReady(worker);

    for (let i = 0; i < WARMUP_JOBS; i++) {
      await adapter.add(queue, 'warmup', { i });
    }

    await waitFor(() => completed >= WARMUP_JOBS, WARMUP_TIMEOUT_MS);
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

async function runSteadyRound(adapter: Adapter, round: number): Promise<SteadyRound> {
  const queueName = `bench-steady-${adapter.name}-r${round}-${Date.now()}`;
  const payload = 'x'.repeat(STEADY_PAYLOAD_BYTES);

  let queue: any;
  let worker: any;

  const enqueueTimes = new Map<string, number>();
  const addLatenciesMs: number[] = [];
  const e2eLatenciesMs: number[] = [];

  let produced = 0;
  let completed = 0;

  try {
    queue = adapter.createQueue(queueName);
    worker = adapter.createWorker(
      queueName,
      async () => {
        if (STEADY_PROCESSOR_MS > 0) {
          await sleep(STEADY_PROCESSOR_MS);
        }
      },
      { concurrency: STEADY_CONCURRENCY },
    );

    worker.on('completed', (job: any) => {
      completed++;
      const id = adapter.jobId(job);
      const queuedAt = enqueueTimes.get(id);
      if (queuedAt != null) {
        e2eLatenciesMs.push(Date.now() - queuedAt);
        enqueueTimes.delete(id);
      }
    });
    worker.on('error', () => {});

    await adapter.waitUntilReady(worker);

    const startedAt = performance.now();
    const producerDeadline = startedAt + STEADY_DURATION_MS;

    const producers = Array.from({ length: STEADY_PRODUCERS }, async () => {
      while (performance.now() < producerDeadline) {
        const i = produced++;
        const addStartPerf = performance.now();
        const addStartWall = Date.now();
        const id = await adapter.add(queue, 'steady', { i, payload });
        addLatenciesMs.push(performance.now() - addStartPerf);
        enqueueTimes.set(id, addStartWall);
      }
    });

    await Promise.all(producers);

    const drained = await waitFor(() => completed >= produced, STEADY_DRAIN_TIMEOUT_MS);
    const elapsedMs = performance.now() - startedAt;

    const addQ = quantiles(addLatenciesMs);
    const e2eQ = quantiles(e2eLatenciesMs);
    const backlog = Math.max(0, produced - completed);

    return {
      library: adapter.name,
      round,
      produced,
      completed,
      backlog,
      drained,
      throughput: elapsedMs > 0 ? (completed / elapsedMs) * 1000 : 0,
      addP50: addQ.p50,
      addP95: addQ.p95,
      addP99: addQ.p99,
      e2eP50: e2eQ.p50,
      e2eP95: e2eQ.p95,
      e2eP99: e2eQ.p99,
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

async function runBurstRound(adapter: Adapter, round: number): Promise<BurstRound> {
  const queueName = `bench-burst-${adapter.name}-r${round}-${Date.now()}`;
  const payload = 'x'.repeat(BURST_PAYLOAD_BYTES);

  let queue: any;
  let worker: any;
  let completed = 0;

  try {
    queue = adapter.createQueue(queueName);

    for (let offset = 0; offset < BURST_JOBS; offset += BURST_BULK_SIZE) {
      const size = Math.min(BURST_BULK_SIZE, BURST_JOBS - offset);
      const jobs = Array.from({ length: size }, (_, i) => ({
        name: 'burst',
        data: { i: offset + i, payload },
      }));
      await adapter.addBulk(queue, jobs);
    }

    worker = adapter.createWorker(
      queueName,
      async () => {
        if (BURST_PROCESSOR_MS > 0) {
          await sleep(BURST_PROCESSOR_MS);
        }
      },
      { concurrency: BURST_CONCURRENCY },
    );

    worker.on('completed', () => {
      completed++;
    });
    worker.on('error', () => {});

    const startedAt = performance.now();
    await adapter.waitUntilReady(worker);
    const drained = await waitFor(() => completed >= BURST_JOBS, BURST_DRAIN_TIMEOUT_MS);
    const drainMs = performance.now() - startedAt;

    return {
      library: adapter.name,
      round,
      completed,
      drained,
      drainMs,
      throughput: drainMs > 0 ? (completed / drainMs) * 1000 : 0,
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

async function runRetryRound(adapter: Adapter, round: number): Promise<RetryRound> {
  const queueName = `bench-retry-${adapter.name}-r${round}-${Date.now()}`;

  let queue: any;
  let worker: any;

  const enqueueTimes = new Map<string, number>();
  const e2eLatenciesMs: number[] = [];

  let completed = 0;
  let failedEvents = 0;
  let retryExecutions = 0;

  try {
    queue = adapter.createQueue(queueName);
    worker = adapter.createWorker(
      queueName,
      async (job: any) => {
        const attempts = adapter.attemptsMade(job);
        if (attempts > 0) {
          retryExecutions++;
        }
        if (RETRY_PROCESSOR_MS > 0) {
          await sleep(RETRY_PROCESSOR_MS);
        }
        if (job.data?.failFirst === true && attempts === 0) {
          throw new Error('synthetic transient failure');
        }
      },
      { concurrency: RETRY_CONCURRENCY, promotionInterval: DELAY_PROMOTION_INTERVAL_MS },
    );

    worker.on('completed', (job: any) => {
      completed++;
      const id = adapter.jobId(job);
      const queuedAt = enqueueTimes.get(id);
      if (queuedAt != null) {
        e2eLatenciesMs.push(Date.now() - queuedAt);
        enqueueTimes.delete(id);
      }
    });

    worker.on('failed', () => {
      failedEvents++;
    });
    worker.on('error', () => {});

    await adapter.waitUntilReady(worker);
    const startedAt = performance.now();

    for (let i = 0; i < RETRY_JOBS; i++) {
      const failFirst = i % RETRY_FAIL_EVERY === 0;
      const id = await adapter.add(
        queue,
        'retry',
        { i, failFirst },
        {
          attempts: 2,
          backoff: {
            type: 'fixed',
            delay: RETRY_BACKOFF_MS,
          },
        },
      );
      enqueueTimes.set(id, Date.now());
    }

    await waitFor(() => completed >= RETRY_JOBS, RETRY_TIMEOUT_MS);
    const elapsedMs = performance.now() - startedAt;
    const e2eQ = quantiles(e2eLatenciesMs);

    return {
      library: adapter.name,
      round,
      completed,
      successRate: (completed / RETRY_JOBS) * 100,
      elapsedMs,
      throughput: elapsedMs > 0 ? (completed / elapsedMs) * 1000 : 0,
      failedEvents,
      retryExecutions,
      e2eP50: e2eQ.p50,
      e2eP95: e2eQ.p95,
      e2eP99: e2eQ.p99,
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

async function runDelayRound(adapter: Adapter, round: number): Promise<DelayRound> {
  const queueName = `bench-delay-${adapter.name}-r${round}-${Date.now()}`;

  let queue: any;
  let worker: any;

  const dueAtById = new Map<string, number>();
  const seenActive = new Set<string>();
  const lateStartsMs: number[] = [];

  let completed = 0;
  let earlyStarts = 0;

  try {
    queue = adapter.createQueue(queueName);
    worker = adapter.createWorker(queueName, async () => {}, {
      concurrency: DELAY_CONCURRENCY,
      promotionInterval: DELAY_PROMOTION_INTERVAL_MS,
    });

    worker.on('active', (job: any) => {
      const id = adapter.jobId(job);
      if (seenActive.has(id)) return;
      seenActive.add(id);
      const dueAt = dueAtById.get(id);
      if (dueAt == null) return;
      const delta = Date.now() - dueAt;
      if (delta < 0) {
        earlyStarts++;
      } else {
        lateStartsMs.push(delta);
      }
    });

    worker.on('completed', () => {
      completed++;
    });
    worker.on('error', () => {});

    await adapter.waitUntilReady(worker);

    for (let i = 0; i < DELAY_JOBS; i++) {
      const delay = DELAY_BUCKETS[i % DELAY_BUCKETS.length];
      const now = Date.now();
      const id = await adapter.add(queue, 'delay', { i, delay }, { delay });
      dueAtById.set(id, now + delay);
    }

    await waitFor(() => completed >= DELAY_JOBS, DELAY_TIMEOUT_MS);

    const lateQ = quantiles(lateStartsMs);

    return {
      library: adapter.name,
      round,
      completed,
      activeSeen: seenActive.size,
      earlyStarts,
      lateP50: lateQ.p50,
      lateP95: lateQ.p95,
      lateP99: lateQ.p99,
      lateMax: lateQ.max,
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

function flattenRounds<T extends { library: LibraryName; round: number }>(rows: Record<LibraryName, T[]>): T[] {
  return [...rows['glide-mq'], ...rows['bullmq']].sort((a, b) => {
    if (a.round !== b.round) return a.round - b.round;
    return a.library.localeCompare(b.library);
  });
}

function runSummary<T extends { library: LibraryName }>(rows: Record<LibraryName, T[]>): Record<LibraryName, T> {
  const out = {} as Record<LibraryName, T>;
  for (const library of ['glide-mq', 'bullmq'] as LibraryName[]) {
    const values = rows[library];
    if (values.length === 0) {
      throw new Error(`No rows for ${library}`);
    }
    out[library] = values[0];
  }
  return out;
}

function steadyBaseline(rows: Record<LibraryName, SteadyRound[]>): Record<LibraryName, SteadyRound> {
  const summary = runSummary(rows);
  for (const library of ['glide-mq', 'bullmq'] as LibraryName[]) {
    const r = rows[library];
    summary[library] = {
      library,
      round: 0,
      produced: Math.round(median(r.map((v) => v.produced))),
      completed: Math.round(median(r.map((v) => v.completed))),
      backlog: Math.round(median(r.map((v) => v.backlog))),
      drained: r.every((v) => v.drained),
      throughput: median(r.map((v) => v.throughput)),
      addP50: median(r.map((v) => v.addP50)),
      addP95: median(r.map((v) => v.addP95)),
      addP99: median(r.map((v) => v.addP99)),
      e2eP50: median(r.map((v) => v.e2eP50)),
      e2eP95: median(r.map((v) => v.e2eP95)),
      e2eP99: median(r.map((v) => v.e2eP99)),
    };
  }
  return summary;
}

function burstBaseline(rows: Record<LibraryName, BurstRound[]>): Record<LibraryName, BurstRound> {
  const summary = runSummary(rows);
  for (const library of ['glide-mq', 'bullmq'] as LibraryName[]) {
    const r = rows[library];
    summary[library] = {
      library,
      round: 0,
      completed: Math.round(median(r.map((v) => v.completed))),
      drained: r.every((v) => v.drained),
      drainMs: median(r.map((v) => v.drainMs)),
      throughput: median(r.map((v) => v.throughput)),
    };
  }
  return summary;
}

function retryBaseline(rows: Record<LibraryName, RetryRound[]>): Record<LibraryName, RetryRound> {
  const summary = runSummary(rows);
  for (const library of ['glide-mq', 'bullmq'] as LibraryName[]) {
    const r = rows[library];
    summary[library] = {
      library,
      round: 0,
      completed: Math.round(median(r.map((v) => v.completed))),
      successRate: median(r.map((v) => v.successRate)),
      elapsedMs: median(r.map((v) => v.elapsedMs)),
      throughput: median(r.map((v) => v.throughput)),
      failedEvents: Math.round(median(r.map((v) => v.failedEvents))),
      retryExecutions: Math.round(median(r.map((v) => v.retryExecutions))),
      e2eP50: median(r.map((v) => v.e2eP50)),
      e2eP95: median(r.map((v) => v.e2eP95)),
      e2eP99: median(r.map((v) => v.e2eP99)),
    };
  }
  return summary;
}

function delayBaseline(rows: Record<LibraryName, DelayRound[]>): Record<LibraryName, DelayRound> {
  const summary = runSummary(rows);
  for (const library of ['glide-mq', 'bullmq'] as LibraryName[]) {
    const r = rows[library];
    summary[library] = {
      library,
      round: 0,
      completed: Math.round(median(r.map((v) => v.completed))),
      activeSeen: Math.round(median(r.map((v) => v.activeSeen))),
      earlyStarts: Math.round(median(r.map((v) => v.earlyStarts))),
      lateP50: median(r.map((v) => v.lateP50)),
      lateP95: median(r.map((v) => v.lateP95)),
      lateP99: median(r.map((v) => v.lateP99)),
      lateMax: median(r.map((v) => v.lateMax)),
    };
  }
  return summary;
}

async function runScenarioRounds<T extends { library: LibraryName; round: number }>(
  label: string,
  runner: (adapter: Adapter, round: number) => Promise<T>,
): Promise<Record<LibraryName, T[]>> {
  const rows: Record<LibraryName, T[]> = {
    'glide-mq': [],
    bullmq: [],
  };

  for (let round = 1; round <= ROUNDS; round++) {
    const order = round % 2 === 1 ? ADAPTERS : [...ADAPTERS].reverse();
    for (const adapter of order) {
      await flushDB();
      console.log(`  [${label}] round ${round}/${ROUNDS} - ${adapter.name}`);
      rows[adapter.name].push(await runner(adapter, round));
    }
  }

  return rows;
}

function writeBaselineFile(payload: unknown): string {
  const outDir = path.join(__dirname, 'results');
  mkdirSync(outDir, { recursive: true });
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const outPath = path.join(outDir, `user-centric-${stamp}.json`);
  writeFileSync(outPath, JSON.stringify(payload, null, 2));
  return outPath;
}

export async function runUserCentric(): Promise<void> {
  console.log('\n## User-Centric MQ Benchmark\n');
  console.log('Method: warmup + alternating order + median baseline across rounds.');
  console.log(
    `Config: rounds=${ROUNDS}, steady=${STEADY_DURATION_MS}ms x ${STEADY_PRODUCERS} producers, burst=${fmt(BURST_JOBS)}, retry=${fmt(RETRY_JOBS)}, delay=${fmt(DELAY_JOBS)}.`,
  );

  console.log('\n### Warmup\n');
  for (const adapter of ADAPTERS) {
    await flushDB();
    console.log(`  warming ${adapter.name}...`);
    await warmup(adapter);
  }

  console.log('\n### Steady State (throughput + tail latency)\n');
  const steady = await runScenarioRounds('steady', runSteadyRound);
  const steadyBase = steadyBaseline(steady);
  printTable(
    ['Library', 'Round', 'Produced', 'Completed', 'Throughput', 'Add p95', 'E2E p95', 'Backlog', 'Drained'],
    flattenRounds(steady).map((r) => [
      r.library,
      r.round.toString(),
      fmt(r.produced),
      fmt(r.completed),
      fmt(r.throughput) + ' j/s',
      fmtMs(r.addP95),
      fmtMs(r.e2eP95),
      fmt(r.backlog),
      r.drained ? 'yes' : 'no',
    ]),
  );

  console.log('\nSteady baseline (median across rounds):\n');
  printTable(
    ['Library', 'Throughput', 'Add p95', 'Add p99', 'E2E p95', 'E2E p99', 'Backlog'],
    (['glide-mq', 'bullmq'] as LibraryName[]).map((lib) => [
      lib,
      fmt(steadyBase[lib].throughput) + ' j/s',
      fmtMs(steadyBase[lib].addP95),
      fmtMs(steadyBase[lib].addP99),
      fmtMs(steadyBase[lib].e2eP95),
      fmtMs(steadyBase[lib].e2eP99),
      fmt(steadyBase[lib].backlog),
    ]),
  );

  console.log('\n### Burst Drain (backlog clearance speed)\n');
  const burst = await runScenarioRounds('burst', runBurstRound);
  const burstBase = burstBaseline(burst);
  printTable(
    ['Library', 'Round', 'Completed', 'Drain Time', 'Throughput', 'Drained'],
    flattenRounds(burst).map((r) => [
      r.library,
      r.round.toString(),
      fmt(r.completed),
      fmtMs(r.drainMs),
      fmt(r.throughput) + ' j/s',
      r.drained ? 'yes' : 'no',
    ]),
  );

  console.log('\nBurst baseline (median across rounds):\n');
  printTable(
    ['Library', 'Drain Time', 'Throughput', 'Completed'],
    (['glide-mq', 'bullmq'] as LibraryName[]).map((lib) => [
      lib,
      fmtMs(burstBase[lib].drainMs),
      fmt(burstBase[lib].throughput) + ' j/s',
      fmt(burstBase[lib].completed),
    ]),
  );

  console.log('\n### Retry Path (transient failures)\n');
  const retry = await runScenarioRounds('retry', runRetryRound);
  const retryBase = retryBaseline(retry);
  printTable(
    ['Library', 'Round', 'Completed', 'Success', 'Throughput', 'E2E p95', 'Retry Exec', 'Failed Events'],
    flattenRounds(retry).map((r) => [
      r.library,
      r.round.toString(),
      fmt(r.completed),
      r.successRate.toFixed(2) + '%',
      fmt(r.throughput) + ' j/s',
      fmtMs(r.e2eP95),
      fmt(r.retryExecutions),
      fmt(r.failedEvents),
    ]),
  );

  console.log('\nRetry baseline (median across rounds):\n');
  printTable(
    ['Library', 'Success', 'Throughput', 'E2E p95', 'E2E p99', 'Retry Exec'],
    (['glide-mq', 'bullmq'] as LibraryName[]).map((lib) => [
      lib,
      retryBase[lib].successRate.toFixed(2) + '%',
      fmt(retryBase[lib].throughput) + ' j/s',
      fmtMs(retryBase[lib].e2eP95),
      fmtMs(retryBase[lib].e2eP99),
      fmt(retryBase[lib].retryExecutions),
    ]),
  );

  console.log('\n### Delayed Job Precision (start-time accuracy)\n');
  const delay = await runScenarioRounds('delay', runDelayRound);
  const delayBase = delayBaseline(delay);
  printTable(
    ['Library', 'Round', 'Completed', 'Active Seen', 'Early Starts', 'Late p95', 'Late p99', 'Late Max'],
    flattenRounds(delay).map((r) => [
      r.library,
      r.round.toString(),
      fmt(r.completed),
      fmt(r.activeSeen),
      fmt(r.earlyStarts),
      fmtMs(r.lateP95),
      fmtMs(r.lateP99),
      fmtMs(r.lateMax),
    ]),
  );

  console.log('\nDelay baseline (median across rounds):\n');
  printTable(
    ['Library', 'Late p50', 'Late p95', 'Late p99', 'Late Max', 'Early Starts'],
    (['glide-mq', 'bullmq'] as LibraryName[]).map((lib) => [
      lib,
      fmtMs(delayBase[lib].lateP50),
      fmtMs(delayBase[lib].lateP95),
      fmtMs(delayBase[lib].lateP99),
      fmtMs(delayBase[lib].lateMax),
      fmt(delayBase[lib].earlyStarts),
    ]),
  );

  const baseline = {
    benchmark: 'user-centric',
    createdAt: new Date().toISOString(),
    env: {
      node: process.version,
      platform: `${process.platform} ${process.arch}`,
      benchHost: process.env.BENCH_HOST || '127.0.0.1',
      benchPort: Number(process.env.BENCH_PORT || 6379),
    },
    config: {
      rounds: ROUNDS,
      steady: {
        durationMs: STEADY_DURATION_MS,
        producers: STEADY_PRODUCERS,
        concurrency: STEADY_CONCURRENCY,
        payloadBytes: STEADY_PAYLOAD_BYTES,
        processorMs: STEADY_PROCESSOR_MS,
      },
      burst: {
        jobs: BURST_JOBS,
        bulkSize: BURST_BULK_SIZE,
        concurrency: BURST_CONCURRENCY,
        payloadBytes: BURST_PAYLOAD_BYTES,
        processorMs: BURST_PROCESSOR_MS,
      },
      retry: {
        jobs: RETRY_JOBS,
        concurrency: RETRY_CONCURRENCY,
        backoffMs: RETRY_BACKOFF_MS,
        failEvery: RETRY_FAIL_EVERY,
        processorMs: RETRY_PROCESSOR_MS,
      },
      delay: {
        jobs: DELAY_JOBS,
        concurrency: DELAY_CONCURRENCY,
        bucketsMs: DELAY_BUCKETS,
        promotionIntervalMs: DELAY_PROMOTION_INTERVAL_MS,
      },
    },
    steady: {
      rounds: steady,
      baseline: steadyBase,
    },
    burst: {
      rounds: burst,
      baseline: burstBase,
    },
    retry: {
      rounds: retry,
      baseline: retryBase,
    },
    delay: {
      rounds: delay,
      baseline: delayBase,
    },
  };

  const baselinePath = writeBaselineFile(baseline);
  console.log(`\nBaseline saved: ${baselinePath}`);
}
