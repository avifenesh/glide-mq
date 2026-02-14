/**
 * Throughput benchmark: measure jobs/sec for add and process operations.
 * Uses time-based measurement: run for a fixed duration and count ops.
 * Compares glide-mq vs BullMQ.
 */

import { Queue, Worker } from '../dist';
import { Queue as BullQueue, Worker as BullWorker } from 'bullmq';
import { flushDB, fmt, printTable, GLIDE_CONNECTION, BULL_CONNECTION } from './utils';

const ADD_DURATION_MS = 5000;
const PROCESS_DURATION_MS = 8000;
const PROCESS_SIZES = [500, 2000];

interface AddResult {
  library: string;
  count: number;
  elapsed: number;
  rate: number;
}

interface ProcessResult {
  library: string;
  target: number;
  completed: number;
  concurrency: number;
  elapsed: number;
  rate: number;
}

// ---- Add throughput (time-based) ----

async function glideAddThroughput(): Promise<AddResult> {
  const queue = new Queue('bench-add', { connection: GLIDE_CONNECTION });
  let count = 0;
  const deadline = performance.now() + ADD_DURATION_MS;

  while (performance.now() < deadline) {
    await queue.add('job', { i: count });
    count++;
  }

  const elapsed = ADD_DURATION_MS;
  await queue.close();
  return { library: 'glide-mq', count, elapsed, rate: (count / elapsed) * 1000 };
}

async function bullAddThroughput(): Promise<AddResult> {
  const queue = new BullQueue('bench-add-bull', { connection: BULL_CONNECTION });
  let count = 0;
  const deadline = performance.now() + ADD_DURATION_MS;

  while (performance.now() < deadline) {
    await queue.add('job', { i: count });
    count++;
  }

  const elapsed = ADD_DURATION_MS;
  await queue.close();
  return { library: 'bullmq', count, elapsed, rate: (count / elapsed) * 1000 };
}

// ---- Process throughput (count-based with timeout) ----

async function glideProcessThroughput(n: number, concurrency: number): Promise<ProcessResult> {
  const queue = new Queue('bench-proc', { connection: GLIDE_CONNECTION });

  for (let i = 0; i < n; i++) {
    await queue.add('job', { i });
  }

  let completed = 0;
  const start = performance.now();

  const result = await Promise.race([
    new Promise<ProcessResult>((resolve) => {
      const worker = new Worker(
        'bench-proc',
        async () => {},
        { connection: GLIDE_CONNECTION, concurrency },
      );

      worker.on('completed', () => {
        completed++;
        if (completed >= n) {
          const elapsed = performance.now() - start;
          worker.close(true).then(() => queue.close()).then(() =>
            resolve({
              library: 'glide-mq',
              target: n,
              completed: n,
              concurrency,
              elapsed,
              rate: (n / elapsed) * 1000,
            }),
          );
        }
      });

      worker.on('error', () => {});
    }),
    // Timeout: report partial results
    new Promise<ProcessResult>((resolve) =>
      setTimeout(async () => {
        const elapsed = performance.now() - start;
        resolve({
          library: 'glide-mq',
          target: n,
          completed,
          concurrency,
          elapsed,
          rate: (completed / elapsed) * 1000,
        });
      }, PROCESS_DURATION_MS),
    ),
  ]);

  // Force cleanup in case of timeout
  try { await queue.close(); } catch {}
  return result;
}

async function bullProcessThroughput(n: number, concurrency: number): Promise<ProcessResult> {
  const queue = new BullQueue('bench-proc-bull', { connection: BULL_CONNECTION });

  for (let i = 0; i < n; i++) {
    await queue.add('job', { i });
  }

  let completed = 0;
  const start = performance.now();

  const result = await Promise.race([
    new Promise<ProcessResult>((resolve) => {
      const worker = new BullWorker(
        'bench-proc-bull',
        async () => {},
        { connection: BULL_CONNECTION, concurrency },
      );

      worker.on('completed', () => {
        completed++;
        if (completed >= n) {
          const elapsed = performance.now() - start;
          worker.close(true).then(() => queue.close()).then(() =>
            resolve({
              library: 'bullmq',
              target: n,
              completed: n,
              concurrency,
              elapsed,
              rate: (n / elapsed) * 1000,
            }),
          );
        }
      });

      worker.on('error', () => {});
    }),
    new Promise<ProcessResult>((resolve) =>
      setTimeout(async () => {
        const elapsed = performance.now() - start;
        resolve({
          library: 'bullmq',
          target: n,
          completed,
          concurrency,
          elapsed,
          rate: (completed / elapsed) * 1000,
        });
      }, PROCESS_DURATION_MS),
    ),
  ]);

  try { await queue.close(); } catch {}
  return result;
}

// ---- Run ----

export async function runThroughput(): Promise<void> {
  console.log('\n## Add Throughput (time-based, 5s)\n');

  await flushDB();
  console.log('  [glide-mq] Adding jobs for 5s...');
  const glideAdd = await glideAddThroughput();

  await flushDB();
  console.log('  [bullmq]   Adding jobs for 5s...');
  const bullAdd = await bullAddThroughput();

  console.log('');
  printTable(
    ['Library', 'Jobs Added', 'Rate (jobs/s)'],
    [
      [glideAdd.library, fmt(glideAdd.count), fmt(glideAdd.rate)],
      [bullAdd.library, fmt(bullAdd.count), fmt(bullAdd.rate)],
    ],
  );

  console.log('\n## Process Throughput\n');

  const processResults: ProcessResult[] = [];

  for (const n of PROCESS_SIZES) {
    for (const concurrency of [1, 10]) {
      await flushDB();
      console.log(`  [glide-mq] Processing ${n} jobs (c=${concurrency})...`);
      processResults.push(await glideProcessThroughput(n, concurrency));

      await flushDB();
      console.log(`  [bullmq]   Processing ${n} jobs (c=${concurrency})...`);
      processResults.push(await bullProcessThroughput(n, concurrency));
    }
  }

  console.log('');
  printTable(
    ['Library', 'Target', 'Concurrency', 'Completed', 'Rate (jobs/s)'],
    processResults.map((r) => [
      r.library,
      fmt(r.target),
      r.concurrency.toString(),
      fmt(r.completed),
      fmt(r.rate),
    ]),
  );
}
