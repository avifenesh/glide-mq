/**
 * Memory benchmark: measure RSS and heap usage before/after processing jobs.
 * Compares glide-mq vs BullMQ.
 */

import { Queue, Worker } from '../dist';
import { Queue as BullQueue, Worker as BullWorker } from 'bullmq';
import { flushDB, fmt, printTable, GLIDE_CONNECTION, BULL_CONNECTION } from './utils';

const N = 10000;

interface MemSnapshot {
  rss: number;
  heapUsed: number;
  heapTotal: number;
}

function snap(): MemSnapshot {
  const m = process.memoryUsage();
  return { rss: m.rss, heapUsed: m.heapUsed, heapTotal: m.heapTotal };
}

function mb(bytes: number): string {
  return (bytes / 1024 / 1024).toFixed(2) + ' MB';
}

// ---- glide-mq ----

async function glideMemory(): Promise<{ before: MemSnapshot; after: MemSnapshot }> {
  // Force GC if available
  if (global.gc) global.gc();
  const before = snap();

  const queue = new Queue('bench-mem', { connection: GLIDE_CONNECTION });

  for (let i = 0; i < N; i++) {
    await queue.add('job', { i, payload: 'x'.repeat(100) });
  }

  let completed = 0;

  await new Promise<void>((resolve) => {
    const worker = new Worker('bench-mem', async () => {}, {
      connection: GLIDE_CONNECTION,
      concurrency: 10,
    });

    worker.on('completed', () => {
      completed++;
      if (completed >= N) {
        worker
          .close(true)
          .then(() => queue.close())
          .then(() => resolve());
      }
    });

    worker.on('error', () => {});
  });

  if (global.gc) global.gc();
  const after = snap();

  return { before, after };
}

// ---- BullMQ ----

async function bullMemory(): Promise<{ before: MemSnapshot; after: MemSnapshot }> {
  if (global.gc) global.gc();
  const before = snap();

  const queue = new BullQueue('bench-mem-bull', { connection: BULL_CONNECTION });

  for (let i = 0; i < N; i++) {
    await queue.add('job', { i, payload: 'x'.repeat(100) });
  }

  let completed = 0;

  await new Promise<void>((resolve) => {
    const worker = new BullWorker('bench-mem-bull', async () => {}, {
      connection: BULL_CONNECTION,
      concurrency: 10,
    });

    worker.on('completed', () => {
      completed++;
      if (completed >= N) {
        worker
          .close(true)
          .then(() => queue.close())
          .then(() => resolve());
      }
    });

    worker.on('error', () => {});
  });

  if (global.gc) global.gc();
  const after = snap();

  return { before, after };
}

// ---- Run ----

export async function runMemory(): Promise<void> {
  console.log(`\n## Memory Benchmark (${fmt(N)} jobs, concurrency=10)\n`);

  await flushDB();
  console.log('  [glide-mq] Measuring memory...');
  const glide = await glideMemory();

  await flushDB();
  console.log('  [bullmq]   Measuring memory...');
  const bull = await bullMemory();

  const rows = [
    [
      'glide-mq',
      mb(glide.before.rss),
      mb(glide.after.rss),
      mb(glide.after.rss - glide.before.rss),
      mb(glide.before.heapUsed),
      mb(glide.after.heapUsed),
      mb(glide.after.heapUsed - glide.before.heapUsed),
    ],
    [
      'bullmq',
      mb(bull.before.rss),
      mb(bull.after.rss),
      mb(bull.after.rss - bull.before.rss),
      mb(bull.before.heapUsed),
      mb(bull.after.heapUsed),
      mb(bull.after.heapUsed - bull.before.heapUsed),
    ],
  ];

  console.log('');
  printTable(
    ['Library', 'RSS Before', 'RSS After', 'RSS Delta', 'Heap Before', 'Heap After', 'Heap Delta'],
    rows,
  );
}
