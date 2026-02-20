/**
 * Latency benchmark: measure per-job round-trip latency.
 * Two modes:
 *   1. Serial: add one job, wait for completion, measure round-trip. Repeat N times.
 *      This measures pure queue overhead without backlog effects.
 *   2. Pipelined: add all N jobs, measure add-to-process-start and add-to-complete.
 *      This measures behavior under load.
 *
 * Compares glide-mq vs BullMQ.
 */

import { Queue, Worker } from '../dist';
import { Queue as BullQueue, Worker as BullWorker } from 'bullmq';
import { flushDB, fmtMs, percentile, printTable, GLIDE_CONNECTION, BULL_CONNECTION } from './utils';

const N_SERIAL = 200;
const N_PIPELINED = 1000;

// ---- Serial round-trip latency ----

async function glideSerialLatency(): Promise<number[]> {
  const queue = new Queue('bench-lat-serial', { connection: GLIDE_CONNECTION });
  const latencies: number[] = [];

  const worker = new Worker('bench-lat-serial', async () => {}, {
    connection: GLIDE_CONNECTION,
    concurrency: 1,
  });
  worker.on('error', () => {});
  await worker.waitUntilReady();

  for (let i = 0; i < N_SERIAL; i++) {
    const start = performance.now();
    const job = await queue.add('job', { i });

    await new Promise<void>((resolve) => {
      const onComplete = (completedJob: any) => {
        if (completedJob.id === job!.id) {
          worker.removeListener('completed', onComplete);
          resolve();
        }
      };
      worker.on('completed', onComplete);
    });

    latencies.push(performance.now() - start);
  }

  await worker.close(true);
  await queue.close();
  return latencies;
}

async function bullSerialLatency(): Promise<number[]> {
  const queue = new BullQueue('bench-lat-serial-bull', { connection: BULL_CONNECTION });
  const latencies: number[] = [];

  const worker = new BullWorker('bench-lat-serial-bull', async () => {}, {
    connection: BULL_CONNECTION,
    concurrency: 1,
  });
  worker.on('error', () => {});
  await worker.waitUntilReady();

  for (let i = 0; i < N_SERIAL; i++) {
    const start = performance.now();
    const job = await queue.add('job', { i });

    await new Promise<void>((resolve) => {
      const onComplete = (completedJob: any) => {
        if (completedJob.id === job.id) {
          worker.removeListener('completed', onComplete);
          resolve();
        }
      };
      worker.on('completed', onComplete);
    });

    latencies.push(performance.now() - start);
  }

  await worker.close(true);
  await queue.close();
  return latencies;
}

// ---- Pipelined (flood) latency ----

interface PipelinedEntry {
  addToStart: number;
  addToComplete: number;
}

async function glidePipelinedLatency(): Promise<PipelinedEntry[]> {
  const queue = new Queue('bench-lat-pipe', { connection: GLIDE_CONNECTION });
  const entries: PipelinedEntry[] = [];
  const addTimes = new Map<string, number>();
  const startTimes = new Map<string, number>();
  let completed = 0;

  return new Promise<PipelinedEntry[]>((resolve) => {
    const worker = new Worker(
      'bench-lat-pipe',
      async (job) => {
        startTimes.set(job.id, performance.now());
      },
      { connection: GLIDE_CONNECTION, concurrency: 10 },
    );

    worker.on('completed', (job) => {
      const now = performance.now();
      const addTime = addTimes.get(job.id);
      const startTime = startTimes.get(job.id);
      if (addTime !== undefined && startTime !== undefined) {
        entries.push({
          addToStart: startTime - addTime,
          addToComplete: now - addTime,
        });
      }
      completed++;
      if (completed >= N_PIPELINED) {
        worker
          .close(true)
          .then(() => queue.close())
          .then(() => resolve(entries));
      }
    });

    worker.on('error', () => {});

    (async () => {
      await worker.waitUntilReady();
      for (let i = 0; i < N_PIPELINED; i++) {
        const t = performance.now();
        const job = await queue.add('job', { i });
        if (job) addTimes.set(job.id, t);
      }
    })();
  });
}

async function bullPipelinedLatency(): Promise<PipelinedEntry[]> {
  const queue = new BullQueue('bench-lat-pipe-bull', { connection: BULL_CONNECTION });
  const entries: PipelinedEntry[] = [];
  const addTimes = new Map<string, number>();
  const startTimes = new Map<string, number>();
  let completed = 0;

  return new Promise<PipelinedEntry[]>((resolve) => {
    const worker = new BullWorker(
      'bench-lat-pipe-bull',
      async (job) => {
        startTimes.set(job.id!, performance.now());
      },
      { connection: BULL_CONNECTION, concurrency: 10 },
    );

    worker.on('completed', (job) => {
      const now = performance.now();
      const id = job.id!;
      const addTime = addTimes.get(id);
      const startTime = startTimes.get(id);
      if (addTime !== undefined && startTime !== undefined) {
        entries.push({
          addToStart: startTime - addTime,
          addToComplete: now - addTime,
        });
      }
      completed++;
      if (completed >= N_PIPELINED) {
        worker
          .close(true)
          .then(() => queue.close())
          .then(() => resolve(entries));
      }
    });

    worker.on('error', () => {});

    (async () => {
      await worker.waitUntilReady();
      for (let i = 0; i < N_PIPELINED; i++) {
        const t = performance.now();
        const job = await queue.add('job', { i });
        addTimes.set(job.id!, t);
      }
    })();
  });
}

// ---- Run ----

function summarizeSerial(label: string, latencies: number[]): string[][] {
  const sorted = [...latencies].sort((a, b) => a - b);
  return [
    [
      label,
      'round-trip',
      fmtMs(percentile(sorted, 50)),
      fmtMs(percentile(sorted, 90)),
      fmtMs(percentile(sorted, 99)),
    ],
  ];
}

function summarizePipelined(label: string, entries: PipelinedEntry[]): string[][] {
  const addToStartSorted = entries.map((e) => e.addToStart).sort((a, b) => a - b);
  const addToCompleteSorted = entries.map((e) => e.addToComplete).sort((a, b) => a - b);
  return [
    [
      label,
      'add -> start',
      fmtMs(percentile(addToStartSorted, 50)),
      fmtMs(percentile(addToStartSorted, 90)),
      fmtMs(percentile(addToStartSorted, 99)),
    ],
    [
      label,
      'add -> complete',
      fmtMs(percentile(addToCompleteSorted, 50)),
      fmtMs(percentile(addToCompleteSorted, 90)),
      fmtMs(percentile(addToCompleteSorted, 99)),
    ],
  ];
}

export async function runLatency(): Promise<void> {
  // Serial latency
  console.log(`\n## Serial Latency (${N_SERIAL} jobs, concurrency=1)\n`);

  await flushDB();
  console.log('  [glide-mq] Measuring serial latency...');
  const glideSerial = await glideSerialLatency();

  await flushDB();
  console.log('  [bullmq]   Measuring serial latency...');
  const bullSerial = await bullSerialLatency();

  console.log('');
  printTable(
    ['Library', 'Metric', 'p50', 'p90', 'p99'],
    [...summarizeSerial('glide-mq', glideSerial), ...summarizeSerial('bullmq', bullSerial)],
  );

  // Pipelined latency
  console.log(`\n## Pipelined Latency (${N_PIPELINED} jobs, concurrency=10)\n`);

  await flushDB();
  console.log('  [glide-mq] Measuring pipelined latency...');
  const glidePipelined = await glidePipelinedLatency();

  await flushDB();
  console.log('  [bullmq]   Measuring pipelined latency...');
  const bullPipelined = await bullPipelinedLatency();

  console.log('');
  printTable(
    ['Library', 'Metric', 'p50', 'p90', 'p99'],
    [
      ...summarizePipelined('glide-mq', glidePipelined),
      ...summarizePipelined('bullmq', bullPipelined),
    ],
  );
}
