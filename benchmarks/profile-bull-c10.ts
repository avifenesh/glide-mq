/**
 * Profile BullMQ c=10: measure per-EVAL latency for comparison.
 */

import { Queue as BullQueue, Worker as BullWorker } from 'bullmq';
import { flushDB } from './utils';

const BENCH_HOST = process.env.BENCH_HOST || '127.0.0.1';
const BENCH_PORT = Number(process.env.BENCH_PORT) || 6379;
const BULL_CONNECTION = { host: BENCH_HOST, port: BENCH_PORT };

const N = 10000;
const CONCURRENCY = 10;

async function run() {
  await flushDB();

  const queue = new BullQueue('bench-profile-bull', { connection: BULL_CONNECTION });

  console.log(`Adding ${N} jobs...`);
  for (let i = 0; i < N; i++) {
    await queue.add('job', { i });
  }
  console.log('Jobs added. Processing...');

  let completed = 0;
  const start = performance.now();

  await new Promise<void>((resolve) => {
    const worker = new BullWorker(
      'bench-profile-bull',
      async () => {},
      { connection: BULL_CONNECTION, concurrency: CONCURRENCY },
    );

    worker.on('completed', () => {
      completed++;
      if (completed >= N) {
        const elapsed = performance.now() - start;
        worker.close(true).then(() => {
          console.log(`\nProcessed ${N} jobs in ${elapsed.toFixed(0)}ms`);
          console.log(`Throughput: ${((N / elapsed) * 1000).toFixed(0)} j/s`);
          console.log(`Implied per-call latency: ${(CONCURRENCY / ((N / elapsed) * 1000) * 1000).toFixed(3)}ms`);
          queue.close().then(resolve);
        });
      }
    });

    worker.on('error', () => {});
  });
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
