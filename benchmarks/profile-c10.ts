/**
 * Profile c=10 hot path: measure where time is spent per job.
 * Instruments processJob phases via monkey-patching.
 */

import { Queue, Worker } from '../dist';
import { flushDB, GLIDE_CONNECTION } from './utils';

const N = 10000;
const CONCURRENCY = 10;

async function run() {
  await flushDB();

  const queue = new Queue('bench-profile', { connection: GLIDE_CONNECTION });

  // Pre-add all jobs
  console.log(`Adding ${N} jobs...`);
  for (let i = 0; i < N; i++) {
    await queue.add('job', { i });
  }
  console.log('Jobs added. Processing...');

  let completed = 0;
  const start = performance.now();

  // Track per-FCALL latency by wrapping the client.fcall
  const fcallLatencies: number[] = [];
  let fcallCount = 0;

  await new Promise<void>((resolve) => {
    const worker = new Worker(
      'bench-profile',
      async () => {},
      { connection: GLIDE_CONNECTION, concurrency: CONCURRENCY, events: false, metrics: false },
    );

    // Patch commandClient.fcall to measure latency
    let patched = false;

    worker.on('completed', () => {
      // Patch once the client is available
      if (!patched) {
        const client = (worker as any).commandClient;
        if (client) {
          const origFcall = client.fcall.bind(client);
          client.fcall = async (...args: any[]) => {
            const t0 = performance.now();
            const result = await origFcall(...args);
            const elapsed = performance.now() - t0;
            fcallLatencies.push(elapsed);
            fcallCount++;
            return result;
          };
          patched = true;
        }
      }

      completed++;
      if (completed >= N) {
        const elapsed = performance.now() - start;
        worker.close(true).then(() => {
          console.log(`\nProcessed ${N} jobs in ${elapsed.toFixed(0)}ms`);
          console.log(`Throughput: ${((N / elapsed) * 1000).toFixed(0)} j/s`);
          console.log(`\nFCALL stats (${fcallCount} calls):`);

          if (fcallLatencies.length > 0) {
            const sorted = [...fcallLatencies].sort((a, b) => a - b);
            const p50 = sorted[Math.floor(sorted.length * 0.5)];
            const p90 = sorted[Math.floor(sorted.length * 0.9)];
            const p99 = sorted[Math.floor(sorted.length * 0.99)];
            const avg = sorted.reduce((a, b) => a + b, 0) / sorted.length;
            const totalFcallMs = sorted.reduce((a, b) => a + b, 0);
            console.log(`  avg: ${avg.toFixed(3)}ms`);
            console.log(`  p50: ${p50.toFixed(3)}ms`);
            console.log(`  p90: ${p90.toFixed(3)}ms`);
            console.log(`  p99: ${p99.toFixed(3)}ms`);
            console.log(`  total FCALL time: ${totalFcallMs.toFixed(0)}ms`);
            console.log(`  FCALL time / wall time: ${((totalFcallMs / elapsed) * 100).toFixed(1)}%`);
            console.log(`  FCALLs per job: ${(fcallCount / N).toFixed(2)}`);
            console.log(`  avg Valkey idle between FCALLs: ${((elapsed - totalFcallMs / CONCURRENCY) / N).toFixed(3)}ms`);
          }

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
