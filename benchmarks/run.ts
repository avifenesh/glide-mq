/**
 * Benchmark CLI entry point.
 * Usage: npx tsx benchmarks/run.ts [throughput|latency|memory|all]
 */

import { runThroughput } from './throughput';
import { runLatency } from './latency';
import { runMemory } from './memory';
import { runSharing } from './client-sharing';

const SUITES: Record<string, () => Promise<void>> = {
  throughput: runThroughput,
  latency: runLatency,
  memory: runMemory,
  sharing: runSharing,
};

async function main(): Promise<void> {
  const arg = process.argv[2] || 'all';

  console.log('# glide-mq vs BullMQ Benchmarks');
  console.log(`Date: ${new Date().toISOString()}`);
  console.log(`Node: ${process.version}`);
  console.log(`Platform: ${process.platform} ${process.arch}`);

  if (arg === 'all') {
    for (const [name, fn] of Object.entries(SUITES)) {
      console.log(`\n--- Running: ${name} ---`);
      await fn();
    }
  } else if (SUITES[arg]) {
    await SUITES[arg]();
  } else {
    console.error(`Unknown benchmark: ${arg}`);
    console.error(`Available: ${Object.keys(SUITES).join(', ')}, all`);
    process.exit(1);
  }

  console.log('\nDone.');
  process.exit(0);
}

main().catch((err) => {
  console.error('Benchmark failed:', err);
  process.exit(1);
});
