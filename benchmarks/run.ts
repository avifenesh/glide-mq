/**
 * Benchmark CLI entry point.
 * Usage: npx tsx benchmarks/run.ts [throughput|latency|memory|sharing|all] [--json <path>]
 */

import * as fs from 'node:fs';
import { runThroughput, type ThroughputResults } from './throughput';
import { runLatency, type LatencyResults } from './latency';
import { runMemory, type MemoryResults } from './memory';
import { runSharing } from './client-sharing';

export interface BenchmarkResults {
  meta: { date: string; node: string; platform: string };
  throughput?: ThroughputResults;
  latency?: LatencyResults;
  memory?: MemoryResults;
}

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const jsonIdx = args.indexOf('--json');
  const jsonPath = jsonIdx >= 0 ? args[jsonIdx + 1] : null;
  const suite = args.find((a) => a !== '--json' && !a.startsWith('-') && (jsonIdx < 0 || a !== args[jsonIdx + 1])) || 'all';

  console.log('# glide-mq vs BullMQ Benchmarks');
  console.log(`Date: ${new Date().toISOString()}`);
  console.log(`Node: ${process.version}`);
  console.log(`Platform: ${process.platform} ${process.arch}`);

  const results: BenchmarkResults = {
    meta: {
      date: new Date().toISOString(),
      node: process.version,
      platform: `${process.platform} ${process.arch}`,
    },
  };

  const suites: Record<string, () => Promise<void>> = {
    throughput: async () => { results.throughput = await runThroughput(); },
    latency: async () => { results.latency = await runLatency(); },
    memory: async () => { results.memory = await runMemory(); },
    sharing: runSharing,
  };

  if (suite === 'all') {
    for (const [name, fn] of Object.entries(suites)) {
      console.log(`\n--- Running: ${name} ---`);
      await fn();
    }
  } else if (suites[suite]) {
    await suites[suite]();
  } else {
    console.error(`Unknown benchmark: ${suite}`);
    console.error(`Available: ${Object.keys(suites).join(', ')}, all`);
    process.exit(1);
  }

  if (jsonPath) {
    fs.writeFileSync(jsonPath, JSON.stringify(results, null, 2));
    console.log(`\nResults written to ${jsonPath}`);
  }

  console.log('\nDone.');
  process.exit(0);
}

main().catch((err) => {
  console.error('Benchmark failed:', err);
  process.exit(1);
});
