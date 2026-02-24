/**
 * Fuzzer entry point.
 *
 * Three phases:
 * 1. TestQueue/TestWorker (no Valkey) - 1 min
 * 2. Standalone Valkey - 1.5 min (skips if unavailable)
 * 3. Cluster Valkey - 1.5 min (skips if unavailable or SKIP_CLUSTER)
 *
 * Reproduce failures: FUZZ_SEED=<seed> npm run fuzz
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { SeededRNG } from './rng';
import { FUZZ_CONFIG } from './config';
import { runFuzzRound } from './runner';
import type { RoundResult } from './types';

const seed = FUZZ_CONFIG.seed;
console.log(`\n[FUZZ] Seed: ${seed}`);
console.log(`[FUZZ] Reproduce: FUZZ_SEED=${seed} npm run fuzz\n`);

async function runPhase(
  phaseName: string,
  mode: 'test' | 'standalone' | 'cluster',
  durationMs: number,
  seedOffset: number,
): Promise<void> {
  const rng = new SeededRNG(seed + seedOffset);
  const start = Date.now();
  let rounds = 0;
  const allViolations: string[] = [];
  const errors: string[] = [];
  const scenarioCounts: Record<string, number> = {};

  while (Date.now() - start < durationMs) {
    const result: RoundResult = await runFuzzRound(rng, mode);
    rounds++;
    scenarioCounts[result.scenario] = (scenarioCounts[result.scenario] || 0) + 1;

    if (result.error) {
      errors.push(`[Round ${rounds}] ${result.scenario}: ${result.error}`);
    }
    if (result.result.violations.length > 0) {
      for (const v of result.result.violations) {
        allViolations.push(`[Round ${rounds}] ${result.scenario}: ${v}`);
      }
    }
  }

  const elapsed = ((Date.now() - start) / 1000).toFixed(1);
  const scenarioSummary = Object.entries(scenarioCounts)
    .sort((a, b) => b[1] - a[1])
    .map(([name, count]) => `${name}:${count}`)
    .join(', ');

  console.log(`[FUZZ] ${phaseName}: ${rounds} rounds in ${elapsed}s`);
  console.log(`[FUZZ]   Scenarios: ${scenarioSummary}`);

  if (errors.length > 0) {
    console.log(`[FUZZ]   Errors (${errors.length}):`);
    for (const e of errors.slice(0, 5)) console.log(`    ${e}`);
    if (errors.length > 5) console.log(`    ... and ${errors.length - 5} more`);
  }

  expect(allViolations, `Invariant violations in ${phaseName}`).toEqual([]);
}

async function isValkeyAvailable(host: string, port: number): Promise<boolean> {
  try {
    const { GlideClient } = require('@glidemq/speedkey') as typeof import('@glidemq/speedkey');
    const client = await GlideClient.createClient({
      addresses: [{ host, port }],
      requestTimeout: 2000,
    });
    await client.ping();
    client.close();
    return true;
  } catch {
    return false;
  }
}

// Phase 1: TestQueue/TestWorker (always runs, no Valkey needed)
describe('Fuzzer: TestQueue/TestWorker', () => {
  it(
    'randomized rounds',
    async () => {
      await runPhase('TestMode', 'test', FUZZ_CONFIG.testModeDurationMs, 0);
    },
    FUZZ_CONFIG.testModeDurationMs + 60_000,
  );
});

// Phase 2: Standalone Valkey
describe('Fuzzer: Standalone Valkey', () => {
  let available = false;

  beforeAll(async () => {
    available = await isValkeyAvailable('localhost', 6379);
    if (!available) console.log('[FUZZ] Standalone Valkey not available, skipping');
  });

  it(
    'randomized rounds',
    async () => {
      if (!available) return;
      await runPhase('Standalone', 'standalone', FUZZ_CONFIG.standaloneDurationMs, 1);
    },
    FUZZ_CONFIG.standaloneDurationMs + 60_000,
  );
});

// Phase 3: Cluster Valkey
describe('Fuzzer: Cluster Valkey', () => {
  let available = false;

  beforeAll(async () => {
    if (process.env.SKIP_CLUSTER) {
      console.log('[FUZZ] Cluster skipped via SKIP_CLUSTER env');
      return;
    }
    available = await isValkeyAvailable('127.0.0.1', 7000);
    if (!available) console.log('[FUZZ] Cluster Valkey not available, skipping');
  });

  it(
    'randomized rounds',
    async () => {
      if (!available) return;
      await runPhase('Cluster', 'cluster', FUZZ_CONFIG.clusterDurationMs, 2);
    },
    FUZZ_CONFIG.clusterDurationMs + 60_000,
  );
});
