/**
 * Round executor: picks a scenario, runs it, returns results.
 */

import { SeededRNG } from './rng';
import { FUZZ_CONFIG } from './config';
import { createScenarioContext } from './context';
import { SCENARIO_REGISTRY, pickScenario } from './generators/scenario';
import type { FuzzMode, RoundResult } from './types';

// Import all scenario factories and wire them into the registry
import { basicThroughput } from './scenarios/basic-throughput';
import { addBulk } from './scenarios/add-bulk';
import { flowProducer } from './scenarios/flow-producer';
import { workflows } from './scenarios/workflows';
import { priorityOrdering } from './scenarios/priority-ordering';
import { delayedPromote } from './scenarios/delayed-promote';
import { dedupModes } from './scenarios/dedup-modes';
import { rateLimiting } from './scenarios/rate-limiting';
import { pauseResume } from './scenarios/pause-resume';
import { revokeAbort } from './scenarios/revoke-abort';
import { cleanDrain } from './scenarios/clean-drain';
import { searchJobs } from './scenarios/search-jobs';
import { jobLifecycle } from './scenarios/job-lifecycle';
import { queueEvents } from './scenarios/queue-events';
import { compression } from './scenarios/compression';
import { dlq } from './scenarios/dlq';
import { scheduler } from './scenarios/scheduler';
import { gracefulShutdown } from './scenarios/graceful-shutdown';
import { mixedStorm } from './scenarios/mixed-storm';
import { sandbox } from './scenarios/sandbox';

const FACTORIES: Record<string, any> = {
  'basic-throughput': basicThroughput,
  'add-bulk': addBulk,
  'flow-producer': flowProducer,
  workflows,
  'priority-ordering': priorityOrdering,
  'delayed-promote': delayedPromote,
  'dedup-modes': dedupModes,
  'rate-limiting': rateLimiting,
  'pause-resume': pauseResume,
  'revoke-abort': revokeAbort,
  'clean-drain': cleanDrain,
  'search-jobs': searchJobs,
  'job-lifecycle': jobLifecycle,
  'queue-events': queueEvents,
  compression,
  dlq,
  scheduler,
  'graceful-shutdown': gracefulShutdown,
  'mixed-storm': mixedStorm,
  sandbox,
};

// Wire factories into registry
for (const entry of SCENARIO_REGISTRY) {
  if (FACTORIES[entry.name]) {
    entry.factory = FACTORIES[entry.name];
  }
}

function timeoutPromise(ms: number, label: string): Promise<never> {
  return new Promise((_, reject) =>
    setTimeout(() => reject(new Error(`Timeout: ${label} after ${ms}ms`)), ms),
  );
}

export async function runFuzzRound(
  rng: SeededRNG,
  mode: FuzzMode,
): Promise<RoundResult> {
  const scenario = pickScenario(rng, mode);
  const ctx = createScenarioContext(rng, mode);
  const start = Date.now();

  try {
    const result = await Promise.race([
      scenario.factory!(ctx),
      timeoutPromise(FUZZ_CONFIG.roundTimeoutMs, scenario.name),
    ]);
    return {
      scenario: scenario.name,
      duration: Date.now() - start,
      result,
    };
  } catch (err: any) {
    return {
      scenario: scenario.name,
      duration: Date.now() - start,
      result: { added: 0, processed: 0, failed: 0, violations: [] },
      error: err.message || String(err),
    };
  } finally {
    try {
      await Promise.race([ctx.cleanup(), timeoutPromise(10_000, 'cleanup')]);
    } catch {}
  }
}
