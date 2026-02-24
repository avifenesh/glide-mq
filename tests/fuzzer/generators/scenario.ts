/**
 * Weighted scenario registry and picker.
 * Factory functions are null placeholders - filled in by the runner
 * when actual scenario modules are imported.
 */

import type { SeededRNG } from '../rng';
import type { ScenarioFn, FuzzMode } from '../types';

export interface ScenarioEntry {
  name: string;
  weight: number;
  factory: ScenarioFn | null;
  requiresValkey: boolean;
}

export const SCENARIO_REGISTRY: ScenarioEntry[] = [
  { name: 'basic-throughput', weight: 10, factory: null, requiresValkey: false },
  { name: 'add-bulk', weight: 8, factory: null, requiresValkey: false },
  { name: 'flow-producer', weight: 6, factory: null, requiresValkey: true },
  { name: 'workflows', weight: 6, factory: null, requiresValkey: true },
  { name: 'priority-ordering', weight: 7, factory: null, requiresValkey: false },
  { name: 'delayed-promote', weight: 6, factory: null, requiresValkey: false },
  { name: 'dedup-modes', weight: 5, factory: null, requiresValkey: false },
  { name: 'rate-limiting', weight: 5, factory: null, requiresValkey: true },
  { name: 'pause-resume', weight: 6, factory: null, requiresValkey: false },
  { name: 'revoke-abort', weight: 5, factory: null, requiresValkey: true },
  { name: 'clean-drain', weight: 5, factory: null, requiresValkey: false },
  { name: 'search-jobs', weight: 4, factory: null, requiresValkey: false },
  { name: 'job-lifecycle', weight: 6, factory: null, requiresValkey: false },
  { name: 'queue-events', weight: 4, factory: null, requiresValkey: true },
  { name: 'compression', weight: 3, factory: null, requiresValkey: true },
  { name: 'dlq', weight: 4, factory: null, requiresValkey: true },
  { name: 'scheduler', weight: 4, factory: null, requiresValkey: false },
  { name: 'graceful-shutdown', weight: 3, factory: null, requiresValkey: false },
  { name: 'mixed-storm', weight: 10, factory: null, requiresValkey: false },
  { name: 'sandbox', weight: 3, factory: null, requiresValkey: true },
];

/**
 * Pick a scenario from the registry using weighted random selection.
 * Filters out requiresValkey scenarios when running in test mode.
 */
export function pickScenario(rng: SeededRNG, mode: FuzzMode): ScenarioEntry {
  const available = mode === 'test' ? SCENARIO_REGISTRY.filter((s) => !s.requiresValkey) : SCENARIO_REGISTRY;

  const eligible = available.filter((s) => s.factory !== null);
  if (eligible.length === 0) {
    throw new Error('No scenarios with registered factories available');
  }

  const weighted: [ScenarioEntry, number][] = eligible.map((s) => [s, s.weight]);
  return rng.weighted(weighted);
}
