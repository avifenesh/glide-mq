/**
 * Shared types for the fuzzer framework.
 */

import type { SeededRNG } from './rng';

export type FuzzMode = 'test' | 'standalone' | 'cluster';

export interface ScenarioContext {
  rng: SeededRNG;
  mode: FuzzMode;
  createQueue(name: string, opts?: any): any;
  createWorker(name: string, processor: any, opts?: any): any;
  /** Only available in standalone/cluster mode. Throws in test mode. */
  createFlowProducer(): any;
  /** Only available in standalone/cluster mode. Throws in test mode. */
  createQueueEvents(name: string): any;
  /** Generate a unique queue name. */
  uid(): string;
  /** Close all tracked resources in reverse order. */
  cleanup(): Promise<void>;
}

export interface ScenarioResult {
  added: number;
  processed: number;
  failed: number;
  violations: string[];
}

export type ScenarioFn = (ctx: ScenarioContext) => Promise<ScenarioResult>;

export interface RoundResult {
  scenario: string;
  duration: number;
  result: ScenarioResult;
  error?: string;
}
