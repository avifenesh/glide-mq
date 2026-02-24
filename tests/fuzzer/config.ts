/**
 * Fuzzer configuration. Seed is deterministic when FUZZ_SEED env var is set.
 */

export const FUZZ_CONFIG = {
  testModeDurationMs: 60_000,
  standaloneDurationMs: 90_000,
  clusterDurationMs: 90_000,
  maxJobsPerRound: 200,
  maxWorkersPerRound: 5,
  maxConcurrency: 15,
  roundTimeoutMs: 30_000,
  seed: process.env.FUZZ_SEED ? parseInt(process.env.FUZZ_SEED, 10) : Date.now(),
};
