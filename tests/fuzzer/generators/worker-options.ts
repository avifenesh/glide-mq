/**
 * Generate random partial WorkerOptions using a seeded PRNG.
 */

import type { SeededRNG } from '../rng';

export function randomWorkerOptions(rng: SeededRNG): any {
  const opts: any = {};

  opts.concurrency = rng.nextInt(1, 15);
  opts.blockTimeout = rng.nextInt(200, 2000);
  opts.stalledInterval = rng.nextInt(5000, 30000);
  opts.maxStalledCount = rng.nextInt(1, 3);
  opts.lockDuration = rng.nextInt(5000, 30000);

  // limiter (20% chance)
  if (rng.chance(0.2)) {
    opts.limiter = {
      max: rng.nextInt(5, 50),
      duration: rng.nextInt(500, 3000),
    };
  }

  return opts;
}
