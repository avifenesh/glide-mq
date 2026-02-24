/**
 * Generate random JobOptions using a seeded PRNG.
 */

import type { SeededRNG } from '../rng';

export function randomJobOptions(rng: SeededRNG): any {
  const opts: any = {};

  // delay (30% chance): 0-5000ms
  if (rng.chance(0.3)) {
    opts.delay = rng.nextInt(0, 5000);
  }

  // priority (30% chance): 0-10
  if (rng.chance(0.3)) {
    opts.priority = rng.nextInt(0, 10);
  }

  // attempts (20% chance): 1-5 with optional backoff
  if (rng.chance(0.2)) {
    opts.attempts = rng.nextInt(1, 5);
    if (rng.chance(0.5)) {
      opts.backoff = {
        type: rng.pick(['fixed', 'exponential']),
        delay: rng.nextInt(50, 1000),
      };
    }
  }

  // timeout (10% chance): 500-5000ms
  if (rng.chance(0.1)) {
    opts.timeout = rng.nextInt(500, 5000);
  }

  // removeOnComplete (20% chance)
  if (rng.chance(0.2)) {
    const variant = rng.nextInt(0, 2);
    if (variant === 0) {
      opts.removeOnComplete = true;
    } else if (variant === 1) {
      opts.removeOnComplete = rng.nextInt(1, 100);
    } else {
      opts.removeOnComplete = {
        age: rng.nextInt(60, 3600),
        count: rng.nextInt(10, 500),
      };
    }
  }

  // removeOnFail (20% chance)
  if (rng.chance(0.2)) {
    const variant = rng.nextInt(0, 2);
    if (variant === 0) {
      opts.removeOnFail = true;
    } else if (variant === 1) {
      opts.removeOnFail = false;
    } else {
      opts.removeOnFail = rng.nextInt(1, 100);
    }
  }

  // deduplication (15% chance)
  if (rng.chance(0.15)) {
    const dedupMode = rng.pick(['simple', 'throttle', 'debounce'] as const);
    const dedup: any = {
      id: `dedup-${rng.nextInt(1, 10000)}`,
      mode: dedupMode,
    };
    if (rng.chance(0.5)) {
      dedup.ttl = rng.nextInt(100, 5000);
    }
    opts.deduplication = dedup;
  }

  // ordering (15% chance)
  if (rng.chance(0.15)) {
    const ordering: any = {
      key: `group-${rng.nextInt(1, 20)}`,
    };
    if (rng.chance(0.4)) {
      ordering.concurrency = rng.nextInt(1, 5);
    }
    if (rng.chance(0.3)) {
      ordering.rateLimit = {
        max: rng.nextInt(5, 50),
        duration: rng.nextInt(500, 3000),
      };
    }
    if (rng.chance(0.2)) {
      ordering.tokenBucket = {
        capacity: rng.nextInt(5, 50),
        refillRate: rng.nextInt(1, 20),
      };
    }
    opts.ordering = ordering;
  }

  // cost (10% chance): 1-5
  if (rng.chance(0.1)) {
    opts.cost = rng.nextInt(1, 5);
  }

  return opts;
}
