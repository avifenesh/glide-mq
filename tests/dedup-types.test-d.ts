import { describe, expectTypeOf, it } from 'vitest';
import type { DeduplicationOptions, JobOptions } from '../src/index';

type Dedup = NonNullable<JobOptions['deduplication']>;

describe('deduplication option types', () => {
  it('JobOptions.deduplication is DeduplicationOptions', () => {
    expectTypeOf<Dedup>().toEqualTypeOf<DeduplicationOptions>();
  });

  it('accepts ttl for throttle mode', () => {
    const throttle: Dedup = { id: 'a', mode: 'throttle', ttl: 5000 };
    const throttleNoTtl: Dedup = { id: 'a', mode: 'throttle' };
    expectTypeOf(throttle).toExtend<Dedup>();
    expectTypeOf(throttleNoTtl).toExtend<Dedup>();
  });

  it('accepts simple and debounce without ttl, and an omitted mode', () => {
    const omitted: Dedup = { id: 'a' };
    const simple: Dedup = { id: 'a', mode: 'simple' };
    const debounce: Dedup = { id: 'a', mode: 'debounce' };
    expectTypeOf(omitted).toExtend<Dedup>();
    expectTypeOf(simple).toExtend<Dedup>();
    expectTypeOf(debounce).toExtend<Dedup>();
  });

  it('rejects ttl for simple, debounce and an omitted mode', () => {
    expectTypeOf<{ id: string; mode: 'simple'; ttl: number }>().not.toExtend<Dedup>();
    expectTypeOf<{ id: string; mode: 'debounce'; ttl: number }>().not.toExtend<Dedup>();
    expectTypeOf<{ id: string; ttl: number }>().not.toExtend<Dedup>();
    expectTypeOf<{ id: string; mode: 'simple' | 'debounce' | 'throttle'; ttl: number }>().not.toExtend<Dedup>();
  });

  it('rejects an unknown mode', () => {
    expectTypeOf<{ id: string; mode: 'other' }>().not.toExtend<Dedup>();
  });
});
