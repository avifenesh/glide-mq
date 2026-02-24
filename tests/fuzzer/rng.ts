/**
 * Seeded PRNG using xorshift128+.
 * Deterministic: same seed produces same sequence across runs.
 */

export class SeededRNG {
  private s0: number;
  private s1: number;

  constructor(seed: number) {
    // Splitmix64 to initialize state from a single seed
    let s = seed | 0;
    s = ((s >>> 16) ^ s) * 0x45d9f3b | 0;
    s = ((s >>> 16) ^ s) * 0x45d9f3b | 0;
    s = (s >>> 16) ^ s;
    this.s0 = s === 0 ? 1 : s;

    s = (seed + 0x9e3779b9) | 0;
    s = ((s >>> 16) ^ s) * 0x45d9f3b | 0;
    s = ((s >>> 16) ^ s) * 0x45d9f3b | 0;
    s = (s >>> 16) ^ s;
    this.s1 = s === 0 ? 1 : s;
  }

  /** Returns a float in [0, 1). */
  next(): number {
    let s1 = this.s0;
    const s0 = this.s1;
    this.s0 = s0;
    s1 ^= s1 << 23;
    s1 ^= s1 >>> 17;
    s1 ^= s0;
    s1 ^= s0 >>> 26;
    this.s1 = s1;
    // Use unsigned right shift to get a positive 32-bit integer
    return ((this.s0 + this.s1) >>> 0) / 0x100000000;
  }

  /** Returns an integer in [min, max] (inclusive). */
  nextInt(min: number, max: number): number {
    return min + Math.floor(this.next() * (max - min + 1));
  }

  /** Pick a random element from an array. */
  pick<T>(arr: T[]): T {
    if (arr.length === 0) throw new Error('Cannot pick from empty array');
    return arr[this.nextInt(0, arr.length - 1)];
  }

  /** Pick N unique random elements from an array (no repeats). */
  pickN<T>(arr: T[], n: number): T[] {
    if (n > arr.length) throw new Error(`Cannot pick ${n} from array of length ${arr.length}`);
    const copy = arr.slice();
    const result: T[] = [];
    for (let i = 0; i < n; i++) {
      const idx = this.nextInt(0, copy.length - 1);
      result.push(copy[idx]);
      copy[idx] = copy[copy.length - 1];
      copy.pop();
    }
    return result;
  }

  /** Fisher-Yates shuffle (in-place, returns same array). */
  shuffle<T>(arr: T[]): T[] {
    for (let i = arr.length - 1; i > 0; i--) {
      const j = this.nextInt(0, i);
      const tmp = arr[i];
      arr[i] = arr[j];
      arr[j] = tmp;
    }
    return arr;
  }

  /** Returns true with probability p (0..1). */
  chance(p: number): boolean {
    return this.next() < p;
  }

  /** Weighted random selection. Items is array of [value, weight] pairs. */
  weighted<T>(items: [T, number][]): T {
    if (items.length === 0) throw new Error('Cannot pick from empty weighted list');
    let total = 0;
    for (const [, w] of items) total += w;
    let r = this.next() * total;
    for (const [item, w] of items) {
      r -= w;
      if (r <= 0) return item;
    }
    return items[items.length - 1][0];
  }
}
