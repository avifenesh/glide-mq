/**
 * Generate random data payloads for fuzz testing.
 */

import type { SeededRNG } from '../rng';

type PayloadGenerator = (rng: SeededRNG) => Record<string, unknown>;

const generators: PayloadGenerator[] = [
  // 0: simple number
  (rng) => ({ value: rng.nextInt(-1_000_000, 1_000_000) }),

  // 1: long string
  (rng) => {
    const len = rng.nextInt(100, 10000);
    const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
    let s = '';
    for (let i = 0; i < len; i++) s += chars[rng.nextInt(0, chars.length - 1)];
    return { text: s };
  },

  // 2: unicode
  () => ({
    text: '\u{1F4A9}\u{1F525}\u{2764}\u{FE0F}\u{1F680}\u{2728}',
    label: '\u00E9\u00E0\u00FC\u00F1\u00E7\u4E16\u754C',
  }),

  // 3: deep nesting
  (rng) => {
    const depth = rng.nextInt(3, 10);
    let obj: Record<string, unknown> = { leaf: rng.nextInt(0, 100) };
    for (let i = 0; i < depth; i++) {
      obj = { [`level${i}`]: obj };
    }
    return obj;
  },

  // 4: large array
  (rng) => {
    const len = rng.nextInt(50, 500);
    return { items: Array.from({ length: len }, (_, i) => ({ id: i, val: rng.next() })) };
  },

  // 5: empty object
  () => ({}),

  // 6: null bytes
  () => ({ raw: 'before\x00middle\x00after', flag: true }),

  // 7: special chars
  () => ({
    html: '<script>alert("xss")</script>',
    sql: "'; DROP TABLE jobs; --",
    path: 'C:\\Users\\test\\file.txt',
    newlines: 'line1\nline2\r\nline3\ttab',
    quotes: 'he said "hello" and \'goodbye\'',
  }),

  // 8: large data (50-200 KB JSON)
  (rng) => {
    const count = rng.nextInt(500, 2000);
    const rows: Record<string, unknown>[] = [];
    for (let i = 0; i < count; i++) {
      rows.push({ id: i, name: `item-${i}`, value: rng.next() });
    }
    return { rows };
  },

  // 9: empty values / edge types
  () => ({
    emptyStr: '',
    zero: 0,
    negZero: -0,
    falseBool: false,
    emptyArr: [],
    emptyObj: {},
    nested: { also: {} },
  }),
];

export function randomPayload(rng: SeededRNG): Record<string, unknown> {
  const idx = rng.nextInt(0, generators.length - 1);
  return generators[idx](rng);
}
