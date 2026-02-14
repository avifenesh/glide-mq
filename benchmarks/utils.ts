/**
 * Shared utilities for benchmark suite.
 */

import Redis from 'ioredis';

/**
 * Flush the entire database between benchmark runs.
 */
export async function flushDB(): Promise<void> {
  const client = new Redis({ host: '127.0.0.1', port: 6379, lazyConnect: true });
  await client.connect();
  await client.flushdb();
  await client.quit();
}

/**
 * Compute percentiles from a sorted array of numbers.
 */
export function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const idx = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, idx)];
}

/**
 * Format a number with commas for readability.
 */
export function fmt(n: number): string {
  return n.toLocaleString('en-US', { maximumFractionDigits: 2 });
}

/**
 * Format milliseconds with 2 decimal places.
 */
export function fmtMs(n: number): string {
  return n.toFixed(2) + ' ms';
}

/**
 * Print a markdown table.
 */
export function printTable(headers: string[], rows: string[][]): void {
  const colWidths = headers.map((h, i) =>
    Math.max(h.length, ...rows.map((r) => (r[i] || '').length)),
  );

  const pad = (s: string, w: number) => s + ' '.repeat(Math.max(0, w - s.length));
  const sep = colWidths.map((w) => '-'.repeat(w));

  console.log('| ' + headers.map((h, i) => pad(h, colWidths[i])).join(' | ') + ' |');
  console.log('| ' + sep.map((s, i) => pad(s, colWidths[i])).join(' | ') + ' |');
  for (const row of rows) {
    console.log('| ' + row.map((c, i) => pad(c || '', colWidths[i])).join(' | ') + ' |');
  }
}

/**
 * Sleep for the given milliseconds.
 */
export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Connection config for glide-mq.
 */
export const GLIDE_CONNECTION = {
  addresses: [{ host: '127.0.0.1', port: 6379 }],
};

/**
 * Connection config for BullMQ.
 */
export const BULL_CONNECTION = {
  host: '127.0.0.1',
  port: 6379,
};
