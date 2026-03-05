/**
 * Shared utilities for benchmark suite.
 */

import Redis from 'ioredis';

const BENCH_HOST = process.env.BENCH_HOST || '127.0.0.1';
const BENCH_PORT = Number(process.env.BENCH_PORT || 6379);

export interface RedisStatsSnapshot {
  keyspaceHits: number;
  keyspaceMisses: number;
  totalCommands: number;
  evictedKeys: number;
  expiredKeys: number;
  usedCpuUser: number;
  usedCpuSys: number;
}

export interface RedisStatsDelta {
  keyspaceHits: number;
  keyspaceMisses: number;
  totalCommands: number;
  evictedKeys: number;
  expiredKeys: number;
  usedCpuUser: number;
  usedCpuSys: number;
}

function parseInfoNumber(info: string, key: string): number {
  const line = info
    .split('\n')
    .map((entry) => entry.trim())
    .find((entry) => entry.startsWith(`${key}:`));

  if (!line) return 0;

  const value = Number(line.slice(key.length + 1));
  return Number.isFinite(value) ? value : 0;
}

/**
 * Flush the entire database between benchmark runs.
 */
export async function flushDB(): Promise<void> {
  const client = new Redis({ host: BENCH_HOST, port: BENCH_PORT, lazyConnect: true });
  await client.connect();
  await client.flushdb();
  await client.quit();
}

/**
 * Snapshot selected Redis stats counters.
 */
export async function readRedisStats(): Promise<RedisStatsSnapshot> {
  const client = new Redis({ host: BENCH_HOST, port: BENCH_PORT, lazyConnect: true });
  await client.connect();

  try {
    const [statsInfo, cpuInfo] = await Promise.all([client.info('stats'), client.info('cpu')]);
    return {
      keyspaceHits: parseInfoNumber(statsInfo, 'keyspace_hits'),
      keyspaceMisses: parseInfoNumber(statsInfo, 'keyspace_misses'),
      totalCommands: parseInfoNumber(statsInfo, 'total_commands_processed'),
      evictedKeys: parseInfoNumber(statsInfo, 'evicted_keys'),
      expiredKeys: parseInfoNumber(statsInfo, 'expired_keys'),
      usedCpuUser: parseInfoNumber(cpuInfo, 'used_cpu_user'),
      usedCpuSys: parseInfoNumber(cpuInfo, 'used_cpu_sys'),
    };
  } finally {
    await client.quit();
  }
}

/**
 * Compute positive deltas between two Redis stats snapshots.
 */
export function diffRedisStats(before: RedisStatsSnapshot, after: RedisStatsSnapshot): RedisStatsDelta {
  return {
    keyspaceHits: Math.max(0, after.keyspaceHits - before.keyspaceHits),
    keyspaceMisses: Math.max(0, after.keyspaceMisses - before.keyspaceMisses),
    totalCommands: Math.max(0, after.totalCommands - before.totalCommands),
    evictedKeys: Math.max(0, after.evictedKeys - before.evictedKeys),
    expiredKeys: Math.max(0, after.expiredKeys - before.expiredKeys),
    usedCpuUser: Math.max(0, after.usedCpuUser - before.usedCpuUser),
    usedCpuSys: Math.max(0, after.usedCpuSys - before.usedCpuSys),
  };
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
  const colWidths = headers.map((h, i) => Math.max(h.length, ...rows.map((r) => (r[i] || '').length)));

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
  addresses: [{ host: BENCH_HOST, port: BENCH_PORT }],
};

/**
 * Connection config for BullMQ.
 */
export const BULL_CONNECTION = {
  host: BENCH_HOST,
  port: BENCH_PORT,
};
