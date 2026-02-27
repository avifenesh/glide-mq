import { gzipSync, gunzipSync } from 'zlib';
import { randomBytes } from 'crypto';

const DEFAULT_PREFIX = 'glide';

// 1MB max payload size to prevent DoS
export const MAX_JOB_DATA_SIZE = 1048576;

// ---- Compression helpers ----

const COMPRESSED_PREFIX = 'gz:';

/**
 * Compress a string with gzip and return a prefixed base64 string.
 * Format: 'gz:' + base64(gzipped data)
 */
export function compress(data: string): string {
  const buf = gzipSync(Buffer.from(data, 'utf8'));
  return COMPRESSED_PREFIX + buf.toString('base64');
}

/**
 * Decompress a 'gz:'-prefixed base64 string back to the original string.
 * If the input is not compressed (no 'gz:' prefix), returns it as-is.
 */
export function decompress(data: string): string {
  if (!data.startsWith(COMPRESSED_PREFIX)) {
    return data;
  }
  const buf = Buffer.from(data.slice(COMPRESSED_PREFIX.length), 'base64');
  return gunzipSync(buf).toString('utf8');
}

// Valkey SCAN glob special characters that must be escaped in key patterns
const GLOB_SPECIAL = /[*?\[\]\\]/g;

export function escapeGlob(str: string): string {
  return str.replace(GLOB_SPECIAL, '\\$&');
}

export function keyPrefix(prefix: string, queueName: string): string {
  return `${prefix}:{${queueName}}`;
}

/**
 * Returns an escaped key prefix safe for use in SCAN MATCH patterns.
 */
export function keyPrefixPattern(prefix: string, queueName: string): string {
  return `${escapeGlob(prefix)}:{${escapeGlob(queueName)}}`;
}

export function buildKeys(queueName: string, prefix = DEFAULT_PREFIX) {
  const p = keyPrefix(prefix, queueName);
  return {
    id: `${p}:id`,
    stream: `${p}:stream`,
    scheduled: `${p}:scheduled`,
    completed: `${p}:completed`,
    failed: `${p}:failed`,
    events: `${p}:events`,
    meta: `${p}:meta`,
    dedup: `${p}:dedup`,
    rate: `${p}:rate`,
    schedulers: `${p}:schedulers`,
    ordering: `${p}:ordering`,
    job: (id: string) => `${p}:job:${id}`,
    log: (id: string) => `${p}:log:${id}`,
    deps: (id: string) => `${p}:deps:${id}`,
    ratelimited: `${p}:ratelimited`,
    group: (key: string) => `${p}:group:${key}`,
    groupq: (key: string) => `${p}:groupq:${key}`,
    worker: (id: string) => `${p}:w:${id}`,
  };
}

// Priority encoding: (priority * 2^42) + timestamp_ms
// Priority 0 is highest. Within same priority, FIFO by timestamp.
const PRIORITY_SHIFT = 2 ** 42;

export function encodeScore(priority: number, timestampMs: number): number {
  return priority * PRIORITY_SHIFT + timestampMs;
}

export function decodeScore(score: number): { priority: number; timestampMs: number } {
  const priority = Math.floor(score / PRIORITY_SHIFT);
  const timestampMs = score % PRIORITY_SHIFT;
  return { priority, timestampMs };
}

export function calculateBackoff(type: string, delay: number, attemptsMade: number, jitter = 0): number {
  let ms: number;
  switch (type) {
    case 'exponential':
      ms = Math.pow(2, attemptsMade - 1) * delay;
      break;
    case 'fixed':
    default:
      ms = delay;
      break;
  }
  if (jitter > 0) {
    ms += Math.random() * jitter * ms;
  }
  return Math.round(ms);
}

export function generateId(): string {
  return `${Date.now()}-${randomBytes(4).toString('hex')}`;
}

/**
 * Compute the next exponential backoff delay.
 * Sequence: 0 -> 1000, 1000 -> 2000, 2000 -> 4000, ..., capped at maxMs.
 */
export function nextReconnectDelay(currentDelay: number, maxMs = 30000): number {
  if (currentDelay === 0) return 1000;
  return Math.min(currentDelay * 2, maxMs);
}

// ---- HashDataType conversion ----

/**
 * Convert a HashDataType array ({ field, value }[]) from hgetall to a plain Record.
 * Returns null if the array is empty or falsy (key does not exist).
 */
export function hashDataToRecord(
  hashData: { field?: unknown; key?: unknown; value: unknown }[] | null,
): Record<string, string> | null {
  if (!hashData || hashData.length === 0) return null;
  const record: Record<string, string> = {};
  for (const entry of hashData) {
    // Batch hgetall returns {key, value}; direct hgetall returns {field, value}
    const k = entry.field ?? entry.key;
    if (k == null) continue;
    record[String(k)] = String(entry.value);
  }
  return record;
}

// ---- Stream jobId extraction ----

/**
 * Extract jobId values from stream entries returned by xrange/xreadgroup.
 * The entries object maps entryId -> [field, value][] pairs.
 */
export function extractJobIdsFromStreamEntries(entries: Record<string, [unknown, unknown][]>): string[] {
  const jobIds: string[] = [];
  for (const fieldPairs of Object.values(entries)) {
    for (const [field, value] of fieldPairs) {
      if (String(field) === 'jobId') {
        jobIds.push(String(value));
      }
    }
  }
  return jobIds;
}

// ---- Reconnect helper ----

export interface ReconnectContext {
  isActive(): boolean;
  getBackoff(): number;
  setBackoff(ms: number): void;
  onError(err: unknown): void;
}

/**
 * Attempt a reconnect operation with exponential backoff.
 * On success, resets backoff and calls resumeFn.
 * On failure, emits error, bumps backoff, and schedules a retry.
 */
export async function reconnectWithBackoff(
  ctx: ReconnectContext,
  reconnectFn: () => Promise<void>,
  resumeFn: () => void,
): Promise<void> {
  if (!ctx.isActive()) return;

  try {
    await reconnectFn();
    ctx.setBackoff(0);
    resumeFn();
  } catch (err) {
    if (!ctx.isActive()) return;
    ctx.onError(err);
    const delay = nextReconnectDelay(ctx.getBackoff());
    ctx.setBackoff(delay);
    setTimeout(() => reconnectWithBackoff(ctx, reconnectFn, resumeFn), delay);
  }
}

// ---- Simple 5-field cron parser ----
// Format: minute hour dayOfMonth month dayOfWeek
// Supports: *, specific numbers, ranges (1-5), steps (*/5), lists (1,3,5)

function parseCronField(field: string, min: number, max: number): number[] {
  const values: Set<number> = new Set();

  for (const part of field.split(',')) {
    const trimmed = part.trim();

    if (trimmed === '*') {
      for (let i = min; i <= max; i++) values.add(i);
      continue;
    }

    const stepMatch = trimmed.match(/^(\*|(\d+)-(\d+))\/(\d+)$/);
    if (stepMatch) {
      const step = parseInt(stepMatch[4], 10);
      if (step <= 0) {
        throw new Error(`Invalid cron step: ${step}`);
      }
      let start = min;
      let end = max;
      if (stepMatch[2] !== undefined) {
        start = parseInt(stepMatch[2], 10);
        end = parseInt(stepMatch[3], 10);
      }
      if (start < min || end > max) {
        throw new Error(`Cron range out of bounds: ${start}-${end}`);
      }
      if (start > end) {
        throw new Error(`Cron range reversed: ${start}-${end}`);
      }
      for (let i = start; i <= end; i += step) values.add(i);
      continue;
    }

    const rangeMatch = trimmed.match(/^(\d+)-(\d+)$/);
    if (rangeMatch) {
      const from = parseInt(rangeMatch[1], 10);
      const to = parseInt(rangeMatch[2], 10);
      if (from < min || to > max) {
        throw new Error(`Cron range out of bounds: ${from}-${to}`);
      }
      if (from > to) {
        throw new Error(`Cron range reversed: ${from}-${to}`);
      }
      for (let i = from; i <= to; i++) values.add(i);
      continue;
    }

    if (!/^\d+$/.test(trimmed)) {
      throw new Error(`Invalid cron token: ${trimmed}`);
    }
    const num = parseInt(trimmed, 10);
    if (num < min || num > max) {
      throw new Error(`Cron value out of bounds: ${num}`);
    }
    values.add(num);
  }

  return [...values].sort((a, b) => a - b);
}

// Maximum search horizon in years to prevent infinite loops (e.g. Feb 30)
// 10 years covers century non-leap-year gaps (e.g. Feb 29 after 2097 -> 2104)
const MAX_SEARCH_YEARS = 10;

/**
 * Compute the next occurrence of a cron pattern after `afterMs` (epoch ms).
 * Supports standard 5-field cron: minute hour dayOfMonth month dayOfWeek.
 * Returns epoch ms of the next matching time.
 */
export function nextCronOccurrence(pattern: string, afterMs: number): number {
  const fields = pattern.trim().split(/\s+/);
  if (fields.length !== 5) {
    throw new Error(`Invalid cron pattern: expected 5 fields, got ${fields.length}`);
  }

  const minutes = parseCronField(fields[0], 0, 59);
  const hours = parseCronField(fields[1], 0, 23);
  const daysOfMonth = parseCronField(fields[2], 1, 31);
  const months = parseCronField(fields[3], 1, 12);
  const daysOfWeek = parseCronField(fields[4], 0, 6); // 0=Sunday

  // Start from the next minute after afterMs (all operations in UTC)
  const d = new Date(afterMs);
  d.setUTCSeconds(0, 0);
  d.setUTCMinutes(d.getUTCMinutes() + 1);

  const endYear = d.getUTCFullYear() + MAX_SEARCH_YEARS;

  while (d.getUTCFullYear() <= endYear) {
    // 1. Month check
    const currentMonth = d.getUTCMonth() + 1;
    if (!months.includes(currentMonth)) {
      // Find next valid month in the list that is > currentMonth
      const nextMonth = months.find((m) => m > currentMonth);
      if (nextMonth != null) {
        // Jump to start of that month in current year
        d.setUTCMonth(nextMonth - 1, 1);
        d.setUTCHours(0, 0, 0, 0);
      } else {
        // No more valid months this year; jump to first valid month of next year
        d.setUTCFullYear(d.getUTCFullYear() + 1, months[0] - 1, 1);
        d.setUTCHours(0, 0, 0, 0);
      }
      continue;
    }

    // 2. Day check
    const currentDay = d.getUTCDate();
    const currentDayOfWeek = d.getUTCDay();
    if (!daysOfMonth.includes(currentDay) || !daysOfWeek.includes(currentDayOfWeek)) {
      // Increment day by 1 and reset time to 00:00
      // Optimization: Could jump to next valid DOM/DOW, but interaction between DOM and DOW is complex.
      // Iterating days is generally fast enough (max 31 per month).
      d.setUTCDate(d.getUTCDate() + 1);
      d.setUTCHours(0, 0, 0, 0);
      continue;
    }

    // 3. Hour check
    const currentHour = d.getUTCHours();
    if (!hours.includes(currentHour)) {
      // Find next valid hour > currentHour
      const nextHour = hours.find((h) => h > currentHour);
      if (nextHour != null) {
        // Jump to that hour, minute 0
        d.setUTCHours(nextHour, 0, 0, 0);
      } else {
        // No more valid hours today; jump to next day
        d.setUTCDate(d.getUTCDate() + 1);
        d.setUTCHours(0, 0, 0, 0);
      }
      continue;
    }

    // 4. Minute check
    const currentMinute = d.getUTCMinutes();
    if (!minutes.includes(currentMinute)) {
      // Find next valid minute > currentMinute
      const nextMinute = minutes.find((m) => m > currentMinute);
      if (nextMinute != null) {
        // Found match!
        d.setUTCMinutes(nextMinute, 0, 0);
        return d.getTime();
      } else {
        // No more valid minutes this hour; jump to next hour
        d.setUTCHours(d.getUTCHours() + 1, 0, 0, 0);
        continue;
      }
    }

    // All fields match
    return d.getTime();
  }

  throw new Error(`No cron match found within ${MAX_SEARCH_YEARS} years for pattern: ${pattern}`);
}
