import { gzipSync, gunzipSync } from 'zlib';
import { randomBytes } from 'crypto';
import type { ScheduleOpts, SchedulerEntry } from './types';

const DEFAULT_PREFIX = 'glide';

// 1MB max payload size to prevent DoS
export const MAX_JOB_DATA_SIZE = 1048576;

/** Characters that break Valkey cluster hash-tag routing when used in queue names. */
export const INVALID_QUEUE_NAME_CHARS = /[{}:]/;

/** Characters that are invalid in job IDs. */
export const INVALID_JOB_ID_CHARS = /[\x00-\x1f\x7f{}:]/;

/** Maximum length for ordering keys. */
export const MAX_ORDERING_KEY_LENGTH = 256;

/**
 * Validate a job ID. Throws if the ID is too long or contains forbidden characters.
 */
export function validateJobId(jobId: string): void {
  if (jobId.length > 256) throw new Error('jobId must be at most 256 characters');
  if (INVALID_JOB_ID_CHARS.test(jobId)) {
    throw new Error('jobId must not contain control characters, curly braces, or colons');
  }
}

/**
 * Validate a queue name. Throws if it contains characters that would corrupt
 * cluster hash-tag routing (curly braces or colons).
 */
export function validateQueueName(name: string): void {
  if (!name || typeof name !== 'string') {
    throw new Error('Queue name must be a non-empty string');
  }
  if (INVALID_QUEUE_NAME_CHARS.test(name)) {
    throw new Error('Queue name must not contain curly braces or colons');
  }
}

export function isPlainStepPayload(value: unknown): value is Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false;
  return Object.getPrototypeOf(value) === Object.prototype;
}

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
  return gunzipSync(buf, { maxOutputLength: MAX_JOB_DATA_SIZE }).toString('utf8');
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
    metricsCompleted: `${p}:metrics:completed`,
    lifo: `${p}:lifo`,
    priority: `${p}:priority`,
    listActive: `${p}:list-active`,
    metricsFailed: `${p}:metrics:failed`,
    group: (key: string) => `${p}:group:${key}`,
    groupq: (key: string) => `${p}:groupq:${key}`,
    parents: (id: string) => `${p}:parents:${id}`,
    worker: (id: string) => `${p}:w:${id}`,
  };
}

// Priority encoding: (priority * 2^42) + timestamp_ms
// Priority 0 is highest. Within same priority, FIFO by timestamp.
const PRIORITY_SHIFT = 2 ** 42;

export function encodeScore(priority: number, timestampMs: number): number {
  if (priority > 2048) {
    throw new Error('Priority must be <= 2048');
  }
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
  const record: Record<string, string> = Object.create(null);
  for (let i = 0; i < hashData.length; i++) {
    const entry = hashData[i];
    // Batch hgetall returns {key, value}; direct hgetall returns {field, value}
    const k = entry.field ?? entry.key;
    if (k == null) continue;
    record[String(k)] = String(entry.value);
  }
  return record;
}

// ---- Job metadata fields (everything except data/returnvalue) ----

/**
 * All known job hash fields except `data` and `returnvalue`.
 * Used by getJob/getJobs with `excludeData: true` to fetch only metadata via HMGET.
 */
// Keep in sync with hash fields written by Lua job-hash writers
// (addJob, completeJob, failJob, moveToActive, revoke, etc.).
export const JOB_METADATA_FIELDS: readonly string[] = Object.freeze([
  'id',
  'name',
  'opts',
  'timestamp',
  'attemptsMade',
  'state',
  'delay',
  'priority',
  'maxAttempts',
  'processedOn',
  'finishedOn',
  'failedReason',
  'parentId',
  'parentQueue',
  'orderingKey',
  'orderingSeq',
  'groupKey',
  'cost',
  'expireAt',
  'progress',
  'revoked',
  'lastActive',
  'schedulerName',
  'parentIds',
  'parentQueues',
]);

/**
 * Convert an HMGET result array to a Record keyed by field name.
 * Returns null if every value is null (key does not exist).
 */
export function hmgetArrayToRecord(
  values: (unknown | null)[],
  fields: readonly string[],
): Record<string, string> | null {
  const record: Record<string, string> = Object.create(null);
  let hasAny = false;
  for (let i = 0; i < fields.length && i < values.length; i++) {
    if (values[i] != null) {
      record[fields[i]] = String(values[i]);
      hasAny = true;
    }
  }
  return hasAny ? record : null;
}

// ---- Stream jobId extraction ----

/**
 * Extract jobId values from stream entries returned by xrange/xreadgroup.
 * The entries object maps entryId -> [field, value][] pairs.
 */
export function extractJobIdsFromStreamEntries(entries: Record<string, [unknown, unknown][]>): string[] {
  const jobIds: string[] = [];
  for (const entryId in entries) {
    if (!Object.prototype.hasOwnProperty.call(entries, entryId)) continue;
    const fieldPairs = entries[entryId];
    for (let i = 0; i < fieldPairs.length; i++) {
      const field = fieldPairs[i][0];
      const value = fieldPairs[i][1];
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

interface CronField {
  set: Set<number>;
  sorted: number[];
}

function parseCronField(field: string, min: number, max: number): CronField {
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

  const sorted = [...values].sort((a, b) => a - b);
  return { set: values, sorted };
}

// Maximum search horizon in years to prevent infinite loops (e.g. Feb 30)
// 10 years covers century non-leap-year gaps (e.g. Feb 29 after 2097 -> 2104)
const MAX_SEARCH_YEARS = 10;

/**
 * Validate an IANA timezone string. Throws if invalid.
 */
export function validateTimezone(tz: string): void {
  try {
    Intl.DateTimeFormat('en-US', { timeZone: tz });
  } catch {
    throw new Error(`Invalid timezone: ${tz}`);
  }
}

export function isValidSchedulerEvery(every: unknown): every is number {
  return typeof every === 'number' && Number.isSafeInteger(every) && every > 0;
}

export function validateSchedulerEvery(every: number | undefined): void {
  if (every == null) return;
  if (!isValidSchedulerEvery(every)) {
    throw new Error('every must be a positive safe integer');
  }
}

export function normalizeScheduleDate(
  value: Date | number | undefined,
  fieldName: 'startDate' | 'endDate',
): number | undefined {
  if (value == null) return undefined;
  const ts = value instanceof Date ? value.getTime() : value;
  if (!Number.isFinite(ts)) {
    throw new Error(`${fieldName} must be a valid Date or timestamp`);
  }
  return ts;
}

export function validateSchedulerBounds(
  startDate: number | undefined,
  endDate: number | undefined,
  limit: number | undefined,
): void {
  if (startDate != null && endDate != null && startDate > endDate) {
    throw new Error('startDate must be less than or equal to endDate');
  }
  if (limit != null && (!Number.isInteger(limit) || limit <= 0)) {
    throw new Error('limit must be a positive integer');
  }
}

// Cache DateTimeFormat instances per timezone for performance
const dtfCache = new Map<string, Intl.DateTimeFormat>();

function getFormatter(tz: string): Intl.DateTimeFormat {
  let f = dtfCache.get(tz);
  if (!f) {
    f = new Intl.DateTimeFormat('en-US', {
      timeZone: tz,
      year: 'numeric',
      month: 'numeric',
      day: 'numeric',
      hour: 'numeric',
      minute: 'numeric',
      second: 'numeric',
      hour12: false,
    });
    dtfCache.set(tz, f);
  }
  return f;
}

interface TzParts {
  year: number;
  month: number; // 1-12
  day: number;
  hour: number;
  minute: number;
  dayOfWeek: number; // 0=Sunday
}

/**
 * Get wall-clock parts for a UTC epoch in the given timezone.
 */
function utcToTzParts(epochMs: number, tz: string): TzParts {
  const f = getFormatter(tz);
  const parts = f.formatToParts(new Date(epochMs));
  const p: Record<string, string> = {};
  for (const part of parts) {
    p[part.type] = part.value;
  }
  const year = parseInt(p.year, 10);
  const month = parseInt(p.month, 10);
  const day = parseInt(p.day, 10);
  // hour12:false can return '24' for midnight in some locales; normalize to 0
  let hour = parseInt(p.hour, 10);
  if (hour === 24) hour = 0;
  const minute = parseInt(p.minute, 10);
  // Compute day of week from a UTC date constructed from these parts
  const dow = new Date(Date.UTC(year, month - 1, day)).getUTCDay();
  return { year, month, day, hour, minute, dayOfWeek: dow };
}

/**
 * Convert wall-clock parts in a timezone to a UTC epoch.
 * For spring-forward gaps (wall-clock time doesn't exist), returns -1.
 * For fall-back overlaps (ambiguous), returns the first (earlier) UTC instant.
 */
function tzPartsToUtc(year: number, month: number, day: number, hour: number, minute: number, tz: string): number {
  // Start with a naive UTC estimate using the same wall-clock components
  const naive = Date.UTC(year, month - 1, day, hour, minute, 0, 0);
  // Get the actual wall-clock time at our naive estimate to compute the offset
  const naiveParts = utcToTzParts(naive, tz);
  // Compute the offset in ms: how much the timezone differs from UTC at this point
  const naiveWall = Date.UTC(
    naiveParts.year,
    naiveParts.month - 1,
    naiveParts.day,
    naiveParts.hour,
    naiveParts.minute,
    0,
    0,
  );
  const offsetMs = naiveWall - naive;
  // Apply offset to get a candidate
  const candidate = naive - offsetMs;
  const cp = utcToTzParts(candidate, tz);
  if (cp.year === year && cp.month === month && cp.day === day && cp.hour === hour && cp.minute === minute) {
    return candidate;
  }
  // Offset might be wrong near DST transitions - try +/- 1 hour
  for (const delta of [-3600000, 3600000, -7200000, 7200000]) {
    const alt = candidate + delta;
    const ap = utcToTzParts(alt, tz);
    if (ap.year === year && ap.month === month && ap.day === day && ap.hour === hour && ap.minute === minute) {
      return alt;
    }
  }
  // Wall-clock time doesn't exist (spring-forward gap)
  return -1;
}

/**
 * Compute the next occurrence of a cron pattern after `afterMs` (epoch ms).
 * Supports standard 5-field cron: minute hour dayOfMonth month dayOfWeek.
 * When `tz` is provided, the cron expression is evaluated in that IANA timezone.
 * Returns epoch ms of the next matching time (always in UTC).
 */
export function nextCronOccurrence(pattern: string, afterMs: number, tz?: string): number {
  if (tz) {
    return nextCronOccurrenceTz(pattern, afterMs, tz);
  }
  return nextCronOccurrenceUtc(pattern, afterMs);
}

export function computeInitialSchedulerNextRun(
  schedule: Pick<ScheduleOpts, 'pattern' | 'every' | 'repeatAfterComplete' | 'tz'> & {
    startDate?: number;
    endDate?: number;
  },
  now: number,
): number | null {
  if (!schedule.pattern && !schedule.repeatAfterComplete) {
    validateSchedulerEvery(schedule.every);
  }
  let nextRun: number;
  if (schedule.repeatAfterComplete) {
    // First job fires immediately (or at startDate if in the future)
    nextRun = schedule.startDate != null && schedule.startDate > now ? schedule.startDate : now;
  } else if (schedule.pattern) {
    const base = schedule.startDate != null && schedule.startDate > now ? schedule.startDate : now;
    nextRun = nextCronOccurrence(schedule.pattern, base - 1, schedule.tz);
  } else if (schedule.every) {
    if (schedule.startDate == null) {
      nextRun = now + schedule.every;
    } else if (schedule.startDate > now) {
      nextRun = schedule.startDate;
    } else {
      const elapsed = now - schedule.startDate;
      const steps = Math.ceil(elapsed / schedule.every);
      nextRun = schedule.startDate + steps * schedule.every;
    }
  } else {
    throw new Error('Schedule must have pattern (cron), every (ms interval), or repeatAfterComplete (ms)');
  }

  if (schedule.endDate != null && nextRun > schedule.endDate) {
    return null;
  }
  return nextRun;
}

export function computeFollowingSchedulerNextRun(
  schedule: Pick<SchedulerEntry, 'pattern' | 'every' | 'repeatAfterComplete' | 'tz' | 'endDate'>,
  afterMs: number,
): number | null {
  if (
    !schedule.pattern &&
    !isValidSchedulerEvery(schedule.every) &&
    !isValidSchedulerEvery(schedule.repeatAfterComplete)
  ) {
    return null;
  }
  let nextRun: number;
  if (schedule.repeatAfterComplete) {
    nextRun = afterMs + schedule.repeatAfterComplete;
  } else if (schedule.pattern) {
    nextRun = nextCronOccurrence(schedule.pattern, afterMs, schedule.tz);
  } else if (schedule.every) {
    nextRun = afterMs + schedule.every;
  } else {
    return null;
  }

  if (schedule.endDate != null && nextRun > schedule.endDate) {
    return null;
  }
  return nextRun;
}

function nextCronOccurrenceUtc(pattern: string, afterMs: number): number {
  const fields = pattern.trim().split(/\s+/);
  if (fields.length !== 5) {
    throw new Error(`Invalid cron pattern: expected 5 fields, got ${fields.length}`);
  }

  const { set: minuteSet, sorted: minutesSorted } = parseCronField(fields[0], 0, 59);
  const { set: hourSet, sorted: hoursSorted } = parseCronField(fields[1], 0, 23);
  const { set: domSet } = parseCronField(fields[2], 1, 31);
  const { set: monthSet, sorted: monthsSorted } = parseCronField(fields[3], 1, 12);
  const { set: dowSet } = parseCronField(fields[4], 0, 6); // 0=Sunday

  // Start from the next minute after afterMs (all operations in UTC)
  const d = new Date(afterMs);
  d.setUTCSeconds(0, 0);
  d.setUTCMinutes(d.getUTCMinutes() + 1);

  const endYear = d.getUTCFullYear() + MAX_SEARCH_YEARS;

  while (d.getUTCFullYear() <= endYear) {
    // 1. Month check
    const currentMonth = d.getUTCMonth() + 1;
    if (!monthSet.has(currentMonth)) {
      const nextMonth = monthsSorted.find((m) => m > currentMonth);
      if (nextMonth != null) {
        d.setUTCMonth(nextMonth - 1, 1);
        d.setUTCHours(0, 0, 0, 0);
      } else {
        d.setUTCFullYear(d.getUTCFullYear() + 1, monthsSorted[0] - 1, 1);
        d.setUTCHours(0, 0, 0, 0);
      }
      continue;
    }

    // 2. Day check
    const currentDay = d.getUTCDate();
    const currentDayOfWeek = d.getUTCDay();
    if (!domSet.has(currentDay) || !dowSet.has(currentDayOfWeek)) {
      d.setUTCDate(d.getUTCDate() + 1);
      d.setUTCHours(0, 0, 0, 0);
      continue;
    }

    // 3. Hour check
    const currentHour = d.getUTCHours();
    if (!hourSet.has(currentHour)) {
      const nextHour = hoursSorted.find((h) => h > currentHour);
      if (nextHour != null) {
        d.setUTCHours(nextHour, 0, 0, 0);
      } else {
        d.setUTCDate(d.getUTCDate() + 1);
        d.setUTCHours(0, 0, 0, 0);
      }
      continue;
    }

    // 4. Minute check
    const currentMinute = d.getUTCMinutes();
    if (!minuteSet.has(currentMinute)) {
      const nextMinute = minutesSorted.find((m) => m > currentMinute);
      if (nextMinute != null) {
        d.setUTCMinutes(nextMinute, 0, 0);
        return d.getTime();
      } else {
        d.setUTCHours(d.getUTCHours() + 1, 0, 0, 0);
        continue;
      }
    }

    return d.getTime();
  }

  throw new Error(`No cron match found within ${MAX_SEARCH_YEARS} years for pattern: ${pattern}`);
}

/**
 * Timezone-aware cron search. Evaluates the cron expression in wall-clock time
 * of the specified timezone, then converts the result to UTC epoch.
 *
 * DST handling:
 * - Spring-forward: if a candidate wall-clock time doesn't exist (gap), skip to next candidate
 * - Fall-back: if a wall-clock time is ambiguous (overlap), pick the first (earlier) UTC instant
 */
function nextCronOccurrenceTz(pattern: string, afterMs: number, tz: string): number {
  const fields = pattern.trim().split(/\s+/);
  if (fields.length !== 5) {
    throw new Error(`Invalid cron pattern: expected 5 fields, got ${fields.length}`);
  }

  const { set: minuteSet, sorted: minutesSorted } = parseCronField(fields[0], 0, 59);
  const { set: hourSet, sorted: hoursSorted } = parseCronField(fields[1], 0, 23);
  const { set: domSet } = parseCronField(fields[2], 1, 31);
  const { set: monthSet, sorted: monthsSorted } = parseCronField(fields[3], 1, 12);
  const { set: dowSet } = parseCronField(fields[4], 0, 6);

  // Get the wall-clock time in the target timezone for "afterMs + 1 minute"
  const startParts = utcToTzParts(afterMs, tz);
  // Advance by 1 minute in wall-clock time
  let year = startParts.year;
  let month = startParts.month;
  let day = startParts.day;
  let hour = startParts.hour;
  let minute = startParts.minute + 1;

  // Normalize overflow
  if (minute >= 60) {
    minute = 0;
    hour++;
    if (hour >= 24) {
      hour = 0;
      // Advance day via UTC date arithmetic for correctness
      const tmp = new Date(Date.UTC(year, month - 1, day + 1));
      year = tmp.getUTCFullYear();
      month = tmp.getUTCMonth() + 1;
      day = tmp.getUTCDate();
    }
  }

  const endYear = year + MAX_SEARCH_YEARS;

  while (year <= endYear) {
    // 1. Month check
    if (!monthSet.has(month)) {
      const nextMonth = monthsSorted.find((m) => m > month);
      if (nextMonth != null) {
        month = nextMonth;
        day = 1;
        hour = 0;
        minute = 0;
      } else {
        year++;
        month = monthsSorted[0];
        day = 1;
        hour = 0;
        minute = 0;
      }
      continue;
    }

    // 2. Day check - use UTC Date for day-of-week and month-length
    const daysInMonth = new Date(Date.UTC(year, month, 0)).getUTCDate();
    if (day > daysInMonth) {
      // Overflowed month
      month++;
      day = 1;
      hour = 0;
      minute = 0;
      if (month > 12) {
        month = 1;
        year++;
      }
      continue;
    }
    const dow = new Date(Date.UTC(year, month - 1, day)).getUTCDay();
    if (!domSet.has(day) || !dowSet.has(dow)) {
      day++;
      hour = 0;
      minute = 0;
      continue;
    }

    // 3. Hour check
    if (!hourSet.has(hour)) {
      const nextHour = hoursSorted.find((h) => h > hour);
      if (nextHour != null) {
        hour = nextHour;
        minute = 0;
      } else {
        day++;
        hour = 0;
        minute = 0;
      }
      continue;
    }

    // 4. Minute check
    if (!minuteSet.has(minute)) {
      const nextMinute = minutesSorted.find((m) => m > minute);
      if (nextMinute != null) {
        minute = nextMinute;
      } else {
        hour++;
        minute = 0;
        continue;
      }
    }

    // Found a candidate wall-clock time - convert to UTC
    const utcMs = tzPartsToUtc(year, month, day, hour, minute, tz);
    if (utcMs === -1) {
      // Spring-forward gap: this wall-clock time doesn't exist, advance 1 minute
      minute++;
      if (minute >= 60) {
        minute = 0;
        hour++;
        if (hour >= 24) {
          hour = 0;
          day++;
        }
      }
      continue;
    }
    // Ensure the result is strictly after afterMs
    if (utcMs > afterMs) {
      return utcMs;
    }
    // Edge case: the UTC result is not after afterMs (possible during fall-back overlap)
    minute++;
    if (minute >= 60) {
      minute = 0;
      hour++;
      if (hour >= 24) {
        hour = 0;
        day++;
      }
    }
  }

  throw new Error(`No cron match found within ${MAX_SEARCH_YEARS} years for pattern: ${pattern}`);
}

// ---- Subject matching for Broadcast filtering ----

/**
 * Match a dot-separated subject against a pattern.
 * - `*` matches exactly one segment
 * - `>` matches one or more trailing segments (must be the last token)
 * - Literal tokens match exactly
 */
export function matchSubject(pattern: string, subject: string): boolean {
  const patParts = pattern.split('.');
  const subParts = subject.split('.');

  for (let i = 0; i < patParts.length; i++) {
    const token = patParts[i];
    if (token === '>') {
      if (i !== patParts.length - 1) {
        throw new Error('`>` wildcard must be the last token in a subject pattern');
      }
      return i < subParts.length;
    }
    if (i >= subParts.length) return false;
    if (token !== '*' && token !== subParts[i]) return false;
  }
  return patParts.length === subParts.length;
}

/**
 * Compile an array of subject patterns into a single matcher function.
 * Returns a function that returns true if the subject matches any pattern.
 * Returns null if patterns is empty or undefined (no filtering).
 */
export function compileSubjectMatcher(patterns: string[] | undefined): ((subject: string) => boolean) | null {
  if (!patterns || patterns.length === 0) return null;
  if (patterns.length === 1) {
    const p = patterns[0];
    return (subject) => matchSubject(p, subject);
  }
  return (subject) => patterns.some((p) => matchSubject(p, subject));
}
