import { gzipSync, gunzipSync } from 'zlib';
import { randomBytes } from 'crypto';
import type { JobOptions, JobTemplate, JobUsage, ScheduleOpts, SchedulerEntry, Serializer } from './types';

const DEFAULT_PREFIX = 'glide';
export const USAGE_BUCKET_MS = 60_000;
export const USAGE_RETENTION_MS = 30 * 24 * 60 * 60 * 1000;
export const USAGE_RETENTION_SECONDS = Math.ceil(USAGE_RETENTION_MS / 1000);

// 1MB max payload size to prevent DoS
export const MAX_JOB_DATA_SIZE = 1048576;

/** Characters that break Valkey cluster hash-tag routing when used in queue names. */
export const INVALID_QUEUE_NAME_CHARS = /[{}:]/;

/** Characters that are invalid in job IDs. */
export const INVALID_JOB_ID_CHARS = /[\x00-\x1f\x7f{}:]/;

/** Maximum length for ordering keys. */
export const MAX_ORDERING_KEY_LENGTH = 256;

/** Highest accepted job priority (lower numbers run first, 0 means no priority). */
export const MAX_JOB_PRIORITY = 2048;

export const MIN_JOB_LOCK_DURATION_MS = 1000;
export const MAX_JOB_LOCK_DURATION_MS = 86_400_000;

/**
 * Scores encode priority in the high bits, so it must be an integer in
 * [0, MAX_JOB_PRIORITY]; negative or fractional values corrupt the scheduled
 * ZSet ordering and score decoding.
 */
export function validateJobPriority(priority: number): void {
  if (priority > MAX_JOB_PRIORITY) {
    throw new Error(`Priority must be <= ${MAX_JOB_PRIORITY}`);
  }
  if (!Number.isInteger(priority) || priority < 0) {
    throw new Error(`Priority must be an integer between 0 and ${MAX_JOB_PRIORITY}`);
  }
}

export function validateOrderingKey(orderingKey: string): void {
  if (orderingKey.length > MAX_ORDERING_KEY_LENGTH) {
    throw new Error(`Ordering key exceeds maximum length (${orderingKey.length} > ${MAX_ORDERING_KEY_LENGTH}).`);
  }
  if (orderingKey === '__') {
    throw new Error("Ordering key '__' is reserved as an internal sentinel.");
  }
}

/**
 * Validate priority, delay, attempts and backoff. Part of validateJobOptions;
 * FlowProducer and addDAG call it directly for every node before any write.
 */
export function validateJobScheduleOptions(
  opts: Pick<JobOptions, 'priority' | 'delay' | 'attempts' | 'backoff'> | undefined,
): void {
  if (opts?.priority != null) validateJobPriority(opts.priority);
  if (opts?.delay != null && (!Number.isFinite(opts.delay) || opts.delay < 0)) {
    throw new Error('delay must be a non-negative finite number');
  }
  if (opts?.attempts != null && (!Number.isInteger(opts.attempts) || opts.attempts < 0)) {
    throw new Error('attempts must be a non-negative integer');
  }
  if (opts?.backoff != null) {
    const { delay, jitter } = opts.backoff;
    // Custom strategies may ignore delay, so only a provided value is checked.
    if (delay != null && (typeof delay !== 'number' || !Number.isFinite(delay) || delay < 0)) {
      throw new Error('backoff.delay must be a non-negative finite number');
    }
    if (jitter != null && (!Number.isFinite(jitter) || jitter < 0)) {
      throw new Error('backoff.jitter must be a non-negative finite number');
    }
  }
}

/**
 * Validate the per-job options shared by Queue.add/addBulk and Producer:
 * priority, delay, attempts, backoff, token bucket, cost, ordering key, lifo,
 * jobId, ttl and lockDuration.
 * Payload size is checked separately.
 */
export function validateJobOptions(opts: JobOptions | undefined): void {
  validateJobScheduleOptions(opts);
  const tb = opts?.ordering?.tokenBucket;
  if (tb) {
    if (!Number.isFinite(tb.capacity) || tb.capacity <= 0)
      throw new Error('tokenBucket.capacity must be a positive finite number');
    if (!Number.isFinite(tb.refillRate) || tb.refillRate <= 0)
      throw new Error('tokenBucket.refillRate must be a positive finite number');
  }
  if (opts?.cost != null) {
    if (!Number.isFinite(opts.cost) || opts.cost < 0) throw new Error('cost must be a non-negative finite number');
  }
  const orderingKey = opts?.ordering?.key ?? '';
  validateOrderingKey(orderingKey);
  if (opts?.lifo && orderingKey) {
    throw new Error('lifo and ordering.key cannot be used together');
  }
  const customJobId = opts?.jobId ?? '';
  if (customJobId !== '') validateJobId(customJobId);
  if (opts?.ttl != null) {
    if (!Number.isFinite(opts.ttl) || opts.ttl < 0) throw new Error('ttl must be a non-negative finite number');
  }
  if (opts?.lockDuration != null) {
    if (
      !Number.isFinite(opts.lockDuration) ||
      opts.lockDuration < MIN_JOB_LOCK_DURATION_MS ||
      opts.lockDuration > MAX_JOB_LOCK_DURATION_MS
    ) {
      throw new Error(
        `lockDuration must be a finite number between ${MIN_JOB_LOCK_DURATION_MS} and ${MAX_JOB_LOCK_DURATION_MS}`,
      );
    }
  }
}

/** Reject serialized job payloads above MAX_JOB_DATA_SIZE bytes. */
export function validateJobDataSize(serialized: string): void {
  // UTF-8 worst case: 4 bytes per char. Skip Buffer.byteLength for small strings.
  if (serialized.length > MAX_JOB_DATA_SIZE / 4) {
    const byteLen = Buffer.byteLength(serialized, 'utf8');
    if (byteLen > MAX_JOB_DATA_SIZE) {
      throw new Error(
        `Job data exceeds maximum size (${byteLen} bytes > ${MAX_JOB_DATA_SIZE} bytes). Use smaller payloads or store large data externally.`,
      );
    }
  }
}

function validateSchedulerTemplateOpts(opts: JobOptions): void {
  validateJobOptions(opts);
  if (opts.priority != null) validateJobPriority(opts.priority);
  const tb = opts.ordering?.tokenBucket;
  if (opts.ordering?.key && tb) {
    // Same rule as glidemq_addJob: a missing or zero cost counts as 1.
    const cost = opts.cost ? Math.round(opts.cost * 1000) : 1000;
    if (cost > Math.round(tb.capacity * 1000)) throw new Error('Job cost exceeds token bucket capacity');
  }
}

/** Serialize a scheduler template's data the way the tick stores it, enforcing the size limit. */
function serializeSchedulerTemplateData(template: JobTemplate, serializer: Serializer): string {
  const data = template.data !== undefined ? serializer.serialize(template.data) : '{}';
  validateJobDataSize(data);
  return data;
}

/**
 * Validate a scheduler job template at upsert time, so a bad template is
 * rejected once instead of failing or being dropped on every tick.
 */
export function validateSchedulerTemplate(template: JobTemplate | undefined, serializer: Serializer): void {
  if (!template) return;
  const opts = template.opts as JobOptions | undefined;
  try {
    if (opts?.jobId != null) {
      throw new Error(
        'jobId is not supported: every run gets a generated id, a fixed id would drop each run after the first as a duplicate',
      );
    }
    // The tick never applies these, so reject them instead of silently dropping them.
    for (const field of ['delay', 'deduplication', 'parent'] as const) {
      if (opts?.[field] != null) {
        throw new Error(`${field} is not supported: scheduler runs do not apply it`);
      }
    }
    if (opts) validateSchedulerTemplateOpts(opts);
    serializeSchedulerTemplateData(template, serializer);
  } catch (err) {
    throw new Error(`Scheduler template: ${err instanceof Error ? err.message : String(err)}`);
  }
}

/**
 * Tick-time check of a stored template. Returns the serialized job data or
 * throws when the template cannot produce a job. A stored jobId is ignored
 * (never passed to addJob), so it is not rejected here.
 */
export function prepareSchedulerTemplateData(template: JobTemplate, serializer: Serializer): string {
  if (template.opts) validateSchedulerTemplateOpts({ ...(template.opts as JobOptions), jobId: undefined });
  return serializeSchedulerTemplateData(template, serializer);
}

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
  if (name.length > 256) {
    throw new Error('Queue name must be at most 256 characters');
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

/** Encode a retryable cross-queue parent notification without delimiter ambiguity. */
export function encodeCrossQueueParentNotify(parentQueue: string, parentId: string, depsMember: string): string {
  return JSON.stringify([parentQueue, parentId, depsMember]);
}

/** Decode a current JSON or legacy tab-delimited parent notification. */
export function parseCrossQueueParentNotification(member: string): [string, string, string] | undefined {
  try {
    const decoded: unknown = JSON.parse(member);
    if (Array.isArray(decoded) && decoded.length === 3 && decoded.every((part) => typeof part === 'string')) {
      return decoded as [string, string, string];
    }
  } catch {
    const legacy = member.split('\t');
    if (legacy.length === 3) return legacy as [string, string, string];
  }
  return undefined;
}

export function buildKeys(queueName: string, prefix = DEFAULT_PREFIX) {
  const p = keyPrefix(prefix, queueName);
  return {
    name: queueName,
    usageQueues: usageQueuesKey(prefix),
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
    listActiveIds: `${p}:list-active-ids`,
    metricsFailed: `${p}:metrics:failed`,
    group: (key: string) => `${p}:group:${key}`,
    groupq: (key: string) => `${p}:groupq:${key}`,
    parents: (id: string) => `${p}:parents:${id}`,
    jstream: (id: string) => `${p}:jstream:${id}`,
    xqPending: `${p}:xq-pending`,
    worker: (id: string) => `${p}:w:${id}`,
    suspended: `${p}:suspended`,
    signals: (id: string) => `${p}:signals:${id}`,
    budget: (flowId: string) => `${p}:budget:${flowId}`,
    tpm: `${p}:tpm`,
    usageBucket: (bucketTs: number) => `${p}:usage:${bucketTs}`,
  };
}

export function usageQueuesKey(prefix = DEFAULT_PREFIX): string {
  return `${prefix}:usage:queues`;
}

export function floorUsageBucket(timestampMs: number): number {
  return Math.floor(timestampMs / USAGE_BUCKET_MS) * USAGE_BUCKET_MS;
}

// Priority encoding: (priority * 2^42) + timestamp_ms
// Lower priority numbers sort first; 0 means no priority. Within same priority, FIFO by timestamp.
const PRIORITY_SHIFT = 2 ** 42;

export function encodeScore(priority: number, timestampMs: number): number {
  validateJobPriority(priority);
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
 * Returns null if the array is empty, falsy (key does not exist) or not an array.
 */
export function hashDataToRecord(
  hashData: { field?: unknown; key?: unknown; value: unknown }[] | null,
): Record<string, string> | null {
  // Non-array input is a per-command batch error (e.g. WRONGTYPE): treat as missing.
  if (!Array.isArray(hashData) || hashData.length === 0) return null;
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
  'suspendReason',
  'suspendedAt',
  'suspendTimeout',
  'signals',
  'tpmTokens',
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
  for (const [_entryId, fieldPairs] of Object.entries(entries)) {
    for (let i = 0; i < fieldPairs.length; i++) {
      const field = fieldPairs[i][0];
      const value = fieldPairs[i][1];
      if (String(field) === 'jobId') {
        jobIds.push(String(value));
        break; // Stop parsing remaining large fields once we found jobId
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
  /** Receives the pending retry timer (or null once it fired) so close() can clear it. */
  setRetryTimer?(timer: ReturnType<typeof setTimeout> | null): void;
}

/**
 * Attempt a reconnect operation with exponential backoff.
 * On success, calls resumeFn. The backoff is left for the caller to reset
 * after its first successful operation, so an error that survives reconnects
 * keeps growing the delay instead of retrying at the minimum.
 * On failure, emits error, bumps backoff, and schedules a retry.
 * reconnectFn must dispose what it created and throw when the owner closes
 * during one of its awaits; resumeFn only runs while the owner is active.
 */
export async function reconnectWithBackoff(
  ctx: ReconnectContext,
  reconnectFn: () => Promise<void>,
  resumeFn: () => void,
): Promise<void> {
  if (!ctx.isActive()) return;

  try {
    await reconnectFn();
    if (!ctx.isActive()) return;
    resumeFn();
  } catch (err) {
    if (!ctx.isActive()) return;
    ctx.onError(err);
    const delay = nextReconnectDelay(ctx.getBackoff());
    ctx.setBackoff(delay);
    const timer = setTimeout(() => {
      ctx.setRetryTimer?.(null);
      void reconnectWithBackoff(ctx, reconnectFn, resumeFn);
    }, delay);
    ctx.setRetryTimer?.(timer);
  }
}

// ---- Cron parser ----
// Format: [second] minute hour dayOfMonth month dayOfWeek (5 or 6 fields, seconds default to 0)
// Supports: *, ?, numbers, names (JAN-DEC, SUN-SAT), ranges (1-5), steps (*/5, 5/15, 1-30/10), lists (1,3,5)
// Day modifiers: L and LW and <n>W in day-of-month, <d>L and <d>#<n> in day-of-week

interface CronField {
  set: Set<number>;
  sorted: number[];
}

interface CronFieldSpec {
  min: number;
  max: number;
  /** Case-insensitive names accepted in place of numbers. */
  names?: Record<string, number>;
  /** '?' is accepted as a synonym for '*'. */
  allowQuestion?: boolean;
}

const MONTH_NAMES: Record<string, number> = {
  JAN: 1,
  FEB: 2,
  MAR: 3,
  APR: 4,
  MAY: 5,
  JUN: 6,
  JUL: 7,
  AUG: 8,
  SEP: 9,
  OCT: 10,
  NOV: 11,
  DEC: 12,
};

const DOW_NAMES: Record<string, number> = { SUN: 0, MON: 1, TUE: 2, WED: 3, THU: 4, FRI: 5, SAT: 6 };

const CRON_SECOND: CronFieldSpec = { min: 0, max: 59 };
const CRON_MINUTE: CronFieldSpec = { min: 0, max: 59 };
const CRON_HOUR: CronFieldSpec = { min: 0, max: 23 };
const CRON_DOM: CronFieldSpec = { min: 1, max: 31, allowQuestion: true };
const CRON_MONTH: CronFieldSpec = { min: 1, max: 12, names: MONTH_NAMES };
// 7 is accepted as a second Sunday and folded onto 0 after parsing.
const CRON_DOW: CronFieldSpec = { min: 0, max: 7, names: DOW_NAMES, allowQuestion: true };

/** Parse one value of a field: a number or a name. Returns undefined for anything else. */
function parseCronValue(token: string, spec: CronFieldSpec): number | undefined {
  if (/^\d+$/.test(token)) return parseInt(token, 10);
  return spec.names?.[token.toUpperCase()];
}

/**
 * Parse one cron field into its set of values. `special` consumes tokens the
 * generic grammar does not cover (L, W, #) and returns true when it did.
 */
function parseCronField(field: string, spec: CronFieldSpec, special?: (token: string) => boolean): CronField {
  const values: Set<number> = new Set();
  const { min, max } = spec;

  const addRange = (from: number, to: number, step: number) => {
    if (from < min || to > max) {
      throw new Error(`Cron range out of bounds: ${from}-${to}`);
    }
    if (from > to) {
      throw new Error(`Cron range reversed: ${from}-${to}`);
    }
    for (let i = from; i <= to; i += step) values.add(i);
  };

  for (const part of field.split(',')) {
    const trimmed = part.trim();

    if (trimmed === '*' || (spec.allowQuestion && trimmed === '?')) {
      addRange(min, max, 1);
      continue;
    }
    if (special?.(trimmed)) continue;

    // Split once on '/', then once on '-'. More separators are malformed.
    const slash = trimmed.split('/');
    if (slash.length > 2 || slash.some((s) => s.length === 0)) {
      throw new Error(`Invalid cron token: ${trimmed}`);
    }
    const [rangeText, stepText] = slash;
    let step = 1;
    if (stepText !== undefined) {
      if (!/^\d+$/.test(stepText)) {
        throw new Error(`Invalid cron token: ${trimmed}`);
      }
      step = parseInt(stepText, 10);
      if (step <= 0) {
        throw new Error(`Invalid cron step: ${step}`);
      }
    }

    if (rangeText === '*') {
      addRange(min, max, step);
      continue;
    }

    const dash = rangeText.split('-');
    if (dash.length > 2 || dash.some((s) => s.length === 0)) {
      throw new Error(`Invalid cron token: ${trimmed}`);
    }
    const from = parseCronValue(dash[0], spec);
    if (from === undefined) {
      throw new Error(`Invalid cron token: ${trimmed}`);
    }
    if (dash.length === 2) {
      const to = parseCronValue(dash[1], spec);
      if (to === undefined) {
        throw new Error(`Invalid cron token: ${trimmed}`);
      }
      addRange(from, to, step);
      continue;
    }
    if (stepText !== undefined) {
      // 'N/step' runs from N to the end of the field, as in cron-parser and Quartz.
      addRange(from, max, step);
      continue;
    }
    if (from < min || from > max) {
      throw new Error(`Cron value out of bounds: ${from}`);
    }
    values.add(from);
  }

  const sorted = [...values].sort((a, b) => a - b);
  return { set: values, sorted };
}

/** Quartz-style day modifiers. Each one adds days to its field's match. */
interface CronDayModifiers {
  /** 'L' in day-of-month: the last day of the month. */
  domLast: boolean;
  /** 'LW' in day-of-month: the last weekday (Mon-Fri) of the month. */
  domLastWeekday: boolean;
  /** '<n>W' in day-of-month: the weekday nearest to day n, inside the same month. */
  domNearestWeekday: number[];
  /** '<d>L' in day-of-week: the last such weekday of the month. */
  dowLast: Set<number>;
  /** '<d>#<n>' in day-of-week: the nth such weekday of the month, n in 1-5. */
  dowNth: Array<[dow: number, nth: number]>;
}

function checkCronBound(value: number, min: number, max: number): number {
  if (value < min || value > max) {
    throw new Error(`Cron value out of bounds: ${value}`);
  }
  return value;
}

function parseDomModifier(mods: CronDayModifiers, token: string): boolean {
  const upper = token.toUpperCase();
  if (upper === 'L') {
    mods.domLast = true;
    return true;
  }
  if (upper === 'LW') {
    mods.domLastWeekday = true;
    return true;
  }
  const nearest = upper.match(/^(\d+)W$/);
  if (nearest) {
    mods.domNearestWeekday.push(checkCronBound(parseInt(nearest[1], 10), CRON_DOM.min, CRON_DOM.max));
    return true;
  }
  return false;
}

function parseDowModifier(mods: CronDayModifiers, token: string): boolean {
  const last = token.match(/^(.+)L$/i);
  if (last) {
    const dow = parseCronValue(last[1], CRON_DOW);
    if (dow === undefined) return false;
    mods.dowLast.add(checkCronBound(dow, CRON_DOW.min, CRON_DOW.max) % 7);
    return true;
  }
  const nth = token.match(/^(.+)#(\d+)$/);
  if (nth) {
    const dow = parseCronValue(nth[1], CRON_DOW);
    if (dow === undefined) return false;
    mods.dowNth.push([checkCronBound(dow, CRON_DOW.min, CRON_DOW.max) % 7, checkCronBound(parseInt(nth[2], 10), 1, 5)]);
    return true;
  }
  return false;
}

interface CronPattern extends CronDayModifiers {
  second: CronField;
  minute: CronField;
  hour: CronField;
  dom: CronField;
  month: CronField;
  dow: CronField;
  /** Six fields were given: the search runs at second granularity. */
  hasSeconds: boolean;
  /** Vixie cron wildcard rule: the minute or hour field contains '*'. Decides the DST behavior. */
  wildcardTime: boolean;
  /** Day-of-month field covers every day 1-31 ('*', '?', '*\/1', '1-31'). */
  domUnrestricted: boolean;
  /** Day-of-week field is '*', '?' or '*\/1'. An explicit '0-6' counts as restricted, as in cron-parser and vixie cron. */
  dowUnrestricted: boolean;
}

function parseCronPattern(pattern: string): CronPattern {
  const fields = pattern.trim().split(/\s+/);
  if (fields.length !== 5 && fields.length !== 6) {
    throw new Error(`Invalid cron pattern: expected 5 or 6 fields, got ${fields.length}`);
  }
  const hasSeconds = fields.length === 6;
  const [secondText, minuteText, hourText, domText, monthText, dowText] = hasSeconds ? fields : ['0', ...fields];
  const mods: CronDayModifiers = {
    domLast: false,
    domLastWeekday: false,
    domNearestWeekday: [],
    dowLast: new Set(),
    dowNth: [],
  };
  const dom = parseCronField(domText, CRON_DOM, (token) => parseDomModifier(mods, token));
  const dow = parseCronField(dowText, CRON_DOW, (token) => parseDowModifier(mods, token));
  if (dow.set.delete(7)) {
    dow.set.add(0);
    dow.sorted = [...dow.set].sort((a, b) => a - b);
  }
  return {
    ...mods,
    second: parseCronField(secondText, CRON_SECOND),
    minute: parseCronField(minuteText, CRON_MINUTE),
    hour: parseCronField(hourText, CRON_HOUR),
    dom,
    month: parseCronField(monthText, CRON_MONTH),
    dow,
    hasSeconds,
    wildcardTime: minuteText.includes('*') || hourText.includes('*'),
    domUnrestricted: dom.set.size === 31,
    dowUnrestricted: dow.set.size === 7 && /[*?]/.test(dowText),
  };
}

function daysInMonth(year: number, month: number): number {
  return new Date(Date.UTC(year, month, 0)).getUTCDate();
}

/** Quartz 'W': the weekday (Mon-Fri) nearest to day n without leaving the month. */
function nearestWeekday(n: number, dowOfN: number, monthLength: number): number {
  if (dowOfN >= 1 && dowOfN <= 5) return n;
  if (dowOfN === 6) return n > 1 ? n - 1 : n + 2;
  return n < monthLength ? n + 1 : n - 2;
}

function cronDomMatches(cron: CronPattern, day: number, dayOfWeek: number, monthLength: number): boolean {
  if (cron.dom.set.has(day)) return true;
  if (cron.domLast && day === monthLength) return true;
  // Day of week of another day n in the same month.
  const dowOf = (n: number) => (((dayOfWeek + n - day) % 7) + 7) % 7;
  if (cron.domLastWeekday && day === nearestWeekday(monthLength, dowOf(monthLength), monthLength)) return true;
  return cron.domNearestWeekday.some((n) => n <= monthLength && nearestWeekday(n, dowOf(n), monthLength) === day);
}

function cronDowMatches(cron: CronPattern, day: number, dayOfWeek: number, monthLength: number): boolean {
  if (cron.dow.set.has(dayOfWeek)) return true;
  if (cron.dowLast.has(dayOfWeek) && day + 7 > monthLength) return true;
  const nth = Math.ceil(day / 7);
  return cron.dowNth.some(([d, n]) => d === dayOfWeek && n === nth);
}

/**
 * Standard cron day rule: when both day-of-month and day-of-week are restricted,
 * a day matches if EITHER field matches. Otherwise both must match (the
 * unrestricted one always does).
 */
function cronDayMatches(cron: CronPattern, day: number, dayOfWeek: number, monthLength: number): boolean {
  const domMatch = cronDomMatches(cron, day, dayOfWeek, monthLength);
  const dowMatch = cronDowMatches(cron, day, dayOfWeek, monthLength);
  if (!cron.domUnrestricted && !cron.dowUnrestricted) return domMatch || dowMatch;
  return domMatch && dowMatch;
}

// Maximum search horizon in years to prevent infinite loops (e.g. Feb 30)
// 10 years covers century non-leap-year gaps (e.g. Feb 29 after 2097 -> 2104)
const MAX_SEARCH_YEARS = 10;
// Every walker step advances at least one second or skips a whole field, so a
// match inside the horizon takes far fewer steps than this. A guard against bugs.
const MAX_SEARCH_STEPS = 200_000;

const validTzCache = new Set<string>();

/**
 * Validate an IANA timezone string. Throws if invalid.
 * Results are memoized to avoid repeated Intl.DateTimeFormat construction.
 */
export function validateTimezone(tz: string): void {
  if (validTzCache.has(tz)) return;
  try {
    Intl.DateTimeFormat('en-US', { timeZone: tz });
  } catch {
    throw new Error(`Invalid timezone: ${tz}`);
  }
  validTzCache.add(tz);
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

/** A wall-clock time in some timezone (or UTC). */
interface Wall {
  year: number;
  month: number; // 1-12
  day: number;
  hour: number;
  minute: number;
  second: number;
}

interface TzParts extends Wall {
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
  const second = parseInt(p.second, 10);
  // Compute day of week from a UTC date constructed from these parts
  const dow = new Date(Date.UTC(year, month - 1, day)).getUTCDay();
  return { year, month, day, hour, minute, second, dayOfWeek: dow };
}

/** Wall-clock components of a UTC instant. */
function utcToWall(epochMs: number): Wall {
  const d = new Date(epochMs);
  return {
    year: d.getUTCFullYear(),
    month: d.getUTCMonth() + 1,
    day: d.getUTCDate(),
    hour: d.getUTCHours(),
    minute: d.getUTCMinutes(),
    second: d.getUTCSeconds(),
  };
}

function wallToNaiveUtc(w: Wall): number {
  return Date.UTC(w.year, w.month - 1, w.day, w.hour, w.minute, w.second, 0);
}

/** The wall time one second later (calendar arithmetic through UTC dates). */
function wallPlusOneSecond(w: Wall): Wall {
  return utcToWall(wallToNaiveUtc(w) + 1000);
}

const MINUTE_MS = 60_000;
// Upper bound on a DST shift and on the distance searched around one.
const DST_WINDOW_MS = 3 * 60 * 60 * 1000;

/** Offset of `tz` from UTC (wall clock minus UTC, ms) at the given instant. */
function tzOffsetMs(epochMs: number, tz: string): number {
  const floored = Math.floor(epochMs / MINUTE_MS) * MINUTE_MS;
  const p = utcToTzParts(floored, tz);
  return Date.UTC(p.year, p.month - 1, p.day, p.hour, p.minute) - floored;
}

/**
 * All UTC instants whose wall clock in `tz` is the given minute, ascending.
 * Empty for a time skipped by spring-forward, two entries for a time repeated
 * by fall-back.
 */
function tzWallToInstants(w: Wall, tz: string): number[] {
  const naive = wallToNaiveUtc(w);
  const guess = tzOffsetMs(naive, tz);
  const offsets = new Set([
    guess,
    tzOffsetMs(naive - guess - DST_WINDOW_MS, tz),
    tzOffsetMs(naive - guess, tz),
    tzOffsetMs(naive - guess + DST_WINDOW_MS, tz),
  ]);
  const instants: number[] = [];
  for (const offset of offsets) {
    const candidate = naive - offset;
    if (tzOffsetMs(candidate, tz) === offset && !instants.includes(candidate)) instants.push(candidate);
  }
  return instants.sort((a, b) => a - b);
}

/**
 * First instant after the spring-forward gap that skips the given wall time.
 * Only valid when tzWallToInstants returned no instants for it.
 */
function tzGapEnd(w: Wall, tz: string): number {
  // Transitions are minute-aligned; drop the seconds so the bisection lands on the transition itself.
  const naive = Math.floor(wallToNaiveUtc(w) / MINUTE_MS) * MINUTE_MS;
  const guess = tzOffsetMs(naive, tz);
  const offsetBefore = tzOffsetMs(naive - guess - DST_WINDOW_MS, tz);
  const offsetAfter = tzOffsetMs(naive - guess + DST_WINDOW_MS, tz);
  // naive - offsetAfter is still before the transition, naive - offsetBefore is after it.
  let lo = naive - offsetAfter;
  let hi = naive - offsetBefore;
  while (hi - lo > MINUTE_MS) {
    const mid = lo + Math.floor((hi - lo) / 2 / MINUTE_MS) * MINUTE_MS;
    if (tzOffsetMs(mid, tz) === offsetBefore) lo = mid;
    else hi = mid;
  }
  return hi;
}

function cronWallMatches(cron: CronPattern, p: TzParts): boolean {
  return (
    cron.month.set.has(p.month) &&
    cronDayMatches(cron, p.day, p.dayOfWeek, daysInMonth(p.year, p.month)) &&
    cron.hour.set.has(p.hour) &&
    cron.minute.set.has(p.minute) &&
    cron.second.set.has(p.second)
  );
}

/**
 * Compute the next occurrence of a cron pattern after `afterMs` (epoch ms).
 * Supports standard 5-field cron: minute hour dayOfMonth month dayOfWeek, with
 * `*`, numbers, ranges, steps and lists. No seconds field, no names, and
 * dayOfWeek is 0-6 (7 is rejected). When both day fields are restricted, a day
 * matching either one matches.
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

/**
 * First run after switching an every/pattern scheduler to repeatAfterComplete.
 * repeatAfterComplete would fire at once, possibly next to a run of the old
 * mode that is still active, so hold it to the old mode's nextRun.
 */
export function holdSchedulerModeSwitch(
  existing: SchedulerEntry,
  nextRun: number,
  endDate: number | undefined,
): number | null {
  if (isValidSchedulerEvery(existing.repeatAfterComplete) || !(existing.nextRun > 0)) return nextRun;
  if (!existing.pattern && !isValidSchedulerEvery(existing.every)) return nextRun;
  const held = Math.max(nextRun, existing.nextRun);
  return endDate != null && held > endDate ? null : held;
}

export function computeFollowingSchedulerNextRun(
  schedule: Pick<SchedulerEntry, 'pattern' | 'every' | 'repeatAfterComplete' | 'tz' | 'endDate'> &
    Partial<Pick<SchedulerEntry, 'nextRun'>>,
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
    if (schedule.nextRun != null && schedule.nextRun <= afterMs) {
      const missedIntervals = Math.floor((afterMs - schedule.nextRun) / schedule.every) + 1;
      nextRun = schedule.nextRun + missedIntervals * schedule.every;
    } else {
      nextRun = schedule.nextRun ?? afterMs + schedule.every;
    }
  } else {
    return null;
  }

  if (schedule.endDate != null && nextRun > schedule.endDate) {
    return null;
  }
  return nextRun;
}

function nextCronOccurrenceUtc(pattern: string, afterMs: number): number {
  const cron = parseCronPattern(pattern);
  const start = utcToWall(Math.floor(afterMs / 1000) * 1000 + 1000);
  return searchCronWall(cron, start, pattern, wallToNaiveUtc);
}

/**
 * Walk wall-clock time from `start` upward, field by field, and hand every
 * time the pattern matches to `resolve`, which turns it into an instant or
 * returns undefined to keep walking. Each step either skips to the next
 * matching value of one field or advances one second, so the walk is
 * O(fields) per step and bounded by MAX_SEARCH_YEARS / MAX_SEARCH_STEPS.
 */
function searchCronWall(
  cron: CronPattern,
  start: Wall,
  pattern: string,
  resolve: (w: Wall) => number | undefined,
): number {
  const { set: secondSet, sorted: secondsSorted } = cron.second;
  const { set: minuteSet, sorted: minutesSorted } = cron.minute;
  const { set: hourSet, sorted: hoursSorted } = cron.hour;
  const { set: monthSet, sorted: monthsSorted } = cron.month;

  let { year, month, day, hour, minute, second } = start;
  const endYear = year + MAX_SEARCH_YEARS;

  for (let steps = 0; year <= endYear && steps < MAX_SEARCH_STEPS; steps++) {
    // 1. Month check
    if (!monthSet.has(month)) {
      const nextMonth = monthsSorted.find((m) => m > month);
      if (nextMonth != null) {
        month = nextMonth;
      } else {
        year++;
        month = monthsSorted[0];
      }
      day = 1;
      hour = minute = second = 0;
      continue;
    }

    // 2. Day check - UTC dates give month length and day of week
    const monthLength = daysInMonth(year, month);
    if (day > monthLength) {
      month++;
      if (month > 12) {
        month = 1;
        year++;
      }
      day = 1;
      hour = minute = second = 0;
      continue;
    }
    const dow = new Date(Date.UTC(year, month - 1, day)).getUTCDay();
    if (!cronDayMatches(cron, day, dow, monthLength)) {
      day++;
      hour = minute = second = 0;
      continue;
    }

    // 3. Hour check
    if (!hourSet.has(hour)) {
      const nextHour = hoursSorted.find((h) => h > hour);
      if (nextHour != null) {
        hour = nextHour;
      } else {
        day++;
        hour = 0;
      }
      minute = second = 0;
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
      }
      second = 0;
      continue;
    }

    // 5. Second check
    if (!secondSet.has(second)) {
      const nextSecond = secondsSorted.find((sec) => sec > second);
      if (nextSecond != null) {
        second = nextSecond;
      } else {
        minute++;
        second = 0;
        continue;
      }
    }

    const found = resolve({ year, month, day, hour, minute, second });
    if (found !== undefined) return found;
    ({ year, month, day, hour, minute, second } = wallPlusOneSecond({ year, month, day, hour, minute, second }));
  }

  throw new Error(`No cron match found within ${MAX_SEARCH_YEARS} years for pattern: ${pattern}`);
}

/**
 * Timezone-aware cron search. Evaluates the cron expression in wall-clock time
 * of the specified timezone, then converts the result to UTC epoch.
 *
 * DST handling follows vixie cron / cronie. A pattern whose minute or hour
 * field contains '*' (e.g. '*\/15 * * * *', '0 * * * *') is a wildcard
 * pattern; any other pattern is a fixed-time pattern.
 * - Fall-back (a wall-clock hour repeats): wildcard patterns fire in both
 *   instances of the repeated hour; fixed-time patterns fire once, at the
 *   earlier instant.
 * - Spring-forward (a wall-clock hour is skipped): wildcard patterns skip the
 *   missing times; fixed-time patterns due inside the gap fire once at the
 *   first instant after it (02:30 in America/New_York runs at 03:00 EDT).
 */
function nextCronOccurrenceTz(pattern: string, afterMs: number, tz: string): number {
  const cron = parseCronPattern(pattern);
  const { wildcardTime } = cron;

  if (wildcardTime) {
    // A fall-back transition within the next few hours means wall-clock order
    // and instant order diverge: the second instance of a wall time already
    // passed comes later than afterMs. Scan instants directly across it.
    const offsetNow = tzOffsetMs(afterMs, tz);
    const offsetLater = tzOffsetMs(afterMs + DST_WINDOW_MS, tz);
    if (offsetLater < offsetNow) {
      const repeatMs = offsetNow - offsetLater;
      const scanEnd = afterMs + repeatMs;
      const stepMs = cron.hasSeconds ? 1000 : MINUTE_MS;
      for (let t = Math.floor(afterMs / stepMs) * stepMs + stepMs; t <= scanEnd; t += stepMs) {
        if (cronWallMatches(cron, utcToTzParts(t, tz))) return t;
      }
    }
  }

  // Start one second after afterMs in wall-clock time.
  const start = wallPlusOneSecond(utcToTzParts(afterMs, tz));

  // A skipped wall time has no instant: fixed-time patterns run at the end of
  // the gap instead. A repeated one has two and only wildcard patterns use the
  // second.
  return searchCronWall(cron, start, pattern, (w) => {
    const instants = tzWallToInstants(w, tz);
    let eligible: number[];
    if (instants.length === 0) {
      eligible = wildcardTime ? [] : [tzGapEnd(w, tz)];
    } else {
      eligible = wildcardTime ? instants : instants.slice(0, 1);
    }
    return eligible.find((t) => t > afterMs);
  });
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

/**
 * Parse a JSON string as a Record<string, number>. Returns undefined on failure
 * or if the result is not a non-null object.
 */
export function parseJsonRecord(raw: string): Record<string, number> | undefined {
  try {
    const p = JSON.parse(raw);
    if (p && typeof p === 'object') return p;
  } catch {
    // ignore
  }
  return undefined;
}

/**
 * Compute weighted total tokens from per-category token counts and weight multipliers.
 * When no per-category tokens yield a weighted sum, falls back to rawTotal.
 */
export function computeWeightedTotal(
  tokens: Record<string, number>,
  weights: Record<string, number>,
  rawTotal: number,
): number {
  let weighted = 0;
  for (const [cat, val] of Object.entries(tokens)) {
    const w = weights[cat] ?? 1;
    weighted += val * (Number.isFinite(w) && w >= 0 ? w : 1);
  }
  if (weighted === 0 && rawTotal > 0) {
    weighted = rawTotal;
  }
  return weighted;
}

/** Validate and resolve a JobUsage object: checks numeric constraints and auto-computes totals. */
export function validateAndResolveUsage(usage: JobUsage): JobUsage {
  if (usage.tokens) {
    for (const [key, val] of Object.entries(usage.tokens)) {
      if (!Number.isFinite(val) || val < 0) {
        throw new Error(`Token count for '${key}' must be a finite non-negative number`);
      }
    }
  }
  if (usage.totalTokens !== undefined && (!Number.isFinite(usage.totalTokens) || usage.totalTokens < 0)) {
    throw new Error('totalTokens must be a finite non-negative number');
  }
  if (usage.costs) {
    for (const [key, val] of Object.entries(usage.costs)) {
      if (!Number.isFinite(val) || val < 0) {
        throw new Error(`Cost for '${key}' must be a finite non-negative number`);
      }
    }
  }
  if (usage.totalCost !== undefined && (!Number.isFinite(usage.totalCost) || usage.totalCost < 0)) {
    throw new Error('totalCost must be a finite non-negative number');
  }

  const resolved: JobUsage = { ...usage };
  if (resolved.totalTokens === undefined && resolved.tokens && Object.keys(resolved.tokens).length > 0) {
    resolved.totalTokens = Object.values(resolved.tokens).reduce((sum, v) => sum + v, 0);
  }
  if (resolved.totalCost === undefined && resolved.costs && Object.keys(resolved.costs).length > 0) {
    resolved.totalCost = Object.values(resolved.costs).reduce((sum, v) => sum + v, 0);
  }
  return resolved;
}

/**
 * Strip C0/C1 controls and Unicode line separators, then cap length so
 * caller-supplied strings cannot forge log lines (CodeQL js/log-injection,
 * js/tainted-format-string).
 */
export function sanitizeForLog(value: unknown, max = 200): string {
  const s = String(value).replace(/[\u0000-\u001f\u007f-\u009f\u2028\u2029]/g, '');
  return s.length > max ? `${s.slice(0, max)}…` : s;
}
