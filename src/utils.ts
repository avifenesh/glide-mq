const DEFAULT_PREFIX = 'glide';

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

export function keys(prefix: string, queueName: string) {
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
    job: (id: string) => `${p}:job:${id}`,
    log: (id: string) => `${p}:log:${id}`,
    deps: (id: string) => `${p}:deps:${id}`,
  };
}

export function buildKeys(queueName: string, prefix = DEFAULT_PREFIX) {
  return keys(prefix, queueName);
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

export function calculateBackoff(
  type: string,
  delay: number,
  attemptsMade: number,
  jitter = 0,
): number {
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
  return `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
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
      let start = min;
      let end = max;
      if (stepMatch[2] !== undefined) {
        start = parseInt(stepMatch[2], 10);
        end = parseInt(stepMatch[3], 10);
      }
      for (let i = start; i <= end; i += step) values.add(i);
      continue;
    }

    const rangeMatch = trimmed.match(/^(\d+)-(\d+)$/);
    if (rangeMatch) {
      const from = parseInt(rangeMatch[1], 10);
      const to = parseInt(rangeMatch[2], 10);
      for (let i = from; i <= to; i++) values.add(i);
      continue;
    }

    const num = parseInt(trimmed, 10);
    if (!isNaN(num)) {
      values.add(num);
    }
  }

  return [...values].sort((a, b) => a - b);
}

// Maximum iterations when searching for next cron match (1 year of minutes)
const MAX_CRON_ITERATIONS = 525960;

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

  // Start from the next minute after afterMs
  const start = new Date(afterMs);
  start.setSeconds(0, 0);
  start.setMinutes(start.getMinutes() + 1);

  const d = new Date(start);
  let iterations = 0;

  while (iterations < MAX_CRON_ITERATIONS) {
    if (
      months.includes(d.getMonth() + 1) &&
      daysOfMonth.includes(d.getDate()) &&
      daysOfWeek.includes(d.getDay()) &&
      hours.includes(d.getHours()) &&
      minutes.includes(d.getMinutes())
    ) {
      return d.getTime();
    }

    // Advance by 1 minute
    d.setMinutes(d.getMinutes() + 1);
    iterations++;
  }

  throw new Error(`No cron match found within ${MAX_CRON_ITERATIONS} iterations for pattern: ${pattern}`);
}
