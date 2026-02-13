const DEFAULT_PREFIX = 'glide';

export function keyPrefix(prefix: string, queueName: string): string {
  return `${prefix}:{${queueName}}`;
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
    deps: (id: string) => `${p}:deps:${id}`,
    parent: (id: string) => `${p}:parent:${id}`,
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
