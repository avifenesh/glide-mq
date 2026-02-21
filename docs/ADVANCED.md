# Advanced Features

## Table of Contents

- [Job Schedulers (Repeatable / Cron Jobs)](#job-schedulers)
- [Sequential Processing (Per-Key Ordering)](#sequential-processing)
- [Deduplication](#deduplication)
- [Global Concurrency](#global-concurrency)
- [Global Rate Limiting](#global-rate-limiting)
- [Job Revocation (Cooperative Cancellation)](#job-revocation)
- [Transparent Compression](#transparent-compression)
- [Retries and Backoff](#retries-and-backoff)
- [Dead Letter Queues](#dead-letter-queues)

---

## Job Schedulers

Use `upsertJobScheduler` to define repeatable jobs driven by a cron expression or a fixed interval. Schedulers survive worker restarts — the next run time is stored in Valkey.

```typescript
const queue = new Queue('tasks', { connection });

// Cron: run "daily-report" every day at 08:00 UTC
await queue.upsertJobScheduler(
  'daily-report',
  { pattern: '0 8 * * *' },
  { name: 'generate-report', data: { type: 'daily' } },
);

// Interval: run "cleanup" every 5 minutes
await queue.upsertJobScheduler(
  'cleanup',
  { every: 5 * 60 * 1_000 },  // ms
  { name: 'cleanup-old-records', data: {} },
);

// List all registered schedulers
const schedulers = await queue.getRepeatableJobs();

// Remove a scheduler (does not cancel jobs already in flight)
await queue.removeJobScheduler('cleanup');
```

The internal `Scheduler` class fires a promotion loop that converts due scheduler entries into real jobs, then re-registers the next occurrence.

---

## Ordering and Group Concurrency

### Sequential processing (concurrency=1)

Add `ordering.key` to a job to guarantee that all jobs with the same key are processed one at a time, in the order they were added.

```typescript
// All jobs with ordering.key = 'user:42' are processed sequentially
await queue.add('process-payment', { userId: 42, amount: 100 }, {
  ordering: { key: 'user:42' },
});
await queue.add('send-receipt', { userId: 42 }, {
  ordering: { key: 'user:42' },
});
```

### Group concurrency (concurrency > 1)

Set `ordering.concurrency` to allow up to N jobs per key to run in parallel across all workers:

```typescript
// Max 3 concurrent jobs for tenant-42, regardless of worker count
await queue.add('process', data, {
  ordering: { key: 'tenant-42', concurrency: 3 },
});
```

Jobs exceeding the group limit are parked in a per-group wait list and automatically released when a slot opens.

```typescript
// Multi-tenant isolation: each client gets max 2 concurrent jobs
for (const job of jobs) {
  await queue.add('task', job.data, {
    ordering: { key: `client-${job.clientId}`, concurrency: 2 },
  });
}
```

### Per-group rate limiting

Limit how many jobs per ordering key can start within a time window, independent of concurrency:

```typescript
// Max 10 jobs per 60 seconds for each tenant
await queue.add('sync', data, {
  ordering: {
    key: `tenant-${tenantId}`,
    concurrency: 3,
    rateLimit: { max: 10, duration: 60_000 },
  },
});
```

When both `concurrency` and `rateLimit` are set, both gates apply - a job must have a free concurrency slot *and* remaining rate capacity to start. Jobs that hit the rate limit are parked in a scheduler-managed promotion queue and released when the window resets.

- **Promotion latency**: rate-limited jobs are promoted by the scheduler loop. Worst-case latency is one `promotionInterval` (default 5 s). Lower `promotionInterval` on the worker if tighter latency is needed.
- **Retried jobs consume rate slots** - a retried job counts against the rate window like any new job.

### Notes

- Jobs with different ordering keys (or no ordering key) are processed concurrently as normal.
- Ordering keys are limited to 256 characters.
- `concurrency=1` (or omitted) preserves strict FIFO ordering per key.
- `concurrency > 1` caps parallelism but does not guarantee FIFO within the group.
- Group concurrency and global concurrency (`setGlobalConcurrency`) compose: both limits are enforced.
- Per-group rate limiting, group concurrency, and global concurrency all compose: all applicable limits are enforced.
- Group slots are released on job complete, fail, retry, DLQ move, and stall recovery.

---

## Deduplication

Prevent duplicate jobs from entering the queue using `deduplication.id`. Three modes are supported:

| Mode | Behaviour |
|------|-----------|
| `simple` | Skip the new job if any job with the same ID already exists (any state). |
| `throttle` | Accept only the first job in a TTL window; later arrivals are dropped. |
| `debounce` | Accept only the last job in a TTL window; earlier arrivals are cancelled. |

```typescript
// Simple: skip if a job with this ID is already queued / active / completed
await queue.add('send-welcome', { userId: 99 }, {
  deduplication: { id: 'welcome-99', mode: 'simple' },
});

// Throttle: at most one "sync" job per 10 s
await queue.add('sync', { region: 'eu' }, {
  deduplication: { id: 'sync-eu', mode: 'throttle', ttl: 10_000 },
});

// Debounce: only the last "search" job within 500 ms is actually queued
await queue.add('search', { query: 'hello' }, {
  deduplication: { id: 'search-user-1', mode: 'debounce', ttl: 500 },
});
```

`queue.add()` returns `null` when a job is skipped by deduplication.

---

## Global Concurrency

Limit the total number of concurrently active jobs across **all workers** sharing a queue, regardless of per-worker `concurrency` settings.

```typescript
const queue = new Queue('tasks', { connection });

// Allow at most 20 active jobs across all workers at once
await queue.setGlobalConcurrency(20);

// Remove the limit
await queue.setGlobalConcurrency(0);
```

Workers check this limit atomically before picking up each job via the `checkConcurrency` server function.

---

## Global Rate Limiting

Cap the total job throughput across all workers sharing a queue. The config is stored in the Valkey meta hash and picked up dynamically by workers within one scheduler tick.

```typescript
const queue = new Queue('tasks', { connection });

// Max 500 jobs per minute across all workers
await queue.setGlobalRateLimit({ max: 500, duration: 60_000 });

// Read current config
const limit = await queue.getGlobalRateLimit();
// { max: 500, duration: 60000 } or null if not set

// Remove the limit
await queue.removeGlobalRateLimit();
```

- Global rate limit takes precedence over `WorkerOptions.limiter`. When both are set, the stricter limit wins.
- Changes are picked up by workers within one scheduler tick (no restart needed).

---

## Job Revocation

Cooperatively cancel a job that is waiting, delayed, or currently being processed.

```typescript
const job = await queue.add('long-task', { input: 'data' });

// Later...
const result = await queue.revoke(job.id);
// 'revoked'  — job was waiting/delayed and is now in the failed set
// 'flagged'  — job is active; the worker will abort it cooperatively
// 'not_found'— job does not exist
```

In your processor, use `job.abortSignal` to react to revocation:

```typescript
const worker = new Worker('tasks', async (job) => {
  for (const chunk of largeDataset) {
    if (job.abortSignal?.aborted) {
      throw new Error('Job revoked');
    }
    await processChunk(chunk);
  }
  return { done: true };
}, { connection });
```

`job.abortSignal` is an [`AbortSignal`](https://developer.mozilla.org/en-US/docs/Web/API/AbortSignal). You can pass it directly to `fetch`, `axios`, or any `AbortSignal`-aware API.

---

## Transparent Compression

Enable gzip compression at the queue level. Workers decompress automatically — no changes required in processors.

```typescript
const queue = new Queue('tasks', {
  connection,
  compression: 'gzip',
});

// Payload is gzip-compressed before storing in Valkey
await queue.add('process-large', { report: '... 15 KB of data ...' });
// Stored size: ~300 bytes (98% savings on repetitive data)
```

**Payload size limit:** job data must be ≤ 1 MB *after* serialisation but *before* compression. Larger payloads throw immediately:

```
Error: Job data exceeds maximum size (1234567 bytes > 1MB).
       Use smaller payloads or store large data externally.
```

Store large blobs in S3/GCS/object storage and pass a reference URL in the job data instead.

---

## Retries and Backoff

Configure retry behaviour per job via `attempts` and `backoff`:

```typescript
await queue.add('send-email', data, {
  attempts: 5,
  backoff: { type: 'exponential', delay: 1_000 },
  // delay sequence: 1s, 2s, 4s, 8s (capped at attempts)
});

// Fixed delay
await queue.add('webhook', data, {
  attempts: 3,
  backoff: { type: 'fixed', delay: 2_000 },
});

// Exponential with jitter (avoids thundering herd)
await queue.add('poll', data, {
  attempts: 10,
  backoff: { type: 'exponential', delay: 500, jitter: 0.1 },
});

// Custom strategy — register on the Worker
const worker = new Worker('tasks', processor, {
  connection,
  backoffStrategies: {
    'rate-limited': (attemptsMade, err) => {
      // Respect Retry-After header
      if (err.retryAfter) return err.retryAfter * 1_000;
      return attemptsMade * 3_000;
    },
  },
});

await queue.add('api-call', data, {
  attempts: 5,
  backoff: { type: 'rate-limited', delay: 0 },
});
```

When `attempts` is exhausted the job moves to the `failed` state (or the DLQ if configured).

---

## Dead Letter Queues

Route permanently failed jobs to a separate queue for later inspection or manual retry.

```typescript
const worker = new Worker('tasks', processor, {
  connection,
  deadLetterQueue: { name: 'tasks-dlq' },
});

// Inspect DLQ contents
const dlqQueue = new Queue('tasks-dlq', { connection });
const failedJobs = await dlqQueue.getJobs('waiting');

// Or use the convenience method on the original queue
const dlqJobs = await queue.getDeadLetterJobs(0, 49);
```

Jobs in the DLQ are ordinary jobs — you can inspect, retry, or remove them like any other job.
