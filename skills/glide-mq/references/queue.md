# Queue Reference

## Constructor

```typescript
import { Queue } from 'glide-mq';

const queue = new Queue('tasks', {
  connection: ConnectionOptions,   // required unless `client` provided
  client?: Client,                 // pre-existing GLIDE client (not owned)
  prefix?: string,                 // key prefix (default: 'glide')
  compression?: 'none' | 'gzip',  // default: 'none'
  serializer?: Serializer,        // default: JSON_SERIALIZER
  events?: boolean,               // emit 'added' events (default: true)
  deadLetterQueue?: { name: string; maxRetries?: number },
});
```

## Adding Jobs

```typescript
// Single job - returns Job | null (null if dedup/collision)
const job = await queue.add(name: string, data: any, opts?: JobOptions);

// Bulk add - 12.7x faster via GLIDE Batch API
const jobs = await queue.addBulk([
  { name: 'job1', data: { a: 1 }, opts?: JobOptions },
]);

// Request-reply - blocks until worker returns result
const result = await queue.addAndWait(name, data, {
  waitTimeout: 30_000,  // producer-side wait budget (separate from job timeout)
  // Does NOT support removeOnComplete or removeOnFail
  // Rejects if dedup returns null
});
```

## JobOptions

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| `delay` | `number` (ms) | 0 | Run after delay |
| `priority` | `number` | 0 | **LOWER = HIGHER** (0 is highest, max 2048) |
| `attempts` | `number` | 1 | Total attempts (initial + retries) |
| `backoff` | `{ type, delay, jitter? }` | - | `'fixed'`, `'exponential'`, or custom name |
| `timeout` | `number` (ms) | - | Fail if processor exceeds this |
| `ttl` | `number` (ms) | - | Fail as `'expired'` if not processed in time. Clock starts at creation. |
| `jobId` | `string` | auto-increment | Custom ID. Max 256 chars. No `{}:` or control chars. Returns `null` on collision. |
| `lifo` | `boolean` | false | Last-in-first-out. Cannot combine with `ordering.key`. |
| `removeOnComplete` | `boolean \| { age, count }` | false | Auto-remove on success |
| `removeOnFail` | `boolean \| number \| { age, count }` | false | Auto-remove on failure. Number = max count to keep. |
| `deduplication` | `{ id, mode, ttl? }` | - | Modes: `'simple'`, `'throttle'`, `'debounce'`. Returns `null` when skipped. |
| `ordering` | `{ key, concurrency?, rateLimit?, tokenBucket? }` | - | Per-key sequential/grouped processing |
| `cost` | `number` | 1 | Token cost for token bucket rate limiting |

> **Note:** Compression is not a per-job option. Set `compression: 'gzip'` at Queue level in the Queue constructor.

### Processing Order

**priority > LIFO > FIFO**. Priority jobs first, then LIFO list, then FIFO stream.

## Queue Management

```typescript
await queue.pause();             // workers stop picking up new jobs
await queue.resume();
const paused = await queue.isPaused();

// Drain - remove waiting jobs
await queue.drain();             // waiting only
await queue.drain(true);         // also delayed/scheduled

// Obliterate - remove ALL queue data
await queue.obliterate();              // fails if active jobs exist
await queue.obliterate({ force: true });

// Clean old jobs by age
const ids = await queue.clean(grace: number, limit: number, type: 'completed' | 'failed');

await queue.close();
```

## Inspecting Jobs

```typescript
const job = await queue.getJob('42');
const job = await queue.getJob('42', { excludeData: true });  // metadata only

const jobs = await queue.getJobs(state, start?, end?);
// state: 'waiting' | 'active' | 'delayed' | 'completed' | 'failed'
const lite = await queue.getJobs('waiting', 0, 99, { excludeData: true });

const counts = await queue.getJobCounts();
// { waiting, active, delayed, completed, failed }

const results = await queue.searchJobs({ state?, name?, data?, limit? });
// data: shallow key-value match. limit default: 100

const waitingCount = await queue.count();  // stream length
```

## Rate Limiting

```typescript
// Per-worker rate limit (in WorkerOptions)
limiter: { max: 100, duration: 60_000 }  // 100 jobs/min

// Global rate limit (across all workers)
await queue.setGlobalRateLimit({ max: 500, duration: 60_000 });
const limit = await queue.getGlobalRateLimit();
await queue.removeGlobalRateLimit();

// Global concurrency
await queue.setGlobalConcurrency(20);
await queue.setGlobalConcurrency(0);  // remove limit
```

## Dead Letter Queue

```typescript
// Configure on Worker
const worker = new Worker('tasks', processor, {
  connection,
  deadLetterQueue: { name: 'tasks-dlq' },
});

// Inspect DLQ
const dlqJobs = await queue.getDeadLetterJobs(0, 49);
```

## Gotchas

- Priority: **0 is highest priority**. Lower number = higher priority. Max 2048.
- `addAndWait()` rejects if dedup returns null. Does not support `removeOnComplete`/`removeOnFail`.
- `queue.add()` returns `null` on custom jobId collision or deduplication skip.
- `FlowProducer.add()` throws on duplicate jobId (flows cannot be partial).
- Payload size limit: job data must be <= 1 MB after serialization, before compression.
- Same serializer must be used on Queue, Worker, and FlowProducer. Mismatch causes silent corruption.
- `lifo` and `ordering.key` are mutually exclusive - throws at enqueue time.
