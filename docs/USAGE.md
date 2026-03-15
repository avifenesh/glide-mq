# Usage Guide

## Table of Contents

- [Queue](#queue)
- [Worker](#worker)
- [Graceful Shutdown](#graceful-shutdown)
- [Cluster Mode](#cluster-mode)
- [Pluggable Serializers](#pluggable-serializers)
- [Broadcast / BroadcastWorker](#broadcast--broadcastworker)
- [Event Listeners](#event-listeners)

---

## Queue

Create a queue by passing a name and a connection config.

```typescript
import { Queue } from 'glide-mq';

const connection = { addresses: [{ host: 'localhost', port: 6379 }] };
const queue = new Queue('tasks', { connection });
```

### Adding jobs

```typescript
// Single job
const job = await queue.add('send-email', { to: 'user@example.com' });

// With options
await queue.add(
  'send-email',
  { to: 'user@example.com' },
  {
    delay: 5_000, // run after 5 s
    priority: 1, // lower = higher priority (default: 0)
    attempts: 3, // run at most 3 times total (initial + 2 retries)
    backoff: { type: 'exponential', delay: 1_000 },
    timeout: 30_000, // fail job if processor exceeds 30 s
    removeOnComplete: true, // auto-remove on success (or { age, count })
    removeOnFail: false, // keep failed jobs for inspection
  },
);

// Bulk add — 12.7× faster than serial via GLIDE Batch API
await queue.addBulk([
  { name: 'job1', data: { a: 1 } },
  { name: 'job2', data: { a: 2 } },
]);
```

#### LIFO mode

Add `lifo: true` so the newest jobs are processed first. LIFO jobs are stored in a dedicated Valkey LIST (RPUSH/RPOP) separate from the default FIFO stream.

```typescript
await queue.add('urgent-report', data, { lifo: true });
```

Processing order: **priority > LIFO > FIFO**. Priority jobs are always consumed first, then LIFO, then the normal FIFO stream. `lifo` cannot be combined with `ordering` keys — they are mutually exclusive.

#### Job TTL

`ttl` (milliseconds) sets a time-to-live on a job. If the job is not processed within this window, it is automatically failed as `'expired'`.

```typescript
// Expire if not processed within 5 minutes
await queue.add('temp', data, { ttl: 300_000 });
```

### Inspecting jobs

```typescript
const job = await queue.getJob('42');

// By state, with optional pagination
const waiting = await queue.getJobs('waiting', 0, 49);
const active = await queue.getJobs('active', 0, 49);
const delayed = await queue.getJobs('delayed', 0, 49);
const done = await queue.getJobs('completed', 0, 49);
const failed = await queue.getJobs('failed', 0, 49);

// Fetch metadata only (omit data and returnvalue) - useful for dashboards
const lite = await queue.getJobs('waiting', 0, 99, { excludeData: true });
const meta = await queue.getJob('42', { excludeData: true });
// lite[0].data === undefined, lite[0].name / .timestamp / .id still present
```

### Queue counts

```typescript
const counts = await queue.getJobCounts();
// { waiting, active, delayed, completed, failed }
```

### Time-series metrics

Get per-minute throughput and latency data for completed or failed jobs:

```typescript
const metrics = await queue.getMetrics('completed');
// {
//   count: 15234,
//   data: [
//     { timestamp: 1709654400000, count: 142, avgDuration: 234 },
//     { timestamp: 1709654460000, count: 156, avgDuration: 218 },
//   ],
//   meta: { resolution: 'minute' }
// }

// Slice data points (e.g. last 10 data points):
const recent = await queue.getMetrics('completed', { start: -10 });
```

Data points are recorded server-side inside the Valkey functions with zero extra RTTs. Minute-resolution buckets are retained for 24 hours and trimmed automatically.

### Pause / resume

```typescript
await queue.pause(); // workers stop picking up new jobs
await queue.resume(); // resume normal operation
const paused = await queue.isPaused();
```

### Drain and obliterate

```typescript
// Remove all waiting jobs (keeps active jobs running)
await queue.drain(); // remove waiting jobs only
await queue.drain(true); // also remove delayed/scheduled jobs

// Remove ALL queue data from Valkey
await queue.obliterate(); // fails if there are active jobs
await queue.obliterate({ force: true }); // unconditional wipe
```

### Cleaning old jobs

Remove completed or failed jobs that are older than a given grace period:

```typescript
// Remove completed jobs older than 1 hour, up to 1000 at a time
const removedIds = await queue.clean(60_000 * 60, 1000, 'completed');

// Remove failed jobs older than 24 hours, up to 500 at a time
const removedFailedIds = await queue.clean(60_000 * 60 * 24, 500, 'failed');

console.log(`Cleaned ${removedIds.length} completed jobs`);
```

- `grace` — minimum age in milliseconds; jobs finished more recently are kept.
- `limit` — maximum number of jobs to remove per call.
- `type` — `'completed'` or `'failed'`.

Returns an array of the removed job IDs.

### Closing

```typescript
await queue.close();
```

---

## Worker

Create a worker with a name, an async processor function, and options.

```typescript
import { Worker } from 'glide-mq';

const worker = new Worker(
  'tasks',
  async (job) => {
    // job.data is typed if you use generics: Worker<MyData, MyResult>
    console.log('Processing', job.name, job.data);

    await job.log('step 1 done'); // append to job log
    await job.updateProgress(50); // broadcast progress (0–100 or object)
    await job.updateData({ ...job.data, enriched: true });

    // Permanently fail a job without consuming retries (two equivalent approaches):
    // 1. Imperative: call job.discard() then throw
    if (job.data.poison) {
      job.discard();
      throw new Error('poisoned job - discarded');
    }
    // 2. Declarative: throw UnrecoverableError - same effect, no discard() needed
    // import { UnrecoverableError } from 'glide-mq';
    // throw new UnrecoverableError('bad input - will not retry');

    return { ok: true }; // becomes job.returnvalue
  },
  {
    connection,
    concurrency: 10, // process up to 10 jobs in parallel (default: 1)
    blockTimeout: 5_000, // XREADGROUP BLOCK timeout in ms
    stalledInterval: 30_000, // how often to check for stalled jobs
    lockDuration: 30_000, // stall detection window per job
    limiter: { max: 100, duration: 60_000 }, // rate limit: 100 jobs / min
    deadLetterQueue: { name: 'dlq' }, // route permanently-failed jobs here
    backoffStrategies: {
      // custom strategy called as: custom(attemptsMade, err) => delayMs
      custom: (attemptsMade) => attemptsMade * 2_000,
    },
  },
);
```

### Worker events

```typescript
worker.on('active', (job, jobId) => {
  console.log(`Job ${jobId} started processing`);
});

worker.on('completed', (job, result) => {
  console.log(`Job ${job.id} finished`, result);
});

worker.on('failed', (job, err) => {
  console.error(`Job ${job.id} failed:`, err.message);
});

worker.on('error', (err) => {
  console.error('Worker error', err);
});

worker.on('stalled', (jobId) => {
  console.warn(`Job ${jobId} stalled and was re-queued`);
});

worker.on('drained', () => {
  console.log('Queue is empty — no more jobs waiting');
});
```

| Event       | Arguments       | Description                                     |
| ----------- | --------------- | ----------------------------------------------- |
| `active`    | `(job, jobId)`  | Fired when a job starts processing              |
| `completed` | `(job, result)` | Fired when a job finishes successfully          |
| `failed`    | `(job, err)`    | Fired when a job throws or times out            |
| `error`     | `(err)`         | Internal worker error (connection issues, etc.) |
| `stalled`   | `(jobId)`       | Job exceeded lock duration and was re-queued    |
| `drained`   | `()`            | Queue transitioned from non-empty to empty      |
| `closing`   | `()`            | Worker is beginning to close                    |
| `closed`    | `()`            | Worker has fully closed                         |

### Pausing / closing a worker

```typescript
await worker.pause(); // stop accepting new jobs (active ones finish)
await worker.pause(true); // force-stop immediately
await worker.resume();

await worker.close(); // graceful: waits for active jobs to finish
await worker.close(true); // force-close immediately
```

---

## Graceful Shutdown

`gracefulShutdown` registers `SIGTERM`/`SIGINT` handlers and resolves once all passed components have closed.

```typescript
import { Queue, Worker, QueueEvents, gracefulShutdown } from 'glide-mq';

const queue = new Queue('tasks', { connection });
const worker = new Worker('tasks', processor, { connection });
const events = new QueueEvents('tasks', { connection });

// Waits for all components to close before the process exits
await gracefulShutdown([queue, worker, events]);
```

Pass any mix of `Queue`, `Worker`, and `QueueEvents` instances. Each receives a `close()` call when a signal is received.

---

## Cluster Mode

Set `clusterMode: true` in the connection config. Everything else is the same — keys are hash-tagged automatically.

```typescript
import { Queue, Worker } from 'glide-mq';

const connection = {
  addresses: [
    { host: 'node1', port: 7000 },
    { host: 'node2', port: 7001 },
  ],
  clusterMode: true,
  // Optional: route reads to same-AZ replicas (AWS ElastiCache / MemoryDB)
  readFrom: 'AZAffinity',
  clientAz: 'us-east-1a',
};

const queue = new Queue('tasks', { connection });
const worker = new Worker('tasks', processor, { connection });
```

### IAM authentication (ElastiCache / MemoryDB)

```typescript
const connection = {
  addresses: [{ host: 'my-cluster.cache.amazonaws.com', port: 6379 }],
  clusterMode: true,
  credentials: {
    type: 'iam',
    serviceType: 'elasticache', // or 'memorydb'
    region: 'us-east-1',
    userId: 'my-iam-user',
    clusterName: 'my-cluster',
  },
};
```

---

## Pluggable Serializers

By default, job data and return values are serialized with `JSON.stringify`/`JSON.parse`. You can replace this with any serializer that implements the `Serializer` interface.

```typescript
import { Queue, Worker, JSON_SERIALIZER } from 'glide-mq';
import type { Serializer } from 'glide-mq';

const base64Serializer: Serializer = {
  serialize(data: unknown): string {
    return Buffer.from(JSON.stringify(data)).toString('base64');
  },
  deserialize(raw: string): unknown {
    return JSON.parse(Buffer.from(raw, 'base64').toString('utf8'));
  },
};

const queue = new Queue('tasks', { connection, serializer: base64Serializer });
const worker = new Worker('tasks', processor, { connection, serializer: base64Serializer });
```

Both `Queue` and `Worker` (and `FlowProducer`, if used) **must use the same serializer**. A mismatch causes silent data corruption — the consumer sees `{}` and the job's `deserializationFailed` flag is set to `true`.

`JSON_SERIALIZER` is the default and is exported for convenience (e.g., when you only need a custom serializer on a subset of queues).

---

## Broadcast / BroadcastWorker

`Broadcast` is a pub/sub fan-out primitive. Unlike `Queue` (point-to-point, each job processed by exactly one worker), `Broadcast` delivers every message to **all** subscribers.

```typescript
import { Broadcast, BroadcastWorker } from 'glide-mq';

const broadcast = new Broadcast('events', {
  connection,
  maxMessages: 1000, // retain at most 1000 messages in the stream
});

// Each subscriber is identified by a unique subscription name (becomes a consumer group)
const inventoryWorker = new BroadcastWorker(
  'events',
  async (job) => {
    console.log('Inventory update:', job.data);
  },
  { connection, subscription: 'inventory-service' },
);

const emailWorker = new BroadcastWorker(
  'events',
  async (job) => {
    console.log('Send notification:', job.data);
  },
  { connection, subscription: 'email-service' },
);

// Publish — every subscriber receives this message
await broadcast.publish({ event: 'order.placed', orderId: 42 });
```

### BroadcastWorker options

Each `BroadcastWorker` supports the same options as `Worker` (concurrency, limiter, backoff, etc.) plus:

- `subscription` (required) — unique name for this subscriber. Becomes the consumer group.
- `startFrom` — stream ID to start reading from when the subscription is first created:
  - `'$'` (default) — only new messages published after subscription creation.
  - `'0-0'` — replay all retained history (backfill).

```typescript
const replayWorker = new BroadcastWorker('events', processor, {
  connection,
  subscription: 'analytics',
  startFrom: '0-0', // backfill all existing messages
  concurrency: 5,
});
```

### Queue vs Broadcast

|                 | Queue                         | Broadcast                         |
| --------------- | ----------------------------- | --------------------------------- |
| Delivery        | Point-to-point (one consumer) | Fan-out (all subscribers)         |
| Use case        | Task processing, job queues   | Event distribution, notifications |
| Add / Publish   | `queue.add(name, data, opts)` | `broadcast.publish(data, opts?)`  |
| Consumer        | `Worker`                      | `BroadcastWorker`                 |
| Retry / backoff | Per job                       | Per subscriber, per message       |
| Stream trimming | Auto (completion/removal)     | `maxMessages` option              |

---

## Event Listeners

### Disabling server-side events

For high-throughput workloads that don't consume `QueueEvents`, disable server-side event emission to save 1 redis.call() per job:

```typescript
// Queue - skip XADD 'added' event on add()
const queue = new Queue('tasks', { connection, events: false });

// Producer - same option
const producer = new Producer('tasks', { connection, events: false });

// Worker - skip XADD 'completed'/'failed' events on process
const worker = new Worker('tasks', handler, { connection, events: false });
```

This only affects the Valkey events stream. TS-side `EventEmitter` events (`worker.on('completed', ...)`) are unaffected.

### `QueueEvents` — stream-based lifecycle events

`QueueEvents` subscribes to the queue's events stream via `XREAD BLOCK`, giving you real-time job lifecycle events without polling.

```typescript
import { QueueEvents } from 'glide-mq';

const events = new QueueEvents('tasks', { connection });

events.on('added', ({ jobId }) => console.log('added', jobId));
events.on('progress', ({ jobId, data }) => console.log('progress', jobId, data));
events.on('completed', ({ jobId, returnvalue }) => console.log('completed', jobId, returnvalue));
events.on('failed', ({ jobId, failedReason }) => console.log('failed', jobId, failedReason));
events.on('stalled', ({ jobId }) => console.log('stalled', jobId));
events.on('paused', () => console.log('queue paused'));
events.on('resumed', () => console.log('queue resumed'));

// Always close QueueEvents when done
await events.close();
```

### Waiting for a specific job to finish

```typescript
// wait until job completes or fails (polls the job hash at the given interval)
const state = await job.waitUntilFinished(500, 30000); // 'completed' | 'failed'
```

### Request-reply with `addAndWait()`

Use `queue.addAndWait()` when the producer needs the final worker result in the same request cycle without polling the job hash.

```typescript
const result = await queue.addAndWait('inference', { prompt: 'Hello', model: 'mini' }, { waitTimeout: 30_000 });

console.log(result);
```

Notes:

- `waitTimeout` is the producer-side wait budget. It is separate from the job’s own `timeout`, which still controls processor execution time.
- `addAndWait()` requires a real `connection` because it uses a dedicated blocking connection to wait on the queue events stream.
- `addAndWait()` is a short-lived request-reply helper. Each in-flight call owns its own blocking wait connection.
- If `add()` is deduplicated and returns `null`, `addAndWait()` rejects instead of hanging.
- `addAndWait()` does not support `removeOnComplete` or `removeOnFail`, because it may need the job hash as a terminal-state fallback.

### Batch Processing

Process multiple jobs at once for higher throughput on I/O-bound operations (bulk database inserts, batch API calls, ML inference).

```typescript
import { Worker, BatchError } from 'glide-mq';

const worker = new Worker(
  'bulk-insert',
  async (jobs) => {
    // jobs is Job[] - process all at once
    const results = await db.insertMany(jobs.map((j) => j.data));
    return results; // must return R[] with length === jobs.length
  },
  {
    connection,
    batch: {
      size: 50, // max jobs per batch
      timeout: 1000, // wait up to 1s for a full batch (optional)
    },
  },
);
```

Options:

- `batch.size` - maximum number of jobs to collect before invoking the processor (1-1000).
- `batch.timeout` - maximum time in ms to wait for additional jobs after a partial batch is received. When omitted, processes whatever is available immediately.

**Partial failures** - throw `BatchError` to report per-job outcomes:

```typescript
const worker = new Worker(
  'mixed',
  async (jobs) => {
    const results = await Promise.allSettled(jobs.map(processOne));
    const mapped = results.map((r) => (r.status === 'fulfilled' ? r.value : r.reason));
    if (mapped.some((r) => r instanceof Error)) {
      throw new BatchError(mapped);
    }
    return mapped;
  },
  { connection, batch: { size: 10 } },
);
```

Each job is individually completed or failed based on its corresponding entry in the `BatchError.results` array. Failed jobs follow normal retry/backoff/DLQ rules.

### Pause and Resume a Job Later (Step Jobs)

Use `job.moveToDelayed(timestampMs, nextStep?)` inside a processor when the same logical job should sleep and resume later instead of completing.

```typescript
const worker = new Worker('drip-campaign', async (job) => {
  switch (job.data.step) {
    case 'send':
      await sendEmail(job.data);
      return job.moveToDelayed(Date.now() + 24 * 3600_000, 'check');
    case 'check':
      if (!(await checkOpened(job.data))) {
        return job.moveToDelayed(Date.now() + 3600_000, 'followup');
      }
      return 'done';
    case 'followup':
      await sendFollowUp(job.data);
      return 'done';
  }
});
```

Notes:

- `moveToDelayed()` must be called from an active worker processor.
- `nextStep` is a convenience for plain object payloads; it updates `job.data.step` atomically with the delayed transition.
- `DelayedError` is exported for advanced/manual control, but `job.moveToDelayed()` is the normal API.

### Dynamic Children (moveToWaitingChildren)

A parent processor can spawn child jobs at runtime, then call `job.moveToWaitingChildren()` to pause until all children complete. When the last child finishes, the parent resumes and the processor is invoked again.

```typescript
import { Queue, Worker, FlowProducer, WaitingChildrenError } from 'glide-mq';

const parentWorker = new Worker(
  'orchestrator',
  async (job) => {
    const step = job.data.step ?? 'spawn';

    if (step === 'spawn') {
      // Dynamically add child jobs
      const childQueue = new Queue('subtasks', { connection });
      await childQueue.add('chunk-1', { chunk: 1 }, { parent: { queue: 'orchestrator', id: job.id } });
      await childQueue.add('chunk-2', { chunk: 2 }, { parent: { queue: 'orchestrator', id: job.id } });
      await childQueue.close();

      // Pause — throws WaitingChildrenError internally
      await job.moveToWaitingChildren();
    }

    // Resumed after all children completed
    const childResults = await job.getChildrenValues();
    return { merged: Object.values(childResults) };
  },
  { connection },
);
```

`moveToWaitingChildren()` throws `WaitingChildrenError` to signal the worker. If all children have already completed by the time the call is made, the job transitions directly back to active.

### UnrecoverableError

Throw `UnrecoverableError` in a processor to skip all remaining retries and move the job directly to the failed state. Useful for validation errors, bad input, or any condition where retrying is pointless.

```typescript
import { Worker, UnrecoverableError } from 'glide-mq';

const worker = new Worker(
  'tasks',
  async (job) => {
    if (!job.data.requiredField) {
      throw new UnrecoverableError('missing requiredField - will not retry');
    }

    // ... normal processing
  },
  { connection, concurrency: 5 },
);
```

The job is marked as permanently failed regardless of the `attempts` configuration. This is equivalent to calling `job.discard()` and then throwing, but more explicit.
