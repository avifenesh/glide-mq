# Usage Guide

## Table of Contents

- [Queue](#queue)
- [Worker](#worker)
- [Graceful Shutdown](#graceful-shutdown)
- [Cluster Mode](#cluster-mode)
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
await queue.add('send-email', { to: 'user@example.com' }, {
  delay: 5_000,           // run after 5 s
  priority: 1,            // lower = higher priority (default: 0)
  attempts: 3,            // run at most 3 times total (initial + 2 retries)
  backoff: { type: 'exponential', delay: 1_000 },
  timeout: 30_000,        // fail job if processor exceeds 30 s
  removeOnComplete: true, // auto-remove on success (or { age, count })
  removeOnFail: false,    // keep failed jobs for inspection
});

// Bulk add — 12.7× faster than serial via GLIDE Batch API
await queue.addBulk([
  { name: 'job1', data: { a: 1 } },
  { name: 'job2', data: { a: 2 } },
]);
```

### Inspecting jobs

```typescript
const job = await queue.getJob('42');

// By state, with optional pagination
const waiting  = await queue.getJobs('waiting',   0, 49);
const active   = await queue.getJobs('active',    0, 49);
const delayed  = await queue.getJobs('delayed',   0, 49);
const done     = await queue.getJobs('completed', 0, 49);
const failed   = await queue.getJobs('failed',    0, 49);
```

### Queue counts

```typescript
const counts = await queue.getJobCounts();
// { waiting, active, delayed, completed, failed }
```

### Pause / resume

```typescript
await queue.pause();   // workers stop picking up new jobs
await queue.resume();  // resume normal operation
const paused = await queue.isPaused();
```

### Drain and obliterate

```typescript
// Remove all waiting jobs (keeps active jobs running)
await queue.drain();           // remove waiting jobs only
await queue.drain(true);       // also remove delayed/scheduled jobs

// Remove ALL queue data from Valkey
await queue.obliterate();             // fails if there are active jobs
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

const worker = new Worker('tasks', async (job) => {
  // job.data is typed if you use generics: Worker<MyData, MyResult>
  console.log('Processing', job.name, job.data);

  await job.log('step 1 done');          // append to job log
  await job.updateProgress(50);          // broadcast progress (0–100 or object)
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

  return { ok: true };                   // becomes job.returnvalue
}, {
  connection,
  concurrency: 10,          // process up to 10 jobs in parallel (default: 1)
  blockTimeout: 5_000,      // XREADGROUP BLOCK timeout in ms
  stalledInterval: 30_000,  // how often to check for stalled jobs
  lockDuration: 30_000,     // stall detection window per job
  limiter: { max: 100, duration: 60_000 }, // rate limit: 100 jobs / min
  deadLetterQueue: { name: 'dlq' },        // route permanently-failed jobs here
  backoffStrategies: {
    // custom strategy called as: custom(attemptsMade, err) => delayMs
    custom: (attemptsMade) => attemptsMade * 2_000,
  },
});
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

| Event | Arguments | Description |
|-------|-----------|-------------|
| `active` | `(job, jobId)` | Fired when a job starts processing |
| `completed` | `(job, result)` | Fired when a job finishes successfully |
| `failed` | `(job, err)` | Fired when a job throws or times out |
| `error` | `(err)` | Internal worker error (connection issues, etc.) |
| `stalled` | `(jobId)` | Job exceeded lock duration and was re-queued |
| `drained` | `()` | Queue transitioned from non-empty to empty |
| `closing` | `()` | Worker is beginning to close |
| `closed` | `()` | Worker has fully closed |

### Pausing / closing a worker

```typescript
await worker.pause();       // stop accepting new jobs (active ones finish)
await worker.pause(true);   // force-stop immediately
await worker.resume();

await worker.close();       // graceful: waits for active jobs to finish
await worker.close(true);   // force-close immediately
```

---

## Graceful Shutdown

`gracefulShutdown` registers `SIGTERM`/`SIGINT` handlers and resolves once all passed components have closed.

```typescript
import { Queue, Worker, QueueEvents, gracefulShutdown } from 'glide-mq';

const queue  = new Queue('tasks', { connection });
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
import { Queue, Worker, ReadFrom } from 'glide-mq';

const connection = {
  addresses: [
    { host: 'node1', port: 7000 },
    { host: 'node2', port: 7001 },
  ],
  clusterMode: true,
  // Optional: route reads to same-AZ replicas (AWS ElastiCache / MemoryDB)
  readFrom: ReadFrom.AZAffinity,
  clientAz: 'us-east-1a',
};

const queue  = new Queue('tasks', { connection });
const worker = new Worker('tasks', processor, { connection });
```

### IAM authentication (ElastiCache / MemoryDB)

```typescript
const connection = {
  addresses: [{ host: 'my-cluster.cache.amazonaws.com', port: 6379 }],
  clusterMode: true,
  credentials: {
    type: 'iam',
    serviceType: 'elasticache',  // or 'memorydb'
    region: 'us-east-1',
    userId: 'my-iam-user',
    clusterName: 'my-cluster',
  },
};
```

---

## Event Listeners

### `QueueEvents` — stream-based lifecycle events

`QueueEvents` subscribes to the queue's events stream via `XREAD BLOCK`, giving you real-time job lifecycle events without polling.

```typescript
import { QueueEvents } from 'glide-mq';

const events = new QueueEvents('tasks', { connection });

events.on('added',     ({ jobId })                   => console.log('added',     jobId));
events.on('active',    ({ jobId })                   => console.log('active',    jobId));
events.on('progress',  ({ jobId, data })             => console.log('progress',  jobId, data));
events.on('completed', ({ jobId, returnvalue })      => console.log('completed', jobId, returnvalue));
events.on('failed',    ({ jobId, failedReason })     => console.log('failed',    jobId, failedReason));
events.on('stalled',   ({ jobId })                   => console.log('stalled',   jobId));
events.on('paused',    ()                            => console.log('queue paused'));
events.on('resumed',   ()                            => console.log('queue resumed'));

// Always close QueueEvents when done
await events.close();
```

### Waiting for a specific job to finish

```typescript
// wait until job completes or fails (polls QueueEvents internally)
const result = await job.waitUntilFinished(events);
```
