# Migrating from BullMQ

This guide covers migrating a Node.js application from BullMQ to glide-mq. It documents every API surface difference, gaps with workarounds, and what you gain by switching.

---

## Table of Contents

- [Multi-tenant job queue pattern](#multi-tenant-job-queue-pattern)
- [Why migrate](#why-migrate)
- [Prerequisites](#prerequisites)
- [Install](#install)
- [Connection config](#connection-config)
- [Quick start](#quick-start)
- [API mapping table](#api-mapping-table)
- [Breaking differences](#breaking-differences)
  - [Queue](#queue)
  - [Worker](#worker)
  - [Job](#job)
  - [QueueEvents](#queueevents)
  - [FlowProducer](#flowproducer)
  - [Scheduling / repeatable jobs](#scheduling--repeatable-jobs)
  - [Retries and backoff](#retries-and-backoff)
  - [Deduplication](#deduplication)
  - [Rate limiting](#rate-limiting)
  - [Priorities](#priorities)
  - [Job ordering / per-key sequencing](#job-ordering--per-key-sequencing)
  - [Dead letter queues](#dead-letter-queues)
  - [Job revocation](#job-revocation)
  - [Global concurrency](#global-concurrency)
  - [Compression](#compression)
- [Gaps and workarounds](#gaps-and-workarounds)
- [What glide-mq adds](#what-glide-mq-adds)
- [Testing without a server](#testing-without-a-server)
- [Workflow helpers](#workflow-helpers)
- [NestJS](#nestjs)
- [Migration checklist](#migration-checklist)

---

## Multi-tenant job queue pattern

A common BullMQ Pro pattern: per-client job isolation, concurrency caps per client, retries with accumulated data, and worker-level rate limiting. This section walks through a full migration of that setup.

### Job grouping and per-client isolation

BullMQ Pro groups jobs by `group.id` so jobs for the same client do not run in parallel:

```ts
// BullMQ Pro
await queue.add('process', { clientId, payload }, {
  group: { id: `client-${clientId}` },
});
```

glide-mq provides `opts.ordering.key` for the same isolation guarantee - jobs sharing a key run sequentially, one at a time, regardless of worker concurrency:

```ts
// glide-mq
await queue.add('process', { clientId, payload }, {
  ordering: { key: `client-${clientId}` },
});
```

**What you get**: jobs for `client-123` always run one at a time, in enqueue order. Jobs for different clients run in parallel. No changes needed to the worker.

### Group-level concurrency limit (max N per client)

BullMQ Pro lets you cap concurrent jobs per group:

```ts
// BullMQ Pro - max 2 concurrent jobs per client
await queue.add('process', { clientId, payload }, {
  group: { id: `client-${clientId}`, limit: { max: 2, duration: 0 } },
});
```

glide-mq supports this natively via `ordering.concurrency`:

```ts
// glide-mq - max 2 concurrent jobs per client (open source, no Pro license)
await queue.add('process', { clientId, payload }, {
  ordering: { key: `client-${clientId}`, concurrency: 2 },
});
```

Jobs exceeding the limit are automatically parked in a per-group wait list and released when a slot opens. No thundering herd - exactly one waiting job is promoted per slot freed.

For strict serialization (1 at a time), omit `concurrency` or set it to 1:

```ts
// glide-mq - one job at a time per client
await queue.add('process', { clientId, payload }, {
  ordering: { key: `client-${clientId}` },
});
```

### Exponential backoff

Identical API - no changes needed:

```ts
// BullMQ
await queue.add('process', { clientId, payload }, {
  attempts: 5,
  backoff: { type: 'exponential', delay: 1000 },
});
```

```ts
// glide-mq - identical
await queue.add('process', { clientId, payload }, {
  attempts: 5,
  backoff: { type: 'exponential', delay: 1000 },
});
```

`job.attemptsMade` is also identical - same property name, same semantics.

### Accumulating partial results across retries with `job.updateData()`

`job.updateData()` works the same way. Use it to accumulate results across retry attempts:

```ts
// BullMQ
const worker = new Worker('process', async (job) => {
  const results = job.data.partialResults ?? [];

  const chunk = await processNextChunk(job.data.clientId, job.attemptsMade);
  results.push(chunk);

  await job.updateData({ ...job.data, partialResults: results });

  if (results.length < job.data.totalChunks) {
    throw new Error('more chunks needed'); // triggers retry
  }

  return results;
});
```

```ts
// glide-mq - identical, no changes
const worker = new Worker('process', async (job) => {
  const results = job.data.partialResults ?? [];

  const chunk = await processNextChunk(job.data.clientId, job.attemptsMade);
  results.push(chunk);

  await job.updateData({ ...job.data, partialResults: results });

  if (results.length < job.data.totalChunks) {
    throw new Error('more chunks needed');
  }

  return results;
}, { connection });
```

For custom attempt tracking in job data:

```ts
// BullMQ
await job.updateData({ ...job.data, customAttemptsMade: job.data.customAttemptsMade + 1 });
```

```ts
// glide-mq - identical
await job.updateData({ ...job.data, customAttemptsMade: (job.data.customAttemptsMade ?? 0) + 1 });
```

### Worker-level rate limiting

Identical API - no changes needed:

```ts
// BullMQ - max 2 jobs per 100ms across this worker
const worker = new Worker('process', processor, {
  connection,
  limiter: { max: 2, duration: 100 },
});
```

```ts
// glide-mq - identical
const worker = new Worker('process', processor, {
  connection: { addresses: [{ host: 'localhost', port: 6379 }] },
  limiter: { max: 2, duration: 100 },
});
```

Dynamic rate limiting from inside the processor also works the same way:

```ts
// Both BullMQ and glide-mq
const worker = new Worker('process', async (job) => {
  const retryAfter = await checkUpstreamRateLimit(job.data.clientId);
  if (retryAfter > 0) {
    await worker.rateLimit(retryAfter);
    throw new Worker.RateLimitError(); // re-queues the job, not counted as failure
  }
  return process(job.data);
}, { connection, limiter: { max: 100, duration: 1000 } });
```

### Full before/after for the described setup

```ts
// BullMQ Pro - original setup
import { Queue, Worker } from 'bullmq';

const connection = { host: 'localhost', port: 6379 };
const queue = new Queue('jobs', { connection });

// Enqueue with per-client isolation, max 2 concurrent
await queue.add('task', { clientId: 'acme', payload: {} }, {
  group: { id: 'acme', limit: { max: 2, duration: 0 } },
  attempts: 5,
  backoff: { type: 'exponential', delay: 1000 },
});

const worker = new Worker('jobs', async (job) => {
  const results = job.data.partialResults ?? [];
  results.push(await doWork(job.data));
  await job.updateData({ ...job.data, partialResults: results });
  return results;
}, {
  connection,
  concurrency: 20,
  limiter: { max: 2, duration: 100 },
});
```

```ts
// glide-mq - migrated (direct equivalent, open source)
import { Queue, Worker } from 'glide-mq';

const connection = { addresses: [{ host: 'localhost', port: 6379 }] };
const queue = new Queue('jobs', { connection });

// ordering.key + concurrency: 2 gives exact BullMQ Pro group behavior
await queue.add('task', { clientId: 'acme', payload: {} }, {
  ordering: { key: 'acme', concurrency: 2 },
  attempts: 5,
  backoff: { type: 'exponential', delay: 1000 },
});

const worker = new Worker('jobs', async (job) => {
  const results = job.data.partialResults ?? [];
  results.push(await doWork(job.data));
  await job.updateData({ ...job.data, partialResults: results });
  return results;
}, {
  connection,
  concurrency: 20,
  limiter: { max: 2, duration: 100 },
});
```

This is a direct equivalent. No behavioral differences.

---



| | BullMQ | glide-mq |
|---|---|---|
| Redis client | ioredis (JS) | valkey-glide (Rust NAPI) |
| RTT per job | 2-3 (fetch + ack + complete) | 1 (`completeAndFetchNext` FCALL) |
| Server-side logic | 53 EVAL Lua scripts | 1 FUNCTION LOAD library |
| Cluster support | Works, not hash-tagged by default | Built-in, all keys `glide:{name}:*` |
| AZ-affinity routing | No | Yes - pin reads to your AZ |
| IAM auth (ElastiCache/MemoryDB) | No | Yes |
| Compression | No | gzip transparent compression |
| Per-key ordering + group rate limit | No (BullMQ Pro groups only) | Yes, `opts.ordering.key` with concurrency, rateLimit, and tokenBucket |
| In-memory test mode | No | Yes, `TestQueue` / `TestWorker` |

glide-mq is a strict superset of BullMQ's core job queue semantics. At-least-once delivery, consumer groups, stall detection, retries, DLQ, flows, and schedulers all work the same way. The differences are in API shape and some missing conveniences listed in [Gaps and workarounds](#gaps-and-workarounds).

---

## Prerequisites

- Node.js 20+
- Valkey 7.0+ or Redis 7.0+ (both are supported)
- TypeScript 5+ recommended

---

## Install

```bash
npm remove bullmq
npm install glide-mq
```

Update your import paths:

```ts
// Before
import { Queue, Worker, Job, QueueEvents, FlowProducer } from 'bullmq';

// After
import { Queue, Worker, Job, QueueEvents, FlowProducer } from 'glide-mq';
```

---

## Connection config

This is the most common source of errors when migrating. BullMQ uses ioredis's flat connection format; glide-mq uses valkey-glide's `addresses` array.

```ts
// BullMQ
const connection = { host: 'localhost', port: 6379 };
const queue = new Queue('tasks', { connection });
const worker = new Worker('tasks', processor, { connection });
```

```ts
// glide-mq
const connection = { addresses: [{ host: 'localhost', port: 6379 }] };
const queue = new Queue('tasks', { connection });
const worker = new Worker('tasks', processor, { connection });
```

For TLS, password, and cluster mode:

```ts
// BullMQ
const connection = {
  host: 'my-cluster.cache.amazonaws.com',
  port: 6379,
  tls: {},
  password: 'secret',
};
```

```ts
// glide-mq
const connection = {
  addresses: [{ host: 'my-cluster.cache.amazonaws.com', port: 6379 }],
  useTLS: true,
  credentials: { password: 'secret' },
  clusterMode: true,           // set to true for Redis Cluster / ElastiCache cluster
};
```

For IAM auth on AWS ElastiCache / MemoryDB (no equivalent in BullMQ):

```ts
// glide-mq only
const connection = {
  addresses: [{ host: 'my-cluster.cache.amazonaws.com', port: 6379 }],
  useTLS: true,
  clusterMode: true,
  credentials: {
    type: 'iam',
    serviceType: 'elasticache',
    region: 'us-east-1',
    userId: 'my-iam-user',
    clusterName: 'my-cluster',
  },
};
```

---

## Quick start

```ts
// BullMQ
import { Queue, Worker } from 'bullmq';

const connection = { host: 'localhost', port: 6379 };

const queue = new Queue('tasks', { connection });
await queue.add('send-email', { to: 'user@example.com' });

const worker = new Worker('tasks', async (job) => {
  await sendEmail(job.data.to);
}, { connection, concurrency: 10 });

worker.on('completed', (job) => console.log(job.id, 'done'));
worker.on('failed', (job, err) => console.error(job.id, err.message));
```

```ts
// glide-mq
import { Queue, Worker } from 'glide-mq';

const connection = { addresses: [{ host: 'localhost', port: 6379 }] };

const queue = new Queue('tasks', { connection });
await queue.add('send-email', { to: 'user@example.com' });

const worker = new Worker('tasks', async (job) => {
  await sendEmail(job.data.to);
}, { connection, concurrency: 10 });

worker.on('completed', (job) => console.log(job.id, 'done'));
worker.on('failed', (job, err) => console.error(job.id, err.message));
```

The processor function signature is identical. The only change is the connection format.

---

## API mapping table

`Full` = identical API. `Changed` = available with different shape. `Gap` = not yet implemented (see [Gaps and workarounds](#gaps-and-workarounds)).

### Queue methods

| BullMQ | glide-mq | Status |
|--------|----------|--------|
| `new Queue(name, { connection, prefix, defaultJobOptions })` | `new Queue(name, { connection, prefix, deadLetterQueue, compression })` | Changed |
| `queue.add(name, data, opts)` | `queue.add(name, data, opts)` | Full |
| `queue.addBulk(jobs)` | `queue.addBulk(jobs)` | Full |
| `queue.pause()` | `queue.pause()` | Full |
| `queue.resume()` | `queue.resume()` | Full |
| `queue.isPaused()` | `queue.isPaused()` | Full |
| `queue.obliterate({ force })` | `queue.obliterate({ force })` | Full |
| `queue.getJob(id)` | `queue.getJob(id)` | Full |
| `queue.getJobs(types, start, end, asc)` | `queue.getJobs(type, start, end)` | Changed |
| `queue.getJobCounts(...types)` | `queue.getJobCounts()` | Changed |
| `queue.getJobCountByTypes(...types)` | `queue.getJobCountByTypes()` | Full |
| `queue.count()` | `queue.count()` | Full |
| `queue.getMetrics(type, start, end)` | `queue.getMetrics(type)` | Changed |
| `queue.getJobLogs(id, start, end)` | `queue.getJobLogs(id, start, end)` | Full |
| `queue.setGlobalConcurrency(n)` | `queue.setGlobalConcurrency(n)` | Full |
| `queue.upsertJobScheduler(id, opts, template)` | `queue.upsertJobScheduler(name, scheduleOpts, template)` | Full |
| `queue.getJobScheduler(id)` | - | Gap [#19](https://github.com/avifenesh/glide-mq/issues/19) |
| `queue.getJobSchedulers()` | `queue.getRepeatableJobs()` | Changed |
| `queue.removeJobScheduler(id)` | `queue.removeJobScheduler(name)` | Full |
| `queue.getWorkers()` | - | Gap [#18](https://github.com/avifenesh/glide-mq/issues/18) |
| `queue.drain(delayed?)` | - | Gap [#15](https://github.com/avifenesh/glide-mq/issues/15) |
| `queue.clean(grace, limit, type)` | - | Gap [#16](https://github.com/avifenesh/glide-mq/issues/16) |
| `queue.retryJobs(opts)` | - | Gap [#17](https://github.com/avifenesh/glide-mq/issues/17) |
| `queue.close()` | `queue.close()` | Full |
| - | `queue.revoke(jobId)` | glide-mq only |
| - | `queue.getDeadLetterJobs(start, end)` | glide-mq only |
| - | `queue.searchJobs(opts)` | glide-mq only |
| - | `queue.setGlobalRateLimit({ max, duration })` | glide-mq only |
| - | `queue.getGlobalRateLimit()` | glide-mq only |
| - | `queue.removeGlobalRateLimit()` | glide-mq only |

### Worker methods and options

| BullMQ | glide-mq | Status |
|--------|----------|--------|
| `new Worker(name, processor, { connection, concurrency, limiter, stalledInterval, maxStalledCount, lockDuration, settings })` | `new Worker(name, processor, { connection, concurrency, globalConcurrency, limiter, stalledInterval, maxStalledCount, lockDuration, backoffStrategies })` | Changed |
| `worker.pause(doNotWaitActive?)` | `worker.pause(force?)` | Full |
| `worker.resume()` | `worker.resume()` | Full |
| `worker.close(force?)` | `worker.close(force?)` | Full |
| `worker.drain()` | `worker.drain()` | Full |
| `worker.rateLimit(ms)` | `worker.rateLimit(ms)` | Full |
| `worker.on('completed', (job, result))` | `worker.on('completed', (job, result))` | Full |
| `worker.on('failed', (job, err))` | `worker.on('failed', (job, err))` | Full |
| `worker.on('error', (err))` | `worker.on('error', (err))` | Full |
| `worker.on('stalled', (jobId))` | `worker.on('stalled', (jobId))` | Full |
| `worker.on('closing')` | `worker.on('closing')` | Full |
| `worker.on('closed')` | `worker.on('closed')` | Full |
| `worker.on('active', (job, prev))` | - | Gap [#20](https://github.com/avifenesh/glide-mq/issues/20) |
| `worker.on('drained')` | - | Gap [#20](https://github.com/avifenesh/glide-mq/issues/20) |
| `Worker.RateLimitError` | `Worker.RateLimitError` | Full |
| Sandboxed processor (file path string) | `new Worker('q', './processor.js', { connection, sandbox: {} })` | Full |

### Job methods

| BullMQ | glide-mq | Status |
|--------|----------|--------|
| `job.id`, `job.name`, `job.data`, `job.opts` | Same | Full |
| `job.attemptsMade`, `job.timestamp`, `job.processedOn`, `job.finishedOn` | Same | Full |
| `job.returnvalue`, `job.failedReason`, `job.progress` | Same | Full |
| `job.updateData(data)` | `job.updateData(data)` | Full |
| `job.updateProgress(progress)` | `job.updateProgress(progress)` | Full |
| `job.log(message)` | `job.log(message)` | Full |
| `job.getState()` | `job.getState()` | Full |
| `job.isCompleted()`, `job.isFailed()`, `job.isActive()`, `job.isWaiting()`, `job.isDelayed()` | Same | Full |
| `job.waitUntilFinished(queueEvents, ttl)` | `job.waitUntilFinished(pollIntervalMs, timeoutMs)` | Changed |
| `job.retry(state?)` | `job.retry()` | Changed |
| `job.remove(opts?)` | `job.remove()` | Full |
| `job.getChildrenValues()` | `job.getChildrenValues()` | Full |
| `job.promote()` | - | Gap [#11](https://github.com/avifenesh/glide-mq/issues/11) |
| `job.changeDelay(delay)` | - | Gap [#12](https://github.com/avifenesh/glide-mq/issues/12) |
| `job.changePriority(opts)` | - | Gap [#13](https://github.com/avifenesh/glide-mq/issues/13) |
| `job.discard()` | - | Gap [#14](https://github.com/avifenesh/glide-mq/issues/14) |
| `job.toJSON()` | - | Use `job.data`, `job.opts`, etc. directly |
| - | `job.abortSignal` | glide-mq only |
| - | `job.isRevoked()` | glide-mq only |

### JobOptions

| BullMQ field | glide-mq field | Status |
|---|---|---|
| `delay` | `delay` | Full |
| `priority` | `priority` | Full |
| `attempts` | `attempts` | Full |
| `backoff` | `backoff` | Full |
| `timeout` | `timeout` | Full |
| `removeOnComplete` | `removeOnComplete` | Full |
| `removeOnFail` | `removeOnFail` | Full |
| `deduplication` | `deduplication` | Changed (see [Deduplication](#deduplication)) |
| `parent` | `parent` | Full |
| `jobId` (custom ID) | - | Gap - IDs are auto-generated |
| `lifo` | - | Gap - not supported |
| `repeat` | - | Gap - use `queue.upsertJobScheduler()` |
| `sizeLimit` | - | 1 MB hard limit enforced internally |
| - | `ordering.key` | glide-mq only |
| - | `ordering.concurrency` | glide-mq only |
| - | `ordering.rateLimit` | glide-mq only |
| - | `ordering.tokenBucket` | glide-mq only |
| - | `cost` | glide-mq only |

### QueueEvents events

| BullMQ event | glide-mq event | Status |
|---|---|---|
| `'added'` | `'added'` | Full |
| `'completed'` | `'completed'` | Full |
| `'failed'` | `'failed'` | Full |
| `'stalled'` | `'stalled'` | Full |
| `'progress'` | `'progress'` | Full |
| `'paused'` | `'paused'` | Full |
| `'resumed'` | `'resumed'` | Full |
| `'removed'` | `'removed'` | Full |
| `'retries-exhausted'` | `'failed'` | Changed - check `job.attemptsMade >= job.opts.attempts` |
| `'waiting'` | - | Gap |
| `'active'` | - | Gap |
| `'delayed'` | - | Gap |
| `'drained'` | - | Gap |
| `'cleaned'` | - | Gap |
| `'deduplicated'` | - | Gap |
| `'waiting-children'` | - | Gap |

---

## Breaking differences

### Queue

**`defaultJobOptions` removed** - BullMQ accepts `defaultJobOptions` on `QueueOptions` to set per-queue defaults. glide-mq does not. Set options explicitly on each `queue.add()` call, or wrap `queue.add` in a helper:

```ts
// BullMQ
const queue = new Queue('tasks', {
  connection,
  defaultJobOptions: { attempts: 3, backoff: { type: 'exponential', delay: 1000 } },
});
```

```ts
// glide-mq - wrap add() with your defaults
const DEFAULTS = { attempts: 3, backoff: { type: 'exponential', delay: 1000 } } as const;
const add = (name: string, data: unknown, opts?: JobOptions) =>
  queue.add(name, data, { ...DEFAULTS, ...opts });
```

**`queue.getJobs()` takes a single type** - BullMQ accepts an array of types; glide-mq takes one type at a time:

```ts
// BullMQ
const jobs = await queue.getJobs(['waiting', 'active'], 0, 99);
```

```ts
// glide-mq
const [waiting, active] = await Promise.all([
  queue.getJobs('waiting', 0, 99),
  queue.getJobs('active', 0, 99),
]);
const jobs = [...waiting, ...active];
```

**`queue.getJobCounts()` returns all states** - BullMQ accepts a variadic list of state names; glide-mq always returns all five (`waiting`, `active`, `delayed`, `completed`, `failed`).

```ts
// BullMQ
const { waiting, active } = await queue.getJobCounts('waiting', 'active');
```

```ts
// glide-mq
const { waiting, active } = await queue.getJobCounts(); // always returns all
```

---

### Worker

**`settings.backoffStrategy` renamed** - BullMQ v3+ uses a single `settings.backoffStrategy` function. glide-mq uses `backoffStrategies` as a named-strategy map:

```ts
// BullMQ
const worker = new Worker('q', processor, {
  connection,
  settings: {
    backoffStrategy: (attemptsMade, type, delay, err) => {
      if (type === 'jitter') return delay + Math.random() * delay;
      return delay * attemptsMade;
    },
  },
});
```

```ts
// glide-mq
const worker = new Worker('q', processor, {
  connection,
  backoffStrategies: {
    jitter: (attemptsMade, err) => 1000 + Math.random() * 1000,
    linear:  (attemptsMade, err) => 1000 * attemptsMade,
  },
});

// Reference by name in job options:
await queue.add('job', data, { backoff: { type: 'jitter', delay: 1000 } });
```

**`globalConcurrency` on WorkerOptions** - BullMQ sets global concurrency via `queue.setGlobalConcurrency(n)` or `WorkerOptions.concurrency` combined with queue-level limits. glide-mq exposes both:

```ts
// glide-mq
const worker = new Worker('q', processor, {
  connection,
  concurrency: 10,          // per-worker concurrency
  globalConcurrency: 50,   // queue-wide cap across all workers (set once, stored in Valkey)
});

// Or set it on the queue separately:
await queue.setGlobalConcurrency(50);
```

**Sandboxed processors** - Both BullMQ and glide-mq support passing a file path string as the processor. The processor runs in a worker thread (default) or child process, isolating crashes from the main process.

```ts
// BullMQ
const worker = new Worker('q', './processor.js', { connection, useWorkerThreads: true });
```

```ts
// glide-mq
const worker = new Worker('q', './processor.js', { connection, sandbox: { useWorkerThreads: true } });
```

The processor file must export a default function: `module.exports = async (job) => { ... }` (CJS) or `export default async (job) => { ... }` (ESM). Inside the sandbox, `job.log()`, `job.updateProgress()`, and `job.updateData()` work normally via IPC proxy. Methods that require direct Valkey access (`job.getState()`, `job.remove()`, etc.) are not available.

**`worker.on('active')` and `worker.on('drained')` not yet available** - tracked in [#20](https://github.com/avifenesh/glide-mq/issues/20). Workarounds:

```ts
// Workaround for 'active': wrap the processor
const worker = new Worker('q', async (job) => {
  myMetrics.jobStarted(job.name);  // ← replaces worker.on('active')
  return processor(job);
}, { connection });

// Workaround for 'drained': poll getJobCounts()
async function waitUntilEmpty(queue: Queue, intervalMs = 500): Promise<void> {
  while (true) {
    const { waiting, active, delayed } = await queue.getJobCounts();
    if (waiting === 0 && active === 0 && delayed === 0) return;
    await new Promise(r => setTimeout(r, intervalMs));
  }
}
```

---

### Job

**`job.waitUntilFinished()` takes different arguments** - BullMQ requires a `QueueEvents` instance. glide-mq polls the job hash directly:

```ts
// BullMQ
const qe = new QueueEvents('tasks', { connection });
const state = await job.waitUntilFinished(qe, 30000);
```

```ts
// glide-mq - no QueueEvents needed
const state = await job.waitUntilFinished(500, 30000);
// args: pollIntervalMs (default 500), timeoutMs (default 30000)
```

**No custom `jobId`** - BullMQ allows setting a deterministic job ID via `opts.jobId`. glide-mq auto-generates sequential numeric IDs. If you need idempotent job creation, use deduplication instead:

```ts
// BullMQ
await queue.add('job', data, { jobId: 'my-deterministic-id' });
```

```ts
// glide-mq - use deduplication for idempotency
await queue.add('job', data, {
  deduplication: { id: 'my-deterministic-id', ttl: 60000 },
});
```

**No `lifo`** - Last-in-first-out ordering is not supported. Use `priority` (lower number = higher priority) to approximate LIFO behavior if needed.

**No `job.promote()`** - tracked in [#11](https://github.com/avifenesh/glide-mq/issues/11). No workaround without re-adding the job.

**No `job.discard()`** - tracked in [#14](https://github.com/avifenesh/glide-mq/issues/14). Workaround: set `attempts: 1` on the job, or throw a specific error type and use a backoff strategy that returns 0 to prevent scheduling.

---

### QueueEvents

The `QueueEvents` class API is compatible, but the available events differ. glide-mq emits fewer events from the stream.

**`'retries-exhausted'` renamed to `'failed'`** - BullMQ emits a separate `retries-exhausted` event when all attempts are used up. In glide-mq, a job exhausting all retries emits `'failed'`. Check `attemptsMade` vs `attempts` to detect exhaustion:

```ts
// BullMQ
qe.on('retries-exhausted', ({ jobId, attemptsMade }) => {
  console.log(`Job ${jobId} gave up after ${attemptsMade} attempts`);
});
```

```ts
// glide-mq
qe.on('failed', async ({ jobId }) => {
  const job = await queue.getJob(jobId);
  if (job && job.attemptsMade >= (job.opts.attempts ?? 1)) {
    console.log(`Job ${jobId} gave up after ${job.attemptsMade} attempts`);
  }
});
```

**`job.waitUntilFinished()` does not need QueueEvents** - see [Job section](#job) above.

**`lastEventId: '0'` for historical replay** - Same as BullMQ:

```ts
// Replay all events from the beginning
const qe = new QueueEvents('tasks', {
  connection,
  lastEventId: '0',
});
```

---

### FlowProducer

The FlowProducer API is compatible. `FlowJob` type shape differs slightly:

```ts
// BullMQ FlowJob
interface FlowJob {
  name: string;
  queueName: string;
  data?: any;
  opts?: Omit<JobsOptions, 'repeat'>;
  children?: FlowChildJob[];
}

// glide-mq FlowJob
interface FlowJob {
  name: string;
  queueName: string;
  data: any;            // required in glide-mq (not optional)
  opts?: JobOptions;
  children?: FlowJob[]; // same type for parent and children
}
```

The behavior is the same: parent job waits in `waiting-children` state until all children complete. If a child fails without retries, the parent is permanently stuck (same as BullMQ's default behavior - use `failParentOnFailure` in BullMQ; glide-mq does not yet expose this option).

```ts
// Both BullMQ and glide-mq - same usage
const flow = await flowProducer.add({
  name: 'parent',
  queueName: 'tasks',
  data: { step: 'final' },
  children: [
    { name: 'child-1', queueName: 'tasks', data: { step: '1' } },
    { name: 'child-2', queueName: 'tasks', data: { step: '2' } },
  ],
});
```

glide-mq also provides higher-level workflow helpers - see [Workflow helpers](#workflow-helpers).

---

### Scheduling / repeatable jobs

BullMQ v5 introduced Job Schedulers (`upsertJobScheduler`) to replace `opts.repeat`. glide-mq uses the same `upsertJobScheduler` API.

**Do not use `opts.repeat` in `queue.add()`** - pass schedule options to `upsertJobScheduler` instead:

```ts
// BullMQ (old repeat API - deprecated in v5)
await queue.add('daily-report', data, {
  repeat: { pattern: '0 9 * * *', tz: 'America/New_York' },
});
```

```ts
// glide-mq (and BullMQ v5+) - upsertJobScheduler
await queue.upsertJobScheduler(
  'daily-report',                             // scheduler name
  { pattern: '0 9 * * *' },                  // schedule (cron or every ms)
  { name: 'daily-report', data: { v: 1 } },  // job template
);
```

`getRepeatableJobs()` returns the scheduler list:

```ts
const schedulers = await queue.getRepeatableJobs();
// [{ name: 'daily-report', entry: { pattern, nextRun, lastRun? } }]
```

Remove a scheduler:

```ts
await queue.removeJobScheduler('daily-report');
```

**`QueueScheduler` class** - BullMQ v1 required a separate `QueueScheduler` process to promote delayed jobs. BullMQ v2+ merged this into the Worker. glide-mq has no `QueueScheduler` - promotion runs inside the Worker. No action needed.

---

### Retries and backoff

`JobOptions.backoff` is compatible:

```ts
// Both BullMQ and glide-mq
await queue.add('job', data, {
  attempts: 5,
  backoff: { type: 'exponential', delay: 1000 },
});
```

glide-mq adds a `jitter` field to spread retries under load:

```ts
// glide-mq only
await queue.add('job', data, {
  attempts: 5,
  backoff: { type: 'exponential', delay: 1000, jitter: 0.25 }, // ±25% random jitter
});
```

Custom backoff strategies moved from `settings.backoffStrategy` to `backoffStrategies` map - see [Worker section](#worker).

**`timeout` option exists but behavior note** - Both BullMQ and glide-mq accept `opts.timeout`. In BullMQ this is also not fully implemented in all versions. In glide-mq the timeout is respected but acts as a heartbeat-based stall detection rather than a hard kill. Use `job.abortSignal` for cooperative cancellation within the processor:

```ts
const worker = new Worker('q', async (job) => {
  if (job.abortSignal?.aborted) return;

  const result = await someOperation();

  if (job.abortSignal?.aborted) return; // check periodically in long tasks
  return result;
}, { connection });
```

---

### Deduplication

Both libraries support deduplication. The option shape differs:

```ts
// BullMQ
await queue.add('job', data, {
  deduplication: { id: 'my-dedup-key' },
  // or with TTL:
  deduplication: { id: 'my-dedup-key', ttl: 60000 },
});
```

```ts
// glide-mq - same shape, plus explicit mode
await queue.add('job', data, {
  deduplication: { id: 'my-dedup-key', ttl: 60000 },
  // optional mode:
  deduplication: { id: 'my-dedup-key', ttl: 60000, mode: 'simple' },
  //                                                mode: 'throttle'  - drop duplicates
  //                                                mode: 'debounce'  - reset window on each add
});
```

Default mode is `'simple'` (drop duplicate if a job with that ID already exists in any active state).

---

### Rate limiting

Both libraries support the `limiter` option and `Worker.RateLimitError`. The usage is identical:

```ts
// Both BullMQ and glide-mq
const worker = new Worker('q', async (job) => {
  if (shouldThrottle()) {
    throw new Worker.RateLimitError();  // re-queues job, does not count as failure
  }
  return process(job);
}, {
  connection,
  limiter: { max: 100, duration: 1000 },  // 100 jobs per second
});
```

`worker.rateLimit(ms)` is also available in both:

```ts
const worker = new Worker('q', async (job) => {
  const retryAfterMs = await checkExternalRateLimit();
  if (retryAfterMs > 0) {
    await worker.rateLimit(retryAfterMs);
    throw new Worker.RateLimitError();
  }
  return process(job);
}, { connection });
```

**Global rate limiting** - glide-mq supports a queue-wide rate limit stored in Valkey, dynamically picked up by all workers:

```ts
// glide-mq only - global rate limit across all workers
await queue.setGlobalRateLimit({ max: 500, duration: 60_000 });

const limit = await queue.getGlobalRateLimit(); // { max, duration } or null
await queue.removeGlobalRateLimit();
```

When both global rate limit and `WorkerOptions.limiter` are set, the stricter limit wins.

**Per-group rate limiting** - glide-mq supports rate limiting per ordering key (N jobs per time window), equivalent to BullMQ Pro's group rate limiting:

```ts
// BullMQ Pro
await queue.add('sync', data, {
  group: { id: `tenant-${id}`, limit: { max: 10, duration: 60_000 } },
});
```

```ts
// glide-mq - per-group rate limit (open source)
await queue.add('sync', data, {
  ordering: {
    key: `tenant-${id}`,
    concurrency: 3,
    rateLimit: { max: 10, duration: 60_000 },
  },
});
```

Rate-limited jobs are promoted by the scheduler loop (latency up to `promotionInterval`, default 5 s). Retried jobs consume rate slots.

---

### Priorities

Priority values work the same way. Lower number = higher priority (0 is default, highest priority):

```ts
// Both BullMQ and glide-mq - identical
await queue.add('urgent', data, { priority: 1 });
await queue.add('normal', data, { priority: 10 });
await queue.add('background', data, { priority: 100 });
```

`job.changePriority()` is not yet implemented in glide-mq - tracked in [#13](https://github.com/avifenesh/glide-mq/issues/13).

---

### Job ordering / per-key sequencing

BullMQ Pro offers group-level concurrency (max N parallel jobs per group key). glide-mq provides per-key sequential ordering (concurrency = 1 per key) as a built-in feature:

```ts
// BullMQ Pro only
await queue.add('job', data, {
  group: { id: 'tenant-123', limit: 1 },  // requires BullMQ Pro license
});
```

```ts
// glide-mq (open source)
await queue.add('job', data, {
  ordering: { key: 'tenant-123' },  // jobs for this key run one at a time, in order
});
```

Jobs with the same `ordering.key` are guaranteed to run sequentially in enqueue order regardless of worker concurrency. Jobs with different keys (or no key) run in parallel as normal.

Group concurrency > 1 is now supported via `ordering.concurrency`:

```ts
// glide-mq - max 3 parallel jobs for tenant X
await queue.add('process', data, {
  ordering: { key: 'tenant-X', concurrency: 3 },
});
```

Jobs exceeding the limit are automatically parked in a per-group wait list and released when a slot opens.

---

### Dead letter queues

BullMQ does not have a native DLQ - failed jobs stay in the failed state. glide-mq has first-class DLQ support configured at the queue level:

```ts
// glide-mq only
const queue = new Queue('tasks', {
  connection,
  deadLetterQueue: {
    name: 'tasks-dlq',   // separate queue for permanently failed jobs
    maxRetries: 3,       // override job's own attempts setting
  },
});

// Retrieve DLQ jobs:
const dlqQueue = new Queue('tasks-dlq', { connection });
const dlqJobs  = await dlqQueue.getDeadLetterJobs();
```

If you were managing a DLQ manually in BullMQ (e.g., moving jobs in the `failed` handler), switch to the native option above.

---

### Job revocation

BullMQ has no equivalent. glide-mq supports revoking a job from outside the worker:

```ts
// glide-mq only
await queue.revoke(jobId);  // signals abort to the processor via job.abortSignal
```

The processor must cooperate:

```ts
const worker = new Worker('q', async (job) => {
  for (const chunk of data) {
    if (job.abortSignal?.aborted) return;  // exits cleanly on revocation
    await processChunk(chunk);
  }
}, { connection });
```

---

### Global concurrency

Both libraries support queue-level global concurrency to cap total parallel jobs across all workers:

```ts
// BullMQ
await queue.setGlobalConcurrency(50);
```

```ts
// glide-mq - same, plus a WorkerOptions shorthand
await queue.setGlobalConcurrency(50);

// Or set it once at worker startup:
const worker = new Worker('q', processor, {
  connection,
  globalConcurrency: 50,
});
```

---

### Compression

BullMQ has no built-in compression. glide-mq supports transparent gzip compression of job payloads:

```ts
// glide-mq only
const queue = new Queue('tasks', {
  connection,
  compression: 'gzip',  // compress all job payloads on write, decompress on read
});
// No change needed in worker or job code - transparent.
```

Useful when job payloads are large (15 KB JSON → 331 bytes with gzip, 98% reduction).

---

## Gaps and workarounds

These features exist in BullMQ but are not yet implemented in glide-mq. Each has a tracking issue.

| Missing feature | Workaround | Issue |
|---|---|---|
| `job.promote()` | Remove and re-add the job with `delay: 0` | [#11](https://github.com/avifenesh/glide-mq/issues/11) |
| `job.changeDelay(delay)` | Remove and re-add with new delay | [#12](https://github.com/avifenesh/glide-mq/issues/12) |
| `job.changePriority(opts)` | Remove and re-add with new priority | [#13](https://github.com/avifenesh/glide-mq/issues/13) |
| `job.discard()` | Set `attempts: 1` upfront, or use a backoff strategy returning a large value that exceeds max attempts | [#14](https://github.com/avifenesh/glide-mq/issues/14) |
| `queue.drain(delayed?)` | `queue.obliterate({ force: false })` removes everything; no surgical drain yet | [#15](https://github.com/avifenesh/glide-mq/issues/15) |
| `queue.clean(grace, limit, type)` | Iterate `getJobs('completed')`, filter by `finishedOn`, call `job.remove()` | [#16](https://github.com/avifenesh/glide-mq/issues/16) |
| `queue.retryJobs(opts)` | `const failed = await queue.getJobs('failed'); await Promise.all(failed.map(j => j.retry()))` | [#17](https://github.com/avifenesh/glide-mq/issues/17) |
| `queue.getWorkers()` | Not available | [#18](https://github.com/avifenesh/glide-mq/issues/18) |
| `queue.getJobScheduler(name)` | `(await queue.getRepeatableJobs()).find(s => s.name === name)` | [#19](https://github.com/avifenesh/glide-mq/issues/19) |
| `worker.on('active')` | Wrap the processor function - see [Worker section](#worker) | [#20](https://github.com/avifenesh/glide-mq/issues/20) |
| `worker.on('drained')` | Poll `queue.getJobCounts()` - see [Worker section](#worker) | [#20](https://github.com/avifenesh/glide-mq/issues/20) |
| Sandboxed processor | `new Worker('q', './processor.js', { connection, sandbox: {} })` | Full - see [Worker section](#worker) |
| Custom `jobId` | Use `deduplication.id` for idempotent creation | - |
| `lifo` | Use `priority` values in reverse insertion order | - |
| QueueEvents `'waiting'`, `'active'`, `'delayed'`, `'drained'`, `'deduplicated'` events | Use the worker-level events or poll `getJobCounts()` | - |
| `@nestjs/bullmq` integration | Not yet supported - use glide-mq directly | - |
| `failParentOnFailure` in FlowJob | Implement manually in the worker's `failed` handler | - |

---

## What glide-mq adds

Beyond BullMQ parity, glide-mq provides:

**1 RTT per job** - `completeAndFetchNext` is a single FCALL that atomically marks the current job complete and fetches the next one. BullMQ uses 2-3 round-trips for the same operation.

**Cluster-native from day one** - All keys use `glide:{queueName}:*` hash tags. Cross-slot operations (flows, global concurrency, ordering) work correctly in Valkey Cluster without any configuration.

**AZ-affinity routing** - Pin worker reads to replicas in your availability zone to reduce cross-AZ network cost and latency:

```ts
const connection = {
  addresses: [{ host: 'cluster.cache.amazonaws.com', port: 6379 }],
  clusterMode: true,
  readFrom: 'AZAffinity',
  clientAz: 'us-east-1a',
};
```

**IAM authentication** - Native AWS ElastiCache and MemoryDB IAM auth with automatic token refresh. No credential management required.

**Transparent compression** - `compression: 'gzip'` on `QueueOptions` compresses all payloads server-side transparently.

**Built-in DLQ** - `deadLetterQueue` on `QueueOptions` routes permanently failed jobs to a named queue without any application-level code.

**Job revocation** - `queue.revoke(jobId)` and `job.abortSignal` allow in-flight jobs to be cancelled cooperatively.

**Per-key ordering** - `opts.ordering.key` guarantees sequential execution per key across any number of workers without a separate lock system. Group concurrency and per-group rate limiting are also supported.

**Cost-based token bucket** - `opts.ordering.tokenBucket` with per-job `opts.cost` enables weighted rate limiting per ordering key. Unlike BullMQ Pro's count-based group rate limiting, glide-mq's token bucket assigns a cost to each job and deducts from a continuously refilling bucket. Open-source, no Pro license required.

**Global rate limiting** - `queue.setGlobalRateLimit()` caps queue-wide throughput across all workers. Stored in Valkey and picked up dynamically.

**`addBulk` via GLIDE Batch API** - 12.7x faster than serial `add()` calls for bulk enqueue operations.

---

## Testing without a server

glide-mq ships an in-memory test mode that requires no Valkey or Redis instance:

```ts
import { TestQueue, TestWorker } from 'glide-mq/testing';

// In your tests:
const queue = new TestQueue<{ email: string }, { sent: boolean }>('tasks');
const worker = new TestWorker('tasks', async (job) => {
  return { sent: true };
});

await queue.add('send-email', { email: 'user@example.com' });
await worker.process(); // runs all pending jobs synchronously

const jobs = queue.getJobs('completed');
// [{ data: { email: 'user@example.com' }, returnvalue: { sent: true } }]
```

BullMQ has no equivalent. You would typically use a real Redis instance or `ioredis-mock`.

---

## Workflow helpers

Beyond `FlowProducer`, glide-mq provides higher-level helpers:

```ts
import { chain, group, chord } from 'glide-mq';

const connection = { addresses: [{ host: 'localhost', port: 6379 }] };

// chain: sequential pipeline - each job is a child of the next
await chain([
  { name: 'step-1', queueName: 'tasks', data: {} },
  { name: 'step-2', queueName: 'tasks', data: {} },
  { name: 'step-3', queueName: 'tasks', data: {} },
], { connection });

// group: parallel fan-out - all jobs run concurrently, parent waits
await group([
  { name: 'shard-1', queueName: 'tasks', data: {} },
  { name: 'shard-2', queueName: 'tasks', data: {} },
], { name: 'aggregate', queueName: 'tasks', data: {} }, { connection });

// chord: group then callback
await chord(
  { name: 'callback', queueName: 'tasks', data: {} },
  [
    { name: 'task-1', queueName: 'tasks', data: {} },
    { name: 'task-2', queueName: 'tasks', data: {} },
  ],
  { connection },
);
```

---

## NestJS

`@nestjs/bullmq` is not yet supported. There is no official glide-mq NestJS package.

In the meantime, use glide-mq directly with NestJS by creating queue and worker instances in providers:

```ts
import { Injectable, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { Queue, Worker } from 'glide-mq';

const connection = { addresses: [{ host: 'localhost', port: 6379 }] };

@Injectable()
export class TaskQueue implements OnModuleInit, OnModuleDestroy {
  private queue = new Queue('tasks', { connection });
  private worker = new Worker('tasks', async (job) => {
    // process job
  }, { connection, concurrency: 10 });

  async add(data: unknown) {
    return this.queue.add('task', data);
  }

  async onModuleDestroy() {
    await this.worker.close();
    await this.queue.close();
  }
}
```

---

## Migration checklist

Work through this after completing your migration:

- [ ] Replace `bullmq` with `glide-mq` in `package.json`
- [ ] Update all imports from `'bullmq'` to `'glide-mq'`
- [ ] Convert all connection configs from `{ host, port }` to `{ addresses: [{ host, port }] }`
- [ ] Replace `opts.repeat` with `queue.upsertJobScheduler()`
- [ ] Replace `opts.jobId` with `opts.deduplication.id` where idempotency is needed
- [ ] Replace `settings.backoffStrategy` with `backoffStrategies` map in `WorkerOptions`
- [ ] Remove `QueueScheduler` instantiation (not needed)
- [ ] Remove `defaultJobOptions` from `QueueOptions`; apply options per job or via a wrapper
- [ ] Replace `queue.getJobs([...types])` with per-type calls
- [ ] Verify any `worker.on('active')` / `worker.on('drained')` handlers are replaced with workarounds
- [ ] Replace `job.waitUntilFinished(queueEvents, ttl)` with `job.waitUntilFinished(pollMs, timeoutMs)`
- [ ] Check `QueueEvents` listeners for removed events (`'waiting'`, `'active'`, `'delayed'`, `'drained'`)
- [ ] Run your test suite: `npm test`
- [ ] Confirm queue counts look correct: `await queue.getJobCounts()`
- [ ] Confirm no jobs are stuck in `active` state: stall detection is running
