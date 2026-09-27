---
name: glide-mq-migrate-bullmq
description: >-
  Use when moving a Node.js project from BullMQ (or BullMQ Pro groups) to
  glide-mq, or comparing the two APIs: connection format, changed signatures,
  removed options, and what replaces them.
license: Apache-2.0
metadata:
  author: glide-mq
  version: "0.15.5"
  tags: glide-mq, bullmq, migration, queue, valkey, redis
  sources: docs/MIGRATION.md
---

# Migrate from BullMQ to glide-mq

The glide-mq API is intentionally similar to BullMQ. Most of a migration is the connection format and imports; the table below lists every change that breaks code, and the steps show each conversion. Work through them in order, then run the project's tests.

## Prerequisites

- Node.js 20+
- Valkey 7.0+ or Redis 7.0+ (both supported)
- TypeScript 5+ recommended

## Install

```bash
npm remove bullmq
npm install glide-mq
```

```ts
// Before
import { Queue, Worker, Job, QueueEvents, FlowProducer } from 'bullmq';

// After
import { Queue, Worker, Job, QueueEvents, FlowProducer } from 'glide-mq';
```

---

## Breaking changes

| Feature | BullMQ | glide-mq |
|---------|--------|----------|
| **Connection config** | `{ host, port }` | `{ addresses: [{ host, port }] }` |
| **TLS** | `tls: {}` | `useTLS: true` |
| **Password** | `password: 'secret'` | `credentials: { password: 'secret' }` |
| **Cluster mode** | Implicit / `natMap` | `clusterMode: true` |
| **`defaultJobOptions`** | On `QueueOptions` | Removed - wrap `queue.add()` with defaults |
| **`queue.getJobs()`** | Accepts array of types | Single type per call |
| **`queue.getJobCounts()`** | Variadic type list | Always returns all states |
| **`settings.backoffStrategy`** | Single function | `backoffStrategies` named map on WorkerOptions |
| **`worker.on('active')`** | Emits `(job, prev)` | Emits `(job, jobId)` |
| **`job.waitUntilFinished()`** | `(queueEvents, ttl)`, resolves to the result | `(pollIntervalMs, timeoutMs)`, resolves to `'completed'` or `'failed'` |
| **Sandboxed processor** | `useWorkerThreads: true` | `sandbox: { useWorkerThreads: true }` |
| **`QueueScheduler`** | Required in v1, optional in v2+ | Does not exist - promotion runs inside Worker |
| **`opts.repeat`** | On `queue.add()` | Removed - use `queue.upsertJobScheduler()` |
| **FlowJob `data`** | Optional | Required |
| **`retries-exhausted` event** | Separate QueueEvents event | Check `attemptsMade >= opts.attempts` in `'failed'` |
| **BullMQ Pro `group.id`** | `group: { id }` (Pro license) | `ordering: { key }` (open source) |
| **Group concurrency** | `group.limit.max` (Pro) | `ordering: { key, concurrency: N }` |
| **Group rate limit** | `group.limit` (Pro) | `ordering: { key, rateLimit: { max, duration } }` |

---

## Step-by-step conversion

### 1. Connection config (the biggest change)

```ts
// BEFORE (BullMQ)
const connection = { host: 'localhost', port: 6379 };
```

```ts
// AFTER (glide-mq)
const connection = { addresses: [{ host: 'localhost', port: 6379 }] };
```

For TLS + password + cluster, see [references/connection-mapping.md](references/connection-mapping.md).

### 2. Queue.add - identical API

```ts
// BEFORE
const queue = new Queue('tasks', { connection });
await queue.add('send-email', { to: 'user@example.com' });
```

```ts
// AFTER - only the connection changes
const queue = new Queue('tasks', { connection });
await queue.add('send-email', { to: 'user@example.com' });
```

### 3. Worker - identical API, different connection

```ts
// BEFORE
const worker = new Worker('tasks', async (job) => {
  await sendEmail(job.data.to);
}, { connection: { host: 'localhost', port: 6379 }, concurrency: 10 });
```

```ts
// AFTER
const worker = new Worker('tasks', async (job) => {
  await sendEmail(job.data.to);
}, { connection: { addresses: [{ host: 'localhost', port: 6379 }] }, concurrency: 10 });
```

### 4. FlowProducer - identical API

```ts
// Both - same usage, only connection format differs
const flow = new FlowProducer({ connection });
await flow.add({
  name: 'parent',
  queueName: 'tasks',
  data: { step: 'final' },       // NOTE: data is required in glide-mq
  children: [
    { name: 'child-1', queueName: 'tasks', data: { step: '1' } },
    { name: 'child-2', queueName: 'tasks', data: { step: '2' } },
  ],
});
```

### 5. QueueEvents - identical API

```ts
// Both - same, only connection format differs
const qe = new QueueEvents('tasks', { connection });
qe.on('completed', ({ jobId }) => console.log(jobId, 'done'));
qe.on('failed', ({ jobId, failedReason }) => console.error(jobId, failedReason));
```

Note: some BullMQ events are not yet emitted. See [Current gaps](#current-gaps).

### 6. Graceful shutdown

```ts
// BullMQ
await worker.close();
await queue.close();
```

```ts
// glide-mq - identical
await worker.close();
await queue.close();
```

### 7. UnrecoverableError - identical

```ts
// Both
import { UnrecoverableError } from 'glide-mq'; // was 'bullmq'

throw new UnrecoverableError('permanent failure');
```

### 8. Scheduling (repeatable jobs)

```ts
// BEFORE - opts.repeat (deprecated in BullMQ v5)
await queue.add('report', data, {
  repeat: { pattern: '0 9 * * *', tz: 'America/New_York' },
});
```

```ts
// AFTER - upsertJobScheduler
await queue.upsertJobScheduler(
  'report',
  { pattern: '0 9 * * *', tz: 'America/New_York' },
  { name: 'report', data: { v: 1 } },
);
```

### 9. Custom backoff strategies

```ts
// BEFORE
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
// AFTER
const worker = new Worker('q', processor, {
  connection,
  backoffStrategies: {
    jitter: (attemptsMade, err) => 1000 + Math.random() * 1000,
    linear: (attemptsMade, err) => 1000 * attemptsMade,
  },
});
```

### 10. defaultJobOptions removal

```ts
// BEFORE
const queue = new Queue('tasks', {
  connection,
  defaultJobOptions: { attempts: 3, backoff: { type: 'exponential', delay: 1000 } },
});
```

```ts
// AFTER - wrap add() with your defaults
const DEFAULTS = { attempts: 3, backoff: { type: 'exponential', delay: 1000 } } as const;
const add = (name: string, data: unknown, opts?: JobOptions) =>
  queue.add(name, data, { ...DEFAULTS, ...opts });
```

### 11. getJobs with multiple types

```ts
// BEFORE
const jobs = await queue.getJobs(['waiting', 'active'], 0, 99);
```

```ts
// AFTER
const [waiting, active] = await Promise.all([
  queue.getJobs('waiting', 0, 99),
  queue.getJobs('active', 0, 99),
]);
const jobs = [...waiting, ...active];
```

### 12. job.waitUntilFinished

```ts
// BEFORE
const qe = new QueueEvents('tasks', { connection });
const result = await job.waitUntilFinished(qe, 30000);
```

```ts
// AFTER - no QueueEvents needed
const state = await job.waitUntilFinished(500, 30000);
// args: pollIntervalMs (default 500), timeoutMs (default 30000)
// resolves to 'completed' or 'failed'; read the result with queue.getJob(job.id)
// or use queue.addAndWait() when you need the return value
```

### 13. BullMQ Pro groups to ordering keys

```ts
// BEFORE (BullMQ Pro)
await queue.add('job', data, {
  group: { id: 'tenant-123', limit: { max: 2, duration: 0 } },
});
```

```ts
// AFTER (glide-mq, open source)
await queue.add('job', data, {
  ordering: { key: 'tenant-123', concurrency: 2 },
});
```

---

## What's new in glide-mq (not in BullMQ)

| Feature | API | Description |
|---------|-----|-------------|
| Per-key ordering | `ordering: { key }` | Sequential execution per key across all workers |
| Group concurrency | `ordering: { key, concurrency: N }` | Max N parallel jobs per key |
| Group rate limit | `ordering: { key, rateLimit: { max, duration } }` | Per-key rate limiting |
| Token bucket | `ordering: { key, tokenBucket }` + `opts.cost` | Weighted rate limiting per key |
| Global rate limit | `queue.setGlobalRateLimit({ max, duration })` | Queue-wide cap across all workers |
| Dead letter queue | `deadLetterQueue: { name, maxRetries }` | Native DLQ on QueueOptions |
| Job revocation | `queue.revoke(jobId)` + `job.abortSignal` | Cancel in-flight jobs cooperatively |
| Transparent compression | `compression: 'gzip'` on QueueOptions | 98% reduction on 15 KB payloads |
| AZ-affinity routing | `readFrom: 'AZAffinity'` | Pin reads to local AZ replicas |
| IAM auth | `credentials: { type: 'iam', ... }` | ElastiCache / MemoryDB native auth |
| In-memory test mode | `TestQueue`, `TestWorker` from `glide-mq/testing` | No Valkey needed for tests |
| Broadcast | `BroadcastWorker` | Pub/sub fan-out to all workers |
| Batch processing | `batch: { size, timeout }` on WorkerOptions | Multiple jobs per processor call |
| DAG workflows | `FlowProducer.addDAG()`, `dag()` helper | Jobs with multiple parents |
| Workflow helpers | `chain()`, `group()`, `chord()` | Higher-level orchestration |
| Step jobs | `job.moveToDelayed(ts, nextStep?)` | Multi-step state machines |
| addAndWait | `queue.addAndWait(name, data, { waitTimeout })` | Request-reply pattern |
| Pluggable serializers | `{ serialize, deserialize }` on options | MessagePack, Protobuf, etc. |
| Job TTL | `opts.ttl` | Auto-expire jobs after N ms |
| repeatAfterComplete | `upsertJobScheduler('name', { repeatAfterComplete: 5000 })` | No-overlap scheduling (ms delay after completion) |
| LIFO mode | `lifo: true` | Last-in-first-out processing |
| Job search | `queue.searchJobs({ state, name, data })` | Filter by state, exact name and shallow data fields |
| excludeData | `queue.getJobs(type, start, end, { excludeData: true })` | Lightweight listings |
| `globalConcurrency` | On WorkerOptions | Set queue-wide cap at worker startup |
| **AI usage tracking** | `job.reportUsage({ model, tokens, costs, ... })` | Per-job LLM usage metadata |
| **Token streaming** | `job.stream({ token })` / `queue.readStream(jobId)` | Real-time LLM output via per-job streams |
| **Suspend/resume** | `job.suspend()` / `queue.signal(jobId, name, data)` | Human-in-the-loop approval |
| **Flow budget** | `flow.add(tree, { budget: { maxTotalTokens } })` | Cap tokens/cost across a flow |
| **Fallback chains** | `opts.fallbacks: [{ model, provider }]` | Ordered model/provider failover |
| **Dual-axis rate limiting** | `tokenLimiter: { maxTokens, duration }` | RPM + TPM for LLM API compliance |
| **Flow usage aggregation** | `queue.getFlowUsage(parentJobId)` | Aggregate tokens/cost across a flow |
| **Vector search** | `queue.createJobIndex()` / `queue.vectorSearch()` | KNN similarity search over job hashes |

See [references/new-features.md](references/new-features.md) for detailed documentation.

---

## Current gaps

| Missing feature | Workaround |
|-----------------|------------|
| QueueEvents `'waiting'`, `'active'`, `'delayed'`, `'drained'`, `'deduplicated'` events | Use worker-level events or poll `getJobCounts()` |
| `failParentOnFailure` in FlowJob | Implement manually in the worker's `failed` handler |

---

## Performance comparison

AWS ElastiCache Valkey 8.2 (r7g.large), TLS enabled, same-region EC2 client.

| Concurrency | glide-mq | BullMQ | Delta |
|:-----------:|----------:|--------:|:-----:|
| c=1 | 2,479 j/s | 2,535 j/s | -2% |
| c=5 | 10,754 j/s | 9,866 j/s | +9% |
| c=10 | **18,218 j/s** | 13,541 j/s | **+35%** |
| c=15 | **19,583 j/s** | 14,162 j/s | **+38%** |
| c=20 | 19,408 j/s | 16,085 j/s | +21% |
| c=50 | 19,768 j/s | 19,159 j/s | +3% |

Most production deployments run c=5 to c=20, where glide-mq's 1-RTT architecture pays off the most.

---

## Migration checklist

```
- [ ] Replace `bullmq` with `glide-mq` in package.json
- [ ] Update all imports from 'bullmq' to 'glide-mq'
- [ ] Convert connection configs: { host, port } -> { addresses: [{ host, port }] }
- [ ] Convert TLS: tls: {} -> useTLS: true
- [ ] Convert password: password -> credentials: { password }
- [ ] Replace opts.repeat with queue.upsertJobScheduler()
- [ ] Replace settings.backoffStrategy with backoffStrategies map
- [ ] Remove QueueScheduler instantiation (not needed)
- [ ] Remove defaultJobOptions from QueueOptions; apply per job or via wrapper
- [ ] Replace queue.getJobs([...types]) with per-type calls
- [ ] Update worker.on('active') handlers: (job, jobId) not (job, prev)
- [ ] Replace job.waitUntilFinished(queueEvents, ttl) with (pollMs, timeoutMs)
- [ ] Check QueueEvents listeners for removed events (waiting, active, delayed, drained)
- [ ] Replace group.id (BullMQ Pro) with ordering.key
- [ ] Run test suite: npm test
- [ ] Confirm queue counts: await queue.getJobCounts()
- [ ] Confirm no jobs stuck in active state
- [ ] Smoke-test QueueEvents or SSE listeners if the app exposes them
- [ ] Confirm workers, queues, and connections close cleanly
```

---

## Troubleshooting

TypeScript catches most leftovers at compile time: `defaultJobOptions`, `settings.backoffStrategy`, `opts.repeat`, an array passed to `getJobs()`, and `QueueScheduler` are not part of glide-mq's types. In plain JavaScript they fail or are ignored at runtime, as below.

| Symptom | Cause | Fix |
|-------|-------|-----|
| `ConnectionError: Failed to create standalone client: ...` on startup | BullMQ `{ host, port }` connection | `{ addresses: [{ host, port }] }` |
| Queue-level defaults (attempts, backoff) silently not applied | `defaultJobOptions` is not a glide-mq option | Wrap `queue.add()` with a helper that spreads defaults |
| Custom backoff ignored | `settings.backoffStrategy` | `backoffStrategies` map on WorkerOptions |
| `TypeError` reading `length` from `getJobs()` | Array of types passed | One `getJobs()` call per type, then combine |
| Import of `QueueScheduler` fails | glide-mq has no QueueScheduler | Remove it; the Worker promotes delayed jobs |
| Repeatable job never repeats | `opts.repeat` on `queue.add()` | `queue.upsertJobScheduler()` |
| `waitUntilFinished` times out or returns a state instead of a result | New `(pollMs, timeoutMs)` signature | Pass numbers; use `addAndWait()` for the return value |
| Job stuck in `active` | Worker crashed mid-job | Stall detection recovers stream jobs; for LIFO/priority jobs, `DEL glide:{queueName}:list-active` |
| `retries-exhausted` listener never fires | No such event | Listen to `'failed'` and check `attemptsMade >= opts.attempts` |
| `FlowProducer.add` throws `The "string" argument must be of type string ... Received undefined` | A FlowJob without `data` | Pass `data` on every node (use `{}` if empty) |
| `queue.add()` returned `null` | A job with that `jobId` already exists | Expected: duplicates are skipped |

## Full Documentation

- [Migration Guide](https://www.glidemq.dev/migration/from-bullmq)
- [New Features Reference](references/new-features.md)
- [Connection Mapping Reference](references/connection-mapping.md)
