---
name: glide-mq-migrate-bee
description: >-
  Use when moving a Node.js project from Bee-Queue to glide-mq: converting the
  chained job builder to options, splitting Queue and Worker, and mapping
  settings, methods and events.
license: Apache-2.0
metadata:
  author: glide-mq
  version: "0.15.5"
  tags: bee-queue, migration, glide-mq, valkey, redis, job-queue
  sources: docs/USAGE.md
---

# Migrate from Bee-Queue to glide-mq

Bee-Queue uses one `Queue` class for producing and consuming, a chained job builder, and unnamed jobs. glide-mq separates `Queue` (or `Producer`) from `Worker`, takes job options as an object, and requires a job name. The tables below map every setting, method and event; the steps show each conversion. Work through them, then run the project's tests.

The payoff is what Bee-Queue does not have: priorities, flows and DAGs, schedulers, rate limiting, batch workers, broadcast, cluster mode and IAM auth (see [What You Gain](#what-you-gain)).

## Breaking Changes Summary

| Feature | Bee-Queue | glide-mq |
|---------|-----------|----------|
| Queue + Worker | Single `Queue` class | Separate `Queue` (producer) and `Worker` (consumer) |
| Job creation | `queue.createJob(data).save()` (chained) | `queue.add(name, data, opts)` (single call) |
| Job name | Not used - no name parameter | **Required** first argument to `queue.add()` |
| Job options | Chained: `.timeout(ms).retries(n)` | Options object: `{ attempts, backoff, delay }` |
| Retries | `.retries(n)` | `{ attempts: n }` (different name!) |
| Processing | `queue.process(concurrency, handler)` | `new Worker(name, handler, { concurrency })` |
| Connection | `redis: { host, port }` or a redis URL | `connection: { addresses: [{ host, port }] }` |
| Progress | `job.reportProgress(anyJSON)` | `job.updateProgress(number \| object)` (number 0-100 or object) |
| Per-job events | `job.on('succeeded', ...)` | `QueueEvents` class (centralized) |
| Stall detection | Manual `checkStalledJobs()` | Automatic on Worker |
| Batch save | `queue.saveAll(jobs)` | `queue.addBulk(jobs)` |
| Producer-only | `{ isWorker: false }` | `Producer` class or just `Queue` |

## Queue Settings Mapping

| Bee-Queue Setting | Default | glide-mq Equivalent | Notes |
|-------------------|---------|---------------------|-------|
| `redis` | `{}` | `connection: { addresses: [...] }` | Array of `{ host, port }` objects |
| `isWorker` | `true` | Use `Producer` or `Queue` class | Separate classes replace flag |
| `getEvents` | `true` | Use `QueueEvents` class | Separate class for event subscription |
| `sendEvents` | `true` | `events: true` on Worker | Controls lifecycle event emission |
| `storeJobs` | `true` | Always true | glide-mq always stores jobs |
| `ensureScripts` | `true` | Automatic | Server Functions loaded automatically |
| `activateDelayedJobs` | `false` | Automatic | Server-side delayed job activation |
| `removeOnSuccess` | `false` | `{ removeOnComplete: true }` | Per-job option on `queue.add()` |
| `removeOnFailure` | `false` | `{ removeOnFail: true }` | Per-job option on `queue.add()` |
| `stallInterval` | `5000` | `lockDuration` on Worker | Lock-based stall detection |
| `nearTermWindow` | `20min` | N/A | Valkey-native delayed processing |
| `delayedDebounce` | `1000` | N/A | Server-side scheduling |
| `prefix` | `'bq'` | `prefix` on Queue | Default: `'glide'` |
| `quitCommandClient` | `true` | Automatic | Handled by graceful shutdown |
| `redisScanCount` | `100` | N/A | Different key strategy |

## Queue Method Mapping

| Bee-Queue Method | glide-mq Equivalent | Notes |
|------------------|---------------------|-------|
| `queue.createJob(data)` | `queue.add(name, data, opts)` | Name is required; returns Job not builder |
| `queue.process(n, handler)` | `new Worker(name, handler, { concurrency: n })` | Separate class |
| `queue.checkStalledJobs(interval)` | Automatic on Worker | No manual call needed |
| `queue.checkHealth()` | `queue.getJobCounts()` | Returns `{ waiting, active, completed, failed, delayed }` |
| `queue.close()` | `gracefulShutdown([...])` | Or individual `.close()` calls |
| `queue.ready()` | `worker.waitUntilReady()` | On Worker, not Queue |
| `queue.isRunning()` | `worker.isRunning()` | On Worker |
| `queue.getJob(id)` | `queue.getJob(id)` | Same API |
| `queue.getJobs(type, page)` | `queue.getJobs(type, start, end)` | Range-based pagination |
| `queue.removeJob(id)` | `(await queue.getJob(id)).remove()` | Via Job instance |
| `queue.saveAll(jobs)` | `queue.addBulk(jobs)` | Different input format |
| `queue.destroy()` | `queue.obliterate()` | Removes all queue data |

## Event Mapping

| Bee-Queue Event | Source | glide-mq Equivalent | Source |
|-----------------|--------|---------------------|--------|
| `queue.on('ready')` | Queue | `worker.waitUntilReady()` | Worker |
| `queue.on('error', err)` | Queue | `worker.on('error', err)` | Worker |
| `queue.on('succeeded', job, result)` | Queue (local) | `worker.on('completed', job, result)` | Worker |
| `queue.on('retrying', job, err)` | Queue (local) | `worker.on('failed', job, err)` | Worker (with retries remaining) |
| `queue.on('failed', job, err)` | Queue (local) | `worker.on('failed', job, err)` | Worker |
| `queue.on('stalled', jobId)` | Queue | `worker.on('stalled', jobId)` | Worker |
| `queue.on('job succeeded', id, result)` | Queue (PubSub) | `events.on('completed', { jobId })` | QueueEvents |
| `queue.on('job failed', id, err)` | Queue (PubSub) | `events.on('failed', { jobId })` | QueueEvents |
| `queue.on('job retrying', id, err)` | Queue (PubSub) | No direct equivalent | Use `events.on('failed')` + retry check |
| `queue.on('job progress', id, data)` | Queue (PubSub) | `events.on('progress', { jobId, data })` | QueueEvents |
| `job.on('succeeded', result)` | Job | `events.on('completed', { jobId })` | QueueEvents (filter by jobId) |
| `job.on('failed', err)` | Job | `events.on('failed', { jobId })` | QueueEvents (filter by jobId) |
| `job.on('progress', data)` | Job | `events.on('progress', { jobId })` | QueueEvents (filter by jobId) |

Per-job events (`job.on(...)`) do not exist in glide-mq. Use `QueueEvents` and filter by `jobId`, or use `queue.addAndWait()` for request-reply patterns.

## Step-by-Step Conversion

### 1. Connection

```typescript
// BEFORE (Bee-Queue)
const Queue = require('bee-queue');
const queue = new Queue('tasks', {
  redis: { host: 'localhost', port: 6379 }
});

// AFTER (glide-mq)
import { Queue, Worker } from 'glide-mq';
const connection = { addresses: [{ host: 'localhost', port: 6379 }] };
const queue = new Queue('tasks', { connection });
```

### 2. Job Creation (Biggest Change)

Bee-Queue uses chained builder with no job name. glide-mq uses a single call with a required name.

```typescript
// BEFORE (Bee-Queue) - chained builder, no name
const job = await queue.createJob({ email: 'user@example.com' })
  .retries(3)
  .backoff('exponential', 1000)
  .delayUntil(Date.now() + 60000)
  .setId('unique-123')
  .save();

// AFTER (glide-mq) - options object, name required
await queue.add('send-email',
  { email: 'user@example.com' },
  {
    attempts: 3,  // NOT "retries" - different name!
    backoff: { type: 'exponential', delay: 1000 },
    delay: 60000,
    jobId: 'unique-123',
  }
);
```

### 3. Worker

```typescript
// BEFORE (Bee-Queue)
queue.process(10, async (job) => {
  return { processed: true };
});
queue.on('succeeded', (job, result) => console.log('Done:', result));

// AFTER (glide-mq) - separate Worker class
const worker = new Worker('tasks', async (job) => {
  return { processed: true };
}, { connection, concurrency: 10 });
worker.on('completed', (job, result) => console.log('Done:', result));
```

### 4. Batch Save

```typescript
// BEFORE (Bee-Queue)
const jobs = items.map(item => queue.createJob(item));
await queue.saveAll(jobs);

// AFTER (glide-mq) - each entry needs a name
await queue.addBulk(items.map(item => ({
  name: 'process',
  data: item
})));
```

### 5. Producer-Only

```typescript
// BEFORE (Bee-Queue) - disable worker mode
const queue = new Queue('tasks', {
  isWorker: false, getEvents: false, sendEvents: false,
  redis: { host: 'localhost', port: 6379 }
});

// AFTER (glide-mq) - Producer class
import { Producer } from 'glide-mq';
const producer = new Producer('tasks', { connection });
await producer.add('job-name', data);
await producer.close();
```

### 6. Progress Reporting

```typescript
// BEFORE (Bee-Queue) - arbitrary JSON
queue.process(async (job) => {
  job.reportProgress({ percent: 50, message: 'halfway' });
  return result;
});

// AFTER (glide-mq) - number (0-100) or object
const worker = new Worker('tasks', async (job) => {
  await job.updateProgress(50);
  await job.updateProgress({ page: 3, total: 10 });  // objects also supported
  await job.log('halfway done');  // structured info goes to job.log()
  return result;
}, { connection });
```

### 7. Stall Detection

```typescript
// BEFORE (Bee-Queue) - manual setup required
const queue = new Queue('tasks', { stallInterval: 5000 });
queue.checkStalledJobs(5000);  // must call manually!

// AFTER (glide-mq) - automatic on Worker
const worker = new Worker('tasks', processor, {
  connection,
  lockDuration: 30000,
  stalledInterval: 30000,
  maxStalledCount: 2
});
// Stall detection runs automatically - no manual call
```

### 8. Health Check

```typescript
// BEFORE (Bee-Queue)
const health = await queue.checkHealth();
// { waiting, active, succeeded, failed, delayed, newestJob }

// AFTER (glide-mq)
const counts = await queue.getJobCounts();
// { waiting, active, completed, failed, delayed }
```

### 9. Web UI (Arena to Dashboard)

```typescript
// BEFORE (Bee-Queue) - Arena
const Arena = require('bull-arena');
app.use('/', Arena({ Bee: require('bee-queue'), queues: [{ name: 'tasks' }] }));

// AFTER (glide-mq) - Dashboard
import { createDashboard } from '@glidemq/dashboard';
app.use('/dashboard', createDashboard([queue]));
```

## What You Gain

Features Bee-Queue does not have that are available after migration:

| Feature | glide-mq API |
|---------|-------------|
| Priority queues | `{ priority: 0 }` (lower = higher, 0 is highest) |
| FlowProducer | Parent-child job trees and DAG workflows |
| Broadcast | Fan-out with subscriber groups |
| Batch processing | Process multiple jobs per worker call |
| Deduplication | Simple, throttle, and debounce modes |
| Schedulers | Cron patterns and interval repeatable jobs |
| Rate limiting | `limiter: { max: 100, duration: 60000 }` on Worker |
| LIFO mode | Process newest jobs first with `{ lifo: true }` |
| Dead letter queue | `deadLetterQueue: { name: 'dlq' }` on Queue |
| Serverless pool | Connection caching for Lambda/Edge |
| HTTP proxy | Cross-language queue access via REST |
| OpenTelemetry | Automatic span emission |
| Testing utilities | `TestQueue`/`TestWorker` without Valkey |
| Cluster support | Hash-tagged keys, AZ-affinity routing |
| TLS / IAM auth | `useTLS: true`, IAM credentials for ElastiCache |
| Native TypeScript | Full generic type support throughout |
| **AI usage tracking** | `job.reportUsage({ model, tokens, costs, ... })` |
| **Token streaming** | `job.stream()` / `queue.readStream()` for real-time LLM output |
| **Suspend/resume** | `job.suspend()` / `queue.signal()` for human-in-the-loop |
| **Flow budget** | `flow.add(tree, { budget: { maxTotalTokens } })` |
| **Fallback chains** | `opts.fallbacks: [{ model, provider }]` |
| **Dual-axis rate limiting** | `tokenLimiter` for RPM + TPM compliance |
| **Vector search** | `queue.createJobIndex()` / `queue.vectorSearch()` |

## Migration Checklist

```
- [ ] Install glide-mq, uninstall bee-queue (and @types/bee-queue if the project added it)
- [ ] Create connection config (addresses array format)
- [ ] Convert queue.createJob().save() to queue.add(name, data, opts)
- [ ] Add job names to every queue.add() call (Bee-Queue had none)
- [ ] Convert .retries(n) to { attempts: n } (different name!)
- [ ] Convert .backoff(strategy, delay) to { backoff: { type, delay } }
- [ ] Convert .delayUntil(date) to { delay: ms }
- [ ] Convert .setId(id) to { jobId: id }
- [ ] Convert queue.process() to new Worker()
- [ ] Convert queue.saveAll() to queue.addBulk()
- [ ] Separate producer queues (isWorker:false to Producer class)
- [ ] Convert job.reportProgress(json) to job.updateProgress(number | object)
- [ ] Remove manual checkStalledJobs() calls (automatic on Worker)
- [ ] Convert checkHealth() to getJobCounts()
- [ ] Update event listeners (queue.on to worker.on or QueueEvents)
- [ ] Convert per-job events (job.on) to QueueEvents
- [ ] Keep the project's existing module system (CommonJS or ESM)
- [ ] Run full test suite
- [ ] Confirm queue counts: await queue.getJobCounts()
- [ ] Confirm no jobs stuck in active state
- [ ] Smoke-test QueueEvents or SSE listeners if the app exposes them
- [ ] Confirm workers, queues, and connections close cleanly
```

## Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `queue.createJob is not a function` | API changed | Use `queue.add(name, data, opts)` |
| `queue.process is not a function` | Separated producer/consumer | Use `new Worker(name, handler, opts)` |
| `Cannot use require()` | Module system mismatch | Keep the project's existing module system; glide-mq supports CommonJS and ESM |
| `job.reportProgress is not a function` | API renamed | Use `job.updateProgress(number)` |
| `Cannot find module 'bee-queue'` | Leftover import | `grep -r "bee-queue" src/` to find remaining |
| `Missing job name` | Bee-Queue had no name | Add a name as first arg to `queue.add()` |
| `retries option not recognized` | Different name | Use `attempts` not `retries` |
| No stall detection | Bee-Queue needed manual start | glide-mq runs it automatically on Worker |
| Progress type changed | Bee-Queue accepted any JSON | Use `job.updateProgress(number \| object)` - numbers (0-100) or objects supported |
| Per-job events not working | No per-job events in glide-mq | Use `QueueEvents` class and filter by `jobId` |

## Quick Start Commands

```bash
npm uninstall bee-queue   # plus @types/bee-queue if present
npm install glide-mq
```

## References

| Document | Content |
|----------|---------|
| [references/api-mapping.md](references/api-mapping.md) | Complete method-by-method API mapping |
| [references/new-features.md](references/new-features.md) | Features available after migration |
