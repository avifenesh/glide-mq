# glide-mq

High-performance message queue for Node.js, built on Valkey/Redis Streams with [Valkey GLIDE](https://github.com/valkey-io/valkey-glide) native NAPI bindings.

## Performance

### Processing throughput

| Concurrency | Throughput |
|-------------|-----------|
| c=1 | 4,376 jobs/s |
| c=5 | 14,925 jobs/s |
| c=10 | 15,504 jobs/s |
| c=50 | 48,077 jobs/s |

### Bulk add (addBulk with Batch API)

| Jobs | Serial | Batch | Speedup |
|------|--------|-------|---------|
| 200 | 76ms | 14ms | 5.4x |
| 1,000 | 228ms | 18ms | 12.7x |

### Payload compression

| Mode | Stored size (15KB payload) | Savings |
|------|--------------------------|---------|
| Plain | 15,327 bytes | - |
| Gzip | 331 bytes | 98% |

No-op processor, Valkey 8.0, single node.

## Why

- **Streams-first** - Redis Streams + consumer groups + PEL instead of Lists + BRPOPLPUSH. Fewer moving parts, built-in at-least-once delivery.
- **Server Functions** - single `FUNCTION LOAD` instead of dozens of EVAL scripts. Persistent across restarts, no NOSCRIPT cache-miss errors.
- **1 RTT per job** - `completeAndFetchNext` combines job completion + next job fetch + activation in a single FCALL round trip.
- **Cluster-native** - hash-tagged keys from day one. No afterthought `{braces}` requirement.
- **Native bindings** - built on [@glidemq/speedkey](https://github.com/avifenesh/speedkey) (valkey-glide with Rust core + NAPI).
- **AZ-Affinity** - route reads to same-AZ replicas, reducing cross-AZ latency and AWS costs by up to 75%.
- **Batch pipelining** - `addBulk` uses GLIDE's Batch API for single round-trip bulk operations (12.7x faster than serial).
- **Transparent compression** - gzip payloads with zero-config decompression on workers (98% savings on repetitive data).

## Install

```bash
npm install glide-mq
```

Requires Node.js 20+ and a running Valkey (7.0+) or Redis (7.0+) instance for FUNCTION support.

## Quick Start

```typescript
import { Queue, Worker } from 'glide-mq';

const connection = { addresses: [{ host: 'localhost', port: 6379 }] };

// Producer
const queue = new Queue('tasks', { connection });
await queue.add('send-email', { to: 'user@example.com', subject: 'Hello' });

// Consumer
const worker = new Worker('tasks', async (job) => {
  console.log(`Processing ${job.name}: ${JSON.stringify(job.data)}`);
  return { sent: true };
}, { connection, concurrency: 10 });

worker.on('completed', (job) => console.log(`Job ${job.id} completed`));
worker.on('failed', (job, err) => console.log(`Job ${job.id} failed: ${err.message}`));
```

## Features

### Core
- **Queue** - add, addBulk, getJob, getJobs, pause, resume, obliterate, drain
- **Worker** - concurrent processing, XREADGROUP polling, graceful shutdown
- **Job** - progress, updateData, retry, remove, log, state queries, waitUntilFinished

### Reliability
- **Retries** - fixed, exponential, jitter backoff + custom strategy functions
- **Stalled recovery** - XAUTOCLAIM with heartbeat-based lock renewal
- **Per-job timeout** - automatic failure if processor exceeds timeout
- **Dead letter queue** - permanently failed jobs routed to a separate queue

### Advanced
- **Deduplication** - simple, throttle, debounce modes
- **Rate limiting** - sliding window, per-queue
- **Global concurrency** - limit active jobs across all workers
- **Job retention** - removeOnComplete/removeOnFail (count, age-based)
- **Priorities** - encoded in sorted set scores, FIFO within same priority
- **Compression** - transparent gzip for job payloads (Node.js zlib, zero deps)

### Workflows
- **FlowProducer** - atomic parent-child job trees with nested flows
- **chain(queue, jobs)** - sequential pipeline, each job receives previous result
- **group(queue, jobs)** - parallel execution, parent completes when all children done
- **chord(queue, group, callback)** - run group, then callback with all results

### Observability
- **QueueEvents** - stream-based event subscription (added, completed, failed, stalled, etc.)
- **Job schedulers** - cron patterns and fixed intervals for repeatable jobs
- **Metrics** - getJobCounts, getMetrics
- **OpenTelemetry** - automatic tracing spans for Queue.add and FlowProducer.add operations (optional peer dependency)

### Cloud-Native (GLIDE-exclusive)
- **AZ-Affinity routing** - route reads to same-AZ replicas for lower latency and reduced cross-AZ costs
- **IAM authentication** - native AWS ElastiCache/MemoryDB auth with auto-token refresh
- **Batch API** - single round-trip bulk operations via GLIDE's non-atomic pipeline
- **Multiplexed connections** - single connection per node instead of connection pools

### Operations
- **Graceful shutdown** - SIGTERM/SIGINT handler, waits for active jobs
- **Connection recovery** - exponential backoff reconnect with function library reload
- **Job revocation** - cooperative cancellation via AbortSignal
- **Cluster mode** - all features work in Valkey Cluster (pass `clusterMode: true`)

## API

### Queue

```typescript
const queue = new Queue('name', {
  connection: {
    addresses: [{ host, port }],
    clusterMode: false,
    readFrom: ReadFrom.AZAffinity,  // route reads to same-AZ replicas
    clientAz: 'us-east-1a',
    credentials: { password: 'secret' },
    // or IAM: { type: 'iam', serviceType: 'elasticache', region: 'us-east-1', userId: 'user', clusterName: 'my-cluster' }
  },
  prefix: 'glide',
  compression: 'gzip',  // transparent payload compression
});

await queue.add('jobName', data, {
  delay: 5000,        // delayed job (ms)
  priority: 1,        // lower = higher priority
  attempts: 3,        // max retry attempts
  backoff: { type: 'exponential', delay: 1000 },
  timeout: 30000,     // per-job timeout (ms)
  removeOnComplete: true,  // or count, or { age, count }
  deduplication: { id: 'unique-key', mode: 'simple' },
});

// Bulk add - 12.7x faster than serial via Batch API
await queue.addBulk([
  { name: 'job1', data: { a: 1 } },
  { name: 'job2', data: { a: 2 } },
]);

await queue.pause();
await queue.resume();
await queue.getJobCounts();  // { waiting, active, delayed, completed, failed }
await queue.obliterate({ force: true });
await queue.close();
```

### Worker

```typescript
const worker = new Worker('name', async (job) => {
  await job.log('Starting processing');
  await job.updateProgress(50);
  return result;
}, {
  connection,
  concurrency: 10,
  blockTimeout: 5000,
  stalledInterval: 30000,
  lockDuration: 30000,
  limiter: { max: 100, duration: 60000 },
  deadLetterQueue: { name: 'failed-jobs' },
  backoffStrategies: {
    custom: (attemptsMade, err) => attemptsMade * 1000,
  },
});

worker.on('completed', (job, result) => {});
worker.on('failed', (job, error) => {});
await worker.close();
```

### Flows

```typescript
import { FlowProducer, chain, group, chord } from 'glide-mq';

// Parent-child
const flow = new FlowProducer({ connection });
await flow.add({
  name: 'parent', queueName: 'tasks', data: {},
  children: [
    { name: 'child1', queueName: 'tasks', data: {} },
    { name: 'child2', queueName: 'tasks', data: {} },
  ],
});

// Chain: A -> B -> C (sequential, each step receives previous result)
await chain('tasks', [
  { name: 'step1', data: {} },
  { name: 'step2', data: {} },
], connection);

// Group: A + B + C (parallel, parent completes when all children done)
await group('tasks', [
  { name: 'task1', data: {} },
  { name: 'task2', data: {} },
], connection);

// Chord: run group in parallel, then callback with all results
await chord('tasks', [
  { name: 'task1', data: {} },
  { name: 'task2', data: {} },
], { name: 'aggregate', data: {} }, connection);
```

### Events

```typescript
import { QueueEvents } from 'glide-mq';

const events = new QueueEvents('tasks', { connection });
events.on('completed', ({ jobId, returnvalue }) => {});
events.on('failed', ({ jobId, failedReason }) => {});
events.on('stalled', ({ jobId }) => {});
```

### OpenTelemetry

```typescript
// Install optional peer dependency
// npm install @opentelemetry/api

// Automatic tracing for Queue.add and FlowProducer.add
const queue = new Queue('tasks', { connection });
await queue.add('job', data);  // Creates span: glide-mq.queue.add

// Custom tracer (optional)
import { setTracer } from 'glide-mq';
setTracer(customTracerInstance);
```

### Graceful Shutdown

```typescript
import { gracefulShutdown } from 'glide-mq';

const queue = new Queue('tasks', { connection });
const worker = new Worker('tasks', processor, { connection });
const events = new QueueEvents('tasks', { connection });

// Registers SIGTERM/SIGINT handlers and resolves when all components are closed
await gracefulShutdown([queue, worker, events]);
```

## Cluster Mode

```typescript
const connection = {
  addresses: [{ host: 'cluster-node', port: 7000 }],
  clusterMode: true,
  readFrom: ReadFrom.AZAffinity,
  clientAz: 'us-east-1a',
};

// Everything works the same - keys are hash-tagged automatically
const queue = new Queue('tasks', { connection });
```

## Testing Mode

glide-mq ships a built-in in-memory backend so you can unit-test your job processors **without a running Valkey instance**.

```typescript
import { TestQueue, TestWorker } from 'glide-mq/testing';

const queue = new TestQueue('tasks');

const worker = new TestWorker(queue, async (job) => {
  return { processed: job.data };
});

worker.on('completed', (job, result) => {
  console.log(`Job ${job.id} done:`, result);
});

worker.on('failed', (job, err) => {
  console.error(`Job ${job.id} failed:`, err.message);
});

await queue.add('send-email', { to: 'user@example.com' });

// Inspect state without touching Valkey
const counts = await queue.getJobCounts();
// { waiting: 0, active: 0, delayed: 0, completed: 1, failed: 0 }

// Search jobs by name or data fields
const jobs = await queue.searchJobs({ name: 'send-email', state: 'completed' });

await worker.close();
await queue.close();
```

`TestQueue` and `TestWorker` mirror the real `Queue` / `Worker` public API (add, addBulk, getJob, getJobs, getJobCounts, pause, resume, events, retries, concurrency), making it straightforward to swap implementations between test and production code.

## Dashboard

glide-mq exposes a REST + Server-Sent Events API that can be consumed by the [`@glidemq/dashboard`](https://github.com/avifenesh/glidemq-dashboard) UI package.

### Quick start with the built-in demo server

```bash
cd demo
npm install
npm run dashboard   # starts http://localhost:3000
```

### REST endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/queues` | List all queues with counts and metrics |
| `GET` | `/api/queues/:name` | Queue details + recent jobs |
| `GET` | `/api/queues/:name/jobs/:id` | Single job details, state, logs |
| `POST` | `/api/queues/:name/jobs` | Add a new job |
| `POST` | `/api/queues/:name/pause` | Pause a queue |
| `POST` | `/api/queues/:name/resume` | Resume a queue |
| `POST` | `/api/queues/:name/jobs/:id/retry` | Retry a failed job |
| `DELETE` | `/api/queues/:name/jobs/:id` | Remove a job |
| `POST` | `/api/queues/:name/drain` | Drain all waiting jobs |
| `POST` | `/api/queues/:name/obliterate` | Obliterate queue and all data |
| `GET` | `/api/events` | SSE stream for real-time job events |

### Real-time events via SSE

```javascript
const es = new EventSource('http://localhost:3000/api/events');
es.onmessage = ({ data }) => {
  const { queue, event, jobId } = JSON.parse(data);
  // event: 'added' | 'completed' | 'failed' | 'progress' | 'stalled' | 'heartbeat'
  console.log(`[${queue}] ${event} – job ${jobId}`);
};
```

### Embedding the dashboard server in your own Express app

```typescript
import express from 'express';
import { Queue, QueueEvents } from 'glide-mq';

const app = express();
app.use(express.json());

const queues: Record<string, Queue> = {
  orders: new Queue('orders', { connection }),
  payments: new Queue('payments', { connection }),
};

app.get('/api/queues', async (_req, res) => {
  const data = await Promise.all(
    Object.entries(queues).map(async ([name, q]) => ({
      name,
      counts: await q.getJobCounts(),
      isPaused: await q.isPaused(),
    })),
  );
  res.json(data);
});
```

## License

Apache-2.0
