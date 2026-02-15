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

Requires Node.js 20+ and a running Valkey (7.2+) or Redis (6.2+) instance.

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
- **OpenTelemetry** - optional spans for Queue.add, Worker.process, FlowProducer.add

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

// Chain: A -> B -> C (sequential)
await chain(connection, 'tasks', [
  { name: 'step1', data: {} },
  { name: 'step2', data: {} },
]);

// Group: A + B + C (parallel)
await group(connection, 'tasks', [
  { name: 'task1', data: {} },
  { name: 'task2', data: {} },
]);
```

### Events

```typescript
import { QueueEvents } from 'glide-mq';

const events = new QueueEvents('tasks', { connection });
events.on('completed', ({ jobId, returnvalue }) => {});
events.on('failed', ({ jobId, failedReason }) => {});
events.on('stalled', ({ jobId }) => {});
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

## License

Apache-2.0
