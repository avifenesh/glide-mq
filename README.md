# glide-mq

High-performance message queue for Node.js, built on Valkey/Redis Streams with [Valkey GLIDE](https://github.com/valkey-io/valkey-glide) native NAPI bindings.

## Performance

| Concurrency | Throughput |
|-------------|-----------|
| c=1 | 4,376 jobs/s |
| c=10 | 20,979 jobs/s |
| c=50 | 44,643 jobs/s |

No-op processor, Valkey 8.0, single node.

## Why

- **Streams-first** - uses Redis Streams + consumer groups + PEL instead of Lists + BRPOPLPUSH. Fewer moving parts, built-in at-least-once delivery.
- **Server Functions** - single `FUNCTION LOAD` instead of dozens of EVAL scripts. Persistent across restarts, no NOSCRIPT cache-miss errors.
- **1 RTT per job** - `completeAndFetchNext` combines job completion + next job fetch + activation in a single FCALL round trip.
- **Cluster-native** - hash-tagged keys from day one. No afterthought `{braces}` requirement.
- **Native bindings** - built on [@glidemq/speedkey](https://github.com/avifenesh/speedkey) (valkey-glide with Rust core + NAPI).

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

### Operations
- **Graceful shutdown** - SIGTERM/SIGINT handler, waits for active jobs
- **Connection recovery** - exponential backoff reconnect with function library reload
- **Job revocation** - cooperative cancellation via AbortSignal
- **Cluster mode** - all features work in Valkey Cluster (pass `clusterMode: true`)

## API

### Queue

```typescript
const queue = new Queue('name', {
  connection: { addresses: [{ host, port }], clusterMode: false },
  prefix: 'glide', // key prefix (default: 'glide')
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

await queue.pause();
await queue.resume();
await queue.getJobCounts();  // { waiting, active, delayed, completed, failed }
await queue.obliterate({ force: true });
await queue.close();
```

### Worker

```typescript
const worker = new Worker('name', async (job) => {
  await job.updateProgress(50);
  return result;
}, {
  connection,
  concurrency: 10,
  blockTimeout: 5000,
  stalledInterval: 30000,
  lockDuration: 30000,
  limiter: { max: 100, duration: 60000 },
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
};

// Everything works the same - keys are hash-tagged automatically
const queue = new Queue('tasks', { connection });
```

## License

Apache-2.0
