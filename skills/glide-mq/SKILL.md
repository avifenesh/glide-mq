---
name: glide-mq
description: >-
  Use when building or changing code that uses glide-mq, the Node.js job queue on
  Valkey/Redis Streams: queues, workers, retries, schedulers, workflows,
  broadcast, serverless producers, HTTP proxy, or its AI primitives (usage,
  streaming, suspend/resume, budgets, fallbacks, vector search).
license: Apache-2.0
metadata:
  author: glide-mq
  version: '0.15.5'
  tags: glide-mq, message-queue, valkey, redis, job-queue, worker, streams, ai-native, llm, vector-search
  sources: docs/USAGE.md, docs/ADVANCED.md, docs/WORKFLOWS.md, docs/BROADCAST.md, docs/SERVERLESS.md, docs/TESTING.md, docs/OBSERVABILITY.md
---

# glide-mq

Job queue for Node.js on Valkey/Redis Streams, with a Rust NAPI client and all queue logic in Valkey Server Functions (one `FCALL` per job operation). The API is close to BullMQ; the differences that bite are in connection config and a few method signatures.

## Quick Start

```typescript
import { Queue, Worker } from 'glide-mq';

const connection = { addresses: [{ host: 'localhost', port: 6379 }] };

const queue = new Queue('tasks', { connection });
await queue.add('send-email', { to: 'user@example.com', subject: 'Hello' });

const worker = new Worker(
  'tasks',
  async (job) => {
    console.log(`Processing ${job.name}:`, job.data);
    return { sent: true };
  },
  { connection, concurrency: 10 },
);

worker.on('completed', (job) => console.log(`Done: ${job.id}`));
worker.on('failed', (job, err) => console.error(`Failed: ${job.id}`, err.message));
```

## References

Read the file that matches the task; each holds the options, defaults and edge cases for its area.

| Area | File |
| --- | --- |
| Queues, adding jobs, retries, dedup, rate limits, job lookup | [references/queue.md](references/queue.md) |
| Workers, concurrency, batch mode, stalled jobs, sandboxing | [references/worker.md](references/worker.md) |
| Connection options, TLS, IAM, cluster, AZ affinity | [references/connection.md](references/connection.md) |
| Parent-child flows, DAGs, `chain` / `group` / `chord` | [references/workflows.md](references/workflows.md) |
| Fan-out with `Broadcast` / `BroadcastWorker` | [references/broadcast.md](references/broadcast.md) |
| Cron and interval schedulers | [references/schedulers.md](references/schedulers.md) |
| `QueueEvents`, metrics, OpenTelemetry | [references/observability.md](references/observability.md) |
| LLM usage, token streaming, suspend/resume, budgets, fallbacks, token rate limits | [references/ai-native.md](references/ai-native.md) |
| Vector search over jobs | [references/search.md](references/search.md) |
| Serverless producers, HTTP proxy and SSE, in-memory testing | [references/serverless.md](references/serverless.md) |

Framework adapters (Hono, Fastify, NestJS, Hapi) are documented at https://www.glidemq.dev/integrations/.

## Key Patterns

### Delayed & Priority Jobs

```typescript
// Delayed: run after 5 minutes
await queue.add('reminder', data, { delay: 300_000 });

// Priority: lower number = higher priority (default: 0)
await queue.add('urgent', data, { priority: 0 });
await queue.add('low-priority', data, { priority: 10 });

// Retries with exponential backoff
await queue.add('webhook', data, {
  attempts: 5,
  backoff: { type: 'exponential', delay: 1000 },
});
```

### Bulk Ingestion

```typescript
const jobs = items.map((item) => ({
  name: 'process',
  data: item,
  opts: { jobId: `item-${item.id}` },
}));
await queue.addBulk(jobs);
```

### Batch Worker (Process Multiple Jobs at Once)

```typescript
const worker = new Worker(
  'analytics',
  async (jobs) => {
    // jobs is Job[] when batch is enabled
    await db.insertMany(
      'events',
      jobs.map((j) => j.data),
    );
  },
  {
    connection,
    batch: { size: 50, timeout: 5000 },
  },
);
```

Batch mode is composable with `priority` and `lifo: true` jobs - list-popped jobs are dispatched into the same batch processor (chunked by `batch.size`).

### Request-Reply (addAndWait)

```typescript
const result = await queue.addAndWait(
  'compute',
  { input: 42 },
  {
    waitTimeout: 30_000,
  },
);
console.log(result); // processor return value
```

### Serverless Producer (No EventEmitter Overhead)

```typescript
import { Producer } from 'glide-mq';
const producer = new Producer('queue', { connection });
await producer.add('job-name', data);
await producer.close();
```

### Graceful Shutdown

```typescript
import { gracefulShutdown } from 'glide-mq';

// Registers SIGTERM/SIGINT handlers. The handle is also a promise:
// `await handle` resolves once a signal-triggered or manual shutdown finishes.
const handle = gracefulShutdown([worker1, worker2, queue, events]);

// For programmatic shutdown (e.g., in tests):
await handle.shutdown();

// To remove signal handlers without closing:
handle.dispose();
```

### Testing Without Valkey

```typescript
import { TestQueue, TestWorker } from 'glide-mq/testing';
const queue = new TestQueue('tasks');
await queue.add('test-job', { key: 'value' });
const worker = new TestWorker(queue, processor);
await worker.run();
```

## Things that differ from what you might expect

- Requires Node.js 20+ and Valkey 7.0+ (or Redis 7.0+).
- Connections are `{ addresses: [{ host, port }] }`, not `{ host, port }`; TLS is `useTLS: true`, passwords go in `credentials`, and cluster needs `clusterMode: true`. The BullMQ shape fails at connect time with a `ConnectionError`.
- Delivery is at least once, so processors should be idempotent.
- Priority: a lower number runs first, 0 (the default) is the highest, and values above 2048 throw.
- Keys are hash-tagged (`glide:{queueName}:*`), so cluster mode needs no extra setup.
- `queue.add()` with a `jobId` that already exists returns `null` instead of adding a duplicate.

## Done

- `npm test` or the project-equivalent test command passes
- `await queue.getJobCounts()` matches the expected queue state
- no jobs are left unexpectedly stuck in `active`
- any QueueEvents or SSE behavior touched by the change has been smoke-tested
- temporary queues, workers, and listeners are closed cleanly

## Full Documentation

https://www.glidemq.dev/
