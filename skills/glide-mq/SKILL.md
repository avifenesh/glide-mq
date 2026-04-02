---
name: glide-mq
description: >-
  Creates message queues, workers, job workflows, and fan-out broadcasts using
  glide-mq on Valkey/Redis Streams. Provides API reference, code patterns, and
  configuration for queues, workers, delayed/priority jobs, schedulers, batch
  processing, DAG workflows, request-reply, serverless producers, and AI-native
  primitives (usage tracking, token streaming, suspend/resume, budget caps,
  fallback chains, dual-axis rate limiting, rolling usage summaries, vector search, HTTP proxy/SSE). Triggers on
  "glide-mq", "glidemq", "job queue valkey", "background tasks valkey",
  "message queue redis streams", "glide-mq LLM queue",
  "glide-mq AI orchestration queue", "glide-mq token rate limiting",
  "glide-mq model fallback", "glide-mq human-in-the-loop queue",
  "glide-mq vector search", "glide-mq AI pipeline".
license: Apache-2.0
metadata:
  author: glide-mq
  version: "0.14.0"
  tags: glide-mq, message-queue, valkey, redis, job-queue, worker, streams, ai-native, llm, vector-search
  sources: docs/USAGE.md, docs/ADVANCED.md, docs/WORKFLOWS.md, docs/BROADCAST.md, docs/SERVERLESS.md, docs/TESTING.md, docs/OBSERVABILITY.md
---

# glide-mq

High-performance AI-native message queue for Node.js on Valkey/Redis Streams with a Rust NAPI core.

## Quick Start

```typescript
import { Queue, Worker } from 'glide-mq';

const connection = { addresses: [{ host: 'localhost', port: 6379 }] };

const queue = new Queue('tasks', { connection });
await queue.add('send-email', { to: 'user@example.com', subject: 'Hello' });

const worker = new Worker('tasks', async (job) => {
  console.log(`Processing ${job.name}:`, job.data);
  return { sent: true };
}, { connection, concurrency: 10 });

worker.on('completed', (job) => console.log(`Done: ${job.id}`));
worker.on('failed', (job, err) => console.error(`Failed: ${job.id}`, err.message));
```

## When to Apply

Use this skill when:
- Creating or configuring queues, workers, or producers
- Adding jobs (single, bulk, delayed, priority)
- Setting up retries, backoff, or dead-letter queues
- Building job workflows (parent-child, DAGs, chains)
- Implementing fan-out broadcast patterns
- Configuring cron/interval schedulers
- Setting up connection options (TLS, IAM, AZ-affinity)
- Working with batch processing or rate limiting
- Tracking AI/LLM usage (tokens, cost, model) per job or flow
- Streaming LLM output tokens in real-time
- Implementing human-in-the-loop approval with suspend/resume
- Setting budget caps (tokens, cost) on workflow flows
- Configuring fallback chains for model/provider failover
- Dual-axis rate limiting (RPM + TPM) for LLM API compliance
- Aggregating rolling usage/cost summaries across queues
- Searching jobs by vector similarity (KNN) with Valkey Search
- Exposing queues or broadcasts over the HTTP proxy, including SSE endpoints
- Integrating with frameworks (Hono, Fastify, NestJS, Hapi)
- Deploying in serverless environments (Lambda, Vercel Edge)

## Core API by Priority

| Priority | Category | Impact | Reference |
|----------|----------|--------|-----------|
| 1 | Queue & Job Operations | CRITICAL | [references/queue.md](references/queue.md) |
| 2 | Worker & Processing | CRITICAL | [references/worker.md](references/worker.md) |
| 3 | Connection & Config | HIGH | [references/connection.md](references/connection.md) |
| 4 | Workflows & FlowProducer | HIGH | [references/workflows.md](references/workflows.md) |
| 5 | Broadcast (Fan-Out) | MEDIUM | [references/broadcast.md](references/broadcast.md) |
| 6 | Schedulers (Cron/Interval) | MEDIUM | [references/schedulers.md](references/schedulers.md) |
| 7 | Observability & Events | MEDIUM | [references/observability.md](references/observability.md) |
| 8 | AI-Native Primitives | HIGH | [references/ai-native.md](references/ai-native.md) |
| 9 | Vector Search | MEDIUM | [references/search.md](references/search.md) |
| 10 | Serverless & Testing | LOW | [references/serverless.md](references/serverless.md) |

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
  backoff: { type: 'exponential', delay: 1000 }
});
```

### Bulk Ingestion (10,000 jobs in ~350ms)

```typescript
const jobs = items.map(item => ({
  name: 'process',
  data: item,
  opts: { jobId: `item-${item.id}` }
}));
await queue.addBulk(jobs);
```

### Batch Worker (Process Multiple Jobs at Once)

```typescript
const worker = new Worker('analytics', async (jobs) => {
  // jobs is Job[] when batch is enabled
  await db.insertMany('events', jobs.map(j => j.data));
}, {
  connection,
  batch: { size: 50, timeout: 5000 }
});
```

### Request-Reply (addAndWait)

```typescript
const result = await queue.addAndWait('compute', { input: 42 }, {
  waitTimeout: 30_000
});
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

// Registers SIGTERM/SIGINT handlers and returns a handle.
// await blocks until a signal fires - use as last line of your program.
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

## Problem-to-Reference Mapping

| Problem | Start With |
|---------|------------|
| Need to create a queue and add jobs | [references/queue.md](references/queue.md) |
| Need to process jobs with workers | [references/worker.md](references/worker.md) |
| Jobs failing, need retries/backoff | [references/queue.md](references/queue.md) - Retry section |
| Need parent-child job dependencies | [references/workflows.md](references/workflows.md) |
| Need fan-out to multiple consumers | [references/broadcast.md](references/broadcast.md) |
| Need cron or repeating jobs | [references/schedulers.md](references/schedulers.md) |
| Connection errors or TLS/IAM setup | [references/connection.md](references/connection.md) |
| Stalled jobs or lock issues | [references/worker.md](references/worker.md) - Stalled Jobs |
| Need real-time job events | [references/observability.md](references/observability.md) |
| Integrating with Fastify/NestJS/Hono | [Framework Integrations](https://www.glidemq.dev/integrations/) |
| Deploying to Lambda/Vercel Edge | [references/serverless.md](references/serverless.md) |
| Need deduplication or idempotent jobs | [references/queue.md](references/queue.md) - Dedup |
| Need rate limiting | [references/queue.md](references/queue.md) - Rate Limit |
| Running tests without Valkey | [references/serverless.md](references/serverless.md) - Testing |
| Need to track LLM tokens/cost per job | [references/ai-native.md](references/ai-native.md) - Usage Metadata |
| Need to stream LLM output tokens | [references/ai-native.md](references/ai-native.md) - Token Streaming |
| Need human approval before proceeding | [references/ai-native.md](references/ai-native.md) - Suspend/Resume |
| Need to cap token/cost budget on a flow | [references/ai-native.md](references/ai-native.md) - Budget |
| Need model fallback on failure | [references/ai-native.md](references/ai-native.md) - Fallback Chains |
| Need RPM + TPM rate limiting for LLM APIs | [references/ai-native.md](references/ai-native.md) - Dual-Axis Rate Limiting |
| Need rolling usage/cost summary across queues | [references/ai-native.md](references/ai-native.md) - Usage Metadata |
| Need vector similarity search over jobs | [references/search.md](references/search.md) |
| Need to aggregate usage across a flow | [references/ai-native.md](references/ai-native.md) - Flow Usage |
| Need to create or inspect flows over HTTP | [references/serverless.md](references/serverless.md) - HTTP Proxy |
| Need cross-language HTTP or SSE access | [references/serverless.md](references/serverless.md) - HTTP Proxy |

## Critical Notes

- **Node.js 20+** and **Valkey 7.0+** (or Redis 7.0+) required
- **At-least-once delivery** - make processors idempotent
- **Priority**: lower number = higher priority (0 is default, highest)
- **Cluster-native** - hash-tagged keys (`glide:{queueName}:*`) work out of the box
- All queue logic runs as a single Valkey Server Function (FCALL) - 1 round-trip per job
- Connection format uses `addresses: [{ host, port }]` array, NOT `{ host, port }` object
- **Never use `customCommand`** - use typed API methods with dummy keys for cluster routing

## Done When

- `npm test` or the project-equivalent test command passes
- `await queue.getJobCounts()` matches the expected queue state
- no jobs are left unexpectedly stuck in `active`
- any QueueEvents or SSE behavior touched by the change has been smoke-tested
- temporary queues, workers, and listeners are closed cleanly

## Full Documentation

https://www.glidemq.dev/
