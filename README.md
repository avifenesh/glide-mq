# glide-mq

[![npm version](https://img.shields.io/npm/v/glide-mq)](https://www.npmjs.com/package/glide-mq)
[![license](https://img.shields.io/npm/l/glide-mq)](https://github.com/avifenesh/glide-mq/blob/main/LICENSE)
[![CI](https://github.com/avifenesh/glide-mq/actions/workflows/ci.yml/badge.svg)](https://github.com/avifenesh/glide-mq/actions/workflows/ci.yml)
[![node](https://img.shields.io/node/v/glide-mq)](https://nodejs.org/)
[![changelog](https://img.shields.io/badge/changelog-CHANGELOG.md-blue)](CHANGELOG.md)
[![docs](https://img.shields.io/badge/docs-glide--mq.dev-6366f1)](https://avifenesh.github.io/glide-mq.dev/)

The AI-native message queue for Node.js. Built on Valkey/Redis Streams with 1-RTT job operations, cluster-native design, and first-class primitives for LLM orchestration - cost tracking, token streaming, human-in-the-loop, model failover, TPM rate limiting, budget caps, and vector search.

glide-mq connects through a Rust-native NAPI client ([valkey-glide](https://github.com/valkey-io/valkey-glide)), executes all queue logic in a single Valkey Server Function call per operation (FCALL, not EVAL), and hash-tags every key for automatic cluster slot alignment. The result is fewer round trips, no Lua cache misses, and zero cluster configuration.

> If glide-mq is useful to you, consider giving it a star on [GitHub](https://github.com/avifenesh/glide-mq). It helps others discover the project.

## Why glide-mq

- Use this when you need **AI orchestration**: cost tracking, token streaming, model failover, budget caps, and human-in-the-loop - built into the queue, not bolted on.
- Use this when you need **throughput**: 18,000+ jobs/s on production infrastructure with TLS - up to 36% faster than alternatives at typical concurrency, 1 RTT per job via Valkey Server Functions.
- Use this when you run **Valkey/Redis clusters**: all keys hash-tagged out of the box, no `{braces}` workarounds.
- Use this when you need **workflows**: parent-child trees, DAGs with fan-in, step jobs, batch processing, and cron scheduling in one library.
- Use this when you deploy to **serverless**: lightweight `Producer` and `ServerlessPool` cache connections across warm invocations.
- Use this when you want **pub/sub with durability**: `Broadcast` delivers to all subscribers with retries, backpressure, and NATS-style subject filtering.
- Use this when you need **vector search**: create indexes over job data, store embeddings, and run KNN similarity queries - all through the queue API.

## Install

```bash
npm install glide-mq
```

Requires Node.js 20+ and a running [Valkey](https://valkey.io) 7.0+ or Redis 7.0+ instance. For vector search and bloom filter support, use `valkey/valkey-bundle`:

```bash
docker compose up -d valkey
```

The included `compose.yaml` starts `valkey/valkey-bundle:latest` with search, JSON, and bloom modules pre-loaded.

## Quick start

```typescript
import { Queue, Worker } from 'glide-mq';

const connection = { addresses: [{ host: 'localhost', port: 6379 }] };

const queue = new Queue('tasks', { connection });
await queue.add('send-email', { to: 'user@example.com', subject: 'Hello' });

const worker = new Worker('tasks', async (job) => {
  console.log(`Processing ${job.name}:`, job.data);
  return { sent: true };
}, { connection, concurrency: 10 });

worker.on('completed', (job) => console.log(`Job ${job.id} done`));
worker.on('failed', (job, err) => console.error(`Job ${job.id} failed:`, err.message));
```

## AI-native primitives

Seven built-in primitives for LLM and agent workflows. No plugins, no middleware - part of the core API.

### Cost tracking - `reportUsage` / `getFlowUsage`

Track model, tokens, cost, and latency per job. Aggregate across entire flows.

```typescript
const worker = new Worker('ai-tasks', async (job) => {
  const start = Date.now();
  const result = await openai.chat.completions.create({
    model: 'gpt-4o',
    messages: job.data.messages,
  });

  await job.reportUsage({
    model: 'gpt-4o',
    provider: 'openai',
    inputTokens: result.usage.prompt_tokens,
    outputTokens: result.usage.completion_tokens,
    costUsd: 0.0032,
    latencyMs: Date.now() - start,
  });

  return result.choices[0].message.content;
}, { connection });

// Aggregate usage across a parent-child flow
const usage = await queue.getFlowUsage(parentJobId);
// { totalInputTokens, totalOutputTokens, totalCostUsd, jobCount, models }
```

### Token streaming - `job.stream` / `queue.readStream`

Stream LLM output tokens to clients in real time. Each chunk is appended to a per-job Valkey stream.

```typescript
// Producer side
const worker = new Worker('chat', async (job) => {
  const stream = await openai.chat.completions.create({
    model: 'gpt-4o',
    messages: job.data.messages,
    stream: true,
  });

  for await (const chunk of stream) {
    const token = chunk.choices[0]?.delta?.content;
    if (token) await job.stream({ token });
  }
}, { connection });

// Consumer side - poll or long-poll with block
let lastId: string | undefined;
const chunks = await queue.readStream(jobId, { lastId, block: 5000 });
// SSE endpoint also available: GET /queues/:name/jobs/:id/stream
```

### Human-in-the-loop - `job.suspend` / `queue.signal`

Pause a job mid-processor to wait for external approval, review, or input.

```typescript
const worker = new Worker('content', async (job) => {
  if (job.signals.length === 0) {
    const draft = await generateDraft(job.data);
    await job.updateProgress({ draft });
    await job.suspend({ reason: 'Awaiting editorial review', timeout: 3600_000 });
  }

  // Resumed with signals - apply feedback
  const approval = job.signals.find(s => s.name === 'approve');
  if (!approval) throw new Error('Rejected');
  return { published: true, edits: approval.data };
}, { connection });

// External system sends the signal
await queue.signal(jobId, 'approve', { notes: 'Looks good, publish it.' });
```

### Adaptive stall detection - per-job `lockDuration`

Override the worker-level lock duration on a per-job basis. Short timeouts for fast tasks, long timeouts for LLM inference.

```typescript
await queue.add('quick-classify', { text }, { lockDuration: 5_000 });     // 5s
await queue.add('long-generation', { prompt }, { lockDuration: 300_000 }); // 5 min
```

### Fallback chains - model failover

Define ordered fallback models. On retryable failure, the next attempt reads `job.currentFallback` for the alternate model/provider.

```typescript
await queue.add('inference', { prompt: 'Summarize this document' }, {
  attempts: 4,
  backoff: { type: 'exponential', delay: 1000 },
  fallbacks: [
    { model: 'claude-sonnet-4-20250514', provider: 'anthropic' },
    { model: 'gpt-4o-mini', provider: 'openai' },
    { model: 'gemini-2.0-flash', provider: 'google' },
  ],
});

const worker = new Worker('inference', async (job) => {
  const fallback = job.currentFallback;
  const model = fallback ? fallback.model : 'gpt-4o';
  const provider = fallback ? fallback.provider : 'openai';
  return callLLM(provider, model, job.data.prompt);
}, { connection });
```

### TPM rate limiting - `tokenLimiter`

Enforce tokens-per-minute limits to stay within LLM API quotas. Dual-axis: combine with RPM `limiter` for both request and token rate control.

```typescript
const worker = new Worker('inference', processor, {
  connection,
  concurrency: 20,
  limiter: { max: 500, duration: 60_000 },              // 500 RPM
  tokenLimiter: { maxTokens: 90_000, duration: 60_000 }, // 90k TPM
});

// Inside the processor, report tokens consumed
async function processor(job) {
  const result = await callLLM(job.data);
  await job.reportTokens(result.usage.total_tokens);
  return result;
}
```

### Flow budget - cost caps

Set hard caps on total tokens or USD cost across an entire flow. Workers enforce the budget automatically - jobs fail or pause when exceeded.

```typescript
import { FlowProducer } from 'glide-mq';

const flow = new FlowProducer({ connection });

await flow.add({
  name: 'pipeline',
  queueName: 'orchestrator',
  data: { topic: 'AI safety' },
  children: [
    { name: 'research', queueName: 'ai-tasks', data: { step: 'research' } },
    { name: 'draft',    queueName: 'ai-tasks', data: { step: 'draft' } },
    { name: 'edit',     queueName: 'ai-tasks', data: { step: 'edit' } },
  ],
}, {
  budget: { maxTotalTokens: 50_000, maxCostUsd: 0.50, onExceeded: 'fail' },
});

// Check budget status
const status = await queue.getFlowBudget(parentJobId);
// { maxTotalTokens, maxCostUsd, usedTokens, usedCost, exceeded, onExceeded }
```

## Vector search

Create Valkey Search indexes over job hashes, store embeddings, and run KNN similarity queries - all through the queue API. Requires `valkey-bundle` with the search module loaded.

```typescript
// Create an index with a vector field
await queue.createJobIndex({
  vectorField: {
    name: 'embedding',
    dimensions: 1536,
    algorithm: 'HNSW',
    distanceMetric: 'COSINE',
  },
  fields: [
    { type: 'TAG', name: 'category' },
    { type: 'NUMERIC', name: 'score' },
  ],
});

// Store a vector on a job during processing
const worker = new Worker('embed', async (job) => {
  const embedding = await openai.embeddings.create({
    model: 'text-embedding-3-small',
    input: job.data.text,
  });
  await job.storeVector('embedding', embedding.data[0].embedding);
  return { indexed: true };
}, { connection });

// Search by similarity with optional pre-filter
const results = await queue.vectorSearch(queryEmbedding, {
  k: 10,
  filter: '@category:{ml}',
});
for (const { job, score } of results) {
  console.log(`${job.name} (score: ${score}):`, job.data);
}

// Clean up
await queue.dropJobIndex();
```

## Performance

Benchmarked on production infrastructure - not localhost, where all queues look similar.

**Setup**: AWS ElastiCache Valkey 8.2 (r7g.large), TLS enabled, EC2 client in the same region. No-op processor, warmup jobs excluded from measurement.

| Concurrency | glide-mq    | Leading alternative | Delta       |
|:-----------:|------------:|--------------------:|:-----------:|
| c=1         | 2,479 j/s   | 2,535 j/s           | -2%         |
| c=5         | 10,754 j/s  | 9,866 j/s           | +9%         |
| c=10        | **18,218 j/s** | 13,541 j/s       | **+35%**    |
| c=15        | **19,583 j/s** | 14,162 j/s       | **+38%**    |
| c=20        | 19,408 j/s  | 16,085 j/s          | +21%        |
| c=50        | 19,768 j/s  | 19,159 j/s          | +3%         |

Most production deployments run workers with concurrency between 5 and 20 - exactly where glide-mq's architecture pays off the most. The advantage comes from completing and fetching the next job in a single server-side function call (1 RTT per job). When the network between your application and Valkey/Redis is real - as it is in every production deployment where app servers and data stores run on separate machines - that round-trip savings compounds across concurrent workers.

At higher concurrency levels both libraries converge toward the Valkey single-thread execution ceiling, which is the expected behavior.

Other highlights:

- **addBulk**: 10,000 jobs in 350 ms
- **Gzip compression**: 98% payload reduction on 15 KB payloads

Run `npm run bench` locally or `npx tsx benchmarks/elasticache-head-to-head.ts` against your own infrastructure to reproduce.

## How it's different

| Aspect | glide-mq approach |
|--------|-------------------|
| **Network per job** | 1 RTT - complete current job + fetch next in a single FCALL |
| **Client** | Rust NAPI bindings ([valkey-glide](https://github.com/valkey-io/valkey-glide)) - no JS protocol parsing |
| **Server logic** | 1 persistent Valkey Function library (FUNCTION LOAD + FCALL) - no per-call EVAL recompilation |
| **Cluster** | Hash-tagged keys (`glide:{queueName}:*`) - all queue data routes to the same slot automatically |
| **AI-native** | Cost tracking, token streaming, human-in-the-loop, fallback chains, TPM limits, budget caps - built in |
| **Vector search** | `createJobIndex`, `vectorSearch`, `storeVector` - KNN over job data via Valkey Search |
| **Workflows** | FlowProducer trees, DAGs with fan-in, chain/group/chord, step jobs, dynamic children |
| **Pub/sub** | Broadcast with NATS-style subject filtering, independent subscriber retries |
| **Serverless** | Lightweight `Producer` + `ServerlessPool` for Lambda/Edge with connection reuse |

## Core concepts

- **Queue** - stores jobs in Valkey Streams. Handles enqueue, delay, priority, pause, drain, bulk operations, streaming, signals, and search.
- **Worker** - processes jobs with configurable concurrency, prefetch, lock duration, TPM rate limiting, and stalled-job recovery.
- **Job** - a unit of work with name, data, options (retries, backoff, priority, TTL, fallbacks, lockDuration), AI usage tracking, and streaming.
- **FlowProducer** - creates parent-child job trees and DAGs with optional budget caps. A parent waits for all children before processing.
- **Producer** - lightweight enqueue-only client. No EventEmitter, no Job instances, returns plain string IDs. Built for serverless.
- **Broadcast** - fan-out pub/sub. Each message is delivered to every subscriber group with independent retries and backpressure.
- **QueueEvents** - real-time stream of job lifecycle events (completed, failed, delayed, waiting, etc.).

## Features

### Core

- **Queues and workers** with configurable concurrency, prefetch, and lock duration ([Usage](docs/USAGE.md))
- **Delayed, priority, and bulk enqueue** for scheduling and high-throughput ingestion ([Usage](docs/USAGE.md))
- **Batch processing** - process multiple jobs at once via `batch: { size, timeout? }` ([Usage](docs/USAGE.md#batch-processing))
- **Request-reply** - `queue.addAndWait(name, data, { waitTimeout })` for synchronous RPC ([Usage](docs/USAGE.md#request-reply-with-addandwait))
- **LIFO mode** - `lifo: true` processes newest jobs first ([Advanced](docs/ADVANCED.md#lifo-mode))
- **Job TTL** - auto-expire jobs after a time-to-live window ([Advanced](docs/ADVANCED.md#job-ttl))
- **Custom job IDs** - deterministic, idempotent enqueue; duplicates return `null` ([Advanced](docs/ADVANCED.md#custom-job-ids))
- **Pluggable serializers** - swap JSON for any `{ serialize, deserialize }` implementation ([Advanced](docs/ADVANCED.md#pluggable-serializers))
- **Transparent compression** - gzip payloads at the queue level ([Advanced](docs/ADVANCED.md#transparent-compression))

### Reliability

- **Retries with exponential, fixed, or custom backoff** and dead-letter queues ([Advanced](docs/ADVANCED.md#retries-and-backoff))
- **UnrecoverableError** - skip all retries and fail permanently ([Usage](docs/USAGE.md#unrecoverableerror))
- **Stalled recovery** - auto-reclaim stuck jobs via consumer group PEL and `XAUTOCLAIM` ([Usage](docs/USAGE.md#worker))
- **Job revocation** - cooperative cancellation with `AbortSignal` ([Advanced](docs/ADVANCED.md#job-revocation))
- **Deduplication** - simple, throttle, and debounce modes with configurable TTL ([Advanced](docs/ADVANCED.md#deduplication))
- **Per-key ordering** - sequential processing per ordering key with configurable group concurrency ([Advanced](docs/ADVANCED.md#ordering-and-group-concurrency))
- **Rate limiting** - per-group sliding window, token bucket, and global queue-wide limits ([Advanced](docs/ADVANCED.md#global-rate-limiting))
- **Sandboxed processors** - run processors in worker threads or child processes ([Architecture](docs/ARCHITECTURE.md))

### Orchestration

- **FlowProducer** - parent-child job trees with `chain`, `group`, and `chord` helpers ([Workflows](docs/WORKFLOWS.md))
- **DAG workflows** - arbitrary dependency graphs with `FlowProducer.addDAG()` and `dag()` helper; multi-parent fan-in, diamond patterns, cycle detection ([Workflows](docs/WORKFLOWS.md))
- **Step jobs** - `job.moveToDelayed(timestamp, nextStep)` suspends a job mid-processor and resumes later ([Usage](docs/USAGE.md#pause-and-resume-a-job-later-step-jobs))
- **Dynamic children** - `job.moveToWaitingChildren()` pauses a parent to add children mid-execution ([Workflows](docs/WORKFLOWS.md))
- **Batch processing** - process multiple jobs at once for bulk I/O ([Usage](docs/USAGE.md#batch-processing))

### Scheduling

- **Cron and interval schedulers** - 5-field cron with timezone, fixed intervals, and `repeatAfterComplete` mode ([Advanced](docs/ADVANCED.md#job-schedulers))
- **Bounded schedulers** - `limit`, `startDate`, and `endDate` for finite schedules ([Advanced](docs/ADVANCED.md#bounded-schedulers))

### Pub/Sub

- **Broadcast** - fan-out delivery to all subscriber groups ([Usage](docs/USAGE.md#broadcast--broadcastworker))
- **BroadcastWorker** - independent consumer groups with own retries, concurrency, and backpressure ([Usage](docs/USAGE.md#broadcast--broadcastworker))
- **Subject filtering** - NATS-style patterns (`*` one segment, `>` trailing wildcard) for topic-based routing ([Usage](docs/USAGE.md#broadcast--broadcastworker))

### AI-native

- **Cost tracking** - `job.reportUsage()` persists model, provider, tokens, cost, and latency; `queue.getFlowUsage()` aggregates across parent and child jobs ([Usage](docs/USAGE.md#ai-native-primitives))
- **Token streaming** - `job.stream(chunk)` appends output chunks during processing; `queue.readStream(jobId)` reads them back with optional long-polling; SSE endpoint `GET /queues/:name/jobs/:id/stream` for real-time consumers ([Usage](docs/USAGE.md#ai-native-primitives))
- **Suspend/resume with signals** - `job.suspend()` pauses a job mid-processor; `queue.signal(jobId, name, data)` resumes it with an external event; optional timeout auto-fails expired suspensions; `onResume` callback for same-worker continuations ([Usage](docs/USAGE.md#suspendresume-with-signals))
- **Per-job lock duration** - override `lockDuration` per job for adaptive stall detection; short for classifiers, long for multi-minute LLM generations
- **Fallback chains** - ordered `fallbacks` array on job options; processor reads `job.currentFallback` for the current model/provider on each retry
- **TPM rate limiting** - `tokenLimiter` on worker options enforces tokens-per-minute caps; dual-axis with RPM `limiter`; scoped to queue, worker, or both
- **Flow budget** - `FlowProducer.add(flow, { budget })` sets `maxTotalTokens` and `maxCostUsd` caps across all jobs in a flow; `queue.getFlowBudget(id)` checks status

### Search

- **Job indexes** - `queue.createJobIndex()` creates a Valkey Search index over job hashes with TAG, NUMERIC, TEXT, and VECTOR fields
- **Vector search** - `queue.vectorSearch(embedding, opts)` runs KNN similarity queries with optional pre-filters (e.g., `@state:{completed}`)
- **Vector storage** - `job.storeVector(field, embedding)` writes Float32 embeddings directly to the job hash for indexing
- **Index management** - `queue.dropJobIndex()` removes an index without affecting job data

### Serverless

- **Producer** - enqueue without EventEmitter overhead, returns plain string IDs ([Usage](docs/USAGE.md))
- **ServerlessPool** - connection caching across warm Lambda/Edge invocations ([Serverless](docs/SERVERLESS.md))

### Observability

- **QueueEvents** - real-time stream-based lifecycle events ([Observability](docs/OBSERVABILITY.md))
- **Time-series metrics** - per-minute throughput and latency retained 24h, recorded server-side ([Observability](docs/OBSERVABILITY.md))
- **OpenTelemetry** - automatic span emission; bring your own tracer or auto-detect `@opentelemetry/api` ([Observability](docs/OBSERVABILITY.md))
- **Job logs** - append structured log entries per job with pagination ([Observability](docs/OBSERVABILITY.md))
- **Job mutations** - `changePriority()`, `changeDelay()`, `promote()` after enqueue; `retryJobs()` and `clean()` in bulk ([Usage](docs/USAGE.md))
- **Graceful shutdown** - `gracefulShutdown()` helper registers SIGTERM/SIGINT handlers ([Usage](docs/USAGE.md#graceful-shutdown))
- **In-memory testing** - `TestQueue` and `TestWorker` with zero Valkey dependency ([Testing](docs/TESTING.md))

### Cloud

- **Cluster-native** - hash-tagged keys `glide:{queueName}:*` route all queue data to the same slot ([Usage](docs/USAGE.md#cluster-mode))
- **IAM authentication** - native SigV4 auth for AWS ElastiCache and MemoryDB ([Usage](docs/USAGE.md#cluster-mode))
- **AZ-affinity routing** - `readFrom: 'AZAffinity'` routes reads to same-AZ replicas ([Usage](docs/USAGE.md#cluster-mode))

## Examples

18 runnable examples covering real-world AI workflows. Each connects to a local Valkey instance.

| Example | Description |
|---------|-------------|
| `usage-tracking.ts` | Track tokens and cost across a multi-step content pipeline with `reportUsage` / `getFlowUsage` |
| `token-streaming.ts` | Stream LLM output tokens to clients via `job.stream` / `queue.readStream` |
| `human-approval.ts` | Suspend a job for editorial review, resume with `queue.signal` |
| `model-failover.ts` | Fallback chains - primary model fails, retries with alternatives via `job.currentFallback` |
| `tpm-throttle.ts` | Dual-axis rate limiting (RPM + TPM) with `tokenLimiter` |
| `budget-cap.ts` | Flow-level budget with `maxTotalTokens` / `maxCostUsd` caps |
| `adaptive-timeout.ts` | Per-job `lockDuration` for variable-latency LLM calls |
| `ai-agent-loop.ts` | Autonomous agent loop with tool calls, token tracking, and budget enforcement |
| `rag-pipeline.ts` | Retrieval-augmented generation with embedding, indexing, and retrieval steps |
| `embedding-pipeline.ts` | Batch embedding pipeline with vector storage |
| `vector-search.ts` | KNN similarity search over document embeddings with tag pre-filters |
| `content-pipeline.ts` | Multi-step content generation flow (research, draft, edit) |
| `agent-memory.ts` | Persistent agent memory backed by job metadata |
| `search-dashboard.ts` | Search and filter jobs using Valkey Search indexes |
| `testing-mode.ts` | In-memory `TestQueue` / `TestWorker` with zero Valkey dependency |
| `broadcast-events.ts` | Fan-out pub/sub with subject filtering |
| `with-langchain.ts` | LangChain integration with token tracking |
| `with-vercel-ai-sdk.ts` | Vercel AI SDK integration with streaming |

Run any example: `npx tsx examples/<name>.ts`

## Framework integrations

| Package | Install | Setup |
|---------|---------|-------|
| [`@glidemq/hono`](https://github.com/avifenesh/glidemq-hono) | `npm i @glidemq/hono` | `app.use(glideMQ({ connection, queues: { ... } }))` |
| [`@glidemq/fastify`](https://github.com/avifenesh/glidemq-fastify) | `npm i @glidemq/fastify` | `app.register(glideMQPlugin, { connection, queues: { ... } })` |
| [`@glidemq/nestjs`](https://github.com/avifenesh/glidemq-nestjs) | `npm i @glidemq/nestjs` | `GlideMQModule.forRoot({ connection, queues: { ... } })` |
| [`@glidemq/dashboard`](https://github.com/avifenesh/glidemq-dashboard) | `npm i @glidemq/dashboard` | `app.use('/dashboard', createDashboard([queue1, queue2]))` |
| [`@glidemq/hapi`](https://github.com/avifenesh/glidemq-hapi) | `npm i @glidemq/hapi` | `await server.register({ plugin: glideMQPlugin, options: { connection, queues } })` |

All framework packages provide REST endpoints, SSE events, and serverless Producer support. See each package's README for full documentation.

## Cross-language

Non-Node.js services can enqueue jobs into glide-mq queues using the HTTP proxy or direct FCALL:

```typescript
import { createProxyServer } from 'glide-mq/proxy';

const proxy = createProxyServer({
  connection: { addresses: [{ host: 'localhost', port: 6379 }] },
  queues: ['emails', 'reports'],
});
proxy.app.listen(3000);
```

```bash
curl -X POST http://localhost:3000/queues/emails/jobs \
  -H 'Content-Type: application/json' \
  -d '{"name": "send-email", "data": {"to": "user@example.com"}}'
```

Endpoints: `POST /queues/:name/jobs`, `POST /queues/:name/jobs/bulk`, `GET /queues/:name/jobs/:id`, `GET /queues/:name/jobs/:id/stream` (SSE), `POST /queues/:name/pause`, `POST /queues/:name/resume`, `GET /queues/:name/counts`, `GET /health`.

For zero-overhead integration, call Valkey Server Functions directly from any language with a Valkey client. See [Wire Protocol](docs/WIRE_PROTOCOL.md) for FCALL signatures, key layout, and examples in Python and Go.

## Documentation

| Guide | Topics |
|-------|--------|
| [Usage](docs/USAGE.md) | Queue, Worker, Producer, batch, request-reply, graceful shutdown, cluster mode |
| [Broadcast](docs/BROADCAST.md) | Pub/sub fan-out, BroadcastWorker, subject filtering |
| [Step Jobs](docs/STEP_JOBS.md) | `moveToDelayed`, `moveToWaitingChildren`, multi-step processors |
| [Advanced](docs/ADVANCED.md) | Schedulers, rate limiting, dedup, compression, retries, DLQ, custom IDs, LIFO, TTL, serializers |
| [Workflows](docs/WORKFLOWS.md) | FlowProducer, DAG, `chain`, `group`, `chord`, dynamic children |
| [Observability](docs/OBSERVABILITY.md) | OpenTelemetry, time-series metrics, job logs, dashboard |
| [Serverless](docs/SERVERLESS.md) | Producer, ServerlessPool, Lambda and Edge deployment |
| [Testing](docs/TESTING.md) | In-memory `TestQueue` and `TestWorker` - no Valkey needed |
| [Wire Protocol](docs/WIRE_PROTOCOL.md) | Cross-language FCALL specs, key layout, Python and Go examples |
| [Architecture](docs/ARCHITECTURE.md) | Key design, Valkey functions, LIFO, Broadcast, DAG internals |
| [Durability](docs/DURABILITY.md) | Persistence modes, crash windows, feature-specific durability |
| [Migration](docs/MIGRATION.md) | Coming from BullMQ? API mapping and step-by-step guide |

## Limitations

- Requires a running Valkey 7.0+ or Redis 7.0+ instance. There is no embedded mode.
- Node.js only. The Rust-native NAPI client (`@valkey/valkey-glide`) does not run in browsers or Deno.
- At-least-once delivery semantics. Jobs may be processed more than once after crashes or stalled recovery.
- Not a streaming platform. glide-mq is a job/task queue, not a replacement for Kafka or NATS JetStream.
- Vector search requires `valkey-bundle` or a Valkey instance with the search module loaded.
- Single dependency on `@glidemq/speedkey` (which wraps `@valkey/valkey-glide`). Native addon compilation is required on install.

## Ecosystem

| Package | Description | Links |
|---------|-------------|-------|
| [glide-mq](https://github.com/avifenesh/glide-mq) | Core queue library | [npm](https://www.npmjs.com/package/glide-mq) |
| [@glidemq/hono](https://github.com/avifenesh/glidemq-hono) | Hono middleware - REST endpoints, SSE, serverless Producer | [npm](https://www.npmjs.com/package/@glidemq/hono) |
| [@glidemq/fastify](https://github.com/avifenesh/glidemq-fastify) | Fastify plugin - REST endpoints, SSE, serverless Producer | [npm](https://www.npmjs.com/package/@glidemq/fastify) |
| [@glidemq/nestjs](https://github.com/avifenesh/glidemq-nestjs) | NestJS module - decorators, DI, lifecycle management | [npm](https://www.npmjs.com/package/@glidemq/nestjs) |
| [@glidemq/dashboard](https://github.com/avifenesh/glidemq-dashboard) | Web UI - metrics charts, scheduler management, job mutations | [npm](https://www.npmjs.com/package/@glidemq/dashboard) |
| [@glidemq/hapi](https://github.com/avifenesh/glidemq-hapi) | Hapi plugin - REST endpoints, SSE, Joi validation | [npm](https://www.npmjs.com/package/@glidemq/hapi) |
| [@glidemq/speedkey](https://github.com/avifenesh/speedkey) | Valkey GLIDE client with native NAPI bindings | [npm](https://www.npmjs.com/package/@glidemq/speedkey) |
| [glidemq-examples](https://github.com/avifenesh/glidemq-examples) | 40+ runnable examples across frameworks and use cases | [GitHub](https://github.com/avifenesh/glidemq-examples) |
| [glide-mq.dev](https://avifenesh.github.io/glide-mq.dev/) | Full documentation, guides, API reference | [Website](https://avifenesh.github.io/glide-mq.dev/) |

> If glide-mq is useful to you, consider [starring the repo](https://github.com/avifenesh/glide-mq). It helps others find the project.

## Contributing

Bug reports, feature requests, and pull requests are welcome. See [CHANGELOG.md](CHANGELOG.md) for release history.

- [Open an issue](https://github.com/avifenesh/glide-mq/issues)
- [Discussions](https://github.com/avifenesh/glide-mq/discussions)

## License

Apache-2.0
