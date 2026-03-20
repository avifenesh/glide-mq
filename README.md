# glide-mq

[![npm version](https://img.shields.io/npm/v/glide-mq)](https://www.npmjs.com/package/glide-mq)
[![license](https://img.shields.io/npm/l/glide-mq)](https://github.com/avifenesh/glide-mq/blob/main/LICENSE)
[![CI](https://github.com/avifenesh/glide-mq/actions/workflows/ci.yml/badge.svg)](https://github.com/avifenesh/glide-mq/actions/workflows/ci.yml)
[![node](https://img.shields.io/node/v/glide-mq)](https://nodejs.org/)
[![changelog](https://img.shields.io/badge/changelog-CHANGELOG.md-blue)](CHANGELOG.md)
[![docs](https://img.shields.io/badge/docs-glide--mq.dev-6366f1)](https://avifenesh.github.io/glide-mq.dev/)

High-performance message queue for Node.js built on Valkey/Redis Streams with 1-RTT job operations and cluster-native design.

glide-mq is for anyone building background jobs, task queues, or workflow orchestration in Node.js. It connects through a Rust-native NAPI client ([valkey-glide](https://github.com/valkey-io/valkey-glide)), executes all queue logic in a single Valkey Server Function call per operation (FCALL, not EVAL), and hash-tags every key for automatic cluster slot alignment. The result is fewer round trips, no Lua cache misses, and zero cluster configuration.

> If glide-mq is useful to you, consider giving it a star on [GitHub](https://github.com/avifenesh/glide-mq). It helps others discover the project.

## Why glide-mq

- Use this when you need **throughput**: 18,000+ jobs/s on production infrastructure with TLS -- up to 36% faster than alternatives at typical concurrency, 1 RTT per job via Valkey Server Functions.
- Use this when you run **Valkey/Redis clusters**: all keys hash-tagged out of the box, no `{braces}` workarounds.
- Use this when you need **workflows**: parent-child trees, DAGs with fan-in, step jobs, batch processing, and cron scheduling in one library.
- Use this when you deploy to **serverless**: lightweight `Producer` and `ServerlessPool` cache connections across warm invocations.
- Use this when you want **pub/sub with durability**: `Broadcast` delivers to all subscribers with retries, backpressure, and NATS-style subject filtering.

## Install

```bash
npm install glide-mq
```

Requires Node.js 20+ and a running [Valkey](https://valkey.io) 7.0+ or Redis 7.0+ instance.

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

## Performance

Benchmarked on production infrastructure -- not localhost, where all queues look similar.

**Setup**: AWS ElastiCache Valkey 8.2 (r7g.large), TLS enabled, EC2 client in the same region. No-op processor, warmup jobs excluded from measurement.

| Concurrency | glide-mq    | Leading alternative | Delta       |
|:-----------:|------------:|--------------------:|:-----------:|
| c=1         | 2,479 j/s   | 2,535 j/s           | -2%         |
| c=5         | 10,754 j/s  | 9,866 j/s           | +9%         |
| c=10        | **18,218 j/s** | 13,541 j/s       | **+35%**    |
| c=15        | **19,583 j/s** | 14,162 j/s       | **+38%**    |
| c=20        | 19,408 j/s  | 16,085 j/s          | +21%        |
| c=50        | 19,768 j/s  | 19,159 j/s          | +3%         |

Most production deployments run workers with concurrency between 5 and 20 -- exactly where glide-mq's architecture pays off the most. The advantage comes from completing and fetching the next job in a single server-side function call (1 RTT per job). When the network between your application and Valkey/Redis is real -- as it is in every production deployment where app servers and data stores run on separate machines -- that round-trip savings compounds across concurrent workers.

At higher concurrency levels both libraries converge toward the Valkey single-thread execution ceiling, which is the expected behavior.

Other highlights:

- **addBulk**: 10,000 jobs in 350 ms
- **Gzip compression**: 98% payload reduction on 15 KB payloads

Run `npm run bench` locally or `npx tsx benchmarks/elasticache-head-to-head.ts` against your own infrastructure to reproduce.

## How it's different

| Aspect | glide-mq approach |
|--------|-------------------|
| **Network per job** | 1 RTT -- complete current job + fetch next in a single FCALL |
| **Client** | Rust NAPI bindings ([valkey-glide](https://github.com/valkey-io/valkey-glide)) -- no JS protocol parsing |
| **Server logic** | 1 persistent Valkey Function library (FUNCTION LOAD + FCALL) -- no per-call EVAL recompilation |
| **Cluster** | Hash-tagged keys (`glide:{queueName}:*`) -- all queue data routes to the same slot automatically |
| **Workflows** | FlowProducer trees, DAGs with fan-in, chain/group/chord, step jobs, dynamic children |
| **Pub/sub** | Broadcast with NATS-style subject filtering, independent subscriber retries |
| **Serverless** | Lightweight `Producer` + `ServerlessPool` for Lambda/Edge with connection reuse |

## Core concepts

- **Queue** -- stores jobs in Valkey Streams. Handles enqueue, delay, priority, pause, drain, and bulk operations.
- **Worker** -- processes jobs with configurable concurrency, prefetch, lock duration, and stalled-job recovery.
- **Job** -- a unit of work with name, data, options (retries, backoff, priority, TTL), and lifecycle events.
- **FlowProducer** -- creates parent-child job trees and DAGs. A parent waits for all children before processing.
- **Producer** -- lightweight enqueue-only client. No EventEmitter, no Job instances, returns plain string IDs. Built for serverless.
- **Broadcast** -- fan-out pub/sub. Each message is delivered to every subscriber group with independent retries and backpressure.
- **QueueEvents** -- real-time stream of job lifecycle events (completed, failed, delayed, waiting, etc.).

## Features

### Core

- **Queues and workers** with configurable concurrency, prefetch, and lock duration ([Usage](docs/USAGE.md))
- **Delayed, priority, and bulk enqueue** for scheduling and high-throughput ingestion ([Usage](docs/USAGE.md))
- **Batch processing** -- process multiple jobs at once via `batch: { size, timeout? }` ([Usage](docs/USAGE.md#batch-processing))
- **Request-reply** -- `queue.addAndWait(name, data, { waitTimeout })` for synchronous RPC ([Usage](docs/USAGE.md#request-reply-with-addandwait))
- **LIFO mode** -- `lifo: true` processes newest jobs first ([Advanced](docs/ADVANCED.md#lifo-mode))
- **Job TTL** -- auto-expire jobs after a time-to-live window ([Advanced](docs/ADVANCED.md#job-ttl))
- **Custom job IDs** -- deterministic, idempotent enqueue; duplicates return `null` ([Advanced](docs/ADVANCED.md#custom-job-ids))
- **Pluggable serializers** -- swap JSON for any `{ serialize, deserialize }` implementation ([Advanced](docs/ADVANCED.md#pluggable-serializers))
- **Transparent compression** -- gzip payloads at the queue level ([Advanced](docs/ADVANCED.md#transparent-compression))

### Reliability

- **Retries with exponential, fixed, or custom backoff** and dead-letter queues ([Advanced](docs/ADVANCED.md#retries-and-backoff))
- **UnrecoverableError** -- skip all retries and fail permanently ([Usage](docs/USAGE.md#unrecoverableerror))
- **Stalled recovery** -- auto-reclaim stuck jobs via consumer group PEL and `XAUTOCLAIM` ([Usage](docs/USAGE.md#worker))
- **Job revocation** -- cooperative cancellation with `AbortSignal` ([Advanced](docs/ADVANCED.md#job-revocation))
- **Deduplication** -- simple, throttle, and debounce modes with configurable TTL ([Advanced](docs/ADVANCED.md#deduplication))
- **Per-key ordering** -- sequential processing per ordering key with configurable group concurrency ([Advanced](docs/ADVANCED.md#ordering-and-group-concurrency))
- **Rate limiting** -- per-group sliding window, token bucket, and global queue-wide limits ([Advanced](docs/ADVANCED.md#global-rate-limiting))
- **Sandboxed processors** -- run processors in worker threads or child processes ([Architecture](docs/ARCHITECTURE.md))

### Orchestration

- **FlowProducer** -- parent-child job trees with `chain`, `group`, and `chord` helpers ([Workflows](docs/WORKFLOWS.md))
- **DAG workflows** -- arbitrary dependency graphs with `FlowProducer.addDAG()` and `dag()` helper; multi-parent fan-in, diamond patterns, cycle detection ([Workflows](docs/WORKFLOWS.md))
- **Step jobs** -- `job.moveToDelayed(timestamp, nextStep)` suspends a job mid-processor and resumes later ([Usage](docs/USAGE.md#pause-and-resume-a-job-later-step-jobs))
- **Dynamic children** -- `job.moveToWaitingChildren()` pauses a parent to add children mid-execution ([Workflows](docs/WORKFLOWS.md))
- **Batch processing** -- process multiple jobs at once for bulk I/O ([Usage](docs/USAGE.md#batch-processing))

### Scheduling

- **Cron and interval schedulers** -- 5-field cron with timezone, fixed intervals, and `repeatAfterComplete` mode ([Advanced](docs/ADVANCED.md#job-schedulers))
- **Bounded schedulers** -- `limit`, `startDate`, and `endDate` for finite schedules ([Advanced](docs/ADVANCED.md#bounded-schedulers))

### Pub/Sub

- **Broadcast** -- fan-out delivery to all subscriber groups ([Usage](docs/USAGE.md#broadcast--broadcastworker))
- **BroadcastWorker** -- independent consumer groups with own retries, concurrency, and backpressure ([Usage](docs/USAGE.md#broadcast--broadcastworker))
- **Subject filtering** -- NATS-style patterns (`*` one segment, `>` trailing wildcard) for topic-based routing ([Usage](docs/USAGE.md#broadcast--broadcastworker))

### Serverless

- **Producer** -- enqueue without EventEmitter overhead, returns plain string IDs ([Usage](docs/USAGE.md))
- **ServerlessPool** -- connection caching across warm Lambda/Edge invocations ([Serverless](docs/SERVERLESS.md))

### Observability

- **QueueEvents** -- real-time stream-based lifecycle events ([Observability](docs/OBSERVABILITY.md))
- **Time-series metrics** -- per-minute throughput and latency retained 24h, recorded server-side ([Observability](docs/OBSERVABILITY.md))
- **OpenTelemetry** -- automatic span emission; bring your own tracer or auto-detect `@opentelemetry/api` ([Observability](docs/OBSERVABILITY.md))
- **Job logs** -- append structured log entries per job with pagination ([Observability](docs/OBSERVABILITY.md))
- **Job mutations** -- `changePriority()`, `changeDelay()`, `promote()` after enqueue; `retryJobs()` and `clean()` in bulk ([Usage](docs/USAGE.md))
- **Graceful shutdown** -- `gracefulShutdown()` helper registers SIGTERM/SIGINT handlers ([Usage](docs/USAGE.md#graceful-shutdown))
- **In-memory testing** -- `TestQueue` and `TestWorker` with zero Valkey dependency ([Testing](docs/TESTING.md))

### Cloud

- **Cluster-native** -- hash-tagged keys `glide:{queueName}:*` route all queue data to the same slot ([Usage](docs/USAGE.md#cluster-mode))
- **IAM authentication** -- native SigV4 auth for AWS ElastiCache and MemoryDB ([Usage](docs/USAGE.md#cluster-mode))
- **AZ-affinity routing** -- `readFrom: 'AZAffinity'` routes reads to same-AZ replicas ([Usage](docs/USAGE.md#cluster-mode))

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

Endpoints: `POST /queues/:name/jobs`, `POST /queues/:name/jobs/bulk`, `GET /queues/:name/jobs/:id`, `POST /queues/:name/pause`, `POST /queues/:name/resume`, `GET /queues/:name/counts`, `GET /health`.

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
| [Testing](docs/TESTING.md) | In-memory `TestQueue` and `TestWorker` -- no Valkey needed |
| [Wire Protocol](docs/WIRE_PROTOCOL.md) | Cross-language FCALL specs, key layout, Python and Go examples |
| [Architecture](docs/ARCHITECTURE.md) | Key design, Valkey functions, LIFO, Broadcast, DAG internals |
| [Durability](docs/DURABILITY.md) | Persistence modes, crash windows, feature-specific durability |
| [Migration](docs/MIGRATION.md) | Coming from BullMQ? API mapping and step-by-step guide |

## Limitations

- Requires a running Valkey 7.0+ or Redis 7.0+ instance. There is no embedded mode.
- Node.js only. The Rust-native NAPI client (`@valkey/valkey-glide`) does not run in browsers or Deno.
- At-least-once delivery semantics. Jobs may be processed more than once after crashes or stalled recovery.
- Not a streaming platform. glide-mq is a job/task queue, not a replacement for Kafka or NATS JetStream.
- Single dependency on `@glidemq/speedkey` (which wraps `@valkey/valkey-glide`). Native addon compilation is required on install.

## Ecosystem

| Package | Description | Links |
|---------|-------------|-------|
| [glide-mq](https://github.com/avifenesh/glide-mq) | Core queue library | [npm](https://www.npmjs.com/package/glide-mq) |
| [@glidemq/hono](https://github.com/avifenesh/glidemq-hono) | Hono middleware -- REST endpoints, SSE, serverless Producer | [npm](https://www.npmjs.com/package/@glidemq/hono) |
| [@glidemq/fastify](https://github.com/avifenesh/glidemq-fastify) | Fastify plugin -- REST endpoints, SSE, serverless Producer | [npm](https://www.npmjs.com/package/@glidemq/fastify) |
| [@glidemq/nestjs](https://github.com/avifenesh/glidemq-nestjs) | NestJS module -- decorators, DI, lifecycle management | [npm](https://www.npmjs.com/package/@glidemq/nestjs) |
| [@glidemq/dashboard](https://github.com/avifenesh/glidemq-dashboard) | Web UI -- metrics charts, scheduler management, job mutations | [npm](https://www.npmjs.com/package/@glidemq/dashboard) |
| [@glidemq/hapi](https://github.com/avifenesh/glidemq-hapi) | Hapi plugin -- REST endpoints, SSE, Joi validation | [npm](https://www.npmjs.com/package/@glidemq/hapi) |
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
