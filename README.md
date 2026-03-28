# glide-mq

[![npm version](https://img.shields.io/npm/v/glide-mq)](https://www.npmjs.com/package/glide-mq)
[![license](https://img.shields.io/npm/l/glide-mq)](https://github.com/avifenesh/glide-mq/blob/main/LICENSE)
[![CI](https://github.com/avifenesh/glide-mq/actions/workflows/ci.yml/badge.svg)](https://github.com/avifenesh/glide-mq/actions/workflows/ci.yml)

High-performance message queue for Node.js with first-class AI orchestration. Built on Valkey/Redis Streams with a Rust NAPI core.

Completes and fetches the next job in a single server-side function call (1 RTT per job), hash-tags every key for zero-config clustering, and ships seven built-in primitives for LLM orchestration - cost tracking, token streaming, human-in-the-loop, model failover, TPM rate limiting, budget caps, and vector search.

```bash
npm install glide-mq
```

### General Usage

```typescript
import { Queue, Worker } from 'glide-mq';

const connection = { addresses: [{ host: 'localhost', port: 6379 }] };
const queue = new Queue('tasks', { connection });

await queue.add('send-email', { to: 'user@example.com', subject: 'Welcome' });

const worker = new Worker(
  'tasks',
  async (job) => {
    await sendEmail(job.data.to, job.data.subject);
    return { sent: true };
  },
  { connection, concurrency: 10 },
);
```

### AI Usage

```typescript
import { Queue, Worker } from 'glide-mq';

const queue = new Queue('ai', { connection });

await queue.add(
  'inference',
  { prompt: 'Explain message queues' },
  {
    fallbacks: [{ model: 'gpt-5.4-nano', provider: 'openai' }],
    lockDuration: 120000,
  },
);

const worker = new Worker(
  'ai',
  async (job) => {
    const result = await callLLM(job.data.prompt);
    await job.reportUsage({
      model: 'gpt-5.4',
      tokens: { input: 50, output: 200 },
      costs: { total: 0.003 },
    });
    await job.stream({ type: 'token', content: result });
    return result;
  },
  { connection, tokenLimiter: { maxTokens: 100000, duration: 60000 } },
);
```

## When to use glide-mq

- **Background jobs and task processing** - email, image processing, data pipelines, webhooks, any async work.
- **Scheduled and recurring work** - cron jobs, interval tasks, bounded schedulers.
- **Distributed workflows** - parent-child trees, DAGs, fan-in/fan-out, step jobs, dynamic children.
- **High-throughput queues over real networks** - 1 RTT per job via Valkey Server Functions, up to 38% faster than alternatives.
- **LLM pipelines and model orchestration** - cost tracking, token streaming, model failover, budget caps without external middleware.
- **Valkey/Redis clusters** - hash-tagged keys out of the box with zero configuration.

## How it's different

| Aspect              | glide-mq                                                                                                  |
| ------------------- | --------------------------------------------------------------------------------------------------------- |
| **Network per job** | 1 RTT - complete + fetch next in a single FCALL                                                           |
| **Client**          | Rust NAPI bindings via [valkey-glide](https://github.com/valkey-io/valkey-glide) - no JS protocol parsing |
| **Server logic**    | Persistent Valkey Function library (FUNCTION LOAD + FCALL) - no per-call EVAL                             |
| **Cluster**         | Hash-tagged keys (`glide:{queueName}:*`) route to the same slot automatically                             |
| **AI-native**       | Cost tracking, token streaming, suspend/resume, fallback chains, TPM limits, budget caps                  |
| **Vector search**   | KNN similarity queries over job data via Valkey Search                                                    |

## AI-native primitives

Seven primitives for LLM and agent workflows, built into the core API.

- **Cost tracking** - `job.reportUsage()` records model, tokens, cost, latency per job. `queue.getFlowUsage()` aggregates across flows.
- **Token streaming** - `job.stream(chunk)` pushes LLM output tokens in real time. `queue.readStream(jobId)` consumes them with optional long-polling.
- **Suspend/resume** - `job.suspend()` pauses mid-processor for human approval or webhook callback. `queue.signal(jobId, name, data)` resumes with external input.
- **Fallback chains** - ordered `fallbacks` array on job options. On failure, the next retry reads `job.currentFallback` for the alternate model/provider.
- **TPM rate limiting** - `tokenLimiter` on worker options enforces tokens-per-minute caps. Combine with RPM `limiter` for dual-axis rate control.
- **Budget caps** - `FlowProducer.add(flow, { budget })` sets `maxTotalTokens` and `maxTotalCost` across all jobs in a flow. Jobs fail or pause when exceeded.
- **Per-job lock duration** - override `lockDuration` per job for adaptive stall detection. Short for classifiers, long for multi-minute LLM calls.

See [Usage - AI-native primitives](docs/USAGE.md#ai-native-primitives) for full examples.

## Features

- **1 RTT per job** - complete current + fetch next in a single server-side function call
- **Cluster-native** - hash-tagged keys, zero cluster configuration
- **Workflows** - FlowProducer trees, DAGs with fan-in, chain/group/chord, step jobs, dynamic children
- **Scheduling** - 5-field cron with timezone, fixed intervals, bounded schedulers
- **Retries** - exponential, fixed, or custom backoff with dead-letter queues
- **Rate limiting** - per-group sliding window, token bucket, global queue-wide limits
- **Broadcast** - fan-out pub/sub with NATS-style subject filtering and independent subscriber retries
- **Batch processing** - process multiple jobs at once for bulk I/O
- **Request-reply** - `queue.addAndWait()` for synchronous RPC patterns
- **Deduplication** - simple, throttle, and debounce modes
- **Compression** - transparent gzip at the queue level
- **Serverless** - lightweight `Producer` and `ServerlessPool` for Lambda/Edge
- **OpenTelemetry** - automatic span emission with bring-your-own tracer
- **In-memory testing** - `TestQueue` and `TestWorker` with zero Valkey dependency
- **Cross-language** - HTTP proxy and wire protocol for non-Node.js services

## Performance

Benchmarked on AWS ElastiCache Valkey 8.2 (r7g.large) with TLS, EC2 client in the same region.

| Concurrency |   glide-mq |     BullMQ | Delta |
| :---------: | ---------: | ---------: | :---: |
|     c=5     | 10,754 j/s |  9,866 j/s |  +9%  |
|    c=10     | 18,218 j/s | 13,541 j/s | +35%  |
|    c=15     | 19,583 j/s | 14,162 j/s | +38%  |
|    c=20     | 19,408 j/s | 16,085 j/s | +21%  |

The advantage comes from completing and fetching the next job in a single FCALL. The savings compound over real network latency - exactly the conditions in every production deployment. At high concurrency both libraries converge toward the Valkey single-thread ceiling.

Reproduce with `npm run bench` or `npx tsx benchmarks/elasticache-head-to-head.ts` against your own infrastructure.

## Examples

All examples live in [glidemq-examples](https://github.com/avifenesh/glidemq-examples). Pick a directory, `npm install`, `npx tsx <file>.ts`.

| AI Orchestration | Core Patterns | Framework Plugins |
|------------------|---------------|-------------------|
| [Usage & Costs](https://github.com/avifenesh/glidemq-examples/tree/main/examples/ai-usage-and-costs) | [Basics](https://github.com/avifenesh/glidemq-examples/tree/main/examples/core-basics) | [Hono](https://github.com/avifenesh/glidemq-examples/tree/main/examples/hono-api) |
| [Budget & TPM](https://github.com/avifenesh/glidemq-examples/tree/main/examples/ai-budget) | [Workflows](https://github.com/avifenesh/glidemq-examples/tree/main/examples/core-workflows) | [Fastify](https://github.com/avifenesh/glidemq-examples/tree/main/examples/fastify-api) |
| [Streaming](https://github.com/avifenesh/glidemq-examples/tree/main/examples/ai-streaming) | [Advanced](https://github.com/avifenesh/glidemq-examples/tree/main/examples/core-advanced) | [Hapi](https://github.com/avifenesh/glidemq-examples/tree/main/examples/hapi-api) |
| [Agent Loops](https://github.com/avifenesh/glidemq-examples/tree/main/examples/ai-agents) | [DAG Flows](https://github.com/avifenesh/glidemq-examples/tree/main/examples/dag-workflows) | [NestJS](https://github.com/avifenesh/glidemq-examples/tree/main/examples/nestjs-module) |
| [Search & Vectors](https://github.com/avifenesh/glidemq-examples/tree/main/examples/ai-search) | [Scheduling](https://github.com/avifenesh/glidemq-examples/tree/main/examples/cron-scheduler) | [Express](https://github.com/avifenesh/glidemq-examples/tree/main/examples/express-dashboard) |
| [SDK Integrations](https://github.com/avifenesh/glidemq-examples/tree/main/examples/ai-integrations) | [Batch Processing](https://github.com/avifenesh/glidemq-examples/tree/main/examples/batch-processing) | [Next.js](https://github.com/avifenesh/glidemq-examples/tree/main/examples/nextjs-api-routes) |

## When NOT to use glide-mq

- **You need a log-based event streaming platform.** glide-mq is a job/task queue, not a partitioned event log. It does not provide Kafka-style topic partitions, consumer offset management, or event replay.
- **You need browser support.** The Rust NAPI client requires a server-side runtime (Node.js 20+, Bun, or Deno with NAPI support).
- **You need exactly-once semantics.** glide-mq provides at-least-once delivery. Duplicate processing is rare but possible - design processors to be idempotent.
- **You need to run without Valkey or Redis.** Production use requires Valkey 7.0+ or Redis 7.0+. For dev/testing, `TestQueue`/`TestWorker` run fully in-memory.

## Documentation

| Guide                                  | Topics                                                      |
| -------------------------------------- | ----------------------------------------------------------- |
| [Usage](docs/USAGE.md)                 | Queue, Worker, Producer, batch, request-reply, cluster mode |
| [Workflows](docs/WORKFLOWS.md)         | FlowProducer, DAG, chain/group/chord, dynamic children      |
| [Advanced](docs/ADVANCED.md)           | Schedulers, rate limiting, dedup, compression, retries, DLQ |
| [Broadcast](docs/BROADCAST.md)         | Pub/sub fan-out, subject filtering                          |
| [Observability](docs/OBSERVABILITY.md) | OpenTelemetry, metrics, job logs, dashboard                 |
| [Serverless](docs/SERVERLESS.md)       | Producer, ServerlessPool, Lambda/Edge                       |
| [Testing](docs/TESTING.md)             | In-memory TestQueue and TestWorker                          |
| [Wire Protocol](docs/WIRE_PROTOCOL.md) | Cross-language FCALL specs, Python/Go examples              |
| [Step Jobs](docs/STEP_JOBS.md)         | Step-job workflows with moveToDelayed                       |
| [Durability](docs/DURABILITY.md)       | Durability guarantees, persistence, delivery semantics      |
| [Architecture](docs/ARCHITECTURE.md)   | Internal architecture and design reference                  |
| [Migration](docs/MIGRATION.md)         | Coming from BullMQ - API mapping guide                      |

## Ecosystem

| Package                                                              | Description                                   |
| -------------------------------------------------------------------- | --------------------------------------------- |
| [@glidemq/speedkey](https://github.com/avifenesh/speedkey)           | Valkey GLIDE client with native NAPI bindings |
| [@glidemq/dashboard](https://github.com/avifenesh/glidemq-dashboard) | Web UI for metrics, schedulers, job mutations |
| [@glidemq/hono](https://github.com/avifenesh/glidemq-hono)           | Hono middleware                               |
| [@glidemq/fastify](https://github.com/avifenesh/glidemq-fastify)     | Fastify plugin                                |
| [@glidemq/nestjs](https://github.com/avifenesh/glidemq-nestjs)       | NestJS module                                 |
| [@glidemq/hapi](https://github.com/avifenesh/glidemq-hapi)           | Hapi plugin                                   |
| [glidemq.dev](https://avifenesh.github.io/glidemq.dev/)            | Full documentation site                       |

## Contributing

Bug reports, feature requests, and pull requests are welcome.

- [Open an issue](https://github.com/avifenesh/glide-mq/issues)
- [Discussions](https://github.com/avifenesh/glide-mq/discussions)
- [Changelog](CHANGELOG.md)

## License

Apache-2.0
