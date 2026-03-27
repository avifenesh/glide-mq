# glide-mq

[![npm version](https://img.shields.io/npm/v/glide-mq)](https://www.npmjs.com/package/glide-mq)
[![license](https://img.shields.io/npm/l/glide-mq)](https://github.com/avifenesh/glide-mq/blob/main/LICENSE)
[![CI](https://github.com/avifenesh/glide-mq/actions/workflows/ci.yml/badge.svg)](https://github.com/avifenesh/glide-mq/actions/workflows/ci.yml)

AI-native message queue for Node.js built on Valkey/Redis Streams with a Rust NAPI core.

glide-mq completes and fetches the next job in a single server-side function call (1 RTT per job), hash-tags every key for zero-config clustering, and ships seven built-in primitives for LLM orchestration - cost tracking, token streaming, human-in-the-loop, model failover, TPM rate limiting, budget caps, and vector search. No Lua EVAL, no cluster hacks, no plugins.

```bash
npm install glide-mq
```

```typescript
import { Queue, Worker } from 'glide-mq';

const connection = { addresses: [{ host: 'localhost', port: 6379 }] };
const queue = new Queue('ai', { connection });

await queue.add('inference', { prompt: 'Explain message queues' }, {
  fallbacks: [{ model: 'gpt-5.4-nano', provider: 'openai' }],
  lockDuration: 120000,
});

const worker = new Worker('ai', async (job) => {
  const result = await callLLM(job.data.prompt);
  await job.reportUsage({ model: 'gpt-5.4', inputTokens: 50, outputTokens: 200, costUsd: 0.003 });
  await job.stream({ type: 'token', content: result });
  return result;
}, { connection, tokenLimiter: { maxTokens: 100000, duration: 60000 } });
```

## When to use glide-mq

- You are building **LLM pipelines** and need cost tracking, token streaming, model failover, or budget caps without external middleware.
- You need **high throughput over real networks** - 1 RTT per job via Valkey Server Functions, up to 36% faster than alternatives at typical concurrency.
- You run **Valkey/Redis clusters** and want hash-tagged keys out of the box with zero configuration.
- You need **workflows** - parent-child trees, DAGs, step jobs, cron scheduling.
- You deploy to **serverless** and want lightweight connection caching across warm invocations.

## How it's different

| Aspect | glide-mq |
|--------|----------|
| **Network per job** | 1 RTT - complete + fetch next in a single FCALL |
| **Client** | Rust NAPI bindings via [valkey-glide](https://github.com/valkey-io/valkey-glide) - no JS protocol parsing |
| **Server logic** | Persistent Valkey Function library (FUNCTION LOAD + FCALL) - no per-call EVAL |
| **Cluster** | Hash-tagged keys (`glide:{queueName}:*`) route to the same slot automatically |
| **AI-native** | Cost tracking, token streaming, suspend/resume, fallback chains, TPM limits, budget caps |
| **Vector search** | KNN similarity queries over job data via Valkey Search |

## AI-native primitives

Seven primitives for LLM and agent workflows, built into the core API.

- **Cost tracking** - `job.reportUsage()` records model, tokens, cost, latency per job. `queue.getFlowUsage()` aggregates across flows.
- **Token streaming** - `job.stream(chunk)` pushes LLM output tokens in real time. `queue.readStream(jobId)` consumes them with optional long-polling.
- **Suspend/resume** - `job.suspend()` pauses mid-processor for human approval or webhook callback. `queue.signal(jobId, name, data)` resumes with external input.
- **Fallback chains** - ordered `fallbacks` array on job options. On failure, the next retry reads `job.currentFallback` for the alternate model/provider.
- **TPM rate limiting** - `tokenLimiter` on worker options enforces tokens-per-minute caps. Combine with RPM `limiter` for dual-axis rate control.
- **Budget caps** - `FlowProducer.add(flow, { budget })` sets `maxTotalTokens` and `maxCostUsd` across all jobs in a flow. Jobs fail or pause when exceeded.
- **Per-job lock duration** - override `lockDuration` per job for adaptive stall detection. Short for classifiers, long for multi-minute LLM calls.

See [Usage - AI-native primitives](docs/USAGE.md#ai-native-primitives) for full examples.

## Vector search

Create indexes over job data, store embeddings, and run KNN similarity queries. Requires `valkey-bundle` with the search module.

```typescript
await queue.createJobIndex({
  vectorField: { name: 'embedding', dimensions: 1536, algorithm: 'HNSW', distanceMetric: 'COSINE' },
  fields: [{ type: 'TAG', name: 'category' }],
});

await job.storeVector('embedding', embedding);
const results = await queue.vectorSearch(queryEmbedding, { k: 10, filter: '@category:{ml}' });
```

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

| Concurrency | glide-mq | BullMQ | Delta |
|:-----------:|----------:|-------:|:-----:|
| c=5 | 10,754 j/s | 9,866 j/s | +9% |
| c=10 | 18,218 j/s | 13,541 j/s | +35% |
| c=15 | 19,583 j/s | 14,162 j/s | +38% |
| c=20 | 19,408 j/s | 16,085 j/s | +21% |

The advantage comes from completing and fetching the next job in a single FCALL. The savings compound over real network latency - exactly the conditions in every production deployment. At high concurrency both libraries converge toward the Valkey single-thread ceiling.

Reproduce with `npm run bench` or `npx tsx benchmarks/elasticache-head-to-head.ts` against your own infrastructure.

## Examples

18 runnable examples in `examples/`. Run any with `npx tsx examples/<name>.ts`.

| Example | What it shows |
|---------|---------------|
| `usage-tracking.ts` | Token and cost tracking across multi-step flows |
| `token-streaming.ts` | Real-time LLM token streaming to clients |
| `human-approval.ts` | Suspend/resume with editorial review gate |
| `model-failover.ts` | Fallback chains across providers |
| `tpm-throttle.ts` | Dual-axis RPM + TPM rate limiting |
| `budget-cap.ts` | Flow-level token and cost caps |
| `vector-search.ts` | KNN similarity search with pre-filters |
| `with-langchain.ts` | LangChain integration with token tracking |
| `with-vercel-ai-sdk.ts` | Vercel AI SDK integration with streaming |
| `rag-pipeline.ts` | RAG with embedding, indexing, retrieval |
| `ai-agent-loop.ts` | Autonomous agent loop with budget enforcement |
| `testing-mode.ts` | In-memory testing without Valkey |

## When NOT to use glide-mq

- **You need a log-based event streaming platform.** glide-mq is a job/task queue, not a partitioned event log. It does not provide Kafka-style topic partitions, consumer offset management, or event replay. For fan-out messaging glide-mq has Broadcast (Valkey Streams-based pub/sub with subject filtering), but the underlying primitive is streams-with-consumer-groups, not Valkey's native Pub/Sub.
- **You need browser support.** The Rust NAPI client requires a server-side runtime (Node.js 20+, Bun, or Deno with NAPI support).
- **You need exactly-once semantics.** glide-mq provides at-least-once delivery. Duplicate processing is rare (only on hard crashes during the completion path) but possible - design processors to be idempotent.
- **You need to run without Valkey or Redis.** Production use requires a Valkey 7.0+ or Redis 7.0+ instance. For development and testing, `TestQueue`/`TestWorker` run fully in-memory with no external dependencies.

## Documentation

| Guide | Topics |
|-------|--------|
| [Usage](docs/USAGE.md) | Queue, Worker, Producer, batch, request-reply, cluster mode |
| [Workflows](docs/WORKFLOWS.md) | FlowProducer, DAG, chain/group/chord, dynamic children |
| [Advanced](docs/ADVANCED.md) | Schedulers, rate limiting, dedup, compression, retries, DLQ |
| [Broadcast](docs/BROADCAST.md) | Pub/sub fan-out, subject filtering |
| [Observability](docs/OBSERVABILITY.md) | OpenTelemetry, metrics, job logs, dashboard |
| [Serverless](docs/SERVERLESS.md) | Producer, ServerlessPool, Lambda/Edge |
| [Testing](docs/TESTING.md) | In-memory TestQueue and TestWorker |
| [Wire Protocol](docs/WIRE_PROTOCOL.md) | Cross-language FCALL specs, Python/Go examples |
| [Migration](docs/MIGRATION.md) | Coming from BullMQ - API mapping guide |

## Ecosystem

| Package | Description |
|---------|-------------|
| [@glidemq/speedkey](https://github.com/avifenesh/speedkey) | Valkey GLIDE client with native NAPI bindings |
| [@glidemq/dashboard](https://github.com/avifenesh/glidemq-dashboard) | Web UI for metrics, schedulers, job mutations |
| [@glidemq/hono](https://github.com/avifenesh/glidemq-hono) | Hono middleware |
| [@glidemq/fastify](https://github.com/avifenesh/glidemq-fastify) | Fastify plugin |
| [@glidemq/nestjs](https://github.com/avifenesh/glidemq-nestjs) | NestJS module |
| [@glidemq/hapi](https://github.com/avifenesh/glidemq-hapi) | Hapi plugin |
| [glide-mq.dev](https://avifenesh.github.io/glide-mq.dev/) | Full documentation site |

## Contributing

Bug reports, feature requests, and pull requests are welcome.

- [Open an issue](https://github.com/avifenesh/glide-mq/issues)
- [Discussions](https://github.com/avifenesh/glide-mq/discussions)
- [Changelog](CHANGELOG.md)

## License

Apache-2.0
