# glide-mq

**High-performance message queue for Node.js** — powered by Valkey/Redis Streams and a Rust-native NAPI client.

If you find this useful, [give it a ⭐ on GitHub](https://github.com/avifenesh/glide-mq) — it helps the project reach more developers.

```bash
npm install glide-mq
```

## Why glide-mq

- **1 RTT per job** — `completeAndFetchNext` finishes the current job and fetches the next one in a single round-trip
- **Rust core, not ioredis** — built on [Valkey GLIDE](https://github.com/valkey-io/valkey-glide)'s native NAPI bindings for lower latency and less GC pressure
- **1 function library, not 53 scripts** — all queue logic runs as a single Valkey Server Function (no EVAL overhead)
- **Cluster-native** — hash-tagged keys work out of the box; no manual `{braces}` needed
- **Cloud-ready** — AZ-affinity routing and IAM auth built in

## Features

### Core queueing
- **Queues & Workers** — producer/consumer with configurable concurrency ([Usage](docs/USAGE.md#queue), [Demo](demo/README.md#demo-scenarios))
- **Delayed, priority, and batch jobs** — schedule jobs, prioritize critical work, and ingest at high throughput with `addBulk` ([Usage](docs/USAGE.md#queue), [Demo](demo/README.md#demo-scenarios))
- **Job search & progress tracking** — query by state/name/data and stream progress updates ([Usage](docs/USAGE.md#worker), [Search tests](tests/search.test.ts))

### Reliability & control
- **Retries, backoff, and DLQ** — exponential/fixed/custom retries with dead-letter queues ([Advanced](docs/ADVANCED.md#retries-and-backoff), [Demo](demo/README.md#demo-scenarios))
- **Stalled recovery, pause/resume, and drain** — auto-reclaim stuck jobs, pause processing, and server-side drain waiting/delayed jobs ([Usage](docs/USAGE.md#worker), [Demo](demo/README.md#api-endpoints-dashboard-server))
- **Job revocation + sandboxed processors** — cooperative cancellation and isolated file-based processors in worker threads/child processes ([Advanced](docs/ADVANCED.md#job-revocation), [Architecture](docs/ARCHITECTURE.md#typescript-api), [Sandbox example](tests/sandbox-integration.test.ts))

### Orchestration & scheduling
- **Workflows** — `FlowProducer` parent-child trees and `chain`/`group`/`chord` helpers ([Workflows](docs/WORKFLOWS.md), [Demo](demo/README.md#demo-scenarios))
- **Schedulers** — cron and interval repeatable jobs persisted across restarts ([Advanced](docs/ADVANCED.md#job-schedulers), [Demo](demo/README.md#demo-scenarios))
- **Per-key ordering, global concurrency, and rate limiting** — deterministic ordering with queue-wide and token-bucket controls ([Advanced](docs/ADVANCED.md#ordering-and-group-concurrency), [Advanced](docs/ADVANCED.md#global-concurrency), [Advanced](docs/ADVANCED.md#global-rate-limiting))
- **Deduplication** — simple, throttle, and debounce modes with TTL ([Advanced](docs/ADVANCED.md#deduplication), [Demo](demo/README.md#demo-scenarios))

### Observability, ops, and testing
- **QueueEvents, metrics, logs, and dashboard** — real-time events + OpenTelemetry + [`@glidemq/dashboard`](https://github.com/avifenesh/glidemq-dashboard) ([Observability](docs/OBSERVABILITY.md), [Dashboard demo API](demo/dashboard-server.ts))
- **Compression, graceful shutdown, and shared connections** — lower payload size and easier process lifecycle management ([Advanced](docs/ADVANCED.md#transparent-compression), [Usage](docs/USAGE.md#graceful-shutdown), [Advanced](docs/ADVANCED.md#shared-client))
- **In-memory testing mode** — `TestQueue` and `TestWorker` with zero Valkey dependency ([Testing](docs/TESTING.md), [Testing-mode test](tests/testing-mode.test.ts))

## Quick Start

```typescript
import { Queue, Worker } from 'glide-mq';

const connection = { addresses: [{ host: 'localhost', port: 6379 }] };

// Producer
const queue = new Queue('tasks', { connection });
await queue.add('send-email', { to: 'user@example.com', subject: 'Hello' });

// Consumer
const worker = new Worker('tasks', async (job) => {
  console.log(`Processing ${job.name}:`, job.data);
  return { sent: true };
}, { connection, concurrency: 10 });

worker.on('completed', (job) => console.log(`Job ${job.id} done`));
worker.on('failed', (job, err) => console.error(`Job ${job.id} failed:`, err.message));
```

Requires Node.js 20+ and a running [Valkey](https://valkey.io) (7.0+) or Redis 7.0+ instance.

## Benchmarks

| Concurrency | Throughput |
|-------------|-----------|
| c=1 | 4,376 jobs/s |
| c=5 | 14,925 jobs/s |
| c=10 | 15,504 jobs/s |
| c=50 | 48,077 jobs/s |

`addBulk` batch API: **1,000 jobs in 18 ms** (12.7× faster than serial).
Gzip compression: **98% payload reduction** on 15 KB payloads.

*Valkey 8.0, single node, no-op processor. Run `npm run bench` to reproduce.*

## Ecosystem

| Package | Description |
|---------|-------------|
| **glide-mq** | Core queue library (you are here) |
| [`@glidemq/hono`](https://github.com/avifenesh/glidemq-hono) | Hono middleware - REST API + SSE events for queue management |
| [`@glidemq/dashboard`](https://github.com/avifenesh/glidemq-dashboard) | Express middleware - web UI for monitoring and managing queues |
| [`@glidemq/speedkey`](https://github.com/avifenesh/speedkey) | Valkey GLIDE client with native NAPI bindings |

### Dashboard

```bash
npm install @glidemq/dashboard
```

```typescript
import { createDashboard } from '@glidemq/dashboard';

app.use('/dashboard', createDashboard([queue1, queue2], {
  // readOnly: true,
  // authorize: (req, action) => checkSession(req),
}));
```

Workers, job schedulers, DLQ, metrics, search, bulk actions (drain, retry, clean) - all from the browser.

### Hono

```bash
npm install @glidemq/hono glide-mq hono
```

```typescript
import { Hono } from 'hono';
import { glideMQ, glideMQApi } from '@glidemq/hono';

const app = new Hono();

app.use(glideMQ({
  connection: { addresses: [{ host: 'localhost', port: 6379 }] },
  queues: { emails: { processor: processEmail, concurrency: 5 } },
}));

app.route('/api/queues', glideMQApi());
```

11 REST endpoints + SSE events, type-safe RPC client, optional Zod validation, in-memory testing mode.

## Documentation

| Guide | What you'll learn | Related examples |
|-------|-------------------|------------------|
| [Usage](docs/USAGE.md) | Queue & Worker basics, graceful shutdown, cluster mode | [Demo scenarios](demo/README.md#demo-scenarios) |
| [Advanced](docs/ADVANCED.md) | Schedulers, rate limiting, dedup, compression, retries & DLQ | [Comprehensive demo app](demo/index.ts) |
| [Workflows](docs/WORKFLOWS.md) | FlowProducer, `chain`, `group`, `chord` pipelines | [Workflow scenarios](demo/README.md#demo-scenarios) |
| [Observability](docs/OBSERVABILITY.md) | OpenTelemetry, job logs, `@glidemq/dashboard` | [Dashboard API server](demo/dashboard-server.ts) |
| [Testing](docs/TESTING.md) | In-memory `TestQueue` & `TestWorker` — no Valkey needed | [Testing mode test](tests/testing-mode.test.ts) |
| [Architecture](docs/ARCHITECTURE.md) | Key design, Valkey functions, data layout | [Architecture validation tests](tests/review-coverage.test.ts) |
| [Migration](docs/MIGRATION.md) | Coming from BullMQ? API mapping & workarounds | [Compatibility suites](tests/compat-bull.test.ts) |

## Get Involved

- ⭐ [Star on GitHub](https://github.com/avifenesh/glide-mq) — helps others find the project
- 🐛 [Open an issue](https://github.com/avifenesh/glide-mq/issues) — bug reports & feature requests welcome
- 💬 [Discussions](https://github.com/avifenesh/glide-mq/discussions) — questions, ideas, show & tell

## License

Apache-2.0
