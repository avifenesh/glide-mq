# glide-mq

**High-performance message queue for Node.js** — powered by Valkey/Redis Streams and a Rust-native NAPI client.

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

- **Queues & Workers** — producer/consumer with configurable concurrency
- **Workflows** — `FlowProducer` trees, `chain`, `group`, `chord` pipelines
- **Schedulers** — cron and interval repeatable jobs
- **Rate limiting** — token-bucket and global rate limiting
- **Retries & DLQ** — exponential/fixed backoff with dead-letter queues
- **Deduplication** — content- and id-based job dedup
- **Compression** — transparent gzip (up to 98% size reduction)
- **Observability** — OpenTelemetry tracing, job-level logs, [`@glidemq/dashboard`](https://github.com/avifenesh/glidemq-dashboard) middleware
- **In-memory testing** — `TestQueue` & `TestWorker` with zero dependencies via `glide-mq/testing`

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

## Documentation

| Guide | What you'll learn |
|-------|-------------------|
| [Usage](docs/USAGE.md) | Queue & Worker basics, graceful shutdown, cluster mode |
| [Advanced](docs/ADVANCED.md) | Schedulers, rate limiting, dedup, compression, retries & DLQ |
| [Workflows](docs/WORKFLOWS.md) | FlowProducer, `chain`, `group`, `chord` pipelines |
| [Observability](docs/OBSERVABILITY.md) | OpenTelemetry, job logs, `@glidemq/dashboard` |
| [Testing](docs/TESTING.md) | In-memory `TestQueue` & `TestWorker` — no Valkey needed |
| [Migration](docs/MIGRATION.md) | Coming from BullMQ? API mapping & workarounds |

## Get Involved

- ⭐ [Star on GitHub](https://github.com/avifenesh/glide-mq) — helps others find the project
- 🐛 [Open an issue](https://github.com/avifenesh/glide-mq/issues) — bug reports & feature requests welcome
- 💬 [Discussions](https://github.com/avifenesh/glide-mq/discussions) — questions, ideas, show & tell

## License

Apache-2.0
