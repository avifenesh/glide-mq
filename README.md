# glide-mq

High-performance message queue for Node.js — Streams-first, native NAPI bindings, cloud-native by default.

Built on [Valkey Streams](https://valkey.io) + consumer groups with [Valkey GLIDE](https://github.com/valkey-io/valkey-glide)'s Rust core. Single function library instead of 53 EVAL scripts. 1 RTT per job.

## Performance

### Processing throughput

| Concurrency | Throughput |
|-------------|-----------|
| c=1 | 4,376 jobs/s |
| c=5 | 14,925 jobs/s |
| c=10 | 15,504 jobs/s |
| c=50 | 48,077 jobs/s |

### Bulk add (`addBulk` via Batch API)

| Jobs | Serial | Batch | Speedup |
|------|--------|-------|---------|
| 200 | 76ms | 14ms | 5.4x |
| 1,000 | 228ms | 18ms | 12.7x |

### Payload compression

| Mode | Stored size (15KB payload) | Savings |
|------|--------------------------|---------|
| Plain | 15,327 bytes | — |
| Gzip | 331 bytes | 98% |

*No-op processor, Valkey 8.0, single node.*

## Why glide-mq

| Feature | glide-mq | Traditional queue |
|---------|----------|-------------------|
| Queue backend | Streams + PEL | Lists + BRPOPLPUSH |
| Server scripts | 1 function library | 53 EVAL scripts |
| RTT per job | 1 (`completeAndFetchNext`) | 2+ |
| Cluster support | Built-in hash tags | Afterthought `{braces}` |
| Client | NAPI Rust core | ioredis |
| AZ-Affinity | ✓ | ✗ |
| IAM auth | ✓ | ✗ |

## Install

```bash
npm install glide-mq
```

Requires Node.js 20+ and a running Valkey (7.0+) or Redis (7.0+) instance.

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

## Documentation

- **[docs/USAGE.md](docs/USAGE.md)** — Queue & Worker basics, graceful shutdown, cluster mode, event listeners
- **[docs/ADVANCED.md](docs/ADVANCED.md)** — Schedulers, ordering, token bucket, deduplication, global concurrency, revocation, compression, retries & DLQ
- **[docs/WORKFLOWS.md](docs/WORKFLOWS.md)** — FlowProducer, `chain`, `group`, `chord` pipelines
- **[docs/OBSERVABILITY.md](docs/OBSERVABILITY.md)** — Job logs, metrics, OpenTelemetry, `@glidemq/dashboard`
- **[docs/TESTING.md](docs/TESTING.md)** — `TestQueue` & `TestWorker`, in-memory testing without Valkey
- **[docs/MIGRATION.md](docs/MIGRATION.md)** — Migrating from BullMQ: API mapping, gaps, and workarounds

## License

Apache-2.0
