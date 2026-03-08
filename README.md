# glide-mq

**High-performance message queue for Node.js** - powered by Valkey/Redis Streams and a Rust-native NAPI client.

```bash
npm install glide-mq
```

## Why glide-mq

| | glide-mq | BullMQ | Bee Queue |
|---|---|---|---|
| **Network per job** | 1 RTT (`completeAndFetchNext`) | 4-7 RTTs (lock + complete + fetch) | 2-3 RTTs |
| **Client** | Rust NAPI ([valkey-glide](https://github.com/valkey-io/valkey-glide)) | ioredis (pure JS) | node_redis (pure JS) |
| **Server logic** | 1 Valkey Function library (persistent, named) | 53 EVAL scripts (cache-miss prone) | Lua scripts |
| **Cluster** | Hash-tagged keys, zero config | Manual `{braces}` or workarounds | Not supported |
| **Workflows** | FlowProducer trees, DAG, chain/group/chord | FlowProducer trees | Not supported |
| **Pub/sub** | Native Broadcast with subject filtering | Not supported | Not supported |
| **Serverless** | Producer + ServerlessPool | Not supported | Not supported |
| **Throughput** | 48k jobs/s (c=50) | ~12k jobs/s (c=50) | ~5k jobs/s (c=50) |

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

## Features

### Core

- **Queues & Workers** - producer/consumer with configurable concurrency, prefetch, and lock duration ([Usage](docs/USAGE.md))
- **Delayed, priority, and bulk enqueue** - schedule jobs, prioritize critical work, `addBulk` for high throughput ([Usage](docs/USAGE.md))
- **Batch processing** - process multiple jobs at once via `batch: { size, timeout? }` ([Usage](docs/USAGE.md#batch-processing))
- **Request-reply** - `queue.addAndWait(name, data, { waitTimeout })` for synchronous RPC ([Usage](docs/USAGE.md#request-reply-with-addandwait))
- **Custom job IDs** - deterministic, idempotent enqueue; duplicates return `null` ([Advanced](docs/ADVANCED.md#custom-job-ids))
- **LIFO mode** - `lifo: true` processes newest jobs first; priority > LIFO > FIFO ([Advanced](docs/ADVANCED.md#lifo-mode))
- **Job TTL** - auto-expire jobs after a time-to-live window ([Advanced](docs/ADVANCED.md#job-ttl))
- **Pluggable serializers** - swap JSON for any `{ serialize, deserialize }` implementation ([Advanced](docs/ADVANCED.md#pluggable-serializers))
- **Transparent compression** - gzip payloads at the queue level; 98% reduction on typical JSON ([Advanced](docs/ADVANCED.md#transparent-compression))

### Reliability & control

- **Retries, backoff, and DLQ** - exponential/fixed/custom retries with dead-letter queues ([Advanced](docs/ADVANCED.md#retries-and-backoff))
- **UnrecoverableError** - throw to skip all retries and fail permanently ([Usage](docs/USAGE.md#unrecoverableerror))
- **Stalled recovery** - auto-reclaim stuck jobs via consumer group PEL and `XAUTOCLAIM` ([Usage](docs/USAGE.md#worker))
- **Pause, resume, and drain** - pause processing, drain waiting/delayed jobs server-side ([Usage](docs/USAGE.md#queue))
- **Job revocation** - cooperative cancellation with `AbortSignal` in the processor ([Advanced](docs/ADVANCED.md#job-revocation))
- **Sandboxed processors** - run processors in worker threads or child processes ([Architecture](docs/ARCHITECTURE.md))
- **Durability guide** - persistence modes, crash windows, and failover caveats ([Durability](docs/DURABILITY.md))

### Orchestration & scheduling

- **Workflows** - `FlowProducer` parent-child trees, `chain`/`group`/`chord` helpers ([Workflows](docs/WORKFLOWS.md))
- **DAG flows** - arbitrary dependency graphs with `FlowProducer.addDAG()` and `dag()` helper; multi-parent fan-in, diamond patterns, cycle detection ([Workflows](docs/WORKFLOWS.md))
- **Step jobs** - `job.moveToDelayed(timestamp, nextStep)` suspends a job mid-processor and resumes later ([Usage](docs/USAGE.md#pause-and-resume-a-job-later-step-jobs))
- **Dynamic children** - `job.moveToWaitingChildren()` pauses a parent to add children mid-execution ([Workflows](docs/WORKFLOWS.md))
- **Cron & interval schedulers** - 5-field cron with timezone, fixed intervals, and `repeatAfterComplete` mode ([Advanced](docs/ADVANCED.md#job-schedulers))
- **Bounded schedulers** - `limit`, `startDate`, and `endDate` for finite schedules ([Advanced](docs/ADVANCED.md#bounded-schedulers))
- **Per-key ordering** - sequential processing per ordering key with configurable group concurrency ([Advanced](docs/ADVANCED.md#ordering-and-group-concurrency))
- **Rate limiting** - per-group sliding window, token bucket, and global queue-wide limits ([Advanced](docs/ADVANCED.md#global-rate-limiting))
- **Deduplication** - simple, throttle, and debounce modes with configurable TTL ([Advanced](docs/ADVANCED.md#deduplication))

### Pub/sub

- **Broadcast** - fan-out publisher that delivers each message to all subscribers ([Usage](docs/USAGE.md#broadcast--broadcastworker))
- **BroadcastWorker** - independent consumer groups with own retries, concurrency, and backpressure ([Usage](docs/USAGE.md#broadcast--broadcastworker))
- **Subject filtering** - NATS-style patterns (`*` one segment, `>` trailing wildcard) for topic-based routing; non-matching messages auto-ACKed at zero cost ([Usage](docs/USAGE.md#broadcast--broadcastworker))

### Serverless & edge

- **Producer** - lightweight enqueue without EventEmitter or Job instances, returns plain string IDs ([Usage](docs/USAGE.md))
- **ServerlessPool** - connection caching across warm Lambda/Edge invocations ([Usage](docs/USAGE.md))

### Observability & ops

- **QueueEvents** - real-time stream-based lifecycle events ([Observability](docs/OBSERVABILITY.md))
- **Time-series metrics** - per-minute throughput and latency retained 24h, recorded server-side with zero extra RTTs ([Observability](docs/OBSERVABILITY.md))
- **OpenTelemetry** - automatic span emission; bring your own tracer or auto-detect `@opentelemetry/api` ([Observability](docs/OBSERVABILITY.md))
- **Job logs** - append structured log entries per job, retrievable with pagination ([Observability](docs/OBSERVABILITY.md))
- **Dashboard** - [`@glidemq/dashboard`](https://github.com/avifenesh/glidemq-dashboard) web UI with metrics charts, scheduler management, job mutations ([Observability](docs/OBSERVABILITY.md))
- **Job mutations** - `changePriority()`, `changeDelay()`, `promote()` after enqueue; `retryJobs()` and `clean()` in bulk ([Usage](docs/USAGE.md))
- **Graceful shutdown** - `gracefulShutdown()` helper registers SIGTERM/SIGINT handlers ([Usage](docs/USAGE.md#graceful-shutdown))
- **In-memory testing** - `TestQueue` and `TestWorker` with zero Valkey dependency ([Testing](docs/TESTING.md))

### Cloud & cluster

- **Cluster-native** - hash-tagged keys `glide:{queueName}:*` route all queue data to the same slot ([Usage](docs/USAGE.md#cluster-mode))
- **IAM authentication** - native SigV4 auth for AWS ElastiCache and MemoryDB, no AWS SDK needed ([Usage](docs/USAGE.md#cluster-mode))
- **AZ-affinity routing** - `readFrom: 'AZAffinity'` routes reads to same-AZ replicas ([Usage](docs/USAGE.md#cluster-mode))
- **Cross-language** - HTTP proxy (`glide-mq/proxy`) and wire protocol (`FCALL`) for Python, Go, Java, etc. ([Wire Protocol](docs/WIRE_PROTOCOL.md))

## Feature Highlights

### Broadcast with subject filtering

Deliver messages to multiple subscribers with NATS-style topic routing:

```typescript
import { Broadcast, BroadcastWorker } from 'glide-mq';

const events = new Broadcast('store-events', { connection });

// All order events
const orders = new BroadcastWorker('store-events', processOrder, {
  connection, subscription: 'order-service', subjects: ['orders.>'],
});

// Only payment failures
const alerts = new BroadcastWorker('store-events', sendAlert, {
  connection, subscription: 'payment-alerts', subjects: ['payments.failed'],
});

await events.publish('orders.placed', { orderId: 'ORD-1', total: 99.99 });
await events.publish('payments.failed', { txId: 'TX-1', reason: 'declined' });
// orders receives orders.placed; alerts receives payments.failed
```

### Serverless Producer

Lightweight enqueue for Lambda, Edge, and serverless functions:

```typescript
import { serverlessPool } from 'glide-mq';

export async function handler(event) {
  const producer = serverlessPool.getProducer('tasks', { connection });
  await producer.add('process', event.body); // returns string ID, not Job
  return { statusCode: 202 };
}
// Connection cached across warm invocations
```

### DAG workflows

Arbitrary dependency graphs with multi-parent fan-in:

```typescript
import { dag } from 'glide-mq';

await dag([
  { name: 'fetch-users',  queueName: 'etl', data: { source: 'users' } },
  { name: 'fetch-orders', queueName: 'etl', data: { source: 'orders' } },
  { name: 'join', queueName: 'etl', data: { type: 'inner' },
    deps: ['fetch-users', 'fetch-orders'] }, // waits for BOTH
  { name: 'export', queueName: 'etl', data: { dest: 's3' },
    deps: ['join'] },
], connection);
```

### Batch processing

Process multiple jobs at once for higher throughput on bulk I/O:

```typescript
const worker = new Worker('analytics', async (jobs) => {
  await db.insertMany(jobs.map(j => j.data));
  return jobs.map(() => ({ indexed: true }));
}, { connection, batch: { size: 50, timeout: 100 } });
```

### Step jobs (state machines)

Pause a job mid-execution and resume later at a different step:

```typescript
const worker = new Worker('campaign', async (job) => {
  switch (job.data.step) {
    case 'welcome':
      await sendEmail(job.data.email, 'Welcome!');
      await job.moveToDelayed(Date.now() + 86400000, 'follow-up');
      break; // unreachable - moveToDelayed throws DelayedError
    case 'follow-up':
      await sendEmail(job.data.email, 'How are things?');
      return { completed: true };
  }
}, { connection, promotionInterval: 200 });
```

## Benchmarks

| Concurrency | Throughput |
|-------------|-----------|
| c=1 | 4,376 jobs/s |
| c=5 | 14,925 jobs/s |
| c=10 | 15,504 jobs/s |
| c=50 | 48,077 jobs/s |

`addBulk` batch API: **1,000 jobs in 18 ms** (12.7x faster than serial).
Gzip compression: **98% payload reduction** on 15 KB payloads.

*Valkey 8.0, single node, no-op processor. Run `npm run bench` to reproduce.*

## Ecosystem

| Package | Description |
|---------|-------------|
| **glide-mq** | Core queue library (you are here) |
| [`@glidemq/hono`](https://github.com/avifenesh/glidemq-hono) | Hono middleware - 20 REST endpoints + SSE + serverless Producer |
| [`@glidemq/fastify`](https://github.com/avifenesh/glidemq-fastify) | Fastify plugin - 20 REST endpoints + SSE + serverless Producer |
| [`@glidemq/dashboard`](https://github.com/avifenesh/glidemq-dashboard) | Express middleware - web UI with metrics charts and scheduler management |
| [`@glidemq/nestjs`](https://github.com/avifenesh/glidemq-nestjs) | NestJS module - decorators for Queue, Worker, FlowProducer, Broadcast, Producer |
| [`@glidemq/speedkey`](https://github.com/avifenesh/speedkey) | Valkey GLIDE client with native NAPI bindings |
| [examples](https://github.com/avifenesh/glidemq-examples) | 34 runnable examples across frameworks and use cases |

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

Time-series metrics charts, scheduler CRUD, job mutations, worker monitoring, DLQ, search, bulk actions - all from the browser.

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
  producers: { tasks: {} }, // lightweight serverless producers
}));
app.route('/api/queues', glideMQApi());
```

20 REST endpoints + SSE events, serverless Producer support, optional Zod validation, in-memory testing mode.

### Fastify

```bash
npm install @glidemq/fastify glide-mq fastify
```

```typescript
import Fastify from 'fastify';
import { glideMQPlugin, glideMQRoutes } from '@glidemq/fastify';

const app = Fastify();
await app.register(glideMQPlugin, {
  connection: { addresses: [{ host: 'localhost', port: 6379 }] },
  queues: { emails: { processor: processEmail, concurrency: 5 } },
});
await app.register(glideMQRoutes, { prefix: '/api/queues' });
```

20 REST endpoints + SSE events, serverless Producer support, optional Zod validation, in-memory testing mode.

### NestJS

```bash
npm install @glidemq/nestjs glide-mq
```

```typescript
import { GlideMQModule } from '@glidemq/nestjs';

@Module({
  imports: [
    GlideMQModule.forRoot({
      connection: { addresses: [{ host: 'localhost', port: 6379 }] },
      queues: { emails: { processor: processEmail } },
    }),
    GlideMQModule.registerBroadcast({ name: 'events' }),
    GlideMQModule.registerProducer({ name: 'tasks' }),
  ],
})
export class AppModule {}
```

Decorators (`@Processor`, `@BroadcastProcessor`, `@InjectQueue`, `@InjectBroadcast`, `@InjectProducer`), dependency injection, lifecycle management, in-memory testing mode.

## Examples

34 runnable examples in the [examples repo](https://github.com/avifenesh/glidemq-examples):

| Category | Examples |
|----------|----------|
| **Core** | [basics](https://github.com/avifenesh/glidemq-examples/tree/main/examples/core-basics), [workflows](https://github.com/avifenesh/glidemq-examples/tree/main/examples/core-workflows), [advanced](https://github.com/avifenesh/glidemq-examples/tree/main/examples/core-advanced) |
| **Features** | [batch-processing](https://github.com/avifenesh/glidemq-examples/tree/main/examples/batch-processing), [request-reply](https://github.com/avifenesh/glidemq-examples/tree/main/examples/request-reply), [custom-job-ids](https://github.com/avifenesh/glidemq-examples/tree/main/examples/custom-job-ids), [step-jobs](https://github.com/avifenesh/glidemq-examples/tree/main/examples/step-job-move-to-delayed), [waiting-children](https://github.com/avifenesh/glidemq-examples/tree/main/examples/move-to-waiting-children), [serializers](https://github.com/avifenesh/glidemq-examples/tree/main/examples/pluggable-serializers), [exclude-data](https://github.com/avifenesh/glidemq-examples/tree/main/examples/exclude-data), [lifo-mode](https://github.com/avifenesh/glidemq-examples/tree/main/examples/lifo-mode), [dag-workflows](https://github.com/avifenesh/glidemq-examples/tree/main/examples/dag-workflows) |
| **Pub/sub** | [broadcast](https://github.com/avifenesh/glidemq-examples/tree/main/examples/broadcast), [subject-filter](https://github.com/avifenesh/glidemq-examples/tree/main/examples/subject-filter) |
| **Scheduling** | [cron-scheduler](https://github.com/avifenesh/glidemq-examples/tree/main/examples/cron-scheduler), [bounded-schedulers](https://github.com/avifenesh/glidemq-examples/tree/main/examples/bounded-schedulers), [repeat-after-complete](https://github.com/avifenesh/glidemq-examples/tree/main/examples/repeat-after-complete) |
| **Serverless** | [serverless-producer](https://github.com/avifenesh/glidemq-examples/tree/main/examples/serverless-producer) |
| **Infrastructure** | [valkey-cluster](https://github.com/avifenesh/glidemq-examples/tree/main/examples/valkey-cluster), [iam-auth](https://github.com/avifenesh/glidemq-examples/tree/main/examples/iam-auth), [otel-tracing](https://github.com/avifenesh/glidemq-examples/tree/main/examples/otel-tracing), [http-proxy](https://github.com/avifenesh/glidemq-examples/tree/main/examples/http-proxy) |
| **Frameworks** | [express](https://github.com/avifenesh/glidemq-examples/tree/main/examples/express-basic), [koa](https://github.com/avifenesh/glidemq-examples/tree/main/examples/koa-basic), [hono](https://github.com/avifenesh/glidemq-examples/tree/main/examples/hono-basic), [next.js](https://github.com/avifenesh/glidemq-examples/tree/main/examples/nextjs-api-routes), [hono-api](https://github.com/avifenesh/glidemq-examples/tree/main/examples/hono-api), [fastify-api](https://github.com/avifenesh/glidemq-examples/tree/main/examples/fastify-api), [dashboard](https://github.com/avifenesh/glidemq-examples/tree/main/examples/express-dashboard), [nestjs](https://github.com/avifenesh/glidemq-examples/tree/main/examples/nestjs-module) |
| **Use cases** | [email-service](https://github.com/avifenesh/glidemq-examples/tree/main/examples/email-service), [image-pipeline](https://github.com/avifenesh/glidemq-examples/tree/main/examples/image-pipeline), [webhook-delivery](https://github.com/avifenesh/glidemq-examples/tree/main/examples/webhook-delivery) |

## Cross-Language Integration

Non-Node.js services (Python, Go, Java, etc.) can enqueue jobs into glide-mq queues using either:

### Option 1: HTTP Proxy (recommended for most use cases)

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
  -d '{"name": "send-email", "data": {"to": "user@example.com"}, "opts": {"priority": 1}}'
```

**Endpoints**: `POST /queues/:name/jobs`, `POST /queues/:name/jobs/bulk`, `GET /queues/:name/jobs/:id`, `POST /queues/:name/pause`, `POST /queues/:name/resume`, `GET /queues/:name/counts`, `GET /health`.

### Option 2: Direct FCALL (zero overhead, any Valkey client)

Call Valkey Server Functions directly from any language. See [Wire Protocol](docs/WIRE_PROTOCOL.md) for FCALL signatures, key layout, and examples in Python and Go.

## Documentation

| Guide | What you'll learn |
|-------|-------------------|
| [Usage](docs/USAGE.md) | Queue, Worker, Broadcast, Producer, batch, request-reply, step jobs, graceful shutdown, cluster mode |
| [Advanced](docs/ADVANCED.md) | Schedulers, rate limiting, dedup, compression, retries, DLQ, custom IDs, LIFO, TTL, serializers |
| [Workflows](docs/WORKFLOWS.md) | FlowProducer, DAG, `chain`, `group`, `chord`, dynamic children |
| [Observability](docs/OBSERVABILITY.md) | OpenTelemetry, time-series metrics, job logs, `@glidemq/dashboard` |
| [Testing](docs/TESTING.md) | In-memory `TestQueue` & `TestWorker` - no Valkey needed |
| [Wire Protocol](docs/WIRE_PROTOCOL.md) | Cross-language FCALL specs, key layout, Python & Go examples |
| [Architecture](docs/ARCHITECTURE.md) | Key design, Valkey functions, LIFO, Broadcast, DAG internals |
| [Durability](docs/DURABILITY.md) | Persistence modes, crash windows, feature-specific durability |
| [Migration](docs/MIGRATION.md) | Coming from BullMQ? API mapping & step-by-step guide |

## Get Involved

- [Star on GitHub](https://github.com/avifenesh/glide-mq) - helps others find the project
- [Open an issue](https://github.com/avifenesh/glide-mq/issues) - bug reports & feature requests welcome
- [Discussions](https://github.com/avifenesh/glide-mq/discussions) - questions, ideas, show & tell

## License

Apache-2.0
