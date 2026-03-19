# Observability Reference

## QueueEvents

Stream-based lifecycle events via `XREAD BLOCK`. Real-time without polling.

```typescript
import { QueueEvents } from 'glide-mq';

const events = new QueueEvents('tasks', { connection });

events.on('added', ({ jobId }) => { ... });
events.on('progress', ({ jobId, data }) => { ... });
events.on('completed', ({ jobId, returnvalue }) => { ... });
events.on('failed', ({ jobId, failedReason }) => { ... });
events.on('stalled', ({ jobId }) => { ... });
events.on('paused', () => { ... });
events.on('resumed', () => { ... });

await events.close();
```

### Disabling Server-Side Events

Save 1 redis.call() per job on high-throughput workloads:

```typescript
const queue = new Queue('tasks', { connection, events: false });
const worker = new Worker('tasks', handler, { connection, events: false });
```

TS-side `EventEmitter` events (`worker.on('completed', ...)`) are unaffected.

### QueueEvents Cannot Share Clients

`QueueEvents` uses `XREAD BLOCK` - always creates its own connection. Throws if you pass `client`.

## Job Logs

```typescript
// Inside processor
await job.log('Starting step 1');
await job.log('Step 1 done');

// Fetching externally
const { logs, count } = await queue.getJobLogs(jobId);
// logs: string[], count: number

// Paginated
const { logs } = await queue.getJobLogs(jobId, 0, 49);   // first 50
const { logs } = await queue.getJobLogs(jobId, 50, 99);  // next 50
```

## Job Progress

```typescript
// Inside processor
await job.updateProgress(50);              // number (0-100)
await job.updateProgress({ step: 3 });     // or object

// Listen via QueueEvents
events.on('progress', ({ jobId, data }) => { ... });

// Or via Worker events
worker.on('active', (job) => { ... });
```

## Job Counts

```typescript
const counts = await queue.getJobCounts();
// { waiting: 12, active: 3, delayed: 5, completed: 842, failed: 7 }

const waitingCount = await queue.count();  // stream length only
```

## Time-Series Metrics

```typescript
const metrics = await queue.getMetrics('completed');
// {
//   count: 15234,
//   data: [
//     { timestamp: 1709654400000, count: 142, avgDuration: 234 },
//     { timestamp: 1709654460000, count: 156, avgDuration: 218 },
//   ],
//   meta: { resolution: 'minute' }
// }

// Slice (e.g., last 10 data points)
const recent = await queue.getMetrics('completed', { start: -10 });
```

- Recorded server-side with zero extra RTTs.
- Minute-resolution buckets retained for 24 hours, trimmed automatically.
- Type: `'completed'` or `'failed'`.

### Disabling Metrics

```typescript
const worker = new Worker('tasks', handler, {
  connection,
  metrics: false,  // skip HINCRBY per job
});
```

## Waiting for a Job

```typescript
// Poll job hash until finished
const state = await job.waitUntilFinished(pollIntervalMs, timeoutMs);
// Returns 'completed' | 'failed'

// Request-reply (no polling)
const result = await queue.addAndWait('inference', data, { waitTimeout: 30_000 });
```

## OpenTelemetry

Auto-emits spans when `@opentelemetry/api` is installed. No code changes needed.

```bash
npm install @opentelemetry/api
```

Initialize tracer provider before creating Queue/Worker (standard OTel setup).

### Custom Tracer

```typescript
import { setTracer, isTracingEnabled } from 'glide-mq';
import { trace } from '@opentelemetry/api';

setTracer(trace.getTracer('my-service', '1.0.0'));
console.log('Tracing:', isTracingEnabled());
```

### Instrumented Operations

| Operation | Span Name | Key Attributes |
|-----------|-----------|----------------|
| `queue.add()` | `glide-mq.queue.add` | `glide-mq.queue`, `glide-mq.job.name`, `glide-mq.job.id`, `.delay`, `.priority` |
| `flowProducer.add()` | `glide-mq.flow.add` | `glide-mq.queue`, `glide-mq.flow.name`, `.childCount` |

## Gotchas

- `QueueEvents` always creates its own connection - cannot use shared `client`.
- Disabling `events` only affects the Valkey events stream, not TS-side EventEmitter.
- `getMetrics()` type is `'completed'` or `'failed'` only.
- OTel spans are automatic if `@opentelemetry/api` is installed - no explicit setup in glide-mq.
- `job.waitUntilFinished()` does NOT require QueueEvents (unlike BullMQ) - polls job hash directly.
