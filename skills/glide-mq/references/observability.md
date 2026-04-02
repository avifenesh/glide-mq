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
events.on('usage', ({ jobId, data }) => { ... });  // AI usage reported

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

## AI Usage Telemetry

### Per-Job Usage

```typescript
// Report usage inside a processor
await job.reportUsage({
  model: 'gpt-5.4',
  provider: 'openai',
  tokens: { input: 500, output: 200 },
  costs: { total: 0.003 },
  costUnit: 'usd',
  latencyMs: 800,
  cached: false,
});

// Emits a 'usage' event on the events stream
events.on('usage', ({ jobId, data }) => {
  const usage = JSON.parse(data);
  console.log(`Job ${jobId}: ${usage.model} - ${usage.totalTokens} tokens`);
});

// Read usage from a completed job
const job = await queue.getJob(jobId);
console.log(job.usage);
// { model, provider, tokens, totalTokens, costs, totalCost, costUnit, latencyMs, cached }
```

### Flow-Level Aggregation

```typescript
const usage = await queue.getFlowUsage(parentJobId);
// {
//   tokens: { input: 2500, output: 1200 },
//   totalTokens: 3700,
//   costs: { total: 0.015 },
//   totalCost: 0.015,
//   costUnit: 'usd',
//   jobCount: 4,
//   models: { 'gpt-5.4': 3, 'claude-sonnet-4-20250514': 1 }
// }
```

Walks the parent job and all children via the deps set. Includes usage from the parent itself.

### Rolling Usage Summary

```typescript
const summary = await queue.getUsageSummary({
  queues: ['tasks', 'embeddings'],
  windowMs: 3_600_000,
});

// { totalTokens, totalCost, jobCount, models, perQueue }
```

This reads rolling per-minute buckets instead of scanning job hashes, so it is the right primitive for dashboards and queue-wide cost telemetry.

### Budget Monitoring

```typescript
const budget = await queue.getFlowBudget(flowId);
if (budget && budget.exceeded) {
  console.warn(`Flow ${flowId} exceeded budget: ${budget.usedTokens} tokens, $${budget.usedCost}`);
}
```

## Proxy SSE Surfaces

For cross-language observability, the HTTP proxy exposes:

| Path | Description |
|------|-------------|
| `/queues/:name/events` | Queue-wide lifecycle events via SSE with `Last-Event-ID` resume |
| `/queues/:name/jobs/:id/stream` | Per-job streaming output via SSE |
| `/broadcast/:name/events` | Broadcast SSE with `subscription` and optional `subjects` filters |

These routes require the proxy to be created with `connection`, because they allocate blocking readers internally.

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
| `flowProducer.addDAG()` | `glide-mq.flow.addDAG` | `glide-mq.flow.nodeCount` |

## Gotchas

- `QueueEvents` always creates its own connection - cannot use shared `client`.
- Disabling `events` only affects the Valkey events stream, not TS-side EventEmitter.
- `getMetrics()` type is `'completed'` or `'failed'` only.
- OTel spans are automatic if `@opentelemetry/api` is installed - no explicit setup in glide-mq.
- `job.waitUntilFinished()` does NOT require QueueEvents (unlike BullMQ) - polls job hash directly.
