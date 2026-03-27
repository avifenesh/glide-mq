# Observability

## Table of Contents

- [Job Logs](#job-logs)
- [Job Counts and Metrics](#job-counts-and-metrics)
- [OpenTelemetry Integration](#opentelemetry-integration)
- [AI Usage Telemetry](#ai-usage-telemetry)
- [`@glidemq/dashboard`](#glidemqdashboard)

---

## Job Logs

Append log lines from inside a processor using `job.log()`, then fetch them from any Queue instance.

```typescript
import { Queue, Worker } from 'glide-mq';

// Inside the processor
const worker = new Worker(
  'tasks',
  async (job) => {
    await job.log('Starting step 1');
    await doStep1();
    await job.log('Step 1 done, starting step 2');
    await doStep2();
    return { done: true };
  },
  { connection },
);

// Fetching logs externally
const queue = new Queue('tasks', { connection });
const { logs, count } = await queue.getJobLogs(jobId);
// logs: ['Starting step 1', 'Step 1 done, starting step 2']
// count: 2

// Paginate logs for long-running jobs
const { logs: page1 } = await queue.getJobLogs(jobId, 0, 49); // first 50
const { logs: page2 } = await queue.getJobLogs(jobId, 50, 99); // next 50
```

---

## Job Counts and Metrics

### `getJobCounts()`

Returns counts for every job state:

```typescript
const counts = await queue.getJobCounts();
// {
//   waiting:   12,
//   active:     3,
//   delayed:    5,
//   completed: 842,
//   failed:     7,
// }
```

### `getMetrics(type, opts?)`

Returns aggregate count plus per-minute time-series data for completed or failed jobs:

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

// Slice data points (e.g. last 10 data points):
const recent = await queue.getMetrics('completed', { start: -10 });
```

Data points are recorded server-side with zero extra RTTs. Minute-resolution buckets are retained for 24 hours and trimmed automatically.

### Disabling server-side telemetry

For maximum throughput, disable both events and metrics:

```typescript
const worker = new Worker('tasks', handler, {
  connection,
  events: false, // skip XADD event emission per job
  metrics: false, // skip HINCRBY metrics recording per job
});

const queue = new Queue('tasks', { connection, events: false });
```

TS-side `EventEmitter` and OpenTelemetry spans are unaffected by these options.

### `count()`

Returns the number of waiting jobs (stream length):

```typescript
const waitingCount = await queue.count();
```

---

## OpenTelemetry Integration

glide-mq emits OpenTelemetry spans automatically when `@opentelemetry/api` is installed. No code changes are required.

### Setup

```bash
npm install @opentelemetry/api
```

Initialise your tracer provider before creating any Queue or Worker instances (standard OTel setup), then use glide-mq normally:

```typescript
import { Queue } from 'glide-mq';

const queue = new Queue('tasks', { connection });
await queue.add('my-job', data);
// → creates span: glide-mq.queue.add
//     attributes: glide-mq.queue, glide-mq.job.name, glide-mq.job.id,
//                 glide-mq.job.delay, glide-mq.job.priority
```

### Custom tracer

If you need to use a specific tracer instance instead of the global one:

```typescript
import { setTracer, isTracingEnabled } from 'glide-mq';
import { trace } from '@opentelemetry/api';

const myTracer = trace.getTracer('my-service', '1.0.0');
setTracer(myTracer);

console.log('Tracing active:', isTracingEnabled());
```

### Instrumented operations

| Operation            | Span name            | Key attributes                                                                                          |
| -------------------- | -------------------- | ------------------------------------------------------------------------------------------------------- |
| `queue.add()`        | `glide-mq.queue.add` | `glide-mq.queue`, `glide-mq.job.name`, `glide-mq.job.id`, `glide-mq.job.delay`, `glide-mq.job.priority` |
| `flowProducer.add()` | `glide-mq.flow.add`  | `glide-mq.queue`, `glide-mq.flow.name`, `glide-mq.flow.childCount`                                      |

---


---

## AI Usage Telemetry

### reportUsage and span attributes

When job.reportUsage() is called inside a processor, the usage metadata is persisted to the job hash. Usage data is stored as hash fields on the job key:

| Hash field | Type | Description |
|------------|------|-------------|
| usage:model | string | Model identifier (e.g. gpt-4o) |
| usage:provider | string | Provider identifier (e.g. openai) |
| usage:inputTokens | string (int) | Input/prompt token count |
| usage:outputTokens | string (int) | Output/completion token count |
| usage:totalTokens | string (int) | Total tokens (auto-computed if not set) |
| usage:costUsd | string (float) | Cost in USD |
| usage:latencyMs | string (int) | Inference latency in ms |
| usage:cached | string | true if response was cached |

### Flow-level usage aggregation

queue.getFlowUsage(parentJobId) traverses the job tree and returns totals for inputTokens, outputTokens, costUsd, plus a models map (model name to job count) and jobCount.

### Budget exceeded events

When a flow budget is exceeded (via glidemq_recordUsageAndCheckBudget), the budget hash exceeded field is set to 1. Subsequent jobs in the flow are either failed or paused depending on the onExceeded setting.

Budget state can be queried via queue.getFlowBudget(flowId) which returns { maxTotalTokens, maxCostUsd, usedTokens, usedCost, exceeded, onExceeded }.

### Token-per-minute (TPM) metrics

When tokenLimiter is configured on a worker, token consumption is tracked either locally (in-memory), in Valkey (glide:{queueName}:tpm), or both (default). This enables dual-axis monitoring: RPM (jobs per window) and TPM (tokens per window).

---

## `@glidemq/dashboard`

The [`@glidemq/dashboard`](https://github.com/avifenesh/glidemq-dashboard) package provides a web UI for inspecting queues in real time.

### Quick start — built-in demo server

```bash
cd demo
npm install
npm run dashboard   # http://localhost:3000
```

### REST API

| Method   | Path                               | Description                             |
| -------- | ---------------------------------- | --------------------------------------- |
| `GET`    | `/api/queues`                      | List all queues with counts and metrics |
| `GET`    | `/api/queues/:name`                | Queue details + recent jobs             |
| `GET`    | `/api/queues/:name/jobs/:id`       | Single job details, state, and logs     |
| `POST`   | `/api/queues/:name/jobs`           | Add a new job                           |
| `POST`   | `/api/queues/:name/pause`          | Pause a queue                           |
| `POST`   | `/api/queues/:name/resume`         | Resume a queue                          |
| `POST`   | `/api/queues/:name/jobs/:id/retry` | Retry a failed job                      |
| `DELETE` | `/api/queues/:name/jobs/:id`       | Remove a job                            |
| `POST`   | `/api/queues/:name/drain`          | Drain all waiting jobs                  |
| `POST`   | `/api/queues/:name/obliterate`     | Obliterate queue and all data           |
| `GET`    | `/api/events`                      | SSE stream for real-time job events     |

### Real-time events via SSE

```javascript
const es = new EventSource('http://localhost:3000/api/events');
es.onmessage = ({ data }) => {
  const { queue, event, jobId } = JSON.parse(data);
  // event: 'added' | 'completed' | 'failed' | 'progress' | 'stalled' | 'heartbeat'
  console.log(`[${queue}] ${event} — job ${jobId}`);
};
```

### Embedding in an existing Express app

```typescript
import express from 'express';
import { Queue } from 'glide-mq';

const app = express();
app.use(express.json());

const queues: Record<string, Queue> = {
  orders: new Queue('orders', { connection }),
  payments: new Queue('payments', { connection }),
};

// Expose queue counts to the dashboard
app.get('/api/queues', async (_req, res) => {
  const data = await Promise.all(
    Object.entries(queues).map(async ([name, q]) => ({
      name,
      counts: await q.getJobCounts(),
      isPaused: await q.isPaused(),
    })),
  );
  res.json(data);
});
```
