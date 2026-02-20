# Observability

## Table of Contents

- [Job Logs](#job-logs)
- [Job Counts and Metrics](#job-counts-and-metrics)
- [OpenTelemetry Integration](#opentelemetry-integration)
- [`@glidemq/dashboard`](#glidemqdashboard)

---

## Job Logs

Append log lines from inside a processor using `job.log()`, then fetch them from any Queue instance.

```typescript
import { Worker } from 'glide-mq';

// Inside the processor
const worker = new Worker('tasks', async (job) => {
  await job.log('Starting step 1');
  await doStep1();
  await job.log('Step 1 done, starting step 2');
  await doStep2();
  return { done: true };
}, { connection });

// Fetching logs externally
const queue = new Queue('tasks', { connection });
const { logs, count } = await queue.getJobLogs(jobId);
// logs: ['Starting step 1', 'Step 1 done, starting step 2']
// count: 2

// Paginate logs for long-running jobs
const { logs: page1 } = await queue.getJobLogs(jobId, 0, 49);   // first 50
const { logs: page2 } = await queue.getJobLogs(jobId, 50, 99);  // next 50
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

### `getMetrics(type)`

Returns the aggregate count for completed or failed jobs:

```typescript
const completedMetrics = await queue.getMetrics('completed');
// { count: 842 }

const failedMetrics = await queue.getMetrics('failed');
// { count: 7 }
```

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

| Operation | Span name | Key attributes |
|-----------|-----------|----------------|
| `queue.add()` | `glide-mq.queue.add` | `glide-mq.queue`, `glide-mq.job.name`, `glide-mq.job.id`, `glide-mq.job.delay`, `glide-mq.job.priority` |
| `flowProducer.add()` | `glide-mq.flow.add` | `glide-mq.queue`, `glide-mq.flow.name`, `glide-mq.flow.childCount` |

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

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/queues` | List all queues with counts and metrics |
| `GET` | `/api/queues/:name` | Queue details + recent jobs |
| `GET` | `/api/queues/:name/jobs/:id` | Single job details, state, and logs |
| `POST` | `/api/queues/:name/jobs` | Add a new job |
| `POST` | `/api/queues/:name/pause` | Pause a queue |
| `POST` | `/api/queues/:name/resume` | Resume a queue |
| `POST` | `/api/queues/:name/jobs/:id/retry` | Retry a failed job |
| `DELETE` | `/api/queues/:name/jobs/:id` | Remove a job |
| `POST` | `/api/queues/:name/drain` | Drain all waiting jobs |
| `POST` | `/api/queues/:name/obliterate` | Obliterate queue and all data |
| `GET` | `/api/events` | SSE stream for real-time job events |

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
  orders:   new Queue('orders',   { connection }),
  payments: new Queue('payments', { connection }),
};

// Expose queue counts to the dashboard
app.get('/api/queues', async (_req, res) => {
  const data = await Promise.all(
    Object.entries(queues).map(async ([name, q]) => ({
      name,
      counts:   await q.getJobCounts(),
      isPaused: await q.isPaused(),
    })),
  );
  res.json(data);
});
```
