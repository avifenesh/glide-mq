# New Features Available After Migration

Everything Bee-Queue cannot do that glide-mq provides out of the box.

## Priority Queues

Bee-Queue has no priority support. glide-mq uses numeric priority where lower = higher priority (0 is the highest, default).

```typescript
// High priority (processed first)
await queue.add('urgent-alert', data, { priority: 0 });

// Normal priority
await queue.add('report', data, { priority: 5 });

// Low priority (processed last)
await queue.add('cleanup', data, { priority: 20 });
```

Processing order: priority > LIFO > FIFO.

## Job Workflows (FlowProducer)

Parent-child job trees and DAG workflows. The parent waits for all children to complete.

```typescript
import { FlowProducer } from 'glide-mq';

const flow = new FlowProducer({ connection });
await flow.add({
  name: 'assemble-report',
  queueName: 'reports',
  data: { reportId: 42 },
  children: [
    { name: 'fetch-users', queueName: 'data', data: { source: 'users' } },
    { name: 'fetch-orders', queueName: 'data', data: { source: 'orders' } },
    { name: 'fetch-metrics', queueName: 'data', data: { source: 'metrics' } },
  ],
});
```

## Broadcast (Fan-Out)

Bee-Queue is point-to-point only. glide-mq supports fan-out where every subscriber receives every message.

```typescript
import { Broadcast, BroadcastWorker } from 'glide-mq';

const broadcast = new Broadcast('events', { connection, maxMessages: 1000 });

// Every subscriber gets the message
const inventory = new BroadcastWorker('events', async (job) => {
  await updateInventory(job.data);
}, { connection, subscription: 'inventory-service' });

const email = new BroadcastWorker('events', async (job) => {
  await sendNotification(job.data);
}, { connection, subscription: 'email-service' });

await broadcast.publish('orders', { event: 'order.placed', orderId: 42 });
```

## Batch Processing

Process multiple jobs in a single handler call for I/O-bound operations.

```typescript
import { Worker, BatchError } from 'glide-mq';

const worker = new Worker('bulk-insert', async (jobs) => {
  // jobs is Job[] when batch is enabled
  const results = await db.insertMany(jobs.map(j => j.data));
  return results; // must return R[] with length === jobs.length
}, {
  connection,
  batch: { size: 50, timeout: 1000 },
});
```

## Deduplication

Prevent duplicate job processing with three modes.

```typescript
// Simple - reject if job with same deduplication ID exists
await queue.add('task', data, {
  deduplication: { id: 'unique-key' },
});

// Throttle - reject duplicates within a time window
await queue.add('task', data, {
  deduplication: { id: 'user-123', ttl: 60000 },
});
```

## Schedulers (Cron and Interval)

Bee-Queue has no repeatable jobs. glide-mq supports cron patterns and fixed intervals.

```typescript
// Cron - run every day at midnight
await queue.upsertJobScheduler(
  'daily-report',
  { pattern: '0 0 * * *' },
  { name: 'daily-report', data: {} },
);

// Interval - run every 5 minutes
await queue.upsertJobScheduler(
  'health-check',
  { every: 300000 },
  { name: 'health-check', data: {} },
);
```

## Rate Limiting

Global and per-group rate limits on workers.

```typescript
const worker = new Worker('api-calls', processor, {
  connection,
  limiter: {
    max: 100,       // max 100 jobs
    duration: 60000, // per minute
  },
});
```

## Dead Letter Queue

Route permanently-failed jobs to a separate queue for inspection.

```typescript
const worker = new Worker('tasks', processor, {
  connection,
  deadLetterQueue: { name: 'failed-jobs' },
});
```

## LIFO Mode

Process newest jobs first instead of FIFO.

```typescript
await queue.add('urgent-report', data, { lifo: true });
```

## Job TTL

Automatically fail jobs that are not processed within a time window.

```typescript
await queue.add('time-sensitive', data, { ttl: 300000 }); // 5 min expiry
```

## Per-Key Ordering

Process jobs sequentially per ordering key while maintaining parallelism across keys.

```typescript
await queue.add('process-order', data, { ordering: { key: 'customer-123' } });
await queue.add('process-order', data, { ordering: { key: 'customer-456' } });
// Jobs for customer-123 run sequentially; customer-456 runs in parallel
```

## Request-Reply

Wait for a worker result in the producer without polling.

```typescript
const result = await queue.addAndWait('inference', { prompt: 'Hello' }, {
  waitTimeout: 30000,
});
console.log(result); // processor return value
```

## Step Jobs (Pause and Resume)

Pause a job and resume it later without completing.

```typescript
const worker = new Worker('drip-campaign', async (job) => {
  if (job.data.step === 'send') {
    await sendEmail(job.data);
    return job.moveToDelayed(Date.now() + 86400000, 'check');
  }
  if (job.data.step === 'check') {
    return await checkOpened(job.data) ? 'done' : job.moveToDelayed(Date.now() + 3600000, 'followup');
  }
  await sendFollowUp(job.data);
  return 'done';
}, { connection });
```

## UnrecoverableError

Skip all retries and fail permanently.

```typescript
import { UnrecoverableError } from 'glide-mq';

const worker = new Worker('tasks', async (job) => {
  if (!job.data.requiredField) {
    throw new UnrecoverableError('missing required field');
  }
  return processJob(job);
}, { connection });
```

## Serverless Producer

Lightweight producer with no EventEmitter overhead for Lambda/Edge.

```typescript
import { Producer } from 'glide-mq';

export async function handler(event) {
  const producer = new Producer('queue', { connection });
  await producer.add('process', event.body);
  await producer.close();
  return { statusCode: 200 };
}
```

## Testing Without Valkey

In-memory queue and worker for unit tests.

```typescript
import { TestQueue, TestWorker } from 'glide-mq/testing';

const queue = new TestQueue('tasks');
await queue.add('test-job', { key: 'value' });
const worker = new TestWorker(queue, async (job) => {
  return { processed: true };
});
await worker.run();
```

## Cluster Support

Native Valkey/Redis Cluster with hash-tagged keys.

```typescript
const connection = {
  addresses: [
    { host: 'node1', port: 7000 },
    { host: 'node2', port: 7001 },
  ],
  clusterMode: true,
  readFrom: 'AZAffinity',
  clientAz: 'us-east-1a',
};
```

## TLS and IAM Authentication

```typescript
// TLS
const connection = {
  addresses: [{ host: 'redis.example.com', port: 6380 }],
  useTLS: true,
};

// AWS IAM
const connection = {
  addresses: [{ host: 'cluster.cache.amazonaws.com', port: 6379 }],
  clusterMode: true,
  credentials: {
    type: 'iam',
    serviceType: 'elasticache',
    region: 'us-east-1',
    userId: 'my-iam-user',
    clusterName: 'my-cluster',
  },
};
```

## QueueEvents (Real-Time Stream)

Centralized job lifecycle events via Valkey Streams - replaces Bee-Queue's PubSub model.

```typescript
import { QueueEvents } from 'glide-mq';

const events = new QueueEvents('tasks', { connection });
events.on('added', ({ jobId }) => console.log('added', jobId));
events.on('completed', ({ jobId, returnvalue }) => console.log('done', jobId));
events.on('failed', ({ jobId, failedReason }) => console.log('failed', jobId));
events.on('progress', ({ jobId, data }) => console.log('progress', jobId, data));
events.on('stalled', ({ jobId }) => console.log('stalled', jobId));
```

## Time-Series Metrics

Per-minute throughput and latency data with zero extra round trips.

```typescript
const metrics = await queue.getMetrics('completed');
// { count, data: [{ timestamp, count, avgDuration }], meta: { resolution: 'minute' } }
```

## Queue Management

```typescript
// Pause/resume all workers
await queue.pause();
await queue.resume();

// Drain waiting jobs
await queue.drain();

// Clean old completed/failed jobs
await queue.clean(3600000, 1000, 'completed'); // older than 1 hour

// Obliterate all queue data
await queue.obliterate({ force: true });
```

## Dashboard

Web UI for monitoring and managing queues.

```typescript
import { createDashboard } from '@glidemq/dashboard';
import express from 'express';

const app = express();
app.use('/dashboard', createDashboard([queue]));
```

## Framework Integrations

Native integrations for Hono, Fastify, NestJS, and Hapi.

## OpenTelemetry

Automatic span emission for distributed tracing.

## Pluggable Serializers

Custom serialization for job data (e.g., MessagePack, Protocol Buffers).

```typescript
const queue = new Queue('tasks', { connection, serializer: customSerializer });
const worker = new Worker('tasks', processor, { connection, serializer: customSerializer });
```

## AI-Native Primitives

glide-mq is purpose-built for LLM/AI orchestration. None of these exist in Bee-Queue.

### Usage Metadata

Track model, tokens, cost, and latency per job.

```typescript
await job.reportUsage({
  model: 'gpt-5.4',
  provider: 'openai',
  tokens: { input: 500, output: 200 },
  costs: { total: 0.003 },
  costUnit: 'usd',
  latencyMs: 800,
});
```

### Token Streaming

Stream LLM output tokens in real-time via per-job Valkey Streams.

```typescript
// Worker: emit chunks
await job.stream({ token: 'Hello' });

// Consumer: read chunks (supports long-polling)
const entries = await queue.readStream(jobId, { block: 5000 });
```

### Suspend / Resume (Human-in-the-Loop)

Pause a job for external approval, resume with signals.

```typescript
await job.suspend({ reason: 'Needs review', timeout: 86_400_000 });
// Externally:
await queue.signal(jobId, 'approve', { reviewer: 'alice' });
```

### Flow Budget

Cap total tokens/cost across all jobs in a workflow flow.

```typescript
await flow.add(flowTree, {
  budget: { maxTotalTokens: 50_000, maxTotalCost: 0.50, costUnit: 'usd' },
});
```

### Fallback Chains

Ordered model/provider alternatives on retryable failure.

```typescript
await queue.add('inference', data, {
  attempts: 4,
  fallbacks: [
    { model: 'gpt-5.4', provider: 'openai' },
    { model: 'claude-sonnet-4-20250514', provider: 'anthropic' },
    { model: 'llama-3-70b', provider: 'groq' },
  ],
});
```

### Dual-Axis Rate Limiting (RPM + TPM)

Rate-limit by both requests and tokens per minute for LLM API compliance.

```typescript
const worker = new Worker('inference', processor, {
  connection,
  limiter: { max: 60, duration: 60_000 },
  tokenLimiter: { maxTokens: 100_000, duration: 60_000 },
});
```

### Flow Usage Aggregation

Aggregate AI usage across all jobs in a flow.

```typescript
const usage = await queue.getFlowUsage(parentJobId);
// { tokens, totalTokens, costs, totalCost, costUnit, jobCount, models }
```

### Vector Search

KNN similarity search over job hashes via Valkey Search.

```typescript
await queue.createJobIndex({
  vectorField: { name: 'embedding', dimensions: 1536 },
});
const job = await queue.add('document', { text: 'Hello world' });
if (job) {
  await job.storeVector('embedding', queryEmbedding);
}
const results = await queue.vectorSearch(queryEmbedding, { k: 10 });
```
