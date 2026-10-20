# glide-mq features not available in BullMQ

These features have no BullMQ equivalent. They are available after migrating to glide-mq.

---

## Per-key ordering

Guarantees sequential execution per key across all workers, regardless of worker concurrency. Jobs with the same `ordering.key` run one at a time in enqueue order. Jobs with different keys run in parallel.

```ts
await queue.add('sync', data, {
  ordering: { key: 'tenant-123' },
});
```

Replaces BullMQ Pro's `group.id` feature (which requires a Pro license).

### Group concurrency

Allow N parallel jobs per key instead of strict serialization:

```ts
await queue.add('sync', data, {
  ordering: { key: 'tenant-123', concurrency: 3 },
});
```

Jobs exceeding the limit are automatically parked in a per-group wait list and released when a slot opens.

### Per-group rate limiting

Cap throughput per ordering key:

```ts
await queue.add('sync', data, {
  ordering: {
    key: 'tenant-123',
    concurrency: 3,
    rateLimit: { max: 10, duration: 60_000 },
  },
});
```

Rate-limited jobs are promoted by the scheduler loop (latency up to `promotionInterval`, default 5 s).

### Cost-based token bucket

Assign a cost to each job and deduct from a refilling bucket per key:

```ts
await queue.add('heavy-job', data, {
  ordering: {
    key: 'tenant-123',
    tokenBucket: { capacity: 100, refillRate: 10 },
  },
  cost: 25, // this job consumes 25 tokens
});
```

---

## Global rate limiting

Queue-wide rate limit stored in Valkey, dynamically picked up by all workers:

```ts
await queue.setGlobalRateLimit({ max: 500, duration: 60_000 });

const limit = await queue.getGlobalRateLimit(); // { max, duration } or null
await queue.removeGlobalRateLimit();
```

When both global rate limit and `WorkerOptions.limiter` are set, the stricter limit wins.

---

## Dead letter queue

First-class DLQ support configured at the queue level:

```ts
const queue = new Queue('tasks', {
  connection,
  deadLetterQueue: {
    name: 'tasks-dlq',
    maxRetries: 3,
  },
});

// Retrieve DLQ jobs:
const dlqQueue = new Queue('tasks-dlq', { connection });
const dlqJobs = await dlqQueue.getDeadLetterJobs();
```

BullMQ has no native DLQ - failed jobs stay in the failed state.

---

## Job revocation

Cancel an in-flight job from outside the worker:

```ts
await queue.revoke(jobId);
```

The processor must cooperate via `job.abortSignal`:

```ts
const worker = new Worker(
  'q',
  async (job) => {
    for (const chunk of data) {
      if (job.abortSignal?.aborted) return;
      await processChunk(chunk);
    }
  },
  { connection },
);
```

---

## Transparent compression

Gzip compression of all job payloads, transparent to application code:

```ts
const queue = new Queue('tasks', {
  connection,
  compression: 'gzip',
});
// No changes needed in worker or job code
```

98% payload reduction on 15 KB JSON payloads (15 KB -> 331 bytes).

---

## AZ-affinity routing

Pin worker reads to replicas in your availability zone to reduce cross-AZ network cost:

```ts
const connection = {
  addresses: [{ host: 'cluster.cache.amazonaws.com', port: 6379 }],
  clusterMode: true,
  readFrom: 'AZAffinity',
  clientAz: 'us-east-1a',
};
```

---

## IAM authentication

Native AWS ElastiCache and MemoryDB IAM auth with automatic token refresh:

```ts
const connection = {
  addresses: [{ host: 'my-cluster.cache.amazonaws.com', port: 6379 }],
  useTLS: true,
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

---

## In-memory test mode

Test queue logic without a running Valkey/Redis instance:

```ts
import { TestQueue, TestWorker } from 'glide-mq/testing';

const queue = new TestQueue<{ email: string }, { sent: boolean }>('tasks');
const worker = new TestWorker(queue, async (job) => {
  return { sent: true };
});

await queue.add('send-email', { email: 'user@example.com' });
await new Promise((r) => setTimeout(r, 10));

const jobs = await queue.getJobs('completed');
```

BullMQ has no equivalent. Typically requires `ioredis-mock` or a real Redis instance.

---

## Broadcast / BroadcastWorker

Pub/sub fan-out where every connected `BroadcastWorker` receives every message. Supports per-subscriber retries for reliable delivery:

```ts
import { Broadcast, BroadcastWorker } from 'glide-mq';

const broadcast = new Broadcast('notifications', { connection });
const bw = new BroadcastWorker(
  'notifications',
  async (message) => {
    console.log('Received:', message);
  },
  { connection, subscription: 'my-group' },
);

await broadcast.publish('alerts', { type: 'alert', text: 'Server restarting' });
```

---

## Batch processing

Process multiple jobs in a single processor invocation:

```ts
const worker = new Worker(
  'q',
  async (jobs) => {
    // jobs is an array when batch mode is enabled
    const results = await bulkProcess(jobs.map((j) => j.data));
    return results; // per-job results array
  },
  {
    connection,
    batch: { size: 50, timeout: 1000 },
  },
);
```

---

## DAG workflows

Arbitrary directed acyclic graphs where a job can depend on multiple parents (BullMQ only supports trees - one parent per job):

```ts
import { FlowProducer, dag } from 'glide-mq';

// Option 1: dag() helper - standalone, creates its own FlowProducer
const jobs = await dag(
  [
    { name: 'fetch-a', queueName: 'tasks', data: { source: 'a' } },
    { name: 'fetch-b', queueName: 'tasks', data: { source: 'b' } },
    { name: 'aggregate', queueName: 'tasks', data: {}, deps: ['fetch-a', 'fetch-b'] },
  ],
  connection,
);

// Option 2: FlowProducer.addDAG() - when you manage the FlowProducer
const flow = new FlowProducer({ connection });
const jobs2 = await flow.addDAG({
  nodes: [
    { name: 'fetch-a', queueName: 'tasks', data: { source: 'a' } },
    { name: 'fetch-b', queueName: 'tasks', data: { source: 'b' } },
    { name: 'aggregate', queueName: 'tasks', data: {}, deps: ['fetch-a', 'fetch-b'] },
  ],
});
await flow.close();
```

---

## Workflow helpers

Higher-level orchestration built on FlowProducer:

```ts
import { chain, group, chord } from 'glide-mq';

const connection = { addresses: [{ host: 'localhost', port: 6379 }] };

// chain: sequential pipeline
await chain(
  'tasks',
  [
    { name: 'step-1', data: {} },
    { name: 'step-2', data: {} },
    { name: 'step-3', data: {} },
  ],
  connection,
);

// group: parallel fan-out, synthetic parent waits for all
await group(
  'tasks',
  [
    { name: 'shard-1', data: {} },
    { name: 'shard-2', data: {} },
  ],
  connection,
);

// chord: group then callback
await chord(
  'tasks',
  [
    { name: 'task-1', data: {} },
    { name: 'task-2', data: {} },
  ],
  { name: 'aggregate', data: {} },
  connection,
);
```

---

## Step jobs

Multi-step state machines using `job.moveToDelayed()` with an optional step token:

```ts
const worker = new Worker(
  'q',
  async (job) => {
    const step = job.data.__step ?? 'init';

    switch (step) {
      case 'init':
        await doInit(job.data);
        await job.moveToDelayed(Date.now(), 'process');
        return;
      case 'process':
        await doProcess(job.data);
        await job.moveToDelayed(Date.now(), 'finalize');
        return;
      case 'finalize':
        return doFinalize(job.data);
    }
  },
  { connection },
);
```

BullMQ's `moveToDelayed` has no step parameter.

---

## addAndWait (request-reply)

Synchronous RPC pattern - enqueue a job and wait for its result:

```ts
const result = await queue.addAndWait(
  'compute',
  { input: 42 },
  {
    waitTimeout: 30_000,
  },
);
console.log(result); // the job's return value
```

---

## Pluggable serializers

Use MessagePack, Protobuf, or any custom format instead of JSON:

```ts
import msgpack from 'msgpack-lite';

const queue = new Queue('tasks', {
  connection,
  serializer: {
    serialize: (data) => msgpack.encode(data),
    deserialize: (buffer) => msgpack.decode(buffer),
  },
});
```

---

## Job TTL

Auto-expire jobs after a given duration:

```ts
await queue.add('ephemeral', data, {
  ttl: 60_000, // job fails if not completed within 60 seconds
});
```

---

## repeatAfterComplete

Scheduler mode that enqueues the next job only after the previous one completes, guaranteeing no overlap:

```ts
await queue.upsertJobScheduler('sequential-poll', { repeatAfterComplete: 5000 }, { name: 'poll', data: {} });
```

---

## LIFO mode

Last-in-first-out retrieval - the newest entry already available in the same LIFO list is popped first:

```ts
await queue.add('urgent', data, { lifo: true });
```

Workers check the already-promoted non-LIFO priority list before LIFO and the stream. This is a retrieval preference, not a global start or completion guarantee; promotion timing, blocking reads, admission gates and other workers can change execution order. Delayed jobs have no independent precedence: promotion routes to LIFO if flagged, otherwise to the positive-priority list, otherwise to the stream. LIFO cannot be combined with ordering keys.

List-backed crash recovery is automatic, not a permanent slot leak. Healthy workers run `glidemq_reclaimStalledListJobs` to re-queue or fail stale LIFO/priority claims under `maxStalledCount` and release their `list-active` reservations. Claims are indexed in `list-active-ids`; `glidemq_healListActive` corrects drift every 10th promotion tick. Until the index is seeded, or when older-library claims make it incomplete, reclaim falls back to a SCAN capped at 1000 steps per call from cursor 0, so a very large keyspace can leave jobs beyond that bound unrecovered in the fallback. Healing uses resumable scans capped at 500 pages per call and may take several ticks. Neither reclaim path runs while paused. Do not routinely delete the counter, which includes live reservations. See [Worker crash behavior](../../../docs/DURABILITY.md#worker-crash-behavior).

---

## Job search

Search over job data fields:

```ts
const results = await queue.searchJobs({
  // search options
});
```

---

## excludeData

Lightweight job listings without payload data:

```ts
const jobs = await queue.getJobs('waiting', 0, 99, { excludeData: true });
// jobs[0].data is undefined - useful for dashboard listings of large-payload queues
```

---

## globalConcurrency on WorkerOptions

Set queue-wide concurrency cap at worker startup (shorthand for `queue.setGlobalConcurrency()`):

```ts
const worker = new Worker('q', processor, {
  connection,
  concurrency: 10,
  globalConcurrency: 50, // queue-wide cap across all workers
});
```

---

## Deduplication modes

Beyond BullMQ's simple deduplication, glide-mq adds explicit modes:

```ts
await queue.add('job', data, {
  deduplication: { id: 'my-dedup-key', mode: 'simple' }, // drop while the job exists (default)
});

// throttle: drop duplicates for `ttl` ms. `ttl` is accepted only here.
await queue.add('job', data, {
  deduplication: { id: 'my-dedup-key', mode: 'throttle', ttl: 60_000 },
});

// debounce: replace the job while it is still delayed (or prioritized); ttl does not apply.
await queue.add('job', data, {
  delay: 5_000,
  deduplication: { id: 'my-dedup-key', mode: 'debounce' },
});
```

---

## Backoff jitter

Spread retries under load with a jitter field:

```ts
await queue.add('job', data, {
  attempts: 5,
  backoff: { type: 'exponential', delay: 1000, jitter: 0.25 }, // +/- 25% random jitter
});
```

---

## AI-Native Primitives

The following features are purpose-built for LLM/AI orchestration pipelines. None of them exist in BullMQ.

### Usage Metadata (job.reportUsage)

Track model, tokens, cost, and latency per job. Persisted to the job hash and emitted as a `'usage'` event.

```ts
const worker = new Worker(
  'inference',
  async (job) => {
    const result = await callLLM(job.data);
    await job.reportUsage({
      model: 'gpt-5.4',
      provider: 'openai',
      tokens: { input: result.promptTokens, output: result.completionTokens },
      costs: { total: 0.003 },
      costUnit: 'usd',
      latencyMs: 800,
    });
    return result.content;
  },
  { connection },
);
```

### Token Streaming (job.stream / queue.readStream)

Stream LLM output tokens in real-time via per-job Valkey Streams.

```ts
// Worker side
const worker = new Worker(
  'chat',
  async (job) => {
    for await (const chunk of llmStream) {
      await job.stream({ token: chunk.text });
    }
    return { done: true };
  },
  { connection },
);

// Consumer side
const entries = await queue.readStream(jobId, { block: 5000 });
```

### Suspend / Resume (Human-in-the-Loop)

Pause a job to wait for external approval, then resume with signals.

```ts
// Suspend in processor
await job.suspend({ reason: 'Needs review', timeout: 86_400_000 });

// Resume externally
await queue.signal(jobId, 'approve', { reviewer: 'alice' });

// On resume, job.signals contains all received signals
```

### Budget Middleware (Flow-Level Caps)

Cap total tokens and/or cost across all jobs in a flow.

```ts
await flow.add(flowTree, {
  budget: { maxTotalTokens: 50_000, maxTotalCost: 0.5, costUnit: 'usd', onExceeded: 'fail' },
});

const budget = await queue.getFlowBudget(parentJobId);
```

### Fallback Chains

Ordered model/provider alternatives tried on retryable failure.

```ts
await queue.add(
  'inference',
  { prompt: '...' },
  {
    attempts: 4,
    fallbacks: [
      { model: 'gpt-5.4', provider: 'openai' },
      { model: 'claude-sonnet-4-20250514', provider: 'anthropic' },
      { model: 'llama-3-70b', provider: 'groq' },
    ],
  },
);

// Worker reads job.currentFallback for the active model/provider
```

### Dual-Axis Rate Limiting (RPM + TPM)

Rate-limit by both requests and tokens per minute for LLM API compliance.

```ts
const worker = new Worker('inference', processor, {
  connection,
  limiter: { max: 60, duration: 60_000 }, // RPM
  tokenLimiter: { maxTokens: 100_000, duration: 60_000 }, // TPM
});

// Report tokens in processor
await job.reportTokens(totalTokens);
```

### Flow Usage Aggregation

Aggregate AI usage across all jobs in a flow.

```ts
const usage = await queue.getFlowUsage(parentJobId);
// { tokens, totalTokens, costs, totalCost, costUnit, jobCount, models }
```

### Vector Search (Valkey Search)

Create search indexes and run KNN vector similarity queries over job hashes.

```ts
await queue.createJobIndex({
  vectorField: { name: 'embedding', dimensions: 1536 },
});

const job = await queue.add('document', { text: 'Hello world' });
if (job) {
  await job.storeVector('embedding', queryEmbedding);
}

const results = await queue.vectorSearch(queryEmbedding, {
  k: 10,
  filter: '@state:{completed}',
});
// results: { job, score }[]

await queue.dropJobIndex();
```

Requires `valkey-search` module on the server (standalone mode).
