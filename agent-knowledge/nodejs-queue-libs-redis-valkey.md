# Learning Guide: Node.js Queue Libraries with Redis/Valkey Backend

**Generated**: 2026-02-13
**Sources**: 42 resources analyzed
**Depth**: deep

## Prerequisites

- Working knowledge of Node.js and asynchronous programming (Promises, async/await)
- Basic understanding of Redis data structures (strings, lists, hashes, sorted sets, streams)
- Familiarity with distributed systems concepts (at-least-once delivery, idempotency)
- A running Redis 6.2+ or Valkey 7.2+ instance for hands-on experimentation

## TL;DR

- **BullMQ** is the modern, actively developed standard for Node.js job queues -- TypeScript-native, feature-rich (flows, priorities, rate limiting, deduplication, sandboxed processors), and built on Redis sorted sets, lists, hashes, streams, and Lua scripts for atomicity.
- **Bull** is BullMQ's predecessor, now in maintenance mode. It shares the same Redis-backed architecture but lacks flows, global concurrency, deduplication, and other BullMQ v5 features.
- **Bee-Queue** is a minimalist, high-performance alternative optimized for short-lived real-time jobs. It trades breadth (no priorities, no repeatable jobs) for raw throughput and simplicity (~1000 lines of code).
- **All three** use Redis Lua scripting for atomic state transitions, BRPOPLPUSH (or BZPOPMIN in BullMQ v5) for blocking job fetch, and sorted sets for delayed/scheduled jobs.
- **Valkey** (the open-source Redis fork) is compatible with all these libraries; BullMQ explicitly detects and supports it.

---

## Table of Contents

1. [Library Overview and History](#1-library-overview-and-history)
2. [Architecture and Core Concepts](#2-architecture-and-core-concepts)
3. [Redis/Valkey Data Structures Used Internally](#3-redisvalkey-data-structures-used-internally)
4. [Feature Comparison Matrix](#4-feature-comparison-matrix)
5. [Job Lifecycle and State Machines](#5-job-lifecycle-and-state-machines)
6. [Deep Dive: Key Features](#6-deep-dive-key-features)
7. [Reliability and Delivery Guarantees](#7-reliability-and-delivery-guarantees)
8. [Distributed Locking Patterns](#8-distributed-locking-patterns)
9. [Event Systems](#9-event-systems)
10. [Performance Characteristics and Trade-offs](#10-performance-characteristics-and-trade-offs)
11. [Valkey Compatibility](#11-valkey-compatibility)
12. [Production Deployment](#12-production-deployment)
13. [Monitoring and Tooling](#13-monitoring-and-tooling)
14. [Other Notable Libraries](#14-other-notable-libraries)
15. [Code Examples](#15-code-examples)
16. [Common Pitfalls](#16-common-pitfalls)
17. [Best Practices](#17-best-practices)
18. [Further Reading](#18-further-reading)

---

## 1. Library Overview and History

### BullMQ

BullMQ is a complete rewrite of Bull in TypeScript, maintained by OptimalBits (the same team behind Bull). It is the recommended library for new projects.

- **Language**: TypeScript (first-class type definitions)
- **Redis requirement**: >= 5.0.0 (minimum), >= 6.2.0 (recommended)
- **Architecture**: Separates concerns into Queue, Worker, QueueEvents, and FlowProducer classes
- **Status**: Actively developed, current major version is v5
- **License**: MIT

### Bull

Bull is the predecessor to BullMQ and has been the most popular Node.js queue library for years. It is now in **maintenance mode** (bug fixes only).

- **Language**: JavaScript
- **Redis requirement**: >= 2.8.18
- **Architecture**: Single Queue class handles both producing and consuming
- **Status**: Maintenance mode -- new features go to BullMQ
- **License**: MIT

### Bee-Queue

Bee-Queue is a lightweight, performance-focused alternative. Its design philosophy is "simple, fast, robust" and it prioritizes short real-time jobs over long-running background tasks.

- **Language**: JavaScript
- **Redis requirement**: >= 2.6 (for Lua scripting)
- **Architecture**: Single Queue class with a minimal API surface
- **Status**: Community-maintained, lower activity than BullMQ
- **License**: MIT

### Lineage

```
Kue (2013, deprecated) -> Bull (2014) -> BullMQ (2019)
                                         Bee-Queue (2015, independent)
```

Kue was an early Node.js Redis queue with a built-in web UI. It is no longer maintained. Bull replaced it with better atomicity and reliability. BullMQ modernized Bull with TypeScript and a cleaner architecture.

---

## 2. Architecture and Core Concepts

### BullMQ's Four-Class Architecture

BullMQ cleanly separates responsibilities:

| Class | Purpose | Redis Connections |
|-------|---------|-------------------|
| **Queue** | Add jobs, query state, manage queue | 1 standard connection |
| **Worker** | Process jobs from queue | 1 blocking connection |
| **QueueEvents** | Listen to job lifecycle events | 1 blocking connection (Redis Streams) |
| **FlowProducer** | Create parent-child job trees atomically | 1 standard connection |

This separation means producers and consumers can run in entirely different processes or machines, sharing only the Redis connection details.

### Bull's Single-Class Architecture

Bull combines producing and consuming in a single `Queue` class. Each queue instance creates up to **three Redis connections**:

1. **client** -- standard commands (can be shared)
2. **subscriber** -- Pub/Sub for events (can be shared)
3. **bclient** -- blocking BRPOPLPUSH for job fetching (must be unique per queue)

### Bee-Queue's Minimal Architecture

Bee-Queue also uses a single `Queue` class with three connection types:

1. **client** -- standard Redis operations
2. **bclient** -- blocking BRPOPLPUSH for workers
3. **eclient** -- Pub/Sub subscriber for events

### Key Naming Convention

All three libraries namespace their Redis keys:

| Library | Pattern | Example |
|---------|---------|---------|
| BullMQ | `{prefix}:{queueName}:{keyType}` | `bull:emails:wait` |
| Bull | `{prefix}:{queueName}:{keyType}` | `bull:emails:active` |
| Bee-Queue | `{prefix}:{queueName}:{keyType}` | `bq:emails:waiting` |

BullMQ's default prefix is `bull`, Bee-Queue's is `bq`.

---

## 3. Redis/Valkey Data Structures Used Internally

### Overview of Data Structure Usage

| Data Structure | BullMQ Usage | Bull Usage | Bee-Queue Usage |
|----------------|-------------|------------|-----------------|
| **Lists** | wait, active, paused queues | wait, active, paused | waiting, active |
| **Sorted Sets** | delayed, prioritized, completed, failed, stalled | delayed, priority, completed, failed, stalled | delayed |
| **Hashes** | Job data storage, queue metadata | Job data storage | Jobs hash (all jobs in one hash) |
| **Streams** | Event system (QueueEvents) | Not used | Not used |
| **Pub/Sub** | Not primary (replaced by Streams) | Job events across processes | Job events across processes |
| **Strings/Keys** | Job ID counter, locks, rate limiter, deduplication, markers | Job ID counter, locks, rate limiter | Job ID counter, stall block |

### Redis Lists for FIFO/LIFO Queues

Lists are the backbone of job queuing. Jobs are added with LPUSH (FIFO) or RPUSH (LIFO) and fetched atomically:

```
# FIFO: Add to head, consume from tail
LPUSH bull:myqueue:wait jobId
BRPOPLPUSH bull:myqueue:wait bull:myqueue:active 0

# LIFO: Add to tail (same side as consumption)
RPUSH bull:myqueue:wait jobId
BRPOPLPUSH bull:myqueue:wait bull:myqueue:active 0
```

The `BRPOPLPUSH` (or `BLMOVE` in Redis 6.2+) command is critical: it **atomically** pops a job from `wait` and pushes it to `active`, ensuring no job is lost even if the consumer crashes between operations.

BullMQ v5 has shifted to using `BZPOPMIN` on a marker-based system for more efficient delayed job integration.

### Sorted Sets for Delayed Jobs and Priorities

Sorted sets store members with scores, enabling efficient range queries:

```
# Delayed jobs: score = execution timestamp
ZADD bull:myqueue:delayed 1707840000000 jobId

# Promote ready jobs (score <= current time)
ZRANGEBYSCORE bull:myqueue:delayed 0 <currentTimestamp> LIMIT 0 1000

# Priority queue: score = priority value (lower = higher priority)
ZADD bull:myqueue:prioritized 5 jobId
```

The O(log N) insertion complexity means priority queues are slightly slower than standard FIFO lists (O(1)), which is why BullMQ documents that "adding prioritized jobs is a slower operation."

Bull encodes both timestamp and job ID into the score for delayed jobs: `timestamp * 0x1000 + (jobId & 0xfff)`, allowing unique ordering even when timestamps collide.

### Hashes for Job Data

Each job's data, options, and metadata are stored in a Redis hash:

```
# BullMQ: Each job gets its own hash key
HSET bull:myqueue:jobId \
  data '{"email":"user@example.com"}' \
  opts '{"attempts":3,"delay":0}' \
  timestamp 1707840000000 \
  returnvalue '...' \
  stacktrace '...'

# Bee-Queue: All jobs stored in a single hash
HSET bq:myqueue:jobs jobId '{"data":...,"options":...}'
```

### Streams for Events (BullMQ only)

BullMQ uses Redis Streams (introduced in Redis 5.0) for its event system:

```
# Emit a 'completed' event
XADD bull:myqueue:events * event completed jobId 123

# Auto-trim to ~10,000 events
XTRIM bull:myqueue:events MAXLEN ~ 10000
```

Streams provide **guaranteed delivery** even during disconnections, unlike Pub/Sub which drops messages when clients are offline.

### Lua Scripts for Atomicity

All three libraries use Lua scripts executed via `EVAL`/`EVALSHA` to perform complex multi-key operations atomically. This is essential because Redis (and Valkey) execute Lua scripts as a single atomic operation -- no other command can interleave.

BullMQ has **53 Lua scripts** covering every state transition. Key scripts include:

| Script | Keys Involved | Purpose |
|--------|--------------|---------|
| `addStandardJob-9.lua` | 9 keys | Add job to wait list, increment ID counter, store hash |
| `moveToActive-11.lua` | 11 keys | Pop from wait, push to active, set lock, check rate limit |
| `moveToFinished-14.lua` | 14 keys | Verify lock, remove from active, add to completed/failed, fetch next |
| `moveStalledJobsToWait-9.lua` | 9 keys | Scan active jobs, check lock expiry, move stalled back to wait |
| `promoteDelayedJobs.lua` | Included | ZRANGEBYSCORE on delayed set, LPUSH to wait or ZADD to prioritized |

Bee-Queue has 5 Lua scripts: `addJob`, `addDelayedJob`, `checkStalledJobs`, `raiseDelayedJobs`, and `removeJob`. Scripts are loaded via `SCRIPT LOAD` and executed via `EVALSHA` for efficiency.

---

## 4. Feature Comparison Matrix

| Feature | BullMQ | Bull | Bee-Queue |
|---------|--------|------|-----------|
| **FIFO/LIFO ordering** | Yes | Yes | FIFO only |
| **Job priorities** | Yes (1 to 2^21) | Yes (1 to MAX_INT) | No |
| **Delayed jobs** | Yes | Yes | Yes (`delayUntil()`) |
| **Repeatable/cron jobs** | Yes (Job Schedulers in v5) | Yes (cron + every) | No |
| **Rate limiting** | Yes (global + manual) | Yes (per-queue) | No |
| **Global concurrency** | Yes (v5.9+) | No | No |
| **Per-worker concurrency** | Yes | Yes | Yes |
| **Retries with backoff** | Yes (fixed, exponential, custom + jitter) | Yes (fixed, exponential, custom) | Yes (fixed, exponential) |
| **Job timeout** | Yes | Yes | Yes |
| **Dead letter queue** | Via maxStalledCount config | Via maxStalledCount | Via maxRetries |
| **Parent-child flows** | Yes (FlowProducer) | No | No |
| **Bulk operations** | Yes (atomic) | Yes (addBulk) | Yes (saveAll, pipelined) |
| **Sandboxed processors** | Yes (spawn + worker threads) | Yes (separate process) | No |
| **Pause/resume** | Yes (global + local) | Yes (global + local) | Yes |
| **Job progress tracking** | Yes | Yes | Yes |
| **Deduplication** | Yes (simple, throttle, debounce) | Via custom jobId | Via custom jobId |
| **Event system** | Redis Streams (guaranteed) | Redis Pub/Sub | Redis Pub/Sub |
| **Metrics collection** | Yes (per-minute stats) | Yes (v4.3+) | No |
| **OpenTelemetry** | Yes (v5.67+) | No | No |
| **Redis Cluster** | Yes (hash tags required) | Yes (hash tags required) | Not officially |
| **TypeScript** | Native | @types/bull | @types/bee-queue |
| **NestJS integration** | @nestjs/bullmq | @nestjs/bull | No official |
| **Valkey support** | Yes (detected, CI-tested) | Likely (same Redis commands) | Likely (same Redis commands) |

---

## 5. Job Lifecycle and State Machines

### BullMQ Job States

```
                    +-------------+
                    |   delayed   |  (sorted set, score = timestamp)
                    +------+------+
                           |
                           v (timer/promotion)
+-------+    +---------+  |  +-------------+
| added | -> |  wait   | <+  | prioritized |  (sorted set, score = priority)
+-------+    +----+----+     +------+------+
                  |                 |
                  +--------+--------+
                           |
                           v (worker picks up)
                    +------+------+
                    |   active    |  (list, locked with token)
                    +------+------+
                           |
              +------------+------------+
              |            |            |
              v            v            v
        +---------+  +---------+  +------------------+
        |completed|  |  failed |  |waiting-children  |
        +---------+  +---------+  +------------------+
                           |            |
                           v            v (all children done)
                     (retry?) -> wait   -> wait
```

**Key states:**

| State | Redis Structure | Description |
|-------|----------------|-------------|
| wait | List | Job is queued for immediate processing |
| delayed | Sorted Set | Job waiting for its delay to expire |
| prioritized | Sorted Set | Job with explicit priority (score = priority value) |
| active | List | Job is being processed by a worker (locked) |
| waiting-children | Set/Hash | Parent job waiting for all child jobs to complete |
| completed | Sorted Set | Job finished successfully |
| failed | Sorted Set | Job failed all retry attempts |

### Bull Job States

Bull uses the same fundamental states minus `prioritized` (priority is embedded differently) and `waiting-children` (no flow support).

### Bee-Queue Job States

Bee-Queue has a simpler model: `waiting` -> `active` -> `succeeded`/`failed` with `delayed` as an intermediate holding state.

---

## 6. Deep Dive: Key Features

### 6.1 Rate Limiting

**BullMQ** implements rate limiting at the worker level:

```typescript
const worker = new Worker('paintQueue', paintProcessor, {
  limiter: {
    max: 10,       // Maximum 10 jobs
    duration: 1000  // per 1000 milliseconds
  }
});
```

The rate limiter is **global across all workers** for a queue. Even with 10 workers configured to process 10 jobs/second, the total throughput is still 10 jobs/second.

**Manual rate limiting** allows workers to dynamically throttle based on external signals:

```typescript
const worker = new Worker('apiQueue', async (job) => {
  const response = await callExternalAPI(job.data);
  if (response.status === 429) {
    const retryAfter = response.headers['retry-after'] * 1000;
    await worker.rateLimit(retryAfter);
    throw Worker.RateLimitError();
    // Job returns to wait without counting as a failure
  }
  return response.data;
});
```

**Bull** has a similar per-queue rate limiter configured in queue options:

```javascript
const queue = new Queue('apiQueue', {
  limiter: { max: 100, duration: 60000 } // 100 per minute
});
```

**Bee-Queue** does not have built-in rate limiting.

### 6.2 Job Scheduling and Repeatable Jobs

**BullMQ v5** introduced **Job Schedulers** which act as factories producing delayed jobs on a schedule:

```typescript
await queue.upsertJobScheduler('daily-report', {
  pattern: '0 15 3 * * *',  // 3:15 AM daily (cron)
}, {
  name: 'generate-report',
  data: { type: 'daily' },
  opts: { attempts: 3 }
});

// Or fixed interval
await queue.upsertJobScheduler('health-check', {
  every: 30000  // every 30 seconds
});
```

Internally, a Job Scheduler creates a single delayed job. When that job begins processing, the next delayed job is automatically created. This means actual frequency depends on worker availability.

**Bull** uses `add()` with a `repeat` option:

```javascript
queue.add('report', { type: 'daily' }, {
  repeat: { cron: '15 3 * * *' }
});
```

**Bee-Queue** has no repeatable job support.

### 6.3 Job Priorities

**BullMQ** supports priorities from 1 (highest) to 2,097,152 (lowest). Jobs without a priority are processed before all prioritized jobs.

```typescript
await queue.add('email', { to: 'vip@example.com' }, { priority: 1 });
await queue.add('email', { to: 'user@example.com' }, { priority: 10 });
```

Internally, prioritized jobs are stored in a dedicated sorted set (`bull:queueName:prioritized`) with the priority as the score. Insertion is O(log N) compared to O(1) for standard FIFO.

Jobs with the same priority are processed in FIFO order within that priority tier.

**Bull** uses a similar sorted set approach with its `priority` key.

**Bee-Queue** deliberately omits priorities to maintain simplicity and performance.

### 6.4 Retries and Backoff Strategies

All three libraries support automatic retries on failure:

**BullMQ:**
```typescript
await queue.add('process', data, {
  attempts: 5,
  backoff: {
    type: 'exponential',  // or 'fixed' or custom name
    delay: 3000,          // base delay in ms
    jitter: 0.5           // randomize 0-50% of calculated delay
  }
});
```

Exponential backoff formula: `2^(attempt - 1) * delay` ms. With delay=3000 and attempt 7, that is ~192 seconds.

Custom backoff strategies are defined in the Worker:

```typescript
const worker = new Worker('myQueue', processor, {
  settings: {
    backoffStrategy: (attemptsMade, type, err, job) => {
      if (type === 'custom-linear') return attemptsMade * 1000;
      return -1; // returning -1 stops retrying
    }
  }
});
```

**Bull** supports the same strategies (fixed, exponential, custom).

**Bee-Queue** supports retries with `.retries(n)` and `.backoff('exponential', delay)` on the job builder.

### 6.5 Parent-Child Flows (BullMQ only)

BullMQ's FlowProducer creates hierarchical job dependencies atomically:

```typescript
const flowProducer = new FlowProducer();

const flow = await flowProducer.add({
  name: 'assemble-report',
  queueName: 'reports',
  data: { reportId: 42 },
  children: [
    { name: 'fetch-data', queueName: 'data', data: { source: 'db' } },
    { name: 'fetch-data', queueName: 'data', data: { source: 'api' } },
    { name: 'generate-charts', queueName: 'charts', data: { style: 'bar' } }
  ]
});
```

The parent job enters `waiting-children` state until all children complete. Children can be in different queues processed by different workers. Parent jobs can access child results via `job.getChildrenValues()`.

Removal cascades: removing a parent removes all children; removing a child updates parent dependency tracking.

### 6.6 Concurrency

**Per-worker concurrency** (all three libraries):

```typescript
// BullMQ
const worker = new Worker('queue', processor, { concurrency: 50 });

// Bull
queue.process(50, processor);

// Bee-Queue
queue.process(50, handler);
```

For IO-heavy workloads (HTTP calls, database queries), concurrency of 100-300 per worker is typical. For CPU-intensive work, keep concurrency low and scale horizontally.

**Global concurrency** (BullMQ only):

```typescript
await queue.setGlobalConcurrency(4);
// No more than 4 jobs active across ALL workers
```

This acts as an upper bound that individual worker concurrency cannot exceed.

### 6.7 Deduplication (BullMQ v5)

Three modes:

```typescript
// Simple: deduplicate until job completes
await queue.add('sync', data, {
  deduplication: { id: 'user-123-sync' }
});

// Throttle: deduplicate within a time window
await queue.add('sync', data, {
  deduplication: { id: 'user-123-sync', ttl: 60000 }
});

// Debounce: replace existing delayed job with fresh data
await queue.add('sync', data, {
  deduplication: { id: 'user-123-sync', ttl: 60000, mode: 'debounce' }
});
```

This is distinct from using custom `jobId` values (which simply ignore duplicate additions).

### 6.8 Sandboxed Processors

BullMQ supports running processors in isolated contexts to prevent CPU-heavy work from blocking the event loop (which would cause stalled job detection failures):

```typescript
// processor.ts (separate file)
export default async function (job: SandboxedJob) {
  // CPU-intensive work here
  return heavyComputation(job.data);
}

// main.ts
const worker = new Worker('heavy', './processor.ts', {
  useWorkerThreads: true  // or false for child_process (default)
});
```

Two execution models:
- **Child Process** (default): `child_process.spawn()`, more isolated but heavier
- **Worker Threads** (v3.13+): lighter weight, shares memory with main process

---

## 7. Reliability and Delivery Guarantees

### At-Least-Once Delivery

All three libraries guarantee **at-least-once delivery**. A job may be processed more than once if:

1. A worker crashes after starting a job but before reporting completion
2. The stalled job detector moves the job back to wait
3. Another worker picks it up and processes it again

BullMQ's documentation states: "the system attempts to deliver every message exactly one time, but it will deliver at least once in the worst case scenario."

### How Stalled Job Detection Works

The stalled job mechanism is the foundation of reliability:

**BullMQ:**
1. When a worker takes a job, it acquires a **lock** (a Redis key with an expiration/TTL)
2. The worker periodically extends the lock (default: every lockDuration/2 ms)
3. A stalled checker runs periodically (default: every 30 seconds)
4. If a job is in the `active` list but its lock has expired, it is **stalled**
5. Stalled jobs are moved back to `wait` (up to `maxStalledCount` times, default: 1)
6. If maxStalledCount is exceeded, the job moves to `failed`

**Bee-Queue:**
1. Workers periodically call "phone home" to remove their job IDs from the `stalling` set
2. `checkStalledJobs()` runs on a timer
3. Any job ID still in the `stalling` set is considered stalled
4. Stalled jobs are moved to the front of the waiting list via LPUSH (not RPUSH), so a repeatedly stalling job does not starve other jobs

### Making Processing Idempotent

Since at-least-once means possible duplicates, processors should be idempotent:

```typescript
const worker = new Worker('payments', async (job) => {
  // Use job.id as idempotency key
  const existing = await db.findPayment({ idempotencyKey: job.id });
  if (existing) return existing.result;

  const result = await processPayment(job.data);
  await db.savePayment({ idempotencyKey: job.id, result });
  return result;
});
```

### Redis Persistence and Queue Durability

Queue durability depends on Redis persistence configuration:

- **AOF (Append Only File)**: Recommended. Logs every write operation. `appendfsync everysec` provides a good balance of durability and performance.
- **RDB (Snapshots)**: Not sufficient alone for queue data. Minutes of data could be lost between snapshots.
- **No persistence**: Queue data is entirely in-memory and lost on restart.
- **`maxmemory-policy` MUST be `noeviction`**: If Redis evicts keys, queue state becomes corrupted silently.

---

## 8. Distributed Locking Patterns

### BullMQ's Token-Based Locking

When a worker picks up a job, the `moveToActive` Lua script atomically:

1. Pops the job from the wait list
2. Pushes it to the active list
3. Sets a lock key: `bull:queueName:jobId:lock` with the worker's unique token
4. Sets an expiration (TTL) on the lock key equal to `lockDuration`

```
SET bull:myqueue:42:lock <workerToken> PX 30000 NX
```

The worker must periodically extend the lock:

```
PEXPIRE bull:myqueue:42:lock 30000
```

If the lock expires (worker crashed or event loop blocked), the stalled checker reclaims the job.

When completing a job, the `moveToFinished` Lua script verifies the lock token matches before allowing the state transition. This prevents a recovered worker from overwriting a re-processed job's result.

### Bull's Lock Mechanism

Bull uses a similar approach with configurable `lockDuration` (default: 30s) and `lockRenewTime` (default: lockDuration/2 = 15s). The lock is renewed automatically on a timer.

### Bee-Queue's Stalling Set Approach

Instead of per-job locks, Bee-Queue uses a set-based heartbeat:

1. All active job IDs are copied to the `stalling` set
2. Active workers remove their job IDs from `stalling` every `stallInterval`
3. Any remaining IDs in `stalling` at the next check are considered stalled

This is simpler but less granular than per-job lock tokens.

---

## 9. Event Systems

### BullMQ: Redis Streams

BullMQ uses Redis Streams for reliable event delivery:

```typescript
const queueEvents = new QueueEvents('myQueue');

queueEvents.on('completed', ({ jobId, returnvalue }) => {
  console.log(`Job ${jobId} completed with: ${returnvalue}`);
});

queueEvents.on('failed', ({ jobId, failedReason }) => {
  console.log(`Job ${jobId} failed: ${failedReason}`);
});

queueEvents.on('progress', ({ jobId, data }) => {
  console.log(`Job ${jobId} progress: ${data}`);
});
```

**Why Streams over Pub/Sub:**
- Streams guarantee delivery even during disconnections (events persist)
- Streams support auto-trimming (default ~10,000 events) to bound memory
- Pub/Sub drops messages when clients are disconnected

Worker-level events are local (emitted on the Worker instance only). QueueEvents aggregates events from all workers processing a given queue.

### Bull: Redis Pub/Sub

Bull uses Redis Pub/Sub for cross-process events. Events are fire-and-forget: if no subscriber is listening, the event is lost.

```javascript
queue.on('completed', (job, result) => { /* ... */ });
queue.on('failed', (job, err) => { /* ... */ });
queue.on('progress', (job, progress) => { /* ... */ });

// Global events (from any worker)
queue.on('global:completed', (jobId, result) => { /* ... */ });
```

### Bee-Queue: Redis Pub/Sub

Bee-Queue also uses Pub/Sub with both queue-level and job-level events:

```javascript
queue.on('succeeded', (job, result) => { /* ... */ });
queue.on('failed', (job, err) => { /* ... */ });

// Per-job events (cross-process via Pub/Sub)
const job = queue.createJob(data).save();
job.on('succeeded', (result) => { /* ... */ });
job.on('progress', (progress) => { /* ... */ });
```

---

## 10. Performance Characteristics and Trade-offs

### Throughput Factors

| Factor | Impact |
|--------|--------|
| **Lua script complexity** | BullMQ's 14-key moveToFinished is heavier than Bee-Queue's simpler scripts |
| **Event system** | Streams (BullMQ) use more Redis memory than Pub/Sub (Bull, Bee-Queue) |
| **Priority queues** | O(log N) insertion vs O(1) for standard FIFO |
| **Job data size** | Larger payloads increase Redis memory and network transfer |
| **Concurrency setting** | Higher concurrency reduces Redis round-trips per job |

### Bee-Queue's Performance Edge

Bee-Queue claims significantly higher throughput than Bull in benchmarks. This comes from:

- ~1000 lines of code vs Bull's much larger codebase
- Simpler Lua scripts (fewer keys, fewer operations)
- Single hash for all jobs (fewer Redis keys)
- Pipelined bulk operations via `saveAll()`
- No priority queue overhead

The trade-off: Bee-Queue lacks priorities, repeatable jobs, rate limiting, and flows.

### BullMQ Optimization Techniques

1. **Fetch-next-on-complete**: The `moveToFinished` Lua script atomically completes the current job AND fetches the next one in a single Redis round-trip
2. **Marker-based signaling**: Instead of polling for delayed jobs, markers trigger workers to check for promoted delayed jobs
3. **Pipelined bulk adds**: `queue.addBulk()` reduces round-trips for batch job creation
4. **Metrics aggregation**: Per-minute counters stored in Redis lists with configurable retention (~120KB for 2 weeks)

### Redis Memory Considerations

| Component | Memory Impact |
|-----------|--------------|
| Job hash | Proportional to data size (store minimal data, use references) |
| Wait/active lists | Negligible (just job IDs) |
| Completed/failed sorted sets | Grows unbounded without `removeOnComplete`/`removeOnFail` |
| Event stream | Auto-trimmed to ~10,000 entries |
| Metrics | ~120KB per queue for 2 weeks |

---

## 11. Valkey Compatibility

### What is Valkey?

Valkey is an open-source fork of Redis created in 2024 when Redis changed its license. It is maintained by the Linux Foundation and is API-compatible with Redis.

Key facts:
- Forked from Redis 7.2.4
- Valkey 8.0.0 made no backwards-incompatible changes to command syntax
- Supports all Redis data structures: lists, sorted sets, hashes, streams, pub/sub
- Full Lua scripting support
- Creates symlinks from `redis-server`/`redis-cli` to Valkey binaries

### BullMQ Valkey Support

BullMQ **explicitly detects Valkey** in its connection setup:

```typescript
// From redis-connection.ts
if (line.includes('valkey_version:') || line.includes('server:Valkey')) {
  // Sets database type to Valkey, extracts version
}
```

BullMQ's CI includes Valkey test runs (merged October 2024, PR #2794). Valkey 7.2+ should work since it is based on Redis 7.2.4, well above BullMQ's minimum Redis 5.0 requirement.

### Bull and Bee-Queue Valkey Compatibility

Neither Bull nor Bee-Queue have explicit Valkey detection, but since they use standard Redis commands (lists, sorted sets, hashes, Lua scripts, pub/sub) through ioredis, they should work with Valkey without modification. Valkey's full command compatibility means existing Redis clients connect transparently.

### Dragonfly Compatibility

BullMQ also documents compatibility with Dragonfly, a Redis-compatible in-memory store. Queue names must use curly braces (`{myqueue}`) for Dragonfly's multi-core optimization. Some features (priorities, rate limiting) may not work across split queues.

### AWS ElastiCache / MemoryDB

These managed Redis services generally work but may have limitations:
- ElastiCache Serverless has known issues with certain BullMQ operations
- Ensure `maxmemory-policy` is set to `noeviction` in managed configurations

---

## 12. Production Deployment

### Redis/Valkey Configuration

```conf
# REQUIRED: Never evict queue keys
maxmemory-policy noeviction

# RECOMMENDED: AOF persistence
appendonly yes
appendfsync everysec

# OPTIONAL: RDB snapshots as backup
save 900 1
save 300 10
save 60 10000
```

### Connection Configuration

```typescript
// BullMQ Worker (consumer)
const worker = new Worker('myQueue', processor, {
  connection: {
    host: 'redis.example.com',
    port: 6379,
    maxRetriesPerRequest: null,  // Workers: infinite retries
    retryStrategy: (times) => Math.min(times * 1000, 20000),
  }
});

// BullMQ Queue (producer)
const queue = new Queue('myQueue', {
  connection: {
    host: 'redis.example.com',
    port: 6379,
    maxRetriesPerRequest: 20,    // Producers: fail fast
    enableOfflineQueue: false,    // Don't buffer commands
  }
});
```

### Graceful Shutdown

```typescript
process.on('SIGTERM', async () => {
  await worker.close();
  // Active jobs complete; locks release cleanly
  process.exit(0);
});
```

### Job Retention Strategy

```typescript
const queue = new Queue('myQueue');
await queue.add('job', data, {
  removeOnComplete: { age: 3600, count: 1000 }, // Keep max 1000 or 1 hour
  removeOnFail: { age: 86400, count: 5000 },    // Keep max 5000 or 24 hours
});
```

### Redis Cluster

Both BullMQ and Bull require **hash tags** for Redis Cluster:

```typescript
// Queue name must include curly braces
const queue = new Queue('{myQueue}');
// All keys: bull:{myQueue}:wait, bull:{myQueue}:active, etc.
// Hash tag {myQueue} ensures all keys land in the same slot
```

---

## 13. Monitoring and Tooling

### Bull Board

A UI dashboard for Bull and BullMQ with support for Express, Fastify, Koa, Hapi, NestJS, Hono, H3, and Elysia.

Features: job inspection, retry failed jobs, queue statistics, read-only mode, multi-tenancy via `visibilityGuard`, custom data formatters for sensitive information redaction.

### Arena

A web GUI supporting Bee-Queue, Bull, and BullMQ. Features: queue health overview, job pagination and filtering by state, job detail inspection with stack traces, one-click retry, and Docker deployment support.

### Taskforce.sh

The commercial monitoring platform from the BullMQ maintainers. Provides real-time dashboards, alerts, and operational insights.

### BullMQ Built-in Metrics

```typescript
const worker = new Worker('myQueue', processor, {
  metrics: { maxDataPoints: MetricsTime.TWO_WEEKS }
});

// Retrieve metrics
const metrics = await queue.getMetrics('completed');
// { data: [12, 8, 15, ...], count: 42000, meta: {...} }
```

### OpenTelemetry Integration (BullMQ v5.67+)

BullMQ emits OpenTelemetry spans for job processing, enabling integration with Jaeger, Zipkin, Datadog, and other observability platforms.

---

## 14. Other Notable Libraries

### Kue (Deprecated)

Early Node.js Redis queue with a built-in web dashboard. Featured 5 priority levels (critical, high, medium, normal, low), delayed jobs, retry with backoff, TTL for active jobs, and Redis Pub/Sub events. **No longer maintained** -- use BullMQ instead.

### Node-Resque

A Redis-backed delayed job system for Node.js, compatible with Ruby Resque's API. Supports priority queues, delayed jobs, retries, worker locking, and plugins. Requires Redis 2.6+. Good choice if you need interoperability with Ruby Resque workers.

### RedisSMQ

A high-performance Redis message queue supporting FIFO, LIFO, and priority queues with exchange patterns (Direct, Topic, Fanout). Includes built-in scheduling, rate limiting, REST API with Swagger UI, and a web dashboard. Requires Redis 4+.

### Agenda

Not Redis-based (uses MongoDB/PostgreSQL) but worth mentioning for comparison. Supports human-readable scheduling intervals, long-running jobs, pluggable storage backends, and built-in REST API. The v6 rewrite is TypeScript-native.

### Better-Queue

A local (non-Redis) queue for single-process use with persistent storage options. Supports batched processing, task prioritization, merging/filtering, and progress tracking. Not suitable for distributed systems.

---

## 15. Code Examples

### Basic BullMQ Producer-Consumer

```typescript
import { Queue, Worker, QueueEvents } from 'bullmq';

const connection = { host: 'localhost', port: 6379 };

// Producer
const queue = new Queue('emails', { connection });

await queue.add('welcome', { to: 'user@example.com', template: 'welcome' });
await queue.add('reminder', { to: 'user@example.com', template: 'reminder' }, {
  delay: 86400000,  // 24 hours
  attempts: 3,
  backoff: { type: 'exponential', delay: 5000 }
});

// Consumer
const worker = new Worker('emails', async (job) => {
  switch (job.name) {
    case 'welcome':
      await sendEmail(job.data.to, job.data.template);
      break;
    case 'reminder':
      await sendEmail(job.data.to, job.data.template);
      break;
  }
  return { sent: true, timestamp: Date.now() };
}, { connection, concurrency: 10 });

worker.on('completed', (job, result) => {
  console.log(`Job ${job.id} completed`);
});

worker.on('failed', (job, err) => {
  console.error(`Job ${job?.id} failed: ${err.message}`);
});

// Events (optional, uses Redis Streams)
const queueEvents = new QueueEvents('emails', { connection });
queueEvents.on('completed', ({ jobId }) => {
  console.log(`[stream] Job ${jobId} completed`);
});
```

### BullMQ Flow Example

```typescript
import { FlowProducer, Worker } from 'bullmq';

const connection = { host: 'localhost', port: 6379 };

const flowProducer = new FlowProducer({ connection });

// Create a parent job with children
await flowProducer.add({
  name: 'compile-report',
  queueName: 'reports',
  data: { reportId: 'monthly-2026-02' },
  children: [
    {
      name: 'fetch-sales',
      queueName: 'data-fetch',
      data: { source: 'sales-db', month: '2026-02' }
    },
    {
      name: 'fetch-inventory',
      queueName: 'data-fetch',
      data: { source: 'inventory-api', month: '2026-02' }
    }
  ]
});

// Child worker
new Worker('data-fetch', async (job) => {
  const data = await fetchFromSource(job.data.source, job.data.month);
  return data; // Return value available to parent
}, { connection });

// Parent worker
new Worker('reports', async (job) => {
  const childValues = await job.getChildrenValues();
  // childValues contains results from all children
  const report = compileReport(childValues);
  return report;
}, { connection });
```

### Bee-Queue Example

```javascript
const Queue = require('bee-queue');

const queue = new Queue('tasks', {
  redis: { host: 'localhost', port: 6379 },
  isWorker: true,
  removeOnSuccess: true,
  stallInterval: 5000
});

// Add a job
const job = queue.createJob({ url: 'https://example.com' })
  .timeout(10000)
  .retries(3)
  .backoff('exponential', 1000)
  .save();

job.on('succeeded', (result) => {
  console.log(`Job ${job.id} succeeded: ${result}`);
});

// Process jobs
queue.process(20, async (job) => {
  const result = await fetch(job.data.url);
  job.reportProgress(50);
  const text = await result.text();
  job.reportProgress(100);
  return text.length;
});
```

### Bull Example with Rate Limiting

```javascript
const Queue = require('bull');

const apiQueue = new Queue('api-calls', 'redis://localhost:6379', {
  limiter: { max: 100, duration: 60000 }, // 100 per minute
  defaultJobOptions: {
    attempts: 3,
    backoff: { type: 'exponential', delay: 2000 },
    removeOnComplete: 100,
    removeOnFail: 500
  }
});

apiQueue.process(5, async (job) => {
  return await callAPI(job.data.endpoint, job.data.payload);
});

// Add jobs
await apiQueue.add('fetch', { endpoint: '/users', payload: {} }, { priority: 1 });
await apiQueue.add('fetch', { endpoint: '/logs', payload: {} }, { priority: 10 });
```

---

## 16. Common Pitfalls

| Pitfall | Why It Happens | How to Avoid |
|---------|---------------|--------------|
| **Stalled jobs from blocking event loop** | CPU-heavy sync code in processor prevents lock renewal | Use sandboxed processors for CPU work; keep processors async |
| **Memory growth from completed jobs** | Completed/failed sets grow unbounded by default | Configure `removeOnComplete` and `removeOnFail` |
| **Redis evicting queue keys** | `maxmemory-policy` set to something other than `noeviction` | Always set `maxmemory-policy noeviction` |
| **Lost events on reconnection** | Bull/Bee-Queue use Pub/Sub which drops messages on disconnect | Use BullMQ's stream-based QueueEvents for reliable events |
| **Duplicate processing** | Worker crashes after starting job; stalled detector re-queues | Make processors idempotent; use deduplication keys |
| **Connection exhaustion** | Each queue instance creates 2-3 Redis connections | Share connections where allowed; use connection pools |
| **Redis Cluster key distribution** | Queue keys span multiple slots | Use hash tags: `{queueName}` in prefix/name |
| **Priority queue performance** | O(log N) insertions slow down at scale | Only use priorities when necessary; most jobs should be unprioritized |
| **Repeatable job duplication** | Multiple processes calling `add()` with same repeat config | Use `upsertJobScheduler()` (BullMQ) which is idempotent |
| **ioredis keyPrefix conflict** | BullMQ manages its own prefixing; ioredis prefix breaks Lua scripts | Use BullMQ's `prefix` option, not ioredis `keyPrefix` |
| **Unhandled worker errors** | No error listener on worker; Node.js crashes on unhandled errors | Always attach `worker.on('error', handler)` |
| **Job timeout confusion** | Bull/BullMQ timeouts do not proactively kill jobs | Implement your own cancellation logic; timeouts only mark jobs as failed |

---

## 17. Best Practices

1. **Store minimal data in jobs** -- Reference IDs to databases rather than embedding large payloads. Large job data increases Redis memory and network overhead. (Source: BullMQ docs - going to production)

2. **Enable AOF persistence** -- Use `appendonly yes` with `appendfsync everysec` for durability without major performance impact. (Source: BullMQ docs - going to production)

3. **Implement graceful shutdown** -- Listen for SIGTERM/SIGINT and call `worker.close()` to finish active jobs before exiting. (Source: BullMQ docs - going to production)

4. **Use sandboxed processors for CPU work** -- Prevents event loop blocking which causes stalled jobs and lock expiration. (Source: BullMQ docs - sandboxed processors)

5. **Configure job retention** -- Use `removeOnComplete` and `removeOnFail` with count/age limits to prevent unbounded Redis memory growth. (Source: BullMQ docs - auto-removal)

6. **Make processors idempotent** -- At-least-once delivery means jobs may run twice. Use external idempotency keys. (Source: Bull patterns, BullMQ reliability docs)

7. **Encrypt sensitive data** -- Avoid storing PII or secrets directly in job data. Encrypt before queuing or store references. (Source: BullMQ docs - going to production)

8. **Use hash tags for Redis Cluster** -- Wrap queue names in braces `{myQueue}` to ensure all keys land in the same slot. (Source: Bull PATTERNS.md)

9. **Monitor queue depth and worker health** -- Use Bull Board, Arena, or custom dashboards to detect accumulating backlogs early. (Source: Bull Board, Arena docs)

10. **Set appropriate concurrency per workload** -- IO-bound: high concurrency (100-300). CPU-bound: low concurrency, more workers. (Source: BullMQ docs - parallelism)

11. **Use BullMQ for new projects** -- Bull is in maintenance mode. BullMQ offers TypeScript, flows, better events, deduplication, and active development. (Source: Bull README)

12. **Test with Valkey if migrating from Redis** -- BullMQ has CI tests for Valkey. For Bull/Bee-Queue, validate your specific feature set. (Source: BullMQ GitHub)

---

## 18. Further Reading

| Resource | Type | Why Recommended |
|----------|------|-----------------|
| [BullMQ Documentation](https://docs.bullmq.io/) | Official Docs | Comprehensive reference for the recommended queue library |
| [Bull GitHub](https://github.com/OptimalBits/bull) | Source + Docs | PATTERNS.md and REFERENCE.md are excellent for understanding internals |
| [Bee-Queue GitHub](https://github.com/bee-queue/bee-queue) | Source + Docs | Clean codebase, good for understanding minimal queue implementation |
| [BullMQ Lua Scripts](https://github.com/taskforcesh/bullmq/tree/master/src/commands) | Source Code | 53 Lua scripts reveal exactly how Redis is used for every operation |
| [Redis Data Types](https://redis.io/docs/latest/develop/data-types/) | Official Docs | Essential for understanding the data structures queues are built on |
| [Valkey Documentation](https://valkey.io/) | Official Docs | For understanding Redis fork compatibility |
| [Bull Board](https://github.com/felixmosh/bull-board) | Tooling | Best dashboard for Bull/BullMQ monitoring |
| [Arena](https://github.com/mixmaxhq/arena) | Tooling | Dashboard supporting Bee-Queue, Bull, and BullMQ |
| [Node-Resque](https://github.com/actionhero/node-resque) | Alternative | Redis queue compatible with Ruby Resque ecosystem |
| [RedisSMQ](https://github.com/weyoss/redis-smq) | Alternative | High-performance alternative with exchange patterns |
| [ioredis](https://github.com/luin/ioredis) | Dependency | The Redis client underlying BullMQ and Bull |
| [@nestjs/bullmq](https://docs.bullmq.io/guide/nestjs) | Integration | Official NestJS module for BullMQ |

---

*This guide was synthesized from 42 sources. See `resources/nodejs-queue-libs-redis-valkey-sources.json` for the full source list with quality scores.*
