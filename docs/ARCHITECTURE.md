# glide-mq Architecture Plan

## Context

Building a Node.js message queue library to replace BullMQ. Built exclusively on speedkey (valkey-glide with direct NAPI bindings). Streams-first architecture. Cluster-native from day one. Full feature parity with BullMQ plus differentiators. This is the winning horse.

## Key Schema

All keys share a hash tag `{q}` where q = queue name. Cluster-safe by design.

```
glide:{q}:id                    # String - auto-increment job ID counter
glide:{q}:stream                # Stream - ready jobs (primary queue)
glide:{q}:scheduled             # ZSet - delayed + priority staging (score = timestamp | priority-encoded)
glide:{q}:job:{id}              # Hash - job data, opts, state, timestamps, return value, stacktrace
glide:{q}:completed             # ZSet - score = completed timestamp
glide:{q}:failed                # ZSet - score = failed timestamp
glide:{q}:events                # Stream - lifecycle events (completed, failed, progress, etc.)
glide:{q}:meta                  # Hash - queue metadata (paused, concurrency, rate limiter state)
glide:{q}:deps:{id}             # Set - child job IDs for parent (flows)
glide:{q}:parent:{id}           # Hash - parent queue + job ID reference
glide:{q}:dedup                 # Hash - field=dedup_id, value=job_id|timestamp
glide:{q}:rate                  # Hash - rate limiter counters (window start, count)
glide:{q}:schedulers            # Hash - field=scheduler_name, value=next_run_ts
```

## Job State Machine

```
             scheduled (ZSet)
               |
               v (promotion loop)
added --> stream (ready) --> PEL (active) --> completed (ZSet)
                                |
                                +--> failed (ZSet)
                                |      |
                                |      v (retry)
                                |    scheduled (with backoff delay)
                                |
                                +--> waiting-children (deps:{id} non-empty)
                                       |
                                       v (all children done)
                                     stream (re-queued)
```

States map to Redis structures:

- **waiting**: In stream, not yet claimed by consumer group
- **active**: In PEL (Pending Entries List) - claimed via XREADGROUP
- **delayed**: In scheduled ZSet with future timestamp as score
- **prioritized**: In scheduled ZSet with priority-encoded score (promoted in order)
- **completed**: In completed ZSet + job hash updated
- **failed**: In failed ZSet + job hash updated
- **waiting-children**: Parent job waiting, deps:{id} set tracks remaining children

## Priority Encoding in Scheduled ZSet

Score format: `(priority * 2^42) + timestamp_ms`

- Priority 0 (highest) jobs always sort before priority 1, regardless of timestamp
- Within same priority, FIFO by timestamp
- Max priority: 2^21 (matches BullMQ range)
- Non-delayed priority jobs get score with timestamp = 0 so they promote immediately

## Server Functions (not EVAL scripts)

Use Valkey Functions (FUNCTION LOAD / FCALL) instead of EVAL/EVALSHA scripts.

### Why Functions over Scripts

- **Persistent**: Loaded once, survive server restarts (stored in RDB/AOF). EVAL scripts are ephemeral.
- **Named**: Called by name (`FCALL addJob`) not SHA hash. Readable, debuggable.
- **Library namespaced**: All glide-mq functions in one library (`#!lua name=glidemq`). Load once, get all functions.
- **No cache miss**: EVALSHA fails if script not cached (NOSCRIPT error, requires reload). Functions are always available after FUNCTION LOAD.
- **Recovery**: On connection to a new node or after failover, one `FUNCTION LOAD` restores everything. No per-script reload loop.
- **Differentiator**: BullMQ uses 53 EVAL scripts. We use a single function library.

### Loading Strategy

1. On Queue/Worker creation, check if library exists: `FUNCTION LIST LIBRARYNAME glidemq`
2. If missing, load via `FUNCTION LOAD` with the full library source
3. If version mismatch (we track a version field in the library), reload with `FUNCTION LOAD REPLACE`
4. Library source is embedded in the npm package as a string constant (built from .lua source files at compile time)
5. In cluster mode, `FUNCTION LOAD` must be sent to all nodes (use route: "allNodes")

### Function Library: `glidemq`

```lua
#!lua name=glidemq

-- All functions registered in one library
redis.register_function('glidemq_addJob', function(keys, args) ... end)
redis.register_function('glidemq_addBulk', function(keys, args) ... end)
redis.register_function('glidemq_promote', function(keys, args) ... end)
...
```

### Functions (~15, not 53)

| Function               | Keys | Purpose                                                                      |
| ---------------------- | ---- | ---------------------------------------------------------------------------- |
| glidemq_addJob         | 4    | INCR id, HSET job, XADD stream or ZADD scheduled, XADD event                 |
| glidemq_addBulk        | 4    | Pipelined version of addJob for batch inserts                                |
| glidemq_promote        | 3    | ZRANGEBYSCORE scheduled, XADD to stream, ZREM from scheduled                 |
| glidemq_complete       | 5    | XACK stream, ZADD completed, HSET job, XADD event, check parent deps         |
| glidemq_fail           | 5    | XACK stream, ZADD failed or ZADD scheduled (retry), HSET job, XADD event     |
| glidemq_retry          | 4    | ZADD scheduled with backoff delay, HSET job attempts, XADD event             |
| glidemq_reclaimStalled | 3    | XAUTOCLAIM on stream, HSET stalled count, move to failed if exceeded         |
| glidemq_pause          | 2    | HSET meta paused=1, XADD event                                               |
| glidemq_resume         | 2    | HSET meta paused=0, XADD event                                               |
| glidemq_rateLimit      | 3    | Check/increment rate counter, return delay if exceeded                       |
| glidemq_addFlow        | N    | Atomic: create parent + children, set deps, add children to stream/scheduled |
| glidemq_completeChild  | 4    | Remove from parent deps set, if deps empty -> re-queue parent                |
| glidemq_dedup          | 3    | Check dedup hash, skip or add based on mode (simple/throttle/debounce)       |
| glidemq_removeJob      | 4    | Clean job hash, remove from all sets/streams                                 |
| glidemq_getMetrics     | 2    | Read per-minute counters from stats hash                                     |

### speedkey API for Functions

```typescript
// Load library (once, on init)
await client.functionLoad(librarySource, true /* replace */);

// Call function
await client.fcall('glidemq_addJob', [key1, key2, key3, key4], [arg1, arg2, ...]);

// In cluster mode, load to all nodes
await clusterClient.functionLoad(librarySource, true, { route: 'allPrimaries' });
```

## Connection Model

Each component uses dedicated GlideClient/GlideClusterClient instances:

```
Queue (producer)
  └── commandClient (1 non-blocking client)

Worker (consumer)
  ├── commandClient (1 non-blocking client) - XACK, HSET, Lua scripts
  ├── blockingClient (1 blocking client) - XREADGROUP BLOCK only
  └── schedulerLoop (uses commandClient) - promote delayed, reclaim stalled

QueueEvents (observer)
  └── blockingClient (1 blocking client) - XREAD BLOCK on events stream

FlowProducer (orchestrator)
  └── commandClient (1 non-blocking client)
```

Workers share commandClient for all non-blocking ops. blockingClient is dedicated to XREADGROUP BLOCK - never used for anything else.

## Consumer Group Strategy

- One consumer group per queue: `glide:{q}:workers`
- Each worker instance is a consumer: `worker-{uuid}`
- XREADGROUP GROUP workers worker-{uuid} COUNT {prefetch} BLOCK {timeout}
- XACK after job completes/fails
- XAUTOCLAIM for stalled job recovery (replaces BullMQ's lock-based stalling)

## TypeScript API

### Queue<Data, Result>

```typescript
class Queue<D = any, R = any> {
  constructor(name: string, opts?: QueueOptions);
  add(name: string, data: D, opts?: JobOptions): Promise<Job<D, R>>;
  addBulk(jobs: { name: string; data: D; opts?: JobOptions }[]): Promise<Job<D, R>[]>;
  getJob(id: string): Promise<Job<D, R> | null>;
  pause(): Promise<void>;
  resume(): Promise<void>;
  getMetrics(type: 'completed' | 'failed'): Promise<Metrics>;
  obliterate(opts?: { force: boolean }): Promise<void>;
  close(): Promise<void>;

  // Job schedulers (repeatable/cron)
  upsertJobScheduler(name: string, schedule: ScheduleOpts, template?: JobTemplate): Promise<void>;
  removeJobScheduler(name: string): Promise<void>;
}
```

### Worker<Data, Result>

```typescript
class Worker<D = any, R = any> {
  constructor(name: string, processor: Processor<D, R>, opts?: WorkerOptions);
  pause(force?: boolean): Promise<void>;
  resume(): Promise<void>;
  close(force?: boolean): Promise<void>;
  on(event: WorkerEvent, handler: Function): void;

  // Rate limiting
  rateLimit(ms: number): Promise<void>;
  static RateLimitError: typeof RateLimitError;
}

type Processor<D, R> = (job: Job<D, R>) => Promise<R>;

interface WorkerOptions {
  concurrency?: number; // per-worker, default 1
  globalConcurrency?: number; // across all workers
  prefetch?: number; // XREADGROUP COUNT
  blockTimeout?: number; // XREADGROUP BLOCK ms
  lockDuration?: number; // stall detection window
  stalledInterval?: number; // XAUTOCLAIM frequency
  maxStalledCount?: number; // max reclaims before fail
  limiter?: { max: number; duration: number };
}
```

### Job<Data, Result>

```typescript
class Job<D = any, R = any> {
  id: string;
  name: string;
  data: D;
  opts: JobOptions;
  attemptsMade: number;
  returnvalue: R | undefined;
  failedReason: string | undefined;
  progress: number | object;
  timestamp: number;
  finishedOn: number | undefined;
  processedOn: number | undefined;
  parentId?: string;

  updateProgress(progress: number | object): Promise<void>;
  updateData(data: D): Promise<void>;
  getChildrenValues(): Promise<Record<string, R>>;
  moveToFailed(err: Error): Promise<void>;
  remove(): Promise<void>;
  retry(): Promise<void>;
}

interface JobOptions {
  delay?: number;
  priority?: number; // 0 (highest) to 2^21
  attempts?: number;
  backoff?: { type: 'fixed' | 'exponential' | string; delay: number; jitter?: number };
  timeout?: number;
  removeOnComplete?: boolean | number | { age: number; count: number };
  removeOnFail?: boolean | number | { age: number; count: number };
  deduplication?: { id: string; ttl?: number; mode?: 'simple' | 'throttle' | 'debounce' };
  parent?: { queue: string; id: string };
}
```

### QueueEvents

```typescript
class QueueEvents {
  constructor(name: string, opts?: QueueEventsOptions);
  on(event: 'completed', handler: (args: { jobId: string; returnvalue: string }) => void): void;
  on(event: 'failed', handler: (args: { jobId: string; failedReason: string }) => void): void;
  on(event: 'progress', handler: (args: { jobId: string; data: any }) => void): void;
  on(event: 'added' | 'stalled' | 'paused' | 'resumed', handler: Function): void;
  close(): Promise<void>;
}
```

### FlowProducer

```typescript
class FlowProducer {
  constructor(opts?: FlowProducerOptions);
  add(flow: FlowJob): Promise<JobNode>;
  addBulk(flows: FlowJob[]): Promise<JobNode[]>;
  close(): Promise<void>;
}

interface FlowJob {
  name: string;
  queueName: string;
  data: any;
  opts?: JobOptions;
  children?: FlowJob[];
}
```

## Project Structure

```
glide-mq/
├── src/
│   ├── index.ts                # Public exports
│   ├── queue.ts                # Queue class
│   ├── worker.ts               # Worker class
│   ├── job.ts                  # Job class
│   ├── queue-events.ts         # QueueEvents class
│   ├── flow-producer.ts        # FlowProducer class
│   ├── connection.ts           # Client factory (blocking vs non-blocking)
│   ├── functions/
│   │   ├── index.ts            # Library loader, version check, FCALL wrappers
│   │   ├── glidemq.lua         # Single Lua library source (all functions)
│   │   └── build.ts            # Embeds .lua as string constant at build time
│   ├── types.ts                # Shared type definitions
│   ├── errors.ts               # Error classes
│   ├── utils.ts                # Key builders, score encoding, backoff calc
│   └── scheduler.ts            # Internal: promote delayed, reclaim stalled, job schedulers
├── tests/
│   ├── queue.test.ts
│   ├── worker.test.ts
│   ├── job.test.ts
│   ├── flow.test.ts
│   ├── events.test.ts
│   ├── scripts.test.ts
│   ├── cluster.test.ts
│   └── helpers/
│       └── setup.ts            # Valkey test server management
├── package.json
├── tsconfig.json
├── CLAUDE.md
├── HANDOVER.md
└── README.md
```

## Differentiators vs BullMQ

1. **Streams-first**: PEL replaces active list + lock tokens. XAUTOCLAIM replaces stalled job checker scripts. Single function library (~15 functions vs 53 EVAL scripts).
2. **Native NAPI performance**: speedkey's Rust core handles I/O, freeing Node.js event loop. No ioredis overhead.
3. **Cluster-native**: Hash tags enforced at queue creation. No afterthought `{braces}` requirement.
4. **Batch API**: Use speedkey's non-atomic Batch for pipelined multi-command operations (auto-splits across cluster nodes).
5. **Built-in observability**: Events stream + OpenTelemetry via speedkey's native integration.
6. **Typed end-to-end**: Generics on Queue/Worker/Job for data and result types.
7. **Simpler reliability model**: Consumer group semantics (XREADGROUP + XACK + XAUTOCLAIM) vs lock-renew-check-stall cycle.
8. **Server Functions**: Single FUNCTION LOAD, persistent across restarts, no NOSCRIPT cache-miss errors, named calls via FCALL. BullMQ uses ephemeral EVAL/EVALSHA with 53 scripts that must be re-cached on every new connection.

## Implementation Phases

### Phase 1: Core (Queue + Worker + Job)

- Connection factory (blocking vs non-blocking clients)
- Key builder utilities
- Lua scripts: addJob, promote, complete, fail, reclaimStalled
- Queue: add, addBulk, pause, resume, close
- Worker: XREADGROUP loop, processor, concurrency, stalled recovery
- Job: data access, progress, state queries
- Tests for all above

### Phase 2: Advanced Features

- Delayed jobs (scheduled ZSet + promotion loop)
- Priorities (encoded scores)
- Retries with backoff (fixed, exponential, custom, jitter)
- Job retention (removeOnComplete/removeOnFail)
- Deduplication (simple, throttle, debounce)
- Rate limiting
- Global concurrency

### Phase 3: Flows + Events

- FlowProducer: parent-child job trees
- QueueEvents: stream-based event subscription
- Job schedulers (repeatable/cron jobs)
- Metrics collection

### Phase 4: Production Hardening

- Graceful shutdown
- Connection error recovery
- OpenTelemetry integration
- Comprehensive cluster tests
- Documentation

## Verification

- Unit tests for each Lua script in isolation
- Integration tests with real Valkey instance (standalone + cluster)
- Benchmark against BullMQ (jobs/sec, latency p50/p99, memory)
- Stalled job recovery test (kill worker mid-processing)
- Cluster failover test
