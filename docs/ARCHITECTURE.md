# glide-mq Architecture Plan

## Context

Building a Node.js message queue library to replace BullMQ. Built exclusively on speedkey (valkey-glide with direct NAPI bindings). Streams-first architecture. Cluster-native from day one. Full feature parity with BullMQ plus differentiators. This is the winning horse.

## Key Schema

All keys share a hash tag `{queueName}` where `queueName` is the queue name. This ensures all keys for a given queue hash to the same cluster slot. Default prefix is `glide`.

```
glide:{queueName}:id            # String - auto-increment job ID counter
glide:{queueName}:stream        # Stream - ready jobs (primary queue)
glide:{queueName}:scheduled     # ZSet - delayed + priority staging (score = timestamp | priority-encoded)
glide:{queueName}:job:{id}      # Hash - job data, opts, state, timestamps, return value, stacktrace, cost
glide:{queueName}:completed             # ZSet - score = completed timestamp
glide:{queueName}:failed                # ZSet - score = failed timestamp
glide:{queueName}:events                # Stream - lifecycle events (completed, failed, progress, etc.)
glide:{queueName}:meta                  # Hash - queue metadata (paused, concurrency, rate limiter state,
                                #        rateLimitMax, rateLimitDuration for global rate limit)
glide:{queueName}:deps:{id}             # Set - child job IDs for parent (flows)
glide:{queueName}:parent:{id}           # Hash - parent queue + job ID reference
glide:{queueName}:dedup                 # Hash - field=dedup_id, value=job_id|timestamp
glide:{queueName}:rate                  # Hash - rate limiter counters (window start, count)
glide:{queueName}:schedulers            # Hash - field=scheduler_name, value=next_run_ts
glide:{queueName}:ordering              # Hash - per-key sequence counters (for concurrency=1)
glide:{queueName}:orderdone:pending:{k} # Hash - pending sequence tracking per ordering key
glide:{queueName}:group:{key}           # Hash - group state (active count, maxConcurrency,
                                #        rateMax, rateDuration, rateWindowStart, rateCount,
                                #        tbCapacity, tbTokens, tbRefillRate, tbLastRefill,
                                #        tbRefillRemainder)
glide:{queueName}:groupq:{key}          # List - FIFO wait list for group-limited jobs
glide:{queueName}:ratelimited           # ZSet - scheduler-managed promotion queue for rate-limited jobs
                                #        (score = earliest eligible timestamp)
```

## Job State Machine

```
             scheduled (ZSet)
               |
               v (promotion loop)
added --> stream (ready) --> PEL (active) --> completed (ZSet)
                |                |
                |                +--> failed (ZSet)
                |                |      |
                |                |      v (retry)
                |                |    scheduled (with backoff delay)
                |                |
                |                +--> waiting-children (deps:{id} non-empty)
                |                       |
                |                       v (all children done)
                |                     stream (re-queued)
                |
                +--> group-waiting (groupq:{key} list)
                |      |
                |      v (slot freed on complete/fail)
                |    stream (re-queued)
                |
                +--> rate-limited (ratelimited ZSet)
                       |
                       v (window reset, scheduler promotes)
                     stream (re-queued)
```

`moveToActive` may return `GROUP_RATE_LIMITED` when a job's ordering-key sliding window rate limit is exceeded, or `GROUP_TOKEN_LIMITED` when the token bucket has insufficient tokens. In both cases, the job is parked in the `glide:{queueName}:ratelimited` ZSet with a score equal to the earliest eligible timestamp. The scheduler's promotion loop picks it up once capacity is available. If a job's `cost` exceeds the bucket's `tbCapacity`, `moveToActive` moves the job to the DLQ instead.

States map to Valkey structures:
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
3. If version mismatch (we track a version field in the library - currently `LIBRARY_VERSION = 28`), reload with `FUNCTION LOAD REPLACE`
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

### Functions (26 in 1 library, not 53 scripts)

| Function | Keys | Purpose |
|----------|------|---------|
| glidemq_version | 0 | Return library version |
| glidemq_addJob | 4 | INCR id, HSET job, XADD stream or ZADD scheduled, XADD event |
| glidemq_promote | 3 | ZRANGEBYSCORE scheduled, XADD to stream, ZREM from scheduled |
| glidemq_complete | 5 | XACK stream, ZADD completed, HSET job, XADD event, check parent deps |
| glidemq_completeAndFetchNext | 6 | Complete current + fetch next in single RTT |
| glidemq_fail | 5 | XACK stream, ZADD failed or ZADD scheduled (retry), HSET job, XADD event |
| glidemq_reclaimStalled | 3 | XAUTOCLAIM on stream, HSET stalled count, move to failed if exceeded |
| glidemq_pause | 2 | HSET meta paused=1, XADD event |
| glidemq_resume | 2 | HSET meta paused=0, XADD event |
| glidemq_dedup | 3 | Check dedup hash, skip or add based on mode (simple/throttle/debounce) |
| glidemq_rateLimit | 3 | Check/increment rate counter, return delay if exceeded |
| glidemq_promoteRateLimited | 2 | Move rate-limited jobs back to stream |
| glidemq_checkConcurrency | 2 | Check global concurrency limit before processing |
| glidemq_moveToActive | 4 | XREADGROUP + set state to active |
| glidemq_deferActive | 3 | Return active job to stream for reprocessing |
| glidemq_addFlow | N | Atomic: create parent + children, set deps, add children to stream/scheduled |
| glidemq_completeChild | 4 | Remove from parent deps set, if deps empty -> re-queue parent |
| glidemq_removeJob | 4 | Clean job hash, remove from all sets/streams |
| glidemq_clean | 3 | Bulk-remove old completed/failed jobs by age |
| glidemq_revoke | 2 | Revoke a job by ID |
| glidemq_changePriority | 3 | Re-prioritize a waiting/delayed job |
| glidemq_changeDelay | 3 | Change delay of a delayed job |
| glidemq_promoteJob | 3 | Move a delayed job to waiting immediately |
| glidemq_searchByName | 2 | Search jobs by name pattern |
| glidemq_drain | 3 | Remove all waiting (and optionally delayed) jobs |
| glidemq_retryJobs | 3 | Bulk-retry failed jobs |

### speedkey API for Functions
```typescript
// Load library (once, on init)
await client.functionLoad(librarySource, { replace: true });

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

- One consumer group per queue: `glide:{queueName}:workers`
- Each worker instance is a consumer: `worker-{uuid}`
- XREADGROUP GROUP workers worker-{uuid} COUNT {prefetch} BLOCK {timeout}
- XACK after job completes/fails
- XAUTOCLAIM for stalled job recovery (replaces BullMQ's lock-based stalling)

## TypeScript API

### Queue<Data, Result>

```typescript
class Queue<D = any, R = any> extends EventEmitter {
  constructor(name: string, opts: QueueOptions)
  add(name: string, data: D, opts?: JobOptions): Promise<Job<D, R> | null>
  addBulk(jobs: { name: string; data: D; opts?: JobOptions }[]): Promise<Job<D, R>[]>
  getJob(id: string): Promise<Job<D, R> | null>
  getJobs(type: 'waiting' | 'active' | 'delayed' | 'completed' | 'failed', start?: number, end?: number): Promise<Job<D, R>[]>
  getJobCounts(): Promise<JobCounts>
  getJobCountByTypes(): Promise<JobCounts>
  count(): Promise<number>
  pause(): Promise<void>
  resume(): Promise<void>
  isPaused(): Promise<boolean>
  revoke(jobId: string): Promise<string>
  getMetrics(type: 'completed' | 'failed'): Promise<Metrics>
  obliterate(opts?: { force: boolean }): Promise<void>
  close(): Promise<void>

  // Bulk operations
  clean(grace: number, limit: number, type: 'completed' | 'failed'): Promise<string[]>
  drain(delayed?: boolean): Promise<void>
  retryJobs(opts?: { count?: number }): Promise<number>

  // Global concurrency and rate limiting
  setGlobalConcurrency(n: number): Promise<void>
  setGlobalRateLimit(config: RateLimitConfig): Promise<void>
  getGlobalRateLimit(): Promise<RateLimitConfig | null>
  removeGlobalRateLimit(): Promise<void>

  // Workers
  getWorkers(): Promise<WorkerInfo[]>

  // Logs and DLQ
  getJobLogs(id: string, start?: number, end?: number): Promise<{ logs: string[]; count: number }>
  getDeadLetterJobs(start?: number, end?: number): Promise<Job<D, R>[]>
  searchJobs(opts: SearchJobsOptions): Promise<Job<D, R>[]>

  // Job schedulers (repeatable/cron)
  upsertJobScheduler(name: string, schedule: ScheduleOpts, template?: JobTemplate): Promise<void>
  getJobScheduler(name: string): Promise<SchedulerEntry | null>
  getRepeatableJobs(): Promise<{ name: string; entry: SchedulerEntry }[]>
  removeJobScheduler(name: string): Promise<void>
}
```

### Worker<Data, Result>

```typescript
class Worker<D = any, R = any> extends EventEmitter {
  constructor(name: string, processor: Processor<D, R> | string, opts: WorkerOptions)
  waitUntilReady(): Promise<void>
  pause(force?: boolean): Promise<void>
  resume(): Promise<void>
  drain(): Promise<void>
  close(force?: boolean): Promise<void>
  abortJob(jobId: string): boolean
  isRunning(): boolean
  isPaused(): boolean
  rateLimit(ms: number): Promise<void>
  on(event: WorkerEvent, handler: Function): void
  static RateLimitError: typeof RateLimitError
}

type Processor<D, R> = (job: Job<D, R>) => Promise<R>
type WorkerEvent = 'completed' | 'failed' | 'error' | 'stalled' | 'closing' | 'closed' | 'active' | 'drained'

interface WorkerOptions {
  concurrency?: number           // per-worker, default 1
  globalConcurrency?: number     // across all workers
  prefetch?: number              // XREADGROUP COUNT
  blockTimeout?: number          // XREADGROUP BLOCK ms
  lockDuration?: number          // stall detection window
  stalledInterval?: number       // XAUTOCLAIM frequency
  maxStalledCount?: number       // max reclaims before fail
  promotionInterval?: number     // delayed job promotion interval
  limiter?: { max: number; duration: number }
  backoffStrategies?: Record<string, (attemptsMade: number, err: Error) => number>
  sandbox?: SandboxOptions       // run processor in child process/thread
}
```

### Job<Data, Result>

```typescript
class Job<D = any, R = any> {
  readonly id: string
  readonly name: string
  data: D
  readonly opts: JobOptions
  attemptsMade: number
  returnvalue: R | undefined
  failedReason: string | undefined
  progress: number | object
  timestamp: number
  finishedOn: number | undefined
  processedOn: number | undefined
  parentId?: string
  parentQueue?: string
  orderingKey?: string
  groupKey?: string
  cost?: number
  abortSignal?: AbortSignal
  discarded: boolean

  // Lifecycle
  log(message: string): Promise<void>
  updateProgress(progress: number | object): Promise<void>
  updateData(data: D): Promise<void>
  discard(): void
  moveToFailed(err: Error): Promise<void>
  remove(): Promise<void>
  retry(): Promise<void>
  changePriority(newPriority: number): Promise<void>
  changeDelay(newDelay: number): Promise<void>
  promote(): Promise<void>
  waitUntilFinished(pollIntervalMs?: number, timeoutMs?: number): Promise<'completed' | 'failed'>

  // Queries
  getChildrenValues(): Promise<Record<string, R>>
  getState(): Promise<string>
  isCompleted(): Promise<boolean>
  isFailed(): Promise<boolean>
  isDelayed(): Promise<boolean>
  isActive(): Promise<boolean>
  isWaiting(): Promise<boolean>
  isRevoked(): Promise<boolean>
}

interface JobOptions {
  delay?: number
  priority?: number              // 0 (highest) to 2^21
  attempts?: number
  backoff?: { type: 'fixed' | 'exponential' | string; delay: number; jitter?: number }
  timeout?: number
  removeOnComplete?: boolean | number | { age: number; count: number }
  removeOnFail?: boolean | number | { age: number; count: number }
  deduplication?: { id: string; ttl?: number; mode?: 'simple' | 'throttle' | 'debounce' }
  parent?: { queue: string; id: string }
}
```

### QueueEvents

```typescript
class QueueEvents {
  constructor(name: string, opts?: QueueEventsOptions)
  on(event: 'completed', handler: (args: { jobId: string; returnvalue: string }) => void): void
  on(event: 'failed', handler: (args: { jobId: string; failedReason: string }) => void): void
  on(event: 'progress', handler: (args: { jobId: string; data: any }) => void): void
  on(event: 'added' | 'stalled' | 'paused' | 'resumed', handler: Function): void
  close(): Promise<void>
}
```

### FlowProducer

```typescript
class FlowProducer {
  constructor(opts?: FlowProducerOptions)
  add(flow: FlowJob): Promise<JobNode>
  addBulk(flows: FlowJob[]): Promise<JobNode[]>
  close(): Promise<void>
}

interface FlowJob {
  name: string
  queueName: string
  data: any
  opts?: JobOptions
  children?: FlowJob[]
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
│   │   └── index.ts            # Lua library source (embedded as string) + FCALL wrappers
│   ├── sandbox/
│   │   ├── index.ts            # Sandbox factory
│   │   ├── pool.ts             # Worker pool manager
│   │   ├── runner.ts           # Child process/thread runner
│   │   ├── sandbox-job.ts      # IPC-proxied job for sandbox context
│   │   └── types.ts            # Sandbox type definitions
│   ├── types.ts                # Shared type definitions
│   ├── errors.ts               # Error classes (UnrecoverableError)
│   ├── utils.ts                # Key builders, score encoding, backoff calc
│   ├── scheduler.ts            # Internal: promote delayed, reclaim stalled, job schedulers
│   ├── testing.ts              # In-memory TestQueue and TestWorker
│   ├── workflows.ts            # chain, group, chord helpers
│   ├── telemetry.ts            # OpenTelemetry integration
│   └── graceful-shutdown.ts    # Process signal handling
├── tests/                      # 80+ test files (vitest)
│   ├── integration.test.ts     # Full integration tests
│   ├── testing-mode.test.ts    # In-memory mode tests (no Valkey)
│   ├── search.test.ts          # Search feature tests
│   ├── sandbox.test.ts         # Sandbox tests
│   └── ...                     # Per-feature test files
├── docs/
│   ├── USAGE.md                # Queue & Worker basics
│   ├── ADVANCED.md             # Schedulers, rate limiting, DLQ
│   ├── WORKFLOWS.md            # FlowProducer, chain, group, chord
│   ├── OBSERVABILITY.md        # OpenTelemetry, job logs
│   ├── TESTING.md              # TestQueue & TestWorker
│   ├── MIGRATION.md            # BullMQ migration guide
│   └── ARCHITECTURE.md         # This file
├── package.json
├── tsconfig.json
├── CHANGELOG.md
├── LICENSE
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
