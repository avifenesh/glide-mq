# Learning Guide: BullMQ Test Suite Catalog

**Generated**: 2026-02-14
**Sources**: 26 resources analyzed
**Depth**: medium

## TL;DR

- BullMQ test suite contains 31+ test files covering 800+ test cases
- Core testing areas: Queue, Worker, Job, Flow (parent-child), Events, Rate limiting, Stalled jobs, Repeat/Scheduler
- Comprehensive edge case coverage: job cancellation, deduplication, Redis cluster, sandboxed processes, metrics
- Tests validate atomicity, delivery guarantees, concurrency, error handling, and state transitions
- Focus on distributed system concerns: lock management, connection handling, crash recovery

## Test File Organization

BullMQ's test suite (https://github.com/taskforcesh/bullmq/tree/master/tests) contains 31 test files organized by feature domain:

### Core Functionality (7 files)
- `queue.test.ts` - ~33 tests covering queue operations, parent-child flows, FIFO/LIFO, priority
- `worker.test.ts` - ~200-250 tests for job processing, concurrency, backoff, progress tracking
- `job.test.ts` - ~80-90 tests for job lifecycle, state transitions, data updates, removal
- `flow.test.ts` - ~45-50 tests for parent-child dependencies, failure propagation, runtime chaining
- `events.test.ts` - ~23 tests for event emission, global events, custom events, trimming
- `getters.test.ts` - ~40 tests for job retrieval, state queries, pagination, Prometheus metrics
- `connection.test.ts` - ~35 tests for Redis connection management, shared connections, blocking vs non-blocking

### Scheduling & Timing (4 files)
- `repeat.test.ts` - ~45-50 tests for repeatable jobs, cron patterns, exponential backoff, endDate handling
- `job_scheduler.test.ts` - ~80-90 tests for job scheduling, upsert patterns, cron strategy, removeOnComplete
- `delay.test.ts` - ~12 tests for delayed job execution, timestamps, FIFO ordering with delays
- `job_scheduler_stress.test.ts` - stress testing for scheduler under load

### Concurrency & Performance (5 files)
- `concurrency.test.ts` - ~11 tests for max concurrency, dynamic limits, drained events, backoff with concurrency
- `rate_limiter.test.ts` - ~15+ tests for global/local rate limits, priority with limits, dynamic rate changes
- `bulk.test.ts` - 4 tests for bulk job operations, parent-child in bulk, worker distribution
- `async_fifo_queue.test.ts` - 7 tests for internal async FIFO queue, rejection handling, concurrent fetching
- `metrics.test.ts` - 4 tests for completed/failed metrics, maxDataPoints, pagination

### Job Lifecycle & States (5 files)
- `pause.test.ts` - ~11 tests for queue pause/resume, local vs global pause, graceful shutdown
- `clean.test.ts` - ~40+ tests for job cleanup, grace periods, state-specific cleaning, flow cleanup
- `stalled_jobs.test.ts` - ~14 tests for stalled job detection, retry limits, lock extension, parent propagation
- `job_cancellation.test.ts` - ~24 tests for AbortSignal integration, cancel all, lock renewal failure
- `job_cancellation_advanced.test.ts` - advanced cancellation scenarios

### Advanced Features (6 files)
- `deduplication.test.ts` - ~25-30 tests for debouncing, deduplication, TTL handling, key removal
- `sandboxed_process.test.ts` - ~50+ tests for child processes vs worker threads, .cjs/.mjs, env vars, SIGKILL
- `cluster.test.ts` - ~21 tests for Redis cluster support, authentication, parallel processing, worker fetching
- `obliterate.test.ts` - ~13 tests for complete queue destruction, flow cleanup, force obliterate with active jobs
- `telemetry_interface.test.ts` - telemetry/OpenTelemetry integration tests
- `lock_manager.test.ts` - ~15+ tests for lock lifecycle, renewal, error handling, telemetry

### Utilities & Scripts (4 files)
- `scripts.test.ts` - 6 tests for Redis script pagination (set/hash), cursor tracking
- `script_loader.test.ts` - script loading and caching tests
- `error-forwarding.test.ts` - 2 tests for error propagation from Repeat/JobScheduler to Queue
- `child-pool.test.ts` - 7 tests for subprocess pooling, reuse, termination handling

## Key Test Categories by Feature

### 1. Queue Operations

**File**: `queue.test.ts` (~33 tests)

**Categories tested**:
- Generic type support
- Job addition with parent-child relationships
- Queue naming validation (colons, empty names)
- `.drain()` - draining delayed, prioritized, specific jobs
- `.retryJobs()` - retry with timestamp filters, paused state handling
- `.promoteJobs()` - promoting delayed jobs to waiting
- `.removeDeprecatedPriorityKey()` - migration support

**Edge cases**:
- Parent in same queue vs different queue
- Multiple pending children vs single child
- Delayed option true/false with flows
- Queue paused during retry operations

### 2. Worker Processing

**File**: `worker.test.ts` (~200-250 tests)

**Categories tested**:
- LIFO queue processing
- Serial vs concurrent job execution
- Progress updates (number, object, string, boolean)
- Progress event capping
- Pre-existing jobs before worker start
- Job return data handling
- Retry logic with different backoff strategies
- Concurrency limits and worker limits
- Error handling and UnrecoverableError
- Worker lifecycle (pause, resume, close)
- Sandboxed vs inline processors
- Lock renewal during long-running jobs

**Edge cases**:
- Jobs added before worker instantiation
- Worker crash recovery
- Multiple workers on same queue
- Lock expiration during processing
- Signal-based cancellation (AbortSignal)
- Worker close during active job execution

### 3. Job Lifecycle

**File**: `job.test.ts` (~80-90 tests)

**Categories tested**:
- `.create()` - job creation with custom IDs, size limits, validation
- `JSON.stringify()` - serialization correctness
- `.update()` - updating job data
- `.remove()` - single and bulk removal
- `.log()` - job logging
- `.clearLogs()` - log cleanup
- `.moveToCompleted()` - manual state transition
- `.moveToFailed()` - manual failure
- `.changeDelay()` - updating delay
- `.changePriority()` - updating priority
- `.promote()` - promoting delayed job
- `.getState()` - state queries
- `.finished()` - completion promise

**Edge cases**:
- Empty custom job ID (falls back to numeric)
- Size limit violations with ASCII and non-ASCII data
- Invalid option combinations (delay + repeat, conflicting parent options)
- Priority validation (float, out of range)
- Empty deduplication/debounce IDs
- Jitter validation (0-1 range)
- Updating removed jobs (throws error)
- Removing 4000+ jobs in time range

### 4. Parent-Child Flows

**File**: `flow.test.ts` (~45-50 tests)

**Categories tested**:
- Parent waits for children completion
- Children results storage in parent
- `removeOnComplete` with age in children
- `removeOnFail` in last pending child
- Priority inheritance and override
- Backoff strategies in flows
- Runtime job chaining
- Failure propagation (failParentOnFailure, ignoreDependencyOnFailure, removeDependencyOnFailure)
- Flow producer operations

**Edge cases**:
- Parent in same queue as children
- Parent in different queue from children
- Multiple children in same queue
- Single child in same queue
- Children in different queues
- Age-based cleanup timing
- Parent stuck prevention when child removed

### 5. Events System

**File**: `events.test.ts` (~23 tests)

**Categories tested**:
- `waiting`, `active`, `completed`, `failed` events
- Global vs local events
- `duplicated` event for deduplication
- `cleaned`, `drained` events
- `waiting-children` event
- Custom events publishing
- Event trimming (maxLen: 0 and maxLen > 0)
- Error event propagation
- Autorun: false with manual `.run()`

**Edge cases**:
- Multiple workers causing duplicate drained events (should emit once)
- Event errors triggering queue error event
- Removing non-existent jobs (no events published)
- Delayed jobs with event trimming
- Concurrent drained detection with concurrency > 1

### 6. Rate Limiting

**File**: `rate_limiter.test.ts` (~15+ tests)

**Categories tested**:
- Global rate limit enforcement
- Per-job rate limit options
- Rate limit removal (jobs process immediately)
- Dynamic rate limit changes
- Priority with rate limiting
- Queue pause interaction with rate limits
- Processing time vs limiter delay
- Worker close with slow rate limits

**Edge cases**:
- Jobs should NOT enter delayed queue when rate limited (use separate tracking)
- Respecting processing time without adding limiter delay
- Quickly closing worker even with slow rate limit
- Rate limit hit during job execution

### 7. Stalled Jobs

**File**: `stalled_jobs.test.ts` (~14 tests)

**Categories tested**:
- Stalled job detection on worker start
- `skipStalledCheck` option
- `maxStalledCount` enforcement
- `stalledCounter` tracking
- Lock extension prevention
- Parent failure propagation from stalled child
- `removeOnFail` with numbers, booleans, objects
- `failParentOnFailure`, `continueParentOnFailure`, `ignoreDependencyOnFailure`, `removeDependencyOnFailure`

**Edge cases**:
- Jobs stalling more than allowable limit
- Stalled jobs with maxStalledCount > 1
- Lock expiration while job still processing
- Parent transitions when child stalls (failed vs waiting)
- Age-based removeOnFail respecting timestamps

### 8. Repeatable Jobs

**File**: `repeat.test.ts` (~45-50 tests)

**Categories tested**:
- Cron pattern parsing and execution
- `every` option (milliseconds)
- `endDate` enforcement
- Legacy cron format support
- Custom cron strategies
- Exponential backoff with repeat
- Removing repeatable jobs
- Job scheduler ID uniqueness
- Upsert behavior
- Clock drift handling

**Edge cases**:
- endDate not greater than current timestamp (throws)
- Multiple jobs with same cron, different scheduler IDs
- Same scheduler ID with different every patterns (creates one)
- Rapid upserts (should create only one scheduler)
- Clocks slightly out of sync between servers
- Repeatable job failure handling

### 9. Job Cancellation

**File**: `job_cancellation.test.ts` (~24 tests)

**Categories tested**:
- Cancel via abort event (recommended)
- Cancel via polling (alternative)
- `AbortSignal` integration with fetch-like APIs
- `signal.aborted` checks in async operations
- `cancelAll()` for active jobs
- Cancel non-existent job (returns false)
- Cancel completed job (returns false)
- Cancellation with UnrecoverableError
- Concurrent cancellations
- Cancellation reason support
- Lock loss during moveToFailed

**Edge cases**:
- Processor ignoring signal (backward compatibility)
- Job throws error during cancellation
- Multiple workers, one job cancelled
- AbortController only created when processor uses signal parameter
- Lock renewal failure during cancellation

### 10. Deduplication

**File**: `deduplication.test.ts` (~25-30 tests)

**Categories tested**:
- Debouncing with TTL
- Debouncing without TTL (waits for job finish)
- Deduplication (blocks duplicate adds)
- `deduplicated` event emission
- Debounce key removal on job removal
- Debounce key removal on job completion
- Manual deduplication key removal

**Edge cases**:
- TTL provided vs not provided (different removal timing)
- Removing debounced job removes debounce key
- Manual removal on debounced job in finished state (key persists)
- Deduplication key persists until explicitly removed
- Adding deduplicated job emits event but doesn't create duplicate

### 11. Sandboxed Processes

**File**: `sandboxed_process.test.ts` (~50+ tests)

**Categories tested**:
- Child processes vs worker threads
- `.cjs` (CommonJS) processor files
- `.mjs` (ESM) processor files
- URL-based processor paths
- esbuild-compiled processors
- `workerForkOptions` and `workerThreadsOptions`
- Processor with >2 params (token, signal)
- stdout/stderr handling
- UnrecoverableError propagation
- Env variable passing
- Direct SIGKILL handling
- Function not exported error

**Edge cases**:
- Processor killed with SIGKILL while processing
- Processor killed directly after completing
- Job completes but process crashes immediately
- Extra processor params ignored
- Timeout in workerForkOptions
- Processor ignoring signal parameter

### 12. Cluster Support

**File**: `cluster.test.ts` (~21 tests)

**Categories tested**:
- Redis Cluster connection configuration
- `Cluster.duplicate()` vs `Redis.duplicate()`
- Worker connection names on cluster
- `getWorkers()` fetching from all nodes
- `getQueueEvents()` from all nodes
- `getWorkersCount()` aggregation
- Authenticated cluster connections
- Parallel job processing
- Delayed jobs on cluster
- Job retries on cluster

**Edge cases**:
- Fetching client list from all cluster nodes
- Returning workers from node with most matching connections
- Empty array when no matching workers on any node
- Worker blocking connection with authentication
- QueueEvents with authentication
- Unnamed workers in results

### 13. Cleanup & Obliteration

**File**: `clean.test.ts` (~40+ tests), `obliterate.test.ts` (~13 tests)

**Categories tested**:
- Cleaning by state (completed, failed, delayed, waiting, prioritized)
- Grace period enforcement
- Job limit parameter
- Timestamp-based cleaning
- "wait" vs "waiting" acceptance
- Flow cleanup (parent and children)
- Job scheduler presence during clean
- Queue obliteration (complete destruction)
- Force obliterate with active jobs
- Repeatable job removal
- Job log removal

**Edge cases**:
- Empty queue clean
- Queue with past jobs but currently empty
- Jobs outside grace period vs inside
- Limit higher than actual job count
- Job without timestamp
- Parent in same vs different queue during obliterate
- Multiple children vs single child obliteration
- Obliterate with active jobs (throws unless force=true)

### 14. Pause & Resume

**File**: `pause.test.ts` (~11 tests)

**Categories tested**:
- Global queue pause
- Local worker pause
- Pause with delayed jobs (don't process)
- Resume after pause
- Pause events emission
- Wait for active jobs before pause
- Multiple workers paused
- Blocking job retrieval during pause
- Fast pause when queue drained
- `isPaused()` state query

**Edge cases**:
- Pausing running queue with active jobs (waits for completion)
- Local pause with >1 worker active
- Blocking BRPOP interrupted by pause
- Backoff=0 with pause (jobs move to paused state)
- Resume without error after pause

### 15. Metrics & Observability

**Files**: `metrics.test.ts` (4 tests), `telemetry_interface.test.ts`, `getters.test.ts` (Prometheus section)

**Categories tested**:
- Completed job metrics
- Failed job metrics
- `maxDataPoints` enforcement
- Metrics pagination
- Prometheus gauge export
- Telemetry trace integration
- Lock extension tracing

**Edge cases**:
- Only keeping metrics for last N data points
- Paginated retrieval of large metric sets
- Empty states in Prometheus format
- Global variables in Prometheus export

### 16. Connection Management

**File**: `connection.test.ts` (~35 tests)

**Categories tested**:
- Default extraOptions merging
- `skipVersionCheck` option
- `skipWaitingForReady` option
- Blocking vs non-blocking connections
- `maxRetriesPerRequest` configuration
- Shared connections
- Connection reuse
- Worker blocking connection
- FlowProducer connection
- Clustered IORedis connection
- Connection failure handling
- Reconnection logic
- maxmemory-policy validation

**Edge cases**:
- Version initialized as minimum when skipVersionCheck=true
- maxRetriesPerRequest=null when blocking=true
- Preserved maxRetriesPerRequest when blocking=false
- Worker uses blockingConnection with shared:false
- FlowProducer shares connection if Redis instance provided
- Propagating skipWaitingForReady to RedisConnection
- maxmemory-policy != noeviction warning

### 17. Utilities & Internal APIs

**Files**: `scripts.test.ts` (6 tests), `child-pool.test.ts` (7 tests), `async_fifo_queue.test.ts` (7 tests), `error-forwarding.test.ts` (2 tests), `lock_manager.test.ts` (~15 tests)

**Categories tested**:
- Redis set pagination
- Redis hash pagination
- Child process pool reuse
- Worker thread pool management
- Async FIFO queue
- Promise rejection handling in queue
- Error forwarding from Repeat/JobScheduler
- Lock manager lifecycle
- Lock renewal timing
- Lock tracking and untracking

**Edge cases**:
- Paginating small sets (same/different size)
- Paginating large sets in pages
- Returning same child if free
- Spawning new child if killed
- Many retained, one free (reuse old)
- execArgv consumption from process
- FIFO queue with random delays
- Rejected promises don't block queue
- Lock manager closed (no tracking)
- Multiple close calls (graceful)

## Common Testing Patterns

### 1. Setup/Teardown
All tests use Vitest with consistent lifecycle hooks:
- `beforeAll()` - initialize Redis connections
- `beforeEach()` - create fresh Queue/Worker instances
- `afterEach()` - cleanup jobs, close workers
- `afterAll()` - disconnect from Redis

### 2. Time Manipulation
Most scheduler/delay tests use Sinon's `useFakeTimers()` to control time:
```typescript
let clock: sinon.SinonFakeTimers;
beforeEach(() => {
  clock = sinon.useFakeTimers();
});
afterEach(() => {
  clock.restore();
});
```

### 3. Event Spies
Extensive use of Sinon spies to verify event emission:
```typescript
const spy = sinon.spy();
worker.on('completed', spy);
// ... trigger completion ...
expect(spy.callCount).toBe(1);
```

### 4. Concurrent Scenarios
Many tests spawn multiple workers to validate distributed behavior:
```typescript
const worker1 = new Worker(queueName, processor);
const worker2 = new Worker(queueName, processor);
// ... test concurrent processing ...
```

### 5. Error Injection
Tests inject failures at specific points:
- Redis connection drops
- Lock expiration
- Processor throws UnrecoverableError
- Signal/token cancellation
- Process SIGKILL

### 6. State Verification
Comprehensive state checking after operations:
```typescript
const state = await job.getState();
expect(state).toBe('completed');
const counts = await queue.getJobCounts();
expect(counts.completed).toBe(1);
```

## Known Skipped/Failing Tests

From `error-forwarding.test.ts`, there's a documented bug:
- Error forwarding from Repeat/JobScheduler to Queue currently fails
- Tests expect `spy.callCount = 1` but get `0`
- Bug tracked in comments: "This test is currently failing due to a bug"

## Test Coverage Gaps (Inferred)

Based on test catalog, these areas appear well-covered:
- Core job lifecycle
- Worker concurrency
- Parent-child flows
- Redis cluster support
- Sandboxed execution

Potential gaps (not explicitly tested):
- Network partition scenarios
- Redis failover during job execution
- Extremely large job payloads (>512MB)
- Cross-language flows (Python/Elixir/PHP interop)
- Job migration between queues

## Testing Tools Used

- **Test Framework**: Vitest
- **Mocking**: Sinon (spies, stubs, fake timers)
- **Redis**: IORedis client
- **Assertions**: Vitest's `expect()` API
- **Async**: async/await patterns throughout

## Behavioral Test Examples

### Job Addition Validation
```typescript
it('throws an error when delay and repeat options are provided', async () => {
  await expect(
    queue.add('test', { foo: 'bar' }, { delay: 1000, repeat: { every: 5000 } })
  ).rejects.toThrow();
});
```

### Stalled Job Recovery
```typescript
it('process stalled jobs when starting a queue', async () => {
  // Add job
  await queue.add('test', { foo: 'bar' });
  // Simulate crash (worker dies without completing)
  await worker1.close();
  // Start new worker
  const worker2 = new Worker(queueName, processor);
  // Verify job is recovered and processed
  await worker2.waitUntilReady();
  const completed = await queue.getCompleted();
  expect(completed.length).toBe(1);
});
```

### Flow Parent-Child
```typescript
it('should process children before the parent', async () => {
  const flow = await flowProducer.add({
    name: 'parent',
    queueName,
    data: {},
    children: [
      { name: 'child1', queueName, data: {} },
      { name: 'child2', queueName, data: {} }
    ]
  });

  const processing = [];
  const worker = new Worker(queueName, async (job) => {
    processing.push(job.name);
    return {};
  });

  await worker.waitUntilReady();
  // Allow processing
  await delay(1000);

  expect(processing[0]).toMatch(/child/);
  expect(processing[1]).toMatch(/child/);
  expect(processing[2]).toBe('parent');
});
```

### Rate Limiting
```typescript
it('should obey the rate limit', async () => {
  await queue.setGlobalConcurrency(5); // max 5 jobs per second

  const jobs = Array.from({ length: 10 }, (_, i) => ({ name: 'test', data: { idx: i } }));
  await queue.addBulk(jobs);

  const start = Date.now();
  const completed = [];
  worker.on('completed', (job) => completed.push({ job, time: Date.now() - start }));

  await worker.waitUntilReady();
  await delay(2500);

  // First 5 in first second, next 5 after rate limit window
  expect(completed.filter(c => c.time < 1000).length).toBe(5);
  expect(completed.filter(c => c.time >= 1000 && c.time < 2000).length).toBe(5);
});
```

## Integration with glide-mq

Relevant test patterns for glide-mq implementation:

### High Priority
1. **Stalled job detection** - Critical for Valkey-based locks
2. **Flow parent-child** - Dependency tracking across jobs
3. **Worker concurrency** - Multiple workers on shared Valkey instance
4. **Job state transitions** - Atomic state changes via Lua scripts
5. **Rate limiting** - Per-queue and global rate limits

### Medium Priority
6. **Deduplication** - Debouncing and deduplication logic
7. **Delayed jobs** - Sorted set management for delays
8. **Event system** - Redis streams for events
9. **Metrics** - Time-series data for completed/failed counts
10. **Pause/Resume** - Queue and worker state management

### Lower Priority (glide-mq Phase 2+)
11. **Repeatable jobs** - Cron scheduling
12. **Sandboxed processes** - Child process/worker thread isolation
13. **Cluster support** - Redis Cluster compatibility
14. **Job scheduler stress** - High-throughput scheduling

## Sources

1. BullMQ GitHub Repository - https://github.com/taskforcesh/bullmq
2. BullMQ Test Directory - https://github.com/taskforcesh/bullmq/tree/master/tests
3. BullMQ Documentation - https://docs.bullmq.io
4. queue.test.ts - Queue operations and flows
5. worker.test.ts - Worker processing and concurrency
6. job.test.ts - Job lifecycle and state transitions
7. flow.test.ts - Parent-child dependencies
8. events.test.ts - Event emission and handling
9. rate_limiter.test.ts - Rate limiting patterns
10. stalled_jobs.test.ts - Stalled job recovery
11. repeat.test.ts - Repeatable jobs and cron
12. metrics.test.ts - Queue metrics collection
13. getters.test.ts - Job retrieval and queries
14. sandboxed_process.test.ts - Process isolation
15. delay.test.ts - Delayed job execution
16. concurrency.test.ts - Concurrent processing
17. pause.test.ts - Queue pause/resume
18. clean.test.ts - Job cleanup logic
19. bulk.test.ts - Bulk operations
20. deduplication.test.ts - Deduplication and debouncing
21. job_cancellation.test.ts - Job cancellation with AbortSignal
22. obliterate.test.ts - Queue obliteration
23. cluster.test.ts - Redis Cluster support
24. job_scheduler.test.ts - Job scheduling
25. connection.test.ts - Connection management
26. async_fifo_queue.test.ts - Internal FIFO queue
27. error-forwarding.test.ts - Error propagation
28. lock_manager.test.ts - Lock lifecycle management
29. scripts.test.ts - Redis script utilities
30. child-pool.test.ts - Subprocess pooling

---

*This guide synthesized 800+ test cases from 31 test files. See `resources/bullmq-test-cases-sources.json` for full source list.*
