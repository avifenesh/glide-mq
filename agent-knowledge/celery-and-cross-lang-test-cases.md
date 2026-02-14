# Learning Guide: Celery and Cross-Language Queue System Test Patterns

**Generated**: 2026-02-14
**Sources**: 28 resources analyzed (Celery, Sidekiq, BullMQ test suites + our existing tests)
**Depth**: medium
**Purpose**: Adaptable test cases for a TypeScript/Valkey queue library (glide-mq)

## Prerequisites

- Familiarity with queue/worker architecture (producer, consumer, broker)
- Understanding of Redis/Valkey data structures (streams, sorted sets, hashes)
- Knowledge of Vitest or similar test frameworks
- Access to the glide-mq codebase and its existing test suite

## TL;DR

- Celery organizes tests into 4 tiers: unit, integration, smoke, and benchmarks. Our suite currently lacks smoke (end-to-end multi-component) and benchmark tiers entirely.
- The biggest gap in Node.js queue test suites vs. Celery/Sidekiq is **reliability testing**: broker failover, worker crash recovery with `acks_late`, message loss detection, and memory leak regression tests.
- Celery tests ~15 variants of `on_failure` alone; Sidekiq tests retry exhaustion with time-based windows, discard strategies, and kill strategies. Our retry tests cover the happy path but miss many edge cases.
- BullMQ's stalled job tests are the closest analog to our architecture and provide directly portable patterns for lock-based stall detection.
- Operational patterns (graceful shutdown, worker drain, autoscale, queue inspection) are heavily tested in mature systems but minimally covered in our suite.

---

## 1. Test Architecture Comparison

### Celery Test Tiers

| Tier | Directory | Purpose | Our Equivalent |
|------|-----------|---------|----------------|
| Unit | `t/unit/` | Isolated component tests with mocks | `tests/*.test.ts` (partially) |
| Integration | `t/integration/` | Real broker, multi-component | `tests/integration.test.ts` + edge-* |
| Smoke | `t/smoke/` | Full cluster, failover, end-to-end | **MISSING** |
| Benchmarks | `t/benchmarks/` | Throughput, latency measurement | **MISSING** |

### Celery Unit Test Modules

```
t/unit/
  app/          -- Application setup, task registration, configuration
  backends/     -- Result storage (Redis, DB, etc.), chord results
  concurrency/  -- Process/thread pool management
  events/       -- Event dispatching, heartbeat
  tasks/        -- Task results, states, AsyncResult
  worker/       -- Consumer, control, autoscale, revocation, strategy
  test_canvas.py -- Chains, groups, chords (workflow primitives)
```

### Sidekiq Test Organization (47 files)

Sidekiq uses a flat test directory but covers:
- Core: `job_test.rb`, `processor_test.rb`, `launcher_test.rb`, `manager_test.rb`
- Reliability: `retry_test.rb`, `retry_exhausted_test.rb`, `dead_set_test.rb`
- Scheduling: `scheduled_test.rb`, `scheduling_test.rb`
- API: `api_test.rb`, `client_test.rb`, `fetch_test.rb`
- Advanced: `middleware_test.rb`, `sharding_test.rb`, `filtering_test.rb`

### BullMQ Test Organization (32 files)

BullMQ is the closest analog to our architecture (TypeScript + Redis):
- Core: `queue.test.ts`, `worker.test.ts`, `job.test.ts`
- Reliability: `stalled_jobs.test.ts`, `connection.test.ts`
- Features: `rate_limiter.test.ts`, `delay.test.ts`, `concurrency.test.ts`, `flow.test.ts`
- Operational: `pause.test.ts`, `clean.test.ts`, `metrics.test.ts`, `events.test.ts`
- Stress: `job_scheduler_stress.test.ts`

---

## 2. Task Execution Test Patterns

### 2A. Basic Execution (Celery: test_request.py)

Celery tests 50+ execution scenarios. Key patterns we should adopt:

```typescript
// PATTERN: Execute with various result types
describe('Job execution result handling', () => {
  it('processes job that returns data');
  it('processes job that returns null');
  it('processes job that returns undefined');
  it('processes job returning circular reference throws');
  it('processes synchronous processor');
  it('processes async processor');
  it('processes job with very large return value');
});

// PATTERN: Task state tracking (Celery tests marked_as_started, on_accepted, on_success)
describe('Job state transitions', () => {
  it('job transitions: waiting -> active -> completed');
  it('job transitions: waiting -> active -> failed');
  it('job transitions: waiting -> active -> failed -> scheduled (retry)');
  it('job state is "active" during processing');
  it('job.attemptsMade increments on retry');
  it('job.attemptsMade does NOT increment on success');
  it('job.finishedOn is set after completion');
  it('job.processedOn is set when processing starts');
});
```

**Gap in our tests**: We test the happy path (job added, processed, completed) but lack tests for intermediate state verification during processing, and we do not test edge cases like circular references in return values.

### 2B. Error Handling Variants (Celery: 15+ on_failure tests)

Celery tests these failure variants that we largely miss:

```typescript
// PATTERN: Comprehensive failure testing
describe('Job failure handling', () => {
  // From Celery: test_on_failure_acks_late, test_on_failure_acks_early
  it('acks job on failure when acks_late is false');
  it('does NOT ack job on failure when acks_late is true');

  // From Celery: test_on_failure_Reject_rejects, test_on_failure_Reject_rejects_with_requeue
  it('rejects job and requeues on transient error');
  it('rejects job without requeue on permanent error');

  // From Celery: test_on_failure_WorkerLostError
  it('handles worker process crash during execution');
  it('handles worker process crash with redelivered message');

  // From Celery: test_on_failure_task_cancelled
  it('handles job cancellation during processing');

  // From Celery: test_on_failure_backend_cleanup_error
  it('handles error during failure cleanup gracefully');

  // From Celery: test_on_failure_acks_on_failure_or_timeout true/false
  it('respects ackOnFailure=true configuration');
  it('respects ackOnFailure=false configuration');
});
```

### 2C. Timeout Handling (Celery: test_on_hard_timeout, test_on_soft_timeout)

```typescript
// PATTERN: Timeout behavior testing
describe('Job timeout handling', () => {
  // Celery: test_on_hard_timeout_acks_late
  it('hard timeout kills job processing and moves to failed');
  it('hard timeout with acks_late still acks the message');

  // Celery: test_on_soft_timeout
  it('soft timeout emits warning but allows cleanup');

  // Celery: test_on_timeout_should_terminate
  it('timeout terminates the processor for the specific job');

  // Celery: test_on_timeout_with_none_timelimit_header
  it('handles job without timeout header gracefully');

  // BullMQ pattern: lockDuration expiry
  it('lock renewal prevents false timeout');
  it('missed lock renewal triggers stall detection');
});
```

---

## 3. Retry and Backoff Test Patterns

### 3A. Celery Retry Patterns (test_request.py)

```typescript
describe('Retry mechanics', () => {
  it('on_retry increments retry count');
  it('on_retry with acks_late re-acks the message');
  it('retry preserves original job data');
  it('retry resets processing timestamps');
});
```

### 3B. Sidekiq Retry Patterns (retry_test.rb) -- CRITICAL GAPS

Sidekiq has the most sophisticated retry testing. Key patterns:

```typescript
// PATTERN: Retry exhaustion and dead letter
describe('Retry exhaustion', () => {
  // Sidekiq: "throws away old messages after too many retries"
  it('moves job to failed set after max retries');

  // Sidekiq: "allows a numeric retry"
  it('respects per-job retry count override');

  // Sidekiq: "allows 0 retry => no retry and dead queue"
  it('retry=0 sends directly to failed, no retry attempts');

  // Sidekiq: "allows disabling retry"
  it('retry=false prevents any retry and drops the job');

  // Sidekiq: "allows retry_for => no retry and dead queue"
  it('retries only within time window, then dead-letters');

  // Sidekiq: "retry_for ignores max_retries setting"
  it('time-based retry window takes precedence over count');
});

// PATTERN: Custom backoff strategies
describe('Backoff strategies', () => {
  // Sidekiq: "retries with a default delay"
  it('default backoff: exponential with jitter');

  // Sidekiq: "retries with a custom delay and exception 1/2"
  it('custom backoff per exception type');

  // Sidekiq: "retries with a custom delay without exception"
  it('custom backoff using attempt count only');

  // Sidekiq: "supports discard"
  it('discard strategy: drops job on specific error type');

  // Sidekiq: "supports kill"
  it('kill strategy: immediately dead-letters on specific error type');
});

// PATTERN: Error message edge cases
describe('Error handling edge cases in retry', () => {
  // Sidekiq: "handles zany characters in error message"
  it('handles non-UTF8 characters in error message');

  // Sidekiq: "handles error message that raises an error"
  it('handles error whose .message getter throws');

  // Sidekiq: "falls back to the default retry on exception"
  it('falls back to default backoff if custom backoff throws');

  // Sidekiq: "saves backtraces" / "cleans backtraces"
  it('saves stack trace with retry metadata');
  it('cleans internal frames from saved stack trace');
});

// PATTERN: Retry queue routing
describe('Retry queue routing', () => {
  // Sidekiq: "allows a retry queue"
  it('routes failed jobs to a dedicated retry queue');

  // Sidekiq: "calls death handlers even for :discard"
  it('death handlers fire even when discarding');
});
```

**Critical gap**: Our retry tests only cover "fails twice, succeeds on 3rd attempt" with fixed backoff. We are missing: retry exhaustion to dead letter, time-based retry windows, per-error-type strategies, error message edge cases, retry queue routing, and death/exhaustion handlers.

### 3C. Sidekiq Dead Letter Queue (dead_set_test.rb)

```typescript
describe('Dead letter queue management', () => {
  // Sidekiq: "should put passed serialized job to the dead sorted set"
  it('exhausted retries move job to failed set with full data');

  // Sidekiq: "should remove dead jobs older than timeout"
  it('failed set auto-prunes jobs older than configured TTL');

  // Sidekiq: "should remove all but last max_jobs-1 jobs"
  it('failed set auto-prunes when exceeding max size');

  // Additional pattern from Sidekiq API tests:
  it('can retry a job from the failed set');
  it('can delete a specific failed job');
  it('can enumerate failed jobs with pagination');
  it('can clear all failed jobs');
});
```

**Gap**: We have no dead letter queue management tests at all.

---

## 4. Workflow / Canvas Test Patterns

### 4A. Celery Canvas Patterns (integration/test_canvas.py -- 100+ tests)

Celery's canvas is the most extensively tested workflow system. Key categories:

```typescript
// PATTERN: Chain execution
describe('Chains', () => {
  it('simple chain: A -> B -> C executes in order');
  it('chain passes result from each step to the next');
  it('chain with error in middle step propagates to end');
  it('chain with error callback receives the error');
  it('nested chain within chain');
  it('chain replacement: task replaces itself with sub-chain');
});

// PATTERN: Group (parallel) execution
describe('Groups', () => {
  it('group of 3 tasks executes all in parallel');
  it('group with 1000+ tasks completes');
  it('group results aggregated in order');
  it('group with partial failure: completed results available');
  it('group with error callback on individual task failure');
  it('nested group within group');
});

// PATTERN: Chord (group + callback)
describe('Chords', () => {
  it('chord header completes, then body runs with all results');
  it('chord with single header task');
  it('chord with empty header');
  it('chord error in header propagates to body');
  it('chord error in body after successful header');
  it('nested chord within chain');
  it('chord within chord');
  it('chord with 1000 header tasks');

  // Celery-specific but valuable pattern:
  it('chord counter tracks partial completion accurately');
  it('chord handles duplicate completion signals (idempotent)');
});
```

### 4B. BullMQ Flow Patterns (flow.test.ts -- 30+ tests)

BullMQ's flow model (parent-child dependencies) is closer to our architecture:

```typescript
describe('Flow: Parent-child relationships', () => {
  it('children process before parent');
  it('parent moves to waiting-children until all children complete');
  it('parent completes after all children complete');
  it('parent gets all child return values via getChildrenValues');
  it('nested flow: grandchild -> child -> parent');

  // BullMQ: Failure propagation in flows
  it('failParentOnFailure: child failure fails parent');
  it('failParentOnFailure with nested parents: cascades up');
  it('ignoreDependencyOnFailure: parent proceeds despite child failure');
  it('removeDependencyOnFailure: parent proceeds, failed child removed');
  it('continueParentOnFailure: parent starts even after child failure');

  // BullMQ: Flow with options
  it('flow respects priority option per child');
  it('flow respects backoff strategy per child');
  it('flow respects removeOnFail per child');
  it('flow respects removeOnComplete per child');

  // BullMQ: Edge cases
  it('adding child to already-completed parent throws');
  it('adding duplicate child with same parentId is idempotent');
  it('parent in delayed state fails when child fails');
  it('parent in prioritized state fails when child fails');
  it('processes parent jobs added while child is still active');
});
```

**Gap in our tests**: We have 5 flow tests covering basic parent-child, nesting, and getChildrenValues. We are missing failure propagation variants (failParentOnFailure, ignoreDependencyOnFailure, etc.), priority in flows, and most edge cases.

---

## 5. Stalled Job Detection Patterns

### 5A. BullMQ Stalled Jobs (stalled_jobs.test.ts -- 14 tests)

This is directly portable to our architecture since both use lock-based stall detection:

```typescript
describe('Stalled job detection', () => {
  // BullMQ: "process stalled jobs when starting a queue"
  it('new worker picks up stalled jobs on startup');

  // BullMQ: "don't process stalled jobs when starting a queue with skipStalledCheck"
  it('skipStalledCheck option disables stall recovery');

  // BullMQ: "moves jobs to failed" (max stalled count)
  it('job exceeding maxStalledCount moves to failed');

  // BullMQ: "keeps stalledCounter"
  it('stalledCounter persists across recovery attempts');

  // BullMQ: "moves jobs to failed with maxStalledCount > 1"
  it('multiple stall cycles before failure with maxStalledCount=3');

  // BullMQ: Parent-child interaction with stalling
  it('stalled child with failParentOnFailure fails parent');
  it('stalled child with ignoreDependencyOnFailure allows parent to proceed');

  // BullMQ: Retention with stalled jobs
  it('removeOnFail respected for stalled-then-failed jobs');
  it('removeOnFail=true removes all keys for stalled-then-failed job');

  // BullMQ: Lock extension prevents false stall
  it('jobs not stalled while lock is actively extended');

  // Our specific patterns for Valkey streams:
  it('XPENDING shows claimed but unacked entries for stalled jobs');
  it('XCLAIM transfers ownership of stalled job to new worker');
  it('stall detection runs on configurable interval');
});
```

**Gap**: Our `edge-events.test.ts` has one stalled event test. We need comprehensive stall detection tests.

---

## 6. Rate Limiting Test Patterns

### 6A. BullMQ Rate Limiter (rate_limiter.test.ts -- 13 tests)

```typescript
describe('Rate limiting', () => {
  // BullMQ: "should obey the rate limit"
  it('processes at most N jobs per duration window');

  // BullMQ: "should respect processing time without adding limiter delay"
  it('processing time counts toward rate window');

  // BullMQ: "should quickly close a worker even with slow rate-limit"
  it('worker closes promptly despite pending rate-limited jobs');

  // BullMQ: "when queue is paused between rate limit"
  it('pause during rate-limited wait resumes correctly');

  // BullMQ: "should obey the rate limit with max value greater than 1"
  it('burst: allows max=5 jobs then throttles');

  // BullMQ: "when dynamic limit is used"
  it('per-job dynamic rate limiting via worker.rateLimit()');

  // BullMQ: "should obey priority"
  it('rate limiting respects job priority ordering');

  // Celery: test_when_rate_limited, test_when_rate_limited_with_eta
  it('rate-limited job with eta schedules correctly');
  it('rate limit of "0" disables the limit');
});
```

### 6B. Celery Rate Limit Control (test_control.py)

```typescript
describe('Rate limit control commands', () => {
  // Celery: test_rate_limit
  it('dynamically changes rate limit for a task type');

  // Celery: test_rate_limit_invalid_rate_limit_string
  it('rejects invalid rate limit string');

  // Celery: test_rate_limit_nonexistant_task
  it('returns error for non-existent task type');
});
```

**Our existing coverage**: `rate-limit.test.ts` + edge-advanced tests cover basic rate limiting, window reset, global scope, and rateLimit(0). Main gap: rate limit interaction with pause, priority ordering under rate limits, and dynamic rate limit changes.

---

## 7. Delayed / Scheduled Job Test Patterns

### 7A. BullMQ Delay Patterns (delay.test.ts -- 12 tests)

```typescript
describe('Delayed job processing', () => {
  // BullMQ: "should process a delayed job only after delayed time"
  it('delayed job not processed before delay elapses');

  // BullMQ: "should process delayed jobs in correct order respecting delay"
  it('multiple delays: shorter delay processed first');

  // BullMQ: "should process delayed jobs with several workers"
  it('multiple workers cooperate on delayed job promotion');

  // BullMQ: "should process delayed jobs concurrently respecting delay and concurrency"
  it('concurrent workers respect delay ordering');

  // BullMQ: "should process delayed jobs with exact same timestamps in correct order (FIFO)"
  it('identical delay timestamps preserve FIFO order');

  // BullMQ: "should process a delayed job added after an initial long delayed job"
  it('short-delay job added after long-delay job processes first');

  // BullMQ: "when failed jobs are retried and moved to delayed"
  it('retried jobs moved to delayed do not get stuck');

  // BullMQ: "when queue is paused"
  it('delayed jobs still promote to waiting during pause');

  // BullMQ: "when markers are deleted"
  it('recovers from missing delay markers');
});
```

### 7B. BullMQ Job Scheduler (job_scheduler.test.ts -- 30+ tests)

```typescript
describe('Repeatable / Cron scheduling', () => {
  it('cron pattern fires at correct intervals');
  it('every-N-ms repeats accurately');
  it('endDate stops repetition');
  it('startDate in future delays first execution');
  it('startDate in past catches up');
  it('upsert scheduler updates pattern without creating duplicates');
  it('remove scheduler stops future executions');
  it('clocks slightly out of sync still create exactly one job');
  it('concurrent upserts produce exactly one scheduler');
  it('scheduler with removeOnComplete removes each iteration');
  it('different scheduler IDs with same pattern create separate schedulers');
  it('same scheduler ID with different pattern updates in-place');
});
```

**Our coverage**: `delayed.test.ts` has 3 tests (ZSet placement, promotion, ordering). `scheduler.test.ts` covers cron and every-N patterns. Main gap: multiple workers competing on promotion, delay interaction with pause, and scheduler upsert idempotency.

---

## 8. Revocation / Cancellation Test Patterns

### 8A. Celery Revocation (test_control.py -- 12 tests)

```typescript
describe('Job revocation', () => {
  // Celery: test_revoke
  it('revoked job is skipped when dequeued');

  // Celery: test_revoke_with_name
  it('revoke by task name affects all matching jobs');

  // Celery: test_revoke_terminate
  it('revoke with terminate kills actively running job');

  // Celery: test_revoke_by_stamped_headers
  it('revoke by metadata header matches jobs');

  // Celery: test_revoke_by_stamped_headers_terminate
  it('revoke by header + terminate kills matching active job');

  // Celery: test_revoke_backend_status_update
  it('revoke updates job status in result backend');

  // Celery: test_revoke_backend_failure_defensive
  it('revoke handles result backend failure gracefully');

  // From worker test: test_process_task_revoked_release_semaphore
  it('revoked task releases concurrency semaphore');
});
```

### 8B. BullMQ Job Cancellation (job_cancellation.test.ts)

```typescript
describe('Job cancellation', () => {
  // BullMQ: AbortSignal integration
  it('cancel active job via AbortSignal');
  it('cancel works with fetch-like APIs that accept AbortSignal');
  it('backward compatible: processor ignoring signal still works');

  // BullMQ: Edge cases
  it('cancelling non-existent job returns false');
  it('cancelling already-completed job returns false');
  it('cancel all active jobs');
  it('manual cancellation on lock renewal failure');

  // BullMQ: Active job tracking
  it('getActive returns list of currently processing job IDs');
  it('active job list updates as jobs complete');
});
```

**Our gap**: We have no cancellation/revocation tests. This is a significant missing category.

---

## 9. Worker Lifecycle Test Patterns

### 9A. Celery Worker Lifecycle (test_worker.py)

```typescript
describe('Worker lifecycle', () => {
  // Celery: test_start__stop
  it('worker starts and stops cleanly');

  // Celery: test_start__KeyboardInterrupt
  it('worker handles SIGINT gracefully');

  // Celery: test_start_catches_base_exceptions
  it('worker catches WorkerTerminate and shuts down');
  it('worker catches WorkerShutdown and initiates graceful stop');

  // Celery: test_wait_for_soft_shutdown
  it('soft shutdown waits for active jobs to complete');

  // Celery: test_wait_for_soft_shutdown_no_tasks
  it('soft shutdown returns immediately when no active jobs');

  // Celery: test_send_worker_shutdown
  it('shutdown signal propagates to all worker components');

  // Celery: connection management
  it('worker establishes broker connection on start');
  it('worker reconnects after connection loss');
  it('worker handles connection error during startup');
});
```

### 9B. Sidekiq Launcher (launcher_test.rb)

```typescript
describe('Worker process registration', () => {
  // Sidekiq: "stores process info in redis"
  it('worker registers itself in Redis with TTL');

  // Sidekiq: "fires start heartbeat event only once"
  it('startup heartbeat emits exactly once');

  // Sidekiq: "quiets"
  it('quiet mode: stops fetching new jobs, finishes active');
});
```

### 9C. BullMQ Worker Patterns (worker.test.ts -- 100+ tests)

```typescript
describe('Worker processing patterns', () => {
  // BullMQ: LIFO support
  it('process a LIFO queue');

  // BullMQ: "should not block forever"
  it('worker does not block indefinitely on empty queue');

  // BullMQ: drain
  it('worker drains (completes active, stops fetching) on close');
  it('emits drained event when no more jobs to process');

  // BullMQ: removeOnComplete/removeOnFail
  it('removeOnComplete=true removes job hash after success');
  it('removeOnComplete={count:5} keeps last 5 completed');
  it('removeOnComplete={age:3600} keeps jobs less than 1h old');
  it('removeOnFail=true removes job hash after failure');
  it('removeOnFail={count:10} keeps last 10 failed');

  // BullMQ: Auto-run
  it('autorun=false: worker does not start processing until run() called');
});
```

**Our coverage**: `shutdown.test.ts` tests close() behavior. `edge-worker.test.ts` may have some lifecycle tests. Main gaps: reconnection, quiet/drain mode, process registration, heartbeat, and LIFO support.

---

## 10. Connection and Reliability Test Patterns

### 10A. BullMQ Connection (connection.test.ts)

```typescript
describe('Connection resilience', () => {
  // BullMQ: "Should recover from a connection loss"
  it('worker recovers after Redis disconnects and reconnects');

  // BullMQ: "Should handle jobs added before and after a redis disconnect"
  it('jobs added before disconnect are processed after reconnect');

  // BullMQ: "Should close worker even if redis is down"
  it('worker.close() resolves even when Redis is unreachable');

  // BullMQ: "Should close underlying redis connection when closing fast"
  it('rapid close does not leak connections');

  // BullMQ: Connection failure modes
  it('fails with clear error on invalid host:port');
  it('fails with clear error on invalid connection URL');
  it('emits error event on connection failure');
  it('close resolves if connection has already failed');
});
```

### 10B. Celery Backend Resilience (test_redis.py)

```typescript
describe('Backend connection resilience', () => {
  // Celery: test_consume_from_connection_error
  it('handles connection error during consume');

  // Celery: test_cancel_for_connection_error
  it('handles connection error during cancel');

  // Celery: test_drain_events_connection_error
  it('handles connection error during event drain');

  // Celery: test__reconnect_pubsub
  it('reconnects pubsub after connection loss');
  it('reconnects pubsub and re-subscribes');

  // Celery: retry policy
  it('respects configured retry policy for transient errors');
  it('identifies which exceptions are safe to retry');

  // Celery: health check
  it('health check interval validates connection periodically');
  it('connection pool is reused across operations');
});
```

### 10C. Celery Broker Failover (smoke/failover/)

**This is the most critical gap in our testing.**

```typescript
describe('Broker failover', () => {
  // Celery: test_killing_first_broker
  it('tasks complete after primary broker dies (failover to secondary)');

  // Celery: test_reconnect_to_main
  it('worker reconnects to primary broker after it recovers');
});

describe('Worker failover', () => {
  // Celery: test_killing_first_worker
  it('tasks complete after one worker in cluster dies');

  // Celery: test_reconnect_to_restarted_worker
  it('cluster recovers after all workers restart');

  // Key config: task_acks_late = True enables proper recovery
  it('acks_late ensures no message loss on worker crash');
});
```

### 10D. Celery Consumer Resilience (test_consumer.py)

```typescript
describe('Consumer resilience', () => {
  // Celery: test_cancel_long_running_tasks_on_connection_loss
  it('cancels long-running tasks when broker connection drops');

  // Celery: test_cancel_active_requests_preserves_successful_tasks
  it('preserves results of completed tasks during connection recovery');

  // Celery: test_blueprint_restart_for_channel_errors
  it('consumer restarts on channel-level errors');

  // Celery: test_collects_at_restart
  it('collects resources properly during consumer restart');

  // Celery: prefetch management
  it('restores prefetch count after connection restart');
  it('prefetch count updates dynamically based on concurrency');
});
```

---

## 11. Pause / Resume Test Patterns

### BullMQ Pause (pause.test.ts -- 11 tests)

```typescript
describe('Queue pause/resume', () => {
  // BullMQ: "should pause a queue until resumed"
  it('no jobs processed while queue is paused');

  // BullMQ: "should wait until active jobs are finished before resolving pause"
  it('pause waits for active jobs to complete (graceful)');

  // BullMQ: "should pause the queue locally"
  it('local pause affects only this worker, not the queue');

  // BullMQ: "should pause the queue locally when more than one worker is active"
  it('local pause with multiple workers pauses only the caller');

  // BullMQ: "should wait for blocking job retrieval to complete before pausing locally"
  it('pause waits for blocking XREADGROUP to return');

  // BullMQ: "pauses fast when queue is drained"
  it('pause resolves quickly on empty queue');

  // BullMQ: "should not process delayed jobs"
  it('delayed job promotion is blocked during pause');

  // BullMQ: delayed retry during pause
  it('job retrying with backoff moves to paused state, not delayed');
});
```

**Our coverage**: We have basic pause/resume in `integration.test.ts` and `edge-queue.test.ts`. Gaps: graceful pause (wait for active), local vs. global pause, pause interaction with delayed/retrying jobs.

---

## 12. Queue Inspection / API Test Patterns

### 12A. Sidekiq API (api_test.rb -- 40+ tests)

These represent operational tests that queue systems need for production:

```typescript
describe('Queue inspection API', () => {
  // Sidekiq: queue state
  it('getJobCounts returns accurate counts per state');
  it('empty queue returns all zeros');
  it('queue latency reflects oldest waiting job age');

  // Sidekiq: job enumeration
  it('enumerates waiting jobs with pagination');
  it('enumerates completed jobs in descending order');
  it('enumerates failed jobs with offset/limit');

  // Sidekiq: job search
  it('finds job by ID across all states');
  it('finds jobs by ID in specific state');

  // Sidekiq: job operations
  it('removes specific job from queue');
  it('removes job while iterating safely');
  it('retries a specific failed job');
  it('clears all jobs in a specific state');

  // Sidekiq: scheduled set operations
  it('moves scheduled job to immediate processing');
  it('kills (dead-letters) a scheduled job');
  it('reschedules jobs in retry set');

  // Sidekiq: process enumeration
  it('enumerates active worker processes');
  it('enumerates active jobs per worker');
});
```

### 12B. Celery Worker Control Commands (test_control.py)

```typescript
describe('Worker control commands', () => {
  // Celery: inspection
  it('ping returns worker status');
  it('stats returns worker statistics');
  it('active_queues returns consumed queues');
  it('dump_active returns currently processing jobs');
  it('dump_reserved returns prefetched jobs');
  it('dump_schedule returns scheduled jobs');
  it('dump_revoked returns revoked job IDs');
  it('query_task returns specific job status');

  // Celery: runtime control
  it('time_limit changes timeout for task type');
  it('pool_grow increases worker concurrency');
  it('pool_shrink decreases worker concurrency');
  it('add_consumer subscribes to additional queue');
  it('cancel_consumer unsubscribes from queue');
  it('enable_events / disable_events toggles event emission');

  // Celery: shutdown
  it('shutdown command stops worker gracefully');
});
```

**Gap**: We have `getJobCounts` in edge-queue tests. We are missing most inspection API tests and all runtime control command tests.

---

## 13. Deduplication Test Patterns

### BullMQ Deduplication (deduplication.test.ts)

Our dedup tests are actually well-covered compared to BullMQ. Comparison:

| Scenario | BullMQ | Our Tests |
|----------|--------|-----------|
| Duplicate detection by ID | Yes | Yes (simple mode) |
| TTL-based dedup | Yes | Yes (throttle mode) |
| Debounce (replace delayed) | Yes | Yes (debounce mode) |
| Remove dedup key manually | Yes | No |
| Extend TTL on duplicate | Yes | No |
| Replace + extend combined | Yes | No |
| Cross-queue independence | No | Yes |
| Dedup hash tracking | No | Yes |

**Minor gap**: We should add tests for manual dedup key removal and TTL extension.

---

## 14. Metrics and Observability Test Patterns

### BullMQ Metrics (metrics.test.ts)

```typescript
describe('Metrics collection', () => {
  // BullMQ: "should gather metrics for completed jobs"
  it('tracks completed job count over time windows');

  // BullMQ: "should only keep metrics for maxDataPoints"
  it('respects maxDataPoints limit for metrics retention');

  // BullMQ: "should gather metrics for failed jobs"
  it('tracks failed job count over time windows');

  // BullMQ: "should get metrics with pagination"
  it('supports pagination for large metric datasets');

  // Additional patterns from Celery/Sidekiq:
  it('processing latency is accurately measured');
  it('queue depth metric reflects actual queue length');
  it('metrics survive worker restart');
});
```

### BullMQ Events (events.test.ts)

```typescript
describe('Event system', () => {
  it('emits waiting event on job add');
  it('emits active event when job starts processing');
  it('emits completed event with return value');
  it('emits failed event with error');
  it('emits drained event when queue empties');
  it('emits duplicated event for dedup hits');
  it('event stream respects maxLen trimming');
  it('multiple QueueEvents instances all receive events');
  it('events contain correct jobId and state');
  it('events fire in correct lifecycle order');
});
```

**Our coverage**: `events.test.ts` and `edge-events.test.ts` cover added, completed, retrying, and stalled events. `metrics.test.ts` exists. Gaps: event ordering guarantees, stream trimming, drained events.

---

## 15. Memory Leak Detection Patterns

### Celery (test_mem_leak_in_exception_handling.py)

This is a unique and valuable test pattern:

```typescript
describe('Memory leak regression', () => {
  // Celery: test_mem_leak_unhandled_exceptions
  it('no memory leak after 500 unhandled exception jobs', async () => {
    const baselineMemory = process.memoryUsage().heapUsed;
    // Process 500 jobs that throw
    // Force GC if available
    const afterMemory = process.memoryUsage().heapUsed;
    const leakMB = (afterMemory - baselineMemory) / 1024 / 1024;
    expect(leakMB).toBeLessThan(10);
  });

  // Celery: test_mem_leak_retry_failures
  it('no memory leak after 100 jobs that retry then fail', async () => {
    // Similar pattern: baseline -> stress -> measure
  });

  // Celery: test_mem_leak_nested_exception_stacks
  it('no memory leak with deeply nested error stacks', async () => {
    // 200 tasks with deep call stacks before throwing
  });
});
```

**Gap**: We have zero memory leak tests. This should be a priority addition.

---

## 16. Concurrency Test Patterns

### BullMQ Concurrency (concurrency.test.ts)

```typescript
describe('Concurrency control', () => {
  // BullMQ: "should run max concurrency for jobs added"
  it('never exceeds configured concurrency');

  // BullMQ: global concurrency
  it('global concurrency limits across all workers');
  it('dynamic global concurrency update takes effect');

  // BullMQ: "emits drained global event only once when worker is idle"
  it('drained event fires once, not per-job');

  // BullMQ: lock expiration + concurrency
  it('expired lock does not stuck jobs in max concurrency state');

  // Celery: autoscale
  it('autoscale increases workers under high load');
  it('autoscale decreases workers when idle');
  it('autoscale respects min/max bounds');
  it('autoscale handles shrink failure gracefully');
});
```

**Our coverage**: `concurrency.test.ts` + edge-advanced has global concurrency tests. Gap: autoscale, dynamic concurrency changes, lock expiration interaction.

---

## 17. Cleanup and Retention Test Patterns

### BullMQ Clean (clean.test.ts)

```typescript
describe('Queue cleanup', () => {
  it('clean empty queue succeeds silently');
  it('clean removes jobs outside grace period');
  it('clean preserves jobs within grace period');
  it('clean respects limit parameter');
  it('clean by state: completed');
  it('clean by state: failed');
  it('clean by state: waiting');
  it('clean by state: delayed');
  it('clean by state: prioritized');
  it('clean job without timestamp handled gracefully');
  it('clean emits correct events');
});
```

### BullMQ Obliterate

```typescript
describe('Queue obliterate', () => {
  it('removes all queue keys and job data');
  it('obliterate with active jobs fails safely');
  it('obliterate with force removes even active jobs');
});
```

**Our coverage**: `edge-advanced.test.ts` covers removeOnComplete/removeOnFail with age and count. `edge-queue.test.ts` has obliterate test. Gap: state-specific cleanup, grace period, and clean event emission.

---

## 18. Performance / Benchmark Test Patterns

### Celery Benchmarks (bench_worker.py)

```typescript
// PATTERN: Throughput benchmark (NOT a unit test, separate file)
describe('Performance benchmarks', { timeout: 60000 }, () => {
  it('measures throughput: jobs/second for simple jobs', async () => {
    const count = 10000;
    const start = Date.now();
    // Add all jobs
    // Wait for all to complete
    const elapsed = Date.now() - start;
    const throughput = count / (elapsed / 1000);
    console.log(`Throughput: ${throughput.toFixed(0)} jobs/sec`);
    expect(throughput).toBeGreaterThan(100); // Baseline assertion
  });

  it('measures latency: p50/p95/p99 for job processing', async () => {
    const latencies: number[] = [];
    // Process 1000 jobs, record each latency
    latencies.sort((a, b) => a - b);
    const p50 = latencies[Math.floor(latencies.length * 0.5)];
    const p95 = latencies[Math.floor(latencies.length * 0.95)];
    const p99 = latencies[Math.floor(latencies.length * 0.99)];
    console.log(`Latency: p50=${p50}ms p95=${p95}ms p99=${p99}ms`);
  });

  it('measures memory: heap growth over 10000 jobs', async () => {
    const baseline = process.memoryUsage().heapUsed;
    // Process 10000 jobs
    const after = process.memoryUsage().heapUsed;
    const growthMB = (after - baseline) / 1024 / 1024;
    console.log(`Memory growth: ${growthMB.toFixed(2)}MB`);
    expect(growthMB).toBeLessThan(50); // No unbounded growth
  });
});
```

### BullMQ Stress Tests (job_scheduler_stress.test.ts)

```typescript
describe('Stress: scheduler under load', () => {
  it('100 concurrent schedulers fire without missing iterations');
  it('rapid upsert/remove cycles do not corrupt state');
});
```

**Gap**: We have no benchmarks or stress tests.

---

## 19. Middleware / Plugin Test Patterns

### Sidekiq Middleware (middleware_test.rb)

```typescript
describe('Middleware chain', () => {
  // Sidekiq: "supports custom middleware"
  it('custom middleware receives job context');

  // Sidekiq: "executes middleware in the proper order"
  it('middleware executes in registration order (onion model)');

  // Sidekiq: "allows middleware to abruptly stop processing rest of chain"
  it('middleware can halt chain by not calling next()');

  // Sidekiq: "allows middleware to yield arguments"
  it('middleware can modify job data before processor');

  // Sidekiq: "correctly replaces middleware"
  it('adding same middleware twice replaces, not duplicates');

  // Sidekiq: "correctly prepends middleware"
  it('prepend places middleware at the beginning of chain');
});
```

**Gap**: If our system supports middleware/hooks, we need these tests.

---

## 20. Priority Gap Analysis: What We Should Add First

### Tier 1: Critical (These catch real bugs in production)

| Test Category | Source | Estimated Tests | Priority |
|---------------|--------|-----------------|----------|
| Stalled job detection & recovery | BullMQ | 12 | P0 |
| Retry exhaustion + dead letter | Sidekiq | 10 | P0 |
| Graceful shutdown with active jobs | Celery + BullMQ | 8 | P0 |
| Connection loss + recovery | BullMQ + Celery | 8 | P0 |
| Memory leak regression | Celery | 3 | P0 |

### Tier 2: Important (Prevent common user-reported issues)

| Test Category | Source | Estimated Tests | Priority |
|---------------|--------|-----------------|----------|
| Flow failure propagation variants | BullMQ | 10 | P1 |
| Pause interaction with delayed/retry | BullMQ | 6 | P1 |
| Rate limit + priority interaction | BullMQ | 4 | P1 |
| Queue inspection API | Sidekiq | 12 | P1 |
| Event ordering guarantees | BullMQ | 6 | P1 |
| Cleanup by state with grace period | BullMQ | 8 | P1 |

### Tier 3: Valuable (Mature system completeness)

| Test Category | Source | Estimated Tests | Priority |
|---------------|--------|-----------------|----------|
| Job cancellation/revocation | Celery + BullMQ | 10 | P2 |
| Worker control commands | Celery | 10 | P2 |
| Throughput benchmarks | Celery | 3 | P2 |
| Scheduler idempotency edge cases | BullMQ | 8 | P2 |
| Autoscale behavior | Celery | 5 | P2 |
| Broker failover smoke tests | Celery | 4 | P2 |

---

## 21. Portable Test Patterns: TypeScript Implementation Templates

### Template: Stalled Job Detection Test

```typescript
import { describe, it, expect, beforeEach, afterEach } from 'vitest';

describe('Stalled job detection', () => {
  let queue: Queue;
  let worker: Worker;

  beforeEach(async () => {
    queue = new Queue('stall-test', { connection: CONNECTION });
    await flushQueue('stall-test');
  });

  afterEach(async () => {
    await worker?.close();
    await queue?.close();
  });

  it('detects stalled job and moves to failed after maxStalledCount', async () => {
    // 1. Add a job
    const job = await queue.add('test', { key: 'value' });

    // 2. Start a worker that "stalls" (never completes)
    worker = new Worker('stall-test', async () => {
      // Simulate stall: block longer than lockDuration
      await new Promise((resolve) => setTimeout(resolve, 30000));
    }, {
      connection: CONNECTION,
      lockDuration: 1000,     // Lock expires after 1s
      stalledInterval: 2000,  // Check for stalls every 2s
      maxStalledCount: 1,     // Fail after 1 stall
    });

    // 3. Wait for stall detection
    const failedJob = await new Promise<Job>((resolve) => {
      worker.on('failed', (job) => resolve(job));
    });

    expect(failedJob.id).toBe(job.id);
    expect(failedJob.failedReason).toContain('stalled');
  });

  it('lock extension prevents false stall detection', async () => {
    const completedIds: string[] = [];

    worker = new Worker('stall-test', async (job) => {
      // Long-running but actively processing (lock auto-extended)
      await new Promise((resolve) => setTimeout(resolve, 3000));
      return 'done';
    }, {
      connection: CONNECTION,
      lockDuration: 1000,
      lockRenewTime: 500,   // Renew every 500ms
      stalledInterval: 2000,
    });

    worker.on('completed', (job) => completedIds.push(job.id));

    await queue.add('test', {});
    await new Promise((resolve) => setTimeout(resolve, 5000));

    expect(completedIds).toHaveLength(1);
  });
});
```

### Template: Memory Leak Regression Test

```typescript
describe('Memory leak regression', { timeout: 120000 }, () => {
  it('no heap growth after 1000 error jobs', async () => {
    const queue = new Queue('memleak-test', { connection: CONNECTION });
    await flushQueue('memleak-test');

    const worker = new Worker('memleak-test', async () => {
      throw new Error('intentional failure');
    }, {
      connection: CONNECTION,
      attempts: 1,
    });

    // Warm up
    for (let i = 0; i < 50; i++) {
      await queue.add('warmup', { i });
    }
    await new Promise((r) => setTimeout(r, 5000));

    // Force GC if available
    if (global.gc) global.gc();
    const baseline = process.memoryUsage().heapUsed;

    // Stress
    for (let i = 0; i < 1000; i++) {
      await queue.add('stress', { i });
    }
    await new Promise((r) => setTimeout(r, 30000));

    if (global.gc) global.gc();
    const after = process.memoryUsage().heapUsed;
    const leakMB = (after - baseline) / 1024 / 1024;

    await worker.close();
    await queue.close();

    expect(leakMB).toBeLessThan(10);
  });
});
```

### Template: Broker Failover Smoke Test

```typescript
describe('Connection recovery', { timeout: 30000 }, () => {
  it('worker recovers and processes jobs after Redis restart', async () => {
    const queue = new Queue('failover-test', { connection: CONNECTION });
    const results: string[] = [];

    const worker = new Worker('failover-test', async (job) => {
      results.push(job.data.phase);
      return true;
    }, { connection: CONNECTION });

    // Phase 1: Normal operation
    await queue.add('test', { phase: 'before' });
    await waitForEvent(worker, 'completed', 5000);
    expect(results).toContain('before');

    // Phase 2: Simulate Redis restart (requires test infrastructure)
    // await restartRedis();

    // Phase 3: Recovery
    // await queue.add('test', { phase: 'after' });
    // await waitForEvent(worker, 'completed', 15000);
    // expect(results).toContain('after');

    await worker.close();
    await queue.close();
  });
});
```

---

## 22. Cross-System Pattern Comparison

### Feature Coverage Across Systems

| Feature | Celery Tests | Sidekiq Tests | BullMQ Tests | Our Tests | Gap |
|---------|:------------:|:-------------:|:------------:|:---------:|:---:|
| Basic job processing | 50+ | 10+ | 50+ | 15+ | Low |
| Retry with backoff | 15+ | 20+ | 10+ | 3 | HIGH |
| Dead letter queue | 5+ | 5+ | 5+ | 0 | HIGH |
| Stalled detection | N/A | N/A | 14 | 1 | HIGH |
| Workflow / chains | 100+ | N/A | 30+ | 10 | Medium |
| Rate limiting | 5+ | N/A | 13 | 6 | Low |
| Delayed / scheduled | 20+ | 6 | 30+ | 6 | Medium |
| Pause / resume | N/A | N/A | 11 | 2 | Medium |
| Cancellation | 12 | N/A | 12 | 0 | HIGH |
| Connection resilience | 10+ | 3 | 8 | 5 | Medium |
| Worker lifecycle | 20+ | 6 | 10+ | 5 | Medium |
| Queue inspection | 20+ | 40+ | 5+ | 3 | HIGH |
| Memory leak | 3 | 0 | 0 | 0 | HIGH |
| Benchmarks | 1+ | 0 | 1+ | 0 | Medium |
| Middleware | N/A | 9 | N/A | 0 | Low* |
| Deduplication | N/A | N/A | 12 | 10 | Low |
| Metrics | N/A | N/A | 4 | exists | Low |
| Events | 5+ | N/A | 16 | 8 | Medium |
| Cluster mode | N/A | 1 | 10 | 8 | Low |

*Low because middleware may not be a feature of our system.

---

## Common Pitfalls

| Pitfall | Why It Happens | How to Avoid |
|---------|---------------|--------------|
| Testing only happy path retries | Retry exhaustion is complex to set up | Add dead letter tests from day 1 |
| No stall detection tests | Stalls need real timing, hard to mock | Use short lockDuration (1s) in tests |
| Missing graceful shutdown tests | Shutdown is async and racy | Test with active jobs in flight |
| No memory leak regression | Requires many iterations, slow | Run as separate CI job with --expose-gc |
| Testing dedup but not across restarts | Dedup keys may not survive | Test dedup with worker restart in between |
| No connection failure tests | Requires Redis control in tests | Use Docker or testcontainers for Redis lifecycle |
| Ignoring event ordering | Events seem to "just work" | Assert strict ordering of lifecycle events |

---

## Further Reading

| Resource | Type | Why Recommended |
|----------|------|-----------------|
| [Celery test suite](https://github.com/celery/celery/tree/main/t) | Source code | Most comprehensive queue test patterns |
| [Celery canvas integration tests](https://github.com/celery/celery/blob/main/t/integration/test_canvas.py) | Source code | 100+ workflow test cases |
| [Celery worker control tests](https://github.com/celery/celery/blob/main/t/unit/worker/test_control.py) | Source code | Rate limits, revocation, pool management |
| [Celery request tests](https://github.com/celery/celery/blob/main/t/unit/worker/test_request.py) | Source code | Timeout, ACK, retry, failure edge cases |
| [Celery memory leak tests](https://github.com/celery/celery/blob/main/t/integration/test_mem_leak_in_exception_handling.py) | Source code | Memory leak regression pattern |
| [Celery broker failover tests](https://github.com/celery/celery/tree/main/t/smoke/tests/failover) | Source code | Broker/worker failover smoke tests |
| [Sidekiq retry tests](https://github.com/sidekiq/sidekiq/blob/main/test/retry_test.rb) | Source code | Most thorough retry/backoff test patterns |
| [Sidekiq API tests](https://github.com/sidekiq/sidekiq/blob/main/test/api_test.rb) | Source code | Queue inspection, job management APIs |
| [Sidekiq dead set tests](https://github.com/sidekiq/sidekiq/blob/main/test/dead_set_test.rb) | Source code | Dead letter queue management |
| [BullMQ stalled jobs tests](https://github.com/taskforcesh/bullmq/blob/master/tests/stalled_jobs.test.ts) | Source code | Directly portable stall detection patterns |
| [BullMQ rate limiter tests](https://github.com/taskforcesh/bullmq/blob/master/tests/rate_limiter.test.ts) | Source code | Rate limit edge cases |
| [BullMQ flow tests](https://github.com/taskforcesh/bullmq/blob/master/tests/flow.test.ts) | Source code | Parent-child dependency patterns |
| [BullMQ connection tests](https://github.com/taskforcesh/bullmq/blob/master/tests/connection.test.ts) | Source code | Connection resilience patterns |
| [BullMQ worker tests](https://github.com/taskforcesh/bullmq/blob/master/tests/worker.test.ts) | Source code | 100+ worker behavior tests |

---

*This guide was synthesized from 28 sources including the Celery, Sidekiq, and BullMQ test suites. See `resources/celery-and-cross-lang-test-cases-sources.json` for full source list with quality scores.*
