# Bull (Legacy) Test Suite - Complete Catalog of Test Cases

**Generated**: 2026-02-14
**Sources**: 22 resources analyzed
**Depth**: medium
**Repository**: https://github.com/OptimalBits/bull (develop branch)

## Prerequisites

- Familiarity with Bull queue library concepts (jobs, queues, workers, Redis)
- Understanding of Mocha test framework (describe/it/before/after hooks)
- Redis running locally on port 6379 for test execution

## TL;DR

- Bull's test suite contains **14 test files** with approximately **200+ individual test cases**
- Tests cover: queue lifecycle, job CRUD, events, pause/resume, repeatable jobs, rate limiting, sandboxed processes, metrics, obliterate, getters, connections, workers, and whenCurrentJobsFinished
- **26 Lua scripts** implement atomic Redis operations that the tests validate
- **15 open bugs** remain, including race conditions in event delivery, infinite loops on Redis failure, and inconsistent event parameters
- Test fixtures include 13 processor files simulating success, failure, crash, slow processing, progress updates, and more

---

## Test Infrastructure

### Test Utilities (test/utils.js)

| Function | Purpose |
|----------|---------|
| `buildQueue(name, opts)` | Creates queue with default Redis config (127.0.0.1:6379), tracks in array |
| `newQueue(name, opts)` | Creates queue and returns its ready promise |
| `cleanupQueue(queue)` | Empties then closes a single queue |
| `cleanupQueues()` | Closes all tracked queues, resets tracking array |
| `simulateDisconnect(queue)` | Disconnects both client and event subscriber for failure testing |
| `sleep(ms)` | Promise-wrapped setTimeout (preserves original setTimeout ref) |

### Mocha Hooks Pattern

- **beforeEach**: Creates fresh Redis client, flushes database (`flushdb`)
- **afterEach**: Restores Sinon sandbox, quits client connection
- Suite-specific hooks create/destroy queue instances per test group

### Test Fixtures (test/fixtures/)

| Fixture File | Purpose |
|-------------|---------|
| `fixture_processor.js` | Standard successful processor |
| `fixture_processor_bar.js` | Named processor "bar" |
| `fixture_processor_broken.js` | Broken/invalid processor file |
| `fixture_processor_callback.js` | Callback-style processor |
| `fixture_processor_callback_fail.js` | Callback-style processor that fails |
| `fixture_processor_crash.js` | Processor that crashes the process |
| `fixture_processor_data.js` | Processor that returns/updates data |
| `fixture_processor_discard.js` | Processor that discards job |
| `fixture_processor_exit.js` | Processor that exits the process |
| `fixture_processor_fail.js` | Processor that throws/rejects |
| `fixture_processor_foo.js` | Named processor "foo" |
| `fixture_processor_progress.js` | Processor that reports progress |
| `fixture_processor_slow.js` | Slow processor for timeout testing |

---

## Test File 1: test_queue.js (Queue Operations)

This is the largest test file, containing the core queue behavior tests.

### 1.1 Queue.close

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | should call end on the client | Closing queue terminates the Redis client connection |
| 2 | should call end on the event subscriber client | Closing queue terminates the event subscriber connection |
| 3 | should resolve the promise when each client has disconnected | close() promise resolves only after all Redis clients disconnect |
| 4 | should return a promise | close() returns a promise (thenable) |
| 5 | should close if the job expires after the lockRenewTime | Queue can close even when a job's lock has expired |
| 6 | should be callable from within > a job handler that takes a callback | close() works inside callback-style job handlers |
| 7 | should be callable from within > a job handler that returns a promise | close() works inside promise-style job handlers |

### 1.2 Instantiation

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | should create a queue with standard redis opts | Queue accepts standard Redis options object |
| 2 | should create a queue with a redis connection string | Queue accepts `redis://` connection string |
| 3 | should create a queue with only a hostname | Queue works with hostname-only config |
| 4 | should create a queue with connection string and password | Queue parses password from connection string |
| 5 | creates a queue using the supplied redis DB | Queue uses specified Redis database number |
| 6 | creates a queue using the supplied redis url as opts | Queue accepts URL in opts parameter |
| 7 | creates a queue using the supplied redis host | Queue connects to specified host |
| 8 | creates a queue with dots in its name | Queue names can contain dots |
| 9 | creates a queue accepting port as a string | Port can be string, not just number |
| 10 | should create a queue with a prefix option | Queue supports key prefix for namespace isolation |
| 11 | should allow reuse redis connections | Queue supports shared Redis connections via createClient |
| 12 | creates a queue with default job options | Default job options applied to all jobs |
| 13 | should not change the options object | Queue constructor does not mutate the passed options |
| 14 | bulk jobs > should default name of job | addBulk uses default job name |
| 15 | bulk jobs > should default options from queue | addBulk inherits queue's default job options |

### 1.3 Worker / Job Processing

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | should process a job | Basic job processing works end-to-end |
| 2 | bulk jobs > should process jobs | addBulk jobs are all processed |
| 3 | process a lifo queue | LIFO option reverses processing order |
| 4 | should processes jobs by priority | Lower priority number = higher priority = processed first |
| 5 | process several jobs serially | Jobs process one at a time with concurrency=1 |
| 6 | process a job that updates progress | job.progress() updates are stored and emitted |
| 7 | process a job that updates progress with an object | Progress can be an object, not just number |
| 8 | process a job that updates progress with an object emits a global event | Progress object emits global:progress event |
| 9 | process a job that returns data in the process handler | Return value stored as job.returnvalue |
| 10 | process a job that returns a string in the process handler | String return values work correctly |
| 11 | process a job that returns data and returnvalue gets stored in database | returnvalue persisted in Redis |
| 12 | process a job that returns a promise | Promise-based processors supported |
| 13 | process a job that returns data in a promise | Promise-resolved data stored as returnvalue |
| 14 | process a synchronous job | Synchronous processor functions work |
| 15 | process stalled jobs when starting a queue | Stalled jobs from previous run are reprocessed on startup |
| 16 | processes jobs that were added before the queue backend started | Jobs added before process() is called are picked up |
| 17 | process a named job that returns a promise | Named processors work with promises |
| 18 | process a two named jobs that returns a promise | Multiple named processors coexist |
| 19 | process all named jobs from one process function | Wildcard `*` processor handles all named jobs |
| 20 | fails job if missing named process | Job fails if no matching named processor registered |
| 21 | should wait for all jobs when closing queue with named processors | close() waits for named processor jobs to finish |
| 22 | processes several stalled jobs when starting several queues | Multiple queue instances coordinate stalled job recovery |
| 23 | does not process a job that is being processed when a new queue starts | Active jobs not double-processed by new queue instances |
| 24 | process stalled jobs without requiring a queue restart | Stalled job detection runs periodically, not just at startup |
| 25 | failed stalled jobs that stall more than allowable stalled limit | Jobs exceeding maxStalledCount are moved to failed |
| 26 | removes failed stalled jobs exceeding limit when removeOnFail present | removeOnFail applies to stalled-then-failed jobs |
| 27 | should clear job from stalled set when job completed | Completed jobs cleaned from stalled tracking |
| 28 | process a job that fails | Failed processor moves job to failed state |
| 29 | process a job that throws an exception | Thrown exceptions captured as job failure |
| 30 | process a job that returns data with a circular dependency | Circular references in return data handled gracefully |
| 31 | process a job that returns a rejected promise | Rejected promises move job to failed |
| 32 | process a job that rejects with a nested error | Nested error objects preserved in failure info |
| 33 | **[SKIP]** does not renew a job lock after the lock has been released [#397] | Skipped: unstable test for lock renewal after release |
| 34 | retry a job that fails | Failed job with attempts>1 retries automatically |
| 35 | retry a job that fails on a paused queue moves the job to paused | Retry on paused queue moves to paused set, not waiting |
| 36 | retry a job that fails using job retry method | job.retry() manually re-queues a failed job |

### 1.4 Auto Job Removal

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | should remove job after completed if removeOnComplete | Completed jobs auto-removed when removeOnComplete=true |
| 2 | should remove a job after completed if default job options specify removeOnComplete | Default job options removeOnComplete works |
| 3 | .retryJobs > should retry all failed jobs | retryJobs() re-queues all failed jobs |
| 4 | .retryJobs > should move to pause all failed jobs if queue is paused | retryJobs() respects paused queue state |
| 5 | should keep specified number of jobs after completed with removeOnComplete | removeOnComplete=N keeps last N completed jobs |
| 6 | should keep of jobs newer than specified after completed with removeOnComplete | removeOnComplete={age} keeps jobs within age window |
| 7 | should keep of jobs newer than specified and up to a count completed with removeOnComplete | removeOnComplete={age,count} combines age and count retention |
| 8 | should keep of jobs newer than specified and up to a count fail with removeOnFail | removeOnFail={age,count} combines age and count retention |
| 9 | should keep specified number of jobs after completed with global removeOnComplete | Global removeOnComplete option works |
| 10 | should remove job after failed if removeOnFail | Failed jobs auto-removed when removeOnFail=true |
| 11 | should remove a job after fail if default job options specify removeOnFail | Default job options removeOnFail works |
| 12 | should keep specified number of jobs after completed with removeOnFail | removeOnFail=N keeps last N failed jobs |
| 13 | should keep specified number of jobs after completed with global removeOnFail | Global removeOnFail option works |

### 1.5 Duplicate / Debounce Detection

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | when job has been added again > emits global duplicated event | Duplicate jobId emits global:duplicated |
| 2 | when job has been added again > emits duplicated event | Duplicate jobId emits local duplicated event |
| 3 | when ttl is provided > used a fixed time period and emits debounced event | Debounce with TTL prevents re-add within window |
| 4 | when removing debounced job > removes debounce key | Removing debounced job clears the debounce key |
| 5 | when ttl is not provided > waits until job is finished before removing debounce key | Without TTL, debounce key persists until job completes |

### 1.6 Delayed Jobs

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | should process a delayed job only after delayed time | Job with delay option not processed before delay expires |
| 2 | should process delayed jobs in correct order | Multiple delayed jobs process in timestamp order |
| 3 | should process delayed jobs in correct order even in case of restart | Order preserved across queue restart |
| 4 | should process delayed jobs with exact same timestamps in correct order (FIFO) | Same-timestamp delayed jobs maintain FIFO order |
| 5 | an unlocked job should not be moved to delayed | Unlocked jobs rejected from delayed transition |
| 6 | an unlocked job should not be moved to waiting | Unlocked jobs rejected from waiting transition |

### 1.7 Concurrency

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | should run job in sequence if concurrency is 1 | Concurrency=1 means strict serial processing |
| 2 | should process job respecting the concurrency set | Concurrency=N allows N simultaneous jobs |
| 3 | should wait for all concurrent processing in case of pause | pause() waits for all concurrent jobs to finish |

### 1.8 Retries and Backoffs

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | should not retry a job if it has been marked as unrecoverable | job.discard() prevents retries |
| 2 | should automatically retry a failed job if attempts > 1 | attempts option enables automatic retry |
| 3 | when job has more priority than delayed jobs > executes retried job first | Retried job with higher priority runs before delayed jobs |
| 4 | should not retry a failed job more than the number of given attempts times | Retry count limited to attempts value |
| 5 | should retry a job after a delay if a fixed backoff is given | backoff:{type:'fixed',delay:N} waits N ms between retries |
| 6 | should retry a job after a delay if an exponential backoff is given | backoff:{type:'exponential',delay:N} doubles delay each retry |
| 7 | should retry a job after a delay if a custom backoff is given | Custom backoff strategy function supported |
| 8 | should pass strategy options to custom backoff | Custom backoff receives strategy options |
| 9 | should not retry a job if the custom backoff returns -1 | Custom backoff returning -1 means immediate failure |
| 10 | should retry a job after delay if custom backoff based on error thrown | Custom backoff can inspect error object |
| 11 | should handle custom backoff if it returns a promise | Async custom backoff strategies supported |
| 12 | should not retry a job that has been removed | Removed jobs skip retry logic |
| 13 | should not retry a job that has been retried already | Double-retry prevention |
| 14 | should not retry a job that is locked | Locked jobs cannot be retried externally |
| 15 | an unlocked job should not be moved to failed | State validation before failed transition |

### 1.9 Drain Queue

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | should count zero after draining the queue | drain() removes all waiting/delayed jobs, count becomes 0 |

### 1.10 Cleaner

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | should reject the cleaner with no grace | clean() requires grace period parameter |
| 2 | should reject the cleaner an unknown type | clean() rejects invalid status type |
| 3 | should clean an empty queue | clean() on empty queue succeeds with no errors |
| 4 | should clean two jobs from the queue | clean() removes multiple completed jobs |
| 5 | should only remove a job outside of the grace period | Jobs within grace period are preserved |
| 6 | should leave a job that was queued before but processed within the grace period | Grace period based on completion time, not queue time |
| 7 | should clean all failed jobs | clean(grace, 'failed') removes failed jobs |
| 8 | should clean all waiting jobs | clean(grace, 'wait') removes waiting jobs |
| 9 | should clean all delayed jobs | clean(grace, 'delayed') removes delayed jobs |
| 10 | should clean the number of jobs requested | clean(grace, status, limit) respects limit |
| 11 | should succeed when limit is higher than actual job count | Limit > count does not error |
| 12 | should clean the number of jobs requested even if first jobs timestamp doesn't match | Limit works correctly with mixed timestamps |
| 13 | shouldn't clean anything if all jobs are in grace period | Grace period correctly protects all jobs |
| 14 | should properly clean jobs from the priority set | Priority queue jobs are cleanable |
| 15 | should clean a job without a timestamp | Jobs missing timestamp field still cleanable |
| 16 | should clean completed jobs outside grace period | Completed status cleanup works |
| 17 | should clean completed jobs outside grace period with limit | Completed cleanup respects limit |
| 18 | should clean completed jobs respecting limit | Limit parameter correctly bounds removal count |
| 19 | should clean failed jobs without leaving any job keys | Full cleanup removes all Redis keys for the job |

---

## Test File 2: test_job.js (Job Lifecycle)

### 2.1 Job.create

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | returns a promise for the job | create() returns promise resolving to Job instance |
| 2 | should not modify input options | create() does not mutate the options object |
| 3 | saves the job in redis | Created job data persisted in Redis hash |
| 4 | should use the custom jobId if one is provided | Custom jobId respected, not auto-generated |
| 5 | should process jobs with custom jobIds | Custom jobId jobs process normally |

### 2.2 Job.createBulk

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | returns a promise for the jobs | createBulk() returns array of Job instances |
| 2 | should not modify input options | createBulk() does not mutate options |
| 3 | saves the first job in redis | First bulk job persisted correctly |
| 4 | saves the second job in redis | Second bulk job persisted correctly |

### 2.3 Priority Queues

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | add 4 jobs with different priorities | Priority ordering maintained in queue |

### 2.4 Job.update

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | should allow updating job data | job.update() modifies stored data |
| 2 | when job was removed > throws an error | Updating removed job throws error |

### 2.5 Job.remove

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | removes the job from redis | job.remove() deletes all job keys from Redis |
| 2 | fails to remove a locked job | Locked (active) jobs cannot be removed |
| 3 | removes any job from active set | Active jobs can be removed from active set |
| 4 | emits removed event | Removal triggers 'removed' event |
| 5 | a successful job should be removable | Completed jobs can be removed |
| 6 | a failed job should be removable | Failed jobs can be removed |

### 2.6 Job.removeFromPattern

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | remove jobs matching pattern | removeJobs(pattern) removes matching job IDs |

### 2.7 Remove on Priority Queues

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | remove a job with jobID 1 and priority 3 and check the new order | Priority queue reorders correctly after removal |
| 2 | add a new job with priority 10 and ID 5 and check order with previous 4 jobs | Priority insertion after removal maintains correct order |

### 2.8 Job.retry

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | emits waiting event | job.retry() emits 'waiting' event |
| 2 | sets retriedOn to a timestamp | job.retry() records retry timestamp |

### 2.9 Locking

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | can take a lock | job.takeLock() acquires Redis lock |
| 2 | take an already taken lock | Taking an already-held lock fails gracefully |
| 3 | can release a lock | job.releaseLock() releases the Redis lock |

### 2.10 Job.progress

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | can set and get progress as number | Numeric progress stored and retrieved |
| 2 | can set and get progress as object | Object progress stored and retrieved |
| 3 | when job was removed > throws an error | Progress update on removed job throws error |

### 2.11 Job.log

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | can log two rows with text | job.log() appends to Redis list, retrievable |
| 2 | when job was removed > throws an error | Logging on removed job throws error |

### 2.12 Job.moveToCompleted

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | marks the job as completed and returns new job | moveToCompleted stores returnvalue and fetches next job |

### 2.13 Job.moveToFailed

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | marks the job as failed | moveToFailed sets failed state |
| 2 | moves the job to wait for retry if attempts are given | Failed with remaining attempts goes to wait |
| 3 | unlocks the job when moving it to delayed | Delayed retry releases the lock |
| 4 | marks the job as failed when attempts made equal to attempts given | All attempts exhausted = permanent failure |
| 5 | moves the job to delayed for retry if attempts given and backoff is non zero | Backoff delay sends job to delayed set |
| 6 | applies stacktrace limit on failure | stackTraceLimit option truncates stored stacktrace |

### 2.14 Job.promote

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | can promote a delayed job to be executed immediately | promote() moves delayed job to waiting |
| 2 | should process a promoted job according to its priority | Promoted job respects priority ordering |
| 3 | should not promote a job that is not delayed | Promoting non-delayed job throws error |
| 4 | should promote delayed job to the right queue if queue is paused | Promoted job goes to paused set if queue paused |

### 2.15 Job Status

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | get job status | job.getState() returns correct state string |

### 2.16 Job.finished

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | should resolve when the job has been completed | finished() resolves on completion |
| 2 | should resolve when the job has been completed and return object | finished() resolves with object returnvalue |
| 3 | should resolve when the job has been delayed and completed and return object | finished() works for delayed-then-completed jobs |
| 4 | should resolve when the job has been completed and return string | finished() resolves with string returnvalue |
| 5 | should resolve when the job has been delayed and completed and return string | finished() works for delayed-then-completed with string |
| 6 | should reject when the job has been failed | finished() rejects on failure |
| 7 | should resolve directly if already processed | finished() on already-completed job resolves immediately |
| 8 | should reject directly if already processed | finished() on already-failed job rejects immediately |

### 2.17 Job.fromJSON

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | should parse JSON data by default | Job data deserialized from JSON |
| 2 | should not parse JSON data if "preventParsingData" option is specified | preventParsingData keeps raw string |

---

## Test File 3: test_events.js (Event System)

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | should emit waiting when a job has been added | 'waiting' event fires with jobId on add() |
| 2 | should emit global:waiting when a job has been added | 'global:waiting' cross-instance event fires |
| 3 | should emit stalled when a job has been stalled | 'stalled' event fires when lock expires |
| 4 | should emit global:stalled when a job has been stalled | 'global:stalled' cross-instance event fires |
| 5 | should emit global:failed when a job has stalled more than allowable times | Permanent stall failure emits global:failed |
| 6 | emits waiting event when a job is added | Confirms waiting event with job data |
| 7 | emits drained and global:drained when all jobs have been processed | 'drained' fires when queue becomes empty |
| 8 | should emit an event when a new message is added to the queue | Generic message event on job add |
| 9 | should emit an event when a new priority message is added to the queue | Priority job add emits event |
| 10 | should emit an event when a job becomes active | 'active' event fires when processing starts |
| 11 | should emit an event if a job fails to extend lock | 'lock-extension-failed' event fires |
| 12 | should listen to global events | Global event listener receives cross-instance events |

---

## Test File 4: test_pause.js (Pause/Resume)

### 4.1 Globally

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | should pause a queue until resumed | pause() stops processing, resume() restarts |
| 2 | should be able to pause a running queue and emit relevant events | Pausing active queue emits 'paused'/'resumed' events |
| 3 | should not processed delayed jobs | Paused queue does not process delayed jobs when they become ready |

### 4.2 Locally

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | should pause the queue locally | Local pause only affects this worker instance |
| 2 | should wait until active jobs are finished before resolving pause | pause() promise waits for active jobs |
| 3 | should pause the queue locally when more than one worker is active | Local pause works with multiple concurrent workers |
| 4 | should wait for blocking job retrieval to complete before pausing | Pause waits for BRPOPLPUSH to return |
| 5 | should not initialize blocking client if not already initialized | Lazy initialization of blocking client |
| 6 | pauses fast when queue is drained | Pause on empty/drained queue resolves quickly |

### 4.3 Locally with doNotWaitActive=true

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | should not wait for active jobs to finish | doNotWaitActive skips waiting for in-progress jobs |
| 2 | should not process new jobs | Paused queue stops accepting new jobs |
| 3 | should not initialize blocking client if not already initialized | Lazy initialization preserved |

---

## Test File 5: test_repeat.js (Repeatable Jobs)

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | should create multiple jobs if they have the same cron pattern | Same cron pattern creates distinct repeatable entries |
| 2 | should add a repeatable job when using startDate and endDate | Date range bounds for repeatable jobs |
| 3 | should get repeatable jobs with different cron pattern | getRepeatableJobs() lists all patterns |
| 4 | should repeat every 2 seconds | `every: 2000` creates job every 2s |
| 5 | should repeat every 2 seconds with startDate in future | Future startDate delays first execution |
| 6 | should repeat every 2 seconds with startDate in past | Past startDate starts immediately |
| 7 | should repeat once a day for 5 days | Daily cron with limit=5 stops after 5 |
| 8 | should repeat 7th day every month at 9:25 | Complex cron expression works |
| 9 | should create two jobs with the same ids | Repeatable jobs with same config share ID |
| 10 | should allow removing a named repeatable job | removeRepeatable(name, opts) works |
| 11 | should be able to remove repeatable jobs by key | removeRepeatableByKey(key) works |
| 12 | should return repeatable job key | Repeatable job key is accessible |
| 13 | should be able to remove repeatable jobs by key that has a jobId | Key-based removal with custom jobId |
| 14 | should allow removing a customId repeatable job | Custom ID repeatable removal |
| 15 | should not re-add a repeatable job after it has been removed | Removal prevents future scheduling |
| 16 | should allow adding a repeatable job after removing it | Re-add after remove creates fresh schedule |
| 17 | should not repeat more than 5 times | limit option caps total executions |
| 18 | should processes delayed jobs by priority | Repeatable jobs respect priority ordering |
| 19 | should use ".every" as a valid interval | `every` ms-based interval alternative to cron |
| 20 | should throw an error when using .cron and .every simultaneously | Mutually exclusive options validated |
| 21 | should emit a waiting event when adding a repeatable job to the waiting list | Repeatable scheduling emits waiting event |
| 22 | should have the right count value | Repeat count tracks number of executions |
| 23 | it should stop repeating after endDate | endDate terminates repeatable schedule |

---

## Test File 6: test_rate_limiter.js (Rate Limiting)

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | should throw exception if missing duration option | Rate limiter requires duration |
| 2 | should throw exception if missing max option | Rate limiter requires max |
| 3 | should obey the rate limit | Processing respects max jobs per duration |
| 4 | **[SKIP]** should obey job priority | Rate limiting with priority (skipped) |
| 5 | should put a job into the delayed queue when limit is hit | Rate-limited jobs moved to delayed set |
| 6 | should not put a job into the delayed queue when discard is true | bounceBack:false discards rate-limited jobs |
| 7 | should rate limit by grouping | Per-group rate limiting via limiter.groupKey |

---

## Test File 7: test_getters.js (Job Getters)

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | should get waiting jobs | getWaiting() returns jobs in wait state |
| 2 | should get paused jobs | getPaused() returns jobs when queue is paused |
| 3 | should get active jobs | getActive() returns currently processing jobs |
| 4 | should get a specific job | getJob(id) returns single job by ID |
| 5 | should get completed jobs | getCompleted() returns finished jobs |
| 6 | should get completed jobs excluding their data | getCompleted() with excludeData option |
| 7 | should get failed jobs | getFailed() returns failed jobs |
| 8 | returns job counts per priority | getCountsPerPriority() groups counts by priority level |
| 9 | when queue is paused > returns job counts per priority | getCountsPerPriority() works on paused queue |
| 10 | fails jobs that exceed their specified timeout | Job timeout causes failure |
| 11 | should return all completed jobs when not setting start/end | Full range retrieval defaults |
| 12 | should return all failed jobs when not setting start/end | Full range retrieval for failed |
| 13 | should return subset of jobs when setting positive range | Positive start/end pagination |
| 14 | should return subset of jobs when setting a negative range | Negative range for tail access |
| 15 | should return subset of jobs when range overflows | Overflow range handled gracefully |
| 16 | should return jobs for multiple types | getJobs(['waiting','active']) multi-type query |

---

## Test File 8: test_child-pool.js (Child Process Pool)

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | should return same child if free | Reuses idle child processes |
| 2 | should return a new child if reused the last free one | Allocates new child when last free is taken |
| 3 | should return a new child if none free | Creates child when pool exhausted |
| 4 | should return a new child if killed | Replaces killed child processes |
| 5 | should return a new child if many retained and none free | Scales pool when all children busy |
| 6 | should return an old child if many retained and one free | Prefers free children over creating new |
| 7 | should return a shared child pool if isSharedChildPool is true | Singleton pool shared across queues |
| 8 | should return a different childPool if isSharedChildPool is false | Separate pools per queue when not shared |
| 9 | should not overwrite the childPool singleton when isSharedChildPool is false | Non-shared pool doesn't affect singleton |

---

## Test File 9: test_connection.js (Redis Connection)

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | should fail if reusing connections with invalid options | Invalid connection reuse detected and rejected |
| 2 | should recover from a connection loss | Queue reconnects after Redis disconnect |
| 3 | should handle jobs added before and after a redis disconnect | Jobs survive connection interruption |
| 4 | should not close external connections | Externally-provided connections not closed by queue |
| 5 | should fail if redis connection fails and does not reconnect | Non-recoverable connection failure reported |
| 6 | should close cleanly if redis connection fails | Clean shutdown even on connection failure |
| 7 | should accept ioredis options on the query string | URL query params parsed as ioredis options |

---

## Test File 10: test_metrics.js (Metrics Collection)

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | should gather metrics for completed jobs | Completed job count tracked per time window |
| 2 | should only keep metrics for "maxDataPoints" | Metrics collection bounded by maxDataPoints |
| 3 | should gather metrics for failed jobs | Failed job count tracked per time window |
| 4 | should get metrics with pagination | getMetrics() supports start/end pagination |

---

## Test File 11: test_obliterate.js (Queue Obliteration)

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | should obliterate an empty queue | obliterate() on empty queue removes all keys |
| 2 | should obliterate a queue with jobs in different statuses | Jobs in all states (wait/active/delayed/completed/failed) removed |
| 3 | should raise exception if queue has active jobs | obliterate() rejects when active jobs exist |
| 4 | should obliterate if queue has active jobs using "force" | obliterate({force:true}) removes even with active jobs |
| 5 | should remove repeatable jobs | obliterate() cleans up repeatable job configs |
| 6 | should remove job logs | obliterate() removes job log entries |
| 7 | should obliterate a queue with high number of jobs in different statuses | Handles large job counts efficiently |

---

## Test File 12: test_sandboxed_process.js (Sandboxed Processors)

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | should process and complete | External file processor completes job |
| 2 | should process with named processor | Named processor from file works |
| 3 | should process with several named processors | Multiple named file processors coexist |
| 4 | should process with concurrent processors | File processors respect concurrency |
| 5 | should reuse process with single processors | Child process reused across jobs |
| 6 | should process and complete using done | Callback-style file processor works |
| 7 | should process and update progress | File processor can call job.progress() |
| 8 | should process and update data | File processor can call job.update() |
| 9 | should process, discard and fail without retry | File processor calling discard() prevents retry |
| 10 | should process and fail | File processor failure captured correctly |
| 11 | should error if processor file is missing | Missing file path produces clear error |
| 12 | should process and fail using callback | Callback-style file processor failure works |
| 13 | should fail if the process crashes | Process crash detected as job failure |
| 14 | should fail if the process exits 0 | Clean exit without completion = failure |
| 15 | should fail if the process exits non-0 | Non-zero exit code = failure |
| 16 | should remove exited process | Exited child process cleaned from pool |
| 17 | should allow the job to complete and then exit on clean | Graceful shutdown waits for job completion |
| 18 | should share child pool across all different queues created | isSharedChildPool shares across queues |
| 19 | should not share childPool if isSharedChildPool isn't specified | Default: separate pools per queue |
| 20 | should fail if the process file is broken | Syntax error in processor file detected |

---

## Test File 13: test_worker.js (Worker Discovery)

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | should get all workers for this queue | getWorkers() returns registered worker list |

---

## Test File 14: test_when_current_jobs_finished.js (Graceful Shutdown)

| # | Test Case | Behavioral Expectation |
|---|-----------|----------------------|
| 1 | should handle queue with no processor | whenCurrentJobsFinished() resolves when no processor registered |
| 2 | should handle queue with no jobs | whenCurrentJobsFinished() resolves immediately when empty |
| 3 | should wait for job to complete | whenCurrentJobsFinished() waits for single active job |
| 4 | should wait for all jobs to complete | whenCurrentJobsFinished() waits for all concurrent active jobs |
| 5 | should wait for job to fail | whenCurrentJobsFinished() waits even for failing jobs |

---

## Lua Scripts (Atomic Operations)

These 26 Lua scripts implement the atomic Redis operations that the test suite validates:

| Script | Keys | Purpose |
|--------|------|---------|
| `addJob-6.lua` | 6 | Atomically adds job, handles priority, delay, deduplication |
| `addLog-2.lua` | 2 | Appends log entry to job's log list |
| `cleanJobsInSet-3.lua` | 3 | Removes jobs from a set respecting grace period and limit |
| `extendLock-2.lua` | 2 | Extends job lock TTL |
| `getCountsPerPriority-4.lua` | 4 | Counts jobs grouped by priority level |
| `isFinished-2.lua` | 2 | Checks if job is in completed or failed set |
| `isJobInList-1.lua` | 1 | Checks if job exists in a Redis list |
| `moveStalledJobsToWait-7.lua` | 7 | Detects stalled jobs, moves to wait or failed |
| `moveToActive-8.lua` | 8 | Atomically moves job from wait to active, acquires lock |
| `moveToDelayed-4.lua` | 4 | Moves active job to delayed set (for retry backoff) |
| `moveToFinished-9.lua` | 9 | Moves job to completed/failed, handles removeOnComplete/Fail |
| `obliterate-2.lua` | 2 | Removes all queue keys from Redis |
| `pause-5.lua` | 5 | Toggles queue pause state, moves jobs between wait/paused |
| `promote-5.lua` | 5 | Moves delayed job to waiting/paused set |
| `releaseLock-1.lua` | 1 | Releases job lock if owned by caller |
| `removeJob-11.lua` | 11 | Removes single job and all associated keys |
| `removeJobs-8.lua` | 8 | Batch removes jobs by pattern |
| `removeRepeatable-2.lua` | 2 | Removes repeatable job configuration |
| `reprocessJob-6.lua` | 6 | Re-queues a job for reprocessing (retry) |
| `retryJob-7.lua` | 7 | Moves failed job back to wait with state validation |
| `retryJobs-5.lua` | 5 | Batch retries all failed jobs |
| `saveStacktrace-1.lua` | 1 | Stores failure stacktrace on job hash |
| `takeLock-1.lua` | 1 | Acquires distributed lock on job |
| `updateData-1.lua` | 1 | Updates job data field |
| `updateDelaySet-6.lua` | 6 | Moves ready delayed jobs to waiting |
| `updateProgress-2.lua` | 2 | Updates job progress field |

---

## Known Bugs and Edge Cases

### Open Bugs (as of 2026-02-14)

| Issue | Title | Category | Impact |
|-------|-------|----------|--------|
| #2198 | Cannot obliterate non-paused queue | Obliterate | Queue must be paused before obliterate |
| #2115 | 'removed' event inconsistent parameter | Events | Job.remove() sends Job object, pattern remove sends job ID string |
| #2005 | Redis Cluster: can't add job to failed queue | Cluster | Failed job transition broken in cluster mode |
| #1927 | Redis cluster mode: 'global:completed' cannot be triggered | Cluster/Events | Global events broken in cluster mode |
| #1833 | Inconsistent delay property | Job State | `job.delay` is 0 during processing, `job.opts.delay` has original value |
| #1758 | Trailing slash in Redis URL causes invalid db number | Connection | URL parsing edge case |
| #1664 | UnhandledPromiseRejection on connection error | Connection | Unhandled rejection crashes process |
| #1602 | Redis Timeout Issue | Connection | Timeout handling insufficient |
| #1389 | Event "failed" fired for every attempt | Events | Local 'failed' fires per attempt, global fires only on final failure |
| #1381 | moveToCompleted throwing errors | Job Lifecycle | State transition errors during completion |
| #1371 | Job#finished never resolves when job is removed | Job Lifecycle | PRIO1: finished() promise leaks when job removed with removeOnComplete |
| #1344 | queue.isReady() throws if client not initialized | Connection | Race condition on early isReady() call |
| #1304 | Possible infinite loop if BRPOPLPUSH fails | Processing | OOM error causes infinite retry loop (fixed in PR #1674) |
| #1116 | Rate Limiter per queue wrong duration | Rate Limiting | Duration calculation incorrect |
| #787 | Stalled jobs aren't removed by removeRepeatable | Stalled/Repeat | Stalled repeatable jobs persist after removeRepeatable |

### Skipped Tests

| Test | Reason | Reference |
|------|--------|-----------|
| does not renew a job lock after the lock has been released | Unstable and difficult to understand | #397 |
| should obey job priority (rate limiter) | Not implemented / flaky | N/A |

### Race Conditions Tested

1. **Stalled job detection vs job completion**: Test verifies completed jobs clear stalled set
2. **Multiple queue instances starting**: Test verifies no double-processing of active jobs
3. **Pause during concurrent processing**: Test verifies all concurrent jobs finish before pause resolves
4. **Connection loss during processing**: Test verifies jobs added before/after disconnect are handled
5. **Delayed job ordering with same timestamp**: Test verifies FIFO ordering as tiebreaker
6. **Retry on paused queue**: Test verifies retried jobs go to paused set, not waiting
7. **Lock expiry during processing**: Test verifies job expires gracefully after lockRenewTime

### Behavioral Contracts from PATTERNS.md

1. **Connection Reuse**: bclient connections MUST NOT be reused; createClient must return new instance for bclient
2. **Lock Duration**: Default 30 seconds; jobs exceeding this are marked stalled
3. **Stalled Recovery**: maxStalledCount (default 1) determines how many times a job can stall before failing
4. **Custom Backoff**: Function receives (attemptsMade, err) and returns ms delay, 0 for immediate, or -1 for fail
5. **Repeatable Scheduling**: Jobs scheduled "on the hour" -- first execution aligns to time boundaries
6. **Redis Cluster**: Requires hash tag prefix (e.g., `{myprefix}`) for atomic Lua operations

### Behavioral Contracts from REFERENCE.md

1. **pause(isLocal=false)**: Global pause stops all workers across instances; local pause only affects current instance
2. **pause()**: Active jobs continue to completion; only new job fetching is stopped
3. **close()**: Waits for active jobs by default; pass `true` to force immediate close
4. **removeOnComplete/removeOnFail**: Accepts `true` (remove all), number (keep N), or `{age, count}` object
5. **Global events**: Pass job ID instead of Job instance (unlike local events)
6. **Job.finished()**: Relies on pub/sub events; will never resolve if job is removed (known bug #1371)
7. **process()**: Can only be called once per queue instance (or once per named processor)
8. **empty()**: Only removes waiting and delayed jobs, NOT active/completed/failed

---

## Test Case Summary Statistics

| Test File | Test Count | Category |
|-----------|-----------|----------|
| test_queue.js | ~100 | Queue operations, processing, retries, cleaning |
| test_job.js | ~42 | Job CRUD, state transitions, locking |
| test_events.js | 12 | Event emission and global events |
| test_pause.js | 12 | Global/local pause and resume |
| test_repeat.js | 23 | Repeatable/cron job scheduling |
| test_rate_limiter.js | 7 | Rate limiting and grouping |
| test_getters.js | 16 | Job retrieval and pagination |
| test_child-pool.js | 9 | Child process pool management |
| test_connection.js | 7 | Redis connection lifecycle |
| test_metrics.js | 4 | Metrics collection and pagination |
| test_obliterate.js | 7 | Queue obliteration |
| test_sandboxed_process.js | 20 | Sandboxed/file-based processors |
| test_worker.js | 1 | Worker discovery |
| test_when_current_jobs_finished.js | 5 | Graceful shutdown |
| **TOTAL** | **~265** | |

---

*This guide was synthesized from 22 sources including the Bull GitHub repository test files, PATTERNS.md, REFERENCE.md, CHANGELOG.md, and GitHub issue tracker. See `resources/bull-test-cases-sources.json` for full source list.*
