# Learning Guide: Bee-Queue and Other Node.js Queue Library Test Suites

**Generated**: 2026-02-14
**Sources**: 25 resources analyzed (direct repository test files)
**Depth**: medium

## Prerequisites

- Familiarity with Node.js queue/job libraries (Bee-Queue, node-resque, RedisSMQ, Agenda)
- Understanding of Redis data structures (lists, sets, sorted sets, pub/sub)
- Knowledge of testing patterns (Mocha/Jest/Vitest describe/it blocks)

## TL;DR

- Bee-Queue has ~120 test cases across 5 files covering queue lifecycle, job CRUD, concurrent processing, stall detection, backoff strategies, pub/sub events, and delay scheduling
- node-resque has ~80 test cases covering queue operations, worker lifecycle, scheduler leader election, plugin systems (retry, locks, noop), and multi-worker scaling
- RedisSMQ has 100+ tests across 25 directories covering message consumption, exchanges, priority queuing, rate limiting, dead-letter queues, health checks, and multi-queue consumers
- Agenda has ~150 test cases covering job scheduling (every/schedule/now), concurrency, locking, priority, backoff strategies, debouncing, decorators, fork mode, and notification channels
- Common patterns across ALL libraries: connection lifecycle, job creation/removal, retry with backoff, stall/timeout detection, concurrent processing limits, event emission, and graceful shutdown

---

## 1. Bee-Queue Test Suite (Complete Catalog)

**Repository**: https://github.com/bee-queue/bee-queue/tree/master/test
**Test files**: 5 files, ~120 test cases
**Framework**: Mocha

### 1.1 queue-test.js (~95 tests)

#### Connection / Close Tests
| Test Case | Behavior Verified |
|-----------|-------------------|
| closes redis clients | Redis client, bclient, eclient properly closed |
| supports callbacks on close | Callback API works for close() |
| can be called multiple times | Idempotent close - no error on double-close |
| callback on double close | Callback still fires on second close |
| quit error during close | Quit errors propagated properly |
| recovery after startup failure | Close works even if startup never completed |
| stops processing despite retry strategy | Redis retry strategy does not prevent close |
| graceful shutdown completes in-progress jobs | Jobs finish before close resolves |
| no new jobs during shutdown | New jobs rejected during graceful shutdown |
| internal timers cancelled | Stall check timer stops on close |
| timeout when jobs do not finish | Close rejects after timeout if jobs hang |
| job failure does not cause timeout error | Failed jobs do not interfere with close timeout |
| error emission when jobs complete after timeout | Late-finishing jobs emit errors |
| normal close emits no errors | Clean shutdown produces no error events |
| compatibility with checkStalledJobs | Close works alongside stall checking |
| quitCommandClient=true forces quit | Command client terminated when flag set |
| quitCommandClient=false preserves client | Command client kept alive when flag false |

#### Connection Recovery Tests
| Test Case | Behavior Verified |
|-----------|-------------------|
| recovers after disconnection and retry | Queue resumes after connection drop |
| backoff retry for brpoplpush failures | Exponential backoff on blocking pop failures |

#### Constructor Tests
| Test Case | Behavior Verified |
|-----------|-------------------|
| default redis settings | Connects with localhost:6379 defaults |
| custom redis settings (host, port, db) | Custom connection parameters applied |
| isWorker=false prevents bclient | No blocking client when not a worker |
| existing redis instance passed | Reuses external redis connection |
| connecting redis instance support | Accepts in-progress connection |
| autoConnect=true connects automatically | Immediate connection on construction |
| autoConnect=false defers connection | No connection until explicit connect() |

#### Job Add/Remove Tests
| Test Case | Behavior Verified |
|-----------|-------------------|
| correct redis prefix on job creation | Keys use configured prefix |
| job removal from queue | Job removed from Redis |
| removal of jobs not in local cache | Works for jobs from other queue instances |
| removal of non-existent jobs | No error on missing job removal |

#### Health Check Tests
| Test Case | Behavior Verified |
|-----------|-------------------|
| reports waiting jobs count | Accurate count of waiting jobs |
| reports active jobs count | Accurate count of active/processing jobs |
| reports succeeded jobs count | Accurate count of completed jobs |
| reports failed jobs count | Accurate count of failed jobs |
| retried job status transitions | Retry counts reflected in health |
| custom job IDs in health checks | Custom IDs do not inflate newest job count |
| callback support for health checks | Callback API works |

#### getJobs Tests
| Test Case | Behavior Verified |
|-----------|-------------------|
| retrieves waiting jobs with pagination | start/end params for list types |
| retrieves active jobs during processing | Active jobs returned mid-process |
| retrieves delayed jobs | Delayed jobs with future timestamps |
| retrieves failed jobs from failure set | Failed jobs accessible |
| retrieves successful jobs | Completed jobs accessible |
| set type scanning with large counts | SSCAN for large sets |
| validates start/end for list/sorted set | Parameter validation |
| validates size for set types | Parameter validation |
| rejects invalid queue type names | Error on unknown type |
| callback-based API | Callback support |
| returns stored job instances | Uses cached jobs when available |
| creates new instances when storeJobs=false | Fresh instances from Redis |

#### getJob Tests
| Test Case | Behavior Verified |
|-----------|-------------------|
| retrieves by ID from same instance | Local cache lookup |
| returns null for non-existent IDs | Null on missing |
| retrieves jobs from different instances | Cross-queue job lookup |
| custom-ID jobs retrieval | Non-numeric IDs work |
| callback-based API | Callback support |

#### saveAll Tests
| Test Case | Behavior Verified |
|-----------|-------------------|
| saves multiple jobs in batch | Batch save efficiency |
| circular reference errors captured | Internal error handling |
| redis script errors captured | NOSCRIPT error handling |
| rejection on batch execution failure | Promise rejection on failure |

#### removeJob Tests
| Test Case | Behavior Verified |
|-----------|-------------------|
| no error on immediate removal | Race condition safety |
| callback-based API | Callback support |

#### Processing Tests
| Test Case | Behavior Verified |
|-----------|-------------------|
| processes single job with return value | Basic job execution and result |
| processes job with non-numeric ID | String/custom ID processing |
| removeOnSuccess removes completed jobs | Auto-cleanup on success |
| removeOnFailure removes failed jobs | Auto-cleanup on failure |
| failed job tracking and error capture | Error stored on job |
| exception handling in processors | Thrown exceptions caught |
| error stack traces captured and stored | Stack trace persistence |
| job retry mechanisms | Auto-retry on failure |
| job timeout detection | Timeout kills hung jobs |
| auto-retry with configurable attempts | Retry count respected |
| retry prevention when retries=0 | No retry on zero config |
| timeout with auto-retry combination | Timeout + retry interaction |
| rejects processing when isWorker=false | Non-worker cannot process |
| rejects multiple process() calls | Double process() throws |
| rejects processing after close | Closed queue cannot process |

#### Processing Many Jobs Tests
| Test Case | Behavior Verified |
|-----------|-------------------|
| sequential processing with single processor | One-at-a-time execution |
| concurrent processing respects limit | Concurrency cap enforced |
| randomly-offset job creation with concurrency | Order-independent processing |
| multi-queue processing without duplication | No double-processing across queues |

#### Backoff Strategy Tests
| Test Case | Behavior Verified |
|-----------|-------------------|
| validates backoff strategy names | Invalid strategy name rejected |
| delay must be positive integer | Negative/non-integer rejected |
| custom backoff strategy registration | User-defined strategy works |
| fixed backoff delay timing | Constant delay between retries |
| immediate backoff (no delay) | Zero-delay retry |
| exponential backoff increases delays | Doubling delay pattern |

#### Stall Detection / Reset Tests
| Test Case | Behavior Verified |
|-----------|-------------------|
| detects and reprocesses stalled jobs on startup | Stall recovery at boot |
| handles large counts (>1000) stalled jobs | Unpacking limit for mass stalls |
| reset from multiple stalled queues | Multi-queue stall recovery |
| concurrent processor stall recovery | Stall detection with concurrency |
| periodic stall checking with intervals | Interval-based stall monitor |
| stall check callbacks work | Callback API for checkStalledJobs |
| error emission from stall checks | Errors emitted on stall check failure |

#### Startup Tests
| Test Case | Behavior Verified |
|-----------|-------------------|
| processes pre-existing jobs after restart | Jobs from before crash processed |
| prevents double-processing of in-progress jobs | No re-execution of active jobs |

#### Pub/Sub Event Tests
| Test Case | Behavior Verified |
|-----------|-------------------|
| succeeded event with result value | Event carries job result |
| succeeded event with undefined (no result) | Event fires even without return |
| failed event with error details | Error propagated via event |
| progress event during processing | Mid-job progress updates |
| retrying event with original error | Retry reason communicated |
| getEvents=false prevents reception | Event subscription disabled |
| sendEvents=false prevents transmission | Event publishing disabled |
| event emission with multiple jobs | Events fire for each job |

#### Destroy Tests
| Test Case | Behavior Verified |
|-----------|-------------------|
| removes all associated redis keys | Full cleanup of queue data |
| rejects destruction after close | Cannot destroy closed queue |
| callback-based API | Callback support |

#### Initialization Tests
| Test Case | Behavior Verified |
|-----------|-------------------|
| initializes without ensuring scripts | Lazy script loading |
| ready callback support | Callback fires when ready |
| running state before/after ready and close | isRunning reflects lifecycle |

### 1.2 job-test.js (~18 tests)

| Test Case | Behavior Verified |
|-----------|-------------------|
| creates a job | Job has id and data properties |
| creates a job without data | Defaults to empty object |
| save with callback | Callback-style persistence |
| sets retries via chaining | `.retries(2)` assigns value |
| rejects invalid retries count | Negative retries throw error |
| rejects invalid delay timestamps | null, NaN, strings, negatives rejected |
| does not save delay to past date | Past timestamps ignored |
| sets timeout via chaining | `.timeout(5000)` assigns value |
| rejects invalid timeout | Negative timeout throws error |
| saves job in redis | Persistence and retrieval roundtrip |
| requires a progress value | `reportProgress()` without args throws |
| supports progress data object | Data object emitted via progress event |
| progress callback support | Callback API for progress |
| removes job from redis | Deletion returns job instance |
| remove with callback | Callback-style removal |
| retry callback support | Callback API for retry |
| isInSet callback support | Set membership check with callback |
| fromId callback support | Static retrieval by ID with callback |

### 1.3 delay-test.js (5 tests)

| Test Case | Behavior Verified |
|-----------|-------------------|
| processes delayed jobs | Job not processed until delay expires |
| processes two proximal delayed jobs | Jobs with similar delays batched together |
| processes a distant delayed job | Far-future delays handled via timer scheduling |
| processes delayed jobs from other workers | Non-worker queue creates, worker processes |
| processes delayed job first time created | Duplicate-ID job uses original delay |

### 1.4 eager-timer-test.js (17 tests)

| Test Case | Behavior Verified |
|-----------|-------------------|
| trigger in the future | Timer fires at scheduled time within max |
| not trigger after maximum delay | Caps at max delay (500ms) |
| trigger again | Reschedules and triggers multiple times |
| overwrite a later timer | Earlier time replaces later scheduled |
| overwrite later timer after delay | Earlier overrides even after partial elapsed |
| not overwrite an earlier timer | Later time does not replace earlier |
| not overwrite earlier with max delay | Max delay does not override earlier |
| trigger after max delay for negative times | Negative timestamps use max delay |
| trigger after max delay on null | Null defaults to max delay |
| trigger after max delay on undefined | Undefined defaults to max delay |
| trigger after max delay on NaN | NaN defaults to max delay |
| not overwrite earlier on null | Null does not replace active timer |
| trigger immediately for past timestamps | Past times fire immediately |
| not trigger after stopping | Stop prevents future triggers |
| not schedule immediately after stopping | Stopped rejects immediate schedule |
| not schedule later after stopping | Stopped rejects all scheduling |
| fail on invalid maximum delays | Negative, non-integer, infinite rejected |

### 1.5 helpers-test.js (~8 tests)

**Suite: finallyRejectsWithInitial**

| Test Case | Behavior Verified |
|-----------|-------------------|
| invokes function on resolve | Callback executes on promise resolve |
| invokes function on reject | Callback executes on promise reject |
| invokes function on delayed resolve | Callback executes on delayed promise |
| prefers original error (resolved+reject) | Callback rejection used when promise resolves |
| prefers original error (reject+reject) | Original promise error takes precedence |
| produces original value | Resolved value preserved regardless of callback |
| handles synchronous exceptions (resolve) | Sync throw propagated on resolve |
| handles synchronous exceptions (reject) | Original error prioritized over sync throw |
| waits for returned Promise | Callback's promise awaited before resolving |

---

## 2. node-resque Test Suite (Complete Catalog)

**Repository**: https://github.com/actionhero/node-resque/tree/main/__tests__
**Test files**: 12 files across 4 directories, ~80 test cases
**Framework**: Jest

### 2.1 core/connection.ts (11 tests)

| Test Case | Behavior Verified |
|-----------|-------------------|
| starts with no redis keys in namespace | Clean namespace verification |
| has loaded Lua commands | popAndStoreJob loaded as function |
| getKeys returns matches by pattern | Wildcard pattern filtering |
| keys with default namespace are correct | Default key format consistency |
| ioredis transparent key prefix | Prefix applied to stored/retrieved keys |
| keys with custom namespace correct | Custom namespace key generation |
| keys with array namespace correct | Array namespaces join with colons |
| dynamic namespace string construction | Runtime namespace building |
| selects redis db from options | Database selection parameter |
| removes empty namespace from key | No extra prefix for empty namespace |
| removes redis event listeners on end | Proper event listener cleanup |

### 2.2 core/connectionError.ts (1 test)

| Test Case | Behavior Verified |
|-----------|-------------------|
| error on failed connection | ENOTFOUND/ETIMEDOUT/ECONNREFUSED/EAI_AGAIN emitted |

### 2.3 core/queue.ts (40 tests)

#### Enqueueing
| Test Case | Behavior Verified |
|-----------|-------------------|
| add a normal job | Basic enqueue with class name and args |
| add delayed job (enqueueAt) | Schedule at specific timestamp |
| delayed job with string timestamp | String-to-number conversion |
| no duplicate delayed job at same time (error) | Duplicate prevention with error |
| no duplicate delayed job (error suppressed) | Duplicate prevention silent |
| add delayed job (enqueueIn) | Schedule relative to now |
| delayed job with string duration | String duration conversion |

#### Queue Inspection
| Test Case | Behavior Verified |
|-----------|-------------------|
| get number of jobs enqueued | Queue length reporting |
| get jobs in queue | Job retrieval with correct args |
| find previously scheduled jobs | Delayed job lookup |
| no match for different args | Search specificity |

#### Job Deletion
| Test Case | Behavior Verified |
|-----------|-------------------|
| delete enqueued job | Specific job removal |
| delete all jobs of a function/class | Bulk deletion by type |
| delete a delayed job | Scheduled job removal |
| delete delayed job leaves empty queue | Clean removal, no orphans |

#### Argument Handling
| Test Case | Behavior Verified |
|-----------|-------------------|
| single arguments without array | Auto-wrapping in array |
| omitting arguments on enqueue | No-arg job creation |
| omitting arguments on delete | No-arg job deletion |
| omitting arguments on delayed add | No-arg delayed job |
| omitting arguments on delayed delete | No-arg delayed deletion |

#### Queue Metadata
| Test Case | Behavior Verified |
|-----------|-------------------|
| determine leader | Leader identification from Redis |
| load stats | Processed/failed count retrieval |

#### Lock Management
| Test Case | Behavior Verified |
|-----------|-------------------|
| get locks | Retrieval of all active locks |
| remove locks | Targeted lock deletion |

#### Failed Job Management
| Test Case | Behavior Verified |
|-----------|-------------------|
| list failed job count | Failed queue length |
| get failed job body content | Detailed failure data |
| remove failed job by payload | Payload-based removal |
| re-enqueue from failed queue | Retry from failure queue |
| error on retry of non-failed job | Invalid retry handling |

#### Delayed Status
| Test Case | Behavior Verified |
|-----------|-------------------|
| list scheduled timestamps | All delay timestamps |
| list jobs at timestamp | Per-timestamp job listing |
| hash with all delayed tasks | Comprehensive delayed retrieval |

#### Worker Status
| Test Case | Behavior Verified |
|-----------|-------------------|
| list running workers | Active worker identification |
| idle worker status | Worker status when waiting |
| active worker job tracking | Worker status when processing |
| remove stuck workers, re-enqueue jobs | Stuck worker cleanup + job recovery |
| do not remove young stuck jobs | Time-limit protection |
| forceClean with error payload | Forced termination + error record |
| forceClean removes all redis keys | Complete cleanup |
| retryStuckJobs | Stuck job retry mechanism |

### 2.4 core/worker.ts (12 tests)

| Test Case | Behavior Verified |
|-----------|-------------------|
| can connect | Worker connection lifecycle |
| run successful job (inline) | Synchronous job returns correct result |
| run successful async job (inline) | Async job completion |
| run failing job (inline) | Error properly caught and re-thrown |
| plugin afterPerform on error | Plugin hooks fire on failure |
| boot and stop | Worker lifecycle with connection |
| determine proper queue names | Auto-discovery of queues |
| notice new queues with queues=* | Wildcard dynamic queue detection |
| mark job as failed | Failure event with metadata |
| work job with successful result | Success event with result + duration |
| accept simple function jobs | Inline function definitions |
| not work undefined jobs | Error for missing job class |
| place failed jobs in failed queue | Failed data persists to Redis |
| ping during slow job | Heartbeat continues during processing |
| error emit on ping failure during shutdown | No unhandled rejection on close |

### 2.5 core/scheduler.ts (8 tests)

| Test Case | Behavior Verified |
|-----------|-------------------|
| can connect | Scheduler connection lifecycle |
| only one leader, failover works | Leader election + automatic failover |
| start and stop | Scheduler lifecycle |
| error emit on poll failure | No unhandled rejection, error event |
| queues see leader | Leader identity queryable |
| move enqueued jobs when time comes | Past-due scheduled jobs promoted |
| do not move future jobs | Future jobs stay in scheduler |
| remove stuck workers, fail their jobs | Dead worker detection + job failure |

### 2.6 core/multiWorker.ts (4 tests)

| Test Case | Behavior Verified |
|-----------|-------------------|
| never less than one worker | Minimum worker count guarantee |
| stop adding at max when CPU low | Max worker cap with I/O jobs |
| do not add workers when CPU high | CPU-aware scaling restraint |
| pass worker emits to multiWorker | Event propagation from children |

### 2.7 plugins/retry.ts (7 tests)

| Test Case | Behavior Verified |
|-----------|-------------------|
| non-crashing jobs work fine | No false failures |
| retry n times before failing | Retry limit honored, then failure queue |
| custom retry count | Configurable retry threshold |
| custom retry times (delay arrays) | Per-attempt backoff delays |
| failed job re-enqueued not failure queue | Re-enqueue during retry window |
| stats tracked for failing jobs | Global + worker failure stats |
| retry counter and metadata stored | Redis stores attempt count + failure data |

### 2.8 plugins/jobLock.ts (5 tests)

| Test Case | Behavior Verified |
|-----------|-------------------|
| different arg objects not locked | Distinct args run in parallel |
| custom lock key function | User-defined lock key generation |
| same args serialized | Duplicate args jobs wait |
| reEnqueue=false skips re-enqueue | Blocked job discarded when configured |
| different args run concurrently | No lock across different args |

### 2.9 plugins/queueLock.ts (6 tests)

| Test Case | Behavior Verified |
|-----------|-------------------|
| no duplicate same-args in queue | Queue-level dedup |
| different args both enqueued | No false dedup |
| lock TTL configuration | Timeout applies to queue lock |
| stuck job re-enqueue after TTL | Lock expiry allows re-enqueue |
| lock removed after job worked | Cleanup on completion |
| lock removed when plugin blocks job | Cleanup even when skipped |

### 2.10 plugins/delayedQueueLock.ts (2 tests)

| Test Case | Behavior Verified |
|-----------|-------------------|
| no duplicate same-args in delayed queue | Delayed queue dedup |
| different args both enqueued | No false dedup |

### 2.11 plugins/noop.ts (2 tests)

| Test Case | Behavior Verified |
|-----------|-------------------|
| non-crashing jobs work fine | No false failures logged |
| failed jobs prevented from failure queue | Errors logged but not queued |

### 2.12 plugins/custom_plugins.ts (1 test)

| Test Case | Behavior Verified |
|-----------|-------------------|
| custom plugin outside plugins directory | External plugin intercepts job |

### 2.13 integration/ (2 files, 12 tests)

Both ioredis.ts and ioredis-mock.ts test identical flows:

| Test Case | Behavior Verified |
|-----------|-------------------|
| queue can be created | Queue instantiation with connection |
| scheduler can be created | Scheduler instantiation |
| worker can be created | Worker instantiation |
| job can be enqueued | Delayed job scheduling |
| scheduler can promote job | Scheduled to active promotion |
| worker can work job | End-to-end processing with result |

---

## 3. RedisSMQ Test Suite (Catalog)

**Repository**: https://github.com/weyoss/redis-smq/tree/master/packages/redis-smq/tests
**Test directories**: 25 directories, 100+ test files
**Framework**: Vitest

### 3.1 consuming-messages/ (~32 tests)

| Test # | Test Case | Behavior Verified |
|--------|-----------|-------------------|
| 01 | countMessagesByStatus | Accurate counts for pending/ack/dead-letter/scheduled across queue types |
| 02 | Produce and consume 1 message | Basic end-to-end single message flow |
| 03 | Produce and consume 100 messages: LIFO | Last-in-first-out ordering verified |
| 04 | Message dead-lettered when TTL exceeded | Expired message goes to dead-letter, not consumer |
| 05 | Default TTL from configuration | Config-level TTL applies to all messages |
| 06 | Unacknowledged when consumeTimeout exceeded | Timeout triggers re-delivery |
| 07 | Unacknowledged re-queued when retryThreshold not exceeded | Auto-retry on handler error |
| 08 | Async exceptions caught during consumption | Async callback errors handled |
| 09 | Dead-lettered when retryThreshold exceeded | Max retries sends to dead-letter |
| 10 | Message not lost on consumer crash | Crash recovery + re-queue |
| 11 | Single delivery with many consumers | Competing consumer pattern - exactly-once |
| 12 | Retry delay honored | Configurable delay between retries |
| 13 | Recovery across multiple queues after crash | Multi-queue crash recovery |
| 14 | Single producer, multiple queue consumption | Queue name normalization (case) |
| 15 | Single consumer, multiple queues: case 1 | Multi-queue subscription + cancel + re-subscribe |
| 16 | Single consumer, multiple queues: case 2 | Different handler behaviors per queue |
| 17 | Message audit: enabled=false | Audit access properly rejected |
| 19 | Dead-lettered when retryThreshold=0 | Zero retries sends immediately to dead-letter |
| 20 | Producing without exchange | MessageExchangeRequiredError thrown |
| 25 | Message audit: ack=false, deadLetter=true | Selective audit configuration |

### 3.2 exchanges/ (3 tests)

| Test Case | Behavior Verified |
|-----------|-------------------|
| Direct exchange: bind/match/unbind | Routing key binding, matching, removal |
| Namespace isolation | Cross-namespace binding rejected |
| Routing key validation | Invalid keys rejected, valid accepted |
| Idempotent binding | Duplicate bind is no-op |
| Deletion constraints | Cannot delete exchange with bound queues |
| getRoutingKeys / getRoutingKeyQueues | Query APIs for exchange topology |

### 3.3 priority-queuing/ (2 tests)

| Test Case | Behavior Verified |
|-----------|-------------------|
| Priority queue consumer execution | Priority queue with point-to-point delivery |
| Priority queue message ordering | Higher priority consumed first |

### 3.4 queue-rate-limit/ (5 tests)

| Test Case | Behavior Verified |
|-----------|-------------------|
| Set/Get/Clear rate limit | Full CRUD lifecycle |
| Error on non-existent queue | QueueNotFoundError |
| Invalid limit value (0) | InvalidRateLimitValueError |
| Invalid interval value (0) | InvalidRateLimitIntervalError |
| Rate limit enforcement | Message throughput capped |

### 3.5 queue-dead-lettered-messages/ (1 test)

| Test Case | Behavior Verified |
|-----------|-------------------|
| Dead-letter queue retrieval | Messages accessible after dead-lettering |

### 3.6 health-check/ (6 tests)

| Test Case | Behavior Verified |
|-----------|-------------------|
| Producer lifecycle events | goingUp/up/goingDown/down sequence |
| Consumer lifecycle events | Same lifecycle sequence for consumers |
| Queue health metrics | Message counts, consumer status |

### 3.7 Other Test Directories (Summary)

| Directory | Focus Area |
|-----------|-----------|
| consume-message-worker | Worker-level consumption patterns |
| deleting-messages | Message deletion and cleanup |
| event-bus | Event emission and subscription |
| message-status | Status transitions and queries |
| misc | Edge cases and miscellaneous |
| namespace | Namespace isolation and management |
| producer | Producer lifecycle and validation |
| pub-sub-delivery | Pub/sub message delivery model |
| publishing-using-exchanges | Exchange-based publishing |
| purging-queues | Queue purge operations |
| queue-acknowledged-messages | Acknowledged message storage |
| queue-message-storage-list | List-based message storage |
| queue-message-storage-set | Set-based message storage |
| queue-message-storage-sorted-set | Sorted-set message storage |
| queue-pending-messages | Pending message management |
| queue-scheduled-messages | Scheduled message handling |
| redis-connection-pool | Connection pooling |
| requeuing-messages | Message requeue operations |
| workers | Worker management and scaling |

---

## 4. Agenda Test Suite (Catalog)

**Repository**: https://github.com/agenda/agenda/tree/main/packages/agenda/test
**Test files**: 5 test files + 12 shared suite files, ~150 test cases
**Framework**: Vitest

### 4.1 unit.test.ts (~80 tests)

#### Configuration
| Test Case | Behavior Verified |
|-----------|-------------------|
| default processEvery (5000ms) | Default polling interval |
| processEvery from string ("3 minutes") | String time parsing |
| processEvery from number | Numeric interval assignment |
| set name | Agenda instance naming |
| set maxConcurrency | Global concurrency limit |
| set defaultConcurrency | Per-job default concurrency |
| set lockLimit | Global lock ceiling |
| set defaultLockLimit | Per-job default lock limit |
| set defaultLockLifetime | Lock expiry duration |
| method chaining (all config methods) | Fluent API returns self |

#### Define
| Test Case | Behavior Verified |
|-----------|-------------------|
| stores definition with options | Definition persistence |
| sets default concurrency | Inherited defaults |
| sets default lockLimit | Inherited defaults |
| inherits defaultLockLifetime | Inherited defaults |

#### Job Creation
| Test Case | Behavior Verified |
|-----------|-------------------|
| creates Job instance | Factory method works |
| creates job with data | Data payload assignment |
| sets type to "normal" | Default type classification |
| sets agenda reference | Back-reference maintained |

#### Job Methods
| Test Case | Behavior Verified |
|-----------|-------------------|
| repeatAt sets repeat time | Time-of-day scheduling |
| repeatAt returns job (chaining) | Fluent API |
| toJson handles null failedAt | Null safety in serialization |
| toJson preserves Date failedAt | Date object retention |
| unique sets constraint | Unique constraint object |
| unique returns job (chaining) | Fluent API |
| repeatEvery sets interval | Interval string assignment |
| repeatEvery accepts timezone | Timezone parameter support |
| repeatEvery handles skipImmediate | Deferred first run |
| schedule from Date object | Date-based nextRunAt |
| schedule from string time | Human-readable time parsing |
| priority number assignment | Numeric priority |
| priority string parsing (lowest=-20, low=-10, normal=0, high=10, highest=20) | Named priority levels |
| computeNextRunAt for intervals | Repeating interval calculation |
| computeNextRunAt for cron | Cron schedule parsing |
| computeNextRunAt respects timezone | Timezone-aware cron |
| computeNextRunAt handles repeatAt | Time-of-day calculation |
| fail sets failReason from Error | Error-to-string conversion |
| fail sets failReason from string | Direct string assignment |
| fail sets failedAt | Timestamp assignment |
| fail increments failCount | Counter increment |
| enable sets disabled=false | Re-enable job |
| disable sets disabled=true | Disable job |

#### computeJobState
| Test Case | Behavior Verified |
|-----------|-------------------|
| "running" when lockedAt set | Active job detection |
| "running" even if also failed | Running takes precedence |
| "failed" when failedAt set, no lastFinishedAt | Failure detection |
| "failed" when failedAt after lastFinishedAt | Recent failure |
| "failed" when failedAt equals lastFinishedAt | Retry exhaustion |
| "completed" when lastFinishedAt after failedAt | Recovery state |
| "completed" when lastFinishedAt set, no failedAt | Success state |
| "repeating" when repeatInterval set | Recurring job state |
| "failed" over "repeating" when failed | Failure precedence |
| "scheduled" when nextRunAt in future | Future scheduling |
| "queued" when nextRunAt is now | Ready to run |
| "queued" when nextRunAt in past | Overdue detection |
| "completed" for job with no dates | Default state |

#### Logging Configuration
| Test Case | Behavior Verified |
|-----------|-------------------|
| no logger when omitted | Default no-logging |
| no logger when false | Explicit disable |
| backend logger when true | Backend logger utilization |
| custom JobLogger instance | Direct injection |
| per-definition logging override (true/false) | Selective logging |
| agendaName in log entries | Context in logs |
| getLogs/clearLogs delegation | Logger API delegation |

### 4.2 Shared Test Suites

#### agenda-test-suite.ts (~60 tests)

**Scheduling Methods**
| Test Case | Behavior Verified |
|-----------|-------------------|
| every() schedules interval job | repeatInterval stored |
| every() with cron expression | Cron syntax accepted |
| every() updates existing job | Upsert behavior |
| every() accepts timezone option | repeatTimezone set |
| every() skipImmediate option | Deferred first execution |
| every() creates multiple from array | Batch creation |
| schedule() for specific time | Date scheduling |
| schedule() with human-readable time | String parsing |
| schedule() creates multiple from array | Batch scheduling |
| now() schedules immediately | nextRunAt ~ current time |
| now() with data payload | Data attachment |

**Query and Cancel**
| Test Case | Behavior Verified |
|-----------|-------------------|
| queryJobs returns matching | Filter by name |
| queryJobs empty on no match | Empty array |
| queryJobs limit | Result count cap |
| queryJobs skip | Offset pagination |
| queryJobs combined limit+skip | Full pagination |
| queryJobs sort | Priority-based ordering |
| cancel by query | Selective deletion |

**Disable/Enable**
| Test Case | Behavior Verified |
|-----------|-------------------|
| Job.disable() sets flag | Instance-level disable |
| Job.enable() clears flag | Instance-level enable |
| disabled jobs not processed | Skip during processing |
| agenda.disable() bulk operation | Query-based bulk disable |
| agenda.enable() bulk operation | Query-based bulk enable |
| 0 returned on no match | No-op safety |

**Processing**
| Test Case | Behavior Verified |
|-----------|-------------------|
| processes job | Processor function executes |
| passes job instance to processor | Job object available |
| success event on completion | Event emission |
| fail event on error | Error event emission |

**Locking**
| Test Case | Behavior Verified |
|-----------|-------------------|
| locks during processing | lockedAt set while running |
| clears locks on stop | lockedAt cleared after shutdown |

**Concurrency**
| Test Case | Behavior Verified |
|-----------|-------------------|
| respects maxConcurrency | Global limit enforced |
| respects per-job concurrency | Definition limit enforced |

**Priority**
| Test Case | Behavior Verified |
|-----------|-------------------|
| higher priority first | Priority-based execution order |

**Lock Limits**
| Test Case | Behavior Verified |
|-----------|-------------------|
| agenda lockLimit respected | Global lock ceiling |
| mixed jobs within lockLimit | Cross-type lock counting |
| definition lockLimit respected | Per-definition ceiling |
| lockLimit during processing interval | Batch-level enforcement |

**Purge**
| Test Case | Behavior Verified |
|-----------|-------------------|
| removes orphaned jobs | Undefined jobs deleted, defined kept |

**Unique Constraint**
| Test Case | Behavior Verified |
|-----------|-------------------|
| modify one job when unique matches | Upsert on match |
| no modify with insertOnly | Insert-only mode |
| two jobs when unique differs | Separate jobs created |

**Job Class Methods**
| Test Case | Behavior Verified |
|-----------|-------------------|
| remove() deletes from database | ID-based deletion |
| run() calls processor directly | Synchronous execution |
| touch() updates lockedAt | Lock extension |
| touch() persists progress | Progress saved to DB |
| fail() sets fields in memory | Failure metadata |
| fail() increments failCount | Counter increment |
| save() persists to database | Creation with _id |
| save() updates existing | Modification persistence |

**Async/Callback Handling**
| Test Case | Behavior Verified |
|-----------|-------------------|
| async functions allowed | Async processor execution |
| errors from async caught | Exception handling |
| waits for callback even if async | Callback awaited |
| callback error if async no reject | Callback error priority |
| async error over callback | Async-first precedence |
| callback error over async | Callback-first precedence |

**Events**
| Test Case | Behavior Verified |
|-----------|-------------------|
| start event on begin | `start:` event fired |
| complete event on finish | `complete:` event fired |
| generic start event | Generic `start` event |

**Drain**
| Test Case | Behavior Verified |
|-----------|-------------------|
| waits for all running jobs | Multi-job drain |
| waits for single running job | Single-job drain |
| resolves immediately if none running | Empty drain |
| timeout returns running count | Timeout parameter |
| timeout as options object | Options syntax |
| abort via AbortController | Signal integration |
| pre-aborted signal | Immediate resolution |
| timeout + signal together | Combined constraints |

**Repeating Jobs**
| Test Case | Behavior Verified |
|-----------|-------------------|
| reschedules after completion | repeatEvery continuation |
| runs at cron time | Cron execution timing |

**Error Handling**
| Test Case | Behavior Verified |
|-----------|-------------------|
| error event on throw | Fail event with exception |
| saves failReason and failedAt | Failure metadata persisted |
| increments failCount | Counter across runs |
| no auto-retry unless rescheduled | Failed jobs stop |
| no unhandled rejection on timeout | Clean timeout handling |

**Notification Channel (conditional)**
| Test Case | Behavior Verified |
|-----------|-------------------|
| accepts in constructor | Initialization with channel |
| accepts via notifyVia() | Runtime attachment |
| throws after start | Post-start rejection |
| connect/disconnect on start/stop | Lifecycle management |
| faster processing with channel | Reduced latency vs polling |

**Fork Mode (conditional)**
| Test Case | Behavior Verified |
|-----------|-------------------|
| job in fork mode | Subprocess execution |
| failure in fork mode | Error capture from subprocess |
| process exit in fork mode | Abnormal termination handling |

#### jobprocessor-test-suite.ts (9 tests)

| Test Case | Behavior Verified |
|-----------|-------------------|
| getRunningStats throws when not running | Pre-start validation |
| stats contain agendaVersion | Version in stats |
| stats show correct job status | Locked/running/concurrency/lockLifetime |
| stats show queueName | Queue name in stats |
| stats show totalQueueSizeDB | Database queue size |
| new jobs fill running queue | Short jobs execute despite long jobs |
| slow jobs time out | lockLifetime exceeded triggers error |
| slow jobs do not timeout with touch() | touch() extends lifetime |
| concurrency filled up | Near-max concurrent execution |

#### retry-test-suite.ts (4 tests)

| Test Case | Behavior Verified |
|-----------|-------------------|
| retry via attrs.nextRunAt | Direct nextRunAt modification |
| retry via schedule() | schedule('now').save() method |
| failCount tracked via nextRunAt | Multi-attempt counting |
| failCount tracked via schedule() | Multi-attempt counting |

#### backoff-test-suite.ts (18 tests)

| Test Case | Behavior Verified |
|-----------|-------------------|
| constant: same delay each attempt | Fixed delay pattern |
| constant: respects maxDelay | Ceiling enforcement |
| linear: increases linearly | Uniform increment |
| linear: delay as default increment | Increment defaults to delay |
| linear: respects maxDelay | Ceiling enforcement |
| exponential: increases exponentially | Doubling/factor pattern |
| exponential: respects maxDelay | Ceiling enforcement |
| exponential: applies jitter | Randomization within range |
| combine: strategies in sequence | Sequential fallthrough |
| when: conditional retry | Error-based condition |
| aggressive preset: fast retries | Rapid retry sequence |
| standard preset: balanced retries | Moderate timing with jitter |
| auto-retry with backoff | End-to-end retry execution |
| retry exhausted event | Event on max retries |
| retry details in event | Metadata in retry events |
| no auto-retry without backoff | Opt-in behavior |
| auto-retry on repeating jobs | Recurring job retry |
| custom backoff function | User-defined logic |

#### debounce-test-suite.ts (12 tests)

| Test Case | Behavior Verified |
|-----------|-------------------|
| trailing: delays by debounce delay | nextRunAt pushed forward |
| trailing: resets timer on saves | Multiple saves extend delay |
| trailing: updates data on saves | Latest data preserved |
| trailing: single DB record | Five saves = one job |
| leading: immediate first execution | No delay on first save |
| leading: keeps original nextRunAt | Subsequent saves no change |
| leading: updates data on saves | Data updated despite fixed time |
| maxWait: forces execution | Ceiling on debounce delay |
| maxWait: normal debounce within limit | Debounce while under maxWait |
| independent per unique key | Separate timers per key |
| requires unique constraint | No unique = separate jobs |
| executes after delay | Consolidated execution |

#### removeoncomplete-test-suite.ts (7 tests)

| Test Case | Behavior Verified |
|-----------|-------------------|
| removes one-time job (global true) | Auto-deletion on success |
| does NOT remove (default false) | Persistence by default |
| does NOT remove recurring jobs | Recurring jobs always kept |
| per-job override (true over global false) | Instance-level opt-in |
| per-job override (false over global true) | Instance-level opt-out |
| complete event before removal | Event ordering guarantee |
| does NOT remove failed jobs | Failures always kept |

#### repository-test-suite.ts (~25 tests)

| Test Case | Behavior Verified |
|-----------|-------------------|
| saveJob assigns ID | Auto-ID generation |
| saveJob updates existing | Modification persistence |
| saveJob upsert single-type | Name-based upsert |
| saveJob preserves disabled flag | Flag persistence |
| getJobById returns job | ID-based retrieval |
| getJobById null for missing | Null safety |
| queryJobs all | Full collection retrieval |
| queryJobs by name | Name filtering |
| queryJobs by multiple names | Array filtering |
| queryJobs by search pattern | Text search |
| queryJobs pagination | skip + limit |
| queryJobs disabled filter | includeDisabled flag |
| removeJobs by name | Name-based deletion |
| removeJobs by ID | ID-based deletion |
| removeJobs by multiple IDs | Batch deletion |
| removeJobs returns 0 on no match | No-op count |
| getDistinctJobNames | Deduplicated names |
| getDistinctJobNames empty | Empty array |
| getQueueSize counts ready jobs | Past nextRunAt only |
| getQueueSize 0 when none ready | Future jobs excluded |
| lock a job | lockedAt set |
| not lock already locked | Returns undefined |
| unlock a job | lockedAt cleared |
| unlock multiple jobs | Batch unlock |
| getNextJobToRun | Ready job with auto-lock |
| getNextJobToRun priority ordering | Higher priority first |
| getNextJobToRun excludes disabled | Disabled jobs skipped |
| getNextJobToRun undefined when none | Empty queue handling |
| saveJobState updates fields | Progress, failCount, timestamps |
| saveJobState clears lockedAt | Completion cleanup |

### 4.3 date-constraints.test.ts (~20 tests)

| Test Case | Behavior Verified |
|-----------|-------------------|
| shouldSkipDay: empty skipDays | Returns false |
| shouldSkipDay: undefined skipDays | Returns false |
| shouldSkipDay: day in skipDays | Returns true (Monday, Sunday) |
| shouldSkipDay: day not in skipDays | Returns false |
| shouldSkipDay: weekend logic | Saturday/Sunday skipped |
| shouldSkipDay: timezone handling | Same UTC, different local result |
| applySkipDays: empty preservation | Unchanged date |
| applySkipDays: non-skipped day | Original date returned |
| applySkipDays: advancement | Saturday to Monday |
| applySkipDays: time preservation | Time maintained on advance |
| applySkipDays: all days skipped | Returns null |
| applySkipDays: consecutive skip | Multi-day advancement |
| applyDateRangeConstraints: no constraints | Unchanged |
| applyDateRangeConstraints: before start | Adjusted to startDate |
| applyDateRangeConstraints: after end | Returns null |
| applyDateRangeConstraints: within range | Unchanged |
| applyAllDateConstraints: combined | Sequential constraint application |
| applyAllDateConstraints: skip exceeds end | Returns null |
| isWithinDateRange variations | Boolean range checking |

### 4.4 decorators.test.ts (18 tests)

| Test Case | Behavior Verified |
|-----------|-------------------|
| @JobsController marks class | isJobsController() returns true |
| @JobsController stores namespace | Metadata preservation |
| @JobsController works without namespace | Optional namespace |
| @Define registers method | Tracked with type "define" |
| @Define stores custom name | Name override |
| @Define stores options | Concurrency, priority metadata |
| @Every registers recurring | Interval storage |
| @Every supports cron | Cron syntax handling |
| @Every stores timezone | Timezone configuration |
| registerJobs throws without decorator | Validation enforcement |
| registerJobs registers @Define | Jobs added to agenda |
| registerJobs prefixes namespace | Namespace prepending |
| registerJobs uses custom name | Name override with namespace |
| registerJobs passes options | Config propagation |
| registerJobs binds to instance | `this` context preservation |
| registerJobs multiple controllers | Simultaneous registration |
| registerJobs multiple jobs same controller | Multi-decorator handling |
| registerJobs with dependencies | Dependency injection integration |

### 4.5 state-notifications.test.ts (~22 tests)

| Test Case | Behavior Verified |
|-----------|-------------------|
| publishState method exists | API surface check |
| subscribeState method exists | API surface check |
| publish and receive notifications | End-to-end notification flow |
| unsubscribe stops delivery | Subscription cleanup |
| throws on disconnected channel | Error on disconnected publish |
| publish when channel connected | Agenda publishes state events |
| extra data in notification | Error/failCount metadata |
| no throw without state support | Graceful degradation |
| subscribe on start | Auto-subscription setup |
| generic event with remote=true | Cross-process event emission |
| job-specific event with remote=true | Scoped event emission |
| fail events with error arg | Error parameter structure |
| retry events with details | Retry metadata |
| no re-emit from same source | Double-fire prevention |
| unsubscribe on stop | Cleanup on shutdown |
| individual state type handlers | start/progress/success/fail/complete/retry |

---

## 5. Cross-Library Pattern Analysis

### 5.1 Universal Test Categories (ALL libraries test these)

| Category | Bee-Queue | node-resque | RedisSMQ | Agenda |
|----------|-----------|-------------|----------|--------|
| Connection lifecycle | Yes | Yes | Yes | Yes |
| Job creation/save | Yes | Yes | Yes | Yes |
| Job removal/cancel | Yes | Yes | Yes | Yes |
| Job processing | Yes | Yes | Yes | Yes |
| Job failure handling | Yes | Yes | Yes | Yes |
| Retry mechanisms | Yes | Yes | Yes | Yes |
| Event emission | Yes | Yes | Yes | Yes |
| Graceful shutdown | Yes | Yes | Yes | Yes |
| Concurrent processing | Yes | Yes | Yes | Yes |
| Job timeout | Yes | Yes (via lock) | Yes (consumeTimeout) | Yes (lockLifetime) |

### 5.2 Unique Test Patterns by Library

#### Bee-Queue Unique Tests
- **EagerTimer** (17 tests): Timer scheduling edge cases (null, NaN, undefined, negative, past timestamps) - unique internal component testing
- **Stall detection at scale**: 1000+ stalled jobs unpacking limit test
- **Multi-queue no-duplication**: Explicit test that same job not processed by multiple queue instances
- **brpoplpush backoff**: Redis blocking pop failure recovery
- **removeOnSuccess/removeOnFailure**: Separate flags for success vs failure cleanup
- **isWorker=false**: Explicit non-worker mode preventing blocking client
- **autoConnect=false**: Deferred connection mode
- **SSCAN for large sets**: Set-type scanning pagination

#### node-resque Unique Tests
- **Leader election + failover**: Multiple schedulers, only one leader, automatic failover
- **CPU-aware worker scaling**: MultiWorker scales based on CPU utilization
- **Plugin system tests**: Middleware pattern (jobLock, queueLock, delayedQueueLock, noop, retry, custom)
- **Worker heartbeat during slow jobs**: Ping continues during long processing
- **Stuck worker detection**: Time-limited stuck worker cleanup
- **forceClean**: Forced worker termination with error payload
- **Wildcard queue discovery**: `queues=*` auto-detects new queues
- **Lua command loading verification**: Checks custom Redis commands loaded
- **ioredis-mock compatibility**: Full suite runs against mock Redis

#### RedisSMQ Unique Tests
- **LIFO queue ordering**: Explicit last-in-first-out verification
- **Exchange routing**: Direct exchange binding/unbinding, routing key validation
- **Rate limiting CRUD**: Set/get/clear rate limits with validation errors
- **Message audit configuration**: Enable/disable per message state type
- **Consumer crash recovery across queues**: Multi-queue crash recovery
- **Queue name normalization**: Case-insensitive queue names
- **Single consumer multiple queues**: subscribe/cancel/re-subscribe lifecycle
- **MessageExchangeRequiredError**: Producing without exchange configured
- **Message storage backends**: Separate tests for list/set/sorted-set storage
- **Connection pool tests**: Redis connection pooling

#### Agenda Unique Tests
- **computeJobState**: 13-case state machine (running > failed > completed > repeating > scheduled > queued > completed)
- **Debounce** (12 tests): Trailing/leading strategies, maxWait, independent per unique key
- **Backoff strategies** (18 tests): Constant/linear/exponential/combine/when/presets
- **RemoveOnComplete** (7 tests): Per-job override, recurring job exception, event ordering
- **Fork mode**: Job execution in subprocess
- **Notification channels**: Cross-process state notifications
- **Decorators**: @JobsController, @Define, @Every with TypeScript decorators
- **Date constraints**: skipDays, date ranges, timezone handling
- **Drain with AbortController**: Signal-based drain cancellation
- **Priority string parsing**: "lowest"=-20 through "highest"=20
- **Repository abstraction**: Backend-agnostic persistence tests
- **Unique constraint with insertOnly**: Upsert vs insert-only modes
- **Logging configuration**: Per-definition logging override

### 5.3 Test Counts Summary

| Library | Test Files | Approximate Test Cases |
|---------|-----------|----------------------|
| Bee-Queue | 5 | ~120 |
| node-resque | 12 | ~80 |
| RedisSMQ | 100+ | ~150+ |
| Agenda | 17 (5 direct + 12 shared) | ~150 |
| **Total** | **~134** | **~500+** |

### 5.4 Edge Cases Tested Across Libraries

| Edge Case | Libraries Testing It |
|-----------|---------------------|
| Double-close safety | Bee-Queue |
| Null/NaN/undefined input | Bee-Queue (EagerTimer), Agenda (date constraints) |
| Circular reference in job data | Bee-Queue |
| Empty namespace handling | node-resque |
| Connection to invalid host | node-resque |
| Past timestamp scheduling | Bee-Queue, Agenda |
| Zero retries configuration | Bee-Queue, RedisSMQ |
| Expired message TTL | RedisSMQ |
| Consumer crash mid-processing | RedisSMQ, Bee-Queue (stall) |
| All days skipped in schedule | Agenda |
| Pre-aborted AbortController signal | Agenda |
| Async callback error precedence | Agenda |
| Duplicate job prevention | node-resque (queueLock), RedisSMQ |

---

## 6. Test Patterns Relevant to Glide-MQ Implementation

### 6.1 Stall Detection Tests (Bee-Queue) - Critical for Any Queue

These are the most important tests to replicate:
1. **Stall detection on startup** - Recover jobs from previous crashed instance
2. **Mass stall recovery (>1000)** - Handle large numbers of stalled jobs
3. **Multi-queue stall recovery** - Stalls from multiple dead queues
4. **Concurrent processor stalls** - Stalls detected while other jobs processing
5. **Periodic stall checking** - Interval-based heartbeat monitoring
6. **Pre-existing jobs on startup** - Process jobs left by previous instance
7. **No double-processing** - In-progress jobs from crashed queues not re-executed prematurely

### 6.2 Connection Resilience Tests

From Bee-Queue and node-resque:
1. **Disconnect and reconnect** - Queue recovers after connection drop
2. **Backoff on blocking pop failure** - Exponential backoff on brpoplpush
3. **Close during retry strategy** - Shutdown works even with active retry
4. **Connection error emission** - Errors properly emitted, not unhandled rejections

### 6.3 Concurrency Tests

From all libraries:
1. **Concurrency limit enforced** - Never exceed configured max
2. **Multi-queue no duplication** - Same job not processed by multiple instances
3. **CPU-aware scaling** (node-resque) - Worker count adjusts to load
4. **Lock limit enforcement** (Agenda) - Global and per-definition lock ceilings
5. **Competing consumers** (RedisSMQ) - Single delivery with many consumers

### 6.4 Event System Tests

From Bee-Queue and Agenda:
1. **Success/failure/progress/retry events** - All event types fire
2. **Event enable/disable** - getEvents=false, sendEvents=false
3. **Event ordering** - Complete event before removal
4. **Cross-process events** (Agenda) - Notification channels
5. **No double-fire** (Agenda) - Same-source deduplication

---

*This guide was synthesized from 25 direct repository source files. See `resources/bee-queue-and-others-test-cases-sources.json` for full source list.*
