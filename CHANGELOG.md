# Changelog

All notable changes to glide-mq are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [Unreleased]

### Added

- **Error classes from `glide-mq/testing`**: `GlideMQError`, `ConnectionError`, `UnrecoverableError`, `DelayedError`, `BatchError`, `WaitingChildrenError`, `SuspendError` and `GroupRateLimitError` (plus the `GroupRateLimitOptions` type) are re-exported, so a test can throw and match them without importing the main entry, which loads the native client. They are the same classes, so `instanceof` works across both entries.

---

## [0.17.0] - 2026-10-04

### Added

- **`WorkerInfo.concurrency`**: `queue.getWorkers()` (and `GET /queues/:name/workers` on the proxy) now reports each worker's configured `concurrency` option, in production and in `glide-mq/testing`. Optional: it is absent for a worker running a glide-mq version that predates the field. In batch mode it counts batches, so up to `concurrency * batch.size` jobs can be active at once. `TestWorker.concurrency` is now a public `readonly` property.
- **Dead-letter queue in `glide-mq/testing`**: `TestWorker` takes the `deadLetterQueue` option and copies every terminally failed job into the `TestQueue` of that name with the production envelope (`originalQueue`, `originalJobId`, `data`, `failedReason`, `attemptsMade`). `TestQueue` gets `getDeadLetterJobs`, `getDeadLetterJob`, `removeDeadLetterJob` and `replayDeadLetterJob`, scoped to the owning queue like `Queue`, and a `deadLetterQueue` option to name the dead-letter queue it reads.

### Changed

- **Behavior change: `prefetch` below `batch.size` now caps batches that wait on `batch.timeout`.** A worker with `prefetch: 2`, `batch: { size: 5, timeout }` kept reading during the timeout window and handed out batches of 5; it now hands out batches of at most 2, the same as without a timeout. This matches the documented `prefetch` contract (it only lowers the claim size). To keep batches of 5, raise `prefetch` to at least `batch.size` or leave it unset.

### Fixed

- **Default proxy logs could contain forged error lines**: request-derived error messages or stacks were passed to `console.error` as a raw `Error`. Default logging now sanitizes error name, message and stack fields, including control characters and Unicode line separators. Custom `onError(err, queueName)` callbacks still receive the original Error.
- **Dependency security maintenance**: refresh compatible lockfile entries for brace-expansion and advisories found by npm audit.

- **Batch workers exceeded `concurrency * batch.size` with `batch.timeout`**: the first read of a poll was capped at the free in-flight budget, but the refill reads during the timeout window topped the batch up to `batch.size` regardless. A `Worker` or `BroadcastWorker` with `concurrency: 2`, `batch: { size: 5, timeout }` and 8 jobs in flight could start a batch of 5 and run 13 jobs at once. Refill reads now stay within `min(batch.size, prefetch - activeJobs)`. Global concurrency was not affected: activation already enforces it.
- **Token bucket refilled from a future `tbLastRefill`**: group setup stamped `tbLastRefill` from the producer's `Date.now()`, so a producer whose clock ran ahead left a full bucket with a future refill stamp. `tbRefill` returned early at capacity without touching it, and the first consumption then refilled with zero elapsed time, so the time since that consumption never counted and the bucket stayed empty longer than `1/refillRate`. Group setup now seeds `tbLastRefill` from the server clock, and `tbRefill` pulls any stamp that differs from server time back to it while the bucket is full. Server function library version is now `133`; workers and producers reload it on connect.
- **`TestWorker` rejects a queue name**: passing a string (the production `Worker` signature) instead of a `TestQueue` instance threw `Cannot read properties of undefined (reading 'add')` from inside the worker; it now throws a `GlideMQError` naming the expected argument.

---

## [0.16.0] - 2026-09-30

### Changed

- **`@glidemq/speedkey` ^0.4.2**: the client is now a re-import of upstream valkey-glide main (security advisories closed, multiplexed-connection deadlock fixed) and `close()` detaches a connection with an in-flight blocking command within a few ms on both standalone and cluster clients (0.4.0 left a cluster `XREADGROUP` attached for the block duration). glide-mq keeps its graceful-close wait for the in-flight read: a claim delivered in the socket-teardown window would be lost with the rejected read. On localhost that window is now too small to hit (0 of 30 closes stranded an entry with or without the wait on 0.4.2; 30 of 30 without it on 0.3.0), on a network it is one round trip, and there is no typed `CLIENT UNBLOCK` to close it server-side.
- **`SearchQueryOptions.scorer` removed**: no released valkey-search accepts `SCORER` (it exists only on valkey-search main since 2026-08-31) and upstream glide does not expose it. JavaScript callers passing it were already ignored; TypeScript callers now get a type error.
- **Proxy bounds**: `POST /queues/:name/retry` retries at most `maxPageSize` per call (default 1000) and returns `{ retried }`; `count > maxPageSize` or `count = 0` returns 400. `POST /queues/:name/jobs/wait` accepts `waitTimeout` up to the new `ProxyOptions.maxWaitTimeout` (default 60000 ms), and a client disconnect aborts the wait and frees the blocking connection. `GET /queues/:name/metrics` returns the whole per-minute hash and is not paged.
- **Broadcast `trimmed` event**: `Broadcast` emits `('trimmed', { trimmed, unread })` after a publish that trims, where `unread` counts messages dropped before some subscription had read them.
- **Server function library version is `132`.** Workers and producers reload it on connect.
- **`DeadLetterQueueOptions.maxRetries` is deprecated** (never read; removal in the next major).
- **The `active` stream event is no longer written anywhere**; `worker.on('active')` is unchanged.

### Fixed

- **Switching a scheduler to `repeatAfterComplete` could still overlap the old mode's running job**: the tick now records the fired job on the entry (`inflightJobId`) and parks the entry until that job finishes; only that job advances it.
- **Cost-over-capacity failures inside `completeAndFetchNext` got no DLQ copy**: the reply carries a `__glidemq_failed_activations__` marker (ignored by old parsers) and the worker adds the copies.
- **Global concurrency could briefly overshoot**: `moveToActive` enforces the cap atomically over the oldest pending claims. A claim beyond it returns `GLOBAL_FULL`; the worker holds the claim and retries with a short backoff (bounded by half the lock duration or stalled interval) so FIFO order is kept, and hands it back only after the bound. `completeAndFetchNext` does not claim past the cap either.
- **Closed consumers stayed in the consumer group forever**: a graceful close removes its consumer when it has no pending entries, and stalled reclaim removes idle empty consumers in bounded batches.
- **Broadcast stall detection was masked across subscriptions**: heartbeats and reclaim use a per-subscription `la` field (shared `lastActive` kept for old reclaimers), a message handed back by a closing or pausing worker is not counted as a stall, `glidemq_recoverBroadcastClaims` re-takes only entries the worker owns, and retry entries from pre-130 libraries are looked up before trimming.
- **Rolling-upgrade gaps**: parked early cross-queue completions are indexed and healed every scheduler tick, and `healListActive` trusts the seeded `list-active-ids` set and, on a mismatch, re-seeds it from a SCAN that resumes across ticks (`meta.listActiveScanCursor`).
- **Sandboxed processors could mutate a timed-out job during the abort grace window**: `updateProgress`, `updateData` and `moveToDelayed` reject after the abort signal; `log` and `discard` still work.
- **Proxy `POST` routes could misread a consumed request body as a disconnect**: `trackDisconnect` counted `req` `close`, which Node emits once the JSON body is consumed while the socket is still open. It now relies on `res` `close` and a dead socket, so `jobs/wait` and other body-reading routes are not cut short.
- **Proxy SSE failures after headers were silent**: errors on `/queues/:name/events`, `/jobs/:id/events`, `/jobs/:id/stream` and `/broadcast/:name/events` now reach `onError(err, queueName)` before the stream ends.
- **Proxy opened a Valkey client per request** for `POST /flows` and `GET /usage/summary`; both use the shared command client.
- **Batch workers never charged or checked flow budgets**: batch completion and every batch failure path now charge reported usage once per job (same `usage:budgeted` marker as single-job workers), and each batch entry is diverted when its budget is already exceeded.
- **`RateLimitError` consumed an attempt**: the requeue incremented `attemptsMade` (or the broadcast per-subscription counter) and wrote `failedReason`. `glidemq_fail` takes an optional `requeueOnly` argument; the job is scheduled after the limiter delay with its counters untouched.
- **Budget `onExceeded: 'pause'` re-delayed jobs by 24 hours with no way out**: paused jobs re-check every 60 seconds, `job.promote()` re-checks at once, and the new `Queue.updateFlowBudget(flowId, limits)` raises, lowers or removes limits and clears `exceeded` when they are no longer breached (`TestQueue.updateFlowBudget` mirrors it).
- **Testing mode round 2**: `delay` is honored (jobs start `delayed`, promote on time, `promote()`/`changeDelay()` follow the server rules, debounce can replace them); priority jobs sit in `prioritized` until a worker pass; `RateLimitError` parks the job for the limiter window without consuming an attempt or emitting `failed` (`TestWorkerOptions.limiter`, `worker.rateLimit(ms)`); `getJobs('waiting')` follows dispatch order; the scheduler tick ignores template `delay`/`deduplication` like production; `repeatAfterComplete` waits for completion; budget `pause` parks in `delayed`. TestJob, TestQueue and TestWorker gained the production methods they lacked (`getState`, `is*`, `waitUntilFinished`, `retry`, `remove`, `moveToFailed`, `log`, `addAndWait`, `count`, `getJobCountByTypes`, `getJobLogs`, `getSuspendedJobs`, `revoke`, `obliterate`, `pause`/`resume`, `drain`). docs/TESTING.md lists the remaining limitations.

### Added

- **`Queue.addAndWait` accepts `signal?: AbortSignal`**: aborting rejects with a `GlideMQError` named `AbortError` and releases the blocking connection; the job stays queued.
- **Cron syntax parity with cron-parser**: month and weekday names, day-of-week `7`, `?`, `N/step`, an optional leading seconds field, `L`, `LW`, `<n>W` in day-of-month and `<d>L`, `<d>#<n>` in day-of-week. A test oracle compares `nextCronOccurrence` with cron-parser 4.9.0 (the version BullMQ uses) over 44 patterns in four zones: 0 mismatches outside DST transitions, where glide-mq keeps cronie's rules. Seconds patterns are honored by the parser; the scheduler still fires on its promotion tick, so sub-tick periods produce one job per tick.
- **Bun and Deno support**: verified on Bun 1.4.2 and Deno 2.9.7 (NAPI client load, Queue/Worker/QueueEvents, gzip, worker_threads and forked sandboxes, flows, broadcast, signals). `npm run compat:bun` / `compat:deno` run the smoke against a local Valkey, CI runs both, and docs/COMPATIBILITY.md lists the required Deno permissions and the known gaps.

### Performance

- **Idle poll loops skip the pre-block list pop** when the previous `completeAndFetchNext` reported the lists empty (`__glidemq_lists_empty__` marker, honored for 100 ms).
- **Failed jobs keep the fetch chain alive**: `glidemq_failAndFetchNext` fails the current job and fetches the next one in one call (1 round trip instead of about 4). Rate-limit requeues, batch and broadcast workers keep the previous path. On a library that lacks the function the worker falls back to `glidemq_fail` once per process.

---

## [0.15.6] - 2026-09-30

### Fixed

- **`addDAG` could release a parent early**: a leaf with one dependent was wired at creation while a sibling with several dependents was wired a round trip later, so a fast leaf could release the parent before the sibling was registered. Leaf ids are reserved and every dependent's deps set is filled before any leaf becomes runnable.
- **Scheduled jobs skipped gzip compression** on queues with `compression: 'gzip'`. The scheduler entry records the upserting queue's compression and the tick compresses like `Queue.add`.
- **Switching a scheduler to `repeatAfterComplete` fired immediately**, overlapping a still-running job from the old mode. The first run is now held until the old mode's next run.
- **`TestJob.moveToDelayed()` was missing** in testing mode.
- **Broadcast subscriptions lost stalled messages**: a stalled message was claimed into the reclaiming worker's PEL and never run again, then failed on the next cycle. Stalled reclaim now re-dispatches the claimed entries for that subscription, and stall counts are kept per subscription instead of on the shared job hash.
- **Broadcast `maxMessages` trimming leaked storage**: trimmed messages left their job hash, per-subscription hashes and completed/failed members behind. `publish` now trims through `glidemq_trimBroadcast`, which deletes them once no subscription still holds the message. `maxMessages` remains a hard cap and can drop messages a slow subscriber has not read.
- **Broadcast priority/LIFO messages were never delivered**: `publish` now rejects `priority` and `lifo` (the proxy returns 400).
- **`obliterate()` wiped running priority/LIFO jobs**: without `force` it counted only stream pending entries. Active list claims now block it too.
- **Worker `stalled` event was never emitted**: workers now emit `('stalled', jobId, 'active')` for jobs their stalled check returned to waiting. The reclaim functions return the IDs on an optional argument; older libraries keep the count reply.
- **`events: false` / `metrics: false` did not apply to failures**: `glidemq_fail` now honors both, skipping `retrying`/`failed` events and failure metrics.
- **A failed eager cross-queue parent notification broke the completion path**: the throw skipped the `completed` event and left an already fetched next job waiting for stall recovery. Delivery errors are now emitted as `error`, and the pending entry is kept for the scheduler retry.
- **No DLQ copy for cost-over-capacity failures** in `moveToActive`.
- **A list job removed after its reservation leaked `list-active`**: the worker now releases the reservation.
- **Cross-queue parent released early**: `Queue.add`/`Producer.add` registered a cross-queue child in the parent's deps only after creating it, so a fast child could release a parent that still had pending children. Such completions are now parked and counted when the child is registered through `glidemq_registerChildDep`. Debounce replacing a cross-queue child inherits the replaced child's dependency.
- **Flow budgets applied late**: the budget hash and each job's `budgetKey` were written after the flow's jobs were runnable. The budget is now created first and `budgetKey` is written in the same call that creates each job.
- **Ordered group jobs completed via `completeAndFetchNext` skipped ordering bookkeeping** when only `groupKey` was stored.
- **BroadcastWorker retry counters reset after 24h**: the per-subscription counter expired before long backoffs elapsed.
- **Jobs claimed by a closing worker waited for stalled recovery**: closing the blocking client does not cancel an in-flight `XREADGROUP BLOCK`, so entries added during or right after `close()` landed in the closed consumer's PEL and were charged a stall they never ran. A graceful close now lets the read return (at most `blockTimeout` + 1s) and hands its claims back before closing. `close(true)` still tears down immediately.
- **Flow budgets ignored failed attempts**: usage reported during a failed attempt was never charged to the flow budget. It now is, without double counting on retry.
- **Reconnect after close() leaked clients and timers**: a reconnect that finished after `close()` installed new clients, a new scheduler and a heartbeat timer that kept the process alive. Reconnect now checks `closing` after every await and closes what it created (Worker and QueueEvents). The first reconnect attempt now waits out its backoff, so persistent non-connection errors no longer spin.
- **Leaked heartbeat kept a job active forever**: if a rate-limit or token-limit call threw, the job's heartbeat interval kept refreshing `lastActive`, so stalled recovery never reclaimed it. Limiter failures and partial batch activations now stop their heartbeats.
- **`Worker.pause()` did not stop chaining under a backlog**: completion kept fetching the next job through `completeAndFetchNext`, so `pause()` resolved only when the queue drained. Paused workers complete without fetch-next, and entries delivered by an in-flight read are handed back instead of run.
- **BroadcastWorker batch mode dropped claimed entries**: the read count was not capped at `batch.size`, so extra entries sat in the PEL and later failed as stalled without running. Both workers now also keep the batch cap when global-concurrency headroom is lower.
- **RateLimitError exhausted BroadcastWorker retries**: the retry used the shared `attemptsMade` instead of the per-subscription counter, so a second rate-limit hit failed the job while the worker reported a retry.
- **QueueEvents**: an init failure is emitted as `'error'` instead of an unhandled rejection, and a throwing listener no longer redelivers the same event forever.
- **`suspend` continuations grew without bound**: entries are dropped on failed suspends and on close, and the map is capped at 10,000.
- **Shutdown**: `close(true)` aborts running jobs' `abortSignal`; limiter waits wake on close and hand the job back; a second SIGINT/SIGTERM during a hung graceful shutdown exits the process.
- **`Queue.addBulk` registered the wrong cross-queue children**: skipped or duplicate jobs shifted the result array, so a later job was added to another job's cross-queue parent. Results now stay aligned with the input, and returned jobs carry `parentQueue`.
- **Negative or fractional priority stuck jobs**: `priority: -1` with a delay was never promoted while the scheduler spun on a 1ms timer, and fractional priorities broke score decoding. Priority must be an integer 0-2048, delay a finite number >= 0, attempts an integer >= 0, and backoff delay/jitter finite and >= 0, on every add path (`Queue.add`/`addBulk`, `Producer`, `FlowProducer`, `addDAG`). FlowProducer and DAG validate the whole tree before any write, including queue names.
- **Cross-queue flows failed with CROSSSLOT in cluster mode**: `FlowProducer.add` put every leaf child's keys into one server function call. In cluster mode, leaf children in another queue are now created first and wired to the parent the same way nested sub-flows are. Standalone still uses the single atomic call.
- **Client leaks during init**: `Queue` closed no client when the function library load failed or `close()` ran during init, and `FlowProducer` created one client per concurrent first call and never retried a failed load.
- **`searchJobs()` without a state returned phantom jobs**: the key scan did not escape glob characters in queue names and matched per-subscription and usage-lock sub-keys. Only real job hashes are returned now.
- **Removing a flow child stranded its parent**: `Job.remove()` on a child that had not completed never resolved the parent's dependency, so the parent stayed in `waiting-children` forever. Removed children now resolve same-queue and DAG parents, and cross-queue parents through the retryable notification path. Debounce replacing a flow child does the same, after the replacement is registered.
- **`drain()` blocked ordered groups**: drained ordered jobs left their sequence open, so later jobs with the same ordering key waited forever. Drain now closes the ordering hole, writes the skip marker and releases a retained slot.
- **Removing an active job corrupted counters**: the worker finishing a removed job released its group slot and `list-active` a second time, and a failure with attempts left recreated a nameless ghost job. `removeJob` now clears the active stream entry, and complete, fail and stalled recovery skip jobs whose hash is gone.
- **Double runs from stale claims**: `moveToActive` activated a job in any state, so a worker holding an entry that stalled recovery had already redispatched could run the job a second time, even after it completed. Activation now requires a waiting-like state (broadcast subscriptions are exempt).
- **Retried ordered jobs broke group concurrency**: a retried failed ordered job was treated as a returning job, skipping the concurrency gate while completion still decremented the counter. Retries get a fresh ordering sequence.
- **`Job.retry()` retried jobs in any state**: it was a non-atomic pipeline that could promote an active job (double run) or leave a completed job in two sets. The new `glidemq_retryJob` requires `failed`, re-arms `ttl`, and `Job.retry()` throws `Cannot retry: <reason>` otherwise.
- **`changePriority`/`changeDelay` failed for list-held waiting jobs**: a promoted priority or LIFO job threw `not_in_stream`, and `changePriority(0)` reported success without moving it.
- **Field updates recreated removed jobs**: `updateProgress`, `updateData`, `reportTokens`, vector storage and `reportUsage` on a removed job recreated a stateless ghost hash. They now write only if the job exists and throw `Job <id> not found` otherwise.
- **List-active accounting**: jobs promoted from a priority list into the stream were treated as list-sourced (explicit `listSourced` marker now, old heuristic kept for legacy hashes), and `healListActive` no longer corrects drift from an incomplete keyspace scan.
- **Repeat-after-complete scheduler stalled on cost overflow** in the `completeAndFetchNext` priority-list path.
- **Re-upserting a `repeatAfterComplete` scheduler while its job ran started overlapping chains**: the awaiting-completion sentinel `nextRun=0` was treated as missing state, so the next tick fired a second job and reset `limit` counting. In-flight state is now kept and a new interval applies from that job's completion.
- **Scheduled jobs ignored template ordering, group limits and cost**: the tick passed empty ordering and zero limits to the server function. They now apply like `Queue.add`.
- **Bad scheduler templates failed silently forever**: an oversized or unserializable template was skipped on every tick without advancing or reporting. Upsert now validates template options, priority, size and serializability, and a stored template that still fails reports through the worker `error` event and advances `nextRun`.
- **Cron DST handling**: timezone crons with a wildcard minute or hour now fire in both copies of the repeated fall-back hour, fixed times fire once at the earlier instant (positive-offset zones resolved to the later one), and a fixed time inside a spring-forward gap fires right after the gap instead of skipping the day. This matches vixie cron and cronie.
- **Hung sandboxed jobs held pool slots forever**: a timed-out or revoked sandboxed job kept its worker until the processor replied, queued waiters for aborted jobs still ran later (alongside their retry), and an already-aborted job was still dispatched. Aborted waiters are now dropped, and a processor that has not settled 5 seconds after its abort has its worker thread terminated or child process SIGKILLed and replaced.
- **Host crash on send to a dead sandbox child**: a proxy response sent after the child exited emitted an unhandled `ERR_IPC_CHANNEL_CLOSED` that killed the worker process. Sends to disconnected children are skipped and the error listener stays attached.
- **Testing mode diverged from production**: `TestQueue`/`TestWorker` now validate job options, payload size and priority with the same helpers as `Queue.add`; persist `updateData`/`updateProgress` to the stored job; dispatch priority, then LIFO, then FIFO jobs; honor `removeOnComplete`/`removeOnFail` (`true`, count, `{ age, count }`); emit the worker `failed` event on every failed attempt, set `failedReason` and park retries in `delayed` for their backoff (`fixed`, `exponential`, jitter, `backoffStrategies`); and apply deduplication like `glidemq_dedup` (simple frees the id once the job finishes, throttle expires after `ttl`, debounce replaces a delayed job). **Behavior change**: deduplication now applies whenever a job sets `deduplication`, without `new TestQueue(name, { dedup: true })`. Pass `dedup: false` to opt out.
- **Proxy SSE leaks on early disconnect**: job, queue-event and broadcast streams registered their disconnect handlers after awaiting a blocking client or broadcast stream, so a client that dropped during setup left a blocking reader (or a `BroadcastWorker` consuming and acking with no listener) running until proxy close. Disconnect listeners are now attached before the first await and every setup step checks them.
- **Proxy opened one Valkey connection per queue name**: cached `Queue` and `Broadcast` instances now share the proxy's command client. Blocking reads still use dedicated connections.
- **Proxy leaked internal errors**: 5xx bodies now carry a generic message and the real error goes to `onError`. `/jobs/wait` no longer turns validation errors into 500, and priority/delay/promote return 400 only for known client errors.

### Changed

- **Scheduler templates reject `delay`, `deduplication` and `parent`** at upsert (the scheduler never applied them). Stored entries keep firing.
- **Server-side priority validation**: `addJob`, `dedup` and `addFlow` reject a priority that is not an integer 0-2048 with an error, as a defense behind the client checks.
- **Server function library version is `130`.** Workers and producers reload it on connect.
- **`completeAndFetchNext` no longer emits `active` events** from its priority and LIFO paths, matching the stream path and `moveToActive`. Workers still emit their local `active` event.
- **Proxy request bounds (`maxPageSize`, default 1000)**: `GET /jobs`, `/dlq` and `/suspended` without `end` (or `end=-1`) return at most `maxPageSize` items from `start`, and larger explicit spans return 400. `dlq/replay-all` replays at most `maxPageSize` per call, and `clean` rejects a `limit` above it. `POST /flows` rejects flows with more than 1000 nodes.
- **`prefetch` is capped at `concurrency`** (`concurrency * batch.size` in batch mode). Prefetch above concurrency ran more processors than `concurrency` allowed, or left entries without heartbeats to be reclaimed and run twice.
- **Producer priority errors** are now plain `Error`s with the same messages as `Queue.add`, instead of `GlideMQError`.
- **`Job.retry()` only retries failed jobs** and throws on any other state, as its documentation already stated.
- **Cron day matching uses OR when both day-of-month and day-of-week are restricted**, like standard cron and cron-parser: `0 0 1 * 1` fires on every 1st and every Monday, not only on Mondays that fall on the 1st.
- **Scheduler templates reject `jobId`** at upsert (a fixed id deduplicated every fire). Stored legacy entries keep working.
- **Re-upserting an in-flight `repeatAfterComplete` scheduler no longer fires immediately.** Remove and re-add the scheduler to force a run.

### Security

- **Dev dependencies**: `vitest` and `@vitest/coverage-v8` upgraded to 4.1.11 (path traversal via `@vitest/mocker` redirect mocks) and `@humanfs/node` to 0.16.8 (recursive copy followed symlinks). Test tooling only; no runtime dependency changed.

### Performance

- **Batch workers** pipeline `moveToActive` and completion calls: 2 round trips per batch instead of 2 per job. A failing command no longer stops the rest of the batch; that entry is left for stalled recovery.
- **Heartbeats** send the `lastActive` write and the revoke check in one round trip.
- **List-active scans** (heal, list stall reclaim, active list job lookup) read a same-slot `list-active-ids` set instead of scanning the keyspace, falling back to SCAN when the set is incomplete (legacy workers).
- **Fewer hash reads** in `complete`, `completeAndFetchNext`, `moveToActive`, `addJob` and `dedup`, and `removeOnComplete`/`removeOnFail: true` skip terminal writes to a hash deleted in the same call.

### Documentation

- **Agent skills rewritten for current models**: short intent-based trigger descriptions, no trigger-phrase lists or impact-priority tables, one references table, and notes on the behavior that differs from expectations. Skill metadata versions now match the package (0.15.5).
- **Skill fixes found against the code**: the Bee-Queue guide logged `job.returnValue`, which does not exist (the completed event passes the result as its second argument); the BullMQ guide implied `waitUntilFinished()` returns the job result (it resolves to `'completed'` or `'failed'`), called `searchJobs()` full-text search (it filters by state, exact name and shallow data fields), and listed error messages glide-mq never emits; the Bee-Queue guide called Bee-Queue unmaintained since 2021 and untyped (2.0.0 shipped in December 2025 with bundled types) and showed an ioredis client (Bee-Queue uses node-redis). The glide-mq skill no longer carries the contributor-only `customCommand` rule. Skill example tests now assert the completed-event result and cover `waitUntilFinished()`.

---

## [0.15.5] - 2026-08-29

### Fixed

- **Reconnect rebuilt Scheduler without lockDuration**: after a connection error, stall reclaim fell back to 30s while heartbeats still used the worker lock, so healthy long jobs were redispatched.
- **Atomic list reservation**: priority/LIFO workers now increment `list-active` in `glidemq_popListsReserve`, while legacy `glidemq_popLists` remains non-reserving for rolling compatibility. New workers fall back to the legacy pop plus typed `INCRBY` when the reservation function is unavailable.
- **`Queue.pause()` did not stop workers**: pause only wrote `meta.paused=1`. Activation paths (`moveToActive`, `completeAndFetchNext`, `popLists`, `rpopAndReserve`) never read it, so workers kept claiming jobs. Pause-race claims restore LIFO/priority lists in their original dispatch order; batch restores preserve claim order; broadcast claims stay in the subscription PEL, and stalled reclaim skips paused queues.
- **Revoked active jobs could complete**: the Lua source library now rejects completion after a revoke, workers abort the affected processor, and batch workers keep each job's abort signal isolated. Batch processor failures for revoked jobs are terminal, while batch timeouts still retry according to job attempts.
- **Stalled recovery cursor**: `glidemq_reclaimStalled` now persists a per-consumer-group `XAUTOCLAIM` cursor in queue metadata and bounds each reclaim batch. Schedulers follow full pages with a guarded yielding continuation instead of waiting another stalled interval.
- `getJobs('waiting')` now follows worker dispatch order across priority, LIFO, and FIFO sources, excludes pending stream entries, and does not expose revoked or removed list jobs.
- Job removal and revocation scan the FIFO stream only when the waiting job was absent from both list-backed sources, avoiding unnecessary O(stream-length) work without orphaning stream entries whose legacy source fields are stale.
- **Queue pause now expires suspended jobs**: `Queue.pause()` immediately sweeps suspended jobs whose timeouts have elapsed, so pausing does not leave expired human-in-the-loop jobs pending until the background sweep.
- **Nested and DAG cross-queue parents could remain in `waiting-children`**: parent wiring now avoids cross-slot FCALLs, preserves nested parent hash fields, retries idempotent notifications from the child slot, reconciles removed children without recreating hashes, and ignores stale notifications for deleted parents. Completion returns newly queued cross-queue edges for eager delivery while deduplicating overlapping tree and DAG metadata.
- Reconnected workers and workflow helpers retain live clients for the lifetime of returned jobs without leaking helper-owned connections.
- List-job delay, suspend, resume, discard, explicit failure, and unrecoverable-error transitions now preserve terminal intent and the correct claim identity across chained resumes.
- Token-bucket refill uses the Redis server clock consistently and normalizes legacy future timestamps, preventing idle buckets from remaining underfilled after caller-clock skew.
- `TestWorker` partial batches flush after their timeout even when the batch never reaches its configured size.
- Empty dependency sets no longer leave jobs stranded in `waiting-children`.

---

## [0.15.4] - 2026-06-04

### Fixed

- **Rate-limited group tombstones**: promotion now skips deleted token-bucket waitlist entries, advances both ordering frontiers, and continues to the next valid job. Cleanup is bounded and re-registers the group when more entries remain.
- **Stalled repeat-after-complete jobs**: terminal stalled recovery now advances the linked scheduler atomically instead of leaving it stuck at `nextRun=0`.
- **Ordered-group holes after pre-activation removal**: debounce replacement, explicit removal, and TTL expiry now mark never-run ordered jobs as skipped and wake parked successors. Group rate-limit requeues retain their ordered slot, and priority-list fast fetches apply the same token-bucket and rate gates as stream fetches.
- **`rateLimitGroup({ currentJob: 'fail' })` promoted successors before the pause**: the fail path decremented `active` and promoted the next sequence before recording `ratelimited`. Promotion now waits for `promoteRateLimited`; returning jobs requeued at the back are found past the waitlist head, and oversized priority jobs close their ordering hole and count toward failed metrics.
- **Rejected token-bucket jobs consumed IDs and ordering sequences**: add, dedup replacement, and flow creation now validate cost before mutating queue state. Revoking an ordered job before activation also closes its sequence hole so successors can run.
- **Interval scheduler drift accumulation**: `every` schedulers now advance from the previous due slot instead of the late worker tick timestamp, so CI/event-loop jitter does not accumulate drift over repeated firings. Missed slots are skipped rather than replayed.
- **Release test command**: `npm test` now passes the fuzzer exclusion as a single Vitest argument, so the release gate runs the intended non-fuzzer suite.
- **Ordered-group rate-limit recovery**: every retained-slot job is tracked independently, so concurrent requeues resume before successors. Terminal paths release retained slots, and oversized token-bucket head cleanup is iterative and bounded.

### Changed

- **Valkey CI images**: CI and local compose coverage now use stable Valkey 9.1.0 images instead of release-candidate images.

---

## [0.15.3] - 2026-05-18

### Fixed

- **DAG `deps` direction inverted** (#244): `DAGNode.deps` was documented as "nodes that must complete before me" but the implementation submitted in deps-first topo order with each dep using its first downstream node as the BullMQ `parentId`, which inverted the semantics. The fix submits in reverse-topological order, treats nodes with `deps` as `waiting-children` parents, and wires every dependent through the parents SET so `deps` now matches the documented direction. `LIBRARY_VERSION` bumped to `88`.

- **Proxy `/flows/:id/tree` rendered DAG flows upside down** (#245): the tree builder used flow-tree semantics (children listed under their parent) for both `tree` and `dag` flow kinds. For DAGs that produces an inverted graph - leaves appear as roots. `buildFlowTreeNodes` now branches on `FlowKind`: for `dag` it uses each node's `parentIds` directly so prerequisites render under the dependent that waits for them.

- **DAG multi-dependent leaf race in `addDAG`** (#246): when a leaf had >1 dependent, the previous wiring piggybacked the first dependent on `addJob`'s atomic `parentId`/`parentDepsKey` and wired the rest via `registerParent`. Workers fetching the leaf between Phase A and Phase B saw `parentIds=undefined` and `completeAndFetchNext` skipped `SMEMBERS A.parents`, leaving subsequent dependents stuck in `waiting-children`. Multi-dependent leaves now submit with no parent fields and wire every dependent through Phase B. Additionally, the `hasParents` arg to `glidemq_completeAndFetchNext` (which gated the `SMEMBERS` on a stale worker snapshot) was dropped - the SET is now always read at completion time. `LIBRARY_VERSION` bumped to `93`.

- **Stalled-job redispatch under threshold** (#242): when a job stalled but its `stalledCount` was still under `maxStalledCount`, `glidemq_reclaimStalled` left the entry parked in the scheduler PEL instead of redispatching to a healthy worker. The redelivery contract in `DURABILITY.md` now holds: stalled entries are ACK+DEL'd and re-XADD'd back to the stream under threshold, only failing once `stalledCount > maxStalledCount`. Aligns with BullMQ semantics.

- **`addFlow` ID-collision races** (#234): `glidemq_addFlow` re-checks `EXISTS` for custom child IDs and skips auto-`INCR`'d parent/child IDs that collide with existing custom-ID jobs - mirrors the same guard the `addJob` path already had, so flows survive shared `idKey` state.

- **Ordering skip-marker advancement unbounded** (#222): debounce-induced skip markers were walked unboundedly per FCALL, which could OOM-trip Lua on large gaps. Skip-marker advancement is now bounded per call; the remaining markers are picked up on the next gate evaluation.

- **Serverless pool credential cache key collisions** (#229, #241): the pool's cache previously bypassed the cache when credentials were present (#229), and then hashed credentials into the cache key so distinct credential sets get distinct entries instead of leaking across tenants (#241).

- **Expired jobs not counted against promote budget** (#235): `glidemq_promoteGroupQ`'s loop budget was decremented only for promotions, so a queue full of expired entries would saturate the budget on expires without ever promoting real jobs. Expired jobs now consume budget too.

- **Group promote loop could iterate unbounded under `maxConcurrency`** (#236): when a group was already at `maxConcurrency`, the promote loop kept popping the waitlist without making progress. Cap added so the loop terminates after one full pass.

- **Long-running jobs marked stalled** (#238): workers now emit periodic heartbeats during job execution so `lockDuration` is honored against actual wall-clock activity, not the time since the worker last polled.

- **Broadcast retries fanned out to healthy subscribers** (#231): a failing subscription's retry attempts re-delivered the message to every other subscriber. Retries are now isolated to the failing subscription via per-subscription PEL tracking.

- **`list-active` counter leaks** (#230): non-processing outcomes of `moveToActive` (revoked, expired, ordering-deferred) failed to `DECR` the `list-active` counter they incremented on the way in.

- **Batch-mode worker over-fetched stream entries** (#233): `XREADGROUP count` was uncapped, so `concurrency * batch.size` was claimed even when only `batch.size` could be processed. Count is now clamped.

- **Heartbeat started after token-limiter wait** (#224): if the token bucket forced a wait before processing, the worker had no heartbeat during the wait and could trigger its own reclaim. Heartbeat now starts before the wait.

- **Proxy accepted unsupported opts keys** (#232): job-add requests with unknown `opts` keys (typos, future fields) were silently accepted and ignored. The proxy now rejects them so client/server schema drift fails loud.

- **Suspended-job cleanup on timeout** (#226): timed-out suspended jobs were missing some cleanup paths (groupq waitlist entry, ordering meta), leaving stale state. Cleanup is now exhaustive.

- **Per-job `lockDuration` validation** (#225): `opts.lockDuration` was not validated; non-finite, negative, or extreme values would corrupt the stall threshold. Now validated and clamped.

- **`Queue.getClient` initialization race** (#227): concurrent first-time access could initialize two clients. Now guarded by a single-flight promise.

- **Cross-queue DLQ shared entries** (#223): DLQ entries were scoped only by job ID, so two queues using the same ID would see each other's DLQ rows. Now scoped to the owning queue.

- **Duplicate stalled-list recovery across schedulers** (#228): with multiple schedulers running, list-backed stalled recovery could run concurrently and double-recover. Now deduped via a coordination key.

### Performance

- **`addDAG` round trips collapsed from O(N) to O(levels)** (#246): submissions are now grouped by topological level and pipelined within each level via non-atomic batch. Bench (5-run median, wide diamond, local Valkey/cluster): N=4 went from ~25 ms to ~0.5 ms (~50x); N=50 went from ~12-16 ms to ~3-4 ms (~4x). Each FCALL stays atomic individually; the race semantics are preserved by `registerParent`'s `already_completed` path.

- **Large-collection deletes use UNLINK** (#243): job hashes, retention purge, `glidemq_clean` batches, and `glidemq_drain` stream/zset/lifo/priority sweeps now use `UNLINK` instead of `DEL`. UNLINK keeps in-script atomicity (the keyspace removal is still synchronous from the script's view) but defers memory reclamation to the bio thread, so obliterate / retention / drain stop blocking the server thread on MB-sized job hashes. Small-key DELs (lockKey) kept as DEL.

### Security / dependencies

- **CVE fixes via `npm audit fix`** (#240): resolved CVEs in transitive deps `langsmith` and `protobufjs`.

### Examples

- Added `ai-research-dag` to the [examples catalog](https://github.com/avifenesh/glidemq-examples/tree/main/examples/ai-research-dag) - a 6-stage AI research pipeline (`plan -> 3x search -> synthesize -> review`) that uses `flow.addDAG()`, `job.reportUsage()`, and per-stage cost aggregation. Mocked LLM calls, no API keys required.

---

## [0.15.2] - 2026-05-08

### Fixed

- **Priority/LIFO jobs in batch-mode workers** (#212, #216): `Worker.tryPopFromLists` dispatched list-popped jobs through the single-job processor, which is a throwing sentinel in batch mode (`Single-job processor called in batch mode`). Any job enqueued with `priority` (or `lifo: true`) to a batch worker would fail. List-popped jobs are now routed through `activateAndProcessBatch`, chunked by `batch.size` so concurrency > 1 doesn't overflow the user's contract.

- **`list-active` counter underflow on duplicate complete/fail/reclaim races** (#217, #218): 10 unguarded `DECR list-active` sites in the Lua FCALL library would underflow the per-queue counter on any path that produced two DECRs for one INCR (duplicate `complete`/`fail`, reclaim race, suspend-then-fail). Once underflowed, `getJobCounts()` returned negative `active` and `glidemq_healListActive` couldn't recover (its `<= 0` early-out is intentional - it only repairs positive drift). All 12 DECR sites now route through a single `decrListActive(listActiveKey)` Lua helper that guards on `> 0`. Note: counters that already underflowed under v0.15.1 are not auto-repaired; drain the queue or manually `SET <prefix>:list-active 0`. `LIBRARY_VERSION` bumped to `82`.

- **Priority/LIFO active jobs invisible in dashboard, list-backed jobs reclaimed despite worker `lockDuration`** (#213, #219): two related bugs.
  - `Queue.getJobs('active')` only read the stream consumer-group PEL, so priority/LIFO active jobs were invisible (the count and the listing disagreed by exactly the list-backed count). Added `glidemq_getActiveListJobIds` Lua FCALL (bounded SCAN, cluster-safe via `{queueName}` hash tag) and merged into `getJobs('active')`. Pre-existing batched-xrange shape mismatch in `resolveActiveJobIds` also fixed - stream-PEL → jobId resolution had been silently returning empty since speedkey changed format.
  - The scheduler conflated `stalledInterval` (cadence) with the stall threshold; both reclaim FCALLs received `stalledInterval` as `minIdleMs`. With `{ lockDuration: 300_000, stalledInterval: 30_000 }`, jobs were reclaimed after 30s despite the 5min lock. The contract is now restored: `lockDuration` is the threshold, `stalledInterval` is the cadence. `glidemq_reclaimStalled` and `glidemq_reclaimStalledListJobs` accept a new `workerLockDuration` arg; per-entry threshold falls back to it before `minIdleMs`. Per-job `opts.lockDuration` overrides still win. `LIBRARY_VERSION` bumped to `84`.

### Behavior change

- Workers that previously relied on a short `stalledInterval` for fast stall recovery (without setting `lockDuration`) will see slower recovery. Default `lockDuration` is 30s, so the new threshold is 30s for those configurations. To preserve the old behavior, set `lockDuration` explicitly to match `stalledInterval`.

---

## [0.15.1] - 2026-04-06

### Fixed

- **`debounce` + `ordering.key` deadlock** (#206): when debounce cancelled a pending ordered job, the deleted sequence created a permanent `nextSeq` gap that blocked all subsequent jobs in the group. Fixed via lightweight skip markers (`skip:<seq>` on the group hash) resolved lazily at all five ordering gates. `LIBRARY_VERSION` bumped to `81` - existing standalone clients reload the fix automatically on next connection.

---

## [0.15.0] - 2026-04-02

### Added

- **HTTP proxy parity expansion** (#192): queue-wide events SSE, per-job lifecycle SSE, `jobs/wait`, workers, metrics, scheduler CRUD, rolling usage summary, broadcast publish/SSE, DLQ inspection/replay, suspended-job inspection, revoke, and queue global rate-limit HTTP management.
- **Flow HTTP API** (#205): `POST /flows`, `GET /flows/:id`, `GET /flows/:id/tree`, and `DELETE /flows/:id` for tree flows and DAGs, with flow inspection responses that include usage, budget, roots, and node state.
- `queue.getUsageSummary()` plus `/usage/summary` for time-windowed usage aggregation across queues.

### Changed

- Examples now live in the dedicated `glidemq-examples` repository, and the docs/skills/integration guides were refreshed to point at the new example catalog and current proxy surface.

### Fixed

- **Suspend timeout enforcement no longer depends on the original worker staying alive** (#193). Timed-out suspended jobs are now swept by any live glide-mq runtime with a connected `Queue` or `Worker`.
- Flow HTTP internals now handle cross-queue parent references correctly, use cluster-safe flow record keys, clean up SSE readers on proxy shutdown, and avoid DLQ pagination/replay gaps caused by deleted stream entries.
- CI cluster bootstrap now installs plain `valkey-server` / `valkey-cli` binaries for the cluster path while keeping `valkey-bundle` for standalone/search coverage.

## [0.14.0] - 2026-03-28

### Breaking Changes

- **JobUsage redesigned**: `inputTokens`/`outputTokens` replaced with `tokens: Record<string, number>` for extensible category tracking (input, output, reasoning, cachedInput, etc.)
- **Cost tracking redesigned**: `costUsd` replaced with `costs: Record<string, number>` + `costUnit` for currency-agnostic per-category cost tracking
- **BudgetOptions expanded**: `maxCostUsd` replaced with `maxTotalCost`. Added `maxTokens` (per-category caps), `tokenWeights` (weighted totals), `maxCosts` (per-category cost caps), `costUnit`
- **getFlowUsage return type changed**: `totalInputTokens`/`totalOutputTokens`/`totalCostUsd` replaced with `tokens`/`costs` maps + `totalTokens`/`totalCost`

### Added

- `job.streamChunk(type, content?)` - typed streaming convenience for reasoning vs content chunks
- Per-category budget enforcement with independent limits per token/cost category
- Weighted token budgets - reasoning tokens can count 4x toward budget
- `ConnectionOptions.requestTimeout` - configurable command timeout (was hardcoded 500ms)
- 9 new examples: thinking-model, cost-breakdown, budget-weighted, reasoning-stream, agent-budget-loop, multi-model-cost, fallback-usage, streaming-sse, batch-embed-tpm
- Upgraded to valkey-search 1.2 in test infrastructure (compose.yaml)
- Bumped speedkey to 0.3.0-rc1

### Fixed

- Budget bypass when only `totalTokens` reported without `tokens` breakdown
- `JSON.parse` null safety in budget and usage parsing
- Prototype pollution prevention with `Object.create(null)` in aggregation maps
- DAG cluster test flaky timeouts (15s -> 30s)
- `TestJobRecord` missing `usage` field causing empty `getFlowUsage()` in testing mode

---

## [0.13.0] - 2026-03-27

### Added

- **Structured AI metadata** (#168): `job.reportUsage({ model, tokens: { input, output }, costs: { total } })` records LLM usage on any job. `queue.getFlowUsage(flowId)` aggregates token counts and cost across an entire flow.
- **Per-job streaming channel** (#169): `job.stream(chunk)` publishes incremental data (LLM tokens, progress events) to a dedicated channel. `queue.readStream(jobId, opts?)` consumes chunks in real time. Blocking reads via XREAD BLOCK.
- **Suspend/resume with signals** (#170): `job.suspend(opts?)` pauses a job mid-processor; `queue.signal(jobId, name, data?)` resumes it with an external event. Enables human-in-the-loop approval gates, webhook callbacks, and any pattern requiring external input before a job can continue.
  - `SuspendOptions`: `reason` (label), `timeout` (auto-fail after N ms)
  - `onResume` callback: best-effort same-worker continuation called with `signals[]` on resume
  - `queue.getSuspendInfo(jobId)`: returns suspension metadata and signals delivered so far
  - `glidemq_suspend` FCALL: moves active job to suspended sorted set, releases group slot
  - `glidemq_signal` FCALL: appends signal, re-queues job to stream
  - `glidemq_sweepSuspended` FCALL: fails timed-out suspended jobs on each stalled recovery tick
  - Proxy: `POST /queues/:name/jobs/:id/signal` endpoint
  - Testing: `TestJob.suspend()` and `TestQueue.signal()` with full parity (no Valkey)
- **Per-job lockDuration override** (#172): set `lockDuration` per job to control heartbeat interval and stall detection timeout independently of the worker default.
- **Fallback chains** (#173): ordered list of model/provider alternatives via `opts.fallbacks`. On processor failure, the job automatically retries with the next fallback entry. Each fallback can override `data` and `metadata`.
- **Budget middleware** (#174): flow-level token and cost caps. Set `budget: { maxTokens, maxCost }` on a flow; jobs that would exceed the budget are failed before execution.
- **Dual-axis rate limiting (RPM + TPM)** (#175): enforce both requests-per-minute and tokens-per-minute limits on a queue. Designed for LLM API compliance where providers impose concurrent rate ceilings.
- **18 real-world AI examples** (#176): framework integrations covering LangChain, Vercel AI SDK, OpenAI, Anthropic, multi-model routing, RAG pipelines, and more.
- **Valkey Search integration** (#177): vector search over jobs using Valkey Search module. `queue.createIndex(schema, opts?)` defines indexes; `queue.search(query, opts?)` runs hybrid vector + filter queries. `IndexCreateOptions` and `SearchQueryOptions` types decoupled from speedkey.
- `SuspendError`, `SuspendOptions`, `SignalEntry` exported from public API.
- Stress tests: 38 tests for correctness under concurrent load and edge-case pressure.
- Docker: `compose.yaml` uses `valkey-bundle` image (search + json + bloom modules).
- CI: `test-search` job with `valkey-bundle` for search integration tests.

### Fixed

- OTel `SpanStatusCode` values corrected (OK=1, ERROR=2) - previously swapped.
- Signal data auto-deserialization: signals received via `onResume` are now parsed from JSON automatically.
- Fallback type uses explicit `metadata` field instead of index signature.
- `glidemq_clean` and `glidemq_drain` now delete `signals:{id}` LIST keys when removing jobs, preventing a key leak when suspended jobs time out or are cleaned after failure.

---

## [0.12.0] - 2026-03-20

### Added

- **Runtime per-group rate limiting** (#148): three complementary APIs for pausing individual ordering groups at runtime.
  - `job.rateLimitGroup(duration, opts?)` - pause from inside the processor (e.g., on 429 response)
  - `throw new GroupRateLimitError(duration, opts?)` - throw-style sugar
  - `queue.rateLimitGroup(groupKey, duration, opts?)` - pause from outside (webhooks, health checks)
  - Options: `currentJob` (requeue|fail), `requeuePosition` (front|back), `extend` (max|replace)
- **Ordering path unification** (#158): all `ordering.key` jobs now route through the group path with implicit `concurrency: 1`. Enables group features (runtime rate limiting, token bucket) for all ordering-key users.
  - ZSET groupq for ordered promotion (score = orderingSeq)
  - `nextSeq` counter on group hash gates all 6 activation paths
  - Step-jobs hold ordering slot until full completion
  - Returning step-jobs bypass concurrency/rate gates
- `GroupRateLimitError` and `GroupRateLimitOptions` exported from public API.
- `BroadcastWorker.waitUntilReady()` method (#149).
- Queue/Producer option `events: false` to skip XADD 'added' event emission on job add.

### Performance

- **HMGET consolidation in `completeAndFetchNext`**: merge 4 separate hash lookups into 1 HMGET. Reduces redis.call()s from 13 to 10 on hot path.
- **Remove auto-ID EXISTS check**: monotonic INCR cannot collide. Saves 1 redis.call() per add.
- **Parallel resource cleanup** in test fixtures (#151).
- **Multi-key DEL** for queue obliteration (#154).
- TS-side micro-optimizations: `withSpan` lazy attributes, `Buffer.byteLength` skip, cached retention objects.

### Fixed

- `Broadcast.publish()` signature documented correctly - subject is first arg (#152).
- DLQ configuration location clarified in docs (#153).
- `addBulk` dedup batch paths correctly pass `skipEvents`.
- `advanceIdCounter` avoids Lua float precision loss on large IDs.
- `flatted` dependency bumped to resolve prototype pollution vulnerability.

### Breaking

- `groupq` key type changed from LIST to ZSET. Existing groups with queued jobs need migration (drain before upgrade). Pre-stable, acceptable.

---

## [0.11.0] - 2026-03-10

### Added

- Subject-based filtering in `BroadcastWorker` via `opts.subjects` glob patterns. Non-matching messages are auto-ACKed and skipped.

### Fixed

- Timer leak in `runProcessor`: `setTimeout` handle is now cleared when the processor resolves before the timeout, preventing orphaned timers under high throughput.
- `glidemq_revoke` and `glidemq_searchByName` now paginate XRANGE with COUNT 1000 instead of loading the entire stream into Lua memory. Prevents memory pressure and event loop blocking on large streams.
- `getJobCounts()` now accounts for list-sourced active jobs (via `list-active` counter) and LIFO/priority list lengths in the waiting count. Previously under-reported active and over-reported waiting when LIFO/priority jobs were in flight.
- `isPaused()` now handles `GlideString` (Buffer) returns correctly via `String()` conversion. Previously could always return `false` when the client returned Buffer values.

---

## [0.10.0] - 2026-03-09

### Performance

- **~108% throughput improvement at c=1** (~1,300 -> ~2,700 jobs/s) by eliminating wasted HMGET round-trip in `buildParentInfo` for non-parent jobs (#126).
- **~9-16% throughput improvement at c=10** (~12,900 -> ~14,000-15,000 jobs/s) via combined hot-path optimizations (#126).
- New `glidemq_popLists` server function: checks priority + LIFO lists in a single FCALL instead of 2 separate RPOPs.
- `completeAndFetchNext` Lua fast-path hints: `processedOn` timestamp passed from TS (skip HGET), `'__'` sentinels for ordering/group keys (skip entire Lua call when confirmed absent), `hasParents` flag (skip SMEMBERS for non-DAG jobs).
- `Date.now()` cached once per job completion, shared across completeAndFetchNext, finishedOn, and scheduler callbacks.

### Added

- Worker options `events: false` / `metrics: false` to skip XADD event stream writes and HINCRBY metrics recording in Lua on the hot path. TS-side EventEmitter (`'completed'`, `'failed'`, etc.) is unaffected. Reduces Valkey memory and CPU for high-throughput deployments that don't consume server-side events or metrics (#126).
- `glidemq_reclaimStalledListJobs` - stall detection for LIFO/priority list-sourced jobs via bounded SCAN. Detects orphaned active list jobs when workers crash (#129).

### Fixed

- `rpopAndReserve` now accepts a `count` argument for batch popping under globalConcurrency - fixes the limitation of popping 1 job at a time (#128).
- `deferActive` now DECRs `list-active` counter for list-sourced jobs, preventing counter drift on defer (#128).
- Ordering key `'__'` is now rejected as reserved (internal sentinel collision guard).
- `hasParents` flag narrowed to DAG-only `parentIds` - saves one SMEMBERS call for single-parent flow jobs.
- Version changelog comments in Lua library corrected (v59-v64 entries).

### Changed

- Function library version bumped from 60 to 67 (auto-upgrades on connection).
- Benchmark durations increased (ADD_DURATION 5s->15s, PROCESS_SIZES [500,2000]->[5000,20000]) for more stable measurements.

---

## [0.9.0] - 2026-03-08

### Added

- Subject-based filtering for `BroadcastWorker` - NATS-style wildcard matching on job names. Configure via `subjects` option with `*` (single-token) and `>` (multi-token) wildcards. Non-matching messages are auto-acknowledged. Exported `matchSubject` and `compileSubjectMatcher` utilities (#119).
- `Producer` class - lightweight job enqueuing for serverless/edge environments without EventEmitter or Job instances. Returns plain string IDs. Use with `ServerlessPool` for automatic connection reuse across warm Lambda/Edge invocations. API: `add(name, data, opts)`, `addBulk(jobs)`, `close()` (#112).
- `ServerlessPool` and `serverlessPool` singleton - connection pooling for serverless environments. Caches Producer instances by queue name and connection fingerprint. API: `getProducer(name, opts)`, `closeAll()`, `size` (#112).
- LIFO (Last-In-First-Out) job processing via `lifo: true` option. Uses dedicated Valkey LIST with RPUSH/RPOP. Priority and delayed jobs take precedence. Cannot be combined with ordering keys (#87).
- Time-series metrics - `queue.getMetrics(type, opts?)` returns per-minute throughput and latency data with 24-hour retention. Zero extra RTTs (#82).
- `opts.jobId` - custom job IDs for deterministic identity. Max 256 characters (#79).
- `queue.addAndWait(name, data, { waitTimeout })` - enqueue and wait for completion without polling.
- `job.moveToDelayed(timestampMs, nextStep?)` - pause active job mid-processor for step-job workflows.
- `DelayedError` - exported error type for step-job control.
- Batch processing via `batch: { size, timeout? }` option. Processor receives `Job[]`, returns `R[]`. `BatchError` for partial failure (#81).
- `glide-mq/proxy` subpath - HTTP proxy for cross-language job enqueue. REST endpoints with queue allowlist, 1MB limit, graceful shutdown (#83).
- Wire protocol documentation (`docs/WIRE_PROTOCOL.md`) - raw FCALL reference for any language (#83).
- DAG workflows - `FlowProducer.addDAG()` and `dag()` helper for arbitrary DAG topologies (#86).
- Serverless usage guide (`docs/SERVERLESS.md`) - Lambda, Cloudflare Workers, Vercel Edge examples.
- List-active counter self-healing via `glidemq_healListActive` Lua function. Automatically corrects counter drift caused by worker crashes during scheduler promotion ticks (#124).
- Proxy endpoint: `GET /queues/:name/jobs/:id` (fetch single job by ID) (#124). Note: `GET /queues/:name/jobs` (list/filter) and `DELETE /queues/:name/jobs/:id` (remove) were planned but not implemented.
- CI: `npm audit` security scanning, `timeout-minutes` on all jobs, `npm ci` with cache in publish workflow (#124).

### Fixed

- 62 issues from deep project audit across 7 domains (security, performance, code quality, architecture, testing, backend, devops) (#124):
  - **Critical**: Worker heartbeat unhandled rejections, proxy validation gaps (NaN/Infinity), proxy queue cache race condition, poll loop promise handling on close, cross-queue parent registration error handling.
  - **Security**: Sandbox path traversal protection via `realpathSync`, proxy input validation with `Number.isFinite`, queue name length limit (256 chars).
  - **Performance**: Lua metrics HKEYS scan frequency reduced 10x, token bucket early exit, DAG string parsing O(n) to O(1).
  - **Reliability**: Worker/Producer `close()` with double-close guard and closed flag, `QueueEvents` recursive poll guard, sandbox pool exit/error listener cleanup, serverless pool closing state guard.
  - **Proxy**: Configurable `onError` callback (replaces silent error swallowing), graceful shutdown with draining flag, pause/resume returns 200 with state.
- `globalConcurrency` enforced for LIFO/priority-list jobs via atomic `rpopAndReserve` (#87).
- Scheduler LIFO forwarding and FlowProducer child LIFO routing (#87).
- `list-active` counter DECR on job removal/deferral (#87).
- Function library bumped to version 60.

### Changed

- **Breaking (internal)**: `Worker` and `BroadcastWorker` now extend `BaseWorker` abstract class. Public API unchanged. Eliminates ~1400 lines of duplication (3407 to 2024 lines, 41% reduction) (#124).
- Worker uses explicit state machine (7 states: created, initializing, running, paused, draining, closing, closed) replacing boolean flags (#124).
- Proxy pause/resume endpoints return 200 with `{ paused: boolean }` instead of 204 (#124).
- Proxy health endpoint includes `queues` count (#124).
- Test suite: 24 hardcoded `setTimeout` waits replaced with `waitFor` predicates (#124).
- 79 eslint `no-unused-vars` warnings resolved across test files (#124).

---

## [0.8.1] - 2026-02-27

### Security

- Reject invalid cron patterns: zero step (`*/0`), out-of-bounds values, reversed ranges, malformed tokens (#56).
- Enforce 1MB payload limit on job data, progress, and logs using `Buffer.byteLength` for correct UTF-8 byte counting. Covers `add`, `addBulk`, `updateData`, `updateProgress`, and `log` (#61).
- Fix path leak in sandbox error messages (#54).

### Performance

- Hierarchical cron search replacing brute-force minute iteration - 4400x speedup for yearly schedules. UTC-correct date handling, 10-year search horizon (#59).
- Batch Redis commands in `Job.retry()` and `updateProgress()` (#53).

### Added

- Comprehensive local fuzzer with pre-push hook.

### Docs

- Dashboard section in README, feature map improvements (#57, #58).

---

## [0.8.0] - 2026-02-23

### Added

- `queue.getJobScheduler(name)` - fetch a single scheduler entry by name. Returns `SchedulerEntry | null` with the schedule configuration (pattern/every), job template, and next run timestamp. Completes the scheduler API alongside `upsertJobScheduler`, `getRepeatableJobs`, and `removeJobScheduler` (#51).
- `queue.getWorkers()` - list all active workers for the queue. Returns `WorkerInfo[]` with id, addr (hostname), pid, startedAt, age (ms), and activeJobs count. Workers register with TTL-based heartbeat keys that auto-expire on crash (#49).
- `queue.drain(delayed?)` — remove all waiting jobs from the queue without touching active jobs. Pass `true` to also remove delayed/scheduled jobs. Implemented as a single Valkey Server Function call; emits a `'drained'` event (#41).
- `TestQueue.drain(delayed?)` — in-memory equivalent; removes waiting (and optionally delayed) jobs from `TestQueue`.
- `active` event on `Worker` and `TestWorker` — emitted with `(job, jobId)` when a job starts processing (#38).
- `drained` event on `Worker` and `TestWorker` — emitted when the queue transitions from non-empty to empty. A new `isDrained` flag prevents repeated emissions (#38).
- `queue.clean(grace, limit, type)` — bulk-remove old `completed` or `failed` jobs by minimum age. Returns an array of removed job IDs. Implemented as a single Valkey Server Function call (#39).
- `job.discard()` — immediately move an active job to failed state, bypassing retries (#40).
- `UnrecoverableError` — throw this error class inside a processor to skip all remaining retry attempts and fail the job permanently (#40).
- `job.changePriority(newPriority)` — re-prioritize a waiting, prioritized, or delayed job after enqueue. Setting priority to `0` moves it back to the normal stream. Throws if the job is active, completed, or failed (#43).
- `job.changeDelay(newDelay)` — mutate the fire time of a delayed job after enqueue. Setting delay to `0` promotes immediately (to waiting or prioritized depending on priority). Setting delay > 0 on a waiting or prioritized job moves it to the scheduled ZSet. Throws if the job is active, completed, or failed (#45).
- `job.promote()` — move a delayed job to waiting immediately. Always moves to the waiting stream regardless of priority (unlike `changeDelay(0)` which preserves priority scheduling). Priority metadata is kept in the job hash. Throws if the job is not in the delayed state (#46).
- `queue.retryJobs(opts?)` — bulk-retry failed jobs in a single Valkey Server Function call. Pass `{ count: N }` to limit the number of jobs retried, or omit to retry all. All retried jobs go to the scheduled ZSet (the promote cycle moves them to the stream). Returns the count of retried jobs (#47).

### Performance

- Batch `getChildrenValues` for O(1) network trips (#50).
- Batch scheduler operations into single pipeline RTT.

### Fixed

- `job.retry()` — now removes the job from the failed ZSet before adding to scheduled, and resets `attemptsMade` and `finishedOn`.
- Sanitize stack traces in sandbox runner (#44).
- Replace hardcoded sleeps with `waitFor` in flaky CI tests (#48).

### Changed

- CI pipeline rewrite from scratch.
- ESLint + Prettier with TypeScript support.
- Prettier formatting applied to src/ and tests/.

---

## [0.7.0]

### Added

- Sandboxed processor: run worker processor in a child process or worker thread (`sandbox: {}` option). Protects the main process from processor crashes and memory leaks (#36).
- Sandbox pool stress tests (#37).

### Fixed

- Resolved 4 source bugs and 12 flaky test files (#34).

---

## [0.6.0]

### Added

- Comprehensive README rewrite: star CTA, install command, expanded feature list, differentiators (#35).

---

## [0.4.0]

Initial public release on npm.

- Valkey Server Functions for all queue operations (FUNCTION LOAD + FCALL).
- Hash-tagged keys for cluster compatibility.
- XREADGROUP + consumer groups + PEL for at-least-once delivery.
- `completeAndFetchNext` single-RTT job transition.
- FlowProducer workflows: `chain`, `group`, `chord`.
- Schedulers: cron and interval repeating jobs.
- Rate limiting, deduplication, compression, retries, DLQ.
- Per-key ordering, global concurrency, job revocation.
- QueueEvents stream-based lifecycle events.
- In-memory `TestQueue` and `TestWorker` (no Valkey needed).
- OpenTelemetry tracing, per-job logs.
