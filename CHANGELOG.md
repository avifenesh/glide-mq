# Changelog

All notable changes to glide-mq are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [Unreleased]

### Added

- LIFO (Last-In-First-Out) job processing order via `lifo: true` option. Jobs are processed in reverse-chronological order (newest first). Uses a dedicated Valkey LIST with RPUSH/RPOP for efficient LIFO fetching. Priority and delayed jobs take precedence over LIFO. Cannot be combined with ordering keys. Supported in `Queue.add`, `Queue.addBulk`, and `FlowProducer` (#87).
- Time-series metrics - `queue.getMetrics(type, opts?)` now returns per-minute throughput and latency data in addition to total count. Returns `{ count, data: MetricsDataPoint[], meta }` with minute-resolution buckets retained for 24 hours. Optional `MetricsOptions` parameter supports slicing data points (`start`/`end`). Metrics are recorded server-side in Valkey functions with zero extra RTTs. `TestQueue.getMetrics()` mirrors the same API in testing mode (#82).
- `opts.jobId` - custom job IDs for deterministic job identity. Max 256 characters; must not contain control characters, colons, or curly braces. `Queue.add` returns `null` on duplicate (silent skip); `FlowProducer.add` throws on duplicate since flows cannot be partially created. Supported on `Queue.add`, `Queue.addBulk`, and `FlowProducer.add`. `TestQueue` mirrors the same behaviour in testing mode (#79).
- `queue.addAndWait(name, data, { waitTimeout })` - enqueue a job and wait for the matching completed/failed event on the queue events stream without polling the job hash.
- `job.moveToDelayed(timestampMs, nextStep?)` - pause an active job mid-processor and resume it later from the scheduled set. Supports step-job workflows and updates `job.data.step` atomically for plain object payloads.
- `DelayedError` - exported error type for advanced/manual step-job control.
- Batch processing - workers can process multiple jobs at once via `batch: { size, timeout? }` option. Processor receives `Job[]` and returns `R[]`. Use `BatchError` for per-job partial failure reporting. Supported in both Worker and TestWorker (#81).
- `glide-mq/proxy` subpath export - HTTP proxy for cross-language job enqueue. Seven REST endpoints (add, bulk add, get job, pause, resume, counts, health) with queue allowlist, 1MB payload limit, and lazy Queue caching. Requires `express` as an optional peer dependency (#83).
- Wire protocol documentation (`docs/WIRE_PROTOCOL.md`) - complete reference for enqueuing and managing jobs from any language using raw FCALL commands. Covers all key layouts, FCALL signatures, priority encoding, compression format, and examples in Python and Go (#83).
- DAG workflows - `FlowProducer.addDAG()` method and `dag()` helper for arbitrary DAG topologies. Each node can declare multiple dependencies via the `deps` array; a job only becomes runnable once all dependencies have completed. Use for fan-in merge scenarios, diamond dependencies, or multi-stage pipelines that converge. See `docs/WORKFLOWS.md` for examples (#86).
- `Producer` class - lightweight job enqueuing for serverless/edge environments without EventEmitter or Job instances. Returns plain string IDs. Use with `ServerlessPool` for automatic connection reuse across warm Lambda/Edge invocations. API: `add(name, data, opts)`, `addBulk(jobs)`, `close()`. Reuses the same battle-tested `addJob`/`dedup` FCALL functions as Queue (#89, #116).
- `ServerlessPool` and `serverlessPool` singleton - connection pooling for serverless environments. Caches Producer instances by queue name and connection fingerprint to reuse connections across warm invocations. Injected clients bypass caching to prevent key collisions. API: `getProducer(name, opts)`, `closeAll()`, `size` (#89).
- Serverless usage guide (`docs/SERVERLESS.md`) - comprehensive guide covering Producer API, ServerlessPool patterns, and examples for AWS Lambda, Cloudflare Workers, and Vercel Edge Functions. Demonstrates both ephemeral and connection-reuse patterns for serverless job enqueueing (#89).

### Fixed

- `globalConcurrency` now enforced for LIFO and priority-list jobs. `glidemq_rpopAndReserve` atomically checks capacity, pops from the list, and increments `list-active` in a single FCALL. Non-atomic path (no global concurrency) uses `rpopCount` for batch pops under high concurrency. `complete` and `fail` functions DECR `list-active` on list-sourced jobs to keep the counter balanced (#87).
- Scheduler LIFO forwarding - `lifo: true` in a job scheduler template is now forwarded to every enqueued job. Previously ignored (#87).
- FlowProducer child LIFO routing - `glidemq_addFlow` now routes child jobs with `lifo: true` to the LIFO list. Previously children were always added to the stream (#87).
- `glidemq_removeJob` and `moveActiveToDelayed` / `moveToWaitingChildren` now DECR `list-active` when removing or deferring an active list-sourced job, preventing counter drift after job removal or mid-execution delay (#87).
- Function library bumped to version 52 (version 51 introduced `list-active` counter; version 52 added FlowProducer LIFO child routing).

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
