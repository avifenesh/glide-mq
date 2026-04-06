# Changelog

All notable changes to glide-mq are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

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
