# Changelog

All notable changes to glide-mq are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [Unreleased]

### Added

- `queue.drain(delayed?)` — remove all waiting jobs from the queue without touching active jobs. Pass `true` to also remove delayed/scheduled jobs. Implemented as a single Valkey Server Function call; emits a `'drained'` event (#15).
- `TestQueue.drain(delayed?)` — in-memory equivalent; removes waiting (and optionally delayed) jobs from `TestQueue`.
- `active` event on `Worker` and `TestWorker` — emitted with `(job, jobId)` when a job starts processing.
- `drained` event on `Worker` and `TestWorker` — emitted when the queue transitions from non-empty to empty (i.e. no more waiting jobs after the last active job completes). A new `isDrained` flag prevents repeated emissions.
- `queue.clean(grace, limit, type)` — bulk-remove old `completed` or `failed` jobs by minimum age. Returns an array of removed job IDs. Implemented as a single Valkey Server Function call (#16).
- `job.discard()` — immediately move an active job to failed state, bypassing retries (#14).
- `UnrecoverableError` — throw this error class inside a processor to skip all remaining retry attempts and fail the job permanently (#14).

---

## [0.7.0]

### Added

- Sandboxed processor: run worker processor in a child process or worker thread (`sandboxed: true` option). Protects the main process from processor crashes and memory leaks (#36).
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
