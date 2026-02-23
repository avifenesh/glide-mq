# HANDOVER

Current state of the glide-mq repository as of 2026-02-23.

## Branch

`feature/get-workers-18` - implements `queue.getWorkers()` (#18). Pending review and merge.

## Last completed task

**Task #18: `queue.getWorkers()`** - list active workers with metadata.
- Branch: `feature/get-workers-18`
- New type: `WorkerInfo` in `src/types.ts` (id, addr, pid, startedAt, age, activeJobs)
- Key builder: `worker(id)` added to `buildKeys()` in `src/utils.ts`
- Worker registration: TTL-based heartbeat keys in `src/worker.ts` using SET PX
- Queue method: `Queue.getWorkers()` in `src/queue.ts` - SCAN + pipeline GET
- Refactored `scanAndDelete` into `scanKeys` + `scanAndDelete` for reuse
- Testing mode: `TestQueue.getWorkers()` in `src/testing.ts`
- Integration tests: `tests/get-workers.test.ts` (7 tests x 2 modes)
- Testing-mode tests: 5 new tests in `tests/testing-mode.test.ts`
- Test fixture cleanup updated for worker keys
- No LIBRARY_VERSION change (no Lua function changes)

Previous: **Task #17: `queue.retryJobs(opts)`** - branch `feature/retry-jobs-17`, merged.

## What comes next

Open issues (by number):
- #19 `queue.getJobScheduler(name)` - fetch a single scheduler entry
- #42 Optimize JSON serialization in Queue.add
- #44 Sanitize stack traces in sandbox runner

## Known state

- Valkey must be on `:6379` (standalone) and `:7000-7005` (cluster) for full integration tests.
- `npm run build` compiles TypeScript to `dist/`.
- `npm test` runs full suite.
- LIBRARY_VERSION is `28`.

## Active configuration

- `src/functions/index.ts` - single Valkey Server Function library loaded once per connection.
- All keys hash-tagged as `glide:{queueName}:*`.
- Worker keys: `glide:{queueName}:w:{consumerId}` - SET with PX TTL for heartbeat.
- FCALL routing uses dummy key `{glidemq}:_` where deterministic cluster routing is needed.
