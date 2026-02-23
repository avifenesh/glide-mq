# HANDOVER

Current state of the glide-mq repository as of 2026-02-23.

## Branch

`feature/get-job-scheduler-19` - implements `queue.getJobScheduler(name)` (#19). Ready for review.

## Last completed task

**Task #19: `queue.getJobScheduler(name)`** - fetch single scheduler entry by name.
- Branch: `feature/get-job-scheduler-19`
- Queue method: `Queue.getJobScheduler(name)` in `src/queue.ts` - HGET on schedulers hash
- Returns `SchedulerEntry | null` with pattern/every, template, nextRun
- Handles malformed JSON gracefully (try-catch, returns null)
- Testing mode: `TestQueue.getJobScheduler(name)` in `src/testing.ts`
- Also added `upsertJobScheduler`, `removeJobScheduler`, `getRepeatableJobs` to TestQueue
- Integration tests: 3 new tests in `tests/scheduler.test.ts`
- Testing-mode tests: 4 new tests in `tests/testing-mode.test.ts`
- docs/MIGRATION.md: Updated gap table (getJobScheduler now resolved)
- No LIBRARY_VERSION change (no Lua function changes)

Previous: **Task #18: `queue.getWorkers()`** - branch `feature/get-workers-18`, merged.

## What comes next

Open issues (by number):
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
