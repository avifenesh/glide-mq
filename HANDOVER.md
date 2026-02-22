# HANDOVER

Current state of the glide-mq repository as of 2026-02-23.

## Branch

`feature/promote-job-11` - implements `job.promote()` (#11). Pending review and merge.

## Last completed task

**Task #11: `job.promote()`** - move a delayed job to waiting immediately.
- Branch: `feature/promote-job-11`
- Lua function: `glidemq_promoteJob` added to `src/functions/index.ts`
- TypeScript wrapper: `promoteJob()` in `src/functions/index.ts`
- Instance method: `Job.promote()` in `src/job.ts`
- Testing mode: `TestJob.promote()` in `src/testing.ts`
- Integration tests: `tests/promote.test.ts` (10 tests x 2 modes)
- Testing-mode tests: 2 new tests in `tests/testing-mode.test.ts`
- LIBRARY_VERSION bumped from `26` to `27`

Previous: **Task #12: `job.changeDelay(newDelay)`** - merged via PR #45 (cffc2e4).

## What comes next

Open issues (by number):
- #17 `queue.retryJobs(opts)` - bulk retry failed jobs
- #18 `queue.getWorkers()` - list active workers with metadata
- #19 `queue.getJobScheduler(name)` - fetch a single scheduler entry
- #42 Optimize JSON serialization in Queue.add
- #44 Sanitize stack traces in sandbox runner

## Known state

- Valkey must be on `:6379` (standalone) and `:7000-7005` (cluster) for full integration tests.
- `npm run build` compiles TypeScript to `dist/`.
- `npm test` runs full suite.
- LIBRARY_VERSION is `27`.

## Active configuration

- `src/functions/index.ts` - single Valkey Server Function library loaded once per connection.
- All keys hash-tagged as `glide:{queueName}:*`.
- FCALL routing uses dummy key `{glidemq}:_` where deterministic cluster routing is needed.
