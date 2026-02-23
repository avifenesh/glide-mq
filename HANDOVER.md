# HANDOVER

Current state of the glide-mq repository as of 2026-02-23.

## Branch

`feature/retry-jobs-17` - implements `queue.retryJobs(opts)` (#17). Pending review and merge.

## Last completed task

**Task #17: `queue.retryJobs(opts)`** - bulk retry failed jobs.
- Branch: `feature/retry-jobs-17`
- Lua function: `glidemq_retryJobs` added to `src/functions/index.ts`
- TypeScript wrapper: `retryJobs()` in `src/functions/index.ts`
- Queue method: `Queue.retryJobs(opts?)` in `src/queue.ts`
- Testing mode: `TestQueue.retryJobs(opts?)` in `src/testing.ts`
- Bug fix: `Job.retry()` in `src/job.ts` - added missing ZREM from failed ZSet, reset attemptsMade/finishedOn
- Integration tests: `tests/retry-jobs.test.ts` (5 tests x 2 modes)
- Testing-mode tests: 4 new tests in `tests/testing-mode.test.ts`
- LIBRARY_VERSION bumped from `27` to `28`

Previous: **Task #11: `job.promote()`** - branch `feature/promote-job-11`, pending review.

## What comes next

Open issues (by number):
- #18 `queue.getWorkers()` - list active workers with metadata
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
- FCALL routing uses dummy key `{glidemq}:_` where deterministic cluster routing is needed.
