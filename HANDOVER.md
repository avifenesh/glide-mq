# HANDOVER

Current state of the glide-mq repository as of 2026-02-23.

## Branch

`main` - clean, up to date.

## Last completed task

**Task #12: `job.changeDelay(newDelay)`** - merged via PR #45 (cffc2e4).

## What comes next

Open issues (by number):
- #11 `job.promote()` - move a delayed job to waiting immediately
- #17 `queue.retryJobs(opts)` - bulk retry failed jobs
- #18 `queue.getWorkers()` - list active workers with metadata
- #19 `queue.getJobScheduler(name)` - fetch a single scheduler entry
- #42 Optimize JSON serialization in Queue.add
- #44 Sanitize stack traces in sandbox runner

## Known state

- Valkey must be on `:6379` (standalone) and `:7000-7005` (cluster) for full integration tests.
- `npm run build` compiles TypeScript to `dist/`.
- `npm test` runs full suite.
- LIBRARY_VERSION is `26`.

## Active configuration

- `src/functions/index.ts` - single Valkey Server Function library loaded once per connection.
- All keys hash-tagged as `glide:{queueName}:*`.
- FCALL routing uses dummy key `{glidemq}:_` where deterministic cluster routing is needed.
