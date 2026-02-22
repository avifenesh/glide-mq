# HANDOVER

Current state of the glide-mq repository as of 2026-02-22.

## Branch

`feature/change-delay-12` - implements task #12.

## In-progress task

**Task #12: `job.changeDelay(newDelay)` - mutate fire time of a delayed job**

### What was done

- Added `glidemq_changeDelay` Lua function in `src/functions/index.ts` - atomically mutates the fire time of a job based on its current state. Handles delayed (update score preserving priority), delayed->waiting (delay=0, priority=0), delayed->prioritized (delay=0, priority>0), waiting->delayed, prioritized->delayed. Returns `error:invalid_state` for active/completed/failed. LIBRARY_VERSION bumped from `25` to `26`.
- Added `changeDelay()` TypeScript wrapper in `src/functions/index.ts`.
- Added `Job.changeDelay(newDelay)` method in `src/job.ts` - validates >= 0, calls Lua function, throws on error results, updates local `opts.delay`.
- Added `TestJob.changeDelay(newDelay)` in `src/testing.ts` for in-memory testing parity.
- Tests in `tests/delayed.test.ts` - 10 integration test cases covering all state transitions, no_op, not_found, active state error, and Job instance method.
- Tests in `tests/testing-mode.test.ts` - 2 test cases for TestJob.changeDelay.

### Files changed

| File | Change |
|------|--------|
| `src/functions/index.ts` | `glidemq_changeDelay` Lua function; `changeDelay()` export; LIBRARY_VERSION 25 -> 26 |
| `src/job.ts` | `Job.changeDelay(newDelay)` method; import added |
| `src/testing.ts` | `TestJob.changeDelay(newDelay)` |
| `tests/delayed.test.ts` | 10 changeDelay integration test cases |
| `tests/testing-mode.test.ts` | 2 TestJob.changeDelay test cases |
| `CHANGELOG.md` | `[Unreleased]` includes `job.changeDelay()` entry |
| `HANDOVER.md` | This file (updated) |

## What comes next

1. Run integration tests against Valkey (standalone + cluster) to validate Lua function behavior.
2. Ship: create PR, push, monitor CI, merge.

## Known state

- Valkey must be on `:6379` (standalone) and `:7000-7005` (cluster) for full integration tests.
- `npm run build` compiles TypeScript to `dist/`.
- `npx vitest run tests/delayed.test.ts` runs the delayed/changeDelay tests.
- `npx vitest run tests/testing-mode.test.ts` runs in-memory tests (no Valkey needed).
- Build and type check pass cleanly. Testing mode tests all pass (26/26).

## Active configuration

- `src/functions/index.ts` - single Valkey Server Function library loaded once per connection.
- All keys hash-tagged as `glide:{queueName}:*`.
- FCALL routing uses dummy key `{glidemq}:_` where deterministic cluster routing is needed.
