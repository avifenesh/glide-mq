# HANDOVER

Current state of the glide-mq repository as of 2026-02-22.

## Branch

`queue-clean-16` — open against `main`. PR not yet created.

## In-progress task

**Task #16: `queue.clean(grace, limit, type)`**

### What was done

- Added `cleanJobs` Valkey Server Function in `src/functions/index.ts` — single FCALL that scans completed/failed job metadata and removes jobs older than `grace` ms, up to `limit` at a time.
- Added `Queue.clean(grace, limit, type)` public method in `src/queue.ts`.
- Added `TestQueue.clean(grace, limit, type)` in `src/testing.ts` — in-memory equivalent using `finishedOn` timestamps.
- New test file `tests/clean.test.ts` — covers completed and failed cleanup, grace boundary, limit cap, and empty-result paths.

### Files changed

| File | Change |
|------|--------|
| `src/functions/index.ts` | New `cleanJobs` FCALL function |
| `src/queue.ts` | `Queue.clean()` public method |
| `src/testing.ts` | `TestQueue.clean()` in-memory implementation |
| `tests/clean.test.ts` | New — tests for all clean() scenarios |

### Docs updated (this session)

| File | Change |
|------|--------|
| `docs/USAGE.md` | New "Cleaning old jobs" section with example |
| `docs/MIGRATION.md` | `queue.clean()` status updated from Gap to Full; workaround row updated |
| `CHANGELOG.md` | `[Unreleased]` section now includes `queue.clean()` entry |
| `HANDOVER.md` | This file (updated) |

## What comes next

1. Commit all changed files as separate logical commits.
2. Run `/deslop` before pushing.
3. Open PR — title: `feat: add queue.clean(grace, limit, type)`.
4. Verify CI passes (unit + integration tests).

## Known state

- Valkey must be on `:6379` (standalone) and `:7000-7005` (cluster) for full integration tests.
- `npm run build` compiles TypeScript to `dist/`.
- `npx vitest run tests/clean.test.ts` runs just the new clean tests.

## Active configuration

- `src/functions/index.ts` — single Valkey Server Function library loaded once per connection.
- All keys hash-tagged as `glide:{queueName}:*`.
- FCALL routing uses dummy key `{glidemq}:_` where deterministic cluster routing is needed.
