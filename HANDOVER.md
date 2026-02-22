# HANDOVER

Current state of the glide-mq repository as of 2026-02-22.

## Branch

`job-discard-14` — open against `main`. PR not yet created.

## In-progress task

**Task #14: `job.discard()` and `UnrecoverableError`**

### What was done

- Added `UnrecoverableError` class in `src/errors.ts` — extends `Error`, exported from `src/index.ts`.
- Added `job.discard()` method in `src/job.ts` — sets a discard flag, causing the worker to move the job directly to failed state without consuming retry attempts.
- Updated `src/worker.ts` to check for `discard` flag and `UnrecoverableError` on job failure — both paths skip retry scheduling.
- Updated `src/sandbox/` types, runner, and pool to propagate discard/unrecoverable signals from child process/thread back to the main process.
- Updated `src/testing.ts` — `TestWorker` honours discard flag and `UnrecoverableError`.
- New test file `tests/discard.test.ts` — covers both `job.discard()` and `UnrecoverableError` in standalone and sandbox modes.
- New fixtures: `tests/fixtures/processors/discard.js`, `tests/fixtures/processors/unrecoverable.js`.

### Files changed

| File | Change |
|------|--------|
| `src/errors.ts` | New `UnrecoverableError` class |
| `src/index.ts` | Export `UnrecoverableError` |
| `src/job.ts` | `job.discard()` method |
| `src/worker.ts` | Handle discard flag and `UnrecoverableError` |
| `src/sandbox/types.ts` | Propagate discard/unrecoverable in IPC types |
| `src/sandbox/runner.ts` | Catch and signal discard/unrecoverable |
| `src/sandbox/pool.ts` | Handle discard/unrecoverable from runner |
| `src/sandbox/sandbox-job.ts` | Expose `discard()` in sandbox job proxy |
| `src/testing.ts` | `TestWorker` honours discard/unrecoverable |
| `tests/discard.test.ts` | New — full test coverage |
| `tests/fixtures/processors/discard.js` | Fixture for sandbox discard test |
| `tests/fixtures/processors/unrecoverable.js` | Fixture for sandbox UnrecoverableError test |

### Docs updated (this session)

| File | Change |
|------|--------|
| `docs/USAGE.md` | Added `job.discard()` / `UnrecoverableError` example in Worker section |
| `docs/MIGRATION.md` | `job.discard()` updated from Gap to Full; workaround text and workarounds table updated |
| `CHANGELOG.md` | `[Unreleased]` section includes `job.discard()` and `UnrecoverableError` entries |
| `HANDOVER.md` | This file (updated) |

## What comes next

1. Commit all changed files as separate logical commits.
2. Run `/deslop` before pushing.
3. Open PR — title: `feat: add job.discard() and UnrecoverableError`.
4. Verify CI passes (unit + integration tests).

## Known state

- Valkey must be on `:6379` (standalone) and `:7000-7005` (cluster) for full integration tests.
- `npm run build` compiles TypeScript to `dist/`.
- `npx vitest run tests/discard.test.ts` runs just the new discard tests.

## Active configuration

- `src/functions/index.ts` — single Valkey Server Function library loaded once per connection.
- All keys hash-tagged as `glide:{queueName}:*`.
- FCALL routing uses dummy key `{glidemq}:_` where deterministic cluster routing is needed.
