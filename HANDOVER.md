# HANDOVER

Current state of the glide-mq repository as of 2026-02-22.

## Branch

`main` — no open PR yet for the current work item.

## In-progress task

**Task #20: Worker `active` and `drained` events**

### What was done

- Added `'active'` and `'drained'` to the `WorkerEvent` union type in `src/worker.ts`.
- `Worker` emits `'active'(job, jobId)` at the moment a job begins processing (before the processor function is called).
- `Worker` emits `'drained'()` when the queue transitions from non-empty to empty. An `isDrained` flag prevents duplicate emissions when the worker loops on an already-empty queue.
- `TestWorker` in `src/testing.ts` mirrors both events with identical semantics.
- New test file `tests/worker-events.test.ts` covers both events, including the dedup guard on `drained`.

### Files changed (uncommitted)

| File | Change |
|------|--------|
| `src/worker.ts` | `WorkerEvent` extended; `active`/`drained` emits added; `isDrained` flag |
| `src/testing.ts` | `active`/`drained` emits added to `TestWorker`; `isDrained` flag |
| `tests/worker-events.test.ts` | New — integration + unit tests for both events |
| `package-lock.json` | Updated (dependency install) |

### Docs updated (this session)

| File | Change |
|------|--------|
| `docs/USAGE.md` | Worker events section now includes `active`/`drained` with full event table |
| `docs/TESTING.md` | TestWorker API table now lists `active` and `drained` |
| `CHANGELOG.md` | Created; `[Unreleased]` section documents both new events |
| `HANDOVER.md` | This file (created) |

## What comes next

1. Commit all changed files as separate logical commits.
2. Run `/deslop` before pushing.
3. Open PR — title: `feat: add active and drained events to Worker and TestWorker`.
4. Verify CI passes (unit + integration tests).

## Known state

- Valkey must be on `:6379` (standalone) and `:7000-7005` (cluster) for full integration tests.
- `npm run build` compiles TypeScript to `dist/`.
- `npx vitest run tests/worker-events.test.ts` runs just the new event tests.

## Active configuration

- `src/functions/index.ts` — single Valkey Server Function library loaded once per connection.
- All keys hash-tagged as `glide:{queueName}:*`.
- FCALL routing uses dummy key `{glidemq}:_` where deterministic cluster routing is needed.
