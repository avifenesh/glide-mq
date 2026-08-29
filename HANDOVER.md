# Handover

## Current State

- **In flight**: `fix/close-fetch-next` (fork #18 / upstream #282) — rebased onto v0.15.5. close() must not strand CAF or poll claims; grouped CAF undo skips `retainedSlot` active rewind. Lua library 124.
- **Audit fix**: `fix/repeat-after-stalled` atomically advances repeat-after-complete schedulers during terminal stalled recovery.
- **Branch**: `automation/cover-open-fixes-20260825` consolidates the reviewed correctness queue.
- **Revoke/timeout safety**: revoked active jobs cannot complete; each batch job owns its abort signal, and timeouts remain retryable.
- **Coverage CI**: fuzzer exclusion is quoted and integration/Lua uploads use Codecov OIDC (`id-token: write`, `use_oidc: true`).
- **Pause sweep**: `Queue.pause()` immediately expires suspended jobs whose timeouts elapsed.
- **Version**: 0.15.5 release candidate on `release/v0.15.5`; GitHub tag and npm publish pending merge.
- **CI**: green across all six fork/upstream stack PRs after the 2026-08-22 packaging update.
- **Ordered-group recovery**: retained rate-limit slots are tracked per job and released on terminal paths; oversized token-bucket heads are cleaned iteratively through bounded sweeps.
- **Local branches**: `refactor/extract-lua-library` -> `ci/ts-coverage` -> `ci/lua-coverage`.
- **Coverage**: CI-included internal pause-worker regressions cover worker/broadcast pause guards, pending-read reset, and batch-refill stops. Standalone TS coverage and focused Lua coverage were verified on an isolated Valkey instance; changed executable lines are hit.
- **Broadcast pause recovery**: a queue-pause activation race records only its parked PEL IDs. Resume uses targeted `XCLAIM`; it never rewinds `XREADGROUP` to `0`, which could redeliver live work from the same consumer's PEL.
- **Stalled recovery**: `XAUTOCLAIM` persists a per-group cursor. Full 100-entry pages continue on a guarded zero-delay timer, yielding to I/O between pages without waiting another stalled interval.
- **Unreleased**: `getJobs('waiting')` reads priority, LIFO, and non-pending FIFO sources; revoke/remove clean list entries. Removal and revocation route FIFO cleanup by actual list membership so list-backed jobs skip the scan while stale source fields cannot orphan stream entries. Server-function library identity is `112`.
- **Review gate**: automatic Claude review is retired; the Revuto GitHub App check reviews pull requests.
- **Dependency security**: the lockfile carries protobufjs 7.6.5, brace-expansion 5.0.9, PostCSS 8.5.25, and body-parser 2.3.0.

## What Was Done (0.15.x series since 0.14.0)

### Released

- **0.15.5**: queue correctness and lifecycle fixes accumulated since 0.15.4, including pause/revoke/reclaim behavior, cross-queue parent completion, reconnect client lifetime, list-job resume semantics, token-bucket clock consistency, partial test-worker batch flushing, and empty-dependency waiting-children handling. `LIBRARY_VERSION` 122.

- **0.15.0** (#192, #205): HTTP proxy parity expansion (queue events SSE, per-job lifecycle SSE, `jobs/wait`, workers, metrics, scheduler CRUD, rolling usage summary, broadcast publish/SSE, DLQ inspection/replay, suspended-job inspection, revoke, queue global rate-limit HTTP management). Flow HTTP API: `POST /flows`, `GET /flows/:id`, `GET /flows/:id/tree`, `DELETE /flows/:id` for tree flows and DAGs. `queue.getUsageSummary()` plus `/usage/summary`.
- **0.15.1** (#206): debounce + ordering.key deadlock fix via lightweight skip markers. `LIBRARY_VERSION` 81.
- **0.15.2** (#212, #213, #216-219): priority/LIFO in batch-mode workers, `list-active` underflow guards (12 sites through one `decrListActive` helper), priority/LIFO active visibility via `glidemq_getActiveListJobIds`, lockDuration-aware stall reclaim (`stalledInterval` no longer conflated with threshold). `LIBRARY_VERSION` 84. **Behavior change**: workers that relied on short `stalledInterval` without setting `lockDuration` now see slower stall recovery; set `lockDuration` explicitly to match if needed.
- **0.15.3** (#222-#246): DAG dependency direction/tree rendering/multi-dependent leaf fixes, `addDAG` level batching, stalled-job redispatch semantics, large-key `UNLINK` cleanup, bounded ordering skip-marker advancement, serverless credential cache scoping, flow ID-collision guards, proxy strict opts validation, long-running job heartbeats, broadcast retry isolation, queue client single-flight, and dependency CVE fixes. `LIBRARY_VERSION` 93.
- **0.15.4**: interval scheduler anchoring prevents late worker ticks from accumulating drift, `npm test` now runs the intended non-fuzzer suite, and CI/local compose coverage use stable Valkey 9.1.0 images.
- **Unreleased**: list-backed workers now reserve `list-active` in `glidemq_popListsReserve`; legacy `glidemq_popLists` remains non-reserving and new workers fall back to it plus typed `INCRBY` during rolling upgrades. `LIBRARY_VERSION` 98 follows stalled-cursor 94.

### In flight (fork)

- **Reconnect lockDuration**: Scheduler rebuilt after reconnect keeps worker `lockDuration`. Branch `fix/reconnect-lock-duration`.

### In flight (fork)

- **Nested/cross-queue parents**: test-first regression `a39d87a`; implementation spans `5603dc0` through `32f1395`, followed by the Sonar cleanup in `79a573a`. The branch is independent of `upstream/main`, uses `LIBRARY_VERSION` 110, reconciles removed children including nested and DAG exists-to-HSET TOCTOU races with ghost parent-set cleanup, JSON-encodes retryable child-slot notifications, eagerly delivers completion-time cross-queue notifications without adding a steady-state RTT, deduplicates overlapping tree/DAG edges, and removes `xq-pending` during obliterate. Standalone and cluster integration CI are green.

### 0.15.4 Release Notes

See CHANGELOG.md `0.15.4` for the full list. Highlights:

- **Scheduler interval anchoring**: `every` schedulers advance from the previous due slot instead of the late worker tick timestamp, preventing CI/event-loop jitter from accumulating drift while still skipping missed slots.
- **Release gate correctness**: `npm test` now passes the fuzzer exclusion as a single Vitest argument, so it covers the intended 2,414-test non-fuzzer suite.
- **Valkey CI images**: standalone, cluster, and search coverage now use stable Valkey 9.1.0 images instead of release-candidate images.

## Open Threads

- **Ordered groups**: pre-activation removal, revoke, and expiry close ordering holes; rejected token-bucket enqueue operations do not consume IDs or sequences; rate-limit requeues preserve their slot and use explicit returning-job markers; priority-list fast fetches apply token-bucket and fixed-window gates; oversized token failures close holes across activation, completion, and promotion paths. `LIBRARY_VERSION` 117.
- **Bun/Deno NAPI compatibility testing**: still pending from 0.14.0 handover.
- **Valkey CI images**: CI is off release candidates. Standalone and cluster coverage use stable `valkey/valkey:9.1.0`; search coverage uses stable `valkey/valkey-bundle:9.1.0`, which carries Valkey Search 1.2.x and keeps the Search 1.1+ option tests active.
- **Coverage**: Codecov project status is informational; patch target 80%. Integration and Lua coverage remain separate CI flags.

## API Design Decisions (locked)

- `DAGNode.deps` = "nodes that must complete before this node runs" (as documented; corrected in #244).
- `dag(nodes, connection, prefix?)` - `queueName` is per-node on each `DAGNode`, not a top-level arg.
- JobUsage.tokens: `Record<string, number>` not flat fields.
- Budget tokenWeights: computed in TS, not Lua.
- TPM uses raw (unweighted) totalTokens.
- costs/costUnit: currency-agnostic.
- streamChunk: thin wrapper over stream(), not new infrastructure.
- Search 1.1+ options: forward-compatible types, graceful skip on older servers.
- Plugins: AI endpoints under `/flows/:id/usage`, `/flows/:id/budget`, `/jobs/:id/stream`.
- **In flight**: `Queue.getJobs('waiting')` follows worker dispatch order across priority, LIFO, and FIFO sources; it pages FIFO stream reads and excludes entries in the consumer-group PEL.
