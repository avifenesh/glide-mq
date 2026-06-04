# Handover

## Current State

- **Branch**: main
- **Version**: 0.15.4 released on npm.
- **CI**: green on main; local 0.15.4 release gates passed on 2026-06-04.
- **Local branches**: no release-blocking local branches.

## What Was Done (0.15.x series since 0.14.0)

### Released

- **0.15.0** (#192, #205): HTTP proxy parity expansion (queue events SSE, per-job lifecycle SSE, `jobs/wait`, workers, metrics, scheduler CRUD, rolling usage summary, broadcast publish/SSE, DLQ inspection/replay, suspended-job inspection, revoke, queue global rate-limit HTTP management). Flow HTTP API: `POST /flows`, `GET /flows/:id`, `GET /flows/:id/tree`, `DELETE /flows/:id` for tree flows and DAGs. `queue.getUsageSummary()` plus `/usage/summary`.
- **0.15.1** (#206): debounce + ordering.key deadlock fix via lightweight skip markers. `LIBRARY_VERSION` 81.
- **0.15.2** (#212, #213, #216-219): priority/LIFO in batch-mode workers, `list-active` underflow guards (12 sites through one `decrListActive` helper), priority/LIFO active visibility via `glidemq_getActiveListJobIds`, lockDuration-aware stall reclaim (`stalledInterval` no longer conflated with threshold). `LIBRARY_VERSION` 84. **Behavior change**: workers that relied on short `stalledInterval` without setting `lockDuration` now see slower stall recovery; set `lockDuration` explicitly to match if needed.
- **0.15.3** (#222-#246): DAG dependency direction/tree rendering/multi-dependent leaf fixes, `addDAG` level batching, stalled-job redispatch semantics, large-key `UNLINK` cleanup, bounded ordering skip-marker advancement, serverless credential cache scoping, flow ID-collision guards, proxy strict opts validation, long-running job heartbeats, broadcast retry isolation, queue client single-flight, and dependency CVE fixes. `LIBRARY_VERSION` 93.
- **0.15.4**: interval scheduler anchoring prevents late worker ticks from accumulating drift, `npm test` now runs the intended non-fuzzer suite, and CI/local compose coverage use stable Valkey 9.1.0 images.

### 0.15.4 Release Notes

See CHANGELOG.md `0.15.4` for the full list. Highlights:

- **Scheduler interval anchoring**: `every` schedulers advance from the previous due slot instead of the late worker tick timestamp, preventing CI/event-loop jitter from accumulating drift while still skipping missed slots.
- **Release gate correctness**: `npm test` now passes the fuzzer exclusion as a single Vitest argument, so it covers the intended 2,414-test non-fuzzer suite.
- **Valkey CI images**: standalone, cluster, and search coverage now use stable Valkey 9.1.0 images instead of release-candidate images.

## Open Threads

- **Bun/Deno NAPI compatibility testing**: still pending from 0.14.0 handover.
- **Valkey CI images**: CI is off release candidates. Standalone and cluster coverage use stable `valkey/valkey:9.1.0`; search coverage uses stable `valkey/valkey-bundle:9.1.0`, which carries Valkey Search 1.2.x and keeps the Search 1.1+ option tests active.

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
