# Handover

## Current State

- **Branch**: main
- **Version**: 0.15.2 released on npm; an `[Unreleased]` series of fixes and perf work has landed on main and awaits a 0.15.3 cut.
- **CI**: green on main
- **Local branches**: `perf/addDAG-batched-fcall` merged to main as #246; safe to delete.

## What Was Done (0.15.x series since 0.14.0)

### Released

- **0.15.0** (#192, #205): HTTP proxy parity expansion (queue events SSE, per-job lifecycle SSE, `jobs/wait`, workers, metrics, scheduler CRUD, rolling usage summary, broadcast publish/SSE, DLQ inspection/replay, suspended-job inspection, revoke, queue global rate-limit HTTP management). Flow HTTP API: `POST /flows`, `GET /flows/:id`, `GET /flows/:id/tree`, `DELETE /flows/:id` for tree flows and DAGs. `queue.getUsageSummary()` plus `/usage/summary`.
- **0.15.1** (#206): debounce + ordering.key deadlock fix via lightweight skip markers. `LIBRARY_VERSION` 81.
- **0.15.2** (#212, #213, #216-219): priority/LIFO in batch-mode workers, `list-active` underflow guards (12 sites through one `decrListActive` helper), priority/LIFO active visibility via `glidemq_getActiveListJobIds`, lockDuration-aware stall reclaim (`stalledInterval` no longer conflated with threshold). `LIBRARY_VERSION` 84. **Behavior change**: workers that relied on short `stalledInterval` without setting `lockDuration` now see slower stall recovery; set `lockDuration` explicitly to match if needed.

### Unreleased (on main, awaiting 0.15.3 cut)

See CHANGELOG.md `[Unreleased]` for the full list. Highlights:

- **DAG correctness pass** (#244, #245, #246): `DAGNode.deps` direction now matches the documented "must complete before me" semantic; the proxy `/flows/:id/tree` renders DAGs in the user-facing direction (prerequisites under their dependents); `addDAG` pipelines submissions by topological level for ~4-50x speedup on local Valkey (cluster gains depend on RTT). Hidden race in multi-dependent leaf wiring fixed - completion now always SMEMBERS the parents SET instead of trusting a worker snapshot of `parentIds`. `LIBRARY_VERSION` bumped 88 -> 93.
- **Reclaim semantics** (#242): under-threshold stalled jobs are now redispatched back to the stream/lifo/priority list so a healthy worker can pick them up; jobs only fail once `stalledCount > maxStalledCount`. Aligns with at-least-once redelivery promise.
- **Scheduler interval anchoring**: `every` schedulers advance from the previous due slot instead of the late worker tick timestamp, preventing CI/event-loop jitter from accumulating drift while still skipping missed slots.
- **Perf: UNLINK for large deletes** (#243): job hashes, retention purge, `glidemq_clean` batches, drain sweeps now use `UNLINK` instead of `DEL`. Atomicity preserved (UNLINK is synchronous from script's view); memory reclamation deferred to bio thread.
- **Other fixes** (#222, #223, #224, #225, #226, #227, #228, #229, #230, #231, #232, #233, #234, #235, #236, #238, #241): `addFlow` ID-collision races, ordering skip-marker bounded advancement, serverless pool credential cache key collisions, expired-jobs promote budget, group promote cap, long-running job heartbeat, broadcast retry isolation, list-active leak on non-processing moveToActive outcomes, batch XREADGROUP count cap, heartbeat-before-token-wait, proxy strict opts validation, suspended-job cleanup on timeout, per-job lockDuration clamp, getClient single-flight, cross-queue DLQ scoping, dedup of list stalled recovery across schedulers.
- **Deps** (#240): npm audit fix for langsmith / protobufjs CVEs.

## Open Threads

- **0.15.3 release**: ~21 PRs sit in `[Unreleased]`. Worth a release when the next user-visible change lands or when the DAG fixes need to ship.
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
