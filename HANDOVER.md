# Handover

## Current State

- **Audit series (2026-09-29)**: four read-only audits (Lua, worker, API, proxy/sandbox/testing), two follow-up audits (schedulers/broadcast, hot path) and a docs-vs-code audit produced about 90 verified findings. Every fix landed with a failing-first test, one PR per lane, self-review comment, revuto verdict and green CI. Merged: #295 sandbox, #296 testing-mode parity, #297 proxy hardening, #298 worker lifecycle, #300 API correctness, #301 Lua round 1, #302 scheduler/cron, #303 vitest 4.1.11 security bump, #304 worker round 2, #305 Lua round 2, #306 scheduler/flow follow-ups, #307 docs (25 claims), #308 Lua/worker follow-ups.
- **Open**: #309 broadcast recovery and retention (`fix/broadcast-recovery-20260929`, library 130). Lane L15 (`fix/backlog-round4-20260929`, based on #309) is working the last backlog items: batch-mode budget charging, rate-limit requeues not consuming attempts, a bounded re-check for `onExceeded: 'pause'`, `glidemq_failAndFetchNext`, and a Broadcast `trimmed` event.
- **Server function library**: `LIBRARY_VERSION` is `129` on main, `130` on #309. Every function added in this series keeps existing KEYS/ARGS layouts; new inputs are optional trailing args, new replies are parsed with a fallback for the old shape, and new functions have a TS fallback on "function not found" so rolling upgrades work in both directions.
- **Behavior changes since 0.15.5** (all in CHANGELOG `[Unreleased]` Changed): `prefetch` capped at `concurrency`; `Job.retry()` only from `failed`; proxy `maxPageSize` (default 1000) bounds list, replay-all and clean requests and 5xx bodies are generic; cron ORs day-of-month and day-of-week and fires through DST transitions like cronie; scheduler templates reject `jobId`, `delay`, `deduplication` and `parent`; re-upserting an in-flight `repeatAfterComplete` scheduler does not fire; server-side priority validation; testing-mode dedup applies without `{ dedup: true }`; graceful `close()` waits up to `blockTimeout` for the in-flight read; `Broadcast.publish` rejects `priority`/`lifo`; `maxMessages` stays a hard cap that can drop unread messages.
- **Version**: package.json is still 0.15.5. The Unreleased section carries about 50 fixes plus behavior changes, so the next release is the owner's call between 0.15.6 and 0.16.0. Skill metadata versions (skills/*/SKILL.md) must move with it.
- **Test infrastructure**: vitest 4.1.11. `tests/helpers/fixture.ts` runs each describe block in standalone (:6379) and cluster (:7000-7005) mode. Cluster clients always `FUNCTION LOAD REPLACE`, so parallel test runs on one server clobber each other's library: serialize Valkey-backed runs (`flock /tmp/gmq-test.lock npx vitest run ...`). Standalone skips the reload when `LIBRARY_VERSION` matches, so iterating on Lua without a bump needs a force load. Unit coverage uploads under the `unit` Codecov flag; patch target 80%.
- **Review gate**: revuto reviews at most two rounds per PR. After the cap, the author self-review comment on the final push plus green CI is the merge gate, with the missing tool half noted in the PR body. Answered inline threads must be resolved or branch protection blocks the merge button.
- **Upstream**: `@glidemq/speedkey` (valkey-glide fork) `close()` does not end a blocked `XREADGROUP` on the server. glide-mq works around it by waiting for the in-flight read on graceful close. The real fix belongs in glide/speedkey. speedkey itself is temporary until valkey-glide publishes the NAPI client and Windows builds.

## What Was Done (0.15.x series since 0.14.0)

### Released

- **0.15.5**: queue correctness and lifecycle fixes accumulated since 0.15.4, including pause/revoke/reclaim behavior, cross-queue parent completion, reconnect client lifetime, list-job resume semantics, token-bucket clock consistency, partial test-worker batch flushing, and empty-dependency waiting-children handling. `LIBRARY_VERSION` 122.
- **0.15.0** (#192, #205): HTTP proxy parity expansion (queue events SSE, per-job lifecycle SSE, `jobs/wait`, workers, metrics, scheduler CRUD, rolling usage summary, broadcast publish/SSE, DLQ inspection/replay, suspended-job inspection, revoke, queue global rate-limit HTTP management). Flow HTTP API: `POST /flows`, `GET /flows/:id`, `GET /flows/:id/tree`, `DELETE /flows/:id` for tree flows and DAGs. `queue.getUsageSummary()` plus `/usage/summary`.
- **0.15.1** (#206): debounce + ordering.key deadlock fix via lightweight skip markers. `LIBRARY_VERSION` 81.
- **0.15.2** (#212, #213, #216-219): priority/LIFO in batch-mode workers, `list-active` underflow guards, priority/LIFO active visibility via `glidemq_getActiveListJobIds`, lockDuration-aware stall reclaim. `LIBRARY_VERSION` 84. **Behavior change**: workers that relied on short `stalledInterval` without setting `lockDuration` now see slower stall recovery.
- **0.15.3** (#222-#246): DAG dependency direction/tree rendering/multi-dependent leaf fixes, `addDAG` level batching, stalled-job redispatch semantics, large-key `UNLINK` cleanup, bounded ordering skip-marker advancement, serverless credential cache scoping, flow ID-collision guards, proxy strict opts validation, long-running job heartbeats, broadcast retry isolation, queue client single-flight, and dependency CVE fixes. `LIBRARY_VERSION` 93.
- **0.15.4**: interval scheduler anchoring, `npm test` runs the intended non-fuzzer suite, CI/local compose use stable Valkey 9.1.0 images.

### Unreleased (audit series, 2026-09-29)

See CHANGELOG `[Unreleased]` for the full list. Highlights by area:

- **Lua correctness**: removed flow children resolve their parents; `drain` closes ordering holes; stale claims are rejected (`STALE`); removed active jobs cannot corrupt group or list counters or come back as ghost hashes; `Job.retry()` is atomic and only from `failed`; `changePriority`/`changeDelay` handle list-held jobs; cross-queue children that finish before registration are parked, not counted; flow budgets are created before their jobs and `budgetKey` is written atomically; `list-active-ids` replaces keyspace SCANs.
- **Worker lifecycle**: reconnect after `close()` no longer leaks clients or timers; heartbeats cannot leak; `pause()` stops chaining; broadcast batch reads are capped; `close()` hands back in-flight claims; `close(true)` aborts running jobs; a second signal during a hung shutdown exits; batch activation and completion are pipelined.
- **Schedulers**: cron DST and OR day matching; in-flight `repeatAfterComplete` re-upsert keeps state and writes through compare-and-set; templates carry ordering, limits, cost and compression; bad templates are rejected at upsert.
- **Proxy**: SSE cleanup on early disconnect, one shared command client, bounded requests, generic 5xx bodies, flow node limit.
- **Sandbox**: hung or aborted jobs free their pool slot (5s grace, then terminate); no host crash on a dead child.
- **Testing mode**: validation, ordering, retention, retries with backoff, dedup modes and `moveToDelayed` match production.
- **Broadcast** (#309): stalled messages are re-run per subscription with per-subscription stall counts; trimmed messages have their job data deleted; `priority`/`lifo` are rejected.

## Open Threads

- **Release**: pick 0.15.6 or 0.16.0, move package.json and the three skill versions, turn `[Unreleased]` into the dated section, tag and publish. A release branch and PR, not a direct push.
- **Broadcast**: `lastActive` is still shared across subscriptions, so stall detection waits while another subscription processes the same message. A worker that reclaims a message and then closes before running it costs one extra stall. A retry entry promoted by a pre-130 library has no `bcastEntry` and is dropped if its original entry is trimmed.
- **Budgets**: `onExceeded: 'pause'` re-delays jobs by 24h until L15 lands its bounded re-check. There is no API to raise a flow budget after creation unless L15 adds one.
- **Scheduler mode switch**: a job from the old `every`/`pattern` mode still running past the old `nextRun` can overlap the first `repeatAfterComplete` run; closing it needs Lua tracking of the in-flight job.
- **Cost overflow inside `completeAndFetchNext`** gets no DLQ copy; the reply does not identify the failed job.
- **Global concurrency** on the stream path is a separate call before `XREADGROUP`, so concurrent workers can briefly overshoot.
- **Cron**: no names, day-of-week 7, `L`/`W`/`#` or seconds field.
- **Bun/Deno NAPI compatibility testing**: still pending from 0.14.0.
- **Coverage**: Codecov project status is informational; patch target 80%. Integration, unit and Lua coverage are separate flags.

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
- Priority: 0 means no priority and runs after every prioritized job; 1 is highest; integers 1-2048. This is the opposite of BullMQ and is marked Changed in MIGRATION.md.
- Broadcast `maxMessages` is an exact hard cap, not a MINID-safe trim.
- `Queue.getJobs('waiting')` follows worker dispatch order across priority, LIFO, and FIFO sources.
