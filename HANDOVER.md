# Handover

## Current State

- **Standalone skill links**: repository-only durability and priority-timing references use public GitHub documentation URLs. `npm run lint:skills` checks local links within each independently installed skill and runs its portability regression.

- **Testing skill reference**: delayed adds and `moveToDelayed` are documented consistently as timer-backed in-memory behavior. The testing backend does not reproduce Valkey persistence or crash recovery.
- **Validated on `feat/bounded-scheduler-inventory`**: `Queue.getRepeatableJobsBounded(maxEntries)` and its `TestQueue` equivalent require a positive safe-integer cap, reject malformed stored entries, and return no partial inventory when the cap is exceeded. The existing `getRepeatableJobs()` remains unchanged. The scheduler and testing-mode owners passed 336/336 cases across standalone and cluster on the current nullable-writer-compatible source. API completeness, queue surface, and library-source owners passed 71 checks on the earlier public-API-identical source epoch. A local coverage run covered all 103 executable TypeScript lines added by the PR; the Lua coverage flag remains a hosted CI gate.

- **0.15.6 released 2026-09-30** (#310): the patch release of the 2026-09-29 audit series (#295 to #309, about 90 verified findings, one PR per lane, failing-first tests, self-review comment, revuto verdict, green CI).
- **0.16 series, merged since 0.15.6**: #311 Bun and Deno support (smoke scripts, docs, CI job), #312 cron-parser syntax parity, #313 testing-mode parity round 2, #314 vitest excludes `.claude/**` worktrees, #315 proxy round 2 (bounded retry and wait, shared client, SSE errors to `onError`, POST disconnect fix), #316 backlog round 4 (batch budgets, rate-limit attempts, budget pause re-check, `updateFlowBudget`, `glidemq_failAndFetchNext`, Broadcast `trimmed` event; library 131), #317 npm-cd polls the registry after publish, #318 Lua round 3 (scheduler in-flight tracking, atomic global concurrency, consumer cleanup, per-subscription broadcast stalls, upgrade healing; library 132), #319 `@glidemq/speedkey` ^0.4.0 and `SearchQueryOptions.scorer` removed.
- **Server function library**: `LIBRARY_VERSION` is `136` (bounded scheduler inventory atomically checks `HLEN` before `HGETALL`). Every function keeps existing KEYS/ARGS layouts; new inputs are optional trailing args, new reply markers go before the parent marker and are parsed with a fallback for the old shape, and new functions have a TypeScript fallback on "function not found" so rolling upgrades work in both directions. The bounded scheduler inventory function is an intentional exception to the fallback rule: it has no separate-command or unbounded fallback, because that would lose the atomic bound guarantee.
- **Version**: 0.19.0. Community PRs #339, #340 and #341 are integrated with their original heads retained. The release includes terminal stalled-failure retention, fractional-count normalization, corrected priority/LIFO routing and recovery documentation, and throttle-only deduplication TTL types. The type restriction is source-breaking for callers whose TTL was previously ignored, so this is a pre-1.0 minor release. Local validation passed 3440 tests across 132 files, 188 randomized fuzzer rounds across in-memory/standalone/cluster backends, runtime audit with zero vulnerabilities, and an installed-package smoke. Independent review and integration CI are pre-tag gates; registry publication and a fresh registry-package smoke are verified after tagging.
- **Test infrastructure**: vitest 4.1.11. `tests/helpers/fixture.ts` runs each describe block in standalone (:6379) and cluster (:7000-7005) mode. Cluster clients always `FUNCTION LOAD REPLACE`, so parallel runs on one server clobber each other's library: serialize Valkey-backed runs with `flock /home/avifenesh/projects/glide-mq/.scratch/gmq-test.lock`. Standalone skips the reload when `LIBRARY_VERSION` matches, so iterating on Lua without a bump needs a force load. The pre-push hook runs the fuzzer (also under the lock). Scratch, logs and helper scripts live in `/home/avifenesh/projects/glide-mq/.scratch`, never `/tmp`.
- **Review gate**: revuto reviews at most two rounds per PR. After the cap, the author self-review comment on the final push plus green CI is the merge gate, with the missing tool half noted in the PR body. Branch protection requires the branch to be up to date (`gh pr update-branch`) and answered inline threads resolved.
- **Client**: `@glidemq/speedkey` 0.4.0 is a re-import of upstream valkey-glide main plus Windows publish, GlideBf and `GlideJson.mset`; 0.4.1 adds the kill-path follow-ups from the upstream #7244 review; both published 2026-09-30. 0.4.2 (cluster `close()` detaches a blocked connection in 2 to 4 ms instead of the block duration) shipped from speedkey#138 the same day; glide-mq depends on `^0.4.2`. glide-mq keeps the graceful-close wait for the in-flight read even with 0.4.2: a claim delivered in the socket-teardown window is lost with the rejected read. Probe (`scripts/probe-close-strand.mjs`, 8 adds during close, 30 closes per mode): 0.3.0 without the wait 30/30 stranded in both modes; 0.4.2 0/30 with or without it on localhost. The window is one network round trip, so the wait stays until a server-side unblock exists.
- **Upstream**: valkey-io/valkey-glide#7244 (`close()` detaches a blocked standalone connection) and #7243 (`GlideJson.mset`) are open, bot findings addressed, waiting on maintainers. The cluster detach sits on the fork branch `fix/cluster-close-detach` and gets its PR once #7244 merges. External PRs carry no self-review or status comments.

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
- **Testing-mode flows** (#331 steps 1 and 2): `TestFlowProducer` (`add` with `budget`, `addBulk`, `addDAG`), `TestJob.getChildrenValues/getParents/moveToWaitingChildren`, and `chain/group/chord/dag` from `glide-mq/testing`, all over the `TestQueue` registry with no connection. Parents start in `waiting-children` (`src/testing-flow.ts`, `src/testing-workflows.ts`; deps live on the record in `src/testing.ts`). Still missing: failed and removed child semantics (step 3), the rest of `addDAG` parity (step 4), and budget checks in batch workers.
- **Broadcast** (#309, merged): stalled messages are re-run per subscription with per-subscription stall counts; trimmed messages have their job data deleted; `priority`/`lifo` are rejected.

## Open Threads

- **0.18.0**: released 2026-10-08. **0.19.0**: tag the integration merge only after local full tests, review, fuzzer hook and CI pass; verify npm and a fresh installed-package smoke. `deduplication.ttl` now requires `mode: 'throttle'`; remove ignored TTL values from simple/debounce options instead.
- **Community PRs**: #324/#326/#327 already merged. #328 merged as bf61863f. #325/#329/#335/#336 integrated with retained heads, independent reviews and review fixes; merging the owned integration PR with a merge commit preserves their ancestry.
- **#332**: production batch timeout refill fix was included in 0.17.0.
- **Graceful-close wait**: stays. Removing it needs a server-side hand-back that runs before the blocked read is rejected; nothing in valkey-glide offers that today.
- **Upstream follow-ups**: after #7244 merges, open the cluster detach PR from `fix/cluster-close-detach`; after a glide release, fix `available-commands.json` in valkey-glide-docs (JSON.MSET listed as unavailable for Node). A GlideBf upstream PR is assessed at about one day (module CI exists upstream) and not started.
- **Owner actions**: rotate the npm token (speedkey `NPM_TOKEN` was taken from the rig's `~/.npmrc`) before it expires; a GPG signing key for Verified commits on upstream PRs; `~/.npmrc` `allow-scripts` is rejected by npm 11.19 (do not edit `~/.npmrc` from a session).
- **Broadcast**: a retry entry promoted by a pre-130 library has no `bcastEntry` and is dropped if its original entry is trimmed. Known limit of the rolling upgrade from 0.15.5, not fixable after the fact.
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
- Priority: 0 means no priority; 1 is highest; integers 1-2048. Numeric priorities order enqueuing within a scheduler promotion pass, not global execution order. This differs from BullMQ and is marked Changed in MIGRATION.md; see USAGE.md Priority timing for promotion, retrieval and admission qualifications.
- Broadcast `maxMessages` is an exact hard cap, not a MINID-safe trim.
- `Queue.getJobs('waiting')` follows worker dispatch order across priority, LIFO, and FIFO sources.
