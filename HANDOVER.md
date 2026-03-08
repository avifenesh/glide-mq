# HANDOVER

Current state of the glide-mq repository as of 2026-03-08.

## Branch

`fix/deep-audit-fixes` - deep audit PR #124 open. Includes all Round 1/2 fixes + H1 BaseWorker.

### Active PRs

- PR #124: Deep audit fixes (62 findings, 7 commits + BaseWorker)
- PR #120: Lua HMGET optimization (M9)
- PR #121: Job state query tests (H10)
- PR #122: Proxy endpoint tests + routes (M27)
- PR #123: Worker state machine (H6+M10) - superseded by BaseWorker in #124

## Recent merges

- Audit sweep 2026-03-07 - 36 fixes across 10 commits (9296ee1 merge)
  - Security: bulk jobs DoS cap (1000), queue name hash-tag injection guard, /health no longer leaks queue names, numeric opts validated in proxy
  - Performance: TestWorker O(N^2) → O(1) waiting queue, cron loops use Set.has(), metricsData capped at 1440 buckets
  - Correctness: scheduler lock loss aborts batch, moveToFailed throws outside Worker, addDAG enforces lifo+ordering.key, DAG registerParent partial-failure error, sandboxClose try-catch, priority > 2048 overflow guard
  - Code quality: constants deduplicated into utils.ts, type guards replacing unsafe casts, resource leak in addAndWait fixed
  - Tests: 177 non-integration tests passing; new graceful-shutdown, dag-utils cycles, job error, proxy error, worker event tests
  - Remaining deferred: BroadcastWorker duplication (large refactor), 5 high-severity test gaps (obliterate, DAG failure propagation, state machine transitions - all need Valkey), 30 low-priority test gaps

- PR #118 (follow-up to #87): LIFO mode gaps - merged 2026-03-07 at 1ac1bf8
  - globalConcurrency enforcement via atomic glidemq_rpopAndReserve (XPENDING+listActive check → RPOP → INCR)
  - Scheduler templates forward lifo flag (template.opts?.lifo)
  - FlowProducer child jobs with lifo:true routed to LIFO list (extractLifoFromOpts)
  - Batch rpopCount for high concurrency (concurrency > 1, gc disabled)
  - list-active counter balanced: complete/fail/moveActiveToDelayed/moveToWaitingChildren/removeJob all DECR
  - LIBRARY_VERSION 57 (56: list-active counter; 57: addFlow LIFO children)
  - 28 LIFO integration tests (standalone + cluster)

- PR #117: Broadcast and BroadcastWorker for pub/sub fan-out - merged 2026-03-07
  - LIBRARY_VERSION 55 in that PR

- PR #115 (issue #87): LIFO job processing mode - merged 2026-03-07

## Known state

- Valkey must be on `:6379` (standalone) and `:7000-7005` (cluster) for full integration tests.
- `npm run build` compiles TypeScript to `dist/`.
- `npm test` runs full suite (1800+ tests expected).
- `claude-review` CI check can fail with SDK infrastructure errors - not code-related.
- Fuzzer pre-push hook: ~4 min per push.

## Remaining known limitations

- `list-active` counter not self-healing on hard worker crash (documented in docs/MIGRATION.md)
- Duplicated list-poll logic in worker.ts pollOnce (pre/post XREADGROUP blocks) - refactoring deferred
- globalConcurrency with batch rpopCount (gc enabled): still pops 1 job at a time (no batch FCALL yet)
- glidemq_deferActive: no DECR for list jobs (but only called for stream jobs in practice)

## What comes next

- Continue with remaining roadmap issues (#84 etc.)
- Potential: integrate list-active counter with stall detection for crash self-healing
- Potential: add batch count param to glidemq_rpopAndReserve for gc+high-concurrency throughput
