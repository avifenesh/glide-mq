# HANDOVER

Current state of the glide-mq repository as of 2026-03-09.

## Branch

`main` - all work merged.

## Recent merges

- PR #126: perf: hot-path optimizations (squash-merged to main)
  - ~108% c=1 throughput improvement (~1,300 -> ~2,700 j/s)
  - ~9-16% c=10 throughput improvement (~12,900 -> ~14,000-15,000 j/s)
  - Key changes: skip HMGET for non-parent buildParentInfo, popLists Lua FCALL, completeAndFetchNext hints (processedOn, ordering/group sentinels, hasParents), events/metrics opt-out
  - LIBRARY_VERSION = '64'
  - Sentinel validation: ordering key '__' is rejected as reserved
  - hasParents flag narrowed to DAG-only parentIds (saves SMEMBERS for single-parent flow jobs)
- PR #124: Deep audit fixes (62 findings, 7 commits + BaseWorker)

## Performance optimization learnings (from PR #126)

- In Valkey Lua, individual redis.call() with single returns beats HMGET/HGETALL batch calls (Lua-C bridge array conversion overhead)
- SKIPPING redis.call()s via hints/sentinels works; CONSOLIDATING them into batch calls does not
- Steady-state: 1 RTT per job (completeAndFetchNext returns next job's hash inline)
- ~14 internal redis.call()s per completeAndFetchNext - all mandatory
- Glide auto-pipelines concurrent async calls via NAPI/Rust core - explicit Batch only helps lightweight commands
- Any micro-batching scheme (queueMicrotask collector) regresses due to scheduling latency
- All batch/pipeline approaches exhausted

## Known state

- Valkey must be on `:6379` (standalone) and `:7000-7005` (cluster) for full integration tests.
- `npm run build` compiles TypeScript to `dist/`.
- `npm test` runs full suite (~1915 tests, all passing).
- `claude-review` CI check can fail with SDK infrastructure errors - not code-related.
- Fuzzer pre-push hook: ~4 min per push.
- LIBRARY_VERSION is now '65' - servers with older cached versions will auto-upgrade on connection.

## Remaining known limitations

- Stall detection does not integrate with list-active counter (healListActive on scheduler ticks is the only correction path)

## What comes next

- Continue with remaining roadmap issues
- Potential: integrate list-active counter with stall detection for crash self-healing
