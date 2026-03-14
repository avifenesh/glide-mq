# HANDOVER

Current state of the glide-mq repository as of 2026-03-14.

## Branch

`main` - all work merged. Uncommitted changes on working tree: TS-side micro-optimizations (+5% localhost).

## Uncommitted changes (ready to commit)

- `src/base-worker.ts`: EventEmitter cached listener flags, skip async isOrderingTurn/buildParentInfo when not needed
- `src/functions/index.ts`: Remove `.map(v => String(v))` allocation in completeAndFetchNext result parsing
- `benchmarks/elasticache-head-to-head.ts`: Standalone ElastiCache head-to-head benchmark
- Measured +5.1% on localhost Docker (c=10 20k: 13,107 -> 13,775 j/s)

## Recent merges

- PR #129: feat: stall detection for list-sourced jobs via `glidemq_reclaimStalledListJobs` bounded SCAN
- PR #128: fix: `rpopAndReserve` batch count for globalConcurrency, `deferActive` list-active DECR
- PR #126: perf: hot-path optimizations (squash-merged to main)
  - ~108% c=1 throughput improvement (~1,300 -> ~2,700 j/s)
  - ~9-16% c=10 throughput improvement (~12,900 -> ~14,000-15,000 j/s)
  - Key changes: skip HMGET for non-parent buildParentInfo, popLists Lua FCALL, completeAndFetchNext hints (processedOn, ordering/group sentinels, hasParents), events/metrics opt-out
  - LIBRARY_VERSION = '66'
  - Sentinel validation: ordering key '__' is rejected as reserved
  - hasParents flag narrowed to DAG-only parentIds (saves SMEMBERS for single-parent flow jobs)
- PR #124: Deep audit fixes (62 findings, 7 commits + BaseWorker)

## ElastiCache benchmark results (2026-03-14)

Valkey 8.2, r7g.large, TLS, EC2 in same AZ, standalone mode:

| Library  | c=1      | c=10      |
|----------|----------|-----------|
| glide-mq | 2,455 j/s | 18,356 j/s |
| BullMQ   | 2,436 j/s | 13,216 j/s |
| Delta    | +0.8%    | **+38.9%** |

Key insight: the localhost Docker gap (glide-mq ~18% slower at c=10) was a mirage. GLIDE's NAPI overhead (0.037ms/call) is significant when loopback RTT is ~0.05ms but becomes noise when real network RTT is ~0.4ms. Over a real network, glide-mq's fewer FCALLs (13 vs 23 redis.call()s) and 1-RTT completeAndFetchNext design dominates.

## Performance optimization learnings (from PR #126 + c=10 investigation)

- In Valkey Lua, individual redis.call() with single returns beats HMGET/HGETALL batch calls (Lua-C bridge array conversion overhead)
- SKIPPING redis.call()s via hints/sentinels works; CONSOLIDATING them into batch calls does not
- Steady-state: 1 RTT per job (completeAndFetchNext returns next job's hash inline)
- 13 internal redis.call()s per completeAndFetchNext on happy path - all mandatory
- Glide auto-pipelines concurrent async calls via NAPI/Rust core - explicit Batch only helps lightweight commands
- Any micro-batching scheme (queueMicrotask collector) regresses due to scheduling latency
- All batch/pipeline approaches exhausted
- XDEL cannot be skipped (stream growth degrades XREADGROUP)
- GLIDE NAPI overhead: 0.037ms/call vs ioredis (measured via raw FCALL vs EVALSHA)
- Warmup inflates reported gaps: glide-mq improves 37% from cold to warm vs BullMQ's 24%
- TS-side overhead per job: 0.009ms (negligible)
- Localhost benchmarks are misleading for production - always validate on real infra with TLS

## Known state

- Valkey must be on `:6379` (standalone) and `:7000-7005` (cluster) for full integration tests.
- `npm run build` compiles TypeScript to `dist/`.
- `npm test` runs full suite (~1915 tests, all passing).
- `claude-review` CI check can fail with SDK infrastructure errors - not code-related.
- Fuzzer pre-push hook: ~4 min per push.
- Version: 0.11.1, LIBRARY_VERSION = '67'
- ElastiCache standalone: `testing-standalone-0227215229` (us-east-2, Valkey 8.2, r7g.large, TLS)
- EC2 test host: `GlideEc2` (SSH alias, ubuntu@ec2-52-14-77-103.us-east-2.compute.amazonaws.com)

## What comes next

- Commit the TS-side micro-optimizations and benchmark script
- Update README performance claims with ElastiCache numbers
- Continue with remaining roadmap issues
