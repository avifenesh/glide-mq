# HANDOVER

Current state of the glide-mq repository as of 2026-03-14.

## Branch

`main` - all committed work pushed. Uncommitted changes on working tree: Lua HMGET optimization + TS-side micro-optimizations.

## Uncommitted changes (ready to commit)

- `src/functions/index.ts`: HMGET optimization in completeAndFetchNext - merge 4 separate hash lookups (EXISTS, HGET revoked, HGET expireAt, HGET groupKey) into 1 HMGET. Reduces redis.call()s from 13 to 10 on hot path. Same optimization applied to priority/LIFO list paths. LIBRARY_VERSION = '68'. Also cached encodeRetention default return objects.
- `src/telemetry.ts`: withSpan overload - accepts callback without attributes param. Callers set attributes via span.setAttribute() inside callback (free when tracing disabled via NOOP_SPAN).
- `src/queue.ts`: Move withSpan attributes to callback body (avoids object allocation when tracing off). Skip Buffer.byteLength for small payloads (< 262KB). Avoid creating empty object for JSON.stringify(opts ?? {}).
- `src/base-worker.ts`: Skip Buffer.byteLength for small return values. Skip completionHints allocation when no ordering/group configured.

## Recent merges

- `903b366` fix: benchmark scripts import shared connections, remove dead code
- `caac4cb` docs: performance claims with ElastiCache numbers
- `d639010` bench: ElastiCache head-to-head + TLS support
- `a1577a2` perf: hot-path micro-optimizations (cached listener flags, completeAndFetchNext array protocol, processedOn hints, skipEvents/skipMetrics)

## ElastiCache benchmark results (2026-03-14)

Valkey 8.2, r7g.large, TLS, EC2 in same AZ, standalone mode.
30k jobs, 2k warmup excluded, optimized build (LIBRARY_VERSION 68):

| c | glide-mq | BullMQ | Delta |
|---|----------|--------|-------|
| 1 | 2,479 j/s | 2,535 j/s | -2.2% |
| 5 | 10,754 j/s | 9,866 j/s | +9.0% |
| 8 | 16,275 j/s | 11,845 j/s | **+37.4%** |
| 10 | 18,218 j/s | 13,541 j/s | **+34.5%** |
| 15 | 19,583 j/s | 14,162 j/s | **+38.3%** |
| 20 | 19,408 j/s | 16,085 j/s | +20.7% |
| 30 | 19,922 j/s | 17,124 j/s | +16.3% |
| 50 | 19,768 j/s | 19,159 j/s | +3.2% |

HMGET optimization impact: c=1 gap narrowed from ~12% to ~2%. Sweet spot c=8-15 at 34-38% faster.

Key insight: on production infra with real network latency, GLIDE's 1-RTT completeAndFetchNext design dominates. Both libraries converge at high concurrency as Valkey single-thread becomes the bottleneck.

## Performance optimization learnings

- **HMGET consolidation within same key works**: Merging EXISTS + HGET + HGET + HGET on the same hash key into 1 HMGET saves 3 redis.call()s (measurable at scale). Previous finding about HMGET being slower was about reads from different keys or HGETALL overhead on large hashes.
- SKIPPING redis.call()s via hints/sentinels works; CONSOLIDATING cross-key calls does not
- Steady-state: 1 RTT per job (completeAndFetchNext returns next job's hash inline)
- 10 internal redis.call()s per completeAndFetchNext on happy path (down from 13 via HMGET optimization)
- GLIDE NAPI overhead: 0.037ms/call vs ioredis (measured via raw FCALL vs EVALSHA)
- Localhost benchmarks are misleading for production - always validate on real infra with TLS
- TS-side micro-optimizations (withSpan, Buffer.byteLength, JSON.stringify) save ~3-5us per call - meaningful in aggregate but not game-changing

## Known state

- Valkey must be on `:6379` (standalone) and `:7000-7005` (cluster) for full integration tests.
- `npm run build` compiles TypeScript to `dist/`.
- `npm test` runs full suite (~1915 tests, all passing).
- `claude-review` CI check can fail with SDK infrastructure errors - not code-related.
- Fuzzer pre-push hook: ~4 min per push.
- Version: 0.11.1, LIBRARY_VERSION = '68' (uncommitted)
- ElastiCache standalone: `testing-standalone-0227215229` (us-east-2, Valkey 8.2, r7g.large, TLS)
- EC2 test host: SSH via `ssh -i ~/.ssh/GlideEc2.pem ubuntu@ec2-52-14-77-103.us-east-2.compute.amazonaws.com`, repo at `~/glide-mq`

## What comes next

- Commit and push the HMGET + TS micro-optimizations
- Update README performance table with latest numbers (c=1 through c=50 sweep)
- Sequential add throughput: glide-mq ~2% slower at c=1 (was 12%). Caused by NAPI per-call overhead on the add path. Could improve via addBulk batching or speedkey NAPI optimizations.
- Cluster mode benchmarks (untested)
- Memory footprint comparison (untested)
