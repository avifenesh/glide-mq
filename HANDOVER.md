# HANDOVER

Current state of the glide-mq repository as of 2026-03-08.

## Branch

`perf/hot-path-optimizations` - performance optimization pass on worker hot paths.

### Commits on this branch

1. `67d01c8` - perf: skip HMGET in buildParentInfo for non-parent jobs
   - Eliminated wasted RTT (HMGET for parent lookup) on every non-parent job
   - ~100% c=1 throughput improvement (~1300 -> ~2500 j/s)

2. `67e338a` - chore: increase benchmark durations for more stable measurements
   - ADD_DURATION 5s->15s, PROCESS_SIZES [500,2000]->[5000,20000]
   - N_SERIAL 200->500, N_PIPELINED 1000->3000

3. `995fdde` - perf: popLists Lua FCALL + timestamp caching in processJob
   - New glidemq_popLists server function: checks priority + LIFO in 1 FCALL (was 2 RPOPs)
   - Date.now() cached once after processor, shared across completeAndFetchNext/finishedOn/scheduler
   - LIBRARY_VERSION bumped to 62
   - ~6-9% c=10 throughput improvement

4. `0ad67f8` - perf: skip redundant redis.call() in completeAndFetchNext for common jobs
   - Fix B: pass processedOn hint from TS, skip HGET processedOn in Lua
   - Fix C: use '__' sentinel for ordering/group keys, skip markOrderingDone/releaseGroupSlotAndPromote when confirmed absent
   - Fix D: pass hasParents flag, skip SMEMBERS parents:{jobId} when no parent relationships exist
   - Saves 2-4 internal redis.call() per standard job completion
   - LIBRARY_VERSION bumped to 63
   - ~2-4% improvement across all scenarios

5. `7d12ca4` - fix: update test for new completeAndFetchNext arg positions

### Performance results (with longer benchmarks)

| Scenario | Before optimizations | After all fixes | Improvement |
|---|---|---|---|
| c=1 20k jobs | ~1,300 j/s (est.) | ~2,700 j/s | ~108% |
| c=10 20k jobs | ~12,900 j/s | ~14,000-15,000 j/s | ~9-16% |

### What was tried and reverted (no measurable improvement or regression)

- Fix #3: completeAndFetchNext parsing - removed `.map()` intermediate array, parsed inline. No improvement.
- Fix #4 alone: Date.now() caching alone. No improvement. Combined with Fix #2 and kept.
- Fix A: HGETALL-first restructure for moveToActive - replaced EXISTS + 3 HGETs + final HGETALL with single early HGETALL + Lua scan. REGRESSED c=10 by -12%. Root cause: Lua string comparisons (~120 per job) are slower than 4 separate C-level redis.call() hash lookups.
- Fix E: HMGET consolidation in fetch-next phase - replaced EXISTS + HGET revoked + HGET expireAt + HGET groupKey with single HMGET. REGRESSED c=1 by -8%. Same root cause: HMGET array result conversion overhead in Lua-C bridge outweighs fewer dispatches.
- Fix F: tostring(timestamp) caching + prefix string optimization. Within noise, reverted.

### Architecture insights

- Steady-state processing is 1 RTT per job (completeAndFetchNext returns next job's hash inline).
- The main win was eliminating wasted RTTs (HMGET for non-parent buildParentInfo, extra RPOPs via popLists FCALL).
- In Valkey Lua, individual redis.call() with single returns beats batch calls (HMGET/HGETALL) that return arrays. The Lua-C bridge overhead for array conversion outweighs fewer dispatches.
- The remaining ~14 internal redis.call()s per completeAndFetchNext are all mandatory (state mutations, validation, events/metrics). Further Lua optimization requires semantics changes (opt-out events/metrics).
- JS-level micro-optimizations have no measurable effect since network RTT and Lua execution dominate.

## Active PRs

- PR #124: Deep audit fixes (merged)

## Recent merges

- PR #124: Deep audit fixes (62 findings, 7 commits + BaseWorker)

## Known state

- Valkey must be on `:6379` (standalone) and `:7000-7005` (cluster) for full integration tests.
- `npm run build` compiles TypeScript to `dist/`.
- `npm test` runs full suite (1915 tests, all passing).
- `claude-review` CI check can fail with SDK infrastructure errors - not code-related.
- Fuzzer pre-push hook: ~4 min per push.
- LIBRARY_VERSION is now '63' - servers with older cached versions will auto-upgrade on connection.

## Remaining known limitations

- globalConcurrency with batch rpopCount (gc enabled): still pops 1 job at a time
- glidemq_deferActive: no DECR for list jobs (stream jobs only in practice)

## What comes next

- Merge perf branch or open PR
- Continue with remaining roadmap issues
- Potential: integrate list-active counter with stall detection for crash self-healing
- Potential: add batch count param to glidemq_rpopAndReserve for gc+high-concurrency throughput
- Potential: opt-in `events: false` / `metrics: false` queue config to skip emitEvent/recordMetrics in Lua (~2 fewer redis.calls per job)
