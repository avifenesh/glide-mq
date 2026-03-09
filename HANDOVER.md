# HANDOVER

Current state of the glide-mq repository as of 2026-03-09.

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

6. `928783e` - feat: add events/metrics opt-out for completeAndFetchNext hot path
   - Worker options `events: false` / `metrics: false` skip XADD events and HINCRBY metrics in Lua
   - TS-side EventEmitter unaffected
   - LIBRARY_VERSION bumped to 64
   - No measurable throughput improvement (XADD/HINCRBY are O(1), dominated by RTT)
   - Kept as feature: reduces Valkey memory/CPU for high-throughput users who don't need events/metrics

### Performance results (with longer benchmarks)

| Scenario | Before optimizations | After all fixes | Improvement |
|---|---|---|---|
| c=1 20k jobs | ~1,300 j/s (est.) | ~2,700 j/s | ~108% |
| c=10 20k jobs | ~12,900 j/s | ~14,000-15,000 j/s | ~9-16% |

### What was tried and reverted (no measurable improvement or regression)

- Fix #3: completeAndFetchNext parsing - removed `.map()` intermediate array, parsed inline. No improvement.
- Fix #4 alone: Date.now() caching alone. No improvement. Combined with Fix #2 and kept.
- Fix A: HGETALL-first restructure for moveToActive - replaced EXISTS + 3 HGETs + final HGETALL with single early HGETALL + Lua scan. REGRESSED c=10 by -12%. Root cause: Lua string comparisons (~120 per job) are slower than 4 separate C-level redis.call() hash lookups.
- Fix A (round 2): HGETALL-first with only 3 fields (~45 comparisons). Mixed results: c=1 regressed 3-5%, c=10 5k regressed 9-20%, c=10 20k improved 11-19%. Reverted - inconsistent.
- Fix E: HMGET consolidation in fetch-next phase - replaced EXISTS + HGET revoked + HGET expireAt + HGET groupKey with single HMGET. REGRESSED c=1 by -8%. Same root cause: HMGET array result conversion overhead in Lua-C bridge outweighs fewer dispatches.
- Fix F: tostring(timestamp) caching + prefix string optimization. Within noise, reverted.
- events/metrics skip: Saves 2-3 redis.call()s per job (XADD + HINCRBY). A/B tested: +0.7% c=1 (noise), +7% avg c=10 but high variance (-0.7% to +13.8%). Kept as feature, not as perf optimization.

### Pipelining investigation

- Glide auto-pipelines concurrent calls: 194k ops/s vs 3k sequential (65x).
- Explicit Batch adds ~30% over concurrent for trivial commands (SET).
- For FCALL completeAndFetchNext (~14 redis.call()s, ~490us server time), network framing overhead is negligible. Explicit Batch would not move the needle.
- The bottleneck is single-threaded Valkey Lua execution, not network pipelining.
- Micro-batcher tested: queueMicrotask collector + Batch(false) pipeline of N FCALLs. Perfect batching (avg=10, max=10 for c=10). REGRESSED -19%. Overhead of Batch object creation + protobuf serialization for 10 FCALLs with ~19 args each + microtask scheduling exceeds savings from guaranteed pipelining.

### Batch Lua FCALL investigation

- Implemented glidemq_batchCompleteAndFetchNext: completes N jobs + fetches N new jobs in a single FCALL (~200 lines Lua, full group gate logic, priority/LIFO/stream fetch).
- TS-side micro-batcher: queueMicrotask collects concurrent completions, single fcall() call (no Batch API).
- Eliminates Batch API overhead (the main cost from the pipeline approach) - just one regular fcall().
- REGRESSED c=10 by -8.3% (13,299 j/s vs ~14,500 baseline). c=1 unaffected (batch disabled at c=1).
- Root cause: queueMicrotask scheduling latency (~100-500us) delays each completion. Since Glide already auto-pipelines concurrent individual FCALLs efficiently, the microtask delay adds overhead without meaningful RTT savings.
- Conclusion: ANY approach that introduces microtask collection overhead will regress. The auto-pipelining already achieves near-optimal network utilization for concurrent FCALL calls.

### Architecture insights

- Steady-state processing is 1 RTT per job (completeAndFetchNext returns next job's hash inline).
- The main win was eliminating wasted RTTs (HMGET for non-parent buildParentInfo, extra RPOPs via popLists FCALL).
- In Valkey Lua, individual redis.call() with single returns beats batch calls (HMGET/HGETALL) that return arrays. The Lua-C bridge overhead for array conversion outweighs fewer dispatches.
- The remaining ~14 internal redis.call()s per completeAndFetchNext are all mandatory (state mutations, validation, events/metrics). Further Lua optimization requires semantics changes.
- JS-level micro-optimizations have no measurable effect since network RTT and Lua execution dominate.
- Glide auto-pipelines concurrent async calls via NAPI/Rust core. Explicit Batch only helps for lightweight commands.
- Any micro-batching scheme (queueMicrotask collector) regresses throughput due to scheduling latency, regardless of whether it uses Batch API or single batch FCALL. Auto-pipelining is already near-optimal.

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
- LIBRARY_VERSION is now '64' - servers with older cached versions will auto-upgrade on connection.

## Remaining known limitations

- globalConcurrency with batch rpopCount (gc enabled): still pops 1 job at a time
- glidemq_deferActive: no DECR for list jobs (stream jobs only in practice)

## What comes next

- Merge perf branch or open PR
- Continue with remaining roadmap issues
- Batch completeAndFetchNext Lua function: TESTED AND REVERTED (-8.3% regression). All batch/pipeline approaches exhausted.
- Potential: integrate list-active counter with stall detection for crash self-healing
- Potential: add batch count param to glidemq_rpopAndReserve for gc+high-concurrency throughput
