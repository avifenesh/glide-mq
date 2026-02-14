# Handover

## Current State (2026-02-14)

### speedkey package
- Extracted valkey-glide Node.js + Rust core from PR #5325 (ipc-replacement-exploration)
- Repo: github.com/avifenesh/speedkey (private)
- Local path: C:\Users\avife\speedkey
- Package renamed to `speedkey` (unscoped npm)
- Targets: linux x64/arm64 (gnu+musl), darwin x64/arm64, win32 x64 msvc
- CD triggered via v0.1.0 tag push (run 22004711981)
- Build status: 6/7 passed, arm64-gnu queued waiting on runner
- `prepare-and-publish` blocked until arm64-gnu completes
- NPM_TOKEN secret set on repo
- Local build verified on Windows

### glide-mq project
- Research phase. No code scaffolded yet.
- Two deep research guides (82 sources total) in agent-knowledge/
- CLAUDE.md written with project rules

### Research completed
- agent-knowledge/nodejs-queue-libs-redis-valkey.md - BullMQ, Bull, Bee-Queue internals, Redis data structures, Lua scripts, job state machines, reliability patterns
- agent-knowledge/valkey-glide-nodejs-client.md - GLIDE architecture (Rust core + NAPI), 380+ commands, cluster, pub/sub, batching, migration from ioredis

### Architecture (approved)
- docs/ARCHITECTURE.md - full architecture plan
- Streams-first design, PEL as active set, XAUTOCLAIM for stalled recovery
- Valkey Functions (FUNCTION LOAD/FCALL) instead of EVAL scripts - single library `glidemq`
- Cluster-native from day one (hash tags on all keys)
- speedkey-only (no ioredis/node-redis abstraction)
- 4 phases: Core -> Advanced -> Flows+Events -> Production Hardening

### Phase 1 complete
- 9 Lua server functions in glidemq library (FUNCTION LOAD, not EVAL)
- Connection factory with function loader, consumer group creation
- Queue: add, addBulk, getJob, pause, resume
- Worker: XREADGROUP BLOCK poll loop, concurrency, processor dispatch
- Job: progress, data update, children values, moveToFailed, remove, retry
- Scheduler: delayed promotion, stalled recovery via XAUTOCLAIM

### Phase 2 complete
- Deduplication: 3 modes (simple, throttle, debounce) via glidemq_dedup
- Rate limiting: sliding window Lua function + Worker integration + manual rateLimit(ms)
- Job retention: removeOnComplete/removeOnFail (true, count, age+count) in complete/fail Lua functions
- Global concurrency: glidemq_checkConcurrency via XPENDING, Queue.setGlobalConcurrency(n)
- Full lifecycle integration tests: delayed promotion, priority ordering, retry with backoff
- 144 tests passing (79 unit + 65 integration, standalone + cluster)

### Phase 3 complete
- FlowProducer: atomic parent-child job trees, recursive nested flows, auto completeChild on worker
- QueueEvents: XREAD BLOCK loop on events stream, typed event emission
- Job schedulers: cron + fixed interval, Scheduler.runSchedulers()
- Metrics: Queue.getMetrics, Queue.getJobCounts
- 164 tests passing (17 files)

### Next steps
- Phase 4: graceful shutdown, error recovery, OpenTelemetry
- speedkey CD: arm64-gnu build still queued on GitHub (ubuntu-24.04-arm runner)
- Rotate NPM token
