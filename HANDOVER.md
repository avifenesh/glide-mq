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

### Phase 4 complete
- Graceful shutdown: idempotent close, SIGTERM/SIGINT handler, closing/closed events
- Connection recovery: exponential backoff reconnect, re-ensure function library
- OpenTelemetry: optional peer dep, spans for Queue.add/Worker.process/FlowProducer.add
- API completeness: obliterate, drain, getJobs, getJobCountByTypes, Job state methods
- 281 tests passing (28 files)

### All 4 phases complete
- Phase 1: Core Queue/Worker/Job with Lua functions
- Phase 2: Dedup, rate limiting, retention, global concurrency
- Phase 3: FlowProducer, QueueEvents, schedulers, metrics
- Phase 4: Shutdown, recovery, OTel, API completeness

### Phase 5 (in progress): Compatibility test suite
- 4 learn agents researching test cases from BullMQ, Bull, Bee-Queue, Celery, Sidekiq, and others
- Goal: catalog every test case and behavioral expectation from competing libraries
- Build a compatibility test suite proving glide-mq passes the same standards
- Research output in agent-knowledge/*-test-cases.md

### Phase 6 (planned): Competitive analysis and gap filling
- Accumulate full feature lists from all competing libraries
- Catalog known bugs, issues, and limitations in each
- Identify features glide-mq is missing or does differently
- Build on top of the gap analysis

### speedkey published
- speedkey@0.1.0 live on npm (6 targets: linux x64/arm64 gnu+musl, darwin arm64, win32 x64)
- Intel Mac (x86_64-apple-darwin) skipped for now
- Post-publish RC tests need fixing (still reference old package name in utils/)
- Rotate NPM token
