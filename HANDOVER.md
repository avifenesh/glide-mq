# Handover

## Current State (2026-02-14)

### speedkey
- speedkey@0.1.0 published on npm
- 6 targets: linux x64/arm64 (gnu+musl), darwin arm64, win32 x64
- Repo: github.com/avifenesh/speedkey (public)
- Intel Mac (x86_64-apple-darwin) skipped - rustup install fails on macos-15 ARM cross-compile
- Post-publish RC tests broken (still reference old package name in utils/)
- NPM token needs rotation (was shared in conversation)

### glide-mq - 347 tests, 31 files, all passing
- Valkey standalone on :6379, cluster on :7000-7005 (WSL)
- speedkey linked as file dependency (file:../speedkey/node)

### Completed phases
- Phase 1: Core Queue/Worker/Job, 15 Lua server functions, connection factory
- Phase 2: Dedup (3 modes), rate limiting, retention, global concurrency
- Phase 3: FlowProducer, QueueEvents, job schedulers (cron+interval), metrics
- Phase 4: Graceful shutdown, connection recovery, OpenTelemetry, API completeness
- Phase 5: Compatibility tests from Bull, Bee-Queue, Celery, Sidekiq patterns (66 tests)
- Edge case tests: worker resilience, queue ops, advanced features, cluster (66 tests)

### Research
- agent-knowledge/nodejs-queue-libs-redis-valkey.md (42 sources)
- agent-knowledge/valkey-glide-nodejs-client.md (40 sources)
- agent-knowledge/bullmq-test-cases.md (800+ tests cataloged)
- agent-knowledge/bull-test-cases.md (265 tests cataloged)
- agent-knowledge/bee-queue-and-others-test-cases.md (500+ tests cataloged)
- agent-knowledge/celery-and-cross-lang-test-cases.md (cross-lang patterns)

## Proven compatible (347 tests)
- FIFO ordering, serial + concurrent processing
- Retry with fixed/exponential/jitter backoff
- Retry exhaustion to permanent failure
- Stalled job detection and recovery (batch)
- Competing consumers (no duplicates)
- Graceful + force shutdown
- Delayed job promotion
- Job progress, state queries, updateData
- Queue inspection (accurate counts)
- Obliterate, drain, getJobs
- removeOnComplete/removeOnFail
- Failed job list, remove, retry
- Resource bounds (no leak after 100 jobs)
- Deduplication (simple, throttle, debounce)
- Rate limiting (sliding window)
- Global concurrency enforcement
- Parent-child flows (nested)
- Events stream (added, completed, failed, retrying, stalled)
- Job schedulers (cron + interval)
- Cluster mode (all features verified)

## Gaps - not yet implemented
1. Sandboxed processors (run processor in child process - Bull/BullMQ feature)
2. Per-job timeout (we use stall detection, not explicit timeouts)
3. Worker autoscale (Celery scales workers based on CPU/queue depth)
4. Job revocation (cancel a running task - Celery feature)
5. Workflow primitives: chains, groups, chords (Celery canvas - we have flows but not these)
6. Benchmark suite (throughput, latency, memory profiling)
7. Broker failover tests (Redis Sentinel/cluster failover mid-processing)
8. Memory leak regression tests (baseline-stress-measure pattern)
9. Dead letter queue as separate concept (we use failed ZSet)
10. Lock renewal for long-running jobs (BullMQ renews, we use PEL + XAUTOCLAIM)
11. Sandboxed processor crash recovery (restart child process)
12. Job log (append log lines to a job - BullMQ feature)
13. Job dependencies beyond parent-child (BullMQ waitForJob)
14. Queue.isPaused(), Queue.count(), Queue.getRepeatableJobs()
15. Worker.isRunning(), Worker.isPaused()
16. Backoff custom strategy function (we support fixed/exponential/jitter, not custom fn)

## Next steps
- Phase 6: Competitive analysis - full feature comparison, known bugs in others, gap prioritization
- Decide which gaps to implement vs skip
- Benchmark suite against BullMQ
- Publish glide-mq to npm
