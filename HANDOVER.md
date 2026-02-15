# Handover

## Current State (2026-02-15)

### @glidemq/speedkey
- Migrated to @glidemq npm scope
- v0.2.0 CD running (all 7 platform packages under @glidemq/speedkey-*)
- Repo: github.com/avifenesh/speedkey (public)
- Scoped packages bypass npm spam detection permanently

### glide-mq
- Repo: github.com/avifenesh/glide-mq (public)
- CI: typecheck + unit tests + integration tests (all green)
- 750+ test executions (standalone + cluster parameterized)
- README written

### Performance
- c=1: 4,376 jobs/s (BullMQ: 2,041 - 2.1x faster)
- c=10: 20,979 jobs/s (10x faster)
- c=50: 44,643 jobs/s
- Key: completeAndFetchNext FCALL (1 RTT/job steady state)

### Features
- Queue, Worker, Job, QueueEvents, FlowProducer
- 15 Valkey Functions (FUNCTION LOAD/FCALL)
- Streams-first (XREADGROUP + PEL + XAUTOCLAIM)
- Cluster-native, dedup, rate limiting, global concurrency
- Per-job timeout, job log, lock renewal, DLQ
- Workflows (chain, group, chord), job revocation
- Schedulers (cron + interval), metrics, OpenTelemetry
- Graceful shutdown, connection recovery, custom backoff

### Testing
- 44 bulletproof tests (known bugs from BullMQ, Celery, Sidekiq, Bee-Queue)
- 20 fuzz/chaos tests
- 4-pass code review (73 findings addressed)
- Code simplified twice

### Next
- Wait for @glidemq/speedkey v0.2.0 publish
- Switch glide-mq dep from file: to npm
- Publish glide-mq to npm
