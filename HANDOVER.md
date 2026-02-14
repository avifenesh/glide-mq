# Handover

## Current State (2026-02-14)

### speedkey
- speedkey@0.1.0 published on npm
- 6 targets: linux x64/arm64 (gnu+musl), darwin arm64, win32 x64
- Repo: github.com/avifenesh/speedkey (public)
- NPM token needs rotation

### glide-mq
- Repo: github.com/avifenesh/glide-mq (public)
- 700+ test executions (standalone + cluster parameterized)
- speedkey linked as file dependency (file:../speedkey/node)

### Performance (no-op processor benchmark)
- c=1: 4,376 jobs/s (BullMQ: 2,041 - we're 2.1x faster)
- c=10: 20,979 jobs/s (BullMQ: ~2,000 - we're 10x faster)
- c=50: 44,643 jobs/s
- Key optimization: completeAndFetchNext FCALL (1 RTT/job steady state)

### Implemented features
- Queue, Worker, Job, QueueEvents, FlowProducer
- 15 Valkey Functions (FUNCTION LOAD, not EVAL scripts)
- Streams-first (XREADGROUP + PEL + XAUTOCLAIM)
- Cluster-native (hash tags on all keys)
- Deduplication (simple, throttle, debounce)
- Rate limiting (sliding window)
- Global concurrency
- Job retention (removeOnComplete/removeOnFail)
- Per-job timeout
- Job log
- Lock renewal (heartbeat)
- Dead letter queue
- Workflow primitives (chain, group, chord)
- Job revocation (cooperative abort)
- Job schedulers (cron + interval)
- Metrics (getJobCounts, getMetrics)
- OpenTelemetry (optional peer dep)
- Graceful shutdown + connection recovery
- Custom backoff strategies

### Skipped (by design)
- Sandboxed processors (container isolation preferred over child_process)
- Worker autoscale (k8s HPA preferred over in-process scaling)

### Next steps
- Publish glide-mq to npm (switch speedkey from file: to npm dep)
- Update benchmarks to reflect latest numbers
- README for glide-mq
