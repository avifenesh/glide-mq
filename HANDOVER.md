# Handover

## Published Packages

| Package | Version | Registry |
|---------|---------|----------|
| glide-mq | 0.4.0 | npm |
| @glidemq/speedkey | 0.2.0 | npm (7 platform binaries) |
| @glidemq/dashboard | 0.1.0 | github (not yet on npm) |

## Repos

| Repo | Description |
|------|-------------|
| github.com/avifenesh/glide-mq | Queue library (main) |
| github.com/avifenesh/speedkey | Native Valkey client (extracted from valkey-glide PR #5325) |
| github.com/avifenesh/glidemq-dashboard | Dashboard Express middleware |
| github.com/avifenesh/glide-mq-demo | E-commerce order pipeline demo app |

## Performance

| Concurrency | glide-mq | BullMQ |
|-------------|----------|--------|
| c=1 | 4,376 jobs/s | 2,240 |
| c=5 | 14,925 | 9,390 |
| c=10 | 15,504 | 13,333 |
| c=50 | 48,077 | - |

Key optimization: completeAndFetchNext FCALL (1 RTT/job steady state).

## Features

Core: Queue, Worker, Job, QueueEvents, FlowProducer
Functions: 15 Valkey Server Functions (FUNCTION LOAD/FCALL, not EVAL)
Architecture: Streams-first (XREADGROUP + PEL + XAUTOCLAIM), cluster-native

Advanced: dedup (3 modes), rate limiting, global concurrency, per-job timeout, job log, lock renewal (heartbeat), DLQ, job revocation (cooperative abort), job schedulers (cron + interval), metrics, priorities, retention, compression (gzip)

Workflows: chain, group, chord + FlowProducer for parent-child trees

Cloud-native (GLIDE-exclusive): AZ-Affinity routing, IAM auth (ElastiCache/MemoryDB), Batch API pipelining, multiplexed connections

Observability: QueueEvents (SSE), OpenTelemetry spans, graceful shutdown, connection recovery

Search: Queue.searchJobs({ state, name, data, limit })
Testing: import { TestQueue, TestWorker } from 'glide-mq/testing' (no Valkey needed)
Dashboard: @glidemq/dashboard (separate package, Express middleware)

## Testing

- 750+ test executions (standalone + cluster parameterized via fixture)
- 60 bulletproof tests (known bugs from BullMQ, Celery, Sidekiq, Bee-Queue)
- 20 fuzz/chaos tests
- 4-pass code review (73 findings addressed)
- Code simplified 3 times, deslopped

## CI/CD

- glide-mq: GitHub Actions (typecheck + unit + integration)
- speedkey: 6-target CD with platform package publishing under @glidemq scope

## Research (agent-knowledge/, gitignored)

10 research guides from 200+ sources covering:
- Queue library internals (BullMQ, Bull, Bee-Queue)
- Valkey GLIDE client architecture
- Test case catalogs from 4 competitor libraries
- Bug reports and failure modes from 4 libraries
- User-requested features from 6 libraries
- Professional dashboard UX patterns

## NPM Token

Needs rotation (was shared in conversation earlier in the session).
