# Durability Guarantees

glide-mq is durable to the extent that your Valkey deployment is durable.

The queue stores jobs in native Valkey data structures:

- job hashes for payload, metadata, attempts, progress, and return values
- streams + consumer groups for waiting and active delivery state
- sorted sets for delayed / prioritized jobs, completed jobs, and failed jobs
- one Valkey function library for queue operations

Those structures survive restarts only if Valkey is configured to persist them.

## The Short Version

- `appendonly yes` is the recommended baseline for production queues.
- `appendfsync everysec` is a good default when you want low latency with a small worst-case loss window.
- `appendfsync always` minimizes the persistence window further, but costs throughput and latency.
- RDB-only snapshots are usually not enough for queues unless you can tolerate losing all writes since the last snapshot.
- glide-mq provides **at-least-once** delivery semantics, not exactly-once.
- If a worker crashes after claiming a job but before acking it, the job is not silently lost; another worker can reclaim it from the pending entries list.

## What Persists

When Valkey persistence is enabled, these glide-mq state transitions are persisted with the rest of the dataset:

- enqueued jobs
- delayed / prioritized jobs waiting in the scheduled sorted set
- completed / failed job state
- stream entries and consumer groups
- pending entries list (PEL) state for jobs that were claimed but not yet acked
- the `glidemq` Valkey function library itself

This matters because glide-mq uses Valkey Streams and consumer groups instead of a BRPOP-style list pop. A claimed-but-unacked job stays recoverable through the PEL until it is acked or reclaimed.

## Persistence Modes

### RDB snapshots

RDB snapshots write point-in-time dumps on a schedule such as `save 60 10000`.

Pros:

- low write overhead
- simple to operate

Trade-offs:

- a crash can lose every write since the last successful snapshot
- not ideal if you need queue state to survive process or host failure with a small loss window

For queues, RDB-only is usually a recovery/snapshot tool, not the primary durability strategy.

### AOF

AOF appends write operations to a log and is the usual recommendation for queue workloads.

Common `appendfsync` choices:

- `everysec`: better performance, but a crash can lose roughly the last second of acknowledged writes
- `always`: strongest persistence, but highest latency / lowest throughput
- `no`: lowest durability; usually not appropriate for production queues

### Hybrid AOF + RDB preamble

If your Valkey version supports it, keeping AOF enabled with the RDB preamble gives faster restarts while preserving the operational model of AOF.

## Recommended Valkey Config

For most production queue workloads, start here and tune from measurement:

```conf
appendonly yes
appendfsync everysec
aof-use-rdb-preamble yes
```

Then choose RDB snapshots based on your backup / restore needs, for example:

```conf
save 60 10000
```

If AOF is already enabled, treat RDB snapshots as optional and tune them against your write volume and save overhead. Frequent background saves can add fork / persistence pressure on busy queue workloads.

If your queue cannot tolerate the `everysec` loss window, move to `appendfsync always` and benchmark the throughput cost before rollout.

## Restart Behavior

With persistence enabled:

- waiting jobs remain in the stream
- delayed and prioritized jobs remain in the scheduled sorted set
- completed / failed jobs remain in their sorted sets and hashes
- consumer groups and pending entries survive with the stream data
- the Valkey function library survives in the dataset and is also re-ensured by clients on startup

glide-mq additionally re-ensures the function library when clients connect or reconnect, including after topology changes and cluster failover.

## Worker Crash Behavior

If a worker crashes mid-processing:

1. the job has already been claimed into the consumer group's pending entries list
2. it is **not** removed from Valkey just because the worker died
3. another worker's scheduler can reclaim the stalled entry via `XAUTOCLAIM`
4. after enough stalled-recovery cycles, glide-mq moves the job to `failed` instead of leaving it in limbo

This is an important difference from older queue designs that rely on destructive pops and app-side lock bookkeeping.

## Cluster Failover Behavior

glide-mq works with Valkey cluster replication and re-ensures its function library on reconnect/failover, but durability during failover still depends on the underlying replication and persistence settings.

Important caveat:

- Valkey replication is asynchronous
- a primary failure can still lose recently acknowledged writes that were not yet replicated or fsynced

That means failover improves availability, but it does not by itself create exactly-once or zero-loss guarantees.

## Delivery Semantics

glide-mq is designed for **at-least-once** processing:

- a job may be redelivered after worker crash, connection loss, or stalled reclaim
- processors should therefore be idempotent whenever side effects matter

glide-mq does **not** guarantee:

- exactly-once execution
- atomic coordination with external side effects
- zero-loss behavior independent of Valkey persistence / replication settings

## Comparison at a Glance

| System | Typical durability model | Delivery semantics | Main trade-off |
|--------|---------------------------|--------------------|----------------|
| glide-mq + Valkey AOF (`everysec`) | AOF-backed queue state with a small crash window | At-least-once | Lower write cost than `always`, in exchange for a small possible loss window on hard crash |
| glide-mq + Valkey AOF (`always`) | Strongest Valkey persistence mode | At-least-once | Smallest persistence window, in exchange for higher write cost |
| pg-boss / PostgreSQL | Backed by PostgreSQL WAL / transaction durability | At-least-once | Strong database durability, in exchange for PostgreSQL operational and latency tradeoffs |
| RabbitMQ with durable queues + persistent messages | Broker durability when queues/messages are durable and producers use publisher confirms appropriately | At-least-once | Mature broker durability model, in exchange for more broker-specific configuration surface |

Compared with BullMQ-style Redis queues, glide-mq still depends on the same underlying Valkey durability settings, but its in-flight recovery model is built on Streams consumer groups, the PEL, and `XAUTOCLAIM` rather than a separate lock-renew / stall-detection cycle.

## Practical Guidance

- If your priority is throughput with strong-enough durability, use AOF with `appendfsync everysec`.
- If your priority is minimizing acknowledged-write loss on crash, use AOF with `appendfsync always` and benchmark first.
- If your jobs trigger irreversible external side effects, make handlers idempotent and store your own deduplication / business keys.
- Treat failover as an availability feature, not proof of zero-loss durability.

## Related Reading

- [Architecture](ARCHITECTURE.md)
- [Advanced](ADVANCED.md)
- [Usage](USAGE.md)
- [Testing](TESTING.md)
