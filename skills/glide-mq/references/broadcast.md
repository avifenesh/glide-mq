# Broadcast Reference

## Overview

`Broadcast` is pub/sub fan-out. Unlike `Queue` (point-to-point), every message is delivered to **all** subscribers.

## Broadcast Constructor

```typescript
import { Broadcast, BroadcastWorker } from 'glide-mq';

const broadcast = new Broadcast('events', {
  connection: ConnectionOptions,
  maxMessages?: number,  // retain at most N messages in the stream
});
```

## Publishing

```typescript
// publish(subject, data, opts?) - subject is the first arg
await broadcast.publish('orders', { event: 'order.placed', orderId: 42 });

// With dotted subjects (for subject filtering)
await broadcast.publish('orders.created', { orderId: 42 });
await broadcast.publish('inventory.low', { sku: 'ABC', qty: 0 });

await broadcast.close();
```

## BroadcastWorker Constructor

```typescript
const worker = new BroadcastWorker(
  'events',                         // broadcast name
  async (job) => {                  // processor
    console.log(job.name, job.data);
  },
  {
    connection: ConnectionOptions,
    subscription: string,           // REQUIRED - unique subscriber name (consumer group)
    startFrom?: string,             // '$' (default, new only) | '0-0' (replay all history)
    subjects?: string[],            // NATS-style subject filter patterns
    concurrency?: number,           // same as Worker
    limiter?: { max, duration },    // same as Worker
    // All other Worker options supported (backoff, etc.)
  },
);

await worker.close();
```

## Subject Filtering (NATS-style)

Patterns use `.` as token separator:

| Token | Meaning |
|-------|---------|
| `*` | Matches exactly one token |
| `>` | Matches one or more tokens (must be last token) |
| literal | Matches exactly |

### Pattern Examples

| Pattern | Matches | Does NOT match |
|---------|---------|----------------|
| `orders.created` | `orders.created` | `orders.updated`, `orders.created.us` |
| `orders.*` | `orders.created`, `orders.updated` | `orders.created.us` |
| `orders.>` | `orders.created`, `orders.created.us`, `orders.a.b.c` | `inventory.created` |
| `*.created` | `orders.created`, `inventory.created` | `orders.updated` |

### Usage

```typescript
// Single pattern
const worker = new BroadcastWorker('events', processor, {
  connection,
  subscription: 'order-handler',
  subjects: ['orders.*'],
});

// Multiple patterns
const worker = new BroadcastWorker('events', processor, {
  connection,
  subscription: 'mixed-handler',
  subjects: ['orders.*', 'inventory.low', 'shipping.>'],
});
```

### How Filtering Works

1. `subjects` compiled to matcher at construction via `compileSubjectMatcher`.
2. Non-matching messages are auto-acknowledged (`XACK`) and skipped.
3. Empty/unset `subjects` = all messages processed.

### Utility Functions

```typescript
import { matchSubject, compileSubjectMatcher } from 'glide-mq';

matchSubject('orders.*', 'orders.created');  // true
matchSubject('orders.*', 'orders.a.b');      // false

const matcher = compileSubjectMatcher(['orders.*', 'shipping.>']);
matcher('orders.created');    // true
matcher('shipping.us.west');  // true
matcher('inventory.low');     // false
```

## Queue vs Broadcast

| | Queue | Broadcast |
|---|---|---|
| Delivery | Point-to-point (one consumer) | Fan-out (all subscribers) |
| Use case | Task processing | Event distribution |
| API | `queue.add(name, data, opts)` | `broadcast.publish(subject, data, opts?)` |
| Consumer | `Worker` | `BroadcastWorker` |
| Retry | Per job | Per subscriber, per message |
| Trimming | Auto (completion/removal) | `maxMessages` option |

## HTTP Proxy

Cross-language producers and consumers can use the proxy instead of `Broadcast` / `BroadcastWorker` directly:

| Method | Path | Description |
|--------|------|-------------|
| POST | `/broadcast/:name` | Publish `{ subject, data?, opts? }` |
| GET | `/broadcast/:name/events` | SSE fan-out stream. Requires `subscription`; optional `subjects=a.*,b.>` |

SSE payloads arrive as `event: message` with JSON `{ id, subject, data, timestamp }`.

## Gotchas

- `subscription` is required on BroadcastWorker - it becomes the consumer group name.
- Proxy SSE `subscription` follows the same rule and becomes the consumer-group name.
- Subject filtering requires publishing with a `name` using dotted convention.
- `>` wildcard must be the **last** token in the pattern.
- `startFrom: '0-0'` replays all retained history (backfill).
- Per-subscriber retries - each subscriber independently retries failed messages.
