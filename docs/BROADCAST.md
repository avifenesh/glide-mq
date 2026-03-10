# Broadcast

`Broadcast` is a pub/sub fan-out primitive. Unlike `Queue` (point-to-point, each job processed by exactly one worker), `Broadcast` delivers every message to **all** subscribers.

## Quick start

```typescript
import { Broadcast, BroadcastWorker } from 'glide-mq';

const connection = { addresses: [{ host: 'localhost', port: 6379 }] };

const broadcast = new Broadcast('events', {
  connection,
  maxMessages: 1000,  // retain at most 1000 messages in the stream
});

// Each subscriber is identified by a unique subscription name (becomes a consumer group)
const inventoryWorker = new BroadcastWorker('events', async (job) => {
  console.log('Inventory update:', job.data);
}, { connection, subscription: 'inventory-service' });

const emailWorker = new BroadcastWorker('events', async (job) => {
  console.log('Send notification:', job.data);
}, { connection, subscription: 'email-service' });

// Publish -- every subscriber receives this message
await broadcast.publish({ event: 'order.placed', orderId: 42 });
```

## Queue vs Broadcast

| | Queue | Broadcast |
|---|---|---|
| Delivery | Point-to-point (one consumer) | Fan-out (all subscribers) |
| Use case | Task processing, job queues | Event distribution, notifications |
| Add / Publish | `queue.add(name, data, opts)` | `broadcast.publish(data, opts?)` |
| Consumer | `Worker` | `BroadcastWorker` |
| Retry / backoff | Per job | Per subscriber, per message |
| Stream trimming | Auto (completion/removal) | `maxMessages` option |

## BroadcastWorker options

Each `BroadcastWorker` supports the same options as `Worker` (concurrency, limiter, backoff, etc.) plus:

- `subscription` (required) -- unique name for this subscriber. Becomes the consumer group.
- `startFrom` -- stream ID to start reading from when the subscription is first created:
  - `'$'` (default) -- only new messages published after subscription creation.
  - `'0-0'` -- replay all retained history (backfill).
- `subjects` -- array of subject patterns for filtering (see below).

```typescript
const replayWorker = new BroadcastWorker('events', processor, {
  connection,
  subscription: 'analytics',
  startFrom: '0-0',     // backfill all existing messages
  concurrency: 5,
});
```

## Subject filtering

BroadcastWorker supports NATS-style subject filtering via the `subjects` option. When set, only messages whose job name matches at least one pattern are processed. Non-matching messages are auto-acknowledged and skipped.

### Patterns

Subject patterns use `.` as a token separator:

- `*` matches exactly one token
- `>` matches one or more tokens (must be the last token)
- Literal tokens match exactly

### Examples

| Pattern | Matches | Does not match |
|---------|---------|----------------|
| `orders.created` | `orders.created` | `orders.updated`, `orders.created.us` |
| `orders.*` | `orders.created`, `orders.updated` | `orders.created.us`, `inventory.created` |
| `orders.>` | `orders.created`, `orders.created.us`, `orders.a.b.c` | `inventory.created` |
| `*.created` | `orders.created`, `inventory.created` | `orders.updated`, `orders.created.us` |

### Usage

```typescript
// Only process order events
const orderWorker = new BroadcastWorker('events', async (job) => {
  console.log('Order event:', job.name, job.data);
}, {
  connection,
  subscription: 'order-handler',
  subjects: ['orders.*'],
  concurrency: 5,
});

// Process all events under a namespace
const allOrderWorker = new BroadcastWorker('events', async (job) => {
  console.log('Deep order event:', job.name, job.data);
}, {
  connection,
  subscription: 'order-deep-handler',
  subjects: ['orders.>'],
});

// Multiple patterns
const mixedWorker = new BroadcastWorker('events', async (job) => {
  console.log('Relevant event:', job.name, job.data);
}, {
  connection,
  subscription: 'mixed-handler',
  subjects: ['orders.*', 'inventory.low', 'shipping.>'],
});
```

### How it works

1. `BroadcastWorker` compiles the `subjects` array into a matcher function at construction time using `compileSubjectMatcher`.
2. On each poll, after reading messages from the stream, the worker checks each job's `name` field against the matcher.
3. Non-matching messages are immediately acknowledged (`XACK`) and skipped -- they never reach the processor.
4. If `subjects` is not set or empty, all messages are processed (no filtering).

### Publishing with names

For subject filtering to work, publish messages with a `name` that follows the dotted convention:

```typescript
await broadcast.publish({ orderId: 42 }, { name: 'orders.created' });
await broadcast.publish({ orderId: 42, status: 'shipped' }, { name: 'orders.shipped' });
await broadcast.publish({ sku: 'ABC', qty: 0 }, { name: 'inventory.low' });
```

### Utility functions

`matchSubject` and `compileSubjectMatcher` are exported from `glide-mq` for use in custom filtering logic:

```typescript
import { matchSubject, compileSubjectMatcher } from 'glide-mq';

matchSubject('orders.*', 'orders.created');  // true
matchSubject('orders.*', 'orders.a.b');      // false

const matcher = compileSubjectMatcher(['orders.*', 'shipping.>']);
matcher('orders.created');    // true
matcher('shipping.us.west');  // true
matcher('inventory.low');     // false
```

## Closing

```typescript
await broadcast.close();
await inventoryWorker.close();
await emailWorker.close();
```

Both `Broadcast` and `BroadcastWorker` support graceful shutdown via `close()`. The worker drains in-progress jobs before disconnecting.
