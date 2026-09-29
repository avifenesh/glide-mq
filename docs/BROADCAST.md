# Broadcast

`Broadcast` is a pub/sub fan-out primitive. Unlike `Queue` (point-to-point, each job processed by exactly one worker), `Broadcast` delivers every message to **all** subscribers.

## Quick start

```typescript
import { Broadcast, BroadcastWorker } from 'glide-mq';

const connection = { addresses: [{ host: 'localhost', port: 6379 }] };

const broadcast = new Broadcast('events', {
  connection,
  maxMessages: 1000, // retain at most 1000 messages in the stream
});

// Each subscriber is identified by a unique subscription name (becomes a consumer group)
const inventoryWorker = new BroadcastWorker(
  'events',
  async (job) => {
    console.log('Inventory update:', job.data);
  },
  { connection, subscription: 'inventory-service' },
);

const emailWorker = new BroadcastWorker(
  'events',
  async (job) => {
    console.log('Send notification:', job.data);
  },
  { connection, subscription: 'email-service' },
);

// Publish - every subscriber receives this message
await broadcast.publish('order.placed', { event: 'order.placed', orderId: 42 });
```

## Queue vs Broadcast

|                 | Queue                         | Broadcast                                 |
| --------------- | ----------------------------- | ----------------------------------------- |
| Delivery        | Point-to-point (one consumer) | Fan-out (all subscribers)                 |
| Use case        | Task processing, job queues   | Event distribution, notifications         |
| Add / Publish   | `queue.add(name, data, opts)` | `broadcast.publish(subject, data, opts?)` |
| Consumer        | `Worker`                      | `BroadcastWorker`                         |
| Retry / backoff | Per job                       | Per subscriber, per message               |
| Stream trimming | Auto (completion/removal)     | `maxMessages` option                      |

## BroadcastWorker options

Each `BroadcastWorker` supports the same options as `Worker` (concurrency, limiter, backoff, etc.) plus:

- `subscription` (required) - unique name for this subscriber. Becomes the consumer group.
- `startFrom` - stream ID to start reading from when the subscription is first created:
  - `'$'` (default) - only new messages published after subscription creation.
  - `'0-0'` - replay all retained history (backfill).
- `subjects` - array of subject patterns for filtering (see below).

```typescript
const replayWorker = new BroadcastWorker('events', processor, {
  connection,
  subscription: 'analytics',
  startFrom: '0-0', // backfill all existing messages
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

| Pattern          | Matches                                               | Does not match                           |
| ---------------- | ----------------------------------------------------- | ---------------------------------------- |
| `orders.created` | `orders.created`                                      | `orders.updated`, `orders.created.us`    |
| `orders.*`       | `orders.created`, `orders.updated`                    | `orders.created.us`, `inventory.created` |
| `orders.>`       | `orders.created`, `orders.created.us`, `orders.a.b.c` | `inventory.created`                      |
| `*.created`      | `orders.created`, `inventory.created`                 | `orders.updated`, `orders.created.us`    |

### Usage

```typescript
// Only process order events
const orderWorker = new BroadcastWorker(
  'events',
  async (job) => {
    console.log('Order event:', job.name, job.data);
  },
  {
    connection,
    subscription: 'order-handler',
    subjects: ['orders.*'],
    concurrency: 5,
  },
);

// Process all events under a namespace
const allOrderWorker = new BroadcastWorker(
  'events',
  async (job) => {
    console.log('Deep order event:', job.name, job.data);
  },
  {
    connection,
    subscription: 'order-deep-handler',
    subjects: ['orders.>'],
  },
);

// Multiple patterns
const mixedWorker = new BroadcastWorker(
  'events',
  async (job) => {
    console.log('Relevant event:', job.name, job.data);
  },
  {
    connection,
    subscription: 'mixed-handler',
    subjects: ['orders.*', 'inventory.low', 'shipping.>'],
  },
);
```

### How it works

1. `BroadcastWorker` compiles the `subjects` array into a matcher function at construction time using `compileSubjectMatcher`.
2. On each poll, after reading messages from the stream, the worker checks each job's `name` field against the matcher.
3. Non-matching messages are immediately acknowledged (`XACK`) and skipped - they never reach the processor.
4. If `subjects` is not set or empty, all messages are processed (no filtering).

### Publishing with names

For subject filtering to work, publish messages with a `name` that follows the dotted convention:

```typescript
await broadcast.publish('orders.created', { orderId: 42 });
await broadcast.publish('orders.shipped', { orderId: 42, status: 'shipped' });
await broadcast.publish('inventory.low', { sku: 'ABC', qty: 0 });
```

### Utility functions

`matchSubject` and `compileSubjectMatcher` are exported from `glide-mq` for use in custom filtering logic:

```typescript
import { matchSubject, compileSubjectMatcher } from 'glide-mq';

matchSubject('orders.*', 'orders.created'); // true
matchSubject('orders.*', 'orders.a.b'); // false

const matcher = compileSubjectMatcher(['orders.*', 'shipping.>']);
matcher('orders.created'); // true
matcher('shipping.us.west'); // true
matcher('inventory.low'); // false
```

## Closing

```typescript
await broadcast.close();
await inventoryWorker.close();
await emailWorker.close();
```

Both `Broadcast` and `BroadcastWorker` support graceful shutdown via `close()`. The worker drains in-progress jobs before disconnecting.

## Crash and stall recovery

A message a subscription's worker claimed but did not finish stays in that subscription's pending entries list: the worker was killed, force-closed with `close(true)`, or received the message while it was closing. Another `BroadcastWorker` on the same subscription reclaims it once the claim has been idle for `stalledInterval` and the message's `lastActive` is older than `lockDuration`, then runs it again. Other subscriptions are not affected.

- Stalls are counted per subscription, in `job:<id>:sub:<subscription>` (24h TTL). After more than `maxStalledCount` stalls in one subscription, the message fails, like a terminal processor failure in that subscription (the message hash, shared by all subscriptions, is marked failed).
- Recovery needs a running `BroadcastWorker` on that subscription. A restarted process gets a new consumer ID, so its own old claims are recovered the same way.
- `lastActive` is shared by all subscriptions. While another subscription is still processing the same message, stall detection waits until that heartbeat stops.
- A reclaimed message that the reclaiming worker has not started when it closes or pauses stays in its pending list and is reclaimed again, which counts one more stall.

## Retention

`removeOnComplete` and `removeOnFail` do not apply to broadcast messages: one message hash is shared by all subscriptions. Without `maxMessages`, every published message keeps its stream entry, job hash and completed/failed set member.

`maxMessages` is a hard cap, applied on each publish:

- The oldest entries are trimmed even when a subscription has not read them yet. That subscription never sees them, and no event is emitted. Size `maxMessages` for the lag of your slowest subscriber.
- For each trimmed message, the publish also deletes its job hash, per-subscription hashes (`job:<id>:sub:<subscription>`), log and completed/failed set member.
- A trimmed message that a subscription still has claimed, has scheduled for a retry, or has a newer retry entry for keeps its data until that settles. So does a message parked outside the stream (delayed, suspended, group-waiting, waiting-children). Claims held this way are re-checked by later publishes, so their data waits for the next publish.
- One publish trims at most 1000 entries. Lowering `maxMessages` on a large stream converges over several publishes.

## HTTP proxy

The proxy exposes broadcast publish and SSE fan-out over HTTP:

```http
POST /broadcast/notifications
Content-Type: application/json

{ "subject": "orders.created", "data": { "orderId": 42 } }
```

```http
GET /broadcast/notifications/events?subscription=analytics&subjects=orders.*,inventory.low
Accept: text/event-stream
```

- `subscription` is required on the SSE route and becomes the consumer-group name, just like `BroadcastWorker`.
- `subjects` is optional and uses the same NATS-style matcher syntax as `BroadcastWorker`.
- SSE messages are emitted as `event: message` with JSON `{ id, subject, data, timestamp }`.

See [Usage - Proxy Endpoints](./USAGE.md#proxy-endpoints) for the full HTTP route table.
