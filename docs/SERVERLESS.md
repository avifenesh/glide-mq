# Serverless / Edge Guide

Lightweight enqueueing for AWS Lambda, Cloudflare Workers, Vercel Edge Functions, and similar environments where persistent connections are expensive or impossible.

## Why Producer over Queue

The `Queue` class extends `EventEmitter`, creates `Job` instances, and is designed for long-running processes. In serverless environments this overhead is wasted:

|                  | Queue              | Producer                      |
| ---------------- | ------------------ | ----------------------------- |
| EventEmitter     | Yes                | No                            |
| Job instances    | Returns `Job<D,R>` | Returns `string` ID           |
| State tracking   | Yes                | No                            |
| Connection reuse | Manual             | Built-in via `ServerlessPool` |
| Overhead         | ~2KB per instance  | Minimal                       |

Both use the **same FCALL functions** on the server - jobs created by `Producer` are identical to those created by `Queue` and are processed by the same `Worker`.

## Quick Start

```typescript
import { Producer } from 'glide-mq';

const producer = new Producer('emails', {
  connection: { addresses: [{ host: 'localhost', port: 6379 }] },
  events: false, // skip XADD event emission - saves 1 redis.call() per add
});

const jobId = await producer.add('send-welcome', {
  to: 'user@example.com',
  subject: 'Welcome!',
});

console.log(`Enqueued job ${jobId}`);
await producer.close();
```

## AWS Lambda

Use `ServerlessPool` to reuse connections across warm invocations:

```typescript
import { serverlessPool } from 'glide-mq';

const CONNECTION = {
  addresses: [{ host: process.env.VALKEY_HOST!, port: 6379 }],
};

export async function handler(event: any) {
  const producer = serverlessPool.getProducer('notifications', {
    connection: CONNECTION,
  });

  const id = await producer.add('push-notification', {
    userId: event.userId,
    message: event.message,
  });

  return { statusCode: 200, body: JSON.stringify({ jobId: id }) };
}

// Optional: clean up on SIGTERM (Lambda extension)
process.on('SIGTERM', async () => {
  await serverlessPool.closeAll();
});
```

On cold starts, `getProducer` creates a new connection. On warm invocations, it returns the cached producer - zero connection overhead.

## Cloudflare Workers

For short-lived environments without connection reuse:

```typescript
import { Producer } from 'glide-mq';

export default {
  async fetch(request: Request, env: any) {
    const producer = new Producer('tasks', {
      connection: {
        addresses: [{ host: env.VALKEY_HOST, port: 6379 }],
        useTLS: true,
      },
    });

    try {
      const body = await request.json();
      const id = await producer.add('process-webhook', body);
      return new Response(JSON.stringify({ jobId: id }), { status: 200 });
    } finally {
      await producer.close();
    }
  },
};
```

## Vercel Edge Functions

```typescript
import { serverlessPool } from 'glide-mq';

const CONNECTION = {
  addresses: [{ host: process.env.VALKEY_HOST!, port: 6379 }],
  useTLS: true,
};

export default async function handler(req: Request) {
  const producer = serverlessPool.getProducer('analytics', {
    connection: CONNECTION,
  });

  const data = await req.json();
  const id = await producer.add('track-event', data);
  return Response.json({ jobId: id });
}

export const config = { runtime: 'edge' };
```

## Bulk Enqueueing

Use `addBulk()` to pipeline multiple jobs in a single round trip:

```typescript
const ids = await producer.addBulk([
  { name: 'send-email', data: { to: 'a@test.com' } },
  { name: 'send-email', data: { to: 'b@test.com' } },
  { name: 'send-sms', data: { phone: '+1234567890' } },
]);
// ids: ['1', '2', '3']
```

## All Queue.add() Features Work

Producer supports the same `JobOptions` as `Queue.add()`:

```typescript
// Delayed job
await producer.add('reminder', data, { delay: 3600000 });

// Priority
await producer.add('urgent', data, { priority: 1 });

// Deduplication
await producer.add('idempotent', data, {
  deduplication: { id: 'unique-key', ttl: 60000 },
});

// Custom job ID
await producer.add('named', data, { jobId: 'order-12345' });

// Ordering key
await producer.add('sequential', data, {
  ordering: { key: 'user-42', concurrency: 1 },
});

// TTL
await producer.add('ephemeral', data, { ttl: 30000 });

// Compression (set at Producer level)
const compressed = new Producer('q', {
  connection: CONNECTION,
  compression: 'gzip',
});
```

## API Reference

### `new Producer(name, opts)`

| Parameter          | Type                | Description                                           |
| ------------------ | ------------------- | ----------------------------------------------------- |
| `name`             | `string`            | Queue name                                            |
| `opts.connection`  | `ConnectionOptions` | Connection config (required unless `client` provided) |
| `opts.client`      | `Client`            | Pre-existing GLIDE client (not owned by Producer)     |
| `opts.prefix`      | `string`            | Key prefix (default: `'glide'`)                       |
| `opts.compression` | `'none' \| 'gzip'`  | Compression mode (default: `'none'`)                  |
| `opts.serializer`  | `Serializer`        | Custom serializer (default: JSON)                     |
| `opts.events`      | `boolean`           | Emit 'added' events on add (default: `true`)          |

### `producer.add(name, data, opts?)`

Returns `Promise<string | null>` - job ID or `null` for dedup/collision.

### `producer.addBulk(jobs)`

Returns `Promise<(string | null)[]>` - array of job IDs.

### `producer.close()`

Closes the owned connection. If an external `client` was provided, it is not closed.

### `ServerlessPool`

```typescript
import { serverlessPool, ServerlessPool } from 'glide-mq';

// Use the module-level singleton
const producer = serverlessPool.getProducer('queue', { connection });

// Or create your own pool
const pool = new ServerlessPool();
pool.getProducer('queue', { connection });
await pool.closeAll();
```

## Connection Behavior

- **Cold start**: `getClient()` creates a new GLIDE connection and loads the function library
- **Warm invocation**: Returns the cached client immediately
- **Container freeze/thaw**: GLIDE auto-reconnects on next command
- **SIGTERM**: Call `serverlessPool.closeAll()` or `producer.close()` for clean shutdown
