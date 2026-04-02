# Serverless / Edge Guide

Lightweight enqueueing for AWS Lambda and other server-side runtimes where persistent connections are expensive or impossible. For edge runtimes without NAPI support, use the HTTP proxy pattern instead of importing `glide-mq` directly.

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

Cloudflare Workers do not provide the server-side NAPI runtime that `glide-mq` requires. Use the HTTP proxy from a Worker instead:

```typescript
export default {
  async fetch(request: Request, env: any) {
    const body = await request.json();
    const res = await fetch(`${env.GLIDEMQ_PROXY_URL}/queues/tasks/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${env.PROXY_TOKEN}` },
      body: JSON.stringify({ name: 'process-webhook', data: body }),
    });
    return new Response(await res.text(), {
      status: res.status,
      headers: { 'content-type': res.headers.get('content-type') ?? 'application/json' },
    });
  },
};
```

## Vercel Edge Functions

Vercel Edge has the same limitation. Use `fetch()` against the HTTP proxy or another server-side endpoint that owns the Valkey connection:

```typescript
export default async function handler(req: Request) {
  const body = await req.json();
  const res = await fetch(`${process.env.GLIDEMQ_PROXY_URL}/queues/analytics/jobs`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${process.env.PROXY_TOKEN!}`,
    },
    body: JSON.stringify({ name: 'track-event', data: body }),
  });

  return new Response(await res.text(), {
    status: res.status,
    headers: { 'content-type': res.headers.get('content-type') ?? 'application/json' },
  });
}
```

If you control a Node.js serverless runtime instead of an edge runtime, `Producer` and `ServerlessPool` remain the preferred direct integration.

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

## HTTP Proxy

Use `createProxyServer()` when you need cross-language producers, request-reply, or SSE consumers from environments that should not host long-lived queue objects.

```typescript
import { createProxyServer } from 'glide-mq/proxy';

const proxy = createProxyServer({
  connection: CONNECTION,
  queues: ['emails', 'events'], // optional allowlist
  compression: 'gzip',
});

proxy.app.listen(3000);
```

### Proxy Options

| Option | Type | Notes |
| ------ | ---- | ----- |
| `connection` | `ConnectionOptions` | Required unless `client` is provided. Required for queue-wide/broadcast SSE routes. |
| `client` | `Client` | Shared GLIDE client for non-blocking routes. |
| `prefix` | `string` | Key prefix (default: `glide`). |
| `queues` | `string[]` | Optional allowlist. Unlisted queue names return `403`. |
| `compression` | `'none' \| 'gzip'` | Transparent job payload compression. |
| `onError` | `(err, queueName) => void` | Queue-level error hook. |

### Route Surface

The proxy now covers the main queue-management surface:

- Queue writes: add, bulk add, add-and-wait, pause/resume, drain, retry, clean
- Job operations: fetch job, list jobs by state, change priority, change delay, promote delayed jobs, stream job output over SSE, send suspend/resume signals
- Queue telemetry: counts, metrics, live workers, queue-wide events over SSE
- Scheduler APIs: list, fetch, upsert, remove
- Flow APIs: create tree flows or DAGs, inspect flow snapshots, read nested tree views, revoke outstanding flow jobs
- AI APIs: flow usage, flow budget, rolling usage summary
- Broadcast APIs: publish plus SSE fan-out with `subscription` and `subjects` filters
- Health: `/health`

See [Usage - Proxy Endpoints](./USAGE.md#proxy-endpoints) for the exact method/path table.

- Add your own auth and rate limiting middleware before exposing the proxy publicly. `glide-mq/proxy` does not ship built-in auth.
- Queue-wide SSE (`/queues/:name/events`) and broadcast SSE (`/broadcast/:name/events`) need `connection`, not only a shared `client`, because they allocate blocking readers.

## AI Primitives in Serverless

The Producer class supports all job options including fallbacks and lockDuration. However, AI-specific runtime operations (reportUsage, stream, suspend) are only available inside a Worker processor.

For serverless AI patterns:

- **Enqueue with fallbacks**: Set fallbacks in JobOptions when adding jobs from Lambda/Edge.
- **Budget on flows**: Create flows via FlowProducer with budget options from any environment.
- **Cross-language flow orchestration**: Use `POST /flows` to create tree flows or DAGs over HTTP. Flow budgets in the HTTP API are currently supported on tree flows only.
- **Read results**: Use `queue.getFlowUsage()`, `queue.getUsageSummary()`, or `queue.readStream()` from a serverless function to read back AI usage or streaming output after the worker processes the job.
