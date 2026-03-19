# Serverless & Testing Reference

## Producer (Lightweight Queue.add)

No EventEmitter, no Job instances, no state tracking. Same FCALL functions as Queue.

```typescript
import { Producer } from 'glide-mq';

const producer = new Producer('emails', {
  connection: ConnectionOptions,  // required unless `client` provided
  client?: Client,               // pre-existing GLIDE client (not owned)
  prefix?: string,               // default: 'glide'
  compression?: 'none' | 'gzip', // default: 'none'
  serializer?: Serializer,       // default: JSON
  events?: boolean,              // emit 'added' events (default: true, set false to save 1 call)
});

// Returns string ID (not Job object) or null for dedup/collision
const id = await producer.add('send-welcome', { to: 'user@example.com' });
const id = await producer.add('urgent', data, { delay: 3600000, priority: 1 });

// Bulk - returns (string | null)[]
const ids = await producer.addBulk([
  { name: 'email', data: { to: 'a@test.com' } },
  { name: 'sms', data: { phone: '+123' } },
]);

await producer.close();  // if external client was provided, it is NOT closed
```

All `JobOptions` work: delay, priority, deduplication, jobId, ordering, ttl, lifo, cost.

## ServerlessPool

Reuses connections across warm Lambda/Edge invocations.

```typescript
import { serverlessPool, ServerlessPool } from 'glide-mq';

// Module-level singleton
const producer = serverlessPool.getProducer('notifications', {
  connection: { addresses: [{ host: process.env.VALKEY_HOST!, port: 6379 }] },
});
await producer.add('push', { userId: 42 });

// Or create your own pool
const pool = new ServerlessPool();
const p = pool.getProducer('queue', { connection });
await pool.closeAll();
```

### AWS Lambda Example

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

process.on('SIGTERM', async () => { await serverlessPool.closeAll(); });
```

### Connection Behavior

- **Cold start**: creates new GLIDE connection + loads function library
- **Warm invocation**: returns cached producer (zero overhead)
- **Container freeze/thaw**: GLIDE auto-reconnects on next command

## HTTP Proxy

Express-based HTTP proxy for enqueueing from any language/environment.

```typescript
import { createProxyServer } from 'glide-mq/proxy';

const proxy = createProxyServer({
  connection: ConnectionOptions,  // required unless client provided
  client?: Client,               // pre-existing GLIDE client
  prefix?: string,               // default: 'glide'
  queues?: string[],             // allowlist (403 for unlisted queues)
  compression?: 'none' | 'gzip',
  onError?: (err, queueName) => void,
});

proxy.app.listen(3000);
await proxy.close();  // shuts down all cached Queue instances
```

### Proxy Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/queues/:name/jobs` | Add single job `{ name, data?, opts? }` |
| POST | `/queues/:name/jobs/bulk` | Add bulk `{ jobs: [...] }` (max 1000) |
| GET | `/queues/:name/jobs/:id` | Get job details |
| GET | `/queues/:name/counts` | Get job counts |
| POST | `/queues/:name/pause` | Pause queue |
| POST | `/queues/:name/resume` | Resume queue |
| GET | `/health` | `{ status, uptime, queues }` |

## Testing (In-Memory)

No Valkey needed. Import from `glide-mq/testing`.

```typescript
import { TestQueue, TestWorker } from 'glide-mq/testing';

const queue = new TestQueue('tasks');   // no connection config needed
const worker = new TestWorker(queue, async (job) => {
  return { processed: job.data };
});

worker.on('completed', (job, result) => { ... });
worker.on('failed', (job, err) => { ... });

await queue.add('send-email', { to: 'user@example.com' });
const counts = await queue.getJobCounts();
// { waiting: 0, active: 0, delayed: 0, completed: 1, failed: 0 }

await worker.close();
await queue.close();
```

### TestQueue API

| Method | Notes |
|--------|-------|
| `add(name, data, opts?)` | Triggers processing immediately |
| `addBulk(jobs)` | Bulk add |
| `getJob(id)` | By ID |
| `getJobs(state, start?, end?)` | By state |
| `getJobCounts()` | `{ waiting, active, delayed, completed, failed }` |
| `searchJobs({ state?, name?, data? })` | Filter by state/name/data (shallow match) |
| `drain(delayed?)` | Remove waiting (+ delayed if true) |
| `pause()` / `resume()` | Pause/resume |
| `isPaused()` | Synchronous (note: real Queue is async) |

### TestJob API

| Method | Notes |
|--------|-------|
| `changePriority(n)` | Re-prioritize |
| `changeDelay(n)` | Change delay |
| `promote()` | Delayed -> waiting immediately |

### TestWorker Events

Same as Worker: `active`, `completed`, `failed`, `drained`.

### Batch Testing

```typescript
const worker = new TestWorker(queue, async (jobs) => {
  return jobs.map(j => ({ doubled: j.data.n * 2 }));
}, { batch: { size: 5, timeout: 100 } });
```

### Key Testing Behaviors

- Processing is synchronous-ish - check state right after `await queue.add()`.
- Delayed jobs become waiting immediately (delay not honored in test mode).
- `moveToDelayed` not supported in test mode.
- Custom jobId returns `null` on duplicate (mirrors production).
- All three dedup modes (`simple`, `throttle`, `debounce`) work.
- Retries work normally with `attempts` and `backoff`.
- Swap without changing processors - same interface as Queue/Worker.

## Gotchas

- Producer returns `string` IDs, not `Job` objects.
- Producer `close()` does NOT close an externally provided `client`.
- `serverlessPool` is a module-level singleton - shared across handler invocations.
- HTTP proxy requires `express` as a peer dependency.
- Proxy `queues` option is an allowlist - unlisted names get 403.
- TestQueue `isPaused()` is synchronous (real Queue returns Promise).
- Test mode does not honor `delay` or `moveToDelayed`.
