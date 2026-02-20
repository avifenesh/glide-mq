# Testing

glide-mq ships a built-in in-memory backend so you can unit-test job processors **without a running Valkey instance**.

## Table of Contents

- [TestQueue and TestWorker](#testqueue-and-testworker)
- [API Surface](#api-surface)
- [Searching Jobs](#searching-jobs)
- [Retry Behaviour in Tests](#retry-behaviour-in-tests)
- [Tips](#tips)

---

## TestQueue and TestWorker

Import from `glide-mq/testing`:

```typescript
import { TestQueue, TestWorker } from 'glide-mq/testing';

const queue  = new TestQueue('tasks');
const worker = new TestWorker(queue, async (job) => {
  // same processor signature as the real Worker
  return { processed: job.data };
});

worker.on('completed', (job, result) => {
  console.log(`Job ${job.id} done:`, result);
});

worker.on('failed', (job, err) => {
  console.error(`Job ${job.id} failed:`, err.message);
});

await queue.add('send-email', { to: 'user@example.com' });

// Check state without touching Valkey
const counts = await queue.getJobCounts();
// { waiting: 0, active: 0, delayed: 0, completed: 1, failed: 0 }

await worker.close();
await queue.close();
```

### Using with a test framework (Vitest / Jest)

```typescript
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { TestQueue, TestWorker } from 'glide-mq/testing';

describe('email processor', () => {
  let queue: TestQueue;
  let worker: TestWorker;

  beforeEach(() => {
    queue  = new TestQueue('email');
    worker = new TestWorker(queue, async (job) => {
      if (!job.data.to) throw new Error('missing recipient');
      return { sent: true };
    });
  });

  afterEach(async () => {
    await worker.close();
    await queue.close();
  });

  it('processes a valid email job', async () => {
    await queue.add('send', { to: 'a@b.com', subject: 'Hi' });
    const job = (await queue.getJobs('completed'))[0];
    expect(job?.returnvalue).toEqual({ sent: true });
  });

  it('fails when recipient is missing', async () => {
    await queue.add('send', { subject: 'No to' });
    const job = (await queue.getJobs('failed'))[0];
    expect(job?.failedReason).toMatch('missing recipient');
  });
});
```

---

## API Surface

`TestQueue` and `TestWorker` mirror the public API of the real `Queue` and `Worker`:

### TestQueue

| Method | Description |
|--------|-------------|
| `add(name, data, opts?)` | Enqueue a job; triggers processing immediately |
| `addBulk(jobs)` | Enqueue multiple jobs |
| `getJob(id)` | Retrieve a job by ID |
| `getJobs(state, start?, end?)` | List jobs by state |
| `getJobCounts()` | Returns `{ waiting, active, delayed, completed, failed }` |
| `searchJobs(opts)` | Filter jobs by state, name, and/or data fields |
| `pause()` / `resume()` | Pause / resume the queue |
| `isPaused()` | Check pause state (synchronous, returns `boolean`) |
| `close()` | Close the queue |

### TestWorker

| Method / Event | Description |
|----------------|-------------|
| `on('completed', fn)` | Fired when a job finishes successfully |
| `on('failed', fn)` | Fired when a job throws |
| `close()` | Stop the worker |

---

## Searching Jobs

`queue.searchJobs()` lets you filter jobs by state, name, and/or data fields (shallow key-value match).

```typescript
// All completed jobs
const all = await queue.searchJobs({ state: 'completed' });

// Completed jobs named 'send-email'
const emails = await queue.searchJobs({ state: 'completed', name: 'send-email' });

// Failed jobs where data.userId === 42
const userFailed = await queue.searchJobs({
  state: 'failed',
  data: { userId: 42 },
});

// Search across all states (scans all job hashes)
const byName = await queue.searchJobs({ name: 'send-email' });

```

`searchJobs` is also available on the real `Queue` class (with an additional `limit` option, default 100).

---

## Retry Behaviour in Tests

Retries work the same as in production. Configure them via job options:

```typescript
const worker = new TestWorker(queue, async (job) => {
  if (job.attemptsMade < 2) throw new Error('transient');
  return { ok: true };
});

await queue.add('flaky', {}, { attempts: 3, backoff: { type: 'fixed', delay: 0 } });

const done = await queue.searchJobs({ state: 'completed', name: 'flaky' });
expect(done[0]?.attemptsMade).toBe(2);
```

---

## Tips

- **No connection config needed.** `TestQueue` takes only a name — no `connection` option.
- **Processing is synchronous-ish.** `TestWorker` processes jobs immediately when they are added via `queue.add()`. In most tests you can check state right after the `await queue.add(...)` call.
- **Delayed jobs are enqueued as waiting.** The `delay` option is accepted but not honoured in test mode — jobs start as `waiting` and are processed immediately.
- **Swap without changing processors.** Because `TestQueue` and `TestWorker` share the same interface as `Queue` and `Worker`, you can parameterise your processor code and pass either implementation.

```typescript
// Production
const queue  = new Queue('tasks', { connection });
const worker = new Worker('tasks', myProcessor, { connection });

// Tests
const queue  = new TestQueue('tasks');
const worker = new TestWorker(queue, myProcessor);
```
