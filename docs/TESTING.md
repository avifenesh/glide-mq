# Testing

glide-mq ships a built-in in-memory backend so you can unit-test job processors **without a running Valkey instance**.

## Table of Contents

- [TestQueue and TestWorker](#testqueue-and-testworker)
- [API Surface](#api-surface)
- [Searching Jobs](#searching-jobs)
- [Retry Behaviour in Tests](#retry-behaviour-in-tests)
- [Custom Job IDs in Tests](#custom-job-ids-in-tests)
- [Batch Testing](#batch-testing)
- [Deduplication Testing](#deduplication-testing)
- [Step Jobs in Tests](#step-jobs-in-tests)
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

Batch processing is also supported in test mode:

```typescript
const batchWorker = new TestWorker(queue, async (jobs) => {
  return jobs.map(j => ({ processed: j.data }));
}, { batch: { size: 10 } });
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
| `drain(delayed?)` | Remove waiting jobs; pass `true` to also remove delayed jobs |
| `pause()` / `resume()` | Pause / resume the queue |
| `isPaused()` | Check pause state (synchronous, returns `boolean` - note: real `Queue.isPaused()` is async) |
| `close()` | Close the queue |

### TestJob

| Method | Description |
|--------|-------------|
| `changePriority(newPriority)` | Re-prioritize a job in the in-memory queue; mirrors `Job.changePriority()` |
| `changeDelay(newDelay)` | Change the delay of a job in the in-memory queue; mirrors `Job.changeDelay()` |
| `promote()` | Move delayed job to waiting immediately; mirrors `Job.promote()` |

### TestWorker

| Method / Event | Description |
|----------------|-------------|
| `on('active', fn)` | Fired when a job starts processing — args: `(job, jobId)` |
| `on('completed', fn)` | Fired when a job finishes successfully |
| `on('failed', fn)` | Fired when a job throws |
| `on('drained', fn)` | Fired when the queue transitions from non-empty to empty |
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

## Custom Job IDs in Tests

`TestQueue.add()` honours the `jobId` option and enforces uniqueness, just like the real `Queue`. If you add a job with a `jobId` that already exists, the call returns `null` instead of creating a duplicate:

```typescript
const first  = await queue.add('task', { v: 1 }, { jobId: 'unique-1' });
const second = await queue.add('task', { v: 2 }, { jobId: 'unique-1' });

expect(first).not.toBeNull();
expect(second).toBeNull(); // duplicate — same behaviour as production
```

This makes it straightforward to test idempotent-add patterns without a running Valkey instance.

---

## Batch Testing

`TestWorker` supports the `batch` option with `size` and optional `timeout`, matching the real `Worker` interface. When batch mode is enabled, the processor receives an array of jobs:

```typescript
const worker = new TestWorker(queue, async (jobs) => {
  return jobs.map(j => ({ doubled: j.data.n * 2 }));
}, { batch: { size: 5, timeout: 100 } });

await queue.addBulk([
  { name: 'calc', data: { n: 1 } },
  { name: 'calc', data: { n: 2 } },
  { name: 'calc', data: { n: 3 } },
]);

const completed = await queue.getJobs('completed');
expect(completed).toHaveLength(3);
```

To test `BatchError` handling (partial failures), throw a `BatchError` from the processor with a map of failed indices:

```typescript
import { BatchError } from 'glide-mq';

const worker = new TestWorker(queue, async (jobs) => {
  const results = [];
  const failedIndexes = new Map<number, Error>();

  for (let i = 0; i < jobs.length; i++) {
    if (jobs[i].data.bad) {
      failedIndexes.set(i, new Error('bad input'));
    } else {
      results[i] = { ok: true };
    }
  }

  if (failedIndexes.size > 0) {
    throw new BatchError(results, failedIndexes);
  }
  return results;
}, { batch: { size: 10 } });

await queue.add('item', { bad: false });
await queue.add('item', { bad: true });

const failed = await queue.getJobs('failed');
expect(failed).toHaveLength(1);
expect(failed[0]?.failedReason).toMatch('bad input');
```

---

## Deduplication Testing

`TestQueue` honours all three deduplication modes — `simple`, `throttle`, and `debounce` — so you can verify dedup logic without Valkey:

```typescript
// Simple mode: second add with the same dedup id is rejected
const a = await queue.add('task', { v: 1 }, {
  deduplication: { id: 'dedup-1', mode: 'simple' },
});
const b = await queue.add('task', { v: 2 }, {
  deduplication: { id: 'dedup-1', mode: 'simple' },
});

expect(a).not.toBeNull();
expect(b).toBeNull(); // deduplicated

// Throttle mode with TTL: after the TTL window expires the same id is accepted again
const c = await queue.add('task', { v: 3 }, {
  deduplication: { id: 'dedup-2', mode: 'throttle', ttl: 50 },
});
expect(c).not.toBeNull();

// Wait for TTL to expire
await new Promise(r => setTimeout(r, 60));

const d = await queue.add('task', { v: 4 }, {
  deduplication: { id: 'dedup-2', mode: 'throttle', ttl: 50 },
});
expect(d).not.toBeNull(); // accepted — window expired
```

---

## Step Jobs in Tests

`moveToDelayed` is **not supported** in test mode. Because delayed jobs become waiting immediately in `TestQueue`, calling `job.moveToDelayed()` inside a processor will not pause the job on a future timestamp the way it does in production.

If your processor relies on `moveToDelayed` for step-job orchestration, use integration tests with a real Valkey instance instead:

```typescript
// Integration test (requires Valkey)
import { Queue, Worker, DelayedError } from 'glide-mq';

const queue  = new Queue('steps', { connection });
const worker = new Worker('steps', async (job) => {
  const step = job.data.step ?? 'start';
  if (step === 'start') {
    await job.updateData({ ...job.data, step: 'finish' });
    await job.moveToDelayed(Date.now() + 1000, 'finish');
  }
  return { done: true };
}, { connection });
```

For unit-testing the logic *around* steps (data transformations, branching decisions), you can still use `TestQueue` and `TestWorker` — just skip the `moveToDelayed` call in test mode or guard it behind an environment check.

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
