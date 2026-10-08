# Testing

glide-mq ships a built-in in-memory backend so you can unit-test job processors **without a running Valkey instance**.

## Table of Contents

- [TestQueue and TestWorker](#testqueue-and-testworker)
- [API Surface](#api-surface)
- [Searching Jobs](#searching-jobs)
- [Retry Behaviour in Tests](#retry-behaviour-in-tests)
- [Dead-Letter Queues in Tests](#dead-letter-queues-in-tests)
- [Delayed, Prioritized and Rate-Limited Jobs](#delayed-prioritized-and-rate-limited-jobs)
- [Custom Job IDs in Tests](#custom-job-ids-in-tests)
- [Batch Testing](#batch-testing)
- [Deduplication Testing](#deduplication-testing)
- [Step Jobs in Tests](#step-jobs-in-tests)
- [AI Primitives in Tests](#ai-primitives-in-tests)
- [Tips](#tips)
- [Known Limitations](#known-limitations)

---

## TestQueue and TestWorker

Import from `glide-mq/testing`:

```typescript
import { TestQueue, TestWorker } from 'glide-mq/testing';

const queue = new TestQueue('tasks');
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
const batchWorker = new TestWorker(
  queue,
  async (jobs) => {
    return jobs.map((j) => ({ processed: j.data }));
  },
  { batch: { size: 10 } },
);
```

### Using with a test framework (Vitest / Jest)

```typescript
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { TestQueue, TestWorker } from 'glide-mq/testing';

describe('email processor', () => {
  let queue: TestQueue;
  let worker: TestWorker;

  beforeEach(() => {
    queue = new TestQueue('email');
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

| Method                                  | Description                                                                                                                                                                                                              |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `add(name, data, opts?)`                | Enqueue a job; `delay` parks it in `delayed`, `priority` parks it in `prioritized`, otherwise a worker picks it up immediately                                                                                           |
| `addBulk(jobs)`                         | Enqueue multiple jobs                                                                                                                                                                                                    |
| `addAndWait(name, data, opts?)`         | Add a job and resolve with its return value, or reject with the failed reason; `waitTimeout` (default 30 s) bounds the wait                                                                                              |
| `waitForJobs(jobs, opts?)`              | Resolve once every job from `addBulk()` / `add()` has settled (`null` entries ignored, a removed record counts as settled); rejects with the first terminal failure or on `timeout` (default 30 s); workers keep running |
| `getJob(id)`                            | Retrieve a job by ID                                                                                                                                                                                                     |
| `getJobs(state, start?, end?)`          | List jobs by state. `waiting` follows the worker dispatch order, `delayed` follows the scheduled order and includes prioritized jobs                                                                                     |
| `getJobCounts()`                        | Returns `{ waiting, active, delayed, completed, failed }`; `delayed` counts prioritized jobs too, like production                                                                                                        |
| `getJobCountByTypes()` / `count()`      | Alias for `getJobCounts()`; `count()` is the FIFO stream length (waiting and active FIFO jobs)                                                                                                                           |
| `searchJobs(opts)`                      | Filter jobs by state, name, and/or data fields                                                                                                                                                                           |
| `getJobLogs(id, start?, end?)`          | Read the lines a processor appended with `job.log()`                                                                                                                                                                     |
| `getSuspendedJobs(start?, end?, opts?)` | List suspended jobs ordered by their timeout deadline                                                                                                                                                                    |
| `revoke(jobId)`                         | Fail a waiting / delayed / prioritized job with reason `revoked`, flag any other existing job; returns the same strings as `Queue`                                                                                       |
| `getDeadLetterJobs(...)`                | List this queue's dead-letter jobs; `getDeadLetterJob`, `removeDeadLetterJob` and `replayDeadLetterJob` work on one entry                                                                                                |
| `retryJobs(opts?)`                      | Move failed jobs back to waiting                                                                                                                                                                                         |
| `drain(delayed?)`                       | Remove waiting jobs; pass `true` to also remove delayed and prioritized jobs                                                                                                                                             |
| `obliterate(opts?)`                     | Wipe jobs, schedulers, dedup entries, budgets and metrics; refuses while jobs are active unless `{ force: true }`                                                                                                        |
| `pause()` / `resume()`                  | Pause / resume the queue                                                                                                                                                                                                 |
| `isPaused()`                            | Check pause state (synchronous, returns `boolean` - note: real `Queue.isPaused()` is async)                                                                                                                              |
| `close()`                               | Close the queue, clear every timer and reject pending `addAndWait` and `waitForJobs` calls                                                                                                                               |

### TestJob

| Method                                   | Description                                                                                                                                                       |
| ---------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `changePriority(newPriority)`            | Same rules as `glidemq_changePriority`: waiting to prioritized, prioritized to waiting on 0, delayed keeps its place; throws `invalid_priority` / `invalid_state` |
| `changeDelay(newDelay)`                  | Same rules as `glidemq_changeDelay`: reschedule a delayed job (0 releases it, or parks it as prioritized when it has a priority), park a waiting job; else throws |
| `promote()`                              | Move a delayed job to waiting immediately; throws `Cannot promote: not_delayed` otherwise, like `Job.promote()`                                                   |
| `getState()`                             | Current state, `'unknown'` once removed                                                                                                                           |
| `isCompleted()` ... `isWaiting()`        | State helpers: `isCompleted`, `isFailed`, `isDelayed`, `isActive`, `isWaiting`, `isRevoked`                                                                       |
| `waitUntilFinished(pollMs?, timeoutMs?)` | Poll until `completed` or `failed`, reject on timeout                                                                                                             |
| `retry()`                                | Move a failed job back to waiting (attempts reset, TTL re-armed); throws `Cannot retry: not_failed`                                                               |
| `remove()`                               | Remove the job; the queue emits `removed`                                                                                                                         |
| `moveToFailed(err)`                      | From inside the processor: fail the active job instead of completing it, then the attempts / backoff rules apply                                                  |
| `log(message)`                           | Append a log line readable through `queue.getJobLogs()`                                                                                                           |
| `updateData(data)` / `updateProgress(p)` | Persist to the stored job                                                                                                                                         |

### TestWorker

| Method / Event               | Description                                                                                                      |
| ---------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `on('active', fn)`           | Fired when a job starts processing, args: `(job, jobId)`                                                         |
| `on('completed', fn)`        | Fired when a job finishes successfully                                                                           |
| `on('failed', fn)`           | Fired on every failed attempt, including retried ones (not on a `RateLimitError`)                                |
| `on('drained', fn)`          | Fired when the queue transitions from non-empty to empty                                                         |
| `pause(force?)` / `resume()` | Stop / restart taking jobs; without `force`, `pause()` resolves once the active jobs finish                      |
| `isPaused()` / `isRunning()` | Worker pause state                                                                                               |
| `rateLimit(ms)`              | Pause dispatch for `ms`, like `Worker.rateLimit()`                                                               |
| `drain()`                    | Process everything waiting, prioritized or delayed, then close                                                   |
| `waitUntilReady()`           | Resolves immediately (no connection to wait for)                                                                 |
| `close()`                    | Stop the worker                                                                                                  |
| `TestWorker.RateLimitError`  | Throw from a processor to requeue the job after the limiter window; `TestWorker.isRateLimitError(err)` checks it |

Options: `concurrency`, `batch`, `limiter` (`{ max, duration }`, same semantics as `WorkerOptions.limiter`), `tokenLimiter`, `backoffStrategies`, `deadLetterQueue`.

The queue also emits `added`, `removed`, `promoted`, `delay-changed` (also when a job is parked by `job.moveToDelayed()` or a budget `pause`, args `(jobId, delayMs)`), `priority-changed`, `revoked`, `retrying`, `completed`, `failed`, `suspended`, `resumed` and `drained`, mirroring the production event stream.

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

Retries follow the production state machine. Every failed attempt sets `job.failedReason` and fires the worker `failed` event. A retryable failure parks the job in `delayed` for the backoff delay (`fixed`, `exponential`, `jitter`, or a custom type from the `backoffStrategies` worker option) and the queue emits `retrying`; the job then returns to `waiting`. Only the terminal failure moves the job to `failed` and fires the queue `failed` event. Backoff uses real timers, so keep delays small in unit tests.

```typescript
const worker = new TestWorker(queue, async (job) => {
  if (job.attemptsMade < 2) throw new Error('transient');
  return { ok: true };
});

await queue.add('flaky', {}, { attempts: 3, backoff: { type: 'fixed', delay: 0 } });

await new Promise((r) => worker.once('completed', r));
const done = await queue.searchJobs({ state: 'completed', name: 'flaky' });
expect(done[0]?.attemptsMade).toBe(2);
```

---

## Dead-Letter Queues in Tests

Pass the `deadLetterQueue` option to a `TestWorker` and every job that fails terminally is copied into the `TestQueue` registered under that name, like the `Worker` option of the same name. The copy is a plain job with the failed job's name, added before the `failed` events fire. Its data is the envelope `{ originalQueue, originalJobId, data, failedReason, attemptsMade }`, where `data` goes through a JSON round trip and `attemptsMade` is the count from before the failing attempt (0 for a job with `attempts: 1`), as the production worker writes it. Retried attempts, completed jobs and jobs that fail without a worker running them (`revoke()`, an expired `ttl`, a suspend timeout) get no copy, as in production.

The queue reads its entries back with `getDeadLetterJobs(start?, end?, opts?)`, `getDeadLetterJob(id, opts?)`, `removeDeadLetterJob(id)` and `replayDeadLetterJob(id)`. They only see entries whose `originalQueue` is this queue, so several queues can share one dead-letter queue. `getDeadLetterJobs()` lists the entries that are still `waiting` or `active`; `getDeadLetterJob()` finds one in any state. `replayDeadLetterJob()` re-adds the original job with its data and options (minus `jobId`, `delay`, `deduplication` and `parent`) when it still exists, otherwise the envelope data with default options, then removes the entry.

The dead-letter queue is looked up by name among the open `TestQueue` instances. Create it before the failure when you want to inspect it or consume it with another `TestWorker`; when none is open, the worker creates one on first use and you can read it through `queue.getDeadLetterJobs()`. The name comes from the `deadLetterQueue` option of the `TestQueue`, or else from the last `TestWorker` created with the option. Closing the dead-letter `TestQueue` discards its jobs, and a second `TestQueue` with the same name replaces the first in the lookup.

```typescript
const queue = new TestQueue('emails');
const dlq = new TestQueue('emails-dlq');
const worker = new TestWorker(
  queue,
  async () => {
    throw new Error('smtp down');
  },
  { deadLetterQueue: { name: 'emails-dlq' } },
);

const job = await queue.add('send', { to: 'a@example.com' }, { attempts: 2, backoff: { type: 'fixed', delay: 0 } });
await job!.waitUntilFinished(10, 2000);

const [dead] = await queue.getDeadLetterJobs();
expect(dead.data).toMatchObject({ originalQueue: 'emails', originalJobId: job!.id, failedReason: 'smtp down' });
expect((await dlq.getJobCounts()).waiting).toBe(1);

await queue.replayDeadLetterJob(dead.id); // re-adds the original job and drops the entry
```

---

## Delayed, Prioritized and Rate-Limited Jobs

Delayed and priority jobs follow the `glidemq_addJob` / `glidemq_promote` state machine:

- `delay` parks the job in `delayed` until the timestamp passes, then it moves to `waiting` and the queue emits `promoted`. `job.promote()` releases it early, `job.changeDelay(ms)` reschedules it.
- `priority > 0` without a delay parks the job in `prioritized`. It is counted under `delayed` by `getJobCounts()` and listed by `getJobs('delayed')`, exactly like the scheduled ZSet in production. An attached worker promotes it to `waiting` on its next pass (also while the queue is paused) and dispatches it before LIFO and FIFO jobs.
- `debounce` deduplication replaces a tracked job that is still `delayed` or `prioritized`.

Timers are real, so keep delays small in unit tests.

```typescript
const queue = new TestQueue('later');
const job = await queue.add('reminder', {}, { delay: 50 });
expect(await job!.getState()).toBe('delayed');
await job!.promote();
expect(await job!.getState()).toBe('waiting');
```

A processor that throws `TestWorker.RateLimitError` (or any error named `RateLimitError`, so the production `Worker.RateLimitError` works too) does not fail the job. As in production, the job is parked in `delayed` for `err.delayMs`, the worker `limiter.duration`, or 1000 ms, with `failedReason` `'rate limited'`; `attemptsMade` is unchanged, the queue emits `retrying`, no `failed` event fires, and the worker pauses dispatch for the same window.

```typescript
const worker = new TestWorker(
  queue,
  async (job) => {
    if (await upstreamIsThrottled()) throw new TestWorker.RateLimitError();
    return callUpstream(job.data);
  },
  { limiter: { max: 10, duration: 1000 } },
);
```

---

## Custom Job IDs in Tests

`TestQueue.add()` honours the `jobId` option and enforces uniqueness, just like the real `Queue`. If you add a job with a `jobId` that already exists, the call returns `null` instead of creating a duplicate:

```typescript
const first = await queue.add('task', { v: 1 }, { jobId: 'unique-1' });
const second = await queue.add('task', { v: 2 }, { jobId: 'unique-1' });

expect(first).not.toBeNull();
expect(second).toBeNull(); // duplicate — same behaviour as production
```

This makes it straightforward to test idempotent-add patterns without a running Valkey instance.

---

## Batch Testing

`TestWorker` supports the `batch` option with `size` and optional `timeout`, matching the real `Worker` interface. When batch mode is enabled, the processor receives an array of jobs:

```typescript
const worker = new TestWorker(
  queue,
  async (jobs) => {
    return jobs.map((j) => ({ doubled: j.data.n * 2 }));
  },
  { batch: { size: 5, timeout: 100 } },
);

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

const worker = new TestWorker(
  queue,
  async (jobs) => {
    // One entry per job: an Error fails that job, any other value completes it
    const results = jobs.map((job) => (job.data.bad ? new Error('bad input') : { ok: true }));

    if (results.some((r) => r instanceof Error)) {
      throw new BatchError(results);
    }
    return results;
  },
  { batch: { size: 10 } },
);

await queue.add('item', { bad: false });
await queue.add('item', { bad: true });

const failed = await queue.getJobs('failed');
expect(failed).toHaveLength(1);
expect(failed[0]?.failedReason).toMatch('bad input');
```

---

## Deduplication Testing

`TestQueue` applies `deduplication` exactly like `Queue.add()`, with no extra flag. It mirrors the `glidemq_dedup` server function:

- `simple` (default): skipped while the job that claimed the id still exists and is not `completed` or `failed`. Once it finishes, or is removed (for example by `removeOnComplete`), the id is free again.
- `throttle`: skipped while less than `ttl` ms have passed since the id was claimed, whatever the job state. Without `ttl` nothing is throttled.
- `debounce`: if the tracked job is `delayed`, it is removed (the queue emits `removed`) and the new job is added. Skipped while the tracked job is `waiting` or `active`.

Skipped adds return `null`.

```typescript
const queue = new TestQueue('tasks');

// Simple mode: second add with the same dedup id is rejected while the first is pending
const a = await queue.add('task', { v: 1 }, { deduplication: { id: 'dedup-1', mode: 'simple' } });
const b = await queue.add('task', { v: 2 }, { deduplication: { id: 'dedup-1', mode: 'simple' } });
expect(a).not.toBeNull();
expect(b).toBeNull();

// Throttle mode: the same id is accepted again after the ttl window
const c = await queue.add('task', { v: 3 }, { deduplication: { id: 'dedup-2', mode: 'throttle', ttl: 50 } });
expect(c).not.toBeNull();
await new Promise((r) => setTimeout(r, 60));
const d = await queue.add('task', { v: 4 }, { deduplication: { id: 'dedup-2', mode: 'throttle', ttl: 50 } });
expect(d).not.toBeNull();
```

Pass `new TestQueue(name, { dedup: false })` to ignore `deduplication` options (the pre-parity default). `{ dedup: true }` is still accepted and changes nothing.

---

## Step Jobs in Tests

`job.moveToDelayed(timestamp, nextStep?)` works in test mode like in production: the job moves to `delayed` without counting an attempt, `nextStep` is written to `job.data.step`, and the job returns to `waiting` once the timestamp passes. It validates the same way and throws outside an active processor.

```typescript
import { TestQueue, TestWorker } from 'glide-mq/testing';

const queue = new TestQueue('steps');
const worker = new TestWorker(queue, async (job) => {
  const step = job.data.step ?? 'start';
  if (step === 'start') {
    await job.moveToDelayed(Date.now() + 100, 'finish');
  }
  return { done: true };
});

const job = await queue.add('flow', {});
// state is 'delayed' until the timestamp, then the processor runs again with step 'finish'
```

---

## AI Primitives in Tests

All AI-native primitives have full testing mode parity - no Valkey needed.

### TestJob methods

| Method                          | Description                                                                                     |
| ------------------------------- | ----------------------------------------------------------------------------------------------- |
| `reportUsage(usage)`            | Store AI usage metadata (model, tokens, cost, latency). Validates non-negative token counts.    |
| `stream(chunk)`                 | Append a chunk to the in-memory streaming channel. Returns a synthetic stream entry ID.         |
| `streamChunk(type, content?)`   | Convenience wrapper over `stream()` - emits `{ type, content }` fields for typed LLM chunks.    |
| `storeVector(field, embedding)` | Store a vector embedding for later similarity search. Accepts number[] or Float32Array.         |
| `suspend(opts?)`                | Move the job to suspended state. Throws SuspendError to halt the processor.                     |
| `moveToDelayed(ts, nextStep?)`  | Park the active job in delayed until `ts`, optionally setting `data.step`. Throws DelayedError. |

### TestQueue methods

| Method                             | Description                                                              |
| ---------------------------------- | ------------------------------------------------------------------------ |
| `readStream(jobId, opts?)`         | Read chunks from a streaming channel. Supports lastId, count, and block. |
| `signal(jobId, name, data?)`       | Send a signal to a suspended job. Returns true if the job was suspended. |
| `getSuspendInfo(jobId)`            | Get suspension state or null.                                            |
| `getFlowUsage(parentJobId)`        | Aggregate usage across parent and children.                              |
| `getFlowBudget(flowId)`            | Get budget state for a flow or null.                                     |
| `updateFlowBudget(flowId, limits)` | Change budget limits and re-evaluate exceeded.                           |
| `createJobIndex(opts?)`            | Store index configuration in memory.                                     |
| `vectorSearch(embedding, opts?)`   | Run cosine-similarity KNN search over stored vectors.                    |

### Example: testing an AI workflow

The TestJob and TestQueue classes mirror the real API:

```ts
const queue = new TestQueue('test');
const worker = new TestWorker(queue, async (job) => {
  await job.reportUsage({ model: 'gpt-5.4', tokens: { input: 100, output: 50 } });
  await job.stream({ type: 'token', content: 'hello' });
  return 'done';
});
const job = await queue.add('ai-task', { prompt: 'test' });
// After processing: job.usage.model === 'gpt-5.4'
// queue.readStream(job.id) returns streamed chunks
```

Call stream(), streamChunk(), reportUsage(), storeVector() inside the processor, then verify with readStream(), getFlowUsage(), and vectorSearch() on the queue.

### Example: testing suspend/resume

Call job.suspend() inside the processor, then queue.signal() from outside. Use getSuspendInfo() to verify state between the two calls.

---

## Tips

- **No connection config needed.** `TestQueue` takes only a name — no `connection` option.
- **Options are validated like production.** `TestQueue.add()` runs the same checks as `Queue.add()` (priority <= 2048, payload size, `ttl`, `lockDuration`, `cost`, `jobId`, ordering key, `lifo` with ordering) and throws the same errors. `job.updateData()` and `job.updateProgress()` persist to the stored job, so `queue.getJob()` sees the new values.
- **Processing is synchronous-ish.** `TestWorker` processes jobs immediately when they are added via `queue.add()`. In most tests you can check state right after the `await queue.add(...)` call.
- **Dispatch order matches the worker.** Jobs with `priority > 0` run first (lower number = higher priority, FIFO within a priority), then `lifo` jobs (newest first), then plain FIFO jobs. A `lifo` job with a priority is dispatched as LIFO, like production. `queue.getJobs('waiting')` lists jobs in that same order.
- **Retention is applied.** `removeOnComplete` / `removeOnFail` accept `true`, a count, or `{ age, count }` (age in seconds) and trim the completed / failed jobs exactly as the server functions do, before the `completed` / `failed` event fires. Jobs failed as `expired` by `ttl` are not removed, same as production.
- **Schedulers match the tick.** `repeatAfterComplete` waits for the produced job to complete or fail terminally before scheduling the next run. A scheduler run never applies `deduplication`, `delay` or `jobId` from a stored template, like `Scheduler.runSchedulers`.
- **Budget pauses park the job.** With `onExceeded: 'pause'` a job whose flow budget is exhausted moves to `delayed` for 24 hours, like `moveActiveToDelayed` in production.
- **Swap without changing processors.** Because `TestQueue` and `TestWorker` share the same interface as `Queue` and `Worker`, you can parameterise your processor code and pass either implementation.

```typescript
// Production
const queue = new Queue('tasks', { connection });
const worker = new Worker('tasks', myProcessor, { connection });

// Tests
const queue = new TestQueue('tasks');
const worker = new TestWorker(queue, myProcessor);
```

---

## Known Limitations

Behaviour that testing mode does not mirror. Everything else in this document follows the production state machine.

- **Ordering keys and concurrency groups are not enforced.** `ordering` options are validated and stored, but jobs sharing a key run concurrently and in dispatch order. `job.rateLimitGroup()` and `queue.rateLimitGroup()` do not exist on the test classes.
- **No global concurrency or queue-wide rate limit.** `setGlobalConcurrency`, `setGlobalRateLimit`, `removeGlobalRateLimit` and `getGlobalRateLimit` are not available; use the `TestWorker` `concurrency` and `limiter` options instead.
- **No flows or DAGs.** There is no `FlowProducer` counterpart; `job.getChildrenValues()`, `job.getParents()` and `job.moveToWaitingChildren()` are not available. `getFlowUsage()` and flow budgets work through `opts.parent.id` and `setBudget()`.
- **No abort support.** `worker.abortJob()` and `job.abortSignal` are not available; `close()` waits for nothing and lets running processors finish on their own.
- **Sandbox processors are CJS only.** A file path processor must be a `.js` (CommonJS) module; `.mjs` throws.
- **`isPaused()` is synchronous** on `TestQueue`; the real `Queue.isPaused()` returns a promise. `await` works on both.
- **Prioritized promotion is worker driven.** A priority job stays `prioritized` until a worker's next pass (a microtask), where production promotes on the scheduler tick. Without a worker it stays `prioritized`, as in production.
- **Invalid legacy scheduler templates delete the scheduler** instead of skipping the run and reporting an error; templates are validated at `upsertJobScheduler()`, so this only affects hand-seeded entries.
- **No connection.** `getClient()` does not exist and `TestQueue` takes no `connection` option.
