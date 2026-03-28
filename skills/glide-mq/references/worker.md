# Worker Reference

## Constructor

```typescript
import { Worker } from 'glide-mq';

const worker = new Worker(
  'tasks',              // queue name
  async (job) => {      // processor function
    // job.data, job.name, job.id, job.opts
    await job.log('step done');
    await job.updateProgress(50);       // 0-100 or object
    await job.updateData({ ...job.data, enriched: true });
    return { ok: true };                // becomes job.returnvalue
  },
  {
    connection: ConnectionOptions,       // required (even if commandClient provided)
    commandClient?: Client,              // shared client for non-blocking ops (alias: client)
    concurrency?: number,                // parallel jobs (default: 1)
    blockTimeout?: number,               // XREADGROUP BLOCK ms (default: 5000)
    stalledInterval?: number,            // stall check interval ms (default: 30000)
    lockDuration?: number,               // stall detection window per job ms (default: 30000)
    maxStalledCount?: number,            // max stall recoveries before fail
    limiter?: { max, duration },         // rate limit per worker
    deadLetterQueue?: { name: string },  // inherited from QueueOptions - usually set on Queue
    events?: boolean,                    // emit completed/failed events (default: true)
    metrics?: boolean,                   // record metrics (default: true)
    prefix?: string,
    serializer?: Serializer,
    tokenLimiter?: {
      maxTokens: number,          // max tokens per window
      duration: number,           // window duration in ms
      scope?: 'queue' | 'worker' | 'both',  // default: 'both'
    },
    backoffStrategies?: Record<string, (attemptsMade: number, err: Error) => number>,
  },
);
```

## Batch Processing

```typescript
import { Worker, BatchError } from 'glide-mq';

const worker = new Worker(
  'bulk-insert',
  async (jobs) => {              // receives Job[] in batch mode
    const results = await db.insertMany(jobs.map(j => j.data));
    return results;              // must return R[] with length === jobs.length
  },
  {
    connection,
    batch: {
      size: 50,          // max jobs per batch (1-1000)
      timeout: 1000,     // ms to wait for full batch (optional)
    },
  },
);

// Partial failures - report per-job outcomes
async (jobs) => {
  const results = await Promise.allSettled(jobs.map(processOne));
  const mapped = results.map(r => r.status === 'fulfilled' ? r.value : r.reason);
  if (mapped.some(r => r instanceof Error)) {
    throw new BatchError(mapped);  // each job individually completed/failed
  }
  return mapped;
};
```

## Worker Events

| Event | Arguments | Description |
|-------|-----------|-------------|
| `active` | `(job, jobId)` | Job started processing |
| `completed` | `(job, result)` | Job finished successfully |
| `failed` | `(job, err)` | Job threw or timed out |
| `error` | `(err)` | Internal worker error (connection issues) |
| `stalled` | `(jobId)` | Job exceeded lockDuration, re-queued |
| `drained` | `()` | Queue transitioned from non-empty to empty |
| `closing` | `()` | Worker beginning to close |
| `closed` | `()` | Worker fully closed |

```typescript
worker.on('completed', (job, result) => { ... });
worker.on('failed', (job, err) => { ... });
worker.on('error', (err) => { ... });
worker.on('stalled', (jobId) => { ... });
```

## Stall Detection

- Worker extends job lock every `lockRenewTime` (default: lockDuration/2).
- If lock expires (job exceeds `lockDuration` without renewal), job is stalled.
- Stalled jobs are re-queued up to `maxStalledCount` times, then failed.
- Check interval controlled by `stalledInterval`.

## LIFO Mode

Workers check sources in order: **priority > LIFO > FIFO**.
Add jobs with `{ lifo: true }` to process newest first.
LIFO uses a dedicated Valkey LIST separate from the FIFO stream.

## Job Revocation (AbortSignal)

```typescript
// Queue-side: revoke a job
const result = await queue.revoke(job.id);
// 'revoked'    - was waiting/delayed, now failed
// 'flagged'    - active, worker will abort cooperatively
// 'not_found'  - job does not exist

// Worker-side: check for revocation
const worker = new Worker('tasks', async (job) => {
  for (const chunk of dataset) {
    if (job.abortSignal?.aborted) throw new Error('Revoked');
    await processChunk(chunk);
  }
}, { connection });
```

`job.abortSignal` is a standard `AbortSignal` - pass to `fetch`, `axios`, etc.

## Pause / Resume / Close

```typescript
await worker.pause();        // stop accepting new jobs (active finish)
await worker.pause(true);    // force-stop immediately
await worker.resume();

await worker.close();        // graceful: waits for active jobs
await worker.close(true);    // force-close immediately
```

## AI Usage & Token Tracking

```typescript
const worker = new Worker('inference', async (job) => {
  const result = await callLLM(job.data.prompt);

  // Report AI usage metadata (persisted to job hash, emits 'usage' event)
  await job.reportUsage({
    model: 'gpt-5.4',
    provider: 'openai',
    tokens: { input: result.promptTokens, output: result.completionTokens },
    costs: { total: 0.003 },
    costUnit: 'usd',
    latencyMs: 800,
  });

  // Or report just tokens for TPM rate limiting
  await job.reportTokens(result.totalTokens);

  return result.content;
}, {
  connection,
  limiter: { max: 60, duration: 60_000 },        // RPM limit
  tokenLimiter: { maxTokens: 100_000, duration: 60_000 },  // TPM limit
});
```

Worker pauses fetching when either RPM limiter or TPM tokenLimiter is exceeded.

## Token Streaming

```typescript
const worker = new Worker('chat', async (job) => {
  const stream = await openai.chat.completions.create({ stream: true, ... });
  for await (const chunk of stream) {
    const token = chunk.choices[0]?.delta?.content;
    if (token) {
      await job.stream({ token });  // XADD to per-job stream
    }
  }
  return { done: true };
}, { connection });
```

Consumers read via `queue.readStream(jobId, opts)`.

## Suspend / Resume (Human-in-the-Loop)

```typescript
const worker = new Worker('review', async (job) => {
  // On resume, signals are populated
  if (job.signals.length > 0) {
    const approval = job.signals.find(s => s.name === 'approve');
    if (approval) return { approved: true };
    return { rejected: true };
  }

  // First run - suspend for human review
  await job.suspend({ reason: 'Needs approval', timeout: 86_400_000 });
  // throws SuspendError - no code after this executes
}, { connection });
```

Resume externally via `queue.signal(jobId, 'approve', { ... })`.

## Fallback Chains

```typescript
const worker = new Worker('inference', async (job) => {
  const fallback = job.currentFallback;
  // undefined on first attempt, then fallbacks[0], fallbacks[1], etc.
  const model = fallback?.model ?? 'gpt-5.4-nano';
  return await callLLM(model, job.data.prompt);
}, { connection });
```

Set via `queue.add('inference', data, { fallbacks: [...], attempts: 4 })`.

## Skipping Retries

```typescript
import { UnrecoverableError } from 'glide-mq';

// Option 1: UnrecoverableError - skips all remaining retries
throw new UnrecoverableError('bad input');

// Option 2: job.discard() + throw - same effect
job.discard();
throw new Error('discarded');
```

## Step Jobs (moveToDelayed)

```typescript
const worker = new Worker('drip', async (job) => {
  switch (job.data.step) {
    case 'send':
      await sendEmail(job.data);
      return job.moveToDelayed(Date.now() + 86400_000, 'check');
    case 'check':
      return 'done';
  }
}, { connection });
```

`moveToDelayed(timestampMs, nextStep?)` - pauses job until timestamp, optionally updates `job.data.step`.

## Graceful Shutdown

```typescript
import { gracefulShutdown } from 'glide-mq';
// Returns a handle that auto-registers SIGTERM/SIGINT handlers.
// await blocks until a signal fires. For manual shutdown: handle.shutdown()
const handle = gracefulShutdown([queue, worker, events]);
await handle.shutdown(); // programmatic trigger
```

## Gotchas

- Worker **always requires `connection`** even with `commandClient` - blocking client is auto-created.
- `commandClient` and `client` are aliases - provide one, not both.
- Don't close shared client while worker is alive. Close worker first.
- Batch processor must return array with length === jobs.length.
- `moveToDelayed()` must be called from active processor. Throws `DelayedError` internally.
- `job.suspend()` throws `SuspendError` internally - no code after it executes.
- `job.reportUsage()` and `job.reportTokens()` reject negative values.
- `reportTokens()` overwrites previous value (does not accumulate).
- `tokenLimiter` scope `'both'` checks local counter first, then Valkey (optimal for most setups).
- Fallback chains require `attempts >= fallbacks.length + 1`.
