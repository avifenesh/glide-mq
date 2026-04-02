# AI-Native Primitives Reference

glide-mq provides 7 AI-native primitives designed for LLM orchestration pipelines.

## 1. Usage Metadata (job.reportUsage)

Track model, tokens, cost, and latency per job.

```typescript
const worker = new Worker('inference', async (job) => {
  const response = await openai.chat.completions.create({ ... });

  await job.reportUsage({
    model: 'gpt-5.4',
    provider: 'openai',
    tokens: {
      input: response.usage.prompt_tokens,
      output: response.usage.completion_tokens,
    },
    // totalTokens auto-computed as sum of all token categories if omitted
    costs: { total: 0.0032 },
    costUnit: 'usd',
    latencyMs: 1200,
    cached: false,
  });

  return response.choices[0].message.content;
}, { connection });
```

### JobUsage Interface

```typescript
interface JobUsage {
  model?: string;           // e.g. 'gpt-5.4', 'claude-sonnet-4-20250514'
  provider?: string;        // e.g. 'openai', 'anthropic'
  tokens?: Record<string, number>;   // e.g. { input: 500, output: 200, reasoning: 100 }
  totalTokens?: number;     // auto-computed as sum of tokens values if omitted
  costs?: Record<string, number>;    // e.g. { total: 0.003 } or { input: 0.001, output: 0.002 }
  totalCost?: number;       // auto-computed as sum of costs values if omitted
  costUnit?: string;        // e.g. 'usd', 'credits', 'ils' (informational)
  latencyMs?: number;       // inference latency (not queue wait)
  cached?: boolean;         // cache hit flag
}
```

- Calling `reportUsage()` multiple times overwrites previous values on that job.
- Token counts must not be negative (throws).
- Emits a `'usage'` event on the events stream with the full usage object.
- Stored in the job hash as `usage:model`, `usage:tokens` (JSON), `usage:costs` (JSON), `usage:totalTokens`, `usage:totalCost`, `usage:costUnit`.
- Also updates rolling per-minute usage buckets used by `queue.getUsageSummary()`.

### Rolling Usage Summary (queue.getUsageSummary / Queue.getUsageSummary)

```typescript
const summary = await queue.getUsageSummary({
  queues: ['inference', 'embeddings'],
  windowMs: 3_600_000,   // last hour
});

// {
//   totalTokens,
//   totalCost,
//   jobCount,
//   models: Record<string, number>,
//   perQueue: Record<string, { totalTokens, totalCost, jobCount, models }>
// }
```

Use `Queue.getUsageSummary()` when you want the same rollup without an existing queue instance. The HTTP proxy exposes the same aggregation at `GET /usage/summary`.

## 2. Token Streaming (job.stream / job.streamChunk / queue.readStream)

Emit and consume LLM output tokens in real-time via per-job Valkey Streams.

### Producer Side (Worker)

```typescript
const worker = new Worker('chat', async (job) => {
  const stream = await openai.chat.completions.create({ stream: true, ... });

  for await (const chunk of stream) {
    const token = chunk.choices[0]?.delta?.content;
    if (token) {
      await job.stream({ token, index: String(chunk.choices[0].index) });
    }
  }

  return { done: true };
}, { connection });
```

`job.stream(chunk)` appends a flat `Record<string, string>` to a per-job Valkey Stream via XADD. Returns the stream entry ID.

### Convenience: job.streamChunk(type, content?)

Typed shorthand for streaming LLM chunks with a `type` field and optional `content`:

```typescript
await job.streamChunk('reasoning', 'Let me think about this...');
await job.streamChunk('content', 'The answer is 42.');
await job.streamChunk('done');
```

Equivalent to `job.stream({ type, content })` - useful for structured streaming with thinking models.

### Consumer Side (Queue)

```typescript
const entries = await queue.readStream(jobId);
// entries: { id: string; fields: Record<string, string> }[]

// Resume from last known position
const more = await queue.readStream(jobId, { lastId: entries.at(-1)?.id });

// Long-polling (blocks until new entries arrive)
const live = await queue.readStream(jobId, { lastId, block: 5000 });
```

### ReadStreamOptions

```typescript
interface ReadStreamOptions {
  lastId?: string;     // resume from this stream ID (exclusive)
  count?: number;      // max entries to return (default: 100)
  block?: number;      // XREAD BLOCK ms for long-polling (0 = non-blocking)
}
```

## 3. Suspend / Resume (Human-in-the-Loop)

Pause a job to wait for external approval, then resume with signals.

### Suspending (Worker Side)

```typescript
const worker = new Worker('content-review', async (job) => {
  // Check if this is a resume after suspension
  if (job.signals.length > 0) {
    const approval = job.signals.find(s => s.name === 'approve');
    if (approval) {
      return { published: true, approver: approval.data.approvedBy };
    }
    return { rejected: true };
  }

  // First run - generate content and suspend for review
  const content = await generateContent(job.data);
  await job.updateData({ ...job.data, generatedContent: content });

  await job.suspend({
    reason: 'Awaiting human review',
    timeout: 86_400_000,  // 24h timeout (0 = infinite, default)
  });
}, { connection });
```

`job.suspend()` throws `SuspendError` internally - no code after it executes. The job moves to `'suspended'` state.

If `timeout` is set, glide-mq stores the deadline on the suspended sorted set and any live `Queue` or `Worker` runtime can fail expired suspended jobs with `'Suspend timeout exceeded'`. This no longer depends on the original worker staying online, but it does require at least one glide-mq process to remain connected to the queue.

### Resuming (Queue Side)

```typescript
// Send a signal to resume the job
const resumed = await queue.signal(jobId, 'approve', { approvedBy: 'alice' });
// true if job was suspended and is now resumed, false otherwise

// Inspect suspension state
const info = await queue.getSuspendInfo(jobId);
// null if not suspended, otherwise:
// {
//   reason?: string,
//   suspendedAt: number (epoch ms),
//   timeout?: number (ms),
//   signals: SignalEntry[]
// }
```

### SignalEntry

```typescript
interface SignalEntry {
  name: string;        // signal name (e.g. 'approve', 'reject')
  data: any;           // arbitrary payload
  receivedAt: number;  // epoch ms
}
```

### SuspendOptions

```typescript
interface SuspendOptions {
  reason?: string;     // human-readable reason
  timeout?: number;    // ms, 0 = infinite (default)
}
```

## 4. Budget Middleware (Flow-Level Caps)

Cap total token usage and/or cost across all jobs in a flow. Supports per-category limits and weighted totals for thinking model budgets.

### Setting Budget on a Flow

```typescript
import { FlowProducer } from 'glide-mq';

const flow = new FlowProducer({ connection });
await flow.add(
  {
    name: 'research-report',
    queueName: 'ai',
    data: { topic: 'quantum computing' },
    children: [
      { name: 'search', queueName: 'ai', data: { query: 'latest papers' } },
      { name: 'summarize', queueName: 'ai', data: {} },
      { name: 'critique', queueName: 'ai', data: {} },
    ],
  },
  {
    budget: {
      maxTotalTokens: 50_000,
      maxTotalCost: 0.50,
      costUnit: 'usd',
      tokenWeights: { reasoning: 4, cachedInput: 0.25 },
      onExceeded: 'fail',    // 'fail' (default) or 'pause'
    },
  },
);
```

### BudgetOptions

```typescript
interface BudgetOptions {
  maxTotalTokens?: number;                // hard cap on weighted total tokens
  maxTokens?: Record<string, number>;     // per-category token caps (e.g. { input: 50000, reasoning: 5000 })
  tokenWeights?: Record<string, number>;  // weight multipliers for maxTotalTokens (unlisted = 1)
  maxTotalCost?: number;                  // hard cap on total cost
  maxCosts?: Record<string, number>;      // per-category cost caps
  costUnit?: string;                      // e.g. 'usd', 'credits', 'ils' (informational)
  onExceeded?: 'pause' | 'fail';         // default: 'fail'
}
```

### Reading Budget State

```typescript
const budget = await queue.getFlowBudget(parentJobId);
// null if no budget was set, otherwise:
// {
//   maxTotalTokens?: number,
//   maxTokens?: Record<string, number>,
//   tokenWeights?: Record<string, number>,
//   maxTotalCost?: number,
//   maxCosts?: Record<string, number>,
//   costUnit?: string,
//   usedTokens: number,
//   usedCost: number,
//   exceeded: boolean,
//   onExceeded: 'pause' | 'fail'
// }
```

Budget is enforced per flow by writing a `budgetKey` to every job hash in the tree.

## 5. Fallback Chains

Ordered list of model/provider alternatives tried on retryable failure.

### Setting Fallbacks

```typescript
await queue.add('inference', { prompt: 'Explain quantum entanglement' }, {
  attempts: 4,  // 1 original + 3 fallbacks
  fallbacks: [
    { model: 'gpt-5.4', provider: 'openai' },
    { model: 'claude-sonnet-4-20250514', provider: 'anthropic' },
    { model: 'llama-3-70b', provider: 'groq', metadata: { temperature: 0.7 } },
  ],
});
```

### Reading Fallback State (Worker Side)

```typescript
const worker = new Worker('inference', async (job) => {
  const fallback = job.currentFallback;
  // undefined on first attempt (original request)
  // { model: 'gpt-5.4', provider: 'openai' } on first fallback
  // { model: 'claude-sonnet-4-20250514', provider: 'anthropic' } on second, etc.

  const model = fallback?.model ?? job.data.defaultModel;
  const provider = fallback?.provider ?? job.data.defaultProvider;

  return await callLLM(provider, model, job.data.prompt);
}, { connection });
```

- `job.fallbackIndex` is 0 for the original request, 1+ for fallback entries.
- `job.currentFallback` returns `fallbacks[fallbackIndex - 1]` or `undefined` when index is 0.
- Each fallback entry has `model` (required), `provider` (optional), and `metadata` (optional).

## 6. Dual-Axis Rate Limiting (RPM + TPM)

Rate-limit workers by both requests-per-minute (RPM) and tokens-per-minute (TPM).

### Configuration

```typescript
const worker = new Worker('inference', processor, {
  connection,
  limiter: { max: 60, duration: 60_000 },          // RPM: 60 req/min
  tokenLimiter: {
    maxTokens: 100_000,
    duration: 60_000,
    scope: 'both',  // 'queue' | 'worker' | 'both' (default)
  },
});
```

### TokenLimiter Options

```typescript
interface TokenLimiter {
  maxTokens: number;        // max tokens per window
  duration: number;         // window duration in ms
  scope?: 'queue' | 'worker' | 'both';
  // 'queue': Valkey counter shared across all workers
  // 'worker': in-memory counter per worker instance
  // 'both': local check first, then Valkey (optimal, default)
}
```

### Reporting Tokens

```typescript
const worker = new Worker('inference', async (job) => {
  const result = await callLLM(job.data);

  // Option 1: report tokens directly for TPM tracking
  await job.reportTokens(result.totalTokens);

  // Option 2: reportUsage auto-extracts totalTokens for TPM
  await job.reportUsage({
    model: 'gpt-5.4',
    tokens: { input: result.promptTokens, output: result.completionTokens },
  });

  return result;
}, { connection, tokenLimiter: { maxTokens: 100_000, duration: 60_000 } });
```

Worker pauses fetching when either RPM or TPM limit is exceeded.

## 7. Flow Usage Aggregation (getFlowUsage)

Aggregate AI usage metadata across all jobs in a flow tree.

```typescript
const usage = await queue.getFlowUsage(parentJobId);
// {
//   tokens: Record<string, number>,    // aggregated per-category tokens (e.g. { input: 2500, output: 1200 })
//   totalTokens: number,               // sum of all token categories
//   costs: Record<string, number>,     // aggregated per-category costs
//   totalCost: number,                 // sum of all cost categories
//   costUnit?: string,                 // unit from the first job that reported one
//   jobCount: number,
//   models: Record<string, number>     // model name -> call count
// }
```

Walks the parent and all children via the deps set. Useful for cost reporting, billing, and observability dashboards.

## Gotchas

- `job.suspend()` and `job.moveToWaitingChildren()` both throw internally - no code after them executes.
- `job.reportUsage()` and `job.reportTokens()` reject negative values.
- `reportUsage()` overwrites previous usage data on the same job.
- `getUsageSummary()` reads rolling buckets, not job hashes, so it is cheap for queue-wide summaries but not a replacement for per-job detail.
- `reportTokens()` overwrites the previous value - it does not accumulate.
- Budget enforcement happens at the flow level, not per-job. Individual jobs report usage; the budget key tracks aggregates.
- Fallback chains require `attempts >= fallbacks.length + 1` (original + N fallbacks).
- `queue.signal()` returns false if the job is not in suspended state.
- `readStream()` with `block > 0` uses XREAD BLOCK (a blocking Valkey call) - do not use on a shared client that serves other queries.
