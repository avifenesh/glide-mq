# Workflows Reference

## FlowProducer

Atomically enqueues a tree of parent-child jobs. Parent only runs after **all** children complete.

```typescript
import { FlowProducer } from 'glide-mq';

const flow = new FlowProducer({ connection });
// Also accepts: { client } for shared client

const { job: parent } = await flow.add({
  name: 'aggregate',
  queueName: 'reports',
  data: { month: '2025-01' },
  children: [
    { name: 'fetch-sales', queueName: 'data', data: { region: 'eu' } },
    { name: 'fetch-returns', queueName: 'data', data: {} },
    {
      name: 'fetch-inventory', queueName: 'data', data: {},
      children: [  // nested children supported
        { name: 'load-a', queueName: 'data', data: {} },
      ],
    },
  ],
});

await flow.close();
```

### FlowJob Structure

```typescript
interface FlowJob {
  name: string;
  queueName: string;
  data: any;
  opts?: JobOptions;
  children?: FlowJob[];
}
```

### Bulk Flows

```typescript
const nodes = await flow.addBulk([
  { name: 'report-jan', queueName: 'reports', data: {}, children: [...] },
  { name: 'report-feb', queueName: 'reports', data: {}, children: [...] },
]);
```

### Reading Child Results

```typescript
const worker = new Worker('reports', async (job) => {
  const childValues = await job.getChildrenValues();
  // Keys are opaque internal IDs - use Object.values()
  const results = Object.values(childValues);
  return { total: results.reduce((s, v) => s + v.count, 0) };
}, { connection });
```

## DAG Workflows (Multiple Parents)

`addDAG()` supports arbitrary DAG topologies where a job can depend on multiple parents.

```typescript
import { FlowProducer, dag } from 'glide-mq';

// Helper function (simpler API)
const jobs = await dag([
  { name: 'A', queueName: 'tasks', data: { step: 1 } },
  { name: 'B', queueName: 'tasks', data: { step: 2 }, deps: ['A'] },
  { name: 'C', queueName: 'tasks', data: { step: 3 }, deps: ['A'] },
  { name: 'D', queueName: 'tasks', data: { step: 4 }, deps: ['B', 'C'] },  // fan-in
], connection);

// Or via FlowProducer directly
const flow = new FlowProducer({ connection });
const jobs = await flow.addDAG({
  nodes: [
    { name: 'A', queueName: 'tasks', data: {}, deps: [] },
    { name: 'B', queueName: 'tasks', data: {}, deps: ['A'] },
    { name: 'C', queueName: 'tasks', data: {}, deps: ['A'] },
    { name: 'D', queueName: 'tasks', data: {}, deps: ['B', 'C'] },
  ],
});
// Returns Map<string, Job> keyed by node name
```

### DAGNode

- `name` - unique within the DAG (used in `deps`)
- `queueName` - target queue
- `data` - payload
- `opts?` - JobOptions
- `deps?` - array of node names that must complete first

### Reading Multiple Parent Results

```typescript
const worker = new Worker('tasks', async (job) => {
  if (job.name === 'D') {
    const parents = await job.getParents();
    // Returns { queue, id }[] - not Job instances
    // Fetch full jobs if needed:
    const parentJobs = await Promise.all(
      parents.map(p => new Queue(p.queue, { connection }).getJob(p.id))
    );
    const results = parentJobs.map(p => p.returnvalue);
    return { merged: results };
  }
}, { connection });
```

## Convenience Helpers

### chain() - Sequential Pipeline

Array is in **reverse execution order** (last element runs first).

```typescript
import { chain } from 'glide-mq';

// Execution: download -> parse -> transform -> upload
await chain('pipeline', [
  { name: 'upload',    data: {} },   // runs LAST (root)
  { name: 'transform', data: {} },
  { name: 'parse',     data: {} },
  { name: 'download',  data: {} },   // runs FIRST (leaf)
], connection);
```

### group() - Parallel Execution

```typescript
import { group } from 'glide-mq';

await group('tasks', [
  { name: 'resize-sm', data: { size: 'sm' } },
  { name: 'resize-md', data: { size: 'md' } },
  { name: 'resize-lg', data: { size: 'lg' } },
], connection);
// Creates synthetic __group__ parent that waits for all children
```

### chord() - Parallel + Callback

```typescript
import { chord } from 'glide-mq';

await chord(
  'tasks',
  // Group (parallel)
  [
    { name: 'score-a', data: { model: 'a' } },
    { name: 'score-b', data: { model: 'b' } },
  ],
  // Callback (after group completes)
  { name: 'select-best', data: {} },
  connection,
);
```

## Dynamic Children (moveToWaitingChildren)

Spawn children at runtime, then pause parent until they complete.

```typescript
import { Queue, Worker, WaitingChildrenError } from 'glide-mq';

const worker = new Worker('orchestrator', async (job) => {
  // Detect re-entry
  const existing = await job.getChildrenValues();
  if (Object.keys(existing).length > 0) {
    return { merged: Object.values(existing) };  // aggregate results
  }

  // Spawn children dynamically
  const childQueue = new Queue('subtasks', { connection });
  for (const url of job.data.urls) {
    await childQueue.add('fetch', { url }, {
      parent: { id: job.id!, queue: job.queueQualifiedName },
    });
  }
  await childQueue.close();

  // Pause until all children complete - throws WaitingChildrenError
  await job.moveToWaitingChildren();
}, { connection });
```

## Budget on Flows

Cap total token usage and/or cost across all jobs in a flow tree. Supports per-category limits and weighted totals.

```typescript
const flow = new FlowProducer({ connection });

await flow.add(
  {
    name: 'research',
    queueName: 'ai',
    data: { topic: 'quantum computing' },
    children: [
      { name: 'search', queueName: 'ai', data: {} },
      { name: 'summarize', queueName: 'ai', data: {} },
    ],
  },
  {
    budget: {
      maxTotalTokens: 50_000,
      maxTotalCost: 0.50,
      costUnit: 'usd',
      tokenWeights: { reasoning: 4, cachedInput: 0.25 },
      onExceeded: 'fail',      // 'fail' (default) or 'pause'
    },
  },
);

// Check budget state
const budget = await queue.getFlowBudget(parentJobId);
// { maxTotalTokens, maxTokens, tokenWeights, maxTotalCost, maxCosts, costUnit,
//   usedTokens, usedCost, exceeded, onExceeded }
```

Budget is propagated to every job in the flow via a `budgetKey` field.

## Suspend / Resume as Workflow Primitive

Suspend a job in a flow to await human approval, then resume and continue the pipeline.

```typescript
const worker = new Worker('ai', async (job) => {
  if (job.name === 'review') {
    if (job.signals.length > 0) {
      return { approved: job.signals.some(s => s.name === 'approve') };
    }
    await job.suspend({ reason: 'Human review required', timeout: 86_400_000 });
  }
  // other job types...
}, { connection });

// Resume externally
await queue.signal(jobId, 'approve', { reviewer: 'alice' });
```

When a suspended job resumes, it re-enters the stream and the processor is invoked again with `job.signals` populated. The parent flow continues once all children (including the resumed one) complete.

## Gotchas

- `chain()` array is **reverse execution order** - last element is leaf (runs first).
- `moveToWaitingChildren()` always throws `WaitingChildrenError`. No code after it executes.
- Processor re-runs **from the top** when children complete. Use `getChildrenValues()` to detect re-entry.
- Children must reference parent via `opts.parent: { id, queue }`.
- Cycles in DAGs are detected and rejected with `CycleError`.
- If a parent in a DAG fails, dependent jobs remain blocked indefinitely.
- `FlowProducer.add()` throws on duplicate jobId (cannot be partially created).
- Cross-queue dependencies are supported - each DAG node can have its own `queueName`.
