# Workflow Pipelines

## Table of Contents

- [FlowProducer — Parent-Child Job Trees](#flowproducer)
- [Reading Child Results](#reading-child-results)
- [DAG Workflows — Multiple Parents](#dag-workflows--multiple-parents)
- [`chain` — Sequential Pipeline](#chain)
- [`group` — Parallel Execution](#group)
- [`chord` — Parallel + Callback](#chord)

---

## FlowProducer

`FlowProducer` lets you atomically enqueue a tree of parent and child jobs. A parent job only becomes runnable once **all** of its children have successfully completed; failed or dead-lettered children do not unblock the parent.

```typescript
import { FlowProducer } from 'glide-mq';

const flow = new FlowProducer({ connection });

const { job: parent } = await flow.add({
  name: 'aggregate',
  queueName: 'reports',
  data: { month: '2025-01' },
  children: [
    { name: 'fetch-sales',    queueName: 'data', data: { region: 'eu' } },
    { name: 'fetch-returns',  queueName: 'data', data: { region: 'eu' } },
    {
      name: 'fetch-inventory',
      queueName: 'data',
      data: {},
      // Nested: child can itself have children
      children: [
        { name: 'load-warehouse-a', queueName: 'data', data: {} },
        { name: 'load-warehouse-b', queueName: 'data', data: {} },
      ],
    },
  ],
});

console.log('Parent job ID:', parent.id);

await flow.close();
```

### Bulk flows

```typescript
const nodes = await flow.addBulk([
  {
    name: 'report-jan', queueName: 'reports', data: {},
    children: [{ name: 'data-jan', queueName: 'data', data: {} }],
  },
  {
    name: 'report-feb', queueName: 'reports', data: {},
    children: [{ name: 'data-feb', queueName: 'data', data: {} }],
  },
]);
```

---

## Reading Child Results

In the parent processor, call `job.getChildrenValues()` to retrieve the return values of all direct children. The keys are internal dependency identifiers (implementation detail — prefer `Object.values()` when you only need the results).

```typescript
const worker = new Worker('reports', async (job) => {
  // Runs only after all children have completed
  const childValues = await job.getChildrenValues();
  // Keys are opaque internal identifiers; use Object.values() for the results:
  const results = Object.values(childValues);
  // [ { sales: 42000 }, { returns: 300 } ]

  const totalSales = results.reduce((s, v) => s + (v.sales ?? 0), 0);
  return { totalSales };
}, { connection });
```

---

## DAG Workflows — Multiple Parents

`FlowProducer.addDAG()` lets you define **arbitrary DAG (Directed Acyclic Graph) topologies** where any job can have multiple parent dependencies. A job only becomes runnable once **all** of its dependencies have successfully completed.

### Use cases

- **Fan-in merge**: Multiple parallel data sources converge into a single aggregation job
- **Diamond dependencies**: Job D depends on both B and C, which both depend on A
- **Multi-stage pipelines**: Complex workflows where certain jobs must wait for multiple upstream tasks

### API

```typescript
import { FlowProducer, dag } from 'glide-mq';

const flow = new FlowProducer({ connection });

// Submit a DAG using the helper function
const jobs = await dag('queueName', [
  { name: 'A', data: { step: 1 } },
  { name: 'B', data: { step: 2 }, deps: ['A'] },
  { name: 'C', data: { step: 3 }, deps: ['A'] },
  { name: 'D', data: { step: 4 }, deps: ['B', 'C'] },
], connection);

// Or use FlowProducer.addDAG() directly
const jobs = await flow.addDAG({
  nodes: [
    { name: 'A', queueName: 'tasks', data: { step: 1 } },
    { name: 'B', queueName: 'tasks', data: { step: 2 }, deps: ['A'] },
    { name: 'C', queueName: 'tasks', data: { step: 3 }, deps: ['A'] },
    { name: 'D', queueName: 'tasks', data: { step: 4 }, deps: ['B', 'C'] },
  ],
});
// Returns Map<string, Job> keyed by node name
```

Each **DAGNode** has:
- `name` — unique identifier within this DAG (used in `deps` arrays)
- `queueName` — queue to submit this job to
- `data` — job payload
- `opts?` — job options (delay, priority, attempts, etc.)
- `deps?` — array of node names that must complete before this job runs

### Example: Fan-in merge

```typescript
import { dag } from 'glide-mq';

// Three parallel data fetches, then one merge job
const jobs = await dag('data', [
  { name: 'fetch-sales', data: { source: 'sales-db' } },
  { name: 'fetch-inventory', data: { source: 'warehouse-db' } },
  { name: 'fetch-returns', data: { source: 'returns-db' } },
  {
    name: 'merge-reports',
    data: { reportId: 'Q1-2025' },
    deps: ['fetch-sales', 'fetch-inventory', 'fetch-returns'],
  },
], connection);

// All three fetches run in parallel.
// 'merge-reports' runs only after all three complete.
```

### Example: Diamond dependency

```typescript
import { dag } from 'glide-mq';

// Job topology:
//       A
//      / \
//     B   C
//      \ /
//       D

const jobs = await dag('tasks', [
  { name: 'A', data: { step: 'root' } },
  { name: 'B', data: { step: 'left' }, deps: ['A'] },
  { name: 'C', data: { step: 'right' }, deps: ['A'] },
  { name: 'D', data: { step: 'converge' }, deps: ['B', 'C'] },
], connection);

// A runs first, then B and C in parallel, then D after both complete.
```

**Implementation notes:**
- DAG validation runs automatically - cycles are detected and rejected with `CycleError`.
- Jobs are submitted in topological order (leaves first, roots last).
- If any parent fails or is dead-lettered, dependent jobs remain blocked indefinitely (manual cleanup required).
- Cross-queue dependencies are supported - each node can specify its own `queueName`.

### Reading results from multiple parents

Use `job.getParents()` to fetch all parent jobs and their results:

```typescript
const worker = new Worker('tasks', async (job) => {
  if (job.name === 'D') {
    const parents = await job.getParents();
    // parents is an array of Job instances
    const results = parents.map(p => p.returnvalue);
    return { merged: results };
  }
}, { connection });
```

Alternatively, manually fetch specific parents if you know their IDs:

```typescript
const parentB = await Job.fromId(queue, 'B-job-id');
const parentC = await Job.fromId(queue, 'C-job-id');
const resultB = parentB?.returnvalue;
const resultC = parentC?.returnvalue;
```

---

## `chain`

Execute a list of jobs **sequentially**, specified in **reverse execution order** (the last element in the array runs first). Each step can read the previous step's result via `getChildrenValues()`.

```typescript
import { chain } from 'glide-mq';

// Execution order: download → parse → transform → upload
await chain('pipeline', [
  { name: 'upload',    data: { bucket: 'my-bucket' } },   // runs last  (root)
  { name: 'transform', data: {} },
  { name: 'parse',     data: {} },
  { name: 'download',  data: { url: 'https://example.com/file.csv' } }, // runs first (leaf)
], connection);
```

- The **last** element in the array is the leaf — it runs first.
- The **first** element in the array is the root — it runs last (after all descendants complete).
- Each step's processor can access the prior step's return value via `Object.values(job.getChildrenValues())[0]`.

```typescript
const worker = new Worker('pipeline', async (job) => {
  if (job.name === 'parse') {
    const prev = await job.getChildrenValues();
    const raw = Object.values(prev)[0]; // result from 'download'
    return parse(raw);
  }
  // ...
}, { connection });
```

---

## `group`

Execute a list of jobs **in parallel**. A synthetic `__group__` parent waits for all children to complete.

```typescript
import { group } from 'glide-mq';

await group('tasks', [
  { name: 'resize-thumb',  data: { imageId: 1, size: 'sm' } },
  { name: 'resize-medium', data: { imageId: 1, size: 'md' } },
  { name: 'resize-large',  data: { imageId: 1, size: 'lg' } },
], connection);
```

The `__group__` parent processor (if you define one) can collect results from all children via `getChildrenValues()`.

---

## `chord`

Run a group of jobs in parallel, then execute a **callback** job once all group members are done. The callback receives the group results.

```typescript
import { chord } from 'glide-mq';

await chord(
  'tasks',
  // Group (runs in parallel)
  [
    { name: 'score-model-a', data: { modelId: 'a' } },
    { name: 'score-model-b', data: { modelId: 'b' } },
    { name: 'score-model-c', data: { modelId: 'c' } },
  ],
  // Callback (runs after all group members complete)
  { name: 'select-best-model', data: {} },
  connection,
);
```

In the callback processor:

```typescript
const worker = new Worker('tasks', async (job) => {
  if (job.name === 'select-best-model') {
    const scores = await job.getChildrenValues();
    // Keys are opaque — use Object.entries() if you need them, or Object.values():
    const best = Object.entries(scores).sort((a, b) => b[1].score - a[1].score)[0];
    return { score: best[1].score };
  }
  // ... other processors
}, { connection });
```
