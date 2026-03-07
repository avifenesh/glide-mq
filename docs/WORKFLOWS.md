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

In addition to parent-child trees, `FlowProducer.add` supports **arbitrary DAG (Directed Acyclic Graph) topologies**. Any child can declare multiple parents via `opts.parents`. A child job only becomes runnable once **all** of its declared parents have successfully completed.

### Use cases

- **Fan-in merge**: Multiple parallel data sources converge into a single aggregation job
- **Diamond dependencies**: Job D depends on both B and C, which both depend on A
- **Multi-stage pipelines**: Complex workflows where certain jobs must wait for multiple upstream tasks

### Example: Fan-in merge

```typescript
import { FlowProducer } from 'glide-mq';

const flow = new FlowProducer({ connection });

// Three parallel data fetches, then one merge job
const { job: mergeJob } = await flow.add({
  name: 'merge-reports',
  queueName: 'reports',
  data: { reportId: 'Q1-2025' },
  children: [
    {
      name: 'fetch-sales',
      queueName: 'data',
      data: { source: 'sales-db' },
    },
    {
      name: 'fetch-inventory',
      queueName: 'data',
      data: { source: 'warehouse-db' },
    },
    {
      name: 'fetch-returns',
      queueName: 'data',
      data: { source: 'returns-db' },
    },
  ],
});

// All three children run in parallel.
// 'merge-reports' runs only after all three complete.
```

### Example: Diamond dependency

```typescript
// Job topology:
//       A
//      / \
//     B   C
//      \ /
//       D

const { job: jobA } = await flow.add({
  name: 'root',
  queueName: 'tasks',
  data: { step: 'A' },
  children: [
    {
      name: 'left-branch',
      queueName: 'tasks',
      data: { step: 'B' },
      children: [
        {
          name: 'converge',
          queueName: 'tasks',
          data: { step: 'D' },
          opts: {
            // D must wait for both B and C
            parents: ['{{jobC.id}}'],  // reference to job C's ID
          },
        },
      ],
    },
    {
      name: 'right-branch',
      queueName: 'tasks',
      data: { step: 'C' },
      opts: { jobId: 'jobC' },  // assign a custom ID so B's child can reference it
      children: [
        // D is already a child of B above; no need to duplicate here
      ],
    },
  ],
});
```

**Implementation notes:**
- Use `opts.jobId` to assign custom IDs to jobs you need to reference.
- Use `opts.parents: [parentId1, parentId2, ...]` to declare additional parent dependencies beyond the structural parent.
- A child with multiple parents blocks until **all** declared parents reach the `completed` state.
- If any parent fails or is dead-lettered, the child remains blocked indefinitely (manual cleanup required).

### Reading results from multiple parents

In a job with multiple parents, `job.getChildrenValues()` only returns the direct structural children (from the `children` array). To access results from **declared parents** (via `opts.parents`), you must manually fetch them:

```typescript
const worker = new Worker('tasks', async (job) => {
  if (job.name === 'converge') {
    // Structural children (if any)
    const childValues = await job.getChildrenValues();

    // Manually fetch parent results
    const parentB = await Job.fromId(queue, 'left-branch-job-id');
    const parentC = await Job.fromId(queue, 'jobC');
    const resultB = parentB?.returnvalue;
    const resultC = parentC?.returnvalue;

    return { merged: [resultB, resultC, ...Object.values(childValues)] };
  }
}, { connection });
```

**Alternative**: If you need automatic result collection from multiple parents, consider using `chord` for a parallel group + callback pattern, or structure your flow so the merge job is a structural parent of the fan-out jobs.

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
