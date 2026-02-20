# Workflow Pipelines

## Table of Contents

- [FlowProducer — Parent-Child Job Trees](#flowproducer)
- [Reading Child Results](#reading-child-results)
- [`chain` — Sequential Pipeline](#chain)
- [`group` — Parallel Execution](#group)
- [`chord` — Parallel + Callback](#chord)

---

## FlowProducer

`FlowProducer` lets you atomically enqueue a tree of parent and child jobs. A parent job only becomes runnable once **all** of its children have completed.

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

In the parent processor, call `job.getChildrenValues()` to retrieve the return values of all direct children, keyed by `queueName::jobId`.

```typescript
const worker = new Worker('reports', async (job) => {
  // Runs only after all children have completed
  const childValues = await job.getChildrenValues();
  // {
  //   'data::1': { sales: 42000 },
  //   'data::2': { returns: 300 },
  // }

  const total = Object.values(childValues).reduce((s, v) => s + v.sales, 0);
  return { total };
}, { connection });
```

---

## `chain`

Execute a list of jobs **sequentially**. Step N+1 runs only after step N completes. Each step can read the previous step's result via `getChildrenValues()`.

```typescript
import { chain } from 'glide-mq';

await chain('pipeline', [
  { name: 'download',  data: { url: 'https://example.com/file.csv' } },
  { name: 'parse',     data: {} },
  { name: 'transform', data: {} },
  { name: 'upload',    data: { bucket: 'my-bucket' } },
], connection);
```

- The first job in the array is the root (the one that finishes last).
- The last job in the array runs first.
- Each step's processor receives the previous step's return value via `job.getChildrenValues()`.

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
    // { 'tasks::1': { score: 0.91 }, 'tasks::2': { score: 0.87 }, ... }
    const best = Object.entries(scores).sort((a, b) => b[1].score - a[1].score)[0];
    return { winner: best[0], score: best[1].score };
  }
  // ... other processors
}, { connection });
```
