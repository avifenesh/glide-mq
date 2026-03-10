# Step Jobs

Step jobs let you pause an active job mid-processor and resume it later. The same job re-enters the processor with updated state, allowing multi-step workflows without separate queues.

## moveToDelayed

Use `job.moveToDelayed(timestampMs, nextStep?)` inside a processor when the same logical job should sleep and resume later instead of completing.

```typescript
import { Worker } from 'glide-mq';

const worker = new Worker('drip-campaign', async (job) => {
  switch (job.data.step) {
    case 'send':
      await sendEmail(job.data);
      return job.moveToDelayed(Date.now() + 24 * 3600_000, 'check');
    case 'check':
      if (!(await checkOpened(job.data))) {
        return job.moveToDelayed(Date.now() + 3600_000, 'followup');
      }
      return 'done';
    case 'followup':
      await sendFollowUp(job.data);
      return 'done';
  }
}, { connection });
```

When `moveToDelayed` is called:

1. The job moves from active to the delayed sorted set with the given timestamp.
2. If `nextStep` is provided and `job.data` is a plain object, `job.data.step` is updated atomically.
3. The processor returns without completing or failing the job.
4. When the delay expires, the scheduler promotes the job back to waiting, and the worker picks it up again.

### Notes

- `moveToDelayed()` must be called from an active worker processor.
- `nextStep` is a convenience for plain object payloads; it updates `job.data.step` atomically with the delayed transition.
- `DelayedError` is exported for advanced/manual control, but `job.moveToDelayed()` is the normal API.

## Dynamic children (moveToWaitingChildren)

A parent processor can spawn child jobs at runtime, then call `job.moveToWaitingChildren()` to pause until all children complete. When the last child finishes, the parent resumes and the processor is invoked again.

```typescript
import { Queue, Worker, FlowProducer, WaitingChildrenError } from 'glide-mq';

const parentWorker = new Worker('orchestrator', async (job) => {
  const step = job.data.step ?? 'spawn';

  if (step === 'spawn') {
    // Dynamically add child jobs
    const childQueue = new Queue('subtasks', { connection });
    await childQueue.add('chunk-1', { chunk: 1 }, { parent: { queue: 'orchestrator', id: job.id } });
    await childQueue.add('chunk-2', { chunk: 2 }, { parent: { queue: 'orchestrator', id: job.id } });
    await childQueue.close();

    // Pause -- throws WaitingChildrenError internally
    await job.moveToWaitingChildren();
  }

  // Resumed after all children completed
  const childResults = await job.getChildrenValues();
  return { merged: Object.values(childResults) };
}, { connection });
```

`moveToWaitingChildren()` throws `WaitingChildrenError` to signal the worker. If all children have already completed by the time the call is made, the job transitions directly back to active.

## Combining both patterns

Step jobs and dynamic children can be combined for complex orchestration:

```typescript
const worker = new Worker('pipeline', async (job) => {
  switch (job.data.step ?? 'validate') {
    case 'validate':
      await validateInput(job.data);
      // Wait 5 minutes for external approval
      return job.moveToDelayed(Date.now() + 5 * 60_000, 'spawn');

    case 'spawn':
      const q = new Queue('tasks', { connection });
      for (const item of job.data.items) {
        await q.add('process-item', item, { parent: { queue: 'pipeline', id: job.id } });
      }
      await q.close();
      await job.moveToWaitingChildren();
      break;

    default:
      // All children done -- collect results
      const results = await job.getChildrenValues();
      return { processed: Object.keys(results).length };
  }
}, { connection });
```

## DelayedError

`DelayedError` is the error class thrown internally by `moveToDelayed()`. You can throw it manually for advanced control:

```typescript
import { DelayedError } from 'glide-mq';

const worker = new Worker('manual', async (job) => {
  // Equivalent to job.moveToDelayed(Date.now() + 60_000)
  await job.updateData({ ...job.data, step: 'next' });
  throw new DelayedError('Pausing for 60 seconds');
}, { connection });
```

In practice, use `job.moveToDelayed()` -- it handles the data update and delay scheduling in one call.
