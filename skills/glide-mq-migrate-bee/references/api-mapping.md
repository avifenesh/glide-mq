# Bee-Queue to glide-mq - Complete API Mapping

Method-by-method reference for converting every Bee-Queue API call to its glide-mq equivalent.

## Constructor

```typescript
// BEFORE
const Queue = require('bee-queue');
const queue = new Queue('tasks', {
  redis: { host: 'localhost', port: 6379 },
  prefix: 'bq',
  isWorker: true,
  getEvents: true,
  sendEvents: true,
  storeJobs: true,
  removeOnSuccess: false,
  removeOnFailure: false,
  stallInterval: 5000,
  activateDelayedJobs: true,
});

// AFTER - split into Queue + Worker
import { Queue, Worker, QueueEvents } from 'glide-mq';
const connection = { addresses: [{ host: 'localhost', port: 6379 }] };

const queue = new Queue('tasks', {
  connection,
  prefix: 'glide',
});

const worker = new Worker('tasks', processor, {
  connection,
  lockDuration: 30000,
  stalledInterval: 30000,
});

const events = new QueueEvents('tasks', { connection });
```

## Job Creation Methods

### createJob + save -> add

```typescript
// BEFORE - chained builder (no job name)
const job = await queue.createJob({ x: 1 }).save();
console.log(job.id);

// AFTER - single call (name required)
const job = await queue.add('compute', { x: 1 });
console.log(job.id);
```

### setId -> jobId option

```typescript
// BEFORE
queue.createJob(data).setId('unique-key').save();

// AFTER
await queue.add('task', data, { jobId: 'unique-key' });
```

### retries -> attempts

**Name change: `retries` becomes `attempts`.**

```typescript
// BEFORE
queue.createJob(data).retries(3).save();

// AFTER
await queue.add('task', data, { attempts: 3 });
```

### backoff -> backoff option

```typescript
// BEFORE - immediate (default)
queue.createJob(data).retries(3).backoff('immediate').save();

// AFTER
await queue.add('task', data, {
  attempts: 3,
  backoff: { type: 'fixed', delay: 0 },
});

// BEFORE - fixed
queue.createJob(data).retries(3).backoff('fixed', 1000).save();

// AFTER
await queue.add('task', data, {
  attempts: 3,
  backoff: { type: 'fixed', delay: 1000 },
});

// BEFORE - exponential
queue.createJob(data).retries(3).backoff('exponential', 1000).save();

// AFTER
await queue.add('task', data, {
  attempts: 3,
  backoff: { type: 'exponential', delay: 1000 },
});
```

### delayUntil -> delay

```typescript
// BEFORE - absolute timestamp
queue.createJob(data).delayUntil(Date.now() + 60000).save();

// AFTER - relative milliseconds
await queue.add('task', data, { delay: 60000 });
```

### timeout -> timeout job option

```typescript
// BEFORE - per-job timeout
queue.createJob(data).timeout(30000).save();

// AFTER - per-job timeout option
await queue.add('task', data, { timeout: 30000 });
```

### Full chained builder conversion

```typescript
// BEFORE - all options chained
const job = await queue.createJob({ email: 'user@example.com' })
  .setId('email-123')
  .retries(3)
  .backoff('exponential', 1000)
  .delayUntil(Date.now() + 60000)
  .timeout(30000)
  .save();

// AFTER - single options object
const job = await queue.add('send-email',
  { email: 'user@example.com' },
  {
    jobId: 'email-123',
    attempts: 3,
    backoff: { type: 'exponential', delay: 1000 },
    delay: 60000,
    timeout: 30000,
  }
);
```

## Processing Methods

### process -> Worker

```typescript
// BEFORE - promise-based
queue.process(async (job) => {
  return { result: job.data.x * 2 };
});

// AFTER
const worker = new Worker('tasks', async (job) => {
  return { result: job.data.x * 2 };
}, { connection });

// BEFORE - with concurrency
queue.process(10, async (job) => {
  return await processJob(job);
});

// AFTER
const worker = new Worker('tasks', async (job) => {
  return await processJob(job);
}, { connection, concurrency: 10 });

// BEFORE - callback-based (deprecated pattern)
queue.process(function(job, done) {
  done(null, { result: job.data.x * 2 });
});

// AFTER - always promise-based
const worker = new Worker('tasks', async (job) => {
  return { result: job.data.x * 2 };
}, { connection });
```

### reportProgress -> updateProgress

```typescript
// BEFORE - any JSON value
queue.process(async (job) => {
  job.reportProgress({ page: 3, total: 10 });
  job.reportProgress(50);
  job.reportProgress('halfway');
  return result;
});

// AFTER - number (0-100) or object, use job.log() for text messages
const worker = new Worker('tasks', async (job) => {
  await job.updateProgress(30);
  await job.updateProgress({ page: 3, total: 10 });  // objects also supported
  await job.log('Processing page 3 of 10');
  await job.updateProgress(50);
  return result;
}, { connection });
```

## Bulk Operations

### saveAll -> addBulk

```typescript
// BEFORE
const jobs = [
  queue.createJob({ x: 1 }),
  queue.createJob({ x: 2 }),
  queue.createJob({ x: 3 }),
];
const errors = await queue.saveAll(jobs);
// errors is Map<Job, Error>

// AFTER
const results = await queue.addBulk([
  { name: 'compute', data: { x: 1 } },
  { name: 'compute', data: { x: 2 } },
  { name: 'compute', data: { x: 3 } },
]);
```

## Query Methods

### getJob

```typescript
// BEFORE
const job = await queue.getJob('42');

// AFTER - same API
const job = await queue.getJob('42');
```

### getJobs

```typescript
// BEFORE - type + page object
const waiting = await queue.getJobs('waiting', { start: 0, end: 25 });
const failed = await queue.getJobs('failed', { size: 100 });

// AFTER - type + start + end
const waiting = await queue.getJobs('waiting', 0, 25);
const failed = await queue.getJobs('failed', 0, 100);
```

### removeJob

```typescript
// BEFORE - by ID on queue
await queue.removeJob('42');

// AFTER - via Job instance
const job = await queue.getJob('42');
await job.remove();
```

### checkHealth -> getJobCounts

```typescript
// BEFORE
const health = await queue.checkHealth();
// { waiting: 5, active: 2, succeeded: 100, failed: 3, delayed: 1, newestJob: '108' }

// AFTER
const counts = await queue.getJobCounts();
// { waiting: 5, active: 2, completed: 100, failed: 3, delayed: 1 }
// Note: "succeeded" renamed to "completed", no "newestJob"
```

## Lifecycle Methods

### close

```typescript
// BEFORE
await queue.close(30000);

// AFTER - close individual components
await worker.close();
await queue.close();
await events.close();

// OR - graceful shutdown (registers SIGTERM/SIGINT, blocks until signal)
import { gracefulShutdown } from 'glide-mq';
const handle = gracefulShutdown([worker, queue, events]);
// For programmatic shutdown: await handle.shutdown();
```

### destroy -> obliterate

```typescript
// BEFORE
await queue.destroy();

// AFTER
await queue.obliterate();
```

### ready

```typescript
// BEFORE
await queue.ready();

// AFTER
await worker.waitUntilReady();
```

### isRunning

```typescript
// BEFORE
queue.isRunning();

// AFTER
worker.isRunning();
```

## Stall Detection

```typescript
// BEFORE - manual setup, repeated call required
queue.checkStalledJobs(5000, (err, numStalled) => {
  console.log('Stalled:', numStalled);
});

// AFTER - automatic, configured on Worker
const worker = new Worker('tasks', processor, {
  connection,
  lockDuration: 30000,     // how long a job can run before considered stalled
  stalledInterval: 30000,  // how often to check for stalled jobs
  maxStalledCount: 2,      // re-queue up to 2 times before failing
});

worker.on('stalled', (jobId) => {
  console.log('Stalled:', jobId);
});
```

## Event Migration

### Local events (Queue -> Worker)

```typescript
// BEFORE
queue.on('succeeded', (job, result) => {});
queue.on('failed', (job, err) => {});
queue.on('retrying', (job, err) => {});
queue.on('stalled', (jobId) => {});
queue.on('error', (err) => {});

// AFTER
worker.on('completed', (job, result) => {});
worker.on('failed', (job, err) => {});
// No separate 'retrying' event - failed fires for all failures
worker.on('stalled', (jobId) => {});
worker.on('error', (err) => {});
```

### PubSub events (Queue -> QueueEvents)

```typescript
// BEFORE
queue.on('job succeeded', (jobId, result) => {});
queue.on('job failed', (jobId, err) => {});
queue.on('job progress', (jobId, data) => {});

// AFTER
const events = new QueueEvents('tasks', { connection });
events.on('completed', ({ jobId, returnvalue }) => {});
events.on('failed', ({ jobId, failedReason }) => {});
events.on('progress', ({ jobId, data }) => {});
```

### Per-job events (Job -> QueueEvents)

```typescript
// BEFORE
const job = await queue.createJob(data).save();
job.on('succeeded', (result) => console.log('Done:', result));
job.on('failed', (err) => console.error('Failed:', err));
job.on('progress', (p) => console.log('Progress:', p));

// AFTER - filter by jobId in QueueEvents
const job = await queue.add('task', data);
const events = new QueueEvents('tasks', { connection });
events.on('completed', ({ jobId, returnvalue }) => {
  if (jobId === job.id) console.log('Done:', returnvalue);
});

// OR - use addAndWait for request-reply
const result = await queue.addAndWait('task', data, { waitTimeout: 30000 });
```

## Custom Backoff Strategies

```typescript
// BEFORE
queue.backoffStrategies.set('linear', (job) => {
  return job.options.backoff.delay * (job.options.retries + 1);
});
queue.createJob(data).retries(5).backoff('linear', 1000).save();

// AFTER
const worker = new Worker('tasks', processor, {
  connection,
  backoffStrategies: {
    linear: (attemptsMade) => attemptsMade * 1000,
  },
});
await queue.add('task', data, {
  attempts: 5,
  backoff: { type: 'linear', delay: 1000 },
});
```

## Connection Formats

```typescript
// BEFORE - object
new Queue('tasks', { redis: { host: 'redis.example.com', port: 6380 } });

// BEFORE - URL string
new Queue('tasks', { redis: 'redis://user:pass@host:6379/0' });

// BEFORE - existing ioredis client
const Redis = require('ioredis');
new Queue('tasks', { redis: new Redis() });

// AFTER - always addresses array
const connection = { addresses: [{ host: 'redis.example.com', port: 6380 }] };

// AFTER - with TLS
const connection = { addresses: [{ host: 'redis.example.com', port: 6380 }], useTLS: true };

// AFTER - cluster mode
const connection = {
  addresses: [
    { host: 'node1', port: 7000 },
    { host: 'node2', port: 7001 },
  ],
  clusterMode: true,
};
```

## Graceful Shutdown

```typescript
// BEFORE
async function shutdown() {
  await queue.close(30000);
  process.exit(0);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

// AFTER - gracefulShutdown registers SIGTERM/SIGINT automatically
import { gracefulShutdown } from 'glide-mq';
const handle = gracefulShutdown([worker, queue, events]);
// Blocks until signal fires. For programmatic: await handle.shutdown()
```
