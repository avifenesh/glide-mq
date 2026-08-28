/**
 * Token bucket must not treat idle-at-capacity time as refill credit.
 *
 * tbRefill returns early when full without updating tbLastRefill. The next
 * consume then refills using the whole idle interval, so two cost=capacity
 * jobs run back-to-back after a pause.
 *
 * Run: npx vitest run tests/tb-idle-refill.test.ts
 */
import { afterAll, beforeAll, expect, it } from 'vitest';
import { createCleanupClient, describeEachMode, flushQueue, waitFor } from './helpers/fixture';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { moveToActive } = require('../dist/functions/index') as typeof import('../src/functions/index');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

describeEachMode('token bucket idle-at-capacity refill', (CONNECTION) => {
  let cleanupClient: any;
  const queues: string[] = [];

  function uniqueQueue(prefix: string): string {
    const name = `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    queues.push(name);
    return name;
  }

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await Promise.all(queues.map((q) => flushQueue(cleanupClient, q).catch(() => {})));
    cleanupClient.close();
  });

  it('does not let a second full-cost job through immediately after an idle-full bucket', async () => {
    const Q = uniqueQueue('tb-idle');
    const queue = new Queue(Q, { connection: CONNECTION });
    const tb = { key: 'idle-group', concurrency: 10, tokenBucket: { capacity: 1, refillRate: 1 } };

    // Creating the group fills the bucket and stamps tbLastRefill. Idle with
    // jobs waiting so activation sees tokens >= capacity and skips the stamp.
    await queue.add('a', { n: 1 }, { ordering: tb, cost: 1 });
    await queue.add('b', { n: 2 }, { ordering: tb, cost: 1 });
    await new Promise((r) => setTimeout(r, 2000));

    const completed: number[] = [];
    const worker = new Worker(
      Q,
      async () => {
        completed.push(Date.now());
      },
      { connection: CONNECTION, concurrency: 4, blockTimeout: 50 },
    );
    worker.on('error', () => {});

    try {
      await waitFor(() => completed.length === 2, 5000, 50);
      completed.sort((a, b) => a - b);
      expect(completed[1]! - completed[0]!).toBeGreaterThanOrEqual(700);
    } finally {
      await worker.close(true);
      await queue.close();
    }
  }, 15000);

  it('does not persist an ahead caller clock as tbLastRefill at capacity', async () => {
    const Q = uniqueQueue('tb-idle-clock');
    const queue = new Queue(Q, { connection: CONNECTION });
    const tb = { key: 'clock-group', concurrency: 10, tokenBucket: { capacity: 1, refillRate: 1 } };
    const job = await queue.add('a', { n: 1 }, { ordering: tb, cost: 1 });
    const keys = buildKeys(Q);
    const streamEntries = (await cleanupClient.xrange(keys.stream, '-', '+')) as Record<string, [string, string][]>;
    const entryId = Object.keys(streamEntries)[0];
    expect(entryId).toBeTruthy();
    await cleanupClient.xgroupCreate(keys.stream, 'workers', '0', { mkStream: true }).catch(() => {});

    const future = Date.now() + 3_600_000;
    await moveToActive(cleanupClient, keys, job.id, future, keys.stream, entryId, 'workers');
    const last = Number(await cleanupClient.hget(keys.group(tb.key), 'tbLastRefill'));
    expect(last).toBeGreaterThan(0);
    expect(last).toBeLessThan(Date.now() + 5_000);

    await queue.close();
  }, 15000);
});
