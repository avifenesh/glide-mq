/**
 * Token bucket must not treat idle-at-capacity time as refill credit.
 *
 * tbRefill returns early when full without updating tbLastRefill. The next
 * consume then refills using the whole idle interval, so two cost=capacity
 * jobs run back-to-back after a pause.
 *
 * Run: npx vitest run tests/tb-idle-refill.test.ts
 */
import { afterAll, beforeAll, expect, it, vi } from 'vitest';
import { createCleanupClient, describeEachMode, flushQueue, waitFor } from './helpers/fixture';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { FlowProducer } = require('../dist/flow-producer') as typeof import('../src/flow-producer');
const { CONSUMER_GROUP, addJob, completeJob, moveToActive } =
  require('../dist/functions/index') as typeof import('../src/functions/index');
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
      { connection: CONNECTION, concurrency: 4, blockTimeout: 50, promotionInterval: 250 },
    );
    worker.on('error', () => {});

    try {
      await waitFor(() => completed.length === 2, 12000, 50);
      completed.sort((a, b) => a - b);
      expect(completed[1]! - completed[0]!).toBeGreaterThanOrEqual(700);
    } finally {
      await worker.close(true);
      await queue.close();
    }
  }, 20000);

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

  // Group setup stamps tbLastRefill from the caller's clock. A producer whose
  // clock runs ahead leaves a future tbLastRefill on a full bucket. tbRefill
  // must pull it back to server time when a job consumes at capacity: if it
  // survives the consumption, the next refill clamps it to "now" with zero
  // elapsed time, and the time since the consumption never refills the bucket.
  it('refills for the time since a consumption when a fast producer clock seeded the group', async () => {
    const Q = uniqueQueue('tb-fast-clock');
    const k = buildKeys(Q);
    const group = 'fast-clock-group';
    // Capacity 1 token, 10 tokens/s (one token per 100 ms), cost 1 token. Values are millitokens.
    const add = (name: string, timestamp: number) =>
      addJob(cleanupClient, k, name, '{}', '{}', timestamp, 0, 0, '', 0, group, 10, 0, 0, 1000, 10000, 1000);
    await cleanupClient.xgroupCreate(k.stream, CONSUMER_GROUP, '0', { mkStream: true }).catch(() => {});
    const activate = async (jobId: string) => {
      const res = (await cleanupClient.xreadgroup(CONSUMER_GROUP, 'w1', { [k.stream]: '>' }, { count: 1 })) as any[];
      const entryId = String(Object.keys(res[0].value)[0]);
      const moved = await moveToActive(cleanupClient, k, jobId, Date.now(), k.stream, entryId, CONSUMER_GROUP);
      return { entryId, moved };
    };

    const a = await add('a', Date.now() + 3_600_000);
    const first = await activate(a);
    expect(typeof first.moved).toBe('object');
    await completeJob(cleanupClient, k, a, first.entryId, 'null', Date.now(), CONSUMER_GROUP);

    // Idle for several refill intervals: the bucket is empty and nothing calls tbRefill.
    await new Promise((r) => setTimeout(r, 500));

    const b = await add('b', Date.now());
    const second = await activate(b);
    expect(second.moved).not.toBe('GROUP_TOKEN_LIMITED');
    expect(typeof second.moved).toBe('object');
  }, 15000);

  // A future tbLastRefill already stored on a full bucket (written by an older
  // library or a fast producer) is pulled back to server time by the next
  // consumption instead of surviving until the bucket drains.
  it('normalizes a stored future tbLastRefill when a job consumes a full bucket', async () => {
    const Q = uniqueQueue('tb-future-stored');
    const k = buildKeys(Q);
    const group = 'future-stored-group';
    const a = await addJob(
      cleanupClient,
      k,
      'a',
      '{}',
      '{}',
      Date.now(),
      0,
      0,
      '',
      0,
      group,
      10,
      0,
      0,
      1000,
      1000,
      1000,
    );
    await cleanupClient.hset(k.group(group), { tbLastRefill: String(Date.now() + 3_600_000) });
    await cleanupClient.xgroupCreate(k.stream, CONSUMER_GROUP, '0', { mkStream: true }).catch(() => {});
    const res = (await cleanupClient.xreadgroup(CONSUMER_GROUP, 'w1', { [k.stream]: '>' }, { count: 1 })) as any[];
    const entryId = String(Object.keys(res[0].value)[0]);

    const moved = await moveToActive(cleanupClient, k, a, Date.now(), k.stream, entryId, CONSUMER_GROUP);
    expect(typeof moved).toBe('object');
    expect(Number(await cleanupClient.hget(k.group(group), 'tbTokens'))).toBe(0);
    expect(Number(await cleanupClient.hget(k.group(group), 'tbLastRefill'))).toBeLessThan(Date.now() + 5_000);
  }, 15000);

  // addFlow seeds each token-bucket group it creates. A fast producer clock must
  // not become the group's tbLastRefill, for the parent group or a child group.
  it('seeds tbLastRefill from the server clock when addFlow creates token bucket groups', async () => {
    const parentQueue = uniqueQueue('tb-flow-parent');
    const childQueue = uniqueQueue('tb-flow-child');
    const bucket = { capacity: 2, refillRate: 1 };
    const flow = new FlowProducer({ connection: CONNECTION });
    const realNow = Date.now();
    const clock = vi.spyOn(Date, 'now').mockReturnValue(realNow + 3_600_000);
    try {
      await flow.add({
        name: 'parent',
        queueName: parentQueue,
        data: {},
        opts: { ordering: { key: 'flow-parent-group', tokenBucket: bucket } },
        children: [
          {
            name: 'child',
            queueName: childQueue,
            data: {},
            opts: { ordering: { key: 'flow-child-group', tokenBucket: bucket } },
          },
        ],
      });
    } finally {
      clock.mockRestore();
      await flow.close();
    }

    for (const [queueName, group] of [
      [parentQueue, 'flow-parent-group'],
      [childQueue, 'flow-child-group'],
    ] as const) {
      const last = Number(await cleanupClient.hget(buildKeys(queueName).group(group), 'tbLastRefill'));
      expect(last).toBeGreaterThanOrEqual(realNow);
      expect(last).toBeLessThan(Date.now() + 5_000);
    }
  }, 15000);
});
