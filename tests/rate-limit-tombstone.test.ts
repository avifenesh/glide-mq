import { afterAll, beforeAll, expect, it } from 'vitest';
import { createCleanupClient, describeEachMode, flushQueue, waitFor } from './helpers/fixture';

const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { promoteRateLimited } = require('../dist/functions/index') as typeof import('../src/functions/index');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

describeEachMode('Rate-limited group tombstones', (CONNECTION) => {
  const queueName = `ratelimit-tombstone-${Date.now()}`;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, queueName);
    cleanupClient.close();
  });

  it('promotes a valid job after discarding a missing token-bucket head', async () => {
    const keys = buildKeys(queueName);
    const groupKey = 'tombstone-group';
    const missingId = 'missing';
    const validId = 'valid';
    const now = Date.now();

    await cleanupClient.hset(keys.group(groupKey), {
      maxConcurrency: '1',
      active: '0',
      tbCapacity: '1000',
      tbTokens: '1000',
      tbRefillRate: '1000',
      tbLastRefill: String(now),
      tbRefillRemainder: '0',
      nextSeq: '1',
    });
    await cleanupClient.hset(keys.job(validId), {
      id: validId,
      name: 'valid',
      state: 'group-waiting',
      groupKey,
      cost: '1000',
      orderingKey: 'ordered',
      orderingSeq: '2',
    });
    await cleanupClient.zadd(keys.groupq(groupKey), [
      { element: missingId, score: 1 },
      { element: validId, score: 2 },
    ]);
    await cleanupClient.zadd(keys.ratelimited, [{ element: groupKey, score: now - 1 }]);

    expect(await promoteRateLimited(cleanupClient, keys, now)).toBe(1);
    expect(String(await cleanupClient.hget(keys.job(validId), 'state'))).toBe('waiting');
    expect(String(await cleanupClient.hget(keys.group(groupKey), 'nextSeq'))).toBe('2');
    expect(Number(await cleanupClient.xlen(keys.stream))).toBe(1);
    expect(Number(await cleanupClient.zcard(keys.ratelimited))).toBe(0);
  });

  it('lets a worker process the successor after discarding an ordered tombstone', async () => {
    const keys = buildKeys(queueName);
    const groupKey = 'worker-tombstone-group';
    const missingId = 'worker-missing';
    const validId = 'worker-valid';
    const now = Date.now();
    const processed: string[] = [];
    const worker = new Worker(
      queueName,
      async (job: any) => {
        processed.push(job.id);
        return 'done';
      },
      { connection: CONNECTION, blockTimeout: 50 },
    );
    worker.on('error', () => {});

    try {
      await worker.waitUntilReady();
      await cleanupClient.hset(keys.group(groupKey), {
        maxConcurrency: '1',
        active: '0',
        tbCapacity: '1000',
        tbTokens: '1000',
        tbRefillRate: '1000',
        tbLastRefill: String(now),
        tbRefillRemainder: '0',
        nextSeq: '1',
      });
      await cleanupClient.hset(keys.job(validId), {
        id: validId,
        name: 'valid',
        state: 'group-waiting',
        groupKey,
        cost: '1000',
        orderingKey: groupKey,
        orderingSeq: '2',
      });
      await cleanupClient.zadd(keys.groupq(groupKey), [
        { element: missingId, score: 1 },
        { element: validId, score: 2 },
      ]);
      await cleanupClient.zadd(keys.ratelimited, [{ element: groupKey, score: now - 1 }]);

      expect(await promoteRateLimited(cleanupClient, keys, now)).toBe(1);
      await waitFor(() => processed.includes(validId));
      expect(String(await cleanupClient.hget(keys.meta, `orderdone:${groupKey}`))).toBe('2');
    } finally {
      await worker.close(true);
    }
  }, 10000);

  it('re-registers a group when bounded tombstone cleanup has more work', async () => {
    const keys = buildKeys(queueName);
    const groupKey = 'many-tombstones';
    const validId = 'valid-after-many';
    const now = Date.now();

    await cleanupClient.hset(keys.group(groupKey), {
      maxConcurrency: '1',
      active: '0',
      tbCapacity: '1000',
      tbTokens: '1000',
      tbRefillRate: '1000',
      tbLastRefill: String(now),
      tbRefillRemainder: '0',
      nextSeq: '1',
    });
    const tombstones = Array.from({ length: 101 }, (_, i) => ({ element: `missing-${i + 1}`, score: i + 1 }));
    await cleanupClient.zadd(keys.groupq(groupKey), [...tombstones, { element: validId, score: 102 }]);
    await cleanupClient.hset(keys.job(validId), {
      id: validId,
      name: 'valid',
      state: 'group-waiting',
      groupKey,
      cost: '1000',
      orderingKey: 'ordered-many',
      orderingSeq: '102',
    });
    await cleanupClient.zadd(keys.ratelimited, [{ element: groupKey, score: now - 1 }]);

    expect(await promoteRateLimited(cleanupClient, keys, now)).toBe(0);
    expect(await cleanupClient.zscore(keys.ratelimited, groupKey)).not.toBeNull();
    expect(await promoteRateLimited(cleanupClient, keys, now)).toBe(1);
    expect(String(await cleanupClient.hget(keys.job(validId), 'state'))).toBe('waiting');
  });
});
