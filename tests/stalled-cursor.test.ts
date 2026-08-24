import { afterAll, beforeAll, expect, it } from 'vitest';
import { Scheduler } from '../dist/scheduler';
import { createCleanupClient, describeEachMode, flushQueue, waitFor } from './helpers/fixture';

const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

describeEachMode('Stalled reclaim cursor', (CONNECTION) => {
  let cleanupClient: any;
  const queueName = `stalled-cursor-${Date.now()}`;
  const group = 'broadcast-cursor';
  const jobCount = 101;
  const stalledInterval = 60_000;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    if (!cleanupClient) return;
    await flushQueue(cleanupClient, queueName);
    await cleanupClient.close();
  });

  it('continues a full reclaim page before the next stalled interval', async () => {
    const keys = buildKeys(queueName);
    const jobIds = Array.from({ length: jobCount }, (_, index) => `job-${index}`);
    const entryIds: string[] = [];

    for (const jobId of jobIds) {
      await cleanupClient.hset(keys.job(jobId), {
        id: jobId,
        name: 'stalled',
        state: 'active',
        lastActive: '0',
      });
      entryIds.push(
        String(
          await cleanupClient.xadd(keys.stream, [
            ['jobId', jobId],
            ['name', 'stalled'],
          ]),
        ),
      );
    }
    await cleanupClient.xgroupCreate(keys.stream, group, '0', { mkStream: true });
    const claimed = await cleanupClient.xreadgroup(group, 'stalled-owner', { [keys.stream]: '>' }, { count: jobCount });
    expect(claimed).not.toBeNull();
    await cleanupClient.xclaim(keys.stream, group, 'stalled-owner', 0, entryIds, {
      idle: stalledInterval,
    });

    const scheduler = new Scheduler(cleanupClient, keys, {
      stalledInterval,
      lockDuration: stalledInterval,
      maxStalledCount: 10,
      consumerGroup: group,
      broadcastMode: true,
    });
    scheduler.start();

    try {
      await waitFor(
        async () => (await cleanupClient.hget(keys.job(jobIds[jobCount - 1]), 'stalledCount')) === '1',
        5000,
        25,
      );
      expect(Number(await cleanupClient.hget(keys.job(jobIds[0]), 'stalledCount'))).toBe(1);
    } finally {
      scheduler.stop();
      await scheduler.waitForIdle();
    }
  });
});
