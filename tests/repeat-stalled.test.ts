import { afterAll, beforeAll, expect, it } from 'vitest';
import { createCleanupClient, describeEachMode, flushQueue } from './helpers/fixture';

const { moveToActive, reclaimStalled, sweepSuspended } = require('../dist/functions/index') as typeof import('../src/functions/index');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

describeEachMode('repeatAfterComplete stalled recovery', (CONNECTION) => {
  const queueName = `repeat-stalled-${Date.now()}`;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, queueName);
    cleanupClient.close();
  });

  it('advances the scheduler when stalled recovery terminally fails its job', async () => {
    const keys = buildKeys(queueName);
    const schedulerName = 'repeat-after-stall';
    const jobId = 'stalled-job';
    const now = Date.now();
    const config = {
      name: schedulerName,
      repeatAfterComplete: 250,
      nextRun: 0,
      lastRun: now - 1000,
      iterationCount: 1,
      template: { name: 'repeat-job', data: { externalId: 123456789012345, nextRun: 0 } },
    };

    await cleanupClient.hset(keys.schedulers, { [schedulerName]: JSON.stringify(config) });
    await cleanupClient.hset(keys.job(jobId), {
      id: jobId,
      name: 'repeat-job',
      state: 'active',
      schedulerName,
      processedOn: String(now - 1000),
      lastActive: '0',
    });
    await cleanupClient.xadd(keys.stream, [
      ['jobId', jobId],
      ['name', 'repeat-job'],
    ]);
    await cleanupClient.xgroupCreate(keys.stream, 'workers', '0', { mkStream: true });
    await cleanupClient.xreadgroup('workers', 'dead-worker', { [keys.stream]: '>' }, { count: 1 });

    expect(await reclaimStalled(cleanupClient, keys, 'scheduler', 0, 0, now)).toBe(1);
    const updatedRaw = await cleanupClient.hget(keys.schedulers, schedulerName);
    const updated = JSON.parse(String(updatedRaw));
    expect(updated.nextRun).toBe(now + 250);
    expect(updated.lastRun).toBe(config.lastRun);
    expect(updated.template.data.externalId).toBe(config.template.data.externalId);
    expect(updated.template.data.nextRun).toBe(0);
  });

  it('advances the scheduler when moveToActive terminally expires its job', async () => {
    const keys = buildKeys(queueName);
    const schedulerName = 'repeat-after-ttl';
    const jobId = 'ttl-job';
    const now = Date.now();
    const config = {
      name: schedulerName,
      repeatAfterComplete: 250,
      nextRun: 0,
      lastRun: now - 1000,
      iterationCount: 1,
      template: { name: 'repeat-job', data: { externalId: 987654321012345, nextRun: 0 } },
    };

    await cleanupClient.hset(keys.schedulers, { [schedulerName]: JSON.stringify(config) });
    await cleanupClient.hset(keys.job(jobId), {
      id: jobId,
      name: 'repeat-job',
      state: 'waiting',
      schedulerName,
      expireAt: String(now - 1),
    });
    const entryId = await cleanupClient.xadd(keys.stream, [
      ['jobId', jobId],
      ['name', 'repeat-job'],
    ]);
    await cleanupClient.xgroupCreate(keys.stream, 'workers-ttl', '0', { mkStream: true });

    expect(await moveToActive(cleanupClient, keys, jobId, now, keys.stream, String(entryId), 'workers-ttl')).toBe(
      'EXPIRED',
    );
    const updatedRaw = await cleanupClient.hget(keys.schedulers, schedulerName);
    const updated = JSON.parse(String(updatedRaw));
    expect(updated.nextRun).toBe(now + 250);
    expect(updated.lastRun).toBe(config.lastRun);
    expect(updated.template.data.externalId).toBe(config.template.data.externalId);
    expect(updated.template.data.nextRun).toBe(0);
  });

  it('advances the scheduler when a suspended job times out', async () => {
    const keys = buildKeys(queueName);
    const schedulerName = 'repeat-after-suspend-timeout';
    const jobId = 'suspended-job';
    const now = Date.now();
    const config = {
      name: schedulerName,
      repeatAfterComplete: 250,
      nextRun: 0,
      lastRun: now - 1000,
      iterationCount: 1,
      template: { name: 'repeat-job', data: { externalId: 123456789876543, nextRun: 0 } },
    };

    await cleanupClient.hset(keys.schedulers, { [schedulerName]: JSON.stringify(config) });
    await cleanupClient.hset(keys.job(jobId), {
      id: jobId,
      name: 'repeat-job',
      state: 'suspended',
      schedulerName,
      processedOn: String(now - 1000),
    });
    await cleanupClient.zadd(keys.suspended, [{ score: now - 1, element: jobId }]);

    expect(await sweepSuspended(cleanupClient, keys, now, keys.id.slice(0, -2))).toBe(1);
    const updatedRaw = await cleanupClient.hget(keys.schedulers, schedulerName);
    const updated = JSON.parse(String(updatedRaw));
    expect(updated.nextRun).toBe(now + 250);
    expect(updated.lastRun).toBe(config.lastRun);
    expect(updated.template.data.externalId).toBe(config.template.data.externalId);
    expect(updated.template.data.nextRun).toBe(0);
  });
});
