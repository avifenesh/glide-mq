import { afterAll, beforeAll, expect, it } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
const { CONSUMER_GROUP, promote } = require('../dist/functions/index') as typeof import('../src/functions/index');
const { RequestError } = require('@glidemq/speedkey') as typeof import('@glidemq/speedkey');

import { createCleanupClient, describeEachMode, flushQueue } from './helpers/fixture';

describeEachMode('Queue.getJobs waiting sources', (CONNECTION) => {
  const queueName = `get-jobs-waiting-${Date.now()}`;
  let cleanupClient: any;
  let queue: InstanceType<typeof Queue>;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
    queue = new Queue(queueName, { connection: CONNECTION });
  });

  afterAll(async () => {
    await queue?.close();
    if (cleanupClient) {
      await flushQueue(cleanupClient, queueName);
      await cleanupClient.close();
    }
  });

  it('includes priority and LIFO jobs while excluding FIFO jobs pending in a worker', async () => {
    const keys = buildKeys(queueName);
    const fifoActive = await queue.add('fifo-active', {});
    const fifoWaiting = await queue.add('fifo-waiting', {});
    const lifoFirst = await queue.add('lifo-first', {}, { lifo: true });
    const lifoSecond = await queue.add('lifo-second', {}, { lifo: true });
    const priorityFirst = await queue.add('priority-first', {}, { priority: 1 });
    const prioritySecond = await queue.add('priority-second', {}, { priority: 2 });

    await promote(cleanupClient, keys, Number.MAX_SAFE_INTEGER);
    await cleanupClient.xgroupCreate(keys.stream, CONSUMER_GROUP, '0');
    await cleanupClient.xreadgroup(CONSUMER_GROUP, 'get-jobs-waiting-test', { [keys.stream]: '>' }, { count: 1 });

    const jobs = await queue.getJobs('waiting', 1, 4);

    expect(jobs.map((job) => job.id)).toEqual([prioritySecond.id, lifoSecond.id, lifoFirst.id, fifoWaiting.id]);
    expect(jobs.map((job) => job.id)).not.toContain(fifoActive.id);
    expect((await queue.getJobs('waiting', 0, 0)).map((job) => job.id)).toEqual([priorityFirst.id]);
  });

  it('paginates the stream and treats only NOGROUP as an empty PEL', async () => {
    const entries: Record<string, [string, string][]> = {};
    const pending: [string, string, number, number][] = [];
    for (let i = 0; i < 1000; i++) {
      const entryId = `${i + 1}-0`;
      entries[entryId] = [['jobId', `pending-${i}`]];
      pending.push([entryId, 'consumer', 0, 1]);
    }

    let page = 0;
    const pagedClient = {
      xrange: async () => (page++ === 0 ? entries : { '1001-0': [['jobId', 'waiting-final']] }),
      xpendingWithOptions: async () => (page === 1 ? pending : []),
    };
    expect(await (queue as any).getWaitingStreamJobIds(pagedClient, -1)).toEqual(['waiting-final']);

    const noGroupClient = {
      xrange: async () => ({ '1-0': [['jobId', 'waiting-without-group']] }),
      xpendingWithOptions: async () => {
        throw new RequestError('NOGROUP: No such key or consumer group');
      },
    };
    expect(await (queue as any).getWaitingStreamJobIds(noGroupClient, -1)).toEqual(['waiting-without-group']);
  });

  it('removes revoked and deleted jobs from list-backed waiting sources', async () => {
    const isolatedName = `${queueName}-stale-list`;
    const isolatedQueue = new Queue(isolatedName, { connection: CONNECTION });
    const revoked = await isolatedQueue.add('revoked-priority', {}, { priority: 1 });
    const removed = await isolatedQueue.add('removed-lifo', {}, { lifo: true });
    const waiting = await isolatedQueue.add('waiting-priority', {}, { priority: 2 });
    await promote(cleanupClient, buildKeys(isolatedName), Number.MAX_SAFE_INTEGER);

    expect(await isolatedQueue.revoke(revoked!.id)).toBe('revoked');
    await removed!.remove();

    expect((await isolatedQueue.getJobs('waiting')).map((job) => job.id)).toEqual([waiting!.id]);

    await isolatedQueue.close();
    await flushQueue(cleanupClient, isolatedName);
  });
});
