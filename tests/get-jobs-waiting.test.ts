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

    let streamPage = 0;
    let pelPage = 0;
    const pagedClient = {
      xrange: async () => (streamPage++ === 0 ? entries : { '1001-0': [['jobId', 'waiting-final']] }),
      xpendingWithOptions: async () => (pelPage++ === 0 ? pending : []),
    };
    expect(await (queue as any).getWaitingStreamJobIds(pagedClient, -1)).toEqual(['waiting-final']);

    const noGroupClient = {
      xrange: async () => ({ '1-0': [['jobId', 'waiting-without-group']] }),
      xpendingWithOptions: async () => {
        throw new RequestError('NOGROUP: No such key or consumer group');
      },
    };
    expect(await (queue as any).getWaitingStreamJobIds(noGroupClient, -1)).toEqual(['waiting-without-group']);

    const deletedPending = Array.from({ length: 1000 }, (_, index) => [`${index + 2}-0`, 'consumer', 0, 1]);
    let deletedPelPage = 0;
    const deletedPelClient = {
      xrange: async () => ({
        '1-0': [['jobId', 'waiting-live']],
        '2000-0': [['jobId', 'active-live']],
      }),
      xpendingWithOptions: async () => (deletedPelPage++ === 0 ? deletedPending : [['2000-0', 'consumer', 0, 1]]),
    };
    expect(await (queue as any).getWaitingStreamJobIds(deletedPelClient, -1)).toEqual(['waiting-live']);
  });

  it('reads a full stream chunk when a limited page starts with PEL entries', async () => {
    const ids = ['1-0', '2-0', '3-0', '4-0', '5-0', '6-0'];
    const pendingIds = new Set(ids.slice(0, 3));
    const xrangeCounts: number[] = [];
    const chunkedClient = {
      xrange: async (
        _key: string,
        start: '-' | { value: string; isInclusive: false },
        _end: '+',
        options: { count: number },
      ) => {
        xrangeCounts.push(options.count);
        const startId = typeof start === 'string' ? undefined : start.value;
        const startIndex = startId == null ? 0 : ids.indexOf(startId) + 1;
        const pageIds = ids.slice(startIndex, startIndex + options.count);
        return Object.fromEntries(pageIds.map((id) => [id, [['jobId', id]]]));
      },
      xpendingWithOptions: async (
        _key: string,
        _group: string,
        options: { start: { value: string }; end: { value: string } },
      ) =>
        ids
          .filter((id) => pendingIds.has(id))
          .filter((id) => id >= options.start.value && id <= options.end.value)
          .map((id) => [id, 'consumer', 0, 1] as [string, string, number, number]),
    };

    const result = await (queue as any).getWaitingStreamJobIds(chunkedClient, 1);
    expect(result).toEqual(['4-0']);
    expect(result).toHaveLength(1);
    expect(xrangeCounts).toEqual([1000]);
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

  it('removes list-backed waiting jobs without scanning the FIFO stream', async () => {
    const isolatedName = `${queueName}-remove-list-no-scan`;
    const isolatedQueue = new Queue(isolatedName, { connection: CONNECTION });
    try {
      const listJob = await isolatedQueue.add('remove-list', {}, { lifo: true });
      const keys = buildKeys(isolatedName);

      // A FIFO scan would raise WRONGTYPE. A list-backed removal must not touch this key.
      await cleanupClient.set(keys.stream, 'list-backed-sentinel');
      await expect(listJob!.remove()).resolves.toBeUndefined();
    } finally {
      await isolatedQueue.close();
      await flushQueue(cleanupClient, isolatedName);
    }
  });

  it('removes a stream-backed job after clearing a delayed LIFO job', async () => {
    const isolatedName = `${queueName}-remove-cleared-lifo-delay`;
    const isolatedQueue = new Queue(isolatedName, { connection: CONNECTION });
    try {
      const job = await isolatedQueue.add('cleared-lifo-delay', {}, { delay: 60_000, lifo: true });
      const keys = buildKeys(isolatedName);

      await job!.changeDelay(0);
      expect(await cleanupClient.xlen(keys.stream)).toBe(1);

      await job!.remove();
      expect(await cleanupClient.xlen(keys.stream)).toBe(0);
      expect(await isolatedQueue.getJobs('waiting')).toEqual([]);
      expect((await isolatedQueue.getJobCounts()).waiting).toBe(0);
    } finally {
      await isolatedQueue.close();
      await flushQueue(cleanupClient, isolatedName);
    }
  });

  it('revokes non-FIFO jobs without scanning the FIFO stream', async () => {
    const cases = [
      { suffix: 'waiting-lifo', options: { lifo: true }, promote: false },
      { suffix: 'waiting-priority', options: { priority: 1 }, promote: true },
      { suffix: 'delayed', options: { delay: 60_000 }, promote: false },
      { suffix: 'prioritized', options: { priority: 1 }, promote: false },
    ];

    for (const testCase of cases) {
      const isolatedName = `${queueName}-revoke-no-scan-${testCase.suffix}`;
      const isolatedQueue = new Queue(isolatedName, { connection: CONNECTION });
      try {
        const job = await isolatedQueue.add('revoke-no-scan', {}, testCase.options);
        const keys = buildKeys(isolatedName);
        if (testCase.promote) {
          await promote(cleanupClient, keys, Number.MAX_SAFE_INTEGER);
        }

        // A FIFO scan would raise WRONGTYPE. These jobs cannot have a stream entry.
        await cleanupClient.set(keys.stream, 'non-fifo-sentinel');
        await expect(isolatedQueue.revoke(job!.id)).resolves.toBe('revoked');
      } finally {
        await isolatedQueue.close();
        await flushQueue(cleanupClient, isolatedName);
      }
    }
  });

  it('revokes a stream-backed job after clearing a delayed LIFO job', async () => {
    const isolatedName = `${queueName}-revoke-cleared-lifo-delay`;
    const isolatedQueue = new Queue(isolatedName, { connection: CONNECTION });
    try {
      const job = await isolatedQueue.add('cleared-lifo-delay', {}, { delay: 60_000, lifo: true });
      const keys = buildKeys(isolatedName);

      await job!.changeDelay(0);
      expect(await cleanupClient.xlen(keys.stream)).toBe(1);

      expect(await isolatedQueue.revoke(job!.id)).toBe('revoked');
      expect(await cleanupClient.xlen(keys.stream)).toBe(0);
    } finally {
      await isolatedQueue.close();
      await flushQueue(cleanupClient, isolatedName);
    }
  });

  it('removes a waiting FIFO stream entry with the legacy seven-key call shape', async () => {
    const isolatedName = `${queueName}-stale-stream`;
    const isolatedQueue = new Queue(isolatedName, { connection: CONNECTION });
    const removed = await isolatedQueue.add('removed-fifo', {});
    const waiting = await isolatedQueue.add('waiting-fifo', {});
    const keys = buildKeys(isolatedName);

    await cleanupClient.fcall(
      'glidemq_removeJob',
      [
        keys.job(removed!.id),
        keys.stream,
        keys.scheduled,
        keys.completed,
        keys.failed,
        keys.events,
        keys.log(removed!.id),
      ],
      [removed!.id],
    );

    expect((await isolatedQueue.getJobs('waiting')).map((job) => job.id)).toEqual([waiting!.id]);

    await isolatedQueue.close();
    await flushQueue(cleanupClient, isolatedName);
  });

  it('paginates both the stream and PEL when the first 1000 entries are pending', async () => {
    const isolatedName = `${queueName}-paged-pel`;
    const isolatedQueue = new Queue(isolatedName, { connection: CONNECTION });
    const keys = buildKeys(isolatedName);
    const jobs = await isolatedQueue.addBulk(
      Array.from({ length: 1001 }, (_, index) => ({ name: 'paged-fifo', data: { index } })),
    );

    await cleanupClient.xgroupCreate(keys.stream, CONSUMER_GROUP, '0');
    await cleanupClient.xreadgroup(CONSUMER_GROUP, 'paged-pel-test', { [keys.stream]: '>' }, { count: 1000 });

    expect((await isolatedQueue.getJobs('waiting')).map((job) => job.id)).toEqual([jobs[1000]!.id]);

    await isolatedQueue.close();
    await flushQueue(cleanupClient, isolatedName);
  });
});
