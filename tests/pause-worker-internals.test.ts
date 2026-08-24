import { describe, expect, it, vi } from 'vitest';
import { BaseWorker } from '../src/base-worker';
import { BroadcastWorker } from '../src/broadcast-worker';
import { deferActive, moveToActive } from '../src/functions';
import { buildKeys } from '../src/utils';
import { Worker } from '../src/worker';

const entry = [
  {
    value: {
      '1-0': [
        ['jobId', 'one'],
        ['name', 'event'],
      ],
    },
  },
] as any;

describe('pause worker internals', () => {
  it('preserves the paused activation contract for workers and function callers', async () => {
    const keys = buildKeys('pause-worker-internals');
    const fcall = vi.fn().mockResolvedValue(1);
    const worker = {
      commandClient: { fcall },
      queueKeys: keys,
      consumerGroup: 'workers',
      broadcastMode: false,
      queuePaused: false,
      emit: vi.fn(),
    };
    (worker as any).deferPausedActivation = (entry: { jobId: string; entryId: string }) =>
      (BaseWorker.prototype as any).deferPausedActivation.call(worker, entry);

    await expect(
      (BaseWorker.prototype as any).handleMoveToActiveEdgeCase.call(worker, 'PAUSED', 'job-1', ''),
    ).resolves.toBe(true);
    expect(worker.queuePaused).toBe(true);
    expect(fcall).toHaveBeenCalledWith(
      'glidemq_deferActive',
      [keys.stream, keys.job('job-1'), keys.listActive],
      ['job-1', '', 'workers', '0', '1'],
    );

    const pausedClient = { fcall: vi.fn().mockResolvedValue('PAUSED') } as any;
    await expect(moveToActive(pausedClient, keys, 'job-2', Date.now(), keys.stream, '', 'workers')).resolves.toBe(
      'PAUSED',
    );

    await deferActive({ fcall } as any, keys, 'job-3', '1-0', 'workers', true, true);
    expect(fcall).toHaveBeenLastCalledWith(
      'glidemq_deferActive',
      [keys.stream, keys.job('job-3'), keys.listActive],
      ['job-3', '1-0', 'workers', '1', '1'],
    );
  });

  it('waits while paused without rewinding broadcast pending reads after resume', async () => {
    const keys = buildKeys('pause-meta-flags');
    const pausedWorker = {
      queuePaused: true,
      refreshMetaFlags: vi.fn(),
    };
    await expect((BaseWorker.prototype as any).waitIfQueuePaused.call(pausedWorker)).resolves.toBe(true);
    expect(pausedWorker.refreshMetaFlags).toHaveBeenCalledOnce();

    const resumedWorker = {
      queuePaused: true,
      refreshMetaFlags: vi.fn(async function (this: any) {
        this.queuePaused = false;
      }),
    };
    await expect((BaseWorker.prototype as any).waitIfQueuePaused.call(resumedWorker)).resolves.toBe(false);

    const resumedBroadcastWorker = {
      commandClient: { hmget: vi.fn().mockResolvedValue([null, null, null, '0']) },
      queueKeys: keys,
      globalConcurrencyEnabled: true,
      globalRateLimitEnabled: true,
      cachedRateLimitMax: 1,
      cachedRateLimitDuration: 1,
      queuePaused: true,
      broadcastMode: true,
      xreadStreams: { [keys.stream]: '>' },
    };
    await (BaseWorker.prototype as any).refreshMetaFlags.call(resumedBroadcastWorker);
    expect(resumedBroadcastWorker.queuePaused).toBe(false);
    expect(resumedBroadcastWorker.xreadStreams[keys.stream]).toBe('>');
  });

  it('does not claim after a pause guard and keeps broadcast reads on new entries', async () => {
    const keys = buildKeys('pause-poll-guards');
    const tryPopFromLists = vi.fn();
    const worker = {
      blockingClient: { xreadgroup: vi.fn() },
      commandClient: {},
      paused: false,
      closing: false,
      queuePaused: true,
      waitIfQueuePaused: vi.fn().mockResolvedValue(false),
      prefetch: 1,
      activeCount: 0,
      batchMode: false,
      globalConcurrencyEnabled: false,
      tryPopFromLists,
    };
    await (Worker.prototype as any).pollOnce.call(worker);
    expect(tryPopFromLists).not.toHaveBeenCalled();
    expect(worker.blockingClient.xreadgroup).not.toHaveBeenCalled();

    const secondListCheck = vi.fn().mockResolvedValue(false);
    const listFallbackWorker = {
      ...worker,
      queuePaused: false,
      tryPopFromLists: vi.fn().mockResolvedValue(false),
      blockingClient: { xreadgroup: vi.fn().mockResolvedValue(null) },
      isDrained: true,
    };
    listFallbackWorker.tryPopFromLists
      .mockImplementationOnce(async () => false)
      .mockImplementationOnce(secondListCheck);
    await (Worker.prototype as any).pollOnce.call(listFallbackWorker);
    expect(secondListCheck).toHaveBeenCalledOnce();

    const xreadgroup = vi.fn().mockResolvedValue(null);
    const broadcastWorker = {
      blockingClient: { xreadgroup },
      commandClient: {},
      paused: false,
      closing: false,
      waitIfQueuePaused: vi.fn().mockResolvedValue(false),
      prefetch: 1,
      activeCount: 0,
      globalConcurrencyEnabled: false,
      queueKeys: keys,
      xreadStreams: { [keys.stream]: '>' },
      consumerGroup: 'sub',
      consumerId: 'consumer',
      blockTimeout: 0,
      isDrained: true,
      recoverPausedBroadcastEntries: vi.fn().mockResolvedValue(null),
    };
    await (BroadcastWorker.prototype as any).pollOnce.call(broadcastWorker);
    expect(xreadgroup).toHaveBeenCalledOnce();
    expect(broadcastWorker.xreadStreams[keys.stream]).toBe('>');
  });

  it('stops batch refills once pause metadata is observed', async () => {
    const keys = buildKeys('pause-batch-refill');
    const activateAndProcessBatch = vi.fn();
    const xreadgroup = vi.fn();
    const broadcastBatchWorker = {
      commandClient: {},
      blockingClient: { xreadgroup },
      batchSize: 2,
      batchTimeout: 100,
      running: true,
      closing: false,
      queuePaused: false,
      refreshMetaFlags: vi.fn(async function (this: any) {
        this.queuePaused = true;
      }),
      consumerGroup: 'sub',
      consumerId: 'consumer',
      xreadStreams: { [keys.stream]: '>' },
      blockTimeout: 10,
      subjectMatcher: null,
      activateAndProcessBatch,
    };
    await (BroadcastWorker.prototype as any).collectAndProcessBatch.call(broadcastBatchWorker, entry);
    expect(xreadgroup).not.toHaveBeenCalled();
    expect(activateAndProcessBatch).toHaveBeenCalledWith([{ jobId: 'one', entryId: '1-0' }]);

    const workerBatchWorker = {
      ...broadcastBatchWorker,
      queuePaused: false,
      refreshMetaFlags: vi.fn(async function (this: any) {
        this.queuePaused = true;
      }),
      activateAndProcessBatch: vi.fn(),
    };
    await (Worker.prototype as any).collectAndProcessBatch.call(workerBatchWorker, entry);
    expect(workerBatchWorker.activateAndProcessBatch).toHaveBeenCalledWith([{ jobId: 'one', entryId: '1-0' }]);
  });
});
