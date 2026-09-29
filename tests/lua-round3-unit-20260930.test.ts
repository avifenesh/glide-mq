/**
 * Unit tests (no Valkey) for the 2026-09-30 round 3 gaps.
 *
 * Run: npx vitest run tests/lua-round3-unit-20260930.test.ts
 */
import { describe, expect, it, vi } from 'vitest';
import path from 'path';
import { SandboxPool } from '../src/sandbox/pool';
import { SandboxJob } from '../src/sandbox/sandbox-job';
import type { Job } from '../src/job';

const PROCESSORS = path.resolve(__dirname, 'fixtures/processors');
const ABORT_WRITES_PROCESSOR = path.join(PROCESSORS, 'abort-writes.js');
const RUNNER_PATH = path.resolve(__dirname, '..', 'dist', 'sandbox', 'runner.js');

describe('sandbox writes after abort (item 10)', () => {
  it('SandboxJob refuses updateProgress, updateData and moveToDelayed once aborted, log still proxies', async () => {
    const sendMessage = vi.fn();
    const job = new SandboxJob(
      { id: '1', name: 'j', data: { step: 'a' }, opts: {}, attemptsMade: 0, timestamp: 0, progress: 0 },
      sendMessage,
      'inv',
    );
    job._abort();

    await expect(job.updateProgress(10)).rejects.toThrow('Job aborted');
    await expect(job.updateData({ step: 'b' })).rejects.toThrow('Job aborted');
    await expect(job.moveToDelayed(Date.now() + 1000)).rejects.toThrow('Job aborted');
    expect(sendMessage).not.toHaveBeenCalled();
    expect(job.progress).toBe(0);
    expect(job.data).toEqual({ step: 'a' });

    const logged = job.log('why it stopped');
    expect(sendMessage).toHaveBeenCalledTimes(1);
    const msg = sendMessage.mock.calls[0][0];
    expect(msg).toMatchObject({ type: 'proxy-request', method: 'log', args: ['why it stopped'] });
    job.handleProxyResponse({ type: 'proxy-response', id: msg.id, result: null });
    await expect(logged).resolves.toBeUndefined();
  });

  for (const useWorkerThreads of [true, false]) {
    const mode = useWorkerThreads ? 'worker thread' : 'fork mode';
    it(`the pool refuses state writes of an aborted job during the grace window (${mode})`, async () => {
      const pool = new SandboxPool(ABORT_WRITES_PROCESSOR, useWorkerThreads, 1, RUNNER_PATH, 5000);
      const ac = new AbortController();
      const log = vi.fn().mockResolvedValue(undefined);
      const updateProgress = vi.fn().mockResolvedValue(undefined);
      const updateData = vi.fn().mockResolvedValue(undefined);
      const job = {
        id: 'aborted-1',
        name: 'test',
        data: {},
        opts: {},
        attemptsMade: 0,
        timestamp: Date.now(),
        progress: 0,
        abortSignal: ac.signal,
        log,
        updateProgress,
        updateData,
      } as unknown as Job;
      try {
        const run = pool.run(job);
        await vi.waitFor(() => expect(log).toHaveBeenCalledWith('started'), { timeout: 10_000 });
        ac.abort();
        const outcome = (await run) as Record<string, string>;
        expect(outcome.updateProgress).toMatch(/Job aborted/);
        expect(outcome.updateData).toMatch(/Job aborted/);
        expect(outcome.log).toBe('ok');
        expect(updateProgress).not.toHaveBeenCalled();
        expect(updateData).not.toHaveBeenCalled();
        expect(log).toHaveBeenCalledWith('aborted');
      } finally {
        await pool.close(true);
      }
    }, 30_000);
  }
});
