import { describe, expect, it, vi } from 'vitest';
import { Scheduler } from '../src/scheduler';
import { buildKeys } from '../src/utils';

describe('Scheduler internals', () => {
  it('abandons scheduler writes when the tick lock is lost before commit', async () => {
    const exec = vi.fn(async () => undefined);
    const client = {
      fcall: vi.fn(async (name: string) => {
        if (name === 'glidemq_tryLock') return 1;
        if (name === 'glidemq_renewLock') return 0;
        if (name === 'glidemq_unlock') return 0;
        throw new Error(`Unexpected function call: ${name}`);
      }),
      hgetall: vi.fn(async () => [
        {
          field: 'due-scheduler',
          value: JSON.stringify({
            every: 1_000,
            nextRun: Date.now() - 1,
            template: { name: 'tick', data: { ok: true } },
          }),
        },
      ]),
      exec,
    } as any;

    const errors: Error[] = [];
    const scheduler = new Scheduler(client, buildKeys('scheduler-lock-loss'), {
      promotionInterval: 5_000,
      onError: (err) => errors.push(err),
    });

    const fired = await scheduler.runSchedulers();

    expect(fired).toBe(0);
    expect(exec).not.toHaveBeenCalled();
    expect(errors.map((err) => err.message)).toContain('Lost scheduler tick lock while processing schedulers');
  });

  it('continues a full stalled reclaim page without overlapping recovery', async () => {
    vi.useFakeTimers();
    let resolveFirstReclaim!: (count: number) => void;
    let reclaimCalls = 0;
    const client = {
      fcall: vi.fn((name: string) => {
        if (name === 'glidemq_reclaimStalled') {
          reclaimCalls++;
          if (reclaimCalls === 1) return new Promise<number>((resolve) => (resolveFirstReclaim = resolve));
          return Promise.resolve(0);
        }
        if (name === 'glidemq_nextDue') return Promise.resolve(-1);
        return Promise.resolve(0);
      }),
    } as any;
    const scheduler = new Scheduler(client, buildKeys('scheduler-stalled-continuation'), {
      promotionInterval: 60_000,
      stalledInterval: 60_000,
    });

    try {
      scheduler.start();
      await vi.advanceTimersByTimeAsync(60_000);
      expect(reclaimCalls).toBe(1);

      resolveFirstReclaim(100);
      await vi.advanceTimersByTimeAsync(0);
      expect(reclaimCalls).toBe(2);
    } finally {
      scheduler.stop();
      await scheduler.waitForIdle();
      vi.useRealTimers();
    }
  });
});
