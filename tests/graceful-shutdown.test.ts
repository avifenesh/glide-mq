/**
 * Unit tests for gracefulShutdown utility.
 * No Valkey connection needed - uses mock closeable objects.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { gracefulShutdown } from '../src/graceful-shutdown';

function makeMockComponent(overrides: Partial<{ close: () => Promise<void> }> = {}) {
  return {
    close: vi.fn().mockResolvedValue(undefined),
    ...overrides,
  } as unknown as Parameters<typeof gracefulShutdown>[0][number];
}

describe('gracefulShutdown', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    // Clean up any leftover signal listeners
  });

  it('shutdown() calls close on all components', async () => {
    const a = makeMockComponent();
    const b = makeMockComponent();
    const c = makeMockComponent();

    const handle = gracefulShutdown([a, b, c]);
    await handle.shutdown();

    expect(a.close).toHaveBeenCalledTimes(1);
    expect(b.close).toHaveBeenCalledTimes(1);
    expect(c.close).toHaveBeenCalledTimes(1);
  });

  it('calling shutdown() twice is idempotent (close called once per component)', async () => {
    const a = makeMockComponent();

    const handle = gracefulShutdown([a]);
    await handle.shutdown();
    await handle.shutdown();

    expect(a.close).toHaveBeenCalledTimes(1);
  });

  it('dispose() prevents repeated signal handling (resolves promise immediately)', async () => {
    const a = makeMockComponent();

    const handle = gracefulShutdown([a]);
    handle.dispose();

    // After dispose, the promise should be resolved
    await expect(handle).resolves.toBeUndefined();

    // shutdown after dispose still runs (shutdownPromise is separate)
    // but close is never called unless shutdown() is explicitly called
    expect(a.close).not.toHaveBeenCalled();
  });

  it('component close() error does not prevent others from closing', async () => {
    const a = makeMockComponent({ close: vi.fn().mockRejectedValue(new Error('close failed')) as any });
    const b = makeMockComponent();
    const c = makeMockComponent({ close: vi.fn().mockRejectedValue(new Error('also failed')) as any });

    const handle = gracefulShutdown([a, b, c]);
    // shutdown uses allSettled so it should not throw
    await handle.shutdown();

    expect(a.close).toHaveBeenCalledTimes(1);
    expect(b.close).toHaveBeenCalledTimes(1);
    expect(c.close).toHaveBeenCalledTimes(1);
  });

  it('a second signal during a hung shutdown re-raises it instead of swallowing it', async () => {
    const hung = makeMockComponent({ close: vi.fn().mockReturnValue(new Promise(() => {})) as any });
    const killSpy = vi.spyOn(process, 'kill').mockImplementation(() => true);
    const sigintBefore = process.listenerCount('SIGINT');
    try {
      gracefulShutdown([hung]);
      expect(process.listenerCount('SIGINT')).toBe(sigintBefore + 1);

      process.emit('SIGINT', 'SIGINT');
      expect(hung.close).toHaveBeenCalledTimes(1);
      expect(killSpy).not.toHaveBeenCalled();

      process.emit('SIGINT', 'SIGINT');
      expect(killSpy).toHaveBeenCalledWith(process.pid, 'SIGINT');
      expect(process.listenerCount('SIGINT')).toBe(sigintBefore);
      expect(hung.close).toHaveBeenCalledTimes(1);
    } finally {
      killSpy.mockRestore();
    }
  });

  it('the first signal after handle.shutdown() does not escalate', async () => {
    const hung = makeMockComponent({ close: vi.fn().mockReturnValue(new Promise(() => {})) as any });
    const killSpy = vi.spyOn(process, 'kill').mockImplementation(() => true);
    const sigtermBefore = process.listenerCount('SIGTERM');
    try {
      const handle = gracefulShutdown([hung]);
      void handle.shutdown();
      expect(hung.close).toHaveBeenCalledTimes(1);

      process.emit('SIGTERM', 'SIGTERM');
      expect(killSpy).not.toHaveBeenCalled();
      expect(process.listenerCount('SIGTERM')).toBe(sigtermBefore + 1);

      process.emit('SIGTERM', 'SIGTERM');
      expect(killSpy).toHaveBeenCalledWith(process.pid, 'SIGTERM');
      expect(hung.close).toHaveBeenCalledTimes(1);
    } finally {
      killSpy.mockRestore();
    }
  });
});
