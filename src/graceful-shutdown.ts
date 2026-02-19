import type { Queue } from './queue';
import type { Worker } from './worker';
import type { QueueEvents } from './queue-events';
import type { FlowProducer } from './flow-producer';

type Closeable = Queue | Worker | QueueEvents | FlowProducer;
export type GracefulShutdownHandle = Promise<void> & {
  shutdown: () => Promise<void>;
  dispose: () => void;
};

/**
 * Register SIGTERM and SIGINT handlers that gracefully close all provided components.
 * Returns a Promise that resolves when all components have been closed.
 *
 * Usage:
 *   const shutdown = gracefulShutdown([queue, worker, queueEvents]);
 *   // ... later, on signal or manually:
 *   await shutdown;
 */
export function gracefulShutdown(
  components: Closeable[],
): GracefulShutdownHandle {
  let done = false;
  let shutdownPromise: Promise<void> | null = null;

  let resolvePromise!: () => void;
  const promise = new Promise<void>((resolve) => {
    resolvePromise = resolve;
  });

  const finish = () => {
    if (done) return;
    done = true;
    process.off('SIGTERM', onSignal);
    process.off('SIGINT', onSignal);
    resolvePromise();
  };

  const shutdown = (): Promise<void> => {
    if (shutdownPromise) return shutdownPromise;
    shutdownPromise = (async () => {
      await Promise.allSettled(components.map((c) => c.close()));
      finish();
    })();
    return shutdownPromise;
  };

  const onSignal = () => {
    void shutdown();
  };

  process.on('SIGTERM', onSignal);
  process.on('SIGINT', onSignal);

  const handle = promise as GracefulShutdownHandle;
  handle.shutdown = shutdown;
  handle.dispose = finish;
  return handle;
}
