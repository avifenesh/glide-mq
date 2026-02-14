import type { Queue } from './queue';
import type { Worker } from './worker';
import type { QueueEvents } from './queue-events';
import type { FlowProducer } from './flow-producer';

type Closeable = Queue | Worker | QueueEvents | FlowProducer;

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
): Promise<void> {
  return new Promise<void>((resolve) => {
    let shutting = false;

    const handler = () => {
      if (shutting) return;
      shutting = true;

      Promise.allSettled(components.map((c) => c.close()))
        .then(() => resolve());
    };

    process.on('SIGTERM', handler);
    process.on('SIGINT', handler);
  });
}
