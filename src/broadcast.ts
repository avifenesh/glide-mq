import { EventEmitter } from 'events';
import type { BroadcastOptions, JobOptions, Client, RateLimitConfig } from './types';
import { Queue } from './queue';
import { buildKeys } from './utils';
import type { QueueKeys } from './functions/index';

/**
 * Broadcast - Fan-out message publisher for pub/sub patterns.
 *
 * Unlike Queue (point-to-point), Broadcast delivers each message to multiple
 * independent subscribers. Each subscriber uses a separate consumer group on
 * the same Redis Stream.
 *
 * Example:
 * ```typescript
 * const broadcast = new Broadcast('events', { connection });
 *
 * const worker1 = new BroadcastWorker('events', async (msg) => {
 *   // Process message
 * }, { connection, subscription: 'inventory-service' });
 *
 * const worker2 = new BroadcastWorker('events', async (msg) => {
 *   // Process message
 * }, { connection, subscription: 'email-service' });
 *
 * await broadcast.publish({ event: 'order.placed', data: {...} });
 * // Both workers receive the message
 * ```
 */
export class Broadcast<D = any> extends EventEmitter {
  readonly name: string;
  private opts: BroadcastOptions;
  private queue: Queue<D, any>;
  readonly keys: QueueKeys;

  constructor(name: string, opts: BroadcastOptions) {
    super();
    this.name = name;
    this.opts = opts;
    this.keys = buildKeys(name, opts.prefix);

    // Delegate to internal Queue for all core functionality
    this.queue = new Queue(name, opts);

    // Forward queue events
    this.queue.on('error', (err) => this.emit('error', err));
  }

  /**
   * Publish a message to all subscribers.
   * Each subscriber (consumer group) receives a copy.
   *
   * @param data - Message data
   * @param opts - Job options (delay, priority, dedup, etc.)
   * @returns Message ID or null if skipped (e.g., due to dedup)
   */
  async publish(data: D, opts?: JobOptions): Promise<string | null> {
    const job = await this.queue.add('message', data, opts);

    // Trim stream to maxMessages if configured
    if (job && this.opts.maxMessages) {
      const client = await this.queue.getClient();
      await client.xtrim(this.keys.stream, {
        method: 'maxlen',
        threshold: this.opts.maxMessages,
        exact: false,
      });
    }

    return job ? job.id : null;
  }

  /**
   * Set global rate limit for all subscribers.
   *
   * @param config - Rate limit configuration { max, duration } or null to remove
   */
  async setGlobalRateLimit(config: RateLimitConfig | null): Promise<void> {
    if (config === null) {
      return this.queue.removeGlobalRateLimit();
    }
    return this.queue.setGlobalRateLimit(config);
  }

  /**
   * Pause message publication (delayed/scheduled messages won't be promoted).
   */
  async pause(): Promise<void> {
    return this.queue.pause();
  }

  /**
   * Resume message publication.
   */
  async resume(): Promise<void> {
    return this.queue.resume();
  }

  /**
   * Get the underlying client for advanced operations.
   */
  async getClient(): Promise<Client> {
    return this.queue.getClient();
  }

  /**
   * Close the broadcast publisher and release connections.
   */
  async close(): Promise<void> {
    return this.queue.close();
  }
}
