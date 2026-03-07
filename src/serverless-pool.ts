/**
 * ServerlessPool - Connection reuse for warm Lambda/Edge invocations.
 *
 * Caches Producer instances by queue name + connection fingerprint so that
 * warm invocations reuse existing connections instead of creating new ones.
 */
import { Producer } from './producer';
import type { ProducerOptions } from './producer';

function fingerprint(name: string, opts: ProducerOptions): string {
  const addresses = opts.connection?.addresses ?? [];
  const sorted = [...addresses].sort((a, b) => {
    const hostCmp = a.host.localeCompare(b.host);
    return hostCmp !== 0 ? hostCmp : a.port - b.port;
  });
  // Include serializer name if custom, otherwise use default marker
  const serializerKey = opts.serializer ? 'custom' : 'json';
  return JSON.stringify({
    name,
    prefix: opts.prefix ?? 'glide',
    addresses: sorted,
    clusterMode: opts.connection?.clusterMode ?? false,
    compression: opts.compression ?? 'none',
    serializer: serializerKey,
    useTLS: opts.connection?.useTLS ?? false,
  });
}

export class ServerlessPool {
  private cache = new Map<string, Producer>();

  /**
   * Get or create a Producer for the given queue name and options.
   * Returns a cached instance if one exists with matching connection parameters.
   *
   * Note: Injected clients (opts.client) bypass caching to prevent collisions
   * where different client instances could map to the same cache key.
   */
  getProducer<D = any>(name: string, opts: ProducerOptions): Producer<D> {
    // Bypass caching for injected clients to prevent cache collisions
    if (opts.client) {
      return new Producer(name, opts);
    }

    const key = fingerprint(name, opts);
    let producer = this.cache.get(key);
    if (!producer) {
      producer = new Producer(name, opts);
      this.cache.set(key, producer);
    }
    return producer as Producer<D>;
  }

  /**
   * Close all cached producers and clear the cache.
   * Call this during Lambda SIGTERM or explicit cleanup.
   */
  async closeAll(): Promise<void> {
    const producers = [...this.cache.values()];
    this.cache.clear();
    await Promise.all(producers.map((p) => p.close()));
  }

  /** Number of cached producers. */
  get size(): number {
    return this.cache.size;
  }
}

/** Module-level singleton for convenient use in serverless handlers. */
export const serverlessPool = new ServerlessPool();
