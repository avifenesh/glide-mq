/**
 * ServerlessPool - Connection reuse for warm Lambda/Edge invocations.
 *
 * Caches Producer instances by queue name + connection fingerprint so that
 * warm invocations reuse existing connections instead of creating new ones.
 */
import { createHash } from 'crypto';
import { Producer } from './producer';
import type { ProducerOptions } from './producer';
import type { ConnectionOptions } from './types';

/**
 * Hash credentials into the fingerprint so different identities do not collide
 * on the same cache entry, while still allowing the same identity to share a
 * cached producer. Plaintext secrets are never embedded in the cache key.
 */
function credentialsFingerprint(creds: ConnectionOptions['credentials']): string {
  if (!creds) return 'none';
  const h = createHash('sha256');
  if ('type' in creds && creds.type === 'iam') {
    h.update('iam');
    h.update('\0');
    h.update(creds.serviceType);
    h.update('\0');
    h.update(creds.region);
    h.update('\0');
    h.update(creds.userId);
    h.update('\0');
    h.update(creds.clusterName);
  } else {
    const pwd = creds as { username?: string; password: string };
    h.update('password');
    h.update('\0');
    h.update(pwd.username ?? '');
    h.update('\0');
    h.update(pwd.password);
  }
  return h.digest('hex');
}

function fingerprint(name: string, opts: ProducerOptions): string {
  const addresses = opts.connection?.addresses ?? [];
  const sorted = [...addresses].sort((a, b) => {
    const hostCmp = a.host.localeCompare(b.host);
    return hostCmp !== 0 ? hostCmp : a.port - b.port;
  });
  // Use default marker for the built-in JSON serializer only
  const serializerKey = 'json';
  return JSON.stringify({
    name,
    prefix: opts.prefix ?? 'glide',
    addresses: sorted,
    clusterMode: opts.connection?.clusterMode ?? false,
    compression: opts.compression ?? 'none',
    serializer: serializerKey,
    useTLS: opts.connection?.useTLS ?? false,
    credentials: credentialsFingerprint(opts.connection?.credentials),
  });
}

export class ServerlessPool {
  private cache = new Map<string, Producer>();
  private closing = false;

  /**
   * Get or create a Producer for the given queue name and options.
   * Returns a cached instance if one exists with matching connection parameters.
   *
   * Note: Injected clients and custom serializers bypass caching to prevent
   * collisions where different instances could map to the same cache key.
   */
  getProducer<D = any>(name: string, opts: ProducerOptions): Producer<D> {
    if (this.closing) {
      throw new Error('ServerlessPool is closing');
    }
    // Custom serializers or injected clients bypass the cache intentionally -
    // serializer instances may hold state and must not be shared across callers.
    // Credentials are folded into the fingerprint (hashed) so different identities
    // do not collide on the same cache entry while still allowing reuse for the
    // same identity.
    if (opts.client || opts.serializer) {
      return new Producer(name, opts);
    }

    const key = fingerprint(name, opts);
    let producer = this.cache.get(key);
    if (!producer || producer.isClosed) {
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
    this.closing = true;
    const producers = [...this.cache.values()];
    this.cache.clear();
    await Promise.allSettled(producers.map((p) => p.close()));
    this.closing = false;
  }

  /** Number of cached producers. */
  get size(): number {
    return this.cache.size;
  }
}

/** Module-level singleton for convenient use in serverless handlers. */
export const serverlessPool = new ServerlessPool();
