/**
 * Test fixture for running integration tests against both standalone and cluster Valkey.
 *
 * Usage:
 *   const { describeEachMode, createCleanupClient, flushQueue } = require('./helpers/fixture');
 *   describeEachMode('My feature', (CONNECTION) => {
 *     it('works', async () => {
 *       const queue = new Queue('test', { connection: CONNECTION });
 *       ...
 *     });
 *   });
 *
 * Each describe block runs twice: once with standalone (:6379), once with cluster (:7000).
 */

import { describe } from 'vitest';

const { GlideClient, GlideClusterClient } = require('speedkey') as typeof import('speedkey');
const { buildKeys, keyPrefix } = require('../../dist/utils') as typeof import('../../src/utils');
const { ensureFunctionLibrary } = require('../../dist/connection') as typeof import('../../src/connection');
const { LIBRARY_SOURCE } = require('../../dist/functions/index') as typeof import('../../src/functions/index');

export interface ConnectionConfig {
  addresses: { host: string; port: number }[];
  clusterMode?: boolean;
}

export const STANDALONE: ConnectionConfig = {
  addresses: [{ host: 'localhost', port: 6379 }],
};

export const CLUSTER: ConnectionConfig = {
  addresses: [{ host: '127.0.0.1', port: 7000 }],
  clusterMode: true,
};

// Skip cluster mode if SKIP_CLUSTER env var is set (e.g., in CI without cluster)
const MODES: { name: string; connection: ConnectionConfig }[] = process.env.SKIP_CLUSTER
  ? [{ name: 'standalone', connection: STANDALONE }]
  : [
      { name: 'standalone', connection: STANDALONE },
      { name: 'cluster', connection: CLUSTER },
    ];

/**
 * Run a describe block against both standalone and cluster.
 * The callback receives the connection config for the current mode.
 */
export function describeEachMode(
  name: string,
  fn: (connection: ConnectionConfig) => void,
): void {
  for (const mode of MODES) {
    describe(`${name} [${mode.name}]`, () => fn(mode.connection));
  }
}

/**
 * Create a cleanup client for the given connection config.
 * Returns either GlideClient or GlideClusterClient.
 * Caller must close() when done.
 */
export async function createCleanupClient(connection: ConnectionConfig) {
  if (connection.clusterMode) {
    const client = await GlideClusterClient.createClient({
      addresses: connection.addresses,
    });
    await ensureFunctionLibrary(client, LIBRARY_SOURCE, true);
    return client;
  }
  const client = await GlideClient.createClient({
    addresses: connection.addresses,
  });
  await ensureFunctionLibrary(client, LIBRARY_SOURCE);
  return client;
}

/**
 * Flush all keys for a queue. Works for both standalone and cluster.
 */
export async function flushQueue(
  client: any,
  queueName: string,
  prefix = 'glide',
): Promise<void> {
  const k = buildKeys(queueName, prefix);
  const staticKeys = [
    k.id, k.stream, k.scheduled, k.completed, k.failed,
    k.events, k.meta, k.dedup, k.rate, k.schedulers,
  ];
  for (const key of staticKeys) {
    try { await client.del([key]); } catch {}
  }

  // Scan and delete job hashes + log keys + deps keys
  const pfx = keyPrefix(prefix, queueName);
  for (const pattern of [`${pfx}:job:*`, `${pfx}:log:*`, `${pfx}:deps:*`]) {
    try {
      if (client.constructor.name === 'GlideClusterClient') {
        // Cluster scan - use string cursor
        let cursor = '0';
        do {
          const result = await client.scan(cursor, { match: pattern, count: 100 });
          cursor = result[0] as string;
          const keys = result[1] as string[];
          if (keys.length > 0) await client.del(keys);
        } while (cursor !== '0');
      } else {
        let cursor = '0';
        do {
          const result = await client.scan(cursor, { match: pattern, count: 100 });
          cursor = result[0] as string;
          const keys = result[1] as string[];
          if (keys.length > 0) await client.del(keys);
        } while (cursor !== '0');
      }
    } catch {}
  }
}
