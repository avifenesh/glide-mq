import { GlideClient, GlideClusterClient } from 'speedkey';
import type { ConnectionOptions, Client } from './types';
import { ConnectionError } from './errors';
import { LIBRARY_VERSION, LIBRARY_SOURCE } from './functions/index';

/**
 * Create a GlideClient (standalone) or GlideClusterClient (cluster) based on options.
 */
export async function createClient(opts: ConnectionOptions): Promise<Client> {
  const config = {
    addresses: opts.addresses,
    useTLS: opts.useTLS,
    credentials: opts.credentials,
  };

  try {
    if (opts.clusterMode) {
      return await GlideClusterClient.createClient(config);
    }
    return await GlideClient.createClient(config);
  } catch (err) {
    throw new ConnectionError(
      `Failed to create ${opts.clusterMode ? 'cluster' : 'standalone'} client: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
}

/**
 * Create a dedicated client for XREADGROUP BLOCK / XREAD BLOCK.
 * Must not be shared with non-blocking commands.
 */
export async function createBlockingClient(opts: ConnectionOptions): Promise<Client> {
  return createClient(opts);
}

/**
 * Ensure the glidemq function library is loaded and up-to-date on the server.
 *
 * Strategy:
 * 1. Try FCALL glidemq_version with empty keys/args
 * 2. If succeeds and version matches, return (already loaded)
 * 3. If fails (function not found) or version mismatch, call functionLoad with replace
 * 4. For cluster clients, load to all primaries via route option
 *
 * @param client - The Valkey client
 * @param librarySource - The Lua library source code to load
 * @param clusterMode - Whether the client is a cluster client (determines routing for FUNCTION LOAD)
 */
export async function ensureFunctionLibrary(
  client: Client,
  librarySource: string = LIBRARY_SOURCE,
  clusterMode: boolean = false,
): Promise<void> {
  let needsLoad = false;

  try {
    const result = await client.fcall('glidemq_version', [], []);
    if (String(result) !== LIBRARY_VERSION) {
      needsLoad = true;
    }
  } catch {
    // Function not found or other error - need to load
    needsLoad = true;
  }

  if (!needsLoad) return;

  if (clusterMode) {
    await (client as GlideClusterClient).functionLoad(librarySource, {
      replace: true,
      route: 'allPrimaries',
    });
  } else {
    await (client as GlideClient).functionLoad(librarySource, {
      replace: true,
    });
  }
}

/**
 * Create a consumer group for the given stream key.
 * Uses XGROUP CREATE with mkStream to auto-create the stream if it doesn't exist.
 * Handles BUSYGROUP error (group already exists) gracefully.
 *
 * @param client - The Valkey client
 * @param streamKey - The stream key to create the group on
 * @param groupName - The consumer group name
 * @param startId - The ID to start reading from (default: '0' = beginning)
 */
export async function createConsumerGroup(
  client: Client,
  streamKey: string,
  groupName: string,
  startId: string = '0',
): Promise<void> {
  try {
    await client.xgroupCreate(streamKey, groupName, startId, {
      mkStream: true,
    });
  } catch (err) {
    // BUSYGROUP means the group already exists - that's fine
    if (err instanceof Error && err.message.includes('BUSYGROUP')) {
      return;
    }
    throw err;
  }
}
