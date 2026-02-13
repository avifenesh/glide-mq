import { GlideClient, GlideClusterClient } from 'speedkey';
import type { ConnectionOptions, Client } from './types';
import { ConnectionError } from './errors';

export async function createClient(opts: ConnectionOptions): Promise<Client> {
  const config = {
    addresses: opts.addresses,
    useTLS: opts.useTLS,
    credentials: opts.credentials,
  };

  if (opts.clusterMode) {
    return GlideClusterClient.createClient(config);
  }
  return GlideClient.createClient(config);
}

export async function createBlockingClient(opts: ConnectionOptions): Promise<Client> {
  // Dedicated client for XREADGROUP BLOCK / XREAD BLOCK
  // Must not be shared with non-blocking commands
  return createClient(opts);
}

export async function ensureFunctionLibrary(client: Client, librarySource: string): Promise<void> {
  // TODO: Check if glidemq library is loaded, load if missing
  // FUNCTION LIST LIBRARYNAME glidemq
  // If missing: FUNCTION LOAD librarySource
  // If version mismatch: FUNCTION LOAD REPLACE librarySource
}
