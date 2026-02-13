import type { Client } from '../types';

// The Lua library source will be embedded here at build time
// For now, defined inline
export const LIBRARY_NAME = 'glidemq';
export const LIBRARY_VERSION = '1';

// TODO: This will be the compiled Lua library source
export const LIBRARY_SOURCE = `#!lua name=${LIBRARY_NAME}
-- glide-mq function library v${LIBRARY_VERSION}
-- Loaded once via FUNCTION LOAD, persistent across restarts

-- placeholder - functions will be added here
redis.register_function('glidemq_version', function(keys, args)
  return '${LIBRARY_VERSION}'
end)
`;

export async function ensureLibrary(client: Client): Promise<void> {
  // Check if library exists and version matches
  // If missing or outdated, load/replace
  try {
    const result = await client.fcall('glidemq_version', [], []);
    if (result === LIBRARY_VERSION) return;
  } catch {
    // Function not found - need to load
  }

  await client.functionLoad(LIBRARY_SOURCE, { replace: true });
}
