import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import EMBEDDED_LIBRARY_FILE from './glidemq.embedded.json';

/**
 * Prefer the sibling .lua file (npm package + Lua coverage instrumentation).
 * Fall back to the build-time embed so bundlers (Lambda/ncc/esbuild) still work
 * when they emit JS without copying glidemq.lua.
 */
export function loadLibraryFile(dir: string = __dirname): string {
  try {
    return readFileSync(join(dir, 'glidemq.lua'), 'utf8');
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === 'ENOENT') {
      return EMBEDDED_LIBRARY_FILE;
    }
    throw err;
  }
}

export function librarySourceFrom(file: string, version: string): string {
  return file.replaceAll('__LIBRARY_VERSION__', version);
}
