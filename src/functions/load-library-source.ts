import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import EMBEDDED_LIBRARY_FILE from './glidemq.embedded.json';

function readLua(path: string): string | undefined {
  try {
    return readFileSync(path, 'utf8');
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === 'ENOENT') return undefined;
    throw err;
  }
}

/**
 * Prefer the sibling .lua file (npm package + Lua coverage instrumentation).
 * Fall back to the generated embed so bundlers (Lambda/ncc/esbuild) still work
 * when they emit JS without copying glidemq.lua.
 *
 * When GLIDEMQ_LUA_COVERAGE=1, prefer the instrumented dist copy so src-imported
 * Queue/Worker clients load the same library as dist clients and do not REPLACE it.
 */
export function loadLibraryFile(dir: string = __dirname): string {
  if (process.env.GLIDEMQ_LUA_COVERAGE === '1') {
    const instrumented = readLua(join(process.cwd(), 'dist/functions/glidemq.lua'));
    if (instrumented !== undefined) return instrumented;
  }
  return readLua(join(dir, 'glidemq.lua')) ?? EMBEDDED_LIBRARY_FILE;
}

export function librarySourceFrom(file: string, version: string): string {
  return file.replaceAll('__LIBRARY_VERSION__', version);
}
