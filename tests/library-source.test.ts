import { existsSync, mkdirSync, mkdtempSync, readFileSync, renameSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';
import EMBEDDED_LIBRARY_FILE from '../src/functions/glidemq.embedded.json';
import { LIBRARY_SOURCE, LIBRARY_VERSION } from '../src/functions/index';
import { librarySourceFrom, loadLibraryFile } from '../src/functions/load-library-source';

const LUA_SRC = 'src/functions/glidemq.lua';

describe('library source loading', () => {
  it('generates an embedded snapshot identical to glidemq.lua', () => {
    expect(EMBEDDED_LIBRARY_FILE).toBe(readFileSync(LUA_SRC, 'utf8'));
  });

  it('uses the sibling lua file when present', () => {
    const dir = mkdtempSync(join(tmpdir(), 'glidemq-lua-'));
    try {
      writeFileSync(join(dir, 'glidemq.lua'), '#!lua name=probed\n');
      expect(loadLibraryFile(dir)).toBe('#!lua name=probed\n');
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it('falls back to the embed when the sibling lua file is missing', () => {
    const dir = mkdtempSync(join(tmpdir(), 'glidemq-lua-'));
    try {
      expect(loadLibraryFile(dir)).toBe(EMBEDDED_LIBRARY_FILE);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it('prefers dist lua over a src sibling when Lua coverage is enabled', () => {
    const prev = process.env.GLIDEMQ_LUA_COVERAGE;
    const distDir = 'dist/functions';
    const distLua = join(distDir, 'glidemq.lua');
    const existed = existsSync(distLua);
    const bak = existed ? readFileSync(distLua, 'utf8') : null;
    mkdirSync(distDir, { recursive: true });
    writeFileSync(distLua, '#!lua name=instrumented\n');
    process.env.GLIDEMQ_LUA_COVERAGE = '1';
    const dir = mkdtempSync(join(tmpdir(), 'glidemq-lua-'));
    try {
      writeFileSync(join(dir, 'glidemq.lua'), '#!lua name=src\n');
      expect(loadLibraryFile(dir)).toBe('#!lua name=instrumented\n');
    } finally {
      if (prev === undefined) delete process.env.GLIDEMQ_LUA_COVERAGE;
      else process.env.GLIDEMQ_LUA_COVERAGE = prev;
      rmSync(dir, { recursive: true, force: true });
      if (bak === null) rmSync(distLua, { force: true });
      else writeFileSync(distLua, bak);
    }
  });

  it('rethrows non-ENOENT fs errors', () => {
    const dir = mkdtempSync(join(tmpdir(), 'glidemq-lua-'));
    const luaPath = join(dir, 'glidemq.lua');
    try {
      mkdirSync(luaPath);
      expect(() => loadLibraryFile(dir)).toThrow(/EISDIR|illegal operation/i);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it('substitutes the library version into the lua placeholder', () => {
    const source = librarySourceFrom('return __LIBRARY_VERSION__\n', '93');
    expect(source).toBe('return 93\n');
    expect(LIBRARY_SOURCE).toContain(`'${LIBRARY_VERSION}'`);
    expect(LIBRARY_SOURCE).not.toContain('__LIBRARY_VERSION__');
  });

  it.skipIf(!existsSync('dist/functions/load-library-source.js'))(
    'still exports LIBRARY_SOURCE if dist/functions/glidemq.lua is absent',
    () => {
      const lua = 'dist/functions/glidemq.lua';
      const bak = `${lua}.bak`;
      renameSync(lua, bak);
      try {
        const { loadLibraryFile: loadFromDist } =
          require('../dist/functions/load-library-source') as typeof import('../src/functions/load-library-source');
        const distDir = join(__dirname, '../dist/functions');
        const file = loadFromDist(distDir);
        expect(file).toBe(EMBEDDED_LIBRARY_FILE);
        expect(librarySourceFrom(file, LIBRARY_VERSION)).toContain(`'${LIBRARY_VERSION}'`);
      } finally {
        renameSync(bak, lua);
      }
    },
  );
});
