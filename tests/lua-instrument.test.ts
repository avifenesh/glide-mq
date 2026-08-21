import { describe, expect, it, vi } from 'vitest';
import { readFileSync } from 'node:fs';
import { instrument, isSkippable, netDepth } from '../scripts/instrument-lua.cjs';

describe('Lua coverage instrumenter', () => {
  it('skips comments, blanks, and closers', () => {
    expect(isSkippable('')).toBe(true);
    expect(isSkippable('-- note')).toBe(true);
    expect(isSkippable('elseif x then')).toBe(true);
    expect(isSkippable(')')).toBe(true);
    expect(isSkippable('local x = 1')).toBe(false);
  });

  it('tracks paren depth across quoted strings', () => {
    expect(netDepth("redis.call('HSET', jobKey,")).toBe(1);
    expect(netDepth("  'state', 'failed')")).toBe(-1);
    expect(netDepth('local x = 1')).toBe(0);
  });

  it('probes original line numbers and wraps register_function', () => {
    const src = [
      '#!lua name=glidemq',
      '',
      'local function decrListActive(key)',
      '  if not key then return end',
      'end',
      "redis.register_function('glidemq_version', function(keys, args)",
      "  return '1'",
      'end)',
      '',
    ].join('\n');
    const { lua, executable } = instrument(src);
    expect(lua).toContain('local __cov = {}');
    expect(lua).toContain('local function __reg(name, fn)');
    expect(lua).toContain("__reg('glidemq_version'");
    expect(lua).toContain('__cov[3]=1;');
    expect(lua).toContain('__cov[4]=1;');
    expect(lua).not.toContain('__cov[5]=1;');
    expect(lua).toContain('glidemq_dumpCoverage');
    expect(executable).toEqual([3, 4, 6, 7]);
  });

  it('does not inject probes into multi-line call arguments', () => {
    const src = ['#!lua name=glidemq', 'redis.call("HSET", jobKey,', "  'state', 'failed'", ')', ''].join('\n');
    const { lua, executable } = instrument(src);
    expect(lua).toContain('__cov[2]=1;');
    expect(lua).not.toContain('__cov[3]=1;');
    expect(lua).not.toContain('__cov[4]=1;');
    expect(executable).toEqual([2]);
  });

  it('stamps a distinct library version when coverage is enabled', async () => {
    vi.stubEnv('GLIDEMQ_LUA_COVERAGE', '1');
    try {
      vi.resetModules();
      const { LIBRARY_VERSION } = await import('../src/functions/index');
      expect(LIBRARY_VERSION).toBe('93-cov');
    } finally {
      vi.unstubAllEnvs();
    }
  });

  it('instruments the real library without breaking shebang or version placeholder', () => {
    const source = readFileSync('src/functions/glidemq.lua', 'utf8');
    const { lua, executable } = instrument(source);
    expect(lua.startsWith('#!lua name=glidemq')).toBe(true);
    expect(lua).toContain("return '__LIBRARY_VERSION__'");
    expect(lua).toContain("__reg('glidemq_addJob'");
    expect(lua).not.toContain("redis.register_function('glidemq_");
    expect(lua).not.toContain('__cov[1108]=1;');
    expect(executable.length).toBeGreaterThan(500);
  });
});
