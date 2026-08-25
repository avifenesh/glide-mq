import { readFileSync } from 'node:fs';
import { instrument, isSkippable, netDepth, stampCoverageVersion } from '../scripts/instrument-lua.cjs';
import { dumpAddress } from '../scripts/dump-lua-coverage.cjs';

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

  it('stamps a distinct library version into dist when coverage is enabled', () => {
    const js = "exports.LIBRARY_VERSION = '93';\nexports.LIBRARY_SOURCE = 'x';\n";
    expect(stampCoverageVersion(js)).toContain("exports.LIBRARY_VERSION = '93-cov'");
  });

  it('fails loudly if the dist version export cannot be stamped', () => {
    expect(() => stampCoverageVersion("exports.LIBRARY_NAME = 'glidemq';\n")).toThrow(
      /could not stamp coverage LIBRARY_VERSION/,
    );
  });

  it('instruments the real library without breaking shebang or version placeholder', () => {
    const source = readFileSync('src/functions/glidemq.lua', 'utf8');
    const { lua, executable } = instrument(source);
    expect(lua.startsWith('#!lua name=glidemq')).toBe(true);
    expect(lua).toContain("return '__LIBRARY_VERSION__'");
    expect(lua).toContain("__reg('glidemq_addJob'");
    expect(lua).not.toContain("redis.register_function('glidemq_");
    expect(executable.length).toBeGreaterThan(500);
  });

  it('keeps instrumentation off the publish build', () => {
    const pkg = JSON.parse(readFileSync('package.json', 'utf8')) as { scripts: Record<string, string> };
    expect(pkg.scripts.build).not.toContain('instrument-lua');
    expect(pkg.scripts['build:lua-cov']).toContain('instrument-lua.cjs');
  });

  it('reads dump host and port from VALKEY_HOST / VALKEY_PORT', () => {
    const prevHost = process.env.VALKEY_HOST;
    const prevPort = process.env.VALKEY_PORT;
    delete process.env.VALKEY_HOST;
    delete process.env.VALKEY_PORT;
    try {
      expect(dumpAddress()).toEqual({ host: 'localhost', port: 6379 });
      process.env.VALKEY_HOST = 'valkey.internal';
      process.env.VALKEY_PORT = '6380';
      expect(dumpAddress()).toEqual({ host: 'valkey.internal', port: 6380 });
    } finally {
      if (prevHost === undefined) delete process.env.VALKEY_HOST;
      else process.env.VALKEY_HOST = prevHost;
      if (prevPort === undefined) delete process.env.VALKEY_PORT;
      else process.env.VALKEY_PORT = prevPort;
    }
  });
});
