import { join, relative, resolve, sep } from 'node:path';
import type { Plugin } from 'vite';
import { coverageConfigDefaults, defineConfig } from 'vitest/config';

const isWindows = process.platform === 'win32';
const maxWorkers = process.env.VITEST_MAX_WORKERS ?? (isWindows ? 1 : 2);
const minWorkers = process.env.VITEST_MIN_WORKERS ?? 1;
const fileParallelism =
  process.env.VITEST_FILE_PARALLELISM != null
    ? process.env.VITEST_FILE_PARALLELISM === '1' || process.env.VITEST_FILE_PARALLELISM === 'true'
    : !isWindows;

const luaCoverage = process.env.GLIDEMQ_LUA_COVERAGE === '1';
const srcFunctions = resolve(process.cwd(), 'src/functions');
const distFunctions = resolve(process.cwd(), 'dist/functions');

function luaCoverageAlias(): Plugin {
  return {
    name: 'lua-coverage-src-functions-alias',
    enforce: 'pre',
    async resolveId(source, importer, options) {
      if (!importer) return null;
      const resolved = await this.resolve(source, importer, { ...options, skipSelf: true });
      if (!resolved || resolved.external) return null;
      const [id, query] = resolved.id.split('?');
      if (id !== srcFunctions && !id.startsWith(srcFunctions + sep)) return null;
      const mapped = join(distFunctions, relative(srcFunctions, id).replace(/\.ts$/, '.js'));
      return query ? `${mapped}?${query}` : mapped;
    },
  };
}

export default defineConfig({
  plugins: luaCoverage ? [luaCoverageAlias()] : [],
  test: {
    globals: true,
    testTimeout: 30000,
    hookTimeout: 60000,
    maxWorkers,
    minWorkers,
    fileParallelism,
    exclude: ['node_modules/**'],
    globalSetup: './tests/helpers/lua-coverage-hooks.ts',
    coverage: {
      provider: 'v8',
      // Integration tests require() dist/*.js. Include those files so V8 can
      // collect hits, then excludeAfterRemap maps them back to src/**/*.ts.
      include: ['src/**/*.ts', 'dist/**/*.js'],
      exclude: coverageConfigDefaults.exclude.filter((p) => p !== 'dist/**' && p !== '**/dist/**'),
      excludeAfterRemap: true,
      reporter: ['text', 'lcov'],
      reportsDirectory: 'coverage',
      reportOnFailure: true,
    },
  },
});
