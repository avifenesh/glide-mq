import { coverageConfigDefaults, defineConfig } from 'vitest/config';

const isWindows = process.platform === 'win32';
const maxWorkers = process.env.VITEST_MAX_WORKERS ?? (isWindows ? 1 : 2);
const minWorkers = process.env.VITEST_MIN_WORKERS ?? 1;
const fileParallelism =
  process.env.VITEST_FILE_PARALLELISM != null
    ? process.env.VITEST_FILE_PARALLELISM === '1' || process.env.VITEST_FILE_PARALLELISM === 'true'
    : !isWindows;

export default defineConfig({
  test: {
    globals: true,
    testTimeout: 30000,
    hookTimeout: 60000,
    maxWorkers,
    minWorkers,
    fileParallelism,
    exclude: ['node_modules/**'],
    globalSetup: './tests/helpers/lua-coverage-setup.ts',
    globalTeardown: './tests/helpers/lua-coverage-teardown.ts',
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
