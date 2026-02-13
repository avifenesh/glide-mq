import { defineConfig } from 'vitest/config';
import path from 'path';

export default defineConfig({
  resolve: {
    alias: {
      // Force vitest to use the built JS output, not TS source
      speedkey: path.resolve(__dirname, '../speedkey/node/build-ts/index.js'),
    },
  },
  test: {
    globals: true,
    testTimeout: 30000,
    hookTimeout: 30000,
  },
});
