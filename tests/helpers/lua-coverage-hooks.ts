export async function setup(): Promise<void> {
  if (process.env.GLIDEMQ_LUA_COVERAGE !== '1') return;
  const { GlideClient } = require('@glidemq/speedkey') as typeof import('@glidemq/speedkey');
  const { LIBRARY_SOURCE } = require('../../dist/functions') as typeof import('../../src/functions');
  const client = await GlideClient.createClient({
    addresses: [
      {
        host: process.env.VALKEY_HOST ?? 'localhost',
        port: Number(process.env.VALKEY_PORT ?? 6379),
      },
    ],
    requestTimeout: 5000,
  });
  try {
    await client.functionLoad(LIBRARY_SOURCE, { replace: true });
  } finally {
    client.close();
  }
}

export async function teardown(): Promise<void> {
  if (process.env.GLIDEMQ_LUA_COVERAGE !== '1') return;
  const { dump } = require('../../scripts/dump-lua-coverage.cjs') as { dump: () => Promise<void> };
  await dump();
}
