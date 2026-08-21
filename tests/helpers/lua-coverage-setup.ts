export default async function luaCoverageSetup(): Promise<void> {
  if (process.env.GLIDEMQ_LUA_COVERAGE !== '1') return;
  const { GlideClient } = require('@glidemq/speedkey') as typeof import('@glidemq/speedkey');
  const { LIBRARY_SOURCE } = require('../../dist/functions') as typeof import('../../src/functions');
  const client = await GlideClient.createClient({ addresses: [{ host: 'localhost', port: 6379 }] });
  try {
    await client.functionLoad(LIBRARY_SOURCE, { replace: true });
  } finally {
    client.close();
  }
}
