'use strict';

const { readFileSync } = require('node:fs');
const { dirname, join } = require('node:path');
const { GlideClient } = require('@glidemq/speedkey');
const { EXECUTABLE, writeLcov } = require('./instrument-lua.cjs');

const LCOV = join(dirname(EXECUTABLE), 'lua.info');
const DUMP_KEY = '{glidemq}:_';

async function dump() {
  const executable = JSON.parse(readFileSync(EXECUTABLE, 'utf8'));
  const client = await GlideClient.createClient({
    addresses: [{ host: 'localhost', port: 6379 }],
    requestTimeout: 5000,
  });
  try {
    const raw = await client.fcall('glidemq_dumpCoverage', [DUMP_KEY], []);
    const hitSet = new Set(
      String(raw)
        .split(',')
        .filter(Boolean)
        .map((n) => Number(n)),
    );
    writeLcov(executable, hitSet, LCOV);
    console.log(`[OK] Lua coverage ${hitSet.size}/${executable.length} -> ${LCOV}`);
  } finally {
    client.close();
  }
}

module.exports = { dump, LCOV };

if (require.main === module) {
  dump().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
