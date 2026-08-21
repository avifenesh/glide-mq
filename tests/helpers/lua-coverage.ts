import { readFileSync } from 'node:fs';
import { GlideClient } from '@glidemq/speedkey';
import { EXECUTABLE, writeLcov } from '../../scripts/instrument-lua.cjs';

const LCOV = 'coverage/lua.info';
const DUMP_KEY = '{glidemq}:_';

export async function dumpLuaCoverage(): Promise<void> {
  const executable: number[] = JSON.parse(readFileSync(EXECUTABLE, 'utf8'));
  const client = await GlideClient.createClient({
    addresses: [{ host: 'localhost', port: 6379 }],
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
  } finally {
    client.close();
  }
}
