// Close-under-load strand probe: how many graceful closes leave an entry in the
// closed consumer's PEL, with and without the in-flight read wait.
// Usage (after npm run build, Valkey on :6379 and cluster on :7000): node scripts/probe-close-strand.mjs [iterations]
import { createRequire } from 'node:module';
const wt = new URL('..', import.meta.url).pathname.replace(/\/$/, '');
const N = Number(process.argv[2] || 30);
const require = createRequire(wt + '/package.json');
const { Queue } = require(wt + '/dist/queue');
const { Worker } = require(wt + '/dist/worker');
const { buildKeys } = require(wt + '/dist/utils');
const { GlideClient, GlideClusterClient } = require(wt + '/node_modules/@glidemq/speedkey');
const pkg = require(wt + '/node_modules/@glidemq/speedkey/package.json');
const MODES = {
  standalone: { addresses: [{ host: 'localhost', port: 6379 }] },
  cluster: { addresses: [{ host: '127.0.0.1', port: 7000 }], clusterMode: true },
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const proto = Object.getPrototypeOf(Worker.prototype);
const realSettle = proto.settlePollLoop;
console.log(`speedkey ${pkg.version}`);
for (const [mode, conn] of Object.entries(MODES)) {
  const client = conn.clusterMode
    ? await GlideClusterClient.createClient({ addresses: conn.addresses })
    : await GlideClient.createClient({ addresses: conn.addresses });
  for (const wait of [true, false]) {
    proto.settlePollLoop = wait ? realSettle : async function () {};
    let stranded = 0;
    const closeMs = [];
    for (let i = 0; i < N; i++) {
      const name = `strand-${mode}-${wait ? 'w' : 'n'}-${i}-${Date.now()}`;
      const queue = new Queue(name, { connection: conn });
      const worker = new Worker(name, async () => 'ok', {
        connection: conn,
        concurrency: 2,
        blockTimeout: 10000,
        stalledInterval: 60000,
      });
      await worker.waitUntilReady();
      await sleep(150);
      const t0 = Date.now();
      const closing = worker.close();
      const adds = [];
      for (let d = 0; d < 8; d++) {
        adds.push(queue.add('j', { d }));
        await sleep(1);
      }
      await Promise.all(adds);
      await closing;
      closeMs.push(Date.now() - t0);
      const k = buildKeys(name);
      const pending = await client.xpending(k.stream, 'workers');
      const mine = (pending[3] ?? []).filter(([c]) => String(c) === worker.consumerId);
      if (mine.length) stranded++;
      await queue.close();
      await client
        .del([
          k.stream,
          k.meta,
          k.id,
          k.scheduled,
          k.completed,
          k.failed,
          k.events,
          k.lifo,
          k.priority,
          k.listActive,
          k.listActiveIds,
        ])
        .catch(() => {});
    }
    closeMs.sort((a, b) => a - b);
    console.log(
      `${mode} wait=${wait}: stranded ${stranded}/${N}, close p50 ${closeMs[Math.floor(N / 2)]} ms, max ${closeMs[N - 1]} ms`,
    );
  }
  client.close();
}
