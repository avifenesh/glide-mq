/**
 * Measure raw FCALL vs ioredis EVALSHA overhead.
 * Calls a trivial function/script N times to isolate client overhead from Lua complexity.
 * Note: ioredis client.eval is Redis EVAL command, not JS eval().
 */

import { GlideClient } from '@glidemq/speedkey';
import Redis from 'ioredis';
import { GLIDE_CONNECTION, BULL_CONNECTION } from './utils';

const N = 10000;
const CONCURRENCY = 10;

async function measureGlideFcall() {
  const client = await GlideClient.createClient(GLIDE_CONNECTION);

  // glidemq_version is already loaded - trivial function that returns a string
  // Warm up
  for (let i = 0; i < 100; i++) {
    await client.fcall('glidemq_version', [], []);
  }

  // Measure sequential
  const seqStart = performance.now();
  for (let i = 0; i < N; i++) {
    await client.fcall('glidemq_version', [], []);
  }
  const seqElapsed = performance.now() - seqStart;

  // Measure concurrent (c=10)
  const concStart = performance.now();
  const inflight: Promise<any>[] = [];
  let done = 0;
  await new Promise<void>((resolve) => {
    function launch() {
      while (inflight.length < CONCURRENCY && done < N) {
        done++;
        const p = client.fcall('glidemq_version', [], []).then(() => {
          inflight.splice(inflight.indexOf(p), 1);
          if (done < N) launch();
          else if (inflight.length === 0) resolve();
        });
        inflight.push(p);
      }
    }
    launch();
  });
  const concElapsed = performance.now() - concStart;

  client.close();
  return { seqElapsed, concElapsed };
}

async function measureIoredisScript() {
  const client = new Redis({ ...BULL_CONNECTION, lazyConnect: true });
  await client.connect();

  // Define and load a trivial script via SCRIPT LOAD, then call with EVALSHA
  const script = "return 'ok'";
  const sha = await client.call('SCRIPT', 'LOAD', script) as string;

  // Warm up
  for (let i = 0; i < 100; i++) {
    await client.call('EVALSHA', sha, '0');
  }

  // Measure sequential
  const seqStart = performance.now();
  for (let i = 0; i < N; i++) {
    await client.call('EVALSHA', sha, '0');
  }
  const seqElapsed = performance.now() - seqStart;

  // Measure concurrent (c=10)
  const concStart = performance.now();
  const inflight: Promise<any>[] = [];
  let done = 0;
  await new Promise<void>((resolve) => {
    function launch() {
      while (inflight.length < CONCURRENCY && done < N) {
        done++;
        const p = (client.call('EVALSHA', sha, '0') as Promise<any>).then(() => {
          inflight.splice(inflight.indexOf(p), 1);
          if (done < N) launch();
          else if (inflight.length === 0) resolve();
        });
        inflight.push(p);
      }
    }
    launch();
  });
  const concElapsed = performance.now() - concStart;

  await client.quit();
  return { seqElapsed, concElapsed };
}

async function main() {
  console.log(`Raw FCALL vs EVALSHA overhead (${N} calls)\n`);

  const glide = await measureGlideFcall();
  const ioredis = await measureIoredisScript();

  console.log('Sequential (c=1):');
  console.log(`  GLIDE FCALL:    ${(glide.seqElapsed / N).toFixed(3)}ms/call  (${(N / glide.seqElapsed * 1000).toFixed(0)} calls/s)`);
  console.log(`  ioredis EVALSHA: ${(ioredis.seqElapsed / N).toFixed(3)}ms/call  (${(N / ioredis.seqElapsed * 1000).toFixed(0)} calls/s)`);

  console.log(`\nConcurrent (c=${CONCURRENCY}):`);
  console.log(`  GLIDE FCALL:    ${(glide.concElapsed / N * CONCURRENCY).toFixed(3)}ms/call  (${(N / glide.concElapsed * 1000).toFixed(0)} calls/s)`);
  console.log(`  ioredis EVALSHA: ${(ioredis.concElapsed / N * CONCURRENCY).toFixed(3)}ms/call  (${(N / ioredis.concElapsed * 1000).toFixed(0)} calls/s)`);

  console.log(`\nOverhead delta (GLIDE - ioredis):`);
  console.log(`  Sequential: ${((glide.seqElapsed - ioredis.seqElapsed) / N).toFixed(3)}ms/call`);
  console.log(`  Concurrent: ${((glide.concElapsed - ioredis.concElapsed) / N * CONCURRENCY).toFixed(3)}ms/call`);
}

main().catch(console.error);
