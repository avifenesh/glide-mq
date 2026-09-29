/**
 * Runtime compatibility smoke for glide-mq against a live Valkey.
 *
 * Runs the same scenario under Node, Bun and Deno (see the sibling entry files) and
 * prints one [OK]/[ERROR] line per step. Every step has its own timeout so a hung
 * runtime primitive (blocking read, worker thread, child process) cannot stall the run.
 *
 * Requires: built dist/ (`npm run build`) and Valkey on VALKEY_HOST:VALKEY_PORT
 * (default localhost:6379). Queue names are unique per run and obliterated at the end.
 */
import { createRequire } from 'node:module';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

type GlideMQ = typeof import('../../src/index.ts');
type Speedkey = typeof import('@glidemq/speedkey');

interface StepResult {
  name: string;
  ok: boolean;
  ms: number;
  error?: string;
}

const STEP_TIMEOUT_MS = 20_000;

function runtimeLabel(): string {
  const v = process.versions as Record<string, string | undefined>;
  if (v.bun) return `bun ${v.bun}`;
  if (v.deno) return `deno ${v.deno}`;
  return `node ${v.node}`;
}

function withTimeout<T>(p: Promise<T>, ms: number, what: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout>;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${what} timed out after ${ms}ms`)), ms);
  });
  return Promise.race([p, timeout]).finally(() => clearTimeout(timer!)) as Promise<T>;
}

async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 10_000,
  intervalMs = 50,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  throw new Error(`waitFor timed out after ${timeoutMs}ms`);
}

function assert(cond: unknown, msg: string): void {
  if (!cond) throw new Error(msg);
}

async function expectCompleted(queue: any, job: any, what: string): Promise<any> {
  const state = await job.waitUntilFinished(50, 10_000);
  const fetched = await queue.getJob(job.id);
  assert(state === 'completed', `${what} ended ${state}: ${fetched?.failedReason ?? 'no failedReason'}`);
  return fetched;
}

export async function runSmoke(): Promise<number> {
  const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
  const require = createRequire(path.join(root, 'package.json'));
  const connection = {
    addresses: [{ host: process.env.VALKEY_HOST ?? 'localhost', port: Number(process.env.VALKEY_PORT ?? 6379) }],
  };
  const runId = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
  const qname = (suffix: string) => `compat-${suffix}-${runId}`;
  const echoProcessor = path.join(root, 'tests', 'fixtures', 'processors', 'echo.js');

  const results: StepResult[] = [];
  const queueNames: string[] = [];
  let gmq: GlideMQ | undefined;
  let speedkey: Speedkey | undefined;

  const step = async (name: string, fn: () => Promise<void>): Promise<void> => {
    const start = Date.now();
    try {
      await withTimeout(fn(), STEP_TIMEOUT_MS, name);
      results.push({ name, ok: true, ms: Date.now() - start });
    } catch (err) {
      const error = err instanceof Error ? `${err.name}: ${err.message}` : String(err);
      results.push({ name, ok: false, ms: Date.now() - start, error });
    }
  };

  console.log(`glide-mq compat smoke: ${runtimeLabel()} (${process.platform}/${process.arch})`);

  await step('napi-load', async () => {
    speedkey = require('@glidemq/speedkey') as Speedkey;
    assert(typeof speedkey.GlideClient === 'function', 'GlideClient export missing');
    const client = await speedkey.GlideClient.createClient({ addresses: connection.addresses, requestTimeout: 2000 });
    try {
      const pong = await client.ping();
      assert(String(pong) === 'PONG', `ping returned ${String(pong)}`);
    } finally {
      client.close();
    }
  });

  await step('dist-load', async () => {
    gmq = require(path.join(root, 'dist', 'index.js')) as GlideMQ;
    for (const name of ['Queue', 'Worker', 'FlowProducer', 'Broadcast', 'BroadcastWorker', 'QueueEvents']) {
      assert(typeof (gmq as any)[name] === 'function', `${name} export missing`);
    }
  });

  if (!gmq || !speedkey) {
    report(results);
    return 1;
  }
  const { Queue, Worker, FlowProducer, Broadcast, BroadcastWorker, QueueEvents } = gmq;

  await step('queue-worker-inline', async () => {
    const name = qname('inline');
    queueNames.push(name);
    const queue = new Queue(name, { connection });
    const events = new QueueEvents(name, { connection });
    const completedEvents: string[] = [];
    events.on('completed', ({ jobId }: { jobId: string }) => completedEvents.push(jobId));
    const worker = new Worker(name, async (job: any) => job.data.n * 2, {
      connection,
      concurrency: 4,
      blockTimeout: 250,
    });
    try {
      await Promise.all([events.waitUntilReady(), worker.waitUntilReady()]);
      const jobs = await Promise.all([1, 2, 3, 4, 5].map((n) => queue.add('double', { n })));
      for (const job of jobs) await expectCompleted(queue, job, `job ${job!.id}`);
      const fetched = await queue.getJob(jobs[2]!.id);
      assert(fetched?.returnvalue === 6, `returnvalue ${JSON.stringify(fetched?.returnvalue)} != 6`);
      await waitFor(() => completedEvents.length >= 5);
    } finally {
      await worker.close(true);
      await events.close();
      await queue.close();
    }
  });

  await step('gzip-compression', async () => {
    const name = qname('gzip');
    queueNames.push(name);
    const payload = { text: 'x'.repeat(4096), bytes: Array.from({ length: 32 }, (_, i) => i) };
    const queue = new Queue(name, { connection, compression: 'gzip' });
    const worker = new Worker(
      name,
      async (job: any) => ({
        len: job.data.text.length,
        sum: job.data.bytes.reduce((a: number, b: number) => a + b, 0),
      }),
      {
        connection,
        compression: 'gzip',
        blockTimeout: 250,
      },
    );
    try {
      await worker.waitUntilReady();
      const job = await queue.add('blob', payload);
      const fetched = await expectCompleted(queue, job, 'job');
      assert(fetched?.data.text.length === 4096, 'compressed data did not round-trip');
      assert(
        fetched?.returnvalue?.len === 4096 && fetched?.returnvalue?.sum === 496,
        `unexpected returnvalue ${JSON.stringify(fetched?.returnvalue)}`,
      );
    } finally {
      await worker.close(true);
      await queue.close();
    }
  });

  for (const useWorkerThreads of [true, false]) {
    const label = useWorkerThreads ? 'sandbox-worker-threads' : 'sandbox-child-process';
    await step(label, async () => {
      const name = qname(useWorkerThreads ? 'thread' : 'fork');
      queueNames.push(name);
      const queue = new Queue(name, { connection });
      const worker = new Worker(name, echoProcessor, { connection, sandbox: { useWorkerThreads }, blockTimeout: 250 });
      const workerErrors: Error[] = [];
      worker.on('error', (err: Error) => workerErrors.push(err));
      try {
        await worker.waitUntilReady();
        const job = await queue.add('echo', { hello: label });
        const fetched = await expectCompleted(queue, job, 'job');
        if (workerErrors.length) throw workerErrors[0];
        assert(fetched?.returnvalue?.hello === label, `echo returned ${JSON.stringify(fetched?.returnvalue)}`);
      } finally {
        await worker.close(true);
        await queue.close();
      }
    });
  }

  await step('flow-producer', async () => {
    const name = qname('flow');
    queueNames.push(name);
    const flow = new FlowProducer({ connection });
    const worker = new Worker(
      name,
      async (job: any) => (job.name === 'parent' ? Object.values(await job.getChildrenValues()) : job.data.idx),
      { connection, concurrency: 3, blockTimeout: 250 },
    );
    const queue = new Queue(name, { connection });
    try {
      await worker.waitUntilReady();
      const node = await flow.add({
        name: 'parent',
        queueName: name,
        data: {},
        children: [
          { name: 'child', queueName: name, data: { idx: 1 } },
          { name: 'child', queueName: name, data: { idx: 2 } },
        ],
      });
      const parent = await queue.getJob(node.job.id);
      assert(parent, 'parent job not found');
      const done = await expectCompleted(queue, parent, 'parent');
      const values = (done?.returnvalue as number[]).slice().sort();
      assert(JSON.stringify(values) === '[1,2]', `children values ${JSON.stringify(done?.returnvalue)}`);
    } finally {
      await worker.close(true);
      await flow.close();
      await queue.close();
    }
  });

  await step('broadcast-fanout', async () => {
    const name = qname('bcast');
    queueNames.push(name);
    const broadcast = new Broadcast(name, { connection });
    const received: Record<string, any[]> = { a: [], b: [] };
    const subs = ['a', 'b'].map(
      (sub) =>
        new BroadcastWorker(name, async (job: any) => void received[sub].push(job.data), {
          connection,
          subscription: `sub-${sub}`,
          blockTimeout: 250,
        }),
    );
    try {
      await Promise.all(subs.map((w) => w.waitUntilReady()));
      await broadcast.publish('message', { seq: 1 });
      await waitFor(() => received.a.length === 1 && received.b.length === 1);
      assert(received.a[0].seq === 1 && received.b[0].seq === 1, 'broadcast payload mismatch');
    } finally {
      await Promise.all(subs.map((w) => w.close(true)));
      await broadcast.close();
    }
  });

  await step('process-signals', async () => {
    let seen = 0;
    const onSignal = () => void seen++;
    process.on('SIGUSR2', onSignal);
    try {
      process.kill(process.pid, 'SIGUSR2');
      await waitFor(() => seen === 1, 3000);
    } finally {
      process.off('SIGUSR2', onSignal);
    }
    const handle = gmq!.gracefulShutdown([]);
    assert(process.listenerCount('SIGTERM') >= 1, 'gracefulShutdown did not register SIGTERM');
    handle.dispose();
  });

  await step('cleanup', async () => {
    const client = await speedkey!.GlideClient.createClient({ addresses: connection.addresses, requestTimeout: 5000 });
    try {
      for (const name of queueNames) {
        const queue = new Queue(name, { client });
        await queue.obliterate({ force: true });
        await queue.close();
      }
      let cursor = '0';
      let leftovers = 0;
      do {
        const [next, keys] = await client.scan(cursor, { match: `glide:{compat-*-${runId}}:*`, count: 1000 });
        cursor = String(next);
        if (keys.length) {
          leftovers += keys.length;
          await client.del(keys.map(String));
        }
      } while (cursor !== '0');
      if (leftovers) console.log(`[WARN] cleanup removed ${leftovers} leftover key(s) after obliterate`);
    } finally {
      client.close();
    }
  });

  report(results);
  return results.every((r) => r.ok) ? 0 : 1;
}

function report(results: StepResult[]): void {
  const width = Math.max(...results.map((r) => r.name.length));
  for (const r of results) {
    const tag = r.ok ? '[OK]   ' : '[ERROR]';
    console.log(`${tag} ${r.name.padEnd(width)} ${String(r.ms).padStart(6)}ms${r.error ? `  ${r.error}` : ''}`);
  }
  const failed = results.filter((r) => !r.ok).length;
  console.log(`${results.length - failed}/${results.length} steps passed`);
}
