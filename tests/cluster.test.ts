/**
 * Cluster integration tests against a real Valkey Cluster.
 * Requires: 6-node Valkey cluster on ports 7000-7005
 * Start with: tests/helpers/start-cluster.sh
 *
 * Tests verify that glide-mq works correctly with hash-tagged keys
 * across multiple cluster nodes.
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';

const { GlideClusterClient } = require('speedkey') as typeof import('speedkey');
const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
const { LIBRARY_SOURCE } = require('../dist/functions/index') as typeof import('../src/functions/index');
const { ensureFunctionLibrary } = require('../dist/connection') as typeof import('../src/connection');

const CLUSTER_CONNECTION = {
  addresses: [{ host: '127.0.0.1', port: 7000 }],
  clusterMode: true,
};

let cleanupClient: InstanceType<typeof GlideClusterClient>;

async function flushQueue(queueName: string) {
  const k = buildKeys(queueName);
  const keysToDelete = [
    k.id, k.stream, k.scheduled, k.completed, k.failed,
    k.events, k.meta, k.dedup, k.rate, k.schedulers,
  ];
  for (const key of keysToDelete) {
    try { await cleanupClient.del([key]); } catch {}
  }
  // Clean job hashes - in cluster mode, scan is per-node
  // Use customCommand for simplicity since keys share a hash tag
  const prefix = `glide:{${queueName}}:job:`;
  try {
    let cursor = '0';
    do {
      const result = await cleanupClient.scan(cursor, { match: `${prefix}*`, count: 100 });
      cursor = result[0] as string;
      const keys = result[1] as string[];
      if (keys.length > 0) {
        await cleanupClient.del(keys);
      }
    } while (cursor !== '0');
  } catch {}
}

beforeAll(async () => {
  cleanupClient = await GlideClusterClient.createClient({
    addresses: [{ host: '127.0.0.1', port: 7000 }],
  });
  // Load function library to all primaries
  await ensureFunctionLibrary(cleanupClient, LIBRARY_SOURCE, true);
}, 15000);

afterAll(async () => {
  cleanupClient.close();
});

describe('Cluster: Function Library', () => {
  it('glidemq functions are available on all primaries', async () => {
    // Use a dummy hash-tagged key for deterministic routing to a primary.
    // Never use customCommand when fcall with a dummy key works.
    const result = await cleanupClient.fcall('glidemq_version', ['{glidemq}:_'], []);
    expect(String(result)).toBe('1');
  });
});

describe('Cluster: Queue operations', () => {
  const Q = 'cl-queue-' + Date.now();
  let queue: InstanceType<typeof Queue>;

  beforeAll(() => {
    queue = new Queue(Q, { connection: CLUSTER_CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(Q);
  });

  it('adds a job in cluster mode', async () => {
    const job = await queue.add('cluster-job', { cluster: true, value: 42 });
    expect(job.id).toBeTruthy();
    expect(job.name).toBe('cluster-job');
  });

  it('retrieves a job by ID in cluster mode', async () => {
    const job = await queue.add('fetch-test', { x: 1 });
    const fetched = await queue.getJob(job.id);
    expect(fetched).not.toBeNull();
    expect(fetched!.id).toBe(job.id);
    expect(fetched!.name).toBe('fetch-test');
  });

  it('all keys land in the same hash slot', async () => {
    const job = await queue.add('slot-test', { x: 1 });
    const k = buildKeys(Q);

    // All these keys contain {Q} as hash tag - verify they resolve
    // by checking they exist on the server (no CROSSSLOT errors)
    const idExists = await cleanupClient.exists([k.id]);
    expect(idExists).toBe(1);

    const jobExists = await cleanupClient.exists([k.job(job.id)]);
    expect(jobExists).toBe(1);
  });

  it('adds delayed job in cluster mode', async () => {
    const job = await queue.add('delayed-cl', { x: 1 }, { delay: 30000 });
    const k = buildKeys(Q);
    const score = await cleanupClient.zscore(k.scheduled, job.id);
    expect(score).not.toBeNull();
  });

  it('adds prioritized job in cluster mode', async () => {
    const job = await queue.add('prio-cl', { x: 1 }, { priority: 3 });
    const k = buildKeys(Q);
    const score = await cleanupClient.zscore(k.scheduled, job.id);
    expect(score).not.toBeNull();
  });

  it('addBulk works in cluster mode', async () => {
    const jobs = await queue.addBulk([
      { name: 'cb1', data: { i: 1 } },
      { name: 'cb2', data: { i: 2 } },
    ]);
    expect(jobs).toHaveLength(2);
    const ids = new Set(jobs.map(j => j.id));
    expect(ids.size).toBe(2);
  });

  it('pause/resume works in cluster mode', async () => {
    const k = buildKeys(Q);
    await queue.pause();
    const paused = await cleanupClient.hget(k.meta, 'paused');
    expect(String(paused)).toBe('1');
    await queue.resume();
    const resumed = await cleanupClient.hget(k.meta, 'paused');
    expect(String(resumed)).toBe('0');
  });
});

describe('Cluster: Worker processes jobs', () => {
  const Q = 'cl-worker-' + Date.now();

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('worker picks up and completes a job in cluster mode', async () => {
    const queue = new Queue(Q, { connection: CLUSTER_CONNECTION });
    const processed: string[] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
      const worker = new Worker(
        Q,
        async (job: any) => {
          processed.push(job.id);
          return { cluster: true, processed: true };
        },
        { connection: CLUSTER_CONNECTION, concurrency: 1, blockTimeout: 1000 },
      );
      worker.on('completed', () => {
        clearTimeout(timeout);
        worker.close(true).then(resolve);
      });
      worker.on('error', () => {});
    });

    await new Promise(r => setTimeout(r, 500));
    const job = await queue.add('cluster-work', { v: 1 });
    await done;

    expect(processed).toContain(job.id);

    // Verify in Valkey cluster
    const k = buildKeys(Q);
    const score = await cleanupClient.zscore(k.completed, job.id);
    expect(score).not.toBeNull();
    expect(String(await cleanupClient.hget(k.job(job.id), 'state'))).toBe('completed');

    await queue.close();
  }, 15000);

  it('worker handles multiple concurrent jobs in cluster mode', async () => {
    const qName = Q + '-mc';
    const queue = new Queue(qName, { connection: CLUSTER_CONNECTION });
    const processed: string[] = [];

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 12000);
      const worker = new Worker(
        qName,
        async (job: any) => {
          await new Promise(r => setTimeout(r, 30));
          processed.push(job.id);
          if (processed.length >= 3) {
            clearTimeout(timeout);
            setTimeout(() => worker.close(true).then(resolve), 200);
          }
          return 'ok';
        },
        { connection: CLUSTER_CONNECTION, concurrency: 3, blockTimeout: 1000 },
      );
      worker.on('error', () => {});
    });

    await new Promise(r => setTimeout(r, 500));
    for (let i = 0; i < 3; i++) {
      await queue.add(`cc-${i}`, { i });
    }
    await done;

    expect(processed.length).toBe(3);
    await queue.close();
    await flushQueue(qName);
  }, 15000);
});

describe('Cluster: Job operations', () => {
  const Q = 'cl-jobops-' + Date.now();
  let queue: InstanceType<typeof Queue>;

  beforeAll(() => {
    queue = new Queue(Q, { connection: CLUSTER_CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(Q);
  });

  it('updateProgress works in cluster mode', async () => {
    const job = await queue.add('cl-prog', { x: 1 });
    const live = await queue.getJob(job.id);
    await live!.updateProgress(88);

    const k = buildKeys(Q);
    const val = await cleanupClient.hget(k.job(job.id), 'progress');
    expect(String(val)).toBe('88');
  });

  it('remove works in cluster mode', async () => {
    const job = await queue.add('cl-rm', { x: 1 });
    const live = await queue.getJob(job.id);
    await live!.remove();

    const k = buildKeys(Q);
    const exists = await cleanupClient.exists([k.job(job.id)]);
    expect(exists).toBe(0);
  });
});

describe('Cluster: Events stream', () => {
  const Q = 'cl-events-' + Date.now();
  let queue: InstanceType<typeof Queue>;

  beforeAll(() => {
    queue = new Queue(Q, { connection: CLUSTER_CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(Q);
  });

  it('events stream receives added event in cluster mode', async () => {
    const job = await queue.add('cl-ev', { x: 1 });
    const k = buildKeys(Q);

    const entries = await cleanupClient.xrange(k.events, '-', '+') as Record<string, [string, string][]>;
    const entryIds = Object.keys(entries);
    expect(entryIds.length).toBeGreaterThan(0);

    let found = false;
    for (const entryId of entryIds) {
      const fields = entries[entryId];
      const map: Record<string, string> = {};
      for (const [f, v] of fields) {
        map[String(f)] = String(v);
      }
      if (map.event === 'added' && map.jobId === job.id) {
        found = true;
      }
    }
    expect(found).toBe(true);
  });
});
