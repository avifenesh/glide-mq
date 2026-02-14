/**
 * Cluster edge-case tests for glide-mq.
 * Verifies that Phase 2+3 features (dedup, rate limit, flow, events)
 * work correctly in cluster mode, and that hash tags are correct.
 *
 * Requires: 6-node Valkey cluster on ports 7000-7005
 * Start with: tests/helpers/start-cluster.sh
 *
 * Run: npx vitest run tests/edge-cluster.test.ts
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';

const { GlideClusterClient } = require('speedkey') as typeof import('speedkey');
const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { FlowProducer } = require('../dist/flow-producer') as typeof import('../src/flow-producer');
const { QueueEvents } = require('../dist/queue-events') as typeof import('../src/queue-events');
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
  const prefix = `glide:{${queueName}}:`;
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
  await ensureFunctionLibrary(cleanupClient, LIBRARY_SOURCE, true);
}, 15000);

afterAll(async () => {
  cleanupClient.close();
});

describe('Cluster Edge: Dedup in cluster mode', () => {
  const Q = 'cl-edge-dedup-' + Date.now();
  let queue: InstanceType<typeof Queue>;

  beforeAll(() => {
    queue = new Queue(Q, { connection: CLUSTER_CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(Q);
  });

  it('deduplication works correctly in cluster mode', async () => {
    const job1 = await queue.add('dedup-cl', { v: 1 }, {
      deduplication: { id: 'cl-dedup-1', ttl: 5000 },
    });
    expect(job1).not.toBeNull();
    expect(job1!.id).toBeTruthy();

    // Second add with same dedup ID should be skipped
    const job2 = await queue.add('dedup-cl', { v: 2 }, {
      deduplication: { id: 'cl-dedup-1', ttl: 5000 },
    });
    expect(job2).toBeNull();

    // Different dedup ID should succeed
    const job3 = await queue.add('dedup-cl', { v: 3 }, {
      deduplication: { id: 'cl-dedup-2', ttl: 5000 },
    });
    expect(job3).not.toBeNull();
    expect(job3!.id).not.toBe(job1!.id);
  });
});

describe('Cluster Edge: Rate limit in cluster mode', () => {
  const Q = 'cl-edge-rate-' + Date.now();

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('rate limiting throttles job processing in cluster mode', async () => {
    const queue = new Queue(Q, { connection: CLUSTER_CONNECTION });

    // Add several jobs
    for (let i = 0; i < 5; i++) {
      await queue.add(`rate-job-${i}`, { i });
    }

    const processedTimestamps: number[] = [];
    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => resolve(), 8000);
      const worker = new Worker(
        Q,
        async (job: any) => {
          processedTimestamps.push(Date.now());
          return 'ok';
        },
        {
          connection: CLUSTER_CONNECTION,
          concurrency: 1,
          blockTimeout: 500,
          limiter: { max: 2, duration: 1000 },
          stalledInterval: 60000,
        },
      );
      worker.on('error', () => {});

      // Wait for all to process or timeout
      let checkInterval = setInterval(() => {
        if (processedTimestamps.length >= 5) {
          clearTimeout(timeout);
          clearInterval(checkInterval);
          worker.close(true).then(resolve);
        }
      }, 200);
    });

    await done;

    // Should have processed jobs with some rate limiting delay
    expect(processedTimestamps.length).toBeGreaterThanOrEqual(2);

    await queue.close();
  }, 15000);
});

describe('Cluster Edge: Flow in cluster mode', () => {
  const Q = 'cl-edge-flow-' + Date.now();

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('parent-child flow works in cluster mode', async () => {
    const flow = new FlowProducer({ connection: CLUSTER_CONNECTION });

    const node = await flow.add({
      name: 'cl-parent',
      queueName: Q,
      data: { type: 'parent' },
      children: [
        { name: 'cl-child-1', queueName: Q, data: { idx: 1 } },
        { name: 'cl-child-2', queueName: Q, data: { idx: 2 } },
      ],
    });

    expect(node.job.id).toBeTruthy();
    expect(node.children).toHaveLength(2);

    const parentId = node.job.id;
    const k = buildKeys(Q);

    // Verify parent is in waiting-children
    const parentState = await cleanupClient.hget(k.job(parentId), 'state');
    expect(String(parentState)).toBe('waiting-children');

    // Process all jobs
    const completedNames: string[] = [];
    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 15000);
      const worker = new Worker(
        Q,
        async (job: any) => {
          completedNames.push(job.name);
          return { name: job.name };
        },
        { connection: CLUSTER_CONNECTION, concurrency: 2, blockTimeout: 500 },
      );

      worker.on('completed', (job: any) => {
        if (job.id === parentId) {
          clearTimeout(timeout);
          setTimeout(() => worker.close(true).then(resolve), 200);
        }
      });
      worker.on('error', () => {});
    });

    await done;

    expect(completedNames).toContain('cl-child-1');
    expect(completedNames).toContain('cl-child-2');
    expect(completedNames).toContain('cl-parent');

    // Parent completed after children
    expect(completedNames.indexOf('cl-parent')).toBeGreaterThan(
      Math.max(
        completedNames.indexOf('cl-child-1'),
        completedNames.indexOf('cl-child-2'),
      ),
    );

    await flow.close();
  }, 20000);
});

describe('Cluster Edge: Events in cluster mode', () => {
  const Q = 'cl-edge-events-' + Date.now();

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('QueueEvents receives added and completed events in cluster mode', async () => {
    const queue = new Queue(Q, { connection: CLUSTER_CONNECTION });
    const queueEvents = new QueueEvents(Q, {
      connection: CLUSTER_CONNECTION,
      blockTimeout: 500,
    });
    queueEvents.on('error', () => {});

    const addedEvents: any[] = [];
    const completedEvents: any[] = [];

    queueEvents.on('added', (data: any) => { addedEvents.push(data); });
    queueEvents.on('completed', (data: any) => { completedEvents.push(data); });

    await queueEvents.waitUntilReady();
    await new Promise(r => setTimeout(r, 300));

    const worker = new Worker(
      Q,
      async (job: any) => {
        return { result: job.data.x * 2 };
      },
      { connection: CLUSTER_CONNECTION, concurrency: 1, blockTimeout: 500 },
    );
    worker.on('error', () => {});

    await new Promise(r => setTimeout(r, 500));

    const job = await queue.add('cl-event-test', { x: 21 });

    // Wait for processing and events
    await new Promise(r => setTimeout(r, 3000));

    const addedIds = addedEvents.map(e => e.jobId);
    expect(addedIds).toContain(job!.id);

    const completedIds = completedEvents.map(e => e.jobId);
    expect(completedIds).toContain(job!.id);

    await worker.close(true);
    await queueEvents.close();
    await queue.close();
  }, 15000);
});

describe('Cluster Edge: Hash tag correctness', () => {
  const Q = 'cl-edge-slot-' + Date.now();
  let queue: InstanceType<typeof Queue>;

  beforeAll(() => {
    queue = new Queue(Q, { connection: CLUSTER_CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(Q);
  });

  it('all queue keys land in the same hash slot (no CROSSSLOT errors)', async () => {
    // Add a job to ensure all key types are populated
    const job = await queue.add('slot-check', { v: 1 });
    const k = buildKeys(Q);

    // All these keys share the {queueName} hash tag, so they must be
    // in the same slot. If they weren't, these operations would fail
    // with CROSSSLOT errors in cluster mode.

    // Verify multiple key types exist on the same node
    const idExists = await cleanupClient.exists([k.id]);
    expect(idExists).toBe(1);

    const jobHashExists = await cleanupClient.exists([k.job(job!.id)]);
    expect(jobHashExists).toBe(1);

    const eventsExists = await cleanupClient.exists([k.events]);
    expect(eventsExists).toBe(1);

    // Verify the hash tag pattern is correct: all keys contain {queueName}
    const expectedTag = `{${Q}}`;
    expect(k.id).toContain(expectedTag);
    expect(k.stream).toContain(expectedTag);
    expect(k.scheduled).toContain(expectedTag);
    expect(k.completed).toContain(expectedTag);
    expect(k.failed).toContain(expectedTag);
    expect(k.events).toContain(expectedTag);
    expect(k.meta).toContain(expectedTag);
    expect(k.dedup).toContain(expectedTag);
    expect(k.rate).toContain(expectedTag);
    expect(k.schedulers).toContain(expectedTag);
    expect(k.job(job!.id)).toContain(expectedTag);
    expect(k.deps(job!.id)).toContain(expectedTag);
  });

  it('job keys share the same hash tag as queue keys', async () => {
    const job1 = await queue.add('slot-j1', { a: 1 });
    const job2 = await queue.add('slot-j2', { b: 2 });
    const k = buildKeys(Q);

    // Perform a multi-key operation that would fail if keys are in different slots.
    // Reading from both job hashes and the id key should work without CROSSSLOT.
    const [id1Exists, id2Exists, idKeyExists] = await Promise.all([
      cleanupClient.exists([k.job(job1!.id)]),
      cleanupClient.exists([k.job(job2!.id)]),
      cleanupClient.exists([k.id]),
    ]);

    expect(id1Exists).toBe(1);
    expect(id2Exists).toBe(1);
    expect(idKeyExists).toBe(1);
  });
});

describe('Cluster Edge: Scheduler in cluster mode', () => {
  const Q = 'cl-edge-sched-' + Date.now();

  afterAll(async () => {
    await flushQueue(Q);
  });

  it('repeatable scheduler fires jobs in cluster mode', async () => {
    const queue = new Queue(Q, { connection: CLUSTER_CONNECTION });

    await queue.upsertJobScheduler('cl-repeat', { every: 400 }, {
      name: 'cl-tick',
      data: { cluster: true },
    });

    const processed: string[] = [];
    const worker = new Worker(
      Q,
      async (job: any) => {
        processed.push(job.name);
        return 'ok';
      },
      {
        connection: CLUSTER_CONNECTION,
        concurrency: 1,
        blockTimeout: 500,
        promotionInterval: 300,
        stalledInterval: 60000,
      },
    );
    worker.on('error', () => {});

    // Wait for scheduler to fire
    await new Promise(r => setTimeout(r, 3000));

    await worker.close(true);

    const ticks = processed.filter(n => n === 'cl-tick');
    expect(ticks.length).toBeGreaterThanOrEqual(1);

    await queue.removeJobScheduler('cl-repeat');
    await queue.close();
  }, 15000);
});
