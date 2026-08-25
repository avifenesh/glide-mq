/**
 * Edge-case tests for Queue operations and data integrity.
 * Runs against both standalone (:6379) and cluster (:7000).
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { describeEachMode, createCleanupClient, flushQueue, waitFor } from './helpers/fixture';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');
const { completeAndFetchNext, moveToActive, popLists, rpopAndReserve } =
  require('../dist/functions') as typeof import('../src/functions');

describeEachMode('Edge: Queue', (CONNECTION) => {
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    cleanupClient.close();
  });

  // ---------------------------------------------------------------------------
  // 1. FIFO ordering: add 20 jobs sequentially, verify worker processes in order
  // ---------------------------------------------------------------------------
  describe('FIFO ordering', () => {
    const Q = 'edge-fifo-' + Date.now();
    const processed: string[] = [];

    afterAll(async () => {
      await flushQueue(cleanupClient, Q);
    });

    it('processes 20 jobs in FIFO order', async () => {
      const queue = new Queue(Q, { connection: CONNECTION });

      const addedIds: string[] = [];
      for (let i = 0; i < 20; i++) {
        const job = await queue.add('fifo', { seq: i });
        addedIds.push(job!.id);
      }

      const done = new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error('timeout')), 15000);
        const worker = new Worker(
          Q,
          async (job: any) => {
            processed.push(job.id);
            if (processed.length >= 20) {
              clearTimeout(timeout);
              setTimeout(() => worker.close(true).then(resolve), 200);
            }
            return 'ok';
          },
          { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
        );
        worker.on('error', () => {});
      });

      await done;
      await queue.close();

      expect(processed).toHaveLength(20);
      expect(processed).toEqual(addedIds);
    }, 20000);
  });

  // ---------------------------------------------------------------------------
  // 2. FIFO is the default (LIFO is not used)
  // ---------------------------------------------------------------------------
  describe('Default ordering is FIFO (not LIFO)', () => {
    const Q = 'edge-fifo-default-' + Date.now();

    afterAll(async () => {
      await flushQueue(cleanupClient, Q);
    });

    it('first job added is the first job processed', async () => {
      const queue = new Queue(Q, { connection: CONNECTION });

      const first = await queue.add('first', { order: 1 });
      await queue.add('second', { order: 2 });
      await queue.add('third', { order: 3 });

      const processed: string[] = [];
      const done = new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
        const worker = new Worker(
          Q,
          async (job: any) => {
            processed.push(job.id);
            if (processed.length >= 3) {
              clearTimeout(timeout);
              setTimeout(() => worker.close(true).then(resolve), 200);
            }
            return 'ok';
          },
          { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
        );
        worker.on('error', () => {});
      });

      await done;
      await queue.close();

      expect(processed[0]).toBe(first!.id);
    }, 15000);
  });

  // ---------------------------------------------------------------------------
  // 3. Large job data: 100KB JSON payload roundtrips correctly
  // ---------------------------------------------------------------------------
  describe('Large job data', () => {
    const Q = 'edge-large-' + Date.now();
    let queue: InstanceType<typeof Queue>;

    beforeAll(() => {
      queue = new Queue(Q, { connection: CONNECTION });
    });

    afterAll(async () => {
      await queue.close();
      await flushQueue(cleanupClient, Q);
    });

    it('100KB JSON payload roundtrips correctly', async () => {
      const largeArray: string[] = [];
      for (let i = 0; i < 2000; i++) {
        largeArray.push('x'.repeat(50) + '-' + i);
      }
      const payload = { items: largeArray, nested: { deep: { value: 42 } } };
      const payloadSize = JSON.stringify(payload).length;
      expect(payloadSize).toBeGreaterThan(100_000);

      const job = await queue.add('large', payload);
      expect(job).not.toBeNull();

      const fetched = await queue.getJob(job!.id);
      expect(fetched).not.toBeNull();
      expect(fetched!.data).toEqual(payload);
    });
  });

  // ---------------------------------------------------------------------------
  // 4. Jobs with empty data, null-ish values, special characters in name
  // ---------------------------------------------------------------------------
  describe('Job edge cases: empty data, special chars', () => {
    const Q = 'edge-special-' + Date.now();
    let queue: InstanceType<typeof Queue>;

    beforeAll(() => {
      queue = new Queue(Q, { connection: CONNECTION });
    });

    afterAll(async () => {
      await queue.close();
      await flushQueue(cleanupClient, Q);
    });

    it('job with empty object data', async () => {
      const job = await queue.add('empty', {});
      const fetched = await queue.getJob(job!.id);
      expect(fetched).not.toBeNull();
      expect(fetched!.data).toEqual({});
    });

    it('job with null-ish values in data', async () => {
      const data = { a: null, b: 0, c: '', d: false, e: undefined };
      const job = await queue.add('nullish', data);
      const fetched = await queue.getJob(job!.id);
      expect(fetched).not.toBeNull();
      expect(fetched!.data.a).toBeNull();
      expect(fetched!.data.b).toBe(0);
      expect(fetched!.data.c).toBe('');
      expect(fetched!.data.d).toBe(false);
      expect(fetched!.data.e).toBeUndefined();
    });

    it('job with special characters in name', async () => {
      const name = 'email:send/batch#1@org.com';
      const job = await queue.add(name, { x: 1 });
      const fetched = await queue.getJob(job!.id);
      expect(fetched).not.toBeNull();
      expect(fetched!.name).toBe(name);
    });

    it('job with unicode characters in name and data', async () => {
      const name = 'task-\u00e9\u00e0\u00fc-\u4e16\u754c-\ud83d\ude00';
      const data = { message: '\u4f60\u597d\u4e16\u754c', emoji: '\ud83d\ude80\ud83c\udf1f' };
      const job = await queue.add(name, data);
      const fetched = await queue.getJob(job!.id);
      expect(fetched).not.toBeNull();
      expect(fetched!.name).toBe(name);
      expect(fetched!.data).toEqual(data);
    });
  });

  // ---------------------------------------------------------------------------
  // 5. Job with every option set simultaneously
  // ---------------------------------------------------------------------------
  describe('Job with all options set simultaneously', () => {
    const Q = 'edge-allopts-' + Date.now();
    let queue: InstanceType<typeof Queue>;

    beforeAll(() => {
      queue = new Queue(Q, { connection: CONNECTION });
    });

    afterAll(async () => {
      await queue.close();
      await flushQueue(cleanupClient, Q);
    });

    it('accepts a job with delay + priority + attempts + backoff + removeOnComplete + dedup', async () => {
      const job = await queue.add(
        'all-opts',
        { v: 1 },
        {
          delay: 5000,
          priority: 3,
          attempts: 5,
          backoff: { type: 'exponential', delay: 1000, jitter: 0.1 },
          removeOnComplete: true,
          deduplication: { id: 'combo-1', ttl: 60000, mode: 'throttle' },
        },
      );

      expect(job).not.toBeNull();
      expect(job!.id).toBeTruthy();

      const fetched = await queue.getJob(job!.id);
      expect(fetched).not.toBeNull();
      expect(fetched!.name).toBe('all-opts');
      expect(fetched!.data).toEqual({ v: 1 });

      const k = buildKeys(Q);
      const score = await cleanupClient.zscore(k.scheduled, job!.id);
      expect(score).not.toBeNull();
    });
  });

  // ---------------------------------------------------------------------------
  // 6. Concurrent Queue.add from 3 separate Queue instances
  // ---------------------------------------------------------------------------
  describe('Concurrent adds from multiple Queue instances', () => {
    const Q = 'edge-concurrent-' + Date.now();
    const queues: InstanceType<typeof Queue>[] = [];

    afterAll(async () => {
      for (const q of queues) {
        await q.close();
      }
      await flushQueue(cleanupClient, Q);
    });

    it('all jobs created with unique IDs from 3 concurrent producers', async () => {
      const q1 = new Queue(Q, { connection: CONNECTION });
      const q2 = new Queue(Q, { connection: CONNECTION });
      const q3 = new Queue(Q, { connection: CONNECTION });
      queues.push(q1, q2, q3);

      const JOBS_PER_QUEUE = 10;

      const [jobs1, jobs2, jobs3] = await Promise.all([
        Promise.all(Array.from({ length: JOBS_PER_QUEUE }, (_, i) => q1.add(`q1-${i}`, { src: 1, i }))),
        Promise.all(Array.from({ length: JOBS_PER_QUEUE }, (_, i) => q2.add(`q2-${i}`, { src: 2, i }))),
        Promise.all(Array.from({ length: JOBS_PER_QUEUE }, (_, i) => q3.add(`q3-${i}`, { src: 3, i }))),
      ]);

      const allJobs = [...jobs1, ...jobs2, ...jobs3].filter(Boolean);
      expect(allJobs).toHaveLength(JOBS_PER_QUEUE * 3);

      const ids = allJobs.map((j) => j!.id);
      const uniqueIds = new Set(ids);
      expect(uniqueIds.size).toBe(JOBS_PER_QUEUE * 3);
    });
  });

  // ---------------------------------------------------------------------------
  // 7. Queue.getJob on completed/failed job - data still accessible
  // ---------------------------------------------------------------------------
  describe('getJob on completed/failed jobs', () => {
    const Q = 'edge-getjob-state-' + Date.now();

    afterAll(async () => {
      await flushQueue(cleanupClient, Q);
    });

    it('completed job data is accessible via getJob', async () => {
      const queue = new Queue(Q, { connection: CONNECTION });
      const jobData = { important: 'payload', count: 42 };
      const job = await queue.add('state-test', jobData);

      const done = new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
        const worker = new Worker(
          Q,
          async () => {
            return { result: 'success' };
          },
          { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
        );
        worker.on('completed', () => {
          clearTimeout(timeout);
          worker.close(true).then(resolve);
        });
        worker.on('error', () => {});
      });

      await done;

      const fetched = await queue.getJob(job!.id);
      expect(fetched).not.toBeNull();
      expect(fetched!.data).toEqual(jobData);
      expect(fetched!.name).toBe('state-test');
      expect(fetched!.returnvalue).toEqual({ result: 'success' });

      await queue.close();
    }, 15000);

    it('completed job with removeOnComplete=true is removed', async () => {
      const qName = Q + '-roc';
      const queue = new Queue(qName, { connection: CONNECTION });
      const job = await queue.add('remove-me', { x: 1 }, { removeOnComplete: true });

      const done = new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
        const worker = new Worker(
          qName,
          async () => {
            return 'done';
          },
          { connection: CONNECTION, concurrency: 1, blockTimeout: 1000 },
        );
        worker.on('completed', () => {
          clearTimeout(timeout);
          worker.close(true).then(resolve);
        });
        worker.on('error', () => {});
      });

      await done;

      await new Promise((r) => setTimeout(r, 200));

      const fetched = await queue.getJob(job!.id);
      expect(fetched).toBeNull();

      await queue.close();
      await flushQueue(cleanupClient, qName);
    }, 15000);
  });

  // ---------------------------------------------------------------------------
  // 8. Queue.pause then add job - job added but not processed until resume
  // ---------------------------------------------------------------------------
  describe('Pause blocks processing, resume allows it', () => {
    const Q = 'edge-pause-add-' + Date.now();

    afterAll(async () => {
      await flushQueue(cleanupClient, Q);
    });

    it('job added during pause is not processed until resume', async () => {
      const queue = new Queue(Q, { connection: CONNECTION });
      const processed: string[] = [];

      await queue.pause();

      const k = buildKeys(Q);
      expect(String(await cleanupClient.hget(k.meta, 'paused'))).toBe('1');

      const fifo = await queue.add('paused-fifo', { v: 1 });
      const priority = await queue.add('paused-priority', { v: 2 }, { priority: 1 });
      const lifo = await queue.add('paused-lifo', { v: 3 }, { lifo: true });
      expect(fifo).not.toBeNull();
      expect(priority).not.toBeNull();
      expect(lifo).not.toBeNull();

      const worker = new Worker(
        Q,
        async (j: any) => {
          processed.push(j.id);
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 1, blockTimeout: 200, stalledInterval: 60000 },
      );
      worker.on('error', () => {});

      await new Promise((r) => setTimeout(r, 1500));
      expect(processed).toHaveLength(0);

      const fifoState = await fifo!.getState();
      const priorityState = await priority!.getState();
      const lifoState = await lifo!.getState();
      expect(fifoState).not.toBe('active');
      expect(fifoState).not.toBe('completed');
      expect(priorityState).not.toBe('active');
      expect(priorityState).not.toBe('completed');
      expect(lifoState).not.toBe('active');
      expect(lifoState).not.toBe('completed');

      await queue.resume();

      await waitFor(() => processed.length >= 3, 8000);
      expect(processed).toHaveLength(3);
      expect(processed).toEqual(expect.arrayContaining([fifo!.id, priority!.id, lifo!.id]));

      await worker.close(true);
      await queue.close();
    }, 20000);
  });

  describe('Pause-race defer restores original placement', () => {
    const Q = 'edge-pause-defer-' + Date.now();

    afterAll(async () => {
      await flushQueue(cleanupClient, Q);
      await flushQueue(cleanupClient, Q + '-bc');
    });

    it('restores priority and LIFO jobs to their lists and FIFO jobs to the stream', async () => {
      const k = buildKeys(Q);
      // Priority lists are consumed with RPOP, so priority 1 must return ahead
      // of an already-waiting priority 2 job after the pause-race restore.
      await cleanupClient.hset(k.job('pri-2'), {
        name: 'waiting-priority',
        state: 'waiting',
        priority: '2',
      });
      await cleanupClient.rpush(k.priority, 'pri-2');
      await cleanupClient.hset(k.job('pri-1'), {
        name: 'paused-priority',
        state: 'active',
        priority: '1',
      });
      await cleanupClient.set(k.listActive, '1');
      await cleanupClient.fcall(
        'glidemq_deferActive',
        [k.stream, k.job('pri-1'), k.listActive],
        ['pri-1', '', 'workers', '0', '1'],
      );
      expect(Number(await cleanupClient.get(k.listActive))).toBe(0);
      expect(Number(await cleanupClient.llen(k.priority))).toBe(2);
      expect(String(await cleanupClient.rpop(k.priority))).toBe('pri-1');
      expect(Number(await cleanupClient.xlen(k.stream))).toBe(0);
      expect(String(await cleanupClient.hget(k.job('pri-1'), 'state'))).toBe('waiting');

      await cleanupClient.hset(k.job('lifo-1'), {
        name: 'paused-lifo',
        state: 'active',
        lifo: '1',
      });
      await cleanupClient.set(k.listActive, '1');
      await cleanupClient.fcall(
        'glidemq_deferActive',
        [k.stream, k.job('lifo-1'), k.listActive],
        ['lifo-1', '', 'workers', '0', '1'],
      );
      expect(Number(await cleanupClient.get(k.listActive))).toBe(0);
      expect(Number(await cleanupClient.llen(k.lifo))).toBe(1);
      expect(Number(await cleanupClient.xlen(k.stream))).toBe(0);
      expect(String(await cleanupClient.hget(k.job('lifo-1'), 'state'))).toBe('waiting');

      await cleanupClient.hset(k.job('fifo-1'), {
        name: 'paused-fifo',
        state: 'active',
      });
      await cleanupClient.set(k.listActive, '1');
      await cleanupClient.fcall(
        'glidemq_deferActive',
        [k.stream, k.job('fifo-1'), k.listActive],
        ['fifo-1', '', 'workers', '0', '1'],
      );
      expect(Number(await cleanupClient.get(k.listActive))).toBe(0);
      expect(Number(await cleanupClient.xlen(k.stream))).toBe(1);
      expect(String(await cleanupClient.hget(k.job('fifo-1'), 'state'))).toBe('waiting');

      // A non-pause defer remains the normal FIFO fallback for list claims.
      await cleanupClient.hset(k.job('default-fifo-1'), {
        name: 'default-fifo',
        state: 'active',
      });
      await cleanupClient.set(k.listActive, '1');
      await cleanupClient.fcall(
        'glidemq_deferActive',
        [k.stream, k.job('default-fifo-1'), k.listActive],
        ['default-fifo-1', '', 'workers', '0'],
      );
      expect(Number(await cleanupClient.get(k.listActive))).toBe(0);
      expect(Number(await cleanupClient.xlen(k.stream))).toBe(2);
      expect(String(await cleanupClient.hget(k.job('default-fifo-1'), 'state'))).toBe('waiting');
    });

    it('does not XADD a duplicate when a paused broadcast claim is deferred', async () => {
      const k = buildKeys(Q + '-bc');
      await cleanupClient.hset(k.job('bc-1'), { name: 'message', state: 'active' });
      const entryId = await cleanupClient.xadd(k.stream, ['jobId', 'bc-1', 'name', 'message']);
      await cleanupClient.xgroupCreate(k.stream, 'sub-a', '0', { mkStream: true });
      await cleanupClient.xgroupCreate(k.stream, 'sub-b', '0');
      await cleanupClient.xreadgroup('sub-a', 'c1', { [k.stream]: '>' }, { count: 1 });
      await cleanupClient.xreadgroup('sub-b', 'c1', { [k.stream]: '>' }, { count: 1 });
      const before = Number(await cleanupClient.xlen(k.stream));
      expect(before).toBe(1);

      await cleanupClient.fcall(
        'glidemq_deferActive',
        [k.stream, k.job('bc-1'), k.listActive],
        ['bc-1', String(entryId), 'sub-a', '1', '1'],
      );

      expect(Number(await cleanupClient.xlen(k.stream))).toBe(1);
      expect(Number((await cleanupClient.xpending(k.stream, 'sub-a'))[0])).toBe(1);
      expect(Number((await cleanupClient.xpending(k.stream, 'sub-b'))[0])).toBe(1);
    });

    it('blocks every Lua activation path while paused', async () => {
      const qName = Q + '-lua-activation';
      const queue = new Queue(qName, { connection: CONNECTION });
      const k = buildKeys(qName);
      const fifo = await queue.add('fifo', { n: 1 });
      const current = await queue.add('current', { n: 3 });
      const priorityId = 'paused-priority';
      await cleanupClient.hset(k.job(priorityId), { name: 'priority', state: 'waiting', priority: '1' });
      await cleanupClient.rpush(k.priority, priorityId);

      expect(fifo).not.toBeNull();
      expect(current).not.toBeNull();

      // Activate the current job before pausing. A subsequent fast-path
      // completion must not claim the priority job behind it.
      expect(await moveToActive(cleanupClient, k, current!.id, Date.now(), k.stream, '', 'workers')).toMatchObject({
        state: 'active',
      });
      await queue.pause();

      expect(await moveToActive(cleanupClient, k, fifo!.id, Date.now(), k.stream, '', 'workers')).toBe('PAUSED');
      expect(await popLists(cleanupClient, k, 1)).toEqual([]);
      expect(await rpopAndReserve(cleanupClient, k, k.priority, 'workers', 1)).toEqual([]);
      expect(Number(await cleanupClient.llen(k.priority))).toBe(1);

      const completion = await completeAndFetchNext(
        cleanupClient,
        k,
        current!.id,
        '',
        '"done"',
        Date.now(),
        'workers',
        'pause-coverage',
      );
      expect(completion).toEqual({ completed: current!.id, next: false, parentNotifications: [] });
      expect(String(await cleanupClient.hget(k.job(priorityId), 'state'))).toBe('waiting');

      await queue.close();
      await flushQueue(cleanupClient, qName);
    });

    it('restores a list job after the worker observes a PAUSED activation result', async () => {
      const qName = Q + '-worker-race';
      const queue = new Queue(qName, { connection: CONNECTION });
      const k = buildKeys(qName);
      const worker = new Worker(qName, async () => 'unexpected', {
        connection: CONNECTION,
        blockTimeout: 200,
        stalledInterval: 60000,
      });
      worker.on('error', () => {});
      await worker.waitUntilReady();
      await worker.pause(true);

      const priorityId = 'paused-priority';
      await cleanupClient.hset(k.job(priorityId), { name: 'priority', state: 'waiting', priority: '1' });
      await cleanupClient.rpush(k.priority, priorityId);
      expect(String(await cleanupClient.rpop(k.priority))).toBe(priorityId);
      await cleanupClient.incrBy(k.listActive, 1);
      await queue.pause();

      const activation = await moveToActive(cleanupClient, k, priorityId, Date.now(), k.stream, '', 'workers');
      expect(activation).toBe('PAUSED');
      expect(await (worker as any).handleMoveToActiveEdgeCase(activation, priorityId, '')).toBe(true);

      expect(String(await cleanupClient.rpop(k.priority))).toBe(priorityId);
      expect(Number(await cleanupClient.get(k.listActive))).toBe(0);
      expect(String(await cleanupClient.hget(k.job(priorityId), 'state'))).toBe('waiting');

      await worker.close(true);
      await queue.close();
      await flushQueue(cleanupClient, qName);
    });

    it('preserves dispatch order when a paused batch restores multiple priority claims', async () => {
      const qName = Q + '-batch-order';
      const queue = new Queue(qName, { connection: CONNECTION });
      const k = buildKeys(qName);
      const worker = new Worker(qName, async (jobs: any[]) => jobs.map(() => 'ok'), {
        connection: CONNECTION,
        concurrency: 2,
        batch: { size: 2 },
        blockTimeout: 200,
        stalledInterval: 60000,
      });
      worker.on('error', () => {});
      await worker.waitUntilReady();
      await worker.pause(true);

      await queue.pause();
      await cleanupClient.hset(k.job('existing'), {
        name: 'existing',
        state: 'waiting',
        priority: '1',
      });
      await cleanupClient.rpush(k.priority, 'existing');
      for (const id of ['first', 'second']) {
        await cleanupClient.hset(k.job(id), {
          name: id,
          state: 'waiting',
          priority: '1',
        });
      }
      await cleanupClient.set(k.listActive, '2');

      await (worker as any).activateAndProcessBatch([
        { jobId: 'first', entryId: '' },
        { jobId: 'second', entryId: '' },
      ]);

      expect(Number(await cleanupClient.get(k.listActive))).toBe(0);
      expect(await cleanupClient.lrange(k.priority, 0, -1)).toEqual(['existing', 'second', 'first']);
      expect(String(await cleanupClient.hget(k.job('first'), 'state'))).toBe('waiting');
      expect(String(await cleanupClient.hget(k.job('second'), 'state'))).toBe('waiting');

      await worker.close(true);
      await queue.close();
      await flushQueue(cleanupClient, qName);
    });

    it('does not reclaim paused broadcast claims or list reservations', async () => {
      const qName = Q + '-reclaim-paused';
      const queue = new Queue(qName, { connection: CONNECTION });
      const k = buildKeys(qName);
      const broadcastJobId = 'broadcast-claim';
      const listJobId = 'list-claim';
      const now = Date.now();
      await queue.pause();

      await cleanupClient.hset(k.job(broadcastJobId), {
        name: 'message',
        state: 'active',
        lastActive: String(now - 120000),
      });
      await cleanupClient.xadd(k.stream, ['jobId', broadcastJobId, 'name', 'message']);
      await cleanupClient.xgroupCreate(k.stream, 'paused-sub', '0', { mkStream: true });
      await cleanupClient.xreadgroup('paused-sub', 'consumer', { [k.stream]: '>' }, { count: 1 });

      await cleanupClient.hset(k.job(listJobId), {
        name: 'list',
        state: 'active',
        priority: '1',
        lastActive: String(now - 120000),
      });
      await cleanupClient.set(k.listActive, '1');

      const reclaimed = await cleanupClient.fcall(
        'glidemq_reclaimStalled',
        [k.stream, k.events],
        ['paused-sub', 'scheduler', '1', '1', String(now), k.failed, '1', '1'],
      );
      const reclaimedLists = await cleanupClient.fcall(
        'glidemq_reclaimStalledListJobs',
        [k.stream, k.events],
        ['1', '1', String(now), k.failed, '1'],
      );

      expect(Number(reclaimed)).toBe(0);
      expect(Number(reclaimedLists)).toBe(0);
      expect(Number((await cleanupClient.xpending(k.stream, 'paused-sub'))[0])).toBe(1);
      expect(String(await cleanupClient.hget(k.job(broadcastJobId), 'state'))).toBe('active');
      expect(String(await cleanupClient.hget(k.job(listJobId), 'state'))).toBe('active');
      expect(Number(await cleanupClient.get(k.listActive))).toBe(1);
      expect(await cleanupClient.zscore(k.failed, broadcastJobId)).toBeNull();
      expect(await cleanupClient.zscore(k.failed, listJobId)).toBeNull();

      await queue.close();
      await flushQueue(cleanupClient, qName);
    });
  });

  // ---------------------------------------------------------------------------
  // 9. Queue obliterate: clean all queue data
  // ---------------------------------------------------------------------------
  describe('Queue obliterate (manual cleanup)', () => {
    const Q = 'edge-obliterate-' + Date.now();

    it('all queue keys are removed after flush', async () => {
      const queue = new Queue(Q, { connection: CONNECTION });

      for (let i = 0; i < 5; i++) {
        await queue.add(`job-${i}`, { i });
      }

      await queue.add('delayed', { x: 1 }, { delay: 60000 });

      const k = buildKeys(Q);
      const idExists = await cleanupClient.exists([k.id]);
      expect(idExists).toBe(1);

      await queue.close();
      await new Promise((r) => setTimeout(r, 200));

      await flushQueue(cleanupClient, Q);

      const checks = await Promise.all([
        cleanupClient.exists([k.id]),
        cleanupClient.exists([k.stream]),
        cleanupClient.exists([k.scheduled]),
        cleanupClient.exists([k.events]),
        cleanupClient.exists([k.meta]),
      ]);
      expect(checks.every((c) => c === 0)).toBe(true);

      const jobKey = k.job('1');
      const jobExists = await cleanupClient.exists([jobKey]);
      expect(jobExists).toBe(0);
    });
  });

  // ---------------------------------------------------------------------------
  // 10. Add 100 jobs rapidly, verify all get unique incrementing IDs
  // ---------------------------------------------------------------------------
  describe('Rapid job addition - 100 jobs with unique IDs', () => {
    const Q = 'edge-rapid-' + Date.now();
    let queue: InstanceType<typeof Queue>;

    beforeAll(() => {
      queue = new Queue(Q, { connection: CONNECTION });
    });

    afterAll(async () => {
      await queue.close();
      await flushQueue(cleanupClient, Q);
    });

    it('100 rapid adds yield unique incrementing IDs', async () => {
      // Use concurrency control to prevent overwhelming the socket in CI
      const jobs = [];
      const batchSize = 25;
      for (let i = 0; i < 100; i += batchSize) {
        const batch = await Promise.all(
          Array.from({ length: batchSize }, (_, j) => queue.add(`rapid-${i + j}`, { i: i + j })),
        );
        jobs.push(...batch);
      }

      expect(jobs).toHaveLength(100);
      const ids = jobs.map((j) => j!.id);

      const uniqueIds = new Set(ids);
      expect(uniqueIds.size).toBe(100);

      const numericIds = ids.map(Number);
      expect(numericIds.every((n) => !isNaN(n) && n > 0)).toBe(true);

      const sorted = [...numericIds].sort((a, b) => a - b);
      for (let i = 1; i < sorted.length; i++) {
        expect(sorted[i]).toBeGreaterThan(sorted[i - 1]);
      }
    }, 30000);
  });

  // ---------------------------------------------------------------------------
  // 11. Job data with Buffer/binary-like content (stringify/parse roundtrip)
  // ---------------------------------------------------------------------------
  describe('Binary-like data roundtrip', () => {
    const Q = 'edge-binary-' + Date.now();
    let queue: InstanceType<typeof Queue>;

    beforeAll(() => {
      queue = new Queue(Q, { connection: CONNECTION });
    });

    afterAll(async () => {
      await queue.close();
      await flushQueue(cleanupClient, Q);
    });

    it('base64-encoded binary data roundtrips correctly', async () => {
      const binaryLike = Buffer.from([0x00, 0x01, 0xff, 0xfe, 0x80, 0x7f, 0xab, 0xcd]);
      const payload = {
        type: 'binary',
        data: binaryLike.toString('base64'),
        length: binaryLike.length,
      };

      const job = await queue.add('binary', payload);
      const fetched = await queue.getJob(job!.id);
      expect(fetched).not.toBeNull();
      expect(fetched!.data).toEqual(payload);

      const reconstructed = Buffer.from(fetched!.data.data, 'base64');
      expect(reconstructed).toEqual(binaryLike);
    });

    it('large nested JSON with array buffers roundtrips', async () => {
      const payload = {
        header: { version: 1, encoding: 'base64' },
        chunks: Array.from({ length: 50 }, (_, i) => ({
          index: i,
          data: Buffer.from(Array.from({ length: 100 }, () => Math.floor(Math.random() * 256))).toString('base64'),
        })),
      };

      const job = await queue.add('chunks', payload);
      const fetched = await queue.getJob(job!.id);
      expect(fetched).not.toBeNull();
      expect(fetched!.data.chunks).toHaveLength(50);
      expect(fetched!.data.header).toEqual(payload.header);
    });
  });

  // ---------------------------------------------------------------------------
  // 12. Queue with custom prefix - verify key namespacing
  // ---------------------------------------------------------------------------
  describe('Queue with custom prefix', () => {
    const Q = 'edge-prefix-' + Date.now();
    const CUSTOM_PREFIX = 'myapp';

    afterAll(async () => {
      await flushQueue(cleanupClient, Q, CUSTOM_PREFIX);
    });

    it('uses custom prefix in all keys', async () => {
      const queue = new Queue(Q, { connection: CONNECTION, prefix: CUSTOM_PREFIX });
      const job = await queue.add('prefixed', { v: 1 });
      expect(job).not.toBeNull();

      const k = buildKeys(Q, CUSTOM_PREFIX);
      expect(k.id).toBe(`${CUSTOM_PREFIX}:{${Q}}:id`);
      expect(k.stream).toBe(`${CUSTOM_PREFIX}:{${Q}}:stream`);

      const idVal = await cleanupClient.get(k.id);
      expect(idVal).not.toBeNull();
      expect(Number(idVal)).toBeGreaterThanOrEqual(1);

      const jobKey = k.job(job!.id);
      expect(jobKey).toContain(CUSTOM_PREFIX);
      const jobExists = await cleanupClient.exists([jobKey]);
      expect(jobExists).toBe(1);

      const defaultK = buildKeys(Q);
      const defaultIdExists = await cleanupClient.exists([defaultK.id]);
      expect(defaultIdExists).toBe(0);

      await queue.close();
    });

    it('two queues with different prefixes are isolated', async () => {
      const q1 = new Queue(Q + '-iso', { connection: CONNECTION, prefix: 'prefix-a' });
      const q2 = new Queue(Q + '-iso', { connection: CONNECTION, prefix: 'prefix-b' });

      const job1 = await q1.add('from-a', { src: 'a' });
      const job2 = await q2.add('from-b', { src: 'b' });

      const k1 = buildKeys(Q + '-iso', 'prefix-a');
      const k2 = buildKeys(Q + '-iso', 'prefix-b');

      const id1 = await cleanupClient.get(k1.id);
      const id2 = await cleanupClient.get(k2.id);
      expect(id1).not.toBeNull();
      expect(id2).not.toBeNull();

      const j1Exists = await cleanupClient.exists([k1.job(job1!.id)]);
      const j2Exists = await cleanupClient.exists([k2.job(job2!.id)]);
      expect(j1Exists).toBe(1);
      expect(j2Exists).toBe(1);

      expect(k1.job(job1!.id)).not.toBe(k2.job(job1!.id));

      await q1.close();
      await q2.close();
      await flushQueue(cleanupClient, Q + '-iso', 'prefix-a');
      await flushQueue(cleanupClient, Q + '-iso', 'prefix-b');
    });
  });

  // ---------------------------------------------------------------------------
  // Additional: getJobCounts accuracy
  // ---------------------------------------------------------------------------
  describe('getJobCounts reflects correct state', () => {
    const Q = 'edge-counts-' + Date.now();

    afterAll(async () => {
      await flushQueue(cleanupClient, Q);
    });

    it('counts waiting, completed, and delayed jobs correctly', async () => {
      const queue = new Queue(Q, { connection: CONNECTION });

      const initial = await queue.getJobCounts();
      expect(initial.waiting).toBe(0);
      expect(initial.completed).toBe(0);
      expect(initial.delayed).toBe(0);

      for (let i = 0; i < 3; i++) {
        await queue.add(`count-${i}`, { i });
      }

      await queue.add('delayed-1', { d: 1 }, { delay: 60000 });
      await queue.add('delayed-2', { d: 2 }, { delay: 60000 });

      const afterAdd = await queue.getJobCounts();
      expect(afterAdd.waiting).toBe(3);
      expect(afterAdd.delayed).toBe(2);

      const done = new Promise<void>((resolve, reject) => {
        let completed = 0;
        const timeout = setTimeout(() => reject(new Error('timeout')), 10000);
        const worker = new Worker(Q, async () => 'ok', { connection: CONNECTION, concurrency: 3, blockTimeout: 1000 });
        worker.on('completed', () => {
          completed++;
          if (completed >= 3) {
            clearTimeout(timeout);
            worker.close(true).then(resolve);
          }
        });
        worker.on('error', () => {});
      });

      await done;

      const afterProcess = await queue.getJobCounts();
      expect(afterProcess.completed).toBe(3);
      expect(afterProcess.waiting).toBe(0);
      expect(afterProcess.delayed).toBe(2);

      await queue.close();
    }, 15000);
  });
});
