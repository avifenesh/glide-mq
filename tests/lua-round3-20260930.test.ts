/**
 * Server-function and worker gap regressions, round 3 (2026-09-30).
 *
 * Run: flock /tmp/gmq-test.lock npx vitest run tests/lua-round3-20260930.test.ts
 */
import { afterAll, beforeAll, expect, it } from 'vitest';
import { createCleanupClient, describeEachMode, flushQueue, waitFor } from './helpers/fixture';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { BroadcastWorker } = require('../dist/broadcast-worker') as typeof import('../src/broadcast-worker');
const { Broadcast } = require('../dist/broadcast') as typeof import('../src/broadcast');
const { buildKeys, keyPrefix } = require('../dist/utils') as typeof import('../src/utils');
const {
  addJob,
  completeAndFetchNext,
  completeChild,
  completeJob,
  deferActive,
  healEarlyDeps,
  healListActive,
  moveToActive,
  reclaimStalledWithIds,
  recoverBroadcastClaims,
  removeIdleConsumer,
  CONSUMER_GROUP,
} = require('../dist/functions') as typeof import('../src/functions');

describeEachMode('Lua round 3 2026-09-30', (CONNECTION) => {
  let cleanupClient: any;
  const queues: string[] = [];

  function uniqueQueue(prefix: string): string {
    const name = `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    queues.push(name);
    return name;
  }

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await Promise.all(queues.map((q) => flushQueue(cleanupClient, q).catch(() => {})));
    cleanupClient.close();
  });

  // Item 1: a job fired by an every scheduler is still running when the
  // scheduler is switched to repeatAfterComplete and the old nextRun passes.
  // The first repeatAfterComplete run must wait for that job.
  it('mode switch to repeatAfterComplete waits for the in-flight job of the old mode', async () => {
    const Q = uniqueQueue('r3-sched-switch');
    const queue = new Queue(Q, { connection: CONNECTION });
    const k = buildKeys(Q);
    const starts: { id: string; at: number }[] = [];
    const ends: { id: string; at: number }[] = [];
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const worker = new Worker(
      Q,
      async (job) => {
        starts.push({ id: job.id, at: Date.now() });
        if (starts.length === 1) await gate;
        ends.push({ id: job.id, at: Date.now() });
      },
      { connection: CONNECTION, concurrency: 4, promotionInterval: 100 },
    );
    try {
      await queue.upsertJobScheduler('sw', { every: 700 }, { name: 'sw' });
      await waitFor(() => starts.length === 1, 5000);
      const firstId = starts[0].id;
      const before = await queue.getJobScheduler('sw');
      expect(before!.inflightJobId).toBe(firstId);

      await queue.upsertJobScheduler('sw', { repeatAfterComplete: 100 }, { name: 'sw' });
      // Past the old nextRun: the tick must park the entry instead of firing.
      await waitFor(async () => (await queue.getJobScheduler('sw'))!.nextRun === 0, 5000);
      await new Promise((r) => setTimeout(r, 400));
      expect(starts.length).toBe(1);

      release();
      await waitFor(() => starts.length >= 2, 5000);
      expect(starts[1].at).toBeGreaterThanOrEqual(ends[0].at + 100 - 5);
      const after = await queue.getJobScheduler('sw');
      expect(after!.repeatAfterComplete).toBe(100);
      // The old-mode job carries the scheduler name so its completion advances the chain.
      expect(String(await cleanupClient.hget(`${k.id.slice(0, -2)}job:${firstId}`, 'schedulerName'))).toBe('sw');
    } finally {
      release();
      await queue.removeJobScheduler('sw').catch(() => {});
      await worker.close();
      await queue.close();
    }
  }, 20000);

  // Item 2: a job failed for cost over capacity while completeAndFetchNext
  // fetches the next job gets its DLQ copy like a moveToActive failure.
  it('completeAndFetchNext reports jobs failed at activation and the worker adds the DLQ copy', async () => {
    const Q = uniqueQueue('r3-caf-dlq');
    const DLQ = uniqueQueue('r3-caf-dlq-dead');
    const k = buildKeys(Q);
    const queue = new Queue(Q, { connection: CONNECTION });
    const dlq = new Queue(DLQ, { connection: CONNECTION });
    const processed: string[] = [];
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const ordering = { key: 'g', concurrency: 4, tokenBucket: { capacity: 10, refillRate: 100 } };
    const worker = new Worker(
      Q,
      async (job) => {
        processed.push(job.id);
        if (job.name === 'first') await gate;
      },
      { connection: CONNECTION, concurrency: 1, deadLetterQueue: { name: DLQ } },
    );
    try {
      const first = await queue.add('first', {}, { ordering });
      await waitFor(() => processed.includes(first!.id), 5000);
      const costly = await queue.add('costly', { n: 2 }, { ordering, cost: 5 });
      // The capacity shrinks under the waiting job; its activation in the
      // completeAndFetchNext of 'first' fails it.
      await cleanupClient.hset(k.group('g'), { tbCapacity: '1', tbTokens: '1' });
      release();
      await waitFor(async () => (await cleanupClient.hget(k.job(costly!.id), 'state')) === 'failed', 5000);
      await waitFor(async () => (await dlq.getJobCounts()).waiting === 1, 5000);
      expect(processed).toEqual([first!.id]);
      const [dead] = await dlq.getJobs('waiting');
      expect(dead.name).toBe('costly');
      expect(dead.data).toMatchObject({
        originalQueue: Q,
        originalJobId: costly!.id,
        data: { n: 2 },
        failedReason: 'cost exceeds token bucket capacity',
      });
    } finally {
      release();
      await worker.close();
      await dlq.close();
      await queue.close();
    }
  }, 20000);

  // Item 3: two workers read the stream at the same time under
  // globalConcurrency 1. Activation ranks the pending claims; the newer one
  // is refused and handed back instead of running as a second active job.
  it('moveToActive enforces globalConcurrency over concurrent stream claims', async () => {
    const Q = uniqueQueue('r3-gc-claim');
    const k = buildKeys(Q);
    const queue = new Queue(Q, { connection: CONNECTION });
    try {
      await queue.setGlobalConcurrency(1);
      const j1 = await queue.add('a', {});
      const j2 = await queue.add('b', {});
      await cleanupClient.xgroupCreate(k.stream, CONSUMER_GROUP, '0', { mkStream: true }).catch(() => {});
      const readEntry = async (consumer: string): Promise<string> => {
        const res = await cleanupClient.xreadgroup(CONSUMER_GROUP, consumer, { [k.stream]: '>' }, { count: 1 });
        return String(Object.keys(res[0].value)[0]);
      };
      const e1 = await readEntry('w1');
      const e2 = await readEntry('w2');
      const activate = (jobId: string, entryId: string) =>
        moveToActive(cleanupClient, k, jobId, Date.now(), k.stream, entryId, CONSUMER_GROUP, undefined, true);

      // The newer claim is outside the cap whether it activates first or second.
      expect(await activate(j2!.id, e2)).toBe('GLOBAL_FULL');
      expect(typeof (await activate(j1!.id, e1))).toBe('object');
      expect(await activate(j2!.id, e2)).toBe('GLOBAL_FULL');
      expect(await cleanupClient.hget(k.job(j2!.id), 'state')).toBe('waiting');

      // Once the older job completes the slot is free.
      await completeJob(cleanupClient, k, j1!.id, e1, 'null', Date.now(), CONSUMER_GROUP);
      expect(typeof (await activate(j2!.id, e2))).toBe('object');

      // Without the flag (older workers) the gate is off.
      const j3 = await queue.add('c', {});
      const e3 = await readEntry('w1');
      expect(typeof (await moveToActive(cleanupClient, k, j3!.id, Date.now(), k.stream, e3, CONSUMER_GROUP))).toBe(
        'object',
      );
    } finally {
      await queue.close();
    }
  });

  it('workers sharing globalConcurrency 1 never run two jobs at once', async () => {
    const Q = uniqueQueue('r3-gc-workers');
    const queue = new Queue(Q, { connection: CONNECTION });
    let active = 0;
    let maxActive = 0;
    const done: string[] = [];
    const processor = async (job: any) => {
      active++;
      maxActive = Math.max(maxActive, active);
      await new Promise((r) => setTimeout(r, 120));
      active--;
      done.push(job.id);
    };
    const opts = { connection: CONNECTION, concurrency: 3, blockTimeout: 200 };
    await queue.setGlobalConcurrency(1);
    const w1 = new Worker(Q, processor, opts);
    const w2 = new Worker(Q, processor, opts);
    try {
      const jobs = await Promise.all(Array.from({ length: 6 }, (_, i) => queue.add('j', { i })));
      await waitFor(() => done.length === jobs.length, 15000);
      expect(maxActive).toBe(1);
    } finally {
      await Promise.all([w1.close(), w2.close()]);
      await queue.close();
    }
  }, 20000);

  async function consumerNames(streamKey: string, group: string): Promise<string[]> {
    const info = await cleanupClient.xinfoConsumers(streamKey, group);
    return info.map((c: any) => String(c.name));
  }

  // Item 4: a graceful close deletes the consumer once it holds no pending
  // entry; stalled reclaim deletes idle consumers with nothing pending.
  it('graceful close removes the consumer and reclaim deletes idle empty consumers', async () => {
    const Q = uniqueQueue('r3-delconsumer');
    const k = buildKeys(Q);
    const queue = new Queue(Q, { connection: CONNECTION });
    const seen: string[] = [];
    const worker = new Worker(Q, async (job) => void seen.push(job.id), { connection: CONNECTION, blockTimeout: 200 });
    try {
      await queue.add('a', {});
      await waitFor(() => seen.length === 1, 5000);
      const consumerId = (worker as any).consumerId as string;
      expect(await consumerNames(k.stream, CONSUMER_GROUP)).toContain(consumerId);
      await worker.close();
      expect(await consumerNames(k.stream, CONSUMER_GROUP)).not.toContain(consumerId);

      // A consumer holding a pending entry is kept by both paths.
      const held = await queue.add('b', {});
      await cleanupClient.xreadgroup(CONSUMER_GROUP, 'busy', { [k.stream]: '>' }, { count: 1 });
      await cleanupClient.xreadgroup(CONSUMER_GROUP, 'dead-idle', { [k.stream]: '>' }, { count: 1 });
      expect(await removeIdleConsumer(cleanupClient, k, CONSUMER_GROUP, 'busy')).toBe(false);
      await new Promise((r) => setTimeout(r, 30));
      await reclaimStalledWithIds(
        cleanupClient,
        k,
        'rescuer',
        60_000,
        5,
        Date.now(),
        CONSUMER_GROUP,
        false,
        60_000,
        false,
        10,
      );
      const names = await consumerNames(k.stream, CONSUMER_GROUP);
      expect(names).toContain('busy');
      expect(names).not.toContain('dead-idle');
      expect(await cleanupClient.hget(k.job(held!.id), 'state')).toBe('waiting');
    } finally {
      await worker.close().catch(() => {});
      await queue.close();
    }
  }, 15000);

  // Item 5a: stall detection per subscription. Subscription B keeps the shared
  // lastActive fresh while A's claim is dead; A must still reclaim.
  it('broadcast stalled reclaim reads the per-subscription heartbeat', async () => {
    const Q = uniqueQueue('r3-bcast-la');
    const k = buildKeys(Q);
    const bcast = new Broadcast(Q, { connection: CONNECTION });
    try {
      const id = (await bcast.publish('evt', { n: 1 }))!;
      for (const g of ['subA', 'subB']) {
        await cleanupClient.xgroupCreate(k.stream, g, '0', { mkStream: true }).catch(() => {});
      }
      const readEntry = async (group: string, consumer: string): Promise<string> => {
        const res = await cleanupClient.xreadgroup(group, consumer, { [k.stream]: '>' }, { count: 1 });
        return String(Object.keys(res[0].value)[0]);
      };
      const entryA = await readEntry('subA', 'deadA');
      const entryB = await readEntry('subB', 'liveB');
      const tsA = Date.now() - 120_000;
      const hashA = await moveToActive(cleanupClient, k, id, tsA, k.stream, entryA, 'subA', true);
      expect(typeof hashA).toBe('object');
      // Activation writes the per-subscription heartbeat.
      expect(await cleanupClient.hget(`${k.job(id)}:sub:subA`, 'la')).toBe(String(tsA));
      await moveToActive(cleanupClient, k, id, Date.now(), k.stream, entryB, 'subB', true);
      // B heartbeats: shared lastActive is fresh, A's own heartbeat is 2 minutes old.
      await cleanupClient.hset(k.job(id), { lastActive: String(Date.now()) });
      await cleanupClient.hset(`${k.job(id)}:sub:subA`, { la: String(Date.now() - 120_000) });
      await new Promise((r) => setTimeout(r, 20));

      const result = await reclaimStalledWithIds(
        cleanupClient,
        k,
        'rescuerA',
        10,
        5,
        Date.now(),
        'subA',
        true,
        30_000,
        true,
      );
      expect(result.stalledIds).toEqual([id]);
      expect(result.redispatch).toEqual([{ jobId: id, entryId: entryA }]);
      expect(await cleanupClient.hget(`${k.job(id)}:sub:subA`, 's')).toBe('1');
      // B's claim is not stalled: its heartbeat is fresh.
      const resultB = await reclaimStalledWithIds(
        cleanupClient,
        k,
        'rescuerB',
        10,
        5,
        Date.now(),
        'subB',
        true,
        30_000,
        true,
      );
      expect(resultB.stalledIds).toEqual([]);
    } finally {
      await bcast.close();
    }
  });

  // Item 5b: a parked claim (worker paused or closed before running it) is
  // handed back without counting a stall, and only its owner re-takes it.
  it('broadcast hand-back marks the claim, reclaim skips the stall count, recovery keeps ownership', async () => {
    const Q = uniqueQueue('r3-bcast-hb');
    const k = buildKeys(Q);
    const bcast = new Broadcast(Q, { connection: CONNECTION });
    try {
      const id = (await bcast.publish('evt', { n: 1 }))!;
      await cleanupClient.xgroupCreate(k.stream, 'sub', '0', { mkStream: true }).catch(() => {});
      const res = await cleanupClient.xreadgroup('sub', 'w1', { [k.stream]: '>' }, { count: 1 });
      const entryId = String(Object.keys(res[0].value)[0]);
      const subKey = `${k.job(id)}:sub:sub`;

      // Another consumer does not own the claim: no mark.
      await deferActive(cleanupClient, k, id, entryId, 'sub', true, { pausedRestore: true, consumer: 'w2' });
      expect(await cleanupClient.hget(subKey, 'hb')).toBeNull();
      // The owner hands it back.
      await deferActive(cleanupClient, k, id, entryId, 'sub', true, { pausedRestore: true, consumer: 'w1' });
      expect(await cleanupClient.hget(subKey, 'hb')).toBe('1');

      // Reclaim redispatches it without a stall count or stalled event.
      await cleanupClient.hset(k.job(id), { lastActive: '1' });
      await new Promise((r) => setTimeout(r, 20));
      const result = await reclaimStalledWithIds(cleanupClient, k, 'w2', 10, 1, Date.now(), 'sub', true, 30_000, true);
      expect(result.redispatch).toEqual([{ jobId: id, entryId }]);
      expect(result.stalledIds).toEqual([]);
      expect(await cleanupClient.hget(subKey, 's')).toBeNull();
      expect(await cleanupClient.hget(subKey, 'hb')).toBeNull();

      // w1 resumes: the entry now belongs to w2, so w1 must not re-take it.
      expect(await recoverBroadcastClaims(cleanupClient, k, 'sub', 'w1', [entryId])).toEqual({});
      const recovered = await recoverBroadcastClaims(cleanupClient, k, 'sub', 'w2', [entryId]);
      expect(Object.keys(recovered)).toEqual([entryId]);
      expect(recovered[entryId].map(([f]) => String(f))).toContain('jobId');
    } finally {
      await bcast.close();
    }
  });

  // Item 5b (worker level): a BroadcastWorker paused before running a parked
  // message hands it back; a second worker's reclaim does not count a stall.
  it('a paused BroadcastWorker hands parked messages back without a stall', async () => {
    const Q = uniqueQueue('r3-bcast-pause');
    const k = buildKeys(Q);
    const bcast = new Broadcast(Q, { connection: CONNECTION });
    const ran: string[] = [];
    const w1 = new BroadcastWorker(Q, async (job) => void ran.push(`w1:${job.id}`), {
      connection: CONNECTION,
      subscription: 'sub',
      blockTimeout: 200,
      stalledInterval: 100_000,
    });
    let w2: InstanceType<typeof BroadcastWorker> | null = null;
    try {
      await waitFor(async () => (await consumerNames(k.stream, 'sub').catch(() => [])).length === 1, 5000);
      // Park a claim as the queue-pause race would: paused worker, entry in its PEL.
      await w1.pause();
      // The read that was in flight when pause() landed has returned.
      await waitFor(() => (w1 as any).pollLoopPromise == null, 5000);
      const id = (await bcast.publish('evt', { n: 1 }))!;
      const res = await cleanupClient.xreadgroup('sub', (w1 as any).consumerId, { [k.stream]: '>' }, { count: 1 });
      const entryId = String(Object.keys(res[0].value)[0]);
      await (w1 as any).deferPausedActivation({ jobId: id, entryId });
      expect(await cleanupClient.hget(`${k.job(id)}:sub:sub`, 'hb')).toBe('1');
      await cleanupClient.hset(k.job(id), { lastActive: '1' });

      w2 = new BroadcastWorker(Q, async (job) => void ran.push(`w2:${job.id}`), {
        connection: CONNECTION,
        subscription: 'sub',
        blockTimeout: 200,
        stalledInterval: 300,
        lockDuration: 1000,
      });
      await waitFor(() => ran.includes(`w2:${id}`), 10_000);
      expect(await cleanupClient.hget(`${k.job(id)}:sub:sub`, 's')).toBeNull();
      // w1 resumes and must not run the message a second time.
      await w1.resume();
      await new Promise((r) => setTimeout(r, 600));
      expect(ran).toEqual([`w2:${id}`]);
    } finally {
      await w1.close();
      await w2?.close();
      await bcast.close();
    }
  }, 20000);

  // Item 5c: a retry entry promoted by a library before 130 (no bcastEntry)
  // keeps its message when the original entry is trimmed.
  it('trimBroadcast looks up a legacy retry entry before deleting the message', async () => {
    const Q = uniqueQueue('r3-bcast-legacy-retry');
    const k = buildKeys(Q);
    const bcast = new Broadcast(Q, { connection: CONNECTION, maxMessages: 2 });
    try {
      const id = (await bcast.publish('evt', { n: 1 }))!;
      await cleanupClient.xgroupCreate(k.stream, 'sub', '0', { mkStream: true }).catch(() => {});
      // Pre-130 state: a retry entry in the stream, attempts recorded, no bcastEntry.
      const retryEntry = String(
        await cleanupClient.xadd(k.stream, [
          ['jobId', id],
          ['name', 'evt'],
          ['retryGroup', 'sub'],
        ]),
      );
      await cleanupClient.hset(`${k.job(id)}:sub:sub`, { a: '1' });
      await cleanupClient.hdel(k.job(id), ['bcastEntry']);
      // This publish trims the original entry; the retry entry stays.
      await bcast.publish('evt', { n: 2 });
      await waitFor(async () => (await cleanupClient.xlen(k.stream)) <= 2, 5000);
      expect(await cleanupClient.exists([k.job(id)])).toBe(1);
      expect(await cleanupClient.hget(k.job(id), 'bcastEntry')).toBe(retryEntry);
      // Trimming the retry entry too removes the message.
      await bcast.publish('evt', { n: 3 });
      await waitFor(async () => (await cleanupClient.exists([k.job(id)])) === 0, 5000);
    } finally {
      await bcast.close();
    }
  });

  // Item 8: the chain reply says when priority list, LIFO list and stream were
  // all empty, and only then.
  it('completeAndFetchNext reports empty lists only when nothing is waiting anywhere', async () => {
    const Q = uniqueQueue('r3-lists-empty');
    const k = buildKeys(Q);
    const queue = new Queue(Q, { connection: CONNECTION });
    try {
      await cleanupClient.xgroupCreate(k.stream, CONSUMER_GROUP, '0', { mkStream: true }).catch(() => {});
      const claim = async (): Promise<{ id: string; entryId: string }> => {
        const res = await cleanupClient.xreadgroup(CONSUMER_GROUP, 'w', { [k.stream]: '>' }, { count: 1 });
        const entryId = String(Object.keys(res[0].value)[0]);
        const id = String(res[0].value[entryId]!.find(([f]: any) => String(f) === 'jobId')![1]);
        await moveToActive(cleanupClient, k, id, Date.now(), k.stream, entryId, CONSUMER_GROUP);
        return { id, entryId };
      };
      const caf = (c: { id: string; entryId: string }) =>
        completeAndFetchNext(cleanupClient, k, c.id, c.entryId, 'null', Date.now(), CONSUMER_GROUP, 'w');

      await queue.add('a', {});
      const empty = await caf(await claim());
      expect(empty.next).toBe(false);
      expect(empty.listsEmpty).toBe(true);

      // A waiting stream job: NEXT_HASH, no marker.
      await queue.add('b', {});
      await queue.add('c', {});
      const chained = await caf(await claim());
      expect(typeof chained.next).toBe('object');
      expect(chained.listsEmpty).toBe(false);
      await completeJob(cleanupClient, k, chained.nextJobId!, chained.nextEntryId!, 'null', Date.now(), CONSUMER_GROUP);

      // Paused queue: NEXT_NONE without the marker.
      await queue.add('d', {});
      const d = await claim();
      await queue.pause();
      const paused = await caf(d);
      expect(paused.next).toBe(false);
      expect(paused.listsEmpty).toBe(false);
      await queue.resume();

      // A LIFO job waiting in its list: the lists were not empty.
      await queue.add('e', {});
      const e = await claim();
      await queue.add('lifo', {}, { lifo: true });
      const withLifo = await caf(e);
      expect(withLifo.listsEmpty).toBe(false);
    } finally {
      await queue.close();
    }
  });

  // Item 9a: a parent already waiting for children whose last child was
  // registered by a plain SADD after that child completed is released by the
  // scheduler tick's heal.
  it('healEarlyDeps releases a parent whose last child registered via plain SADD after completing', async () => {
    const PQ = uniqueQueue('r3-early-parent');
    const CQ = uniqueQueue('r3-early-child');
    const pk = buildKeys(PQ);
    const parentId = String(await addJob(cleanupClient, pk, 'parent', '{}', '{}', Date.now(), 0, 0, '', 0));
    // The parent is parked, waiting for children (its stream entry consumed).
    await cleanupClient.hset(pk.job(parentId), { state: 'waiting-children' });
    await cleanupClient.del([pk.stream]);
    const member = `${keyPrefix('glide', CQ)}:child-1`;
    // The child completes before the old producer's SADD registers it.
    expect(await completeChild(cleanupClient, pk, parentId, member)).toBe(0);
    expect(await cleanupClient.hget(pk.job(parentId), 'depsEarly')).toBe('1');
    await cleanupClient.sadd(pk.deps(parentId), [member]);
    expect(await cleanupClient.hget(pk.job(parentId), 'state')).toBe('waiting-children');

    expect(await healEarlyDeps(cleanupClient, pk)).toBe(1);
    expect(await cleanupClient.hget(pk.job(parentId), 'state')).toBe('waiting');
    expect(await cleanupClient.xlen(pk.stream)).toBe(1);
    expect(await cleanupClient.scard(`${pk.id.slice(0, -2)}deps-early`)).toBe(0);
    expect(await healEarlyDeps(cleanupClient, pk)).toBe(0);
  });

  // Item 9b: heal re-seeds list-active-ids from a complete scan when the set
  // went stale (downgrade and re-upgrade).
  it('healListActive re-seeds a stale list-active-ids set', async () => {
    const Q = uniqueQueue('r3-heal-reseed');
    const k = buildKeys(Q);
    await cleanupClient.hset(k.job('7'), { id: '7', name: 'x', state: 'active', listSourced: '1' });
    await cleanupClient.set(k.listActive, '1');
    await cleanupClient.hset(k.meta, { listActiveIds: '1' });
    // Stale set: a released claim is still a member, the live claim is missing.
    await cleanupClient.sadd(k.listActiveIds, ['stale']);
    expect(await healListActive(cleanupClient, k)).toBe(0);
    const members = (await cleanupClient.smembers(k.listActiveIds)) as Set<string>;
    expect([...members].map(String).sort()).toEqual(['7']);
    expect(String(await cleanupClient.get(k.listActive))).toBe('1');
  });
});
