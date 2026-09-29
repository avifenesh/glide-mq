/**
 * Server-function correctness regressions, round 2 (2026-09-29 audit).
 *
 * Run: npx vitest run tests/lua-round2-20260929.test.ts
 */
import { afterAll, beforeAll, expect, it } from 'vitest';
import { createCleanupClient, describeEachMode, flushQueue } from './helpers/fixture';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { FlowProducer } = require('../dist/flow-producer') as typeof import('../src/flow-producer');
const { buildKeys, keyPrefix, parseCrossQueueParentNotification } =
  require('../dist/utils') as typeof import('../src/utils');
const { addJob, completeAndFetchNext, completeChild, completeJob, dedup, registerChildDep, CONSUMER_GROUP } =
  require('../dist/functions') as typeof import('../src/functions');

describeEachMode('Lua round 2 2026-09-29', (CONNECTION) => {
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

  async function hget(key: string, field: string): Promise<string | null> {
    const v = await cleanupClient.hget(key, field);
    return v == null ? null : String(v);
  }

  // Parent in PQ waiting on one pending cross-queue child c1 (c1 waits on its
  // own child, so it cannot complete during the test). The nested child keeps
  // the flow slot-safe in cluster mode.
  async function parentWithPendingChild(PQ: string, CQ: string) {
    const flow = new FlowProducer({ connection: CONNECTION });
    try {
      const node = await flow.add({
        name: 'parent',
        queueName: PQ,
        data: {},
        children: [{ name: 'c1', queueName: CQ, data: {}, children: [{ name: 'gc', queueName: CQ, data: {} }] }],
      });
      return { parentId: node.job.id, c1Member: `${keyPrefix('glide', CQ)}:${node.children![0].job.id}` };
    } finally {
      await flow.close();
    }
  }

  async function addUnregisteredChild(CQ: string, PQ: string, parentId: string): Promise<string> {
    const id = await addJob(
      cleanupClient,
      buildKeys(CQ),
      'c2',
      '{}',
      '{}',
      Date.now(),
      60_000,
      0,
      parentId,
      0,
      '',
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      '',
      0,
      PQ,
    );
    return `${keyPrefix('glide', CQ)}:${id}`;
  }

  async function deliverXqPending(CQ: string): Promise<void> {
    const members = await cleanupClient.smembers(buildKeys(CQ).xqPending);
    for (const raw of members) {
      const [parentQueue, parentId, depsMember] = parseCrossQueueParentNotification(String(raw))!;
      await completeChild(cleanupClient, buildKeys(parentQueue), parentId, depsMember);
      await cleanupClient.srem(buildKeys(CQ).xqPending, [String(raw)]);
    }
  }

  it('A-11: a cross-queue child finishing before registration does not release the parent early', async () => {
    const PQ = uniqueQueue('r2-xq-early-p');
    const CQ = uniqueQueue('r2-xq-early-c');
    const pk = buildKeys(PQ);
    const { parentId, c1Member } = await parentWithPendingChild(PQ, CQ);
    const c2Member = await addUnregisteredChild(CQ, PQ, parentId);

    // Eager notification of c2's completion arrives before Queue.add registers it.
    await completeChild(cleanupClient, pk, parentId, c2Member);
    expect(await hget(pk.job(parentId), 'state')).toBe('waiting-children');

    await registerChildDep(cleanupClient, pk, parentId, c2Member);
    expect(await hget(pk.job(parentId), 'state')).toBe('waiting-children');
    expect(await hget(pk.job(parentId), 'depsCompleted')).toBe('1');

    await completeChild(cleanupClient, pk, parentId, c1Member);
    expect(await hget(pk.job(parentId), 'state')).toBe('waiting');
    expect(await hget(pk.job(parentId), 'depsCompleted')).toBe('2');
  });

  it('A-11: an early completion registered by a plain SADD is counted by the next completion', async () => {
    const PQ = uniqueQueue('r2-xq-legacy-p');
    const CQ = uniqueQueue('r2-xq-legacy-c');
    const pk = buildKeys(PQ);
    const { parentId, c1Member } = await parentWithPendingChild(PQ, CQ);
    const c2Member = await addUnregisteredChild(CQ, PQ, parentId);

    await completeChild(cleanupClient, pk, parentId, c2Member);
    await cleanupClient.sadd(pk.deps(parentId), [c2Member]);
    expect(await hget(pk.job(parentId), 'state')).toBe('waiting-children');

    await completeChild(cleanupClient, pk, parentId, c1Member);
    expect(await hget(pk.job(parentId), 'state')).toBe('waiting');
    expect(await hget(pk.job(parentId), 'depsCompleted')).toBe('2');
  });

  it('A-11: a debounce replacement of a cross-queue child holds the parent until it finishes', async () => {
    const PQ = uniqueQueue('r2-xq-debounce-p');
    const CQ = uniqueQueue('r2-xq-debounce-c');
    const pk = buildKeys(PQ);
    const ck = buildKeys(CQ);
    const { parentId, c1Member } = await parentWithPendingChild(PQ, CQ);
    const childQueue = new Queue(CQ, { connection: CONNECTION });
    try {
      const dedupOpts = { id: 'deb', mode: 'debounce' as const };
      const d1 = await childQueue.add(
        'd',
        {},
        { parent: { id: parentId, queue: PQ }, deduplication: dedupOpts, delay: 60_000 },
      );
      expect(d1).not.toBeNull();
      await completeChild(cleanupClient, pk, parentId, c1Member);
      expect(await hget(pk.job(parentId), 'state')).toBe('waiting-children');

      // Replace d1 but stop before the caller registers d2 in the parent deps.
      const d2 = await dedup(
        cleanupClient,
        ck,
        'deb',
        0,
        'debounce',
        'd',
        '{}',
        '{}',
        Date.now(),
        60_000,
        0,
        parentId,
        0,
        '',
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        '',
        0,
        PQ,
      );
      await deliverXqPending(CQ);
      expect(await hget(pk.job(parentId), 'state')).toBe('waiting-children');

      const d2Member = `${keyPrefix('glide', CQ)}:${d2}`;
      await registerChildDep(cleanupClient, pk, parentId, d2Member);
      expect(await hget(pk.job(parentId), 'state')).toBe('waiting-children');

      const notifications = await completeJob(cleanupClient, ck, d2, '', 'null', Date.now());
      for (const member of notifications) {
        const [parentQueue, pid, depsMember] = parseCrossQueueParentNotification(member)!;
        await completeChild(cleanupClient, buildKeys(parentQueue), pid, depsMember);
      }
      expect(await hget(pk.job(parentId), 'state')).toBe('waiting');
      expect(await hget(pk.job(parentId), 'depsCompleted')).toBe('3');
    } finally {
      await childQueue.close();
    }
  });

  it('A-10: a budgeted flow creates the budget first and writes budgetKey with each job', async () => {
    const Q = uniqueQueue('r2-budget');
    const k = buildKeys(Q);
    let budgetBeforeJobs: number | null = null;
    let removedChild = '';
    // A worker that finishes (removeOnComplete) a child right after creation.
    const client = new Proxy(cleanupClient, {
      get(target, prop) {
        if (prop === 'fcall') {
          return async (fn: string, keys: string[], args: string[]) => {
            if (fn === 'glidemq_addFlow') budgetBeforeJobs = await target.exists([k.budget(args[8])]);
            const result = await target.fcall(fn, keys, args);
            if (fn === 'glidemq_addFlow') {
              removedChild = JSON.parse(String(result))[1];
              await target.del([k.job(removedChild)]);
            }
            return result;
          };
        }
        const value = target[prop];
        return typeof value === 'function' ? value.bind(target) : value;
      },
    });
    const flow = new FlowProducer({ client, connection: CONNECTION });
    try {
      const node = await flow.add(
        { name: 'parent', queueName: Q, data: {}, children: [{ name: 'c1', queueName: Q, data: {} }] },
        { budget: { maxTotalTokens: 100 } },
      );
      expect(budgetBeforeJobs).toBe(1);
      expect(removedChild).toBe(node.children![0].job.id);
      expect(await cleanupClient.exists([k.job(removedChild)])).toBe(0);
      expect(await hget(k.job(node.job.id), 'budgetKey')).toBe(k.budget(node.job.id));
      expect(node.children![0].job.budgetKey).toBe(k.budget(node.job.id));
    } finally {
      await flow.close();
    }
  });

  it('R2-11: completeAndFetchNext advances the ordering frontier of a group-key job', async () => {
    const Q = uniqueQueue('r2-caf-order');
    const k = buildKeys(Q);
    await cleanupClient.xgroupCreate(k.stream, CONSUMER_GROUP, '0', { mkStream: true });
    await cleanupClient.hset(k.group('g'), { maxConcurrency: '1', active: '1', nextSeq: '2' });
    await cleanupClient.hset(k.job('o1'), { id: 'o1', name: 'o', state: 'active', groupKey: 'g', orderingSeq: '1' });
    // The worker passes the hints it read from the job hash, which stores only groupKey.
    await completeAndFetchNext(
      cleanupClient,
      k,
      'o1',
      '',
      'null',
      Date.now(),
      CONSUMER_GROUP,
      'c1',
      undefined,
      undefined,
      {
        orderingKey: undefined,
        orderingSeq: 1,
        groupKey: 'g',
      },
    );
    expect(await hget(k.meta, 'orderdone:g')).toBe('1');
    expect(await cleanupClient.exists([`${keyPrefix('glide', Q)}:orderdone:pending:g`])).toBe(0);
  });
});
