/**
 * Integration tests for per-key ordering.
 *
 * Run: npx vitest run tests/ordering.test.ts
 */
import { it, expect, beforeAll, afterAll } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys, keyPrefix } = require('../dist/utils') as typeof import('../src/utils');

import { describeEachMode, createCleanupClient, flushQueue, waitFor } from './helpers/fixture';

function createRng(seed = 0x1234abcd) {
  let state = seed >>> 0;
  return () => {
    state = (state * 1664525 + 1013904223) >>> 0;
    return state / 0x100000000;
  };
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describeEachMode('Per-key ordering', (CONNECTION) => {
  const Q = 'test-ordering-' + Date.now();
  let queue: InstanceType<typeof Queue>;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
    queue = new Queue(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('processes jobs sequentially for each ordering key under concurrency', async () => {
    const rng = createRng(0x44aa99);
    const accountCount = 10;
    const totalJobs = 240;
    const createdByAccount = new Map<string, number>();
    const events: { accountId: string; expectedSeq: number; payload: number }[] = [];

    for (let i = 0; i < totalJobs; i += 1) {
      const accountId = `acct-${Math.floor(rng() * accountCount)}`;
      const next = (createdByAccount.get(accountId) || 0) + 1;
      createdByAccount.set(accountId, next);
      events.push({ accountId, expectedSeq: next, payload: i });
    }

    await queue.addBulk(
      events.map((e) => ({
        name: 'ordered',
        data: e,
        opts: {
          ordering: { key: e.accountId },
          // Add heterogeneity to amplify out-of-order risk without ordering controls.
          priority: Math.floor(rng() * 6),
        },
      })),
    );

    const seenByAccount = new Map<string, number>();
    let violations = 0;
    let processed = 0;
    let worker: InstanceType<typeof Worker> | null = null;

    const done = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('timeout')), 20000);
      worker = new Worker(
        Q,
        async (job: any) => {
          const accountId = String(job.data.accountId);
          const expectedSeq = Number(job.data.expectedSeq);
          const last = seenByAccount.get(accountId) || 0;
          if (expectedSeq !== last + 1) {
            violations += 1;
          }
          seenByAccount.set(accountId, Math.max(last, expectedSeq));
          processed += 1;
          if (processed >= totalJobs) {
            clearTimeout(timeout);
            resolve();
          }
          await sleep(2 + Math.floor(rng() * 9));
          return { ok: true };
        },
        { connection: CONNECTION, concurrency: 10, blockTimeout: 250, promotionInterval: 75 },
      );
      worker.on('error', () => {});
    });

    await done;
    if (worker) await worker.close(true);

    expect(violations).toBe(0);
    for (const [accountId, expectedFinal] of createdByAccount.entries()) {
      expect(seenByAccount.get(accountId)).toBe(expectedFinal);
    }
  }, 25000);

  it('deferActive does not recreate removed job hashes', async () => {
    const localQueueName = `test-ordering-defer-${Date.now()}`;
    const k = buildKeys(localQueueName);
    const missingJobId = '99999';
    const entryId = await cleanupClient.xadd(k.stream, ['jobId', missingJobId]);

    const result = await cleanupClient.fcall(
      'glidemq_deferActive',
      [k.stream, k.job(missingJobId), k.listActive],
      [missingJobId, String(entryId), 'workers'],
    );

    expect(Number(result)).toBe(0);

    const hash = await cleanupClient.hgetall(k.job(missingJobId));
    expect(hash).toEqual([]);

    const streamLen = await cleanupClient.xlen(k.stream);
    expect(Number(streamLen)).toBe(0);
  });

  it('remove of an unrun ordered job wakes a successor parked in groupq', async () => {
    const Q2 = 'test-order-remove-hole-' + Date.now();
    const q = new Queue(Q2, { connection: CONNECTION });
    const k = buildKeys(Q2);
    const groupKey = 'remove-hole';
    const processed: string[] = [];

    const job1 = await q.add('task', { seq: 1 }, { delay: 60000, ordering: { key: groupKey } });
    const job2 = await q.add('task', { seq: 2 }, { ordering: { key: groupKey } });
    expect(job1).not.toBeNull();
    expect(job2).not.toBeNull();

    const worker = new Worker(
      Q2,
      async (job: any) => {
        processed.push(job.id);
        return 'ok';
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 100, stalledInterval: 60000 },
    );
    worker.on('error', () => {});

    await waitFor(async () => String(await cleanupClient.hget(k.job(job2!.id), 'state')) === 'group-waiting', 5000);
    await job1!.remove();
    await waitFor(() => processed.includes(job2!.id), 8000);

    await worker.close(true);
    await q.close();
    await flushQueue(cleanupClient, Q2);
  }, 20000);

  it('revoke of an unrun ordered job wakes a successor parked in groupq', async () => {
    const Q2 = 'test-order-revoke-hole-' + Date.now();
    const q = new Queue(Q2, { connection: CONNECTION });
    const k = buildKeys(Q2);
    const groupKey = 'revoke-hole';
    const processed: string[] = [];

    const job1 = await q.add('task', { seq: 1 }, { delay: 60000, ordering: { key: groupKey } });
    const job2 = await q.add('task', { seq: 2 }, { ordering: { key: groupKey } });
    expect(job1).not.toBeNull();
    expect(job2).not.toBeNull();

    const worker = new Worker(
      Q2,
      async (job: any) => {
        processed.push(job.id);
        return 'ok';
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 100, stalledInterval: 60000 },
    );
    worker.on('error', () => {});

    await waitFor(async () => String(await cleanupClient.hget(k.job(job2!.id), 'state')) === 'group-waiting', 5000);
    expect(await q.revoke(job1!.id)).toBe('revoked');
    expect(String(await cleanupClient.hget(k.meta, `orderdone:${groupKey}`))).toBe('1');
    await waitFor(() => processed.includes(job2!.id), 8000);

    await worker.close(true);
    await q.close();
    await flushQueue(cleanupClient, Q2);
  }, 20000);

  it('revoke of a group-waiting ordered job advances the worker ordering gate', async () => {
    const Q2 = 'test-order-revoke-group-waiting-' + Date.now();
    const q = new Queue(Q2, { connection: CONNECTION });
    const k = buildKeys(Q2);
    const groupKey = 'revoke-group-waiting';
    const processed: string[] = [];
    let releaseFirst!: () => void;
    const firstCanFinish = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    let firstStarted!: () => void;
    const firstStartedPromise = new Promise<void>((resolve) => {
      firstStarted = resolve;
    });

    const first = await q.add('task', { seq: 1 }, { ordering: { key: groupKey } });
    const revoked = await q.add('task', { seq: 2 }, { ordering: { key: groupKey } });
    const successor = await q.add('task', { seq: 3 }, { ordering: { key: groupKey } });
    expect(first).not.toBeNull();
    expect(revoked).not.toBeNull();
    expect(successor).not.toBeNull();

    const worker = new Worker(
      Q2,
      async (job: any) => {
        if (job.id === first!.id) {
          firstStarted();
          await firstCanFinish;
        }
        processed.push(job.id);
      },
      { connection: CONNECTION, concurrency: 2, blockTimeout: 100, stalledInterval: 60000 },
    );
    worker.on('error', () => {});

    await firstStartedPromise;
    await waitFor(async () => String(await cleanupClient.hget(k.job(revoked!.id), 'state')) === 'group-waiting', 5000);
    expect(await q.revoke(revoked!.id)).toBe('revoked');
    releaseFirst();

    await waitFor(() => processed.includes(successor!.id), 8000);

    await worker.close(true);
    await q.close();
    await flushQueue(cleanupClient, Q2);
  }, 20000);

  it.each([
    {
      name: 'delayed retry',
      transition: async (k: any, jobId: string, now: number) => {
        await cleanupClient.fcall(
          'glidemq_fail',
          [k.stream, k.failed, k.scheduled, k.events, k.job(jobId), k.metricsFailed],
          [jobId, '', 'retry', String(now), '3', '0', 'workers', '0', '0', '0', '0'],
        );
      },
    },
    {
      name: 'suspended job',
      transition: async (k: any, jobId: string, now: number) => {
        await cleanupClient.fcall(
          'glidemq_suspend',
          [k.job(jobId), k.stream, k.events, k.suspended],
          [jobId, '', 'workers', String(now), 'approval', '0', '0'],
        );
      },
    },
    {
      name: 'waiting-children job',
      transition: async (k: any, jobId: string, now: number) => {
        await cleanupClient.fcall(
          'glidemq_moveToWaitingChildren',
          [k.job(jobId), k.stream, k.events],
          [jobId, '', 'workers', String(now), '0'],
        );
      },
    },
  ])(
    'removing an ordered $name releases its held group slot',
    async ({ transition }) => {
      const Q2 = `test-order-held-slot-remove-${Date.now()}`;
      const q = new Queue(Q2, { connection: CONNECTION });
      const k = buildKeys(Q2);
      const groupKey = 'held-slot-remove';
      const now = Date.now();

      const held = await q.add('held', { seq: 1 }, { ordering: { key: groupKey, concurrency: 1 } });
      const successor = await q.add('successor', { seq: 2 }, { ordering: { key: groupKey, concurrency: 1 } });
      expect(held).not.toBeNull();
      expect(successor).not.toBeNull();

      await cleanupClient.hset(k.job(held!.id), {
        state: 'active',
        groupKey,
        orderingKey: groupKey,
        orderingSeq: '1',
        processedOn: String(now),
      });
      await cleanupClient.hset(k.job(successor!.id), {
        state: 'group-waiting',
        groupKey,
        orderingKey: groupKey,
        orderingSeq: '2',
      });
      await cleanupClient.hset(k.group(groupKey), {
        active: '1',
        maxConcurrency: '1',
        nextSeq: '2',
      });
      await cleanupClient.zadd(k.groupq(groupKey), [{ element: successor!.id, score: 2 }]);

      await transition(k, held!.id, now);
      await cleanupClient.fcall(
        'glidemq_removeJob',
        [k.job(held!.id), k.stream, k.scheduled, k.completed, k.failed, k.events, k.log(held!.id)],
        [held!.id],
      );

      expect(String(await cleanupClient.hget(k.group(groupKey), 'active'))).toBe('0');
      expect(String(await cleanupClient.hget(k.job(successor!.id), 'state'))).toBe('waiting');

      await q.close();
      await flushQueue(cleanupClient, Q2);
    },
    15000,
  );

  it('TTL of an unrun ordered job wakes a successor parked in groupq', async () => {
    const Q2 = 'test-order-ttl-hole-' + Date.now();
    const q = new Queue(Q2, { connection: CONNECTION });
    const k = buildKeys(Q2);
    const groupKey = 'ttl-hole';
    const processed: string[] = [];

    const job1 = await q.add('task', { seq: 1 }, { delay: 400, ttl: 50, ordering: { key: groupKey } });
    const job2 = await q.add('task', { seq: 2 }, { ordering: { key: groupKey } });
    expect(job1).not.toBeNull();
    expect(job2).not.toBeNull();

    const worker = new Worker(
      Q2,
      async (job: any) => {
        processed.push(job.id);
        return 'ok';
      },
      {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 100,
        stalledInterval: 60000,
        promotionInterval: 200,
      },
    );
    worker.on('error', () => {});

    await waitFor(async () => String(await cleanupClient.hget(k.job(job2!.id), 'state')) === 'group-waiting', 5000);
    await waitFor(() => processed.includes(job2!.id), 8000);

    await worker.close(true);
    await q.close();
    await flushQueue(cleanupClient, Q2);
  }, 20000);

  it('rateLimitGroup requeue keeps the ordered slot so the next seq cannot run in parallel', async () => {
    const Q2 = 'test-order-rlg-slot-' + Date.now();
    const q = new Queue(Q2, { connection: CONNECTION });
    const groupKey = 'rlg-slot';
    let concurrent = 0;
    let maxConcurrent = 0;
    let done = 0;
    const firstSeen = new Set<string>();

    await q.add('task', { seq: 1 }, { ordering: { key: groupKey } });
    await q.add('task', { seq: 2 }, { ordering: { key: groupKey } });

    const worker = new Worker(
      Q2,
      async (job: any) => {
        if (job.data.seq === 1 && !firstSeen.has(job.id)) {
          firstSeen.add(job.id);
          await job.rateLimitGroup(200);
        }
        concurrent += 1;
        maxConcurrent = Math.max(maxConcurrent, concurrent);
        await sleep(150);
        concurrent -= 1;
        done += 1;
        return 'ok';
      },
      {
        connection: CONNECTION,
        concurrency: 2,
        blockTimeout: 100,
        stalledInterval: 60000,
        promotionInterval: 100,
      },
    );
    worker.on('error', () => {});

    await waitFor(() => done >= 2, 8000);
    await worker.close(true);
    await q.close();
    await flushQueue(cleanupClient, Q2);
    expect(maxConcurrent).toBe(1);
  }, 20000);

  it('keeps retained-return slots separate from user ordering-key hashes', async () => {
    const Q2 = 'test-order-return-key-namespace-' + Date.now();
    const q = new Queue(Q2, { connection: CONNECTION });
    const k = buildKeys(Q2);
    const now = Date.now();

    try {
      const conflicting = await q.add('conflicting', {}, { ordering: { key: 'return:X' } });
      const returning = await q.add('returning', {}, { ordering: { key: 'X' } });
      expect(conflicting).not.toBeNull();
      expect(returning).not.toBeNull();

      await cleanupClient.hset(k.job(returning!.id), { state: 'active', processedOn: String(now) });

      await expect(
        cleanupClient.fcall(
          'glidemq_rateLimitGroup',
          [k.job(returning!.id), k.stream],
          [returning!.id, '', 'workers', '1000', String(now), '0', 'requeue', 'back', 'max'],
        ),
      ).resolves.toBe(String(now + 1000));
      expect(await cleanupClient.hget(k.group('return:X'), 'maxConcurrency')).toBe('1');
    } finally {
      await q.close();
      await flushQueue(cleanupClient, Q2);
    }
  }, 10000);

  it('migrates and resumes a v115 retained-return slot', async () => {
    const Q2 = 'test-order-return-key-legacy-' + Date.now();
    const q = new Queue(Q2, { connection: CONNECTION });
    const k = buildKeys(Q2);
    const now = Date.now();
    const groupKey = 'legacy-returner';
    const legacyReturningKey = `${keyPrefix('glide', Q2)}:group:return:${groupKey}`;

    try {
      const returning = await q.add('returning', {}, { ordering: { key: groupKey } });
      expect(returning).not.toBeNull();
      await cleanupClient.hset(k.job(returning!.id), { state: 'group-waiting' });
      await cleanupClient.zadd(k.groupq(groupKey), [{ element: returning!.id, score: 1 }]);
      await cleanupClient.zadd(legacyReturningKey, [{ element: returning!.id, score: 1 }]);
      await cleanupClient.hset(k.group(groupKey), { active: '1' });
      await cleanupClient.zadd(k.ratelimited, [{ element: groupKey, score: now }]);

      await expect(
        cleanupClient.fcall('glidemq_promoteRateLimited', [k.ratelimited, k.stream], [String(now)]),
      ).resolves.toBe(1);
      expect(String(await cleanupClient.hget(k.job(returning!.id), 'state'))).toBe('waiting');
      expect(await cleanupClient.type(legacyReturningKey)).toBe('none');
    } finally {
      await q.close();
      await flushQueue(cleanupClient, Q2);
    }
  }, 10000);

  it('completeAndFetchNext parks a priority group job when the group rate window is full', async () => {
    const Q2 = 'test-caf-pri-rate-' + Date.now();
    const q = new Queue(Q2, { connection: CONNECTION });
    const k = buildKeys(Q2);
    const now = Date.now();

    const current = await q.add('current', { n: 1 });
    const pri = await q.add(
      'priority',
      { seq: 1 },
      {
        ordering: { key: 'pri-rate', rateLimit: { max: 1, duration: 10000 } },
      },
    );
    expect(current).not.toBeNull();
    expect(pri).not.toBeNull();

    await cleanupClient.lpush(k.priority, [pri!.id]);

    await cleanupClient.xgroupCreate(k.stream, 'workers', '0', { mkStream: true });
    await cleanupClient.hset(k.job(current!.id), { state: 'active', processedOn: String(now) });
    await cleanupClient.hset(k.group('pri-rate'), {
      rateMax: '1',
      rateDuration: '10000',
      rateWindowStart: String(now),
      rateCount: '1',
    });

    const result = await cleanupClient.fcall(
      'glidemq_completeAndFetchNext',
      [k.stream, k.completed, k.events, k.job(current!.id), k.metricsCompleted],
      [
        current!.id,
        '',
        'ok',
        String(now),
        'workers',
        'c1',
        '0',
        '0',
        '0',
        '',
        '',
        '__',
        '',
        '__',
        '0',
        '',
        '0',
        '1',
        '1',
      ],
    );
    const tag = Array.isArray(result) ? String(result[0]) : String(result);
    expect(tag).toBe('NEXT_NONE');
    expect(String(await cleanupClient.hget(k.job(pri!.id), 'state'))).toBe('group-waiting');

    await q.close();
    await flushQueue(cleanupClient, Q2);
  }, 15000);

  it('rateLimitGroup fail keeps successors parked until the pause expires', async () => {
    const Q2 = 'test-order-rlg-fail-' + Date.now();
    const q = new Queue(Q2, { connection: CONNECTION });
    const groupKey = 'rlg-fail';
    const pauseMs = 400;
    let seq2StartedAt = 0;
    let seq1LimitedAt = 0;
    let seq1Id = '';
    let seq2Done = 0;
    const firstSeen = new Set<string>();

    await q.add('task', { seq: 1 }, { ordering: { key: groupKey } });
    await q.add('task', { seq: 2 }, { ordering: { key: groupKey } });

    const worker = new Worker(
      Q2,
      async (job: any) => {
        if (job.data.seq === 1 && !firstSeen.has(job.id)) {
          firstSeen.add(job.id);
          seq1Id = job.id;
          seq1LimitedAt = Date.now();
          await job.rateLimitGroup(pauseMs, { currentJob: 'fail' });
        }
        if (job.data.seq === 2) seq2StartedAt = Date.now();
        return 'ok';
      },
      {
        connection: CONNECTION,
        concurrency: 2,
        blockTimeout: 100,
        stalledInterval: 60000,
        promotionInterval: 100,
      },
    );
    worker.on('error', () => {});
    worker.on('completed', (job: any) => {
      if (job?.data?.seq === 2) seq2Done += 1;
    });

    await waitFor(() => seq2Done >= 1, 8000);
    await worker.close(true);
    const seq1State = String(await cleanupClient.hget(buildKeys(Q2).job(seq1Id), 'state'));
    await q.close();
    await flushQueue(cleanupClient, Q2);
    expect(seq1State).toBe('failed');
    expect(seq2StartedAt).toBeGreaterThan(0);
    expect(seq2StartedAt - seq1LimitedAt).toBeGreaterThanOrEqual(pauseMs - 80);
  }, 20000);

  it('rateLimitGroup requeue at back still resumes the returning ordered job', async () => {
    const Q2 = 'test-order-rlg-back-' + Date.now();
    const q = new Queue(Q2, { connection: CONNECTION });
    const groupKey = 'rlg-back';
    let concurrent = 0;
    let maxConcurrent = 0;
    let done = 0;
    const firstSeen = new Set<string>();

    await q.add('task', { seq: 1 }, { ordering: { key: groupKey } });
    await q.add('task', { seq: 2 }, { ordering: { key: groupKey } });

    const worker = new Worker(
      Q2,
      async (job: any) => {
        if (job.data.seq === 1 && !firstSeen.has(job.id)) {
          firstSeen.add(job.id);
          await job.rateLimitGroup(200, { requeuePosition: 'back' });
        }
        concurrent += 1;
        maxConcurrent = Math.max(maxConcurrent, concurrent);
        await sleep(150);
        concurrent -= 1;
        done += 1;
        return 'ok';
      },
      {
        connection: CONNECTION,
        concurrency: 2,
        blockTimeout: 100,
        stalledInterval: 60000,
        promotionInterval: 100,
      },
    );
    worker.on('error', () => {});

    await waitFor(() => done >= 2, 8000);
    await worker.close(true);
    await q.close();
    await flushQueue(cleanupClient, Q2);
    expect(maxConcurrent).toBe(1);
    expect(done).toBe(2);
  }, 20000);

  it('keeps an over-cost back returner parked until its token bucket refills', async () => {
    const Q2 = 'test-order-rlg-back-token-bucket-' + Date.now();
    const q = new Queue(Q2, { connection: CONNECTION });
    const k = buildKeys(Q2);
    const groupKey = 'rlg-back-token-bucket';
    const now = Date.now();

    const returning = await q.add(
      'returning',
      { seq: 1 },
      {
        ordering: { key: groupKey, concurrency: 1, tokenBucket: { capacity: 5, refillRate: 1 } },
        cost: 4.5,
      },
    );
    const successor = await q.add(
      'successor',
      { seq: 2 },
      {
        ordering: { key: groupKey, concurrency: 1, tokenBucket: { capacity: 5, refillRate: 1 } },
        cost: 0.1,
      },
    );
    expect(returning).not.toBeNull();
    expect(successor).not.toBeNull();

    await cleanupClient.hset(k.job(returning!.id), { state: 'active', processedOn: String(now) });
    await cleanupClient.hset(k.job(successor!.id), { state: 'group-waiting' });
    await cleanupClient.zadd(k.groupq(groupKey), [{ element: successor!.id, score: 2 }]);
    await cleanupClient.hset(k.group(groupKey), {
      active: '1',
      nextSeq: '2',
      tbCapacity: '5000',
      tbTokens: '500',
      tbRefillRate: '1000',
      tbLastRefill: String(now),
      tbRefillRemainder: '0',
    });

    await cleanupClient.fcall(
      'glidemq_rateLimitGroup',
      [k.job(returning!.id), k.stream],
      [returning!.id, '', 'workers', '1000', String(now), '0', 'requeue', 'back', 'max'],
    );
    const resumeAt = now + 1000;
    await cleanupClient.fcall('glidemq_promoteRateLimited', [k.ratelimited, k.stream], [String(resumeAt)]);

    // This is how the worker will activate a returner only after promotion.
    if (String(await cleanupClient.hget(k.job(returning!.id), 'state')) === 'waiting') {
      await cleanupClient.fcall(
        'glidemq_moveToActive',
        [k.job(returning!.id), k.stream],
        [String(resumeAt), '', 'workers', returning!.id],
      );
    }

    expect(Number(await cleanupClient.hget(k.group(groupKey), 'tbTokens'))).toBeGreaterThanOrEqual(0);
    expect(String(await cleanupClient.hget(k.job(returning!.id), 'state'))).toBe('group-waiting');
    expect(Number(await cleanupClient.zscore(k.ratelimited, groupKey))).toBeGreaterThan(resumeAt);

    await q.close();
    await flushQueue(cleanupClient, Q2);
  }, 10000);

  it('rateLimitGroup back resumes a returning job beyond the promotion scan window', async () => {
    const Q2 = 'test-order-rlg-back-deep-' + Date.now();
    const q = new Queue(Q2, { connection: CONNECTION });
    const k = buildKeys(Q2);
    const groupKey = 'rlg-back-deep';
    const totalJobs = 260;
    let firstSeen = false;
    let resumed = false;
    let done = 0;
    let firstJobId = '';
    let resolveLimited!: () => void;
    const limited = new Promise<void>((resolve) => {
      resolveLimited = resolve;
    });

    await q.addBulk(
      Array.from({ length: totalJobs }, (_, i) => ({
        name: 'task',
        data: { seq: i + 1 },
        opts: { ordering: { key: groupKey, concurrency: 1 } },
      })),
    );

    const worker = new Worker(
      Q2,
      async (job: any) => {
        if (job.data.seq === 1 && !firstSeen) {
          firstSeen = true;
          firstJobId = job.id;
          await waitFor(async () => Number(await cleanupClient.zcard(k.groupq(groupKey))) >= totalJobs - 1, 10000);
          try {
            await job.rateLimitGroup(10000, { requeuePosition: 'back' });
          } finally {
            resolveLimited();
          }
        }
        if (job.data.seq === 1) resumed = true;
        done += 1;
        return 'ok';
      },
      {
        connection: CONNECTION,
        concurrency: 4,
        blockTimeout: 100,
        stalledInterval: 60000,
        promotionInterval: 100,
      },
    );
    worker.on('error', () => {});

    await limited;
    const firstWindow = await cleanupClient.zrange(k.groupq(groupKey), { start: 0, end: 128 });
    expect(firstWindow.map(String)).not.toContain(firstJobId);
    await cleanupClient.fcall('glidemq_promoteRateLimited', [k.ratelimited, k.stream], [String(Date.now() + 10001)]);
    await waitFor(() => done === totalJobs, 15000);
    await worker.close(true);
    await q.close();
    await flushQueue(cleanupClient, Q2);
    expect(firstSeen).toBe(true);
    expect(resumed).toBe(true);
  }, 20000);

  it('releases a retained ordered slot when a parked returner is revoked', async () => {
    const Q2 = 'test-order-retained-revoke-' + Date.now();
    const q = new Queue(Q2, { connection: CONNECTION });
    const k = buildKeys(Q2);
    const groupKey = 'retained-revoke';
    const now = Date.now();

    const returning = await q.add('returning', { seq: 1 }, { ordering: { key: groupKey, concurrency: 1 } });
    const successor = await q.add('successor', { seq: 2 }, { ordering: { key: groupKey, concurrency: 1 } });
    expect(returning).not.toBeNull();
    expect(successor).not.toBeNull();

    await cleanupClient.hset(k.job(returning!.id), { state: 'active', processedOn: String(now) });
    await cleanupClient.hset(k.job(successor!.id), { state: 'group-waiting' });
    await cleanupClient.zadd(k.groupq(groupKey), [{ element: successor!.id, score: 2 }]);
    await cleanupClient.hset(k.group(groupKey), { active: '1', nextSeq: '2' });
    await cleanupClient.fcall(
      'glidemq_rateLimitGroup',
      [k.job(returning!.id), k.stream],
      [returning!.id, '', 'workers', '1000', String(now), '0', 'requeue', 'back', 'max'],
    );

    expect(await q.revoke(returning!.id)).toBe('revoked');
    expect(String(await cleanupClient.hget(k.group(groupKey), 'active'))).toBe('0');
    await cleanupClient.fcall('glidemq_promoteRateLimited', [k.ratelimited, k.stream], [String(now + 1001)]);
    expect(String(await cleanupClient.hget(k.job(successor!.id), 'state'))).toBe('waiting');

    await q.close();
    await flushQueue(cleanupClient, Q2);
  }, 10000);

  it('resumes every concurrent retained ordered returner before successors', async () => {
    const Q2 = 'test-order-retained-multiple-' + Date.now();
    const q = new Queue(Q2, { connection: CONNECTION });
    const k = buildKeys(Q2);
    const groupKey = 'retained-multiple';
    const now = Date.now();

    const first = await q.add('first', { seq: 1 }, { ordering: { key: groupKey, concurrency: 2 } });
    const second = await q.add('second', { seq: 2 }, { ordering: { key: groupKey, concurrency: 2 } });
    const successor = await q.add('successor', { seq: 3 }, { ordering: { key: groupKey, concurrency: 2 } });
    expect(first).not.toBeNull();
    expect(second).not.toBeNull();
    expect(successor).not.toBeNull();

    await cleanupClient.hset(k.job(first!.id), { state: 'active', processedOn: String(now) });
    await cleanupClient.hset(k.job(second!.id), { state: 'active', processedOn: String(now) });
    await cleanupClient.hset(k.job(successor!.id), { state: 'group-waiting' });
    await cleanupClient.zadd(k.groupq(groupKey), [{ element: successor!.id, score: 3 }]);
    await cleanupClient.hset(k.group(groupKey), { active: '2', nextSeq: '3' });

    for (const job of [first, second]) {
      await cleanupClient.fcall(
        'glidemq_rateLimitGroup',
        [k.job(job!.id), k.stream],
        [job!.id, '', 'workers', '1000', String(now), '0', 'requeue', 'back', 'max'],
      );
    }

    await cleanupClient.fcall('glidemq_promoteRateLimited', [k.ratelimited, k.stream], [String(now + 1001)]);
    expect(String(await cleanupClient.hget(k.job(first!.id), 'state'))).toBe('waiting');
    expect(String(await cleanupClient.hget(k.job(second!.id), 'state'))).toBe('waiting');
    expect(String(await cleanupClient.hget(k.job(successor!.id), 'state'))).toBe('group-waiting');

    await q.close();
    await flushQueue(cleanupClient, Q2);
  }, 10000);

  it('completeAndFetchNext closes the ordering hole when a priority job exceeds token-bucket capacity', async () => {
    const Q2 = 'test-caf-pri-tb-hole-' + Date.now();
    const q = new Queue(Q2, { connection: CONNECTION });
    const k = buildKeys(Q2);
    const now = Date.now();

    const current = await q.add('current', { n: 1 });
    const pri = await q.add(
      'priority',
      { seq: 1 },
      {
        ordering: { key: 'pri-tb', tokenBucket: { capacity: 10, refillRate: 10 } },
        cost: 5,
      },
    );
    const next = await q.add(
      'priority',
      { seq: 2 },
      {
        ordering: { key: 'pri-tb', tokenBucket: { capacity: 10, refillRate: 10 } },
        cost: 1,
      },
    );
    expect(current).not.toBeNull();
    expect(pri).not.toBeNull();
    expect(next).not.toBeNull();

    await cleanupClient.lpush(k.priority, [pri!.id]);
    await cleanupClient.xgroupCreate(k.stream, 'workers', '0', { mkStream: true });
    await cleanupClient.hset(k.job(current!.id), { state: 'active', processedOn: String(now) });
    await cleanupClient.hset(k.group('pri-tb'), { tbCapacity: '1', tbTokens: '1', tbRefillRate: '1' });

    const result = await cleanupClient.fcall(
      'glidemq_completeAndFetchNext',
      [k.stream, k.completed, k.events, k.job(current!.id), k.metricsCompleted],
      [
        current!.id,
        '',
        'ok',
        String(now),
        'workers',
        'c1',
        '0',
        '0',
        '0',
        '',
        '',
        '__',
        '',
        '__',
        '0',
        '',
        '0',
        '0',
        '0',
      ],
    );
    const tag = Array.isArray(result) ? String(result[0]) : String(result);
    expect(['NEXT_NONE', 'NEXT_HASH']).toContain(tag);
    expect(String(await cleanupClient.hget(k.job(pri!.id), 'state'))).toBe('failed');
    expect(String(await cleanupClient.hget(k.meta, 'orderdone:pri-tb'))).toBe('1');
    const failedMetrics = await cleanupClient.hgetall(k.metricsFailed);
    const failedCountFields: Record<string, string> = {};
    if (failedMetrics) {
      for (const f of failedMetrics) failedCountFields[String(f.field)] = String(f.value);
    }
    expect(Object.entries(failedCountFields).some(([field, value]) => field.endsWith(':c') && Number(value) > 0)).toBe(
      true,
    );
    const nextState = String(await cleanupClient.hget(k.job(next!.id), 'state'));
    expect(['waiting', 'active', 'completed', 'group-waiting']).toContain(nextState);
    if (nextState === 'group-waiting') {
      const nextSeq = Number(await cleanupClient.hget(k.group('pri-tb'), 'nextSeq'));
      expect(nextSeq).toBeGreaterThan(1);
    }

    await q.close();
    await flushQueue(cleanupClient, Q2);
  }, 15000);
});
