/**
 * Regression tests for issue #217 - list-active counter underflow.
 *
 * Multiple Lua FCALL paths DECR `list-active` without guarding against an
 * already-zero counter. Once the counter underflows, `Queue.getJobCounts()`
 * returns negative values and `glidemq_healListActive` cannot recover
 * (early-out at counter <= 0).
 *
 * Each test pre-stages a list-sourced active job hash, sets `list-active`
 * to the value that would expose the bug, calls the affected FCALL, then
 * asserts the counter never goes negative.
 *
 * Run: npx vitest run tests/list-active-counter.test.ts
 */
import { it, expect, beforeAll, afterAll } from 'vitest';
import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

const CONSUMER_GROUP = 'workers';

interface JobOpts {
  lifo?: boolean;
  priority?: number;
  groupKey?: string;
}

async function stageListJob(client: any, k: any, jobId: string, opts: JobOpts = {}): Promise<void> {
  const fields: Record<string, string> = {
    name: 'task',
    data: '{}',
    opts: '{}',
    state: 'active',
    processedOn: String(Date.now()),
    lastActive: String(Date.now()),
    stalledCount: '0',
    attemptsMade: '0',
  };
  if (opts.lifo) fields.lifo = '1';
  if (opts.priority) fields.priority = String(opts.priority);
  if (opts.groupKey) fields.groupKey = opts.groupKey;
  await client.hset(k.job(jobId), fields);
}

async function getListActive(client: any, k: any): Promise<number> {
  const v = await client.get(k.listActive);
  return Number(v);
}

describeEachMode('list-active counter underflow (issue #217)', (CONNECTION) => {
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    cleanupClient.close();
  });

  it('complete-twice on list-sourced job does not underflow', async () => {
    const Q = `la-complete-twice-${Date.now()}`;
    const k = buildKeys(Q);
    const jobId = 'job-1';
    await stageListJob(cleanupClient, k, jobId, { priority: 5 });
    await cleanupClient.set(k.listActive, '1');

    const keys = [k.stream, k.completed, k.events, k.job(jobId), k.metricsCompleted];
    const args = [jobId, '', 'ok', String(Date.now()), CONSUMER_GROUP, '0', '0', '0', '', '', '0'];
    await cleanupClient.fcall('glidemq_complete', keys, args);
    // Reset state to 'active' so the second call mirrors the duplicate-delivery scenario.
    await cleanupClient.hset(k.job(jobId), { state: 'active' });
    await cleanupClient.fcall('glidemq_complete', keys, args);

    expect(await getListActive(cleanupClient, k)).toBeGreaterThanOrEqual(0);
    await flushQueue(cleanupClient, Q);
  });

  it('fail-twice on retry path does not underflow', async () => {
    const Q = `la-fail-retry-twice-${Date.now()}`;
    const k = buildKeys(Q);
    const jobId = 'job-1';
    await stageListJob(cleanupClient, k, jobId, { priority: 5 });
    await cleanupClient.set(k.listActive, '1');

    const keys = [k.stream, k.failed, k.scheduled, k.events, k.job(jobId), k.metricsFailed];
    // maxAttempts=3, attemptsMade starts at 0 -> retry path on both calls.
    const args = [jobId, '', 'boom', String(Date.now()), '3', '0', CONSUMER_GROUP, '0', '0', '0', '0'];
    await cleanupClient.fcall('glidemq_fail', keys, args);
    await cleanupClient.hset(k.job(jobId), { state: 'active', attemptsMade: '0' });
    await cleanupClient.fcall('glidemq_fail', keys, args);

    expect(await getListActive(cleanupClient, k)).toBeGreaterThanOrEqual(0);
    await flushQueue(cleanupClient, Q);
  });

  it('fail-twice on final path does not underflow', async () => {
    const Q = `la-fail-final-twice-${Date.now()}`;
    const k = buildKeys(Q);
    const jobId = 'job-1';
    await stageListJob(cleanupClient, k, jobId, { priority: 5 });
    await cleanupClient.set(k.listActive, '1');

    const keys = [k.stream, k.failed, k.scheduled, k.events, k.job(jobId), k.metricsFailed];
    // maxAttempts=0 -> final-fail path.
    const args = [jobId, '', 'boom', String(Date.now()), '0', '0', CONSUMER_GROUP, '0', '0', '0', '0'];
    await cleanupClient.fcall('glidemq_fail', keys, args);
    await cleanupClient.hset(k.job(jobId), { state: 'active', attemptsMade: '0' });
    await cleanupClient.fcall('glidemq_fail', keys, args);

    expect(await getListActive(cleanupClient, k)).toBeGreaterThanOrEqual(0);
    await flushQueue(cleanupClient, Q);
  });

  it('reclaim-then-complete race does not underflow', async () => {
    const Q = `la-reclaim-then-complete-${Date.now()}`;
    const k = buildKeys(Q);
    const jobId = 'job-1';
    const stale = Date.now() - 60000;
    await cleanupClient.hset(k.job(jobId), {
      name: 'task',
      data: '{}',
      opts: '{}',
      state: 'active',
      lifo: '1',
      processedOn: String(stale),
      lastActive: String(stale),
      stalledCount: '1',
      attemptsMade: '0',
    });
    await cleanupClient.set(k.listActive, '1');

    // Reclaim sweeper fires twice (stalledCount: 1 -> 2 -> exceed): job moves to failed, list-active DECR (guarded) to 0.
    const now = Date.now();
    await cleanupClient.fcall('glidemq_reclaimStalledListJobs', [k.stream, k.events], ['5000', '1', String(now), k.failed]);
    expect(await getListActive(cleanupClient, k)).toBe(0);

    // Original worker resumes (entryId=''), tries to complete. This unguarded DECR currently underflows.
    const completeKeys = [k.stream, k.completed, k.events, k.job(jobId), k.metricsCompleted];
    const completeArgs = [jobId, '', 'ok', String(Date.now()), CONSUMER_GROUP, '0', '0', '0', '', '', '0'];
    await cleanupClient.fcall('glidemq_complete', completeKeys, completeArgs);

    expect(await getListActive(cleanupClient, k)).toBeGreaterThanOrEqual(0);
    await flushQueue(cleanupClient, Q);
  });

  it('suspend-then-fail does not underflow', async () => {
    const Q = `la-suspend-then-fail-${Date.now()}`;
    const k = buildKeys(Q);
    const jobId = 'job-1';
    await stageListJob(cleanupClient, k, jobId, { priority: 5 });
    await cleanupClient.set(k.listActive, '1');

    const suspendKeys = [k.job(jobId), k.stream, k.events, k.suspended];
    const suspendArgs = [jobId, '', CONSUMER_GROUP, String(Date.now()), 'manual', '0', '0'];
    await cleanupClient.fcall('glidemq_suspend', suspendKeys, suspendArgs);
    expect(await getListActive(cleanupClient, k)).toBe(0);

    // After suspend, a stale fail call should not underflow.
    await cleanupClient.hset(k.job(jobId), { state: 'active', attemptsMade: '0' });
    const failKeys = [k.stream, k.failed, k.scheduled, k.events, k.job(jobId), k.metricsFailed];
    const failArgs = [jobId, '', 'boom', String(Date.now()), '0', '0', CONSUMER_GROUP, '0', '0', '0', '0'];
    await cleanupClient.fcall('glidemq_fail', failKeys, failArgs);

    expect(await getListActive(cleanupClient, k)).toBeGreaterThanOrEqual(0);
    await flushQueue(cleanupClient, Q);
  });

  // Parametric sweep: pre-set list-active to 0, trigger each path on a fresh
  // active list-sourced job, assert the counter never goes negative.
  // Covers the 10 unguarded DECR sites enumerated in the issue.
  type Site = { name: string; setup?: (k: any, jobId: string) => Promise<void>; call: (k: any, jobId: string) => Promise<void> };
  const sites: Site[] = [
    {
      name: 'glidemq_complete',
      call: async (k, jobId) => {
        await cleanupClient.fcall(
          'glidemq_complete',
          [k.stream, k.completed, k.events, k.job(jobId), k.metricsCompleted],
          [jobId, '', 'ok', String(Date.now()), CONSUMER_GROUP, '0', '0', '0', '', '', '0'],
        );
      },
    },
    {
      name: 'glidemq_completeAndFetchNext',
      setup: async (k, _jobId) => {
        // Function calls XREADGROUP to fetch next; needs the stream + consumer group to exist.
        try {
          await cleanupClient.xgroupCreate(k.stream, CONSUMER_GROUP, '0', { mkStream: true });
        } catch {}
      },
      call: async (k, jobId) => {
        await cleanupClient.fcall(
          'glidemq_completeAndFetchNext',
          [k.stream, k.completed, k.events, k.job(jobId), k.metricsCompleted],
          [jobId, '', 'ok', String(Date.now()), CONSUMER_GROUP, 'consumer', '0', '0', '0', '', '', '0'],
        );
      },
    },
    {
      name: 'glidemq_fail (retry path)',
      call: async (k, jobId) => {
        await cleanupClient.fcall(
          'glidemq_fail',
          [k.stream, k.failed, k.scheduled, k.events, k.job(jobId), k.metricsFailed],
          [jobId, '', 'boom', String(Date.now()), '3', '0', CONSUMER_GROUP, '0', '0', '0', '0'],
        );
      },
    },
    {
      name: 'glidemq_fail (final path)',
      call: async (k, jobId) => {
        await cleanupClient.fcall(
          'glidemq_fail',
          [k.stream, k.failed, k.scheduled, k.events, k.job(jobId), k.metricsFailed],
          [jobId, '', 'boom', String(Date.now()), '0', '0', CONSUMER_GROUP, '0', '0', '0', '0'],
        );
      },
    },
    {
      name: 'glidemq_removeJob',
      call: async (k, jobId) => {
        await cleanupClient.fcall(
          'glidemq_removeJob',
          [k.job(jobId), k.stream, k.scheduled, k.completed, k.failed, k.events, `glide:{${'rmq'}}:log:${jobId}`],
          [jobId],
        );
      },
    },
    {
      name: 'glidemq_moveActiveToDelayed (changeDelay)',
      call: async (k, jobId) => {
        await cleanupClient.fcall(
          'glidemq_moveActiveToDelayed',
          [k.job(jobId), k.stream, k.scheduled, k.events],
          [jobId, '', String(Date.now()), String(Date.now() + 1000), CONSUMER_GROUP, '{}', '0'],
        );
      },
    },
    {
      name: 'glidemq_moveToWaitingChildren (normal)',
      call: async (k, jobId) => {
        await cleanupClient.fcall(
          'glidemq_moveToWaitingChildren',
          [k.job(jobId), k.stream, k.events],
          [jobId, '', CONSUMER_GROUP, String(Date.now()), '0'],
        );
      },
    },
    {
      name: 'glidemq_moveToWaitingChildren (deps-complete race)',
      setup: async (k, jobId) => {
        // Pre-create deps so totalDeps > 0 and depsCompleted >= totalDeps -> takes the inner branch.
        await cleanupClient.sadd(k.deps(jobId), ['child-a']);
        await cleanupClient.hset(k.job(jobId), { depsCompleted: '1' });
      },
      call: async (k, jobId) => {
        await cleanupClient.fcall(
          'glidemq_moveToWaitingChildren',
          [k.job(jobId), k.stream, k.events],
          [jobId, '', CONSUMER_GROUP, String(Date.now()), '0'],
        );
      },
    },
    {
      name: 'glidemq_rateLimitGroup',
      setup: async (k, jobId) => {
        await cleanupClient.hset(k.job(jobId), { groupKey: 'g1' });
      },
      call: async (k, jobId) => {
        await cleanupClient.fcall(
          'glidemq_rateLimitGroup',
          [k.job(jobId), k.stream],
          [jobId, '', CONSUMER_GROUP, '1000', String(Date.now()), '0', 'requeue', 'front', 'max'],
        );
      },
    },
    {
      name: 'glidemq_suspend',
      call: async (k, jobId) => {
        await cleanupClient.fcall(
          'glidemq_suspend',
          [k.job(jobId), k.stream, k.events, k.suspended],
          [jobId, '', CONSUMER_GROUP, String(Date.now()), 'manual', '0', '0'],
        );
      },
    },
  ];

  for (const site of sites) {
    it(`DECR-site sweep: ${site.name} does not underflow when list-active is 0`, async () => {
      const Q = `la-sweep-${site.name.replace(/[^a-z0-9]/gi, '_')}-${Date.now()}`;
      const k = buildKeys(Q);
      const jobId = 'job-1';
      await stageListJob(cleanupClient, k, jobId, { priority: 5 });
      if (site.setup) await site.setup(k, jobId);
      await cleanupClient.set(k.listActive, '0');

      await site.call(k, jobId);

      expect(await getListActive(cleanupClient, k)).toBeGreaterThanOrEqual(0);
      await flushQueue(cleanupClient, Q);
    });
  }
});
