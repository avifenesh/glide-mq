/**
 * Regression tests for issue #213.
 *
 * Two bugs:
 * 1. `getJobs('active')` reads only stream XPENDING, so priority/LIFO
 *    list-backed active jobs are invisible to the dashboard. Their count
 *    is included in `getJobCounts().active`, so the count and the listing
 *    disagree by exactly the list-backed count.
 *
 * 2. `glidemq_reclaimStalledListJobs` (and `glidemq_reclaimStalled`) treats
 *    the worker's `stalledInterval` as the stall threshold. With the
 *    contract that `lockDuration` is the threshold and `stalledInterval`
 *    is the check cadence, list-backed jobs configured for a long lock
 *    get reclaimed early.
 *
 * Run: npx vitest run tests/issue-213.test.ts
 */
import { it, expect, beforeAll, afterAll } from 'vitest';
import { describeEachMode, createCleanupClient, flushQueue, waitFor } from './helpers/fixture';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');

describeEachMode('issue #213 active visibility', (CONNECTION) => {
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    cleanupClient.close();
  });

  it('getJobs(active) returns a list-backed (priority) active job', async () => {
    const qName = `i213-prio-active-${Date.now()}`;
    const queue = new Queue(qName, { connection: CONNECTION });
    let release: () => void = () => {};
    const inFlight = new Promise<void>((resolve) => {
      release = resolve;
    });

    const worker = new Worker(
      qName,
      async () => {
        await inFlight;
        return 'ok';
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 200 },
    );
    worker.on('error', () => {});

    try {
      await queue.add('prio', { v: 1 }, { priority: 5 });
      await waitFor(async () => (await queue.getJobCounts()).active === 1, 5000, 50);
      const active = await queue.getJobs('active');
      expect(active.length).toBe(1);
      expect(active[0].name).toBe('prio');
    } finally {
      release();
      await worker.close(true);
      await queue.close();
      await flushQueue(cleanupClient, qName);
    }
  }, 15000);

  it('getJobs(active) returns a list-backed (lifo) active job', async () => {
    const qName = `i213-lifo-active-${Date.now()}`;
    const queue = new Queue(qName, { connection: CONNECTION });
    let release: () => void = () => {};
    const inFlight = new Promise<void>((resolve) => {
      release = resolve;
    });

    const worker = new Worker(
      qName,
      async () => {
        await inFlight;
        return 'ok';
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 200 },
    );
    worker.on('error', () => {});

    try {
      await queue.add('lifo', { v: 1 }, { lifo: true });
      await waitFor(async () => (await queue.getJobCounts()).active === 1, 5000, 50);
      const active = await queue.getJobs('active');
      expect(active.length).toBe(1);
      expect(active[0].name).toBe('lifo');
    } finally {
      release();
      await worker.close(true);
      await queue.close();
      await flushQueue(cleanupClient, qName);
    }
  }, 15000);

  it('getJobs(active) returns mixed stream + priority + lifo active jobs', async () => {
    const qName = `i213-mixed-active-${Date.now()}`;
    const queue = new Queue(qName, { connection: CONNECTION });
    let release: () => void = () => {};
    const inFlight = new Promise<void>((resolve) => {
      release = resolve;
    });

    const worker = new Worker(
      qName,
      async () => {
        await inFlight;
        return 'ok';
      },
      { connection: CONNECTION, concurrency: 3, blockTimeout: 200 },
    );
    worker.on('error', () => {});

    try {
      await queue.add('stream-job', { v: 1 });
      await queue.add('prio-job', { v: 2 }, { priority: 5 });
      await queue.add('lifo-job', { v: 3 }, { lifo: true });

      await waitFor(async () => (await queue.getJobCounts()).active === 3, 5000, 50);

      const active = await queue.getJobs('active');
      const names = active.map((j: any) => j.name).sort();
      expect(names).toEqual(['lifo-job', 'prio-job', 'stream-job']);
    } finally {
      release();
      await worker.close(true);
      await queue.close();
      await flushQueue(cleanupClient, qName);
    }
  }, 15000);

  it('getJobs(active).length matches getJobCounts().active for mixed sources', async () => {
    const qName = `i213-counts-consistent-${Date.now()}`;
    const queue = new Queue(qName, { connection: CONNECTION });
    let release: () => void = () => {};
    const inFlight = new Promise<void>((resolve) => {
      release = resolve;
    });

    const worker = new Worker(
      qName,
      async () => {
        await inFlight;
        return 'ok';
      },
      { connection: CONNECTION, concurrency: 5, blockTimeout: 200 },
    );
    worker.on('error', () => {});

    try {
      await queue.add('s1', {});
      await queue.add('s2', {});
      await queue.add('p1', {}, { priority: 1 });
      await queue.add('p2', {}, { priority: 2 });
      await queue.add('l1', {}, { lifo: true });

      await waitFor(async () => (await queue.getJobCounts()).active === 5, 5000, 50);

      const counts = await queue.getJobCounts();
      const active = await queue.getJobs('active');
      expect(active.length).toBe(counts.active);
      expect(counts.active).toBe(5);
    } finally {
      release();
      await worker.close(true);
      await queue.close();
      await flushQueue(cleanupClient, qName);
    }
  }, 15000);

  it('getJobs(active) pagination works across stream + list sources', async () => {
    const qName = `i213-paginate-${Date.now()}`;
    const queue = new Queue(qName, { connection: CONNECTION });
    let release: () => void = () => {};
    const inFlight = new Promise<void>((resolve) => {
      release = resolve;
    });

    const worker = new Worker(
      qName,
      async () => {
        await inFlight;
        return 'ok';
      },
      { connection: CONNECTION, concurrency: 6, blockTimeout: 200 },
    );
    worker.on('error', () => {});

    try {
      // 3 stream + 3 priority = 6 active jobs
      await queue.add('s1', {});
      await queue.add('s2', {});
      await queue.add('s3', {});
      await queue.add('p1', {}, { priority: 1 });
      await queue.add('p2', {}, { priority: 2 });
      await queue.add('p3', {}, { priority: 3 });

      await waitFor(async () => (await queue.getJobCounts()).active === 6, 5000, 50);

      const all = await queue.getJobs('active');
      expect(all.length).toBe(6);

      const firstHalf = await queue.getJobs('active', 0, 2);
      expect(firstHalf.length).toBe(3);

      const secondHalf = await queue.getJobs('active', 3, 5);
      expect(secondHalf.length).toBe(3);

      // Combined slices should cover all distinct jobs
      const idSet = new Set([...firstHalf.map((j: any) => j.id), ...secondHalf.map((j: any) => j.id)]);
      expect(idSet.size).toBe(6);
    } finally {
      release();
      await worker.close(true);
      await queue.close();
      await flushQueue(cleanupClient, qName);
    }
  }, 15000);
});

describeEachMode('issue #213 stall reclaim respects lockDuration', (CONNECTION) => {
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    cleanupClient.close();
  });

  it('list-backed job is not reclaimed while within lockDuration (stalledInterval << lockDuration)', async () => {
    const qName = `i213-listlock-${Date.now()}`;
    const queue = new Queue(qName, { connection: CONNECTION });
    let processed = 0;
    let activeJobId: string | null = null;
    let observedStalledCount = 0;

    const worker = new Worker(
      qName,
      async (job: any) => {
        activeJobId = job.id;
        // Sleep ~1.2s - longer than stalledInterval but within lockDuration
        await new Promise((r) => setTimeout(r, 1200));
        processed++;
        return 'ok';
      },
      {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 200,
        lockDuration: 5000,
        stalledInterval: 200,
        promotionInterval: 200,
        maxStalledCount: 1,
      },
    );
    worker.on('error', () => {});

    try {
      await queue.add('prio-long', {}, { priority: 5 });
      // While processor is running, poll the job hash for stalledCount.
      // The reclaim sweep fires every stalledInterval. If the bug is present
      // (threshold = stalledInterval), HINCRBY 'stalledCount' will fire on the
      // very first sweep at ~200ms. We poll for 1s of the 1.2s processor.
      await waitFor(async () => activeJobId !== null, 5000, 50);
      const k = require('../dist/utils').buildKeys(qName);
      const deadline = Date.now() + 1000;
      while (Date.now() < deadline) {
        const sc = await cleanupClient.hget(k.job(activeJobId), 'stalledCount');
        observedStalledCount = Math.max(observedStalledCount, Number(sc) || 0);
        await new Promise((r) => setTimeout(r, 50));
      }
      await waitFor(async () => processed === 1, 8000, 100);

      expect(processed).toBe(1);
      expect(observedStalledCount).toBe(0);
    } finally {
      await worker.close(true);
      await queue.close();
      await flushQueue(cleanupClient, qName);
    }
  }, 15000);

  it('stream-backed job is not reclaimed while within lockDuration (stalledInterval << lockDuration)', async () => {
    const qName = `i213-streamlock-${Date.now()}`;
    const queue = new Queue(qName, { connection: CONNECTION });
    let processed = 0;
    let activeJobId: string | null = null;
    let observedStalledCount = 0;

    const worker = new Worker(
      qName,
      async (job: any) => {
        activeJobId = job.id;
        await new Promise((r) => setTimeout(r, 1200));
        processed++;
        return 'ok';
      },
      {
        connection: CONNECTION,
        concurrency: 1,
        blockTimeout: 200,
        lockDuration: 5000,
        stalledInterval: 200,
        promotionInterval: 200,
        maxStalledCount: 1,
      },
    );
    worker.on('error', () => {});

    try {
      await queue.add('stream-long', {});
      await waitFor(async () => activeJobId !== null, 5000, 50);
      const k = require('../dist/utils').buildKeys(qName);
      const deadline = Date.now() + 1000;
      while (Date.now() < deadline) {
        const sc = await cleanupClient.hget(k.job(activeJobId), 'stalledCount');
        observedStalledCount = Math.max(observedStalledCount, Number(sc) || 0);
        await new Promise((r) => setTimeout(r, 50));
      }
      await waitFor(async () => processed === 1, 8000, 100);

      expect(processed).toBe(1);
      expect(observedStalledCount).toBe(0);
    } finally {
      await worker.close(true);
      await queue.close();
      await flushQueue(cleanupClient, qName);
    }
  }, 15000);
});
