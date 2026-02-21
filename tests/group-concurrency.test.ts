/**
 * Integration tests for group-level concurrency.
 *
 * Tests ordering: { key, concurrency } where concurrency > 1 allows
 * max N parallel jobs per key.
 *
 * Run: npx vitest run tests/group-concurrency.test.ts
 */
import { it, expect, beforeAll, afterAll } from 'vitest';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describeEachMode('Group concurrency', (CONNECTION) => {
  const Q = 'test-grpconc-' + Date.now();
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

  it('enforces max N parallel jobs per ordering key', async () => {
    const JOB_COUNT = 10;
    const MAX_CONC = 2;
    const JOB_DURATION = 100;
    let peakConcurrent = 0;
    let currentConcurrent = 0;
    const completed: string[] = [];

    for (let i = 0; i < JOB_COUNT; i++) {
      await queue.add('grp-job', { i }, {
        ordering: { key: 'grpA', concurrency: MAX_CONC },
      });
    }

    const worker = new Worker(Q, async (job: any) => {
      currentConcurrent++;
      peakConcurrent = Math.max(peakConcurrent, currentConcurrent);
      await sleep(JOB_DURATION);
      currentConcurrent--;
      completed.push(job.id);
    }, { connection: CONNECTION, concurrency: 10 });

    // Wait for all jobs to complete
    while (completed.length < JOB_COUNT) {
      await sleep(50);
    }

    await worker.close();

    expect(peakConcurrent).toBeLessThanOrEqual(MAX_CONC);
    expect(completed.length).toBe(JOB_COUNT);
  }, 30000);

  it('concurrency=1 produces strict sequential ordering (backward compat)', async () => {
    const Q2 = Q + '-seq';
    const queue2 = new Queue(Q2, { connection: CONNECTION });
    const order: number[] = [];

    for (let i = 0; i < 10; i++) {
      await queue2.add('seq-job', { i }, {
        ordering: { key: 'seqKey', concurrency: 1 },
      });
    }

    const worker = new Worker(Q2, async (job: any) => {
      order.push(job.data.i);
      await sleep(30);
    }, { connection: CONNECTION, concurrency: 5 });

    while (order.length < 10) {
      await sleep(50);
    }

    await worker.close();
    await queue2.close();
    await flushQueue(cleanupClient, Q2);

    // Should be strictly sequential (FIFO)
    for (let i = 1; i < order.length; i++) {
      expect(order[i]).toBeGreaterThan(order[i - 1]);
    }
  }, 20000);

  it('concurrency omitted defaults to 1 (existing ordering behavior)', async () => {
    const Q3 = Q + '-def';
    const queue3 = new Queue(Q3, { connection: CONNECTION });
    let peakConcurrent = 0;
    let currentConcurrent = 0;
    let completed = 0;

    for (let i = 0; i < 5; i++) {
      await queue3.add('def-job', { i }, {
        ordering: { key: 'defKey' },
      });
    }

    const worker = new Worker(Q3, async () => {
      currentConcurrent++;
      peakConcurrent = Math.max(peakConcurrent, currentConcurrent);
      await sleep(50);
      currentConcurrent--;
      completed++;
    }, { connection: CONNECTION, concurrency: 5 });

    while (completed < 5) {
      await sleep(50);
    }

    await worker.close();
    await queue3.close();
    await flushQueue(cleanupClient, Q3);

    expect(peakConcurrent).toBe(1);
  }, 15000);

  it('group A at capacity does not block group B', async () => {
    const Q4 = Q + '-cross';
    const queue4 = new Queue(Q4, { connection: CONNECTION });
    const groupACompletions: number[] = [];
    const groupBCompletions: number[] = [];

    for (let i = 0; i < 3; i++) {
      await queue4.add('grpA', { group: 'A', i }, {
        ordering: { key: 'groupA', concurrency: 1 },
      });
      await queue4.add('grpB', { group: 'B', i }, {
        ordering: { key: 'groupB', concurrency: 1 },
      });
    }

    const worker = new Worker(Q4, async (job: any) => {
      await sleep(80);
      if (job.data.group === 'A') groupACompletions.push(Date.now());
      else groupBCompletions.push(Date.now());
    }, { connection: CONNECTION, concurrency: 10 });

    while (groupACompletions.length < 3 || groupBCompletions.length < 3) {
      await sleep(50);
    }

    await worker.close();
    await queue4.close();
    await flushQueue(cleanupClient, Q4);

    expect(groupACompletions.length).toBe(3);
    expect(groupBCompletions.length).toBe(3);

    // Both groups should process concurrently with each other
    // (total time ~3*80ms, not ~6*80ms)
    const totalSpan = Math.max(
      groupACompletions[2] - groupACompletions[0],
      groupBCompletions[2] - groupBCompletions[0],
    );
    // Each group processes 3 jobs sequentially at ~80ms each
    // Should not exceed ~400ms per group (with some tolerance)
    expect(totalSpan).toBeLessThan(600);
  }, 15000);

  it('different groups with different concurrency limits', async () => {
    const Q5 = Q + '-diffconc';
    const queue5 = new Queue(Q5, { connection: CONNECTION });
    let peakX = 0;
    let currentX = 0;
    let peakY = 0;
    let currentY = 0;
    let completedX = 0;
    let completedY = 0;

    for (let i = 0; i < 8; i++) {
      await queue5.add('xjob', { group: 'X' }, {
        ordering: { key: 'grpX', concurrency: 2 },
      });
    }
    for (let i = 0; i < 8; i++) {
      await queue5.add('yjob', { group: 'Y' }, {
        ordering: { key: 'grpY', concurrency: 4 },
      });
    }

    const worker = new Worker(Q5, async (job: any) => {
      if (job.data.group === 'X') {
        currentX++;
        peakX = Math.max(peakX, currentX);
        await sleep(80);
        currentX--;
        completedX++;
      } else {
        currentY++;
        peakY = Math.max(peakY, currentY);
        await sleep(80);
        currentY--;
        completedY++;
      }
    }, { connection: CONNECTION, concurrency: 20 });

    while (completedX < 8 || completedY < 8) {
      await sleep(50);
    }

    await worker.close();
    await queue5.close();
    await flushQueue(cleanupClient, Q5);

    expect(peakX).toBeLessThanOrEqual(2);
    expect(peakY).toBeLessThanOrEqual(4);
    expect(completedX).toBe(8);
    expect(completedY).toBe(8);
  }, 30000);

  it('job failure decrements group counter and next job proceeds', async () => {
    const Q6 = Q + '-fail';
    const queue6 = new Queue(Q6, { connection: CONNECTION });
    const completed: string[] = [];

    await queue6.add('fail-job', { shouldFail: true }, {
      ordering: { key: 'failGrp', concurrency: 1 },
    });
    await queue6.add('ok-job', { shouldFail: false }, {
      ordering: { key: 'failGrp', concurrency: 1 },
    });

    const worker = new Worker(Q6, async (job: any) => {
      if (job.data.shouldFail) {
        throw new Error('intentional failure');
      }
      completed.push(job.id);
    }, { connection: CONNECTION, concurrency: 5 });

    // Wait for the second job to complete (first fails, should release slot)
    const deadline = Date.now() + 10000;
    while (completed.length < 1 && Date.now() < deadline) {
      await sleep(100);
    }

    await worker.close();
    await queue6.close();
    await flushQueue(cleanupClient, Q6);

    expect(completed.length).toBe(1);
  }, 15000);

  it('mixed grouped and ungrouped jobs on same queue', async () => {
    const Q7 = Q + '-mixed';
    const queue7 = new Queue(Q7, { connection: CONNECTION });
    let peakGrouped = 0;
    let currentGrouped = 0;
    let completedGrouped = 0;
    let completedUngrouped = 0;

    for (let i = 0; i < 4; i++) {
      await queue7.add('grouped', { grouped: true }, {
        ordering: { key: 'mixGrp', concurrency: 1 },
      });
    }
    for (let i = 0; i < 4; i++) {
      await queue7.add('ungrouped', { grouped: false });
    }

    const worker = new Worker(Q7, async (job: any) => {
      if (job.data.grouped) {
        currentGrouped++;
        peakGrouped = Math.max(peakGrouped, currentGrouped);
        await sleep(60);
        currentGrouped--;
        completedGrouped++;
      } else {
        await sleep(30);
        completedUngrouped++;
      }
    }, { connection: CONNECTION, concurrency: 10 });

    const deadline = Date.now() + 15000;
    while ((completedGrouped < 4 || completedUngrouped < 4) && Date.now() < deadline) {
      await sleep(50);
    }

    await worker.close();
    await queue7.close();
    await flushQueue(cleanupClient, Q7);

    expect(peakGrouped).toBeLessThanOrEqual(1);
    expect(completedGrouped).toBe(4);
    expect(completedUngrouped).toBe(4);
  }, 20000);

  it('group counter is zero after all jobs complete', async () => {
    const Q8 = Q + '-zero';
    const queue8 = new Queue(Q8, { connection: CONNECTION });
    let completed = 0;
    const TOTAL = 10;

    for (let i = 0; i < TOTAL; i++) {
      await queue8.add('ctr-job', { i }, {
        ordering: { key: 'zeroGrp', concurrency: 3 },
      });
    }

    const worker = new Worker(Q8, async () => {
      await sleep(30);
      completed++;
    }, { connection: CONNECTION, concurrency: 10 });

    while (completed < TOTAL) {
      await sleep(50);
    }

    await worker.close();

    // Read the group hash directly
    const keys = buildKeys(Q8);
    const groupKey = keys.group('zeroGrp');
    const active = await cleanupClient.hget(groupKey, 'active');
    const activeCount = active ? Number(String(active)) : 0;

    await queue8.close();
    await flushQueue(cleanupClient, Q8);

    expect(activeCount).toBe(0);
  }, 15000);

  it('obliterate cleans up group concurrency state', async () => {
    const Q9 = Q + '-obl';
    const queue9 = new Queue(Q9, { connection: CONNECTION });

    // Add some group jobs and let them complete
    for (let i = 0; i < 3; i++) {
      await queue9.add('obl-job', { i }, {
        ordering: { key: 'oblGrp', concurrency: 2 },
      });
    }

    let completed = 0;
    const worker = new Worker(Q9, async () => {
      completed++;
    }, { connection: CONNECTION, concurrency: 5 });

    while (completed < 3) {
      await sleep(50);
    }
    await worker.close();

    // Obliterate
    await queue9.obliterate({ force: true });

    // Verify group keys are gone
    const keys = buildKeys(Q9);
    const groupKey = keys.group('oblGrp');
    const exists = await cleanupClient.exists([groupKey]);
    expect(Number(exists)).toBe(0);

    await queue9.close();
  }, 15000);

  it('high concurrency value does not artificially limit', async () => {
    const Q10 = Q + '-high';
    const queue10 = new Queue(Q10, { connection: CONNECTION });
    let peakConcurrent = 0;
    let currentConcurrent = 0;
    let completed = 0;
    const TOTAL = 8;
    const WORKER_CONC = 8;

    for (let i = 0; i < TOTAL; i++) {
      await queue10.add('hi-job', { i }, {
        ordering: { key: 'hiGrp', concurrency: 100 },
      });
    }

    const worker = new Worker(Q10, async () => {
      currentConcurrent++;
      peakConcurrent = Math.max(peakConcurrent, currentConcurrent);
      await sleep(100);
      currentConcurrent--;
      completed++;
    }, { connection: CONNECTION, concurrency: WORKER_CONC });

    while (completed < TOTAL) {
      await sleep(50);
    }

    await worker.close();
    await queue10.close();
    await flushQueue(cleanupClient, Q10);

    // All jobs should be able to run in parallel (limited by worker concurrency, not group)
    expect(peakConcurrent).toBeGreaterThan(1);
    expect(completed).toBe(TOTAL);
  }, 15000);
});
