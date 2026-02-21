/**
 * Fuzzy/chaos tester for glide-mq.
 * Tries to break the system with random, concurrent, and adversarial operations.
 * Runs against real Valkey standalone + cluster.
 */
import { it, expect, beforeAll, afterAll } from 'vitest';
import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');
const { QueueEvents } = require('../dist/queue-events') as typeof import('../src/queue-events');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

function uid() {
  return 'fuzz-' + Date.now() + '-' + Math.random().toString(36).slice(2, 6);
}
function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

// ---- Chaos: concurrent producers + consumers ----

describeEachMode('Fuzz: concurrent producers and consumers', (CONNECTION) => {
  const Q = uid();
  let cleanup: any;

  beforeAll(async () => {
    cleanup = await createCleanupClient(CONNECTION);
  });
  afterAll(async () => {
    await flushQueue(cleanup, Q);
    cleanup.close();
  });

  it('3 producers + 3 workers + 300 jobs - no job lost or duplicated', async () => {
    const producedIds = new Set<string>();
    const processedIds = new Set<string>();
    const TOTAL = 300;

    // 3 producers add 100 jobs each in parallel
    const producers = Array.from({ length: 3 }, async (_, i) => {
      const q = new Queue(Q, { connection: CONNECTION });
      for (let j = 0; j < 100; j++) {
        const job = await q.add(`p${i}-j${j}`, { producer: i, index: j });
        producedIds.add(job.id);
      }
      await q.close();
    });

    // 3 workers consuming in parallel
    const workers: any[] = [];
    const done = new Promise<void>((resolve) => {
      let count = 0;
      for (let i = 0; i < 3; i++) {
        const w = new Worker(
          Q,
          async (job: any) => {
            processedIds.add(job.id);
            // Random delay to simulate real work
            if (Math.random() > 0.8) await sleep(Math.random() * 10);
            return { ok: true };
          },
          { connection: CONNECTION, concurrency: 10, blockTimeout: 500 },
        );
        w.on('completed', () => {
          count++;
          if (count >= TOTAL) resolve();
        });
        w.on('error', () => {});
        workers.push(w);
      }
    });

    await Promise.all(producers);
    await Promise.race([
      done,
      sleep(30000).then(() => {
        throw new Error('timeout');
      }),
    ]);

    for (const w of workers) await w.close(true);

    expect(producedIds.size).toBe(TOTAL);
    expect(processedIds.size).toBe(TOTAL);
    // No duplicates: processedIds should match producedIds exactly
    for (const id of producedIds) {
      expect(processedIds.has(id)).toBe(true);
    }
  }, 45000);
});

// ---- Chaos: rapid add+close cycles ----

describeEachMode('Fuzz: rapid queue lifecycle', (CONNECTION) => {
  it('50 create-add-close cycles without leak', async () => {
    for (let i = 0; i < 50; i++) {
      const q = new Queue(uid(), { connection: CONNECTION });
      await q.add('quick', { i });
      await q.close();
    }
    // If we get here without OOM or hanging, the test passes
    expect(true).toBe(true);
  }, 30000);
});

// ---- Chaos: worker close during processing ----

describeEachMode('Fuzz: worker close during active processing', (CONNECTION) => {
  const Q = uid();
  let cleanup: any;

  beforeAll(async () => {
    cleanup = await createCleanupClient(CONNECTION);
  });
  afterAll(async () => {
    await flushQueue(cleanup, Q);
    cleanup.close();
  });

  it('force-close worker while 10 jobs are processing - no crash', async () => {
    const q = new Queue(Q, { connection: CONNECTION });
    for (let i = 0; i < 10; i++) await q.add('slow', { i });

    const w = new Worker(
      Q,
      async () => {
        await sleep(5000); // Long running
        return 'done';
      },
      { connection: CONNECTION, concurrency: 10, blockTimeout: 500 },
    );

    await sleep(500); // Let worker start processing
    await w.close(true); // Force close
    await q.close();
    expect(true).toBe(true); // No crash
  }, 10000);
});

// ---- Chaos: random failures ----

describeEachMode('Fuzz: random processor failures', (CONNECTION) => {
  const Q = uid();
  let cleanup: any;

  beforeAll(async () => {
    cleanup = await createCleanupClient(CONNECTION);
  });
  afterAll(async () => {
    await flushQueue(cleanup, Q);
    cleanup.close();
  });

  it('50% failure rate - all jobs end in completed or failed', async () => {
    const q = new Queue(Q, { connection: CONNECTION });
    const TOTAL = 50;
    const jobIds: string[] = [];

    for (let i = 0; i < TOTAL; i++) {
      const job = await q.add('maybe-fail', { i }, { attempts: 3, backoff: { type: 'fixed', delay: 50 } });
      jobIds.push(job.id);
    }

    let completed = 0;
    let failed = 0;
    const done = new Promise<void>((resolve) => {
      const w = new Worker(
        Q,
        async () => {
          if (Math.random() < 0.5) throw new Error('random fail');
          return 'ok';
        },
        { connection: CONNECTION, concurrency: 5, blockTimeout: 500, stalledInterval: 60000, lockDuration: 60000 },
      );
      w.on('completed', () => {
        completed++;
        if (completed + failed >= TOTAL) resolve();
      });
      w.on('failed', () => {
        failed++;
        if (completed + failed >= TOTAL) resolve();
      });
      w.on('error', () => {});
    });

    await Promise.race([
      done,
      sleep(30000).then(() => {
        throw new Error('timeout');
      }),
    ]);

    // Every job should be in a terminal state
    expect(completed + failed).toBe(TOTAL);
    // With 50% fail rate and 3 attempts, most should eventually complete
    expect(completed).toBeGreaterThan(0);
    await q.close();
  }, 45000);
});

// ---- Chaos: interleaved add and process ----

describeEachMode('Fuzz: interleaved add and process', (CONNECTION) => {
  const Q = uid();
  let cleanup: any;

  beforeAll(async () => {
    cleanup = await createCleanupClient(CONNECTION);
  });
  afterAll(async () => {
    await flushQueue(cleanup, Q);
    cleanup.close();
  });

  it('add jobs while worker is processing - no race', async () => {
    const q = new Queue(Q, { connection: CONNECTION });
    const processed = new Set<string>();
    const TOTAL = 100;

    const w = new Worker(
      Q,
      async (job: any) => {
        processed.add(job.id);
        return 'ok';
      },
      { connection: CONNECTION, concurrency: 5, blockTimeout: 500 },
    );
    w.on('error', () => {});

    // Add jobs one by one with small delays
    const allIds: string[] = [];
    for (let i = 0; i < TOTAL; i++) {
      const job = await q.add('interleaved', { i });
      allIds.push(job.id);
      if (i % 10 === 0) await sleep(10);
    }

    // Wait for all to process
    const deadline = Date.now() + 15000;
    while (processed.size < TOTAL && Date.now() < deadline) {
      await sleep(100);
    }

    await w.close(true);
    await q.close();

    expect(processed.size).toBe(TOTAL);
    for (const id of allIds) {
      expect(processed.has(id)).toBe(true);
    }
  }, 20000);
});

// ---- Chaos: adversarial data ----

describeEachMode('Fuzz: adversarial job data', (CONNECTION) => {
  const Q = uid();
  let cleanup: any;

  beforeAll(async () => {
    cleanup = await createCleanupClient(CONNECTION);
  });
  afterAll(async () => {
    await flushQueue(cleanup, Q);
    cleanup.close();
  });

  it('handles special characters, unicode, null bytes, huge strings', async () => {
    const q = new Queue(Q, { connection: CONNECTION });

    const weirdData = [
      { name: 'null-bytes', data: { text: 'hello\x00world\x00' } },
      { name: 'unicode-emoji', data: { text: '\u{1F4A9}\u{1F525}\u{2764}\u{FE0F}' } },
      { name: 'nested-deep', data: { a: { b: { c: { d: { e: { f: 'deep' } } } } } } },
      { name: 'array-1000', data: { arr: Array.from({ length: 1000 }, (_, i) => i) } },
      { name: 'empty-string', data: { text: '' } },
      { name: 'special-chars', data: { text: '<script>alert("xss")</script>&amp;' } },
      { name: 'backslashes', data: { path: 'C:\\Users\\test\\file.txt' } },
      { name: 'newlines', data: { text: 'line1\nline2\r\nline3\ttab' } },
      { name: 'quotes', data: { text: 'he said "hello" and \'goodbye\'' } },
      { name: 'large-string', data: { text: 'x'.repeat(100000) } },
    ];

    const processed: Record<string, any> = {};
    const w = new Worker(
      Q,
      async (job: any) => {
        processed[job.name] = job.data;
        return job.data;
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 500 },
    );
    w.on('error', () => {});

    for (const item of weirdData) {
      await q.add(item.name, item.data);
    }

    const deadline = Date.now() + 10000;
    while (Object.keys(processed).length < weirdData.length && Date.now() < deadline) {
      await sleep(100);
    }

    await w.close(true);
    await q.close();

    expect(Object.keys(processed).length).toBe(weirdData.length);
    // Verify data roundtrips correctly
    for (const item of weirdData) {
      expect(JSON.stringify(processed[item.name])).toBe(JSON.stringify(item.data));
    }
  }, 15000);
});

// ---- Chaos: pause/resume storm ----

describeEachMode('Fuzz: pause/resume storm during processing', (CONNECTION) => {
  const Q = uid();
  let cleanup: any;

  beforeAll(async () => {
    cleanup = await createCleanupClient(CONNECTION);
  });
  afterAll(async () => {
    await flushQueue(cleanup, Q);
    cleanup.close();
  });

  it('rapid pause/resume while jobs are being processed', async () => {
    const q = new Queue(Q, { connection: CONNECTION });
    for (let i = 0; i < 20; i++) await q.add('storm', { i });

    const processed = new Set<string>();
    const w = new Worker(
      Q,
      async (job: any) => {
        processed.add(job.id);
        await sleep(10);
        return 'ok';
      },
      { connection: CONNECTION, concurrency: 3, blockTimeout: 500 },
    );
    w.on('error', () => {});

    // Storm: pause and resume rapidly
    for (let i = 0; i < 10; i++) {
      await sleep(50);
      await q.pause();
      await sleep(20);
      await q.resume();
    }

    // Wait for processing to finish
    const deadline = Date.now() + 15000;
    while (processed.size < 20 && Date.now() < deadline) {
      await sleep(100);
    }

    await w.close(true);
    await q.close();

    // All 20 should eventually process despite the storm
    expect(processed.size).toBe(20);
  }, 20000);
});

// ---- Chaos: duplicate job IDs (dedup stress) ----

describeEachMode('Fuzz: dedup stress test', (CONNECTION) => {
  const Q = uid();
  let cleanup: any;

  beforeAll(async () => {
    cleanup = await createCleanupClient(CONNECTION);
  });
  afterAll(async () => {
    await flushQueue(cleanup, Q);
    cleanup.close();
  });

  it('100 rapid adds with same dedup ID - only 1 processed', async () => {
    const q = new Queue(Q, { connection: CONNECTION });
    const processed = new Set<string>();

    const w = new Worker(
      Q,
      async (job: any) => {
        processed.add(job.id);
        await sleep(50);
        return 'ok';
      },
      { connection: CONNECTION, concurrency: 5, blockTimeout: 500 },
    );
    w.on('error', () => {});

    // Rapid fire 100 jobs with same dedup ID
    let addedCount = 0;
    for (let i = 0; i < 100; i++) {
      const result = await q.add(
        'dedup-stress',
        { i },
        {
          deduplication: { id: 'same-id', mode: 'simple' },
        },
      );
      if (result !== null) addedCount++;
    }

    await sleep(3000);
    await w.close(true);
    await q.close();

    // Only 1 should have been added and processed
    expect(addedCount).toBe(1);
    expect(processed.size).toBe(1);
  }, 10000);
});

// ---- Chaos: worker restart mid-batch ----

describeEachMode('Fuzz: worker restart mid-batch', (CONNECTION) => {
  const Q = uid();
  let cleanup: any;

  beforeAll(async () => {
    cleanup = await createCleanupClient(CONNECTION);
  });
  afterAll(async () => {
    await flushQueue(cleanup, Q);
    cleanup.close();
  });

  it('kill and restart worker - all 50 jobs eventually complete', async () => {
    const q = new Queue(Q, { connection: CONNECTION });
    for (let i = 0; i < 50; i++) await q.add('restart', { i });

    const processed = new Set<string>();

    // Worker 1: process some, then die
    const w1 = new Worker(
      Q,
      async (job: any) => {
        processed.add(job.id);
        return 'w1';
      },
      {
        connection: CONNECTION,
        concurrency: 5,
        blockTimeout: 500,
        stalledInterval: 2000,
        maxStalledCount: 3,
        lockDuration: 60000,
      },
    );
    w1.on('error', () => {});

    await sleep(1000);
    await w1.close(true); // Kill w1

    // Worker 2: pick up remaining
    const w2 = new Worker(
      Q,
      async (job: any) => {
        processed.add(job.id);
        return 'w2';
      },
      {
        connection: CONNECTION,
        concurrency: 10,
        blockTimeout: 500,
        stalledInterval: 2000,
        maxStalledCount: 3,
        lockDuration: 60000,
      },
    );
    w2.on('error', () => {});

    const deadline = Date.now() + 20000;
    while (processed.size < 50 && Date.now() < deadline) {
      await sleep(200);
    }

    await w2.close(true);
    await q.close();

    expect(processed.size).toBe(50);
  }, 30000);
});

// ---- Chaos: mixed operations storm ----

describeEachMode('Fuzz: mixed operations storm', (CONNECTION) => {
  const Q = uid();
  let cleanup: any;

  beforeAll(async () => {
    cleanup = await createCleanupClient(CONNECTION);
  });
  afterAll(async () => {
    await flushQueue(cleanup, Q);
    cleanup.close();
  });

  it('simultaneous add, getJob, getJobCounts, pause, resume - no crash', async () => {
    const q = new Queue(Q, { connection: CONNECTION });
    const errors: Error[] = [];

    const w = new Worker(
      Q,
      async () => {
        await sleep(5);
        return 'ok';
      },
      { connection: CONNECTION, concurrency: 5, blockTimeout: 500 },
    );
    w.on('error', () => {});

    // Fire all operations simultaneously
    const ops = [];
    for (let i = 0; i < 20; i++) {
      ops.push(q.add('storm-' + i, { i }).catch((e) => errors.push(e)));
      ops.push(q.getJobCounts().catch((e) => errors.push(e)));
      if (i % 5 === 0) ops.push(q.pause().catch((e) => errors.push(e)));
      if (i % 5 === 2) ops.push(q.resume().catch((e) => errors.push(e)));
      if (i > 0) ops.push(q.getJob(String(i)).catch((e) => errors.push(e)));
    }

    await Promise.all(ops);
    await sleep(2000);
    await w.close(true);
    await q.close();

    // No unexpected errors (closing errors are acceptable)
    const realErrors = errors.filter((e) => !e.message.includes('closing') && !e.message.includes('closed'));
    expect(realErrors.length).toBe(0);
  }, 15000);
});
