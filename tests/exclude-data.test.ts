import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { describeEachMode, createCleanupClient, flushQueue, waitFor } from './helpers/fixture';

const { Queue } = require('../dist/queue') as typeof import('../src/queue');
const { Worker } = require('../dist/worker') as typeof import('../src/worker');

describeEachMode('excludeData option', (CONNECTION) => {
  let cleanupClient: any;
  const Q = 'test-exclude-data-' + Date.now();
  let queue: InstanceType<typeof Queue>;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
    queue = new Queue(Q, { connection: CONNECTION });
  });

  afterAll(async () => {
    await queue.close();
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  describe('getJob', () => {
    it('with excludeData omits data and returnvalue', async () => {
      const qName = Q + '-getjob-exc';
      const q = new Queue(qName, { connection: CONNECTION });
      const added = await q.add('test-job', { largePayload: 'x'.repeat(1000) });

      const job = await q.getJob(added!.id, { excludeData: true });
      expect(job).not.toBeNull();
      expect(job!.id).toBe(added!.id);
      expect(job!.name).toBe('test-job');
      expect(job!.data).toBeUndefined();
      expect(job!.returnvalue).toBeUndefined();
      // Metadata should still be present
      expect(job!.timestamp).toBeGreaterThan(0);
      expect(job!.attemptsMade).toBe(0);

      await q.close();
      await flushQueue(cleanupClient, qName);
    });

    it('without excludeData returns full data (backwards compat)', async () => {
      const qName = Q + '-getjob-full';
      const q = new Queue(qName, { connection: CONNECTION });
      const payload = { key: 'value', nested: { a: 1 } };
      const added = await q.add('full-job', payload);

      const job = await q.getJob(added!.id);
      expect(job).not.toBeNull();
      expect(job!.data).toEqual(payload);

      await q.close();
      await flushQueue(cleanupClient, qName);
    });

    it('returns null for non-existent job with excludeData', async () => {
      const job = await queue.getJob('nonexistent-id-99999', { excludeData: true });
      expect(job).toBeNull();
    });
  });

  describe('getJobs', () => {
    it('with excludeData omits data from all returned jobs', async () => {
      const qName = Q + '-getjobs-exc';
      const q = new Queue(qName, { connection: CONNECTION });

      await q.add('j1', { payload: 'aaa' });
      await q.add('j2', { payload: 'bbb' });
      await q.add('j3', { payload: 'ccc' });

      const jobs = await q.getJobs('waiting', 0, -1, { excludeData: true });
      expect(jobs.length).toBe(3);
      for (const job of jobs) {
        expect(job.data).toBeUndefined();
        expect(job.returnvalue).toBeUndefined();
        expect(job.name).toBeDefined();
        expect(job.timestamp).toBeGreaterThan(0);
      }

      await q.close();
      await flushQueue(cleanupClient, qName);
    });

    it('without opts returns full data (backwards compat)', async () => {
      const qName = Q + '-getjobs-full';
      const q = new Queue(qName, { connection: CONNECTION });

      await q.add('j1', { val: 42 });
      const jobs = await q.getJobs('waiting');
      expect(jobs.length).toBe(1);
      expect(jobs[0].data).toEqual({ val: 42 });

      await q.close();
      await flushQueue(cleanupClient, qName);
    });

    it('works for completed state with excludeData', async () => {
      const qName = Q + '-getjobs-completed';
      const q = new Queue(qName, { connection: CONNECTION });

      await q.add('work', { input: 'data' });

      const w = new Worker(qName, async () => 'result-value', { connection: CONNECTION });

      await waitFor(async () => {
        const counts = await q.getJobCounts();
        return counts.completed === 1;
      });

      const completedJobs = await q.getJobs('completed', 0, -1, { excludeData: true });
      expect(completedJobs.length).toBe(1);
      expect(completedJobs[0].data).toBeUndefined();
      expect(completedJobs[0].returnvalue).toBeUndefined();
      expect(completedJobs[0].name).toBe('work');

      // Verify full fetch still works
      const fullJobs = await q.getJobs('completed');
      expect(fullJobs.length).toBe(1);
      expect(fullJobs[0].data).toEqual({ input: 'data' });
      expect(fullJobs[0].returnvalue).toBe('result-value');

      await w.close();
      await q.close();
      await flushQueue(cleanupClient, qName);
    });
  });

  describe('searchJobs', () => {
    it('with excludeData omits data from search results', async () => {
      const qName = Q + '-search-exc';
      const q = new Queue(qName, { connection: CONNECTION });

      await q.add('email', { to: 'a@b.com', body: 'x'.repeat(500) });
      await q.add('sms', { to: '555', body: 'y'.repeat(500) });
      await q.add('email', { to: 'c@d.com', body: 'z'.repeat(500) });

      const results = await q.searchJobs({ state: 'waiting', name: 'email', excludeData: true });
      expect(results.length).toBe(2);
      for (const job of results) {
        expect(job.name).toBe('email');
        expect(job.data).toBeUndefined();
        expect(job.returnvalue).toBeUndefined();
      }

      await q.close();
      await flushQueue(cleanupClient, qName);
    });

    it('with excludeData and data filter fetches data for filtering', async () => {
      const qName = Q + '-search-filter';
      const q = new Queue(qName, { connection: CONNECTION });

      await q.add('task', { userId: '123', size: 'big' });
      await q.add('task', { userId: '456', size: 'small' });

      // data filter requires data to be fetched, so excludeData is effectively ignored
      const results = await q.searchJobs({
        state: 'waiting',
        data: { userId: '123' },
        excludeData: true,
      });
      expect(results.length).toBe(1);
      // data is present because it was needed for the filter
      expect((results[0].data as any).userId).toBe('123');

      await q.close();
      await flushQueue(cleanupClient, qName);
    });
  });
});
