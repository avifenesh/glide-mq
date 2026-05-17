/**
 * Integration tests for the HTTP proxy.
 * Requires: valkey-server running on localhost:6379
 *
 * Run: npx vitest run tests/proxy.test.ts
 */
import { it, expect, describe, beforeAll, afterAll } from 'vitest';
import type { Server } from 'http';

const { createProxyServer } = require('../dist/proxy/index') as typeof import('../src/proxy/index');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

import { FlowProducer, Queue, Worker } from '../src';
import { createCleanupClient, flushQueue, STANDALONE } from './helpers/fixture';

const CONNECTION = STANDALONE;

type SseEvent = {
  data: any;
  event?: string;
  id?: string;
};

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(predicate: () => Promise<boolean>, timeoutMs = 10000, intervalMs = 50): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await sleep(intervalMs);
  }
  throw new Error(`Timed out after ${timeoutMs}ms`);
}

function parseSseChunk(chunk: string): SseEvent | null {
  const lines = chunk.split('\n');
  const dataLines: string[] = [];
  let event: string | undefined;
  let id: string | undefined;

  for (const line of lines) {
    if (!line || line.startsWith(':')) continue;
    if (line.startsWith('id:')) {
      id = line.slice(3).trim();
      continue;
    }
    if (line.startsWith('event:')) {
      event = line.slice(6).trim();
      continue;
    }
    if (line.startsWith('data:')) {
      dataLines.push(line.slice(5).trimStart());
    }
  }

  if (dataLines.length === 0) return null;
  const rawData = dataLines.join('\n');
  try {
    return { data: JSON.parse(rawData), event, id };
  } catch {
    return { data: rawData, event, id };
  }
}

function createSseReader(response: Response) {
  const body = response.body;
  if (!body) {
    throw new Error('Missing SSE body');
  }

  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';

  async function nextEvent(timeoutMs = 5000): Promise<SseEvent> {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const separator = buffer.indexOf('\n\n');
      if (separator !== -1) {
        const chunk = buffer.slice(0, separator);
        buffer = buffer.slice(separator + 2);
        const parsed = parseSseChunk(chunk);
        if (parsed) return parsed;
        continue;
      }

      const remaining = Math.max(1, deadline - Date.now());
      const result = (await Promise.race([
        reader.read(),
        new Promise<{ timeout: true }>((resolve) => setTimeout(() => resolve({ timeout: true }), remaining)),
      ])) as Awaited<ReturnType<typeof reader.read>> | { timeout: true };

      if ('timeout' in result) {
        throw new Error(`Timed out waiting for SSE event after ${timeoutMs}ms`);
      }
      if (result.done) {
        throw new Error('SSE stream closed');
      }

      buffer += decoder.decode(result.value, { stream: true }).replace(/\r\n/g, '\n');
    }

    throw new Error(`Timed out waiting for SSE event after ${timeoutMs}ms`);
  }

  async function close(): Promise<void> {
    await reader.cancel().catch(() => undefined);
  }

  return { close, nextEvent };
}

describe('HTTP Proxy', () => {
  let server: Server;
  let baseUrl: string;
  let proxyClose: () => Promise<void>;
  let cleanupClient: any;
  const queueNames: string[] = [];

  function uniqueQueue(label: string): string {
    const name = `proxy-test-${Date.now()}-${label}`;
    queueNames.push(name);
    return name;
  }

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);

    const proxy = createProxyServer({ connection: CONNECTION });
    proxyClose = proxy.close;

    await new Promise<void>((resolve) => {
      server = proxy.app.listen(0, () => {
        const addr = server.address();
        if (typeof addr === 'object' && addr) {
          baseUrl = `http://127.0.0.1:${addr.port}`;
        }
        resolve();
      });
    });
  });

  afterAll(async () => {
    await proxyClose();
    await new Promise<void>((resolve, reject) => server.close((err) => (err ? reject(err) : resolve())));
    for (const name of queueNames) {
      await flushQueue(cleanupClient, name);
    }
    await cleanupClient.close();
  });

  it('POST /queues/:name/jobs - adds a job and returns 201', async () => {
    const queueName = uniqueQueue('add');
    const res = await fetch(`${baseUrl}/queues/${queueName}/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: 'send-email', data: { to: 'user@test.com' } }),
    });

    expect(res.status).toBe(201);
    const body = await res.json();
    expect(body.id).toBeTruthy();
    expect(body.name).toBe('send-email');
    expect(typeof body.timestamp).toBe('number');

    // Verify job exists in Valkey
    const k = buildKeys(queueName);
    const exists = await cleanupClient.exists([k.job(body.id)]);
    expect(exists).toBe(1);
  });

  it('POST /queues/:name/jobs with opts (priority, delay, custom jobId)', async () => {
    const queueName = uniqueQueue('opts');
    const res = await fetch(`${baseUrl}/queues/${queueName}/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        name: 'process-payment',
        data: { amount: 100 },
        opts: { priority: 3, delay: 5000, jobId: 'custom-123' },
      }),
    });

    expect(res.status).toBe(201);
    const body = await res.json();
    expect(body.id).toBe('custom-123');
    expect(body.name).toBe('process-payment');
  });

  it('POST /queues/:name/jobs - dedup returns 200 with skipped', async () => {
    const queueName = uniqueQueue('dedup');

    // Add first job
    await fetch(`${baseUrl}/queues/${queueName}/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        name: 'task',
        data: {},
        opts: { deduplication: { id: 'dedup-1', mode: 'simple' } },
      }),
    });

    // Add duplicate
    const res = await fetch(`${baseUrl}/queues/${queueName}/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        name: 'task',
        data: {},
        opts: { deduplication: { id: 'dedup-1', mode: 'simple' } },
      }),
    });

    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.skipped).toBe(true);
  });

  it('POST /queues/:name/jobs/bulk - adds 3 jobs', async () => {
    const queueName = uniqueQueue('bulk');
    const res = await fetch(`${baseUrl}/queues/${queueName}/jobs/bulk`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jobs: [
          { name: 'job-a', data: { idx: 0 } },
          { name: 'job-b', data: { idx: 1 } },
          { name: 'job-c', data: { idx: 2 } },
        ],
      }),
    });

    expect(res.status).toBe(201);
    const body = await res.json();
    expect(body.jobs).toHaveLength(3);
    for (const job of body.jobs) {
      expect(job.id).toBeTruthy();
      expect(typeof job.timestamp).toBe('number');
    }
  });

  it('GET /queues/:name/jobs/:id - returns 200 for existing job', async () => {
    const queueName = uniqueQueue('get');

    // Add a job first
    const addRes = await fetch(`${baseUrl}/queues/${queueName}/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: 'fetch-test', data: { key: 'value' } }),
    });
    const added = await addRes.json();

    // Fetch it
    const res = await fetch(`${baseUrl}/queues/${queueName}/jobs/${added.id}`);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.id).toBe(added.id);
    expect(body.name).toBe('fetch-test');
    expect(body.data).toEqual({ key: 'value' });
    expect(body.state).toBe('waiting');
    expect(body.attemptsMade).toBe(0);
  });

  it('GET /queues/:name/jobs/:id - returns 404 for unknown job', async () => {
    const queueName = uniqueQueue('404');
    const res = await fetch(`${baseUrl}/queues/${queueName}/jobs/nonexistent-999`);
    expect(res.status).toBe(404);
    const body = await res.json();
    expect(body.error).toBe('Job not found');
  });

  it('DLQ inspection, replay, replay-all, and delete endpoints work', async () => {
    const queueName = uniqueQueue('dlq');
    const dlqName = uniqueQueue('dlq-dead');
    const queue = new Queue(queueName, {
      connection: CONNECTION,
      deadLetterQueue: { name: dlqName },
    });
    const worker = new Worker(
      queueName,
      async () => {
        throw new Error('budget exceeded');
      },
      {
        blockTimeout: 500,
        concurrency: 1,
        connection: CONNECTION,
        deadLetterQueue: { name: dlqName },
      },
    );

    try {
      await worker.waitUntilReady();

      await queue.add('dlq-a', { seq: 1 }, { attempts: 1 });
      await queue.add('dlq-b', { seq: 2 }, { attempts: 1 });
      await queue.add('dlq-c', { seq: 3 }, { attempts: 1 });

      await waitFor(async () => (await queue.getDeadLetterJobs()).length === 3, 10000);
      await worker.close(true);

      const listRes = await fetch(`${baseUrl}/queues/${queueName}/dlq`);
      expect(listRes.status).toBe(200);
      const listBody = await listRes.json();
      expect(listBody.count).toBe(3);
      expect(listBody.jobs[0].originalQueue).toBe(queueName);
      expect(listBody.jobs[0].failedReason).toBe('budget exceeded');

      const [first, second] = listBody.jobs as Array<{ id: string }>;

      const getRes = await fetch(`${baseUrl}/queues/${queueName}/dlq/${first.id}`);
      expect(getRes.status).toBe(200);
      const getBody = await getRes.json();
      expect(getBody.id).toBe(first.id);
      expect(getBody.originalQueue).toBe(queueName);

      const replayRes = await fetch(`${baseUrl}/queues/${queueName}/dlq/${first.id}/replay`, { method: 'POST' });
      expect(replayRes.status).toBe(200);
      const replayBody = await replayRes.json();
      expect(replayBody.newJobId).toBeTruthy();

      await waitFor(async () => {
        const replayed = await queue.getJob(replayBody.newJobId);
        return Boolean(replayed);
      });

      const deleteRes = await fetch(`${baseUrl}/queues/${queueName}/dlq/${second.id}`, { method: 'DELETE' });
      expect(deleteRes.status).toBe(200);
      expect((await deleteRes.json()).removed).toBe(true);

      const replayAllRes = await fetch(`${baseUrl}/queues/${queueName}/dlq/replay-all?count=1`, { method: 'POST' });
      expect(replayAllRes.status).toBe(200);
      const replayAllBody = await replayAllRes.json();
      expect(replayAllBody.replayed).toBe(1);
      expect(replayAllBody.jobs).toHaveLength(1);

      const finalListRes = await fetch(`${baseUrl}/queues/${queueName}/dlq`);
      expect(finalListRes.status).toBe(200);
      const finalListBody = await finalListRes.json();
      expect(finalListBody.count).toBe(0);
    } finally {
      await worker.close(true).catch(() => undefined);
      await queue.close();
    }
  });

  it('POST /queues/:name/pause + resume - returns 200 with state', async () => {
    const queueName = uniqueQueue('pause');

    // Create queue by adding a job first
    await fetch(`${baseUrl}/queues/${queueName}/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: 'warmup', data: {} }),
    });

    // Pause
    const pauseRes = await fetch(`${baseUrl}/queues/${queueName}/pause`, { method: 'POST' });
    expect(pauseRes.status).toBe(200);

    // Verify paused
    const k = buildKeys(queueName);
    const paused = await cleanupClient.hget(k.meta, 'paused');
    expect(String(paused)).toBe('1');

    // Resume
    const resumeRes = await fetch(`${baseUrl}/queues/${queueName}/resume`, { method: 'POST' });
    expect(resumeRes.status).toBe(200);

    // Verify resumed
    const resumed = await cleanupClient.hget(k.meta, 'paused');
    expect(String(resumed)).toBe('0');
  });

  it('GET /queues/:name/counts - returns job counts', async () => {
    const queueName = uniqueQueue('counts');

    // Add some jobs
    await fetch(`${baseUrl}/queues/${queueName}/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: 'count-a', data: {} }),
    });
    await fetch(`${baseUrl}/queues/${queueName}/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: 'count-b', data: {} }),
    });

    const res = await fetch(`${baseUrl}/queues/${queueName}/counts`);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.waiting).toBeGreaterThanOrEqual(2);
    expect(typeof body.active).toBe('number');
    expect(typeof body.delayed).toBe('number');
    expect(typeof body.completed).toBe('number');
    expect(typeof body.failed).toBe('number');
  });

  it('GET /queues/:name/jobs and job mutation endpoints work', async () => {
    const queueName = uniqueQueue('list-mutate');
    const k = buildKeys(queueName);

    const addRes = await fetch(`${baseUrl}/queues/${queueName}/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: 'list-me', data: { secret: true } }),
    });
    const added = await addRes.json();

    const listRes = await fetch(`${baseUrl}/queues/${queueName}/jobs?state=waiting&excludeData=true`);
    expect(listRes.status).toBe(200);
    const listBody = await listRes.json();
    const listed = listBody.jobs.find((job: any) => job.id === added.id);
    expect(listed).toBeTruthy();
    expect(listed.state).toBe('waiting');
    expect(listed.data).toBeUndefined();

    const priorityRes = await fetch(`${baseUrl}/queues/${queueName}/jobs/${added.id}/priority`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ priority: 7 }),
    });
    expect(priorityRes.status).toBe(200);
    expect(await cleanupClient.hget(k.job(added.id), 'priority')).toBe('7');

    const delayRes = await fetch(`${baseUrl}/queues/${queueName}/jobs/${added.id}/delay`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ delay: 60000 }),
    });
    expect(delayRes.status).toBe(200);
    await waitFor(async () => String(await cleanupClient.hget(k.job(added.id), 'state')) === 'delayed');

    const promoteRes = await fetch(`${baseUrl}/queues/${queueName}/jobs/${added.id}/promote`, { method: 'POST' });
    expect(promoteRes.status).toBe(200);
    await waitFor(async () => String(await cleanupClient.hget(k.job(added.id), 'state')) === 'waiting');
  });

  it('jobs/wait, workers, and metrics endpoints work together', async () => {
    const queueName = uniqueQueue('wait-metrics');
    const worker = new Worker(
      queueName,
      async (job: any) => {
        await job.updateProgress({ done: true });
        return { echoed: job.data.value };
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 500 },
    );

    try {
      await worker.waitUntilReady();

      const workersRes = await fetch(`${baseUrl}/queues/${queueName}/workers`);
      expect(workersRes.status).toBe(200);
      const workersBody = await workersRes.json();
      expect(workersBody.workers.length).toBeGreaterThanOrEqual(1);

      const waitRes = await fetch(`${baseUrl}/queues/${queueName}/jobs/wait`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: 'rpc-call',
          data: { value: 42 },
          opts: { waitTimeout: 10000 },
        }),
      });

      expect(waitRes.status).toBe(200);
      const waitBody = await waitRes.json();
      expect(waitBody.result).toEqual({ echoed: 42 });

      const metricsRes = await fetch(`${baseUrl}/queues/${queueName}/metrics?type=completed`);
      expect(metricsRes.status).toBe(200);
      const metricsBody = await metricsRes.json();
      expect(metricsBody.count).toBeGreaterThanOrEqual(1);
      expect(metricsBody.meta.resolution).toBe('minute');
      expect(metricsBody.data.length).toBeGreaterThanOrEqual(1);
    } finally {
      await worker.close(true);
    }
  });

  it('suspended job inspection, revoke, and rate-limit endpoints work', async () => {
    const suspendedQueueName = uniqueQueue('suspended');
    const suspendedQueue = new Queue(suspendedQueueName, { connection: CONNECTION });
    const suspendedWorker = new Worker(
      suspendedQueueName,
      async (job: any) => {
        if (job.signals.length === 0) {
          await job.suspend({ reason: 'awaiting-review', timeout: 60000 });
        }
        return { resumed: true };
      },
      { blockTimeout: 500, concurrency: 1, connection: CONNECTION },
    );

    const revokeQueueName = uniqueQueue('revoke-http');
    const revokeQueue = new Queue(revokeQueueName, { connection: CONNECTION });

    try {
      await suspendedWorker.waitUntilReady();

      const suspendedJob = await suspendedQueue.add('needs-review', { step: 'approve' });
      await waitFor(async () => (await suspendedQueue.getSuspendInfo(suspendedJob.id)) !== null, 10000);

      const suspendedListRes = await fetch(`${baseUrl}/queues/${suspendedQueueName}/suspended`);
      expect(suspendedListRes.status).toBe(200);
      const suspendedListBody = await suspendedListRes.json();
      expect(suspendedListBody.jobs).toHaveLength(1);
      expect(suspendedListBody.jobs[0].id).toBe(suspendedJob.id);
      expect(suspendedListBody.jobs[0].suspend.reason).toBe('awaiting-review');

      const suspendInfoRes = await fetch(`${baseUrl}/queues/${suspendedQueueName}/jobs/${suspendedJob.id}/suspend`);
      expect(suspendInfoRes.status).toBe(200);
      const suspendInfoBody = await suspendInfoRes.json();
      expect(suspendInfoBody.reason).toBe('awaiting-review');

      const revokeJob = await revokeQueue.add('cancel-me', { ok: true });
      const revokeRes = await fetch(`${baseUrl}/queues/${revokeQueueName}/jobs/${revokeJob.id}/revoke`, {
        method: 'POST',
      });
      expect(revokeRes.status).toBe(200);
      expect((await revokeRes.json()).status).toBe('revoked');

      await waitFor(async () => {
        const fetched = await revokeQueue.getJob(revokeJob.id);
        return (await fetched?.getState()) === 'failed';
      });
      const revokedFetched = await revokeQueue.getJob(revokeJob.id);
      expect(revokedFetched?.failedReason).toBe('revoked');

      const initialRateRes = await fetch(`${baseUrl}/queues/${revokeQueueName}/rate-limit`);
      expect(initialRateRes.status).toBe(200);
      expect((await initialRateRes.json()).rateLimit).toBeNull();

      const putRateRes = await fetch(`${baseUrl}/queues/${revokeQueueName}/rate-limit`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ duration: 60000, max: 5 }),
      });
      expect(putRateRes.status).toBe(200);
      expect((await putRateRes.json()).rateLimit).toEqual({ duration: 60000, max: 5 });

      const getRateRes = await fetch(`${baseUrl}/queues/${revokeQueueName}/rate-limit`);
      expect(getRateRes.status).toBe(200);
      expect((await getRateRes.json()).rateLimit).toEqual({ duration: 60000, max: 5 });

      const deleteRateRes = await fetch(`${baseUrl}/queues/${revokeQueueName}/rate-limit`, { method: 'DELETE' });
      expect(deleteRateRes.status).toBe(200);
      expect((await deleteRateRes.json()).rateLimit).toBeNull();
    } finally {
      await suspendedWorker.close(true).catch(() => undefined);
      await suspendedQueue.close();
      await revokeQueue.close();
    }
  });

  it('drain, retry, and clean endpoints work', async () => {
    const drainQueueName = uniqueQueue('drain');
    await fetch(`${baseUrl}/queues/${drainQueueName}/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: 'wait-a', data: {} }),
    });
    await fetch(`${baseUrl}/queues/${drainQueueName}/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: 'wait-b', data: {}, opts: { delay: 10000 } }),
    });

    const drainRes = await fetch(`${baseUrl}/queues/${drainQueueName}/drain?delayed=true`, { method: 'POST' });
    expect(drainRes.status).toBe(200);

    const drainCountsRes = await fetch(`${baseUrl}/queues/${drainQueueName}/counts`);
    const drainCounts = await drainCountsRes.json();
    expect(drainCounts.waiting).toBe(0);
    expect(drainCounts.delayed).toBe(0);

    const retryQueueName = uniqueQueue('retry');
    const retryWorker = new Worker(
      retryQueueName,
      async () => {
        throw new Error('boom');
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 500 },
    );

    try {
      await retryWorker.waitUntilReady();
      const queue = new Queue(retryQueueName, { connection: CONNECTION });
      const added = await queue.add('fails-once', {}, { attempts: 1 });

      await waitFor(async () => {
        const job = await queue.getJob(added.id);
        return (await job?.getState()) === 'failed';
      });

      const retryRes = await fetch(`${baseUrl}/queues/${retryQueueName}/retry`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ count: 1 }),
      });
      expect(retryRes.status).toBe(200);
      expect((await retryRes.json()).retried).toBe(1);
      await waitFor(
        async () => String(await cleanupClient.hget(buildKeys(retryQueueName).job(added.id), 'state')) === 'delayed',
      );
      await queue.close();
    } finally {
      await retryWorker.close(true);
    }

    const cleanQueueName = uniqueQueue('clean');
    const cleanQueue = new Queue(cleanQueueName, { connection: CONNECTION });
    const cleanWorker = new Worker(cleanQueueName, async () => 'done', {
      connection: CONNECTION,
      concurrency: 1,
      blockTimeout: 500,
    });

    try {
      await cleanWorker.waitUntilReady();
      const first = await cleanQueue.add('clean-a', {});
      const second = await cleanQueue.add('clean-b', {});

      await waitFor(async () => {
        const job = await cleanQueue.getJob(second.id);
        return (await job?.getState()) === 'completed';
      });

      const cleanRes = await fetch(`${baseUrl}/queues/${cleanQueueName}/clean?state=completed&age=0&limit=10`, {
        method: 'DELETE',
      });
      expect(cleanRes.status).toBe(200);
      const cleanBody = await cleanRes.json();
      expect(cleanBody.removed).toContain(first.id);
      expect(cleanBody.removed).toContain(second.id);
    } finally {
      await cleanWorker.close(true);
      await cleanQueue.close();
    }
  });

  it('scheduler CRUD endpoints work', async () => {
    const queueName = uniqueQueue('schedulers');

    const putRes = await fetch(`${baseUrl}/queues/${queueName}/schedulers/heartbeat`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        schedule: { every: 60000 },
        template: { data: { source: 'proxy' }, name: 'tick' },
      }),
    });
    expect(putRes.status).toBe(200);

    const listRes = await fetch(`${baseUrl}/queues/${queueName}/schedulers`);
    expect(listRes.status).toBe(200);
    const listBody = await listRes.json();
    expect(listBody.schedulers.some((entry: any) => entry.name === 'heartbeat')).toBe(true);

    const getRes = await fetch(`${baseUrl}/queues/${queueName}/schedulers/heartbeat`);
    expect(getRes.status).toBe(200);
    const getBody = await getRes.json();
    expect(getBody.entry.every).toBe(60000);
    expect(getBody.entry.template.name).toBe('tick');

    const deleteRes = await fetch(`${baseUrl}/queues/${queueName}/schedulers/heartbeat`, { method: 'DELETE' });
    expect(deleteRes.status).toBe(200);

    const missingRes = await fetch(`${baseUrl}/queues/${queueName}/schedulers/heartbeat`);
    expect(missingRes.status).toBe(404);
  });

  it('flow usage and budget endpoints work', async () => {
    const queueName = uniqueQueue('flow');
    const queue = new Queue(queueName, { connection: CONNECTION });
    const flow = new FlowProducer({ connection: CONNECTION });
    const worker = new Worker(
      queueName,
      async (job: any) => {
        if (job.name === 'child-1') {
          await job.reportUsage({
            costs: { total: 0.001 },
            model: 'embed-small',
            tokens: { input: 200 },
          });
        } else if (job.name === 'child-2') {
          await job.reportUsage({
            costs: { total: 0.025 },
            model: 'gpt-4o',
            tokens: { input: 1000, output: 500 },
          });
        } else {
          await job.reportUsage({
            costs: { total: 0.001 },
            model: 'gpt-4o',
            tokens: { input: 50, output: 20 },
          });
        }
        return 'ok';
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 500 },
    );

    try {
      await worker.waitUntilReady();
      const node = await flow.add(
        {
          children: [
            { data: { step: 'embed' }, name: 'child-1', queueName },
            { data: { step: 'generate' }, name: 'child-2', queueName },
          ],
          data: { step: 'aggregate' },
          name: 'parent',
          queueName,
        },
        { budget: { maxTotalCost: 5, maxTotalTokens: 10000, onExceeded: 'pause' } },
      );

      await waitFor(async () => {
        const parent = await queue.getJob(node.job.id);
        return Boolean(parent?.finishedOn);
      }, 10000);

      const usageRes = await fetch(`${baseUrl}/queues/${queueName}/flows/${node.job.id}/usage`);
      expect(usageRes.status).toBe(200);
      const usageBody = await usageRes.json();
      expect(usageBody.jobCount).toBe(3);
      expect(usageBody.totalTokens).toBe(1770);
      expect(usageBody.totalCost).toBeCloseTo(0.027, 4);
      expect(usageBody.models['gpt-4o']).toBe(2);

      const budgetRes = await fetch(`${baseUrl}/queues/${queueName}/flows/${node.job.id}/budget`);
      expect(budgetRes.status).toBe(200);
      const budgetBody = await budgetRes.json();
      expect(budgetBody.maxTotalTokens).toBe(10000);
      expect(budgetBody.maxTotalCost).toBe(5);
      expect(budgetBody.onExceeded).toBe('pause');
    } finally {
      await worker.close(true);
      await flow.close();
      await queue.close();
    }
  });

  it('flow HTTP endpoints create, inspect, and delete tree flows', async () => {
    const rootQueueName = uniqueQueue('flow-http-tree-root');
    const childQueueName = uniqueQueue('flow-http-tree-child');
    const queue = new Queue(rootQueueName, { connection: CONNECTION });
    let flowId: string | null = null;

    try {
      const createRes = await fetch(`${baseUrl}/flows`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          budget: { maxTotalTokens: 5000, onExceeded: 'pause' },
          flow: {
            children: [
              { data: { step: 'child-a' }, name: 'child-a', queueName: childQueueName },
              {
                children: [{ data: { step: 'grandchild' }, name: 'grandchild', queueName: childQueueName }],
                data: { step: 'child-b' },
                name: 'child-b',
                queueName: rootQueueName,
              },
            ],
            data: { step: 'root' },
            name: 'root',
            queueName: rootQueueName,
          },
        }),
      });
      expect(createRes.status).toBe(201);
      const createBody = await createRes.json();
      flowId = createBody.flowId;
      expect(createBody.kind).toBe('tree');
      expect(createBody.nodeCount).toBe(4);
      expect(createBody.root.queueName).toBe(rootQueueName);
      expect(createBody.roots).toHaveLength(1);

      const inspectRes = await fetch(`${baseUrl}/flows/${flowId}`);
      expect(inspectRes.status).toBe(200);
      const inspectBody = await inspectRes.json();
      expect(inspectBody.flowId).toBe(flowId);
      expect(inspectBody.kind).toBe('tree');
      expect(inspectBody.nodes).toHaveLength(4);
      expect(inspectBody.nodes.map((node: any) => node.name).sort()).toEqual([
        'child-a',
        'child-b',
        'grandchild',
        'root',
      ]);
      expect(inspectBody.nodes.find((node: any) => node.name === 'child-a').parentQueue).toBe(rootQueueName);
      expect(inspectBody.budget.maxTotalTokens).toBe(5000);
      expect(inspectBody.budget.onExceeded).toBe('pause');
      expect(inspectBody.usage.jobCount).toBe(0);

      const treeRes = await fetch(`${baseUrl}/flows/${flowId}/tree`);
      expect(treeRes.status).toBe(200);
      const treeBody = await treeRes.json();
      expect(treeBody.tree).toHaveLength(1);
      expect(treeBody.tree[0].name).toBe('root');
      expect(treeBody.tree[0].children).toHaveLength(2);
      expect(treeBody.tree[0].queueName).toBe(rootQueueName);
      expect(treeBody.tree[0].children.find((node: any) => node.name === 'child-a').queueName).toBe(childQueueName);
      const childB = treeBody.tree[0].children.find((node: any) => node.name === 'child-b');
      expect(childB.children).toHaveLength(1);
      expect(childB.children[0].name).toBe('grandchild');
      expect(childB.children[0].queueName).toBe(childQueueName);
      expect(childB.children[0].parentQueue).toBe(rootQueueName);

      const deleteRes = await fetch(`${baseUrl}/flows/${flowId}`, { method: 'DELETE' });
      expect(deleteRes.status).toBe(200);
      const deleteBody = await deleteRes.json();
      expect(deleteBody.flowId).toBe(flowId);
      expect(deleteBody.jobs).toHaveLength(4);
      expect(deleteBody.revoked + deleteBody.flagged + deleteBody.skipped).toBe(4);

      const rootJob = await queue.getJob(flowId);
      expect(rootJob).not.toBeNull();
      const rootState = await rootJob!.getState();
      expect(['failed', 'waiting-children']).toContain(rootState);

      const missingRes = await fetch(`${baseUrl}/flows/${flowId}`);
      expect(missingRes.status).toBe(404);
    } finally {
      await queue.close();
      if (flowId) {
        await fetch(`${baseUrl}/flows/${flowId}`, { method: 'DELETE' }).catch(() => undefined);
      }
    }
  });

  it('flow HTTP endpoints create DAG flows and reject unsupported DAG budgets', async () => {
    const queueName = uniqueQueue('flow-http-dag');
    let flowId: string | null = null;

    try {
      const invalidRes = await fetch(`${baseUrl}/flows`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          budget: { maxTotalTokens: 1000, onExceeded: 'fail' },
          dag: {
            nodes: [
              { data: { step: 'A' }, name: 'A', queueName },
              { data: { step: 'B' }, deps: ['A'], name: 'B', queueName },
            ],
          },
        }),
      });
      expect(invalidRes.status).toBe(400);
      const invalidBody = await invalidRes.json();
      expect(invalidBody.error).toContain('tree flows');

      const createRes = await fetch(`${baseUrl}/flows`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          dag: {
            nodes: [
              { data: { step: 'A' }, name: 'A', queueName },
              { data: { step: 'B' }, deps: ['A'], name: 'B', queueName },
              { data: { step: 'C' }, deps: ['A'], name: 'C', queueName },
              { data: { step: 'D' }, deps: ['B', 'C'], name: 'D', queueName },
            ],
          },
        }),
      });
      expect(createRes.status).toBe(201);
      const createBody = await createRes.json();
      flowId = createBody.flowId;
      expect(createBody.kind).toBe('dag');
      expect(createBody.nodeCount).toBe(4);
      expect(createBody.roots).toEqual([{ jobId: expect.any(String), queueName }]);

      const inspectRes = await fetch(`${baseUrl}/flows/${flowId}`);
      expect(inspectRes.status).toBe(200);
      const inspectBody = await inspectRes.json();
      expect(inspectBody.kind).toBe('dag');
      expect(inspectBody.nodes).toHaveLength(4);
      expect(inspectBody.budget).toBeNull();
      expect(inspectBody.usage.jobCount).toBe(0);
      expect(inspectBody.usage.totalTokens).toBe(0);
      // Under deps="must complete before me" semantic: A has no deps so it
      // runs first; A's BullMQ parents (the dependents B and C) are stored
      // in parentIds. D has no dependents - parentIds is unset.
      const nodeA = inspectBody.nodes.find((node: any) => node.name === 'A');
      expect(nodeA.parentIds).toHaveLength(2);

      const treeRes = await fetch(`${baseUrl}/flows/${flowId}/tree`);
      expect(treeRes.status).toBe(200);
      const treeBody = await treeRes.json();
      // The tree endpoint roots at user-facing roots (nodes with no deps).
      // Children are resolved from each node's parentId/parentIds. With the
      // corrected deps semantic the leaf A holds its dependents (B, C) in
      // parentIds, so traversing parentIds as "my parents in the tree map"
      // means A's tree-children are empty - the tree is just the root list.
      expect(treeBody.tree).toHaveLength(1);
      expect(treeBody.tree[0].name).toBe('A');
    } finally {
      if (flowId) {
        await fetch(`${baseUrl}/flows/${flowId}`, { method: 'DELETE' }).catch(() => undefined);
      }
    }
  });

  it('global usage summary endpoint aggregates across queues and supports filtering', async () => {
    const firstQueueName = uniqueQueue('usage-a');
    const secondQueueName = uniqueQueue('usage-b');
    const firstQueue = new Queue(firstQueueName, { connection: CONNECTION });
    const secondQueue = new Queue(secondQueueName, { connection: CONNECTION });

    try {
      const first = await firstQueue.add('summary-a', { prompt: 'hello' });
      const second = await secondQueue.add('summary-b', { prompt: 'world' });
      const firstJob = await firstQueue.getJob(first.id);
      const secondJob = await secondQueue.getJob(second.id);

      await firstJob!.reportUsage({
        costs: { total: 0.002 },
        costUnit: 'usd',
        model: 'gpt-5.4-mini',
        tokens: { input: 30, output: 10 },
      });
      await secondJob!.reportUsage({
        costs: { total: 0.001 },
        costUnit: 'usd',
        model: 'text-embedding-3-small',
        tokens: { input: 25 },
      });

      const summaryRes = await fetch(
        `${baseUrl}/usage/summary?windowMs=300000&queues=${encodeURIComponent(`${firstQueueName},${secondQueueName}`)}`,
      );
      expect(summaryRes.status).toBe(200);
      const summaryBody = await summaryRes.json();
      expect(summaryBody.jobCount).toBe(2);
      expect(summaryBody.totalTokens).toBe(65);
      expect(summaryBody.totalCost).toBeCloseTo(0.003, 10);
      expect(summaryBody.models['gpt-5.4-mini']).toBe(1);
      expect(summaryBody.models['text-embedding-3-small']).toBe(1);
      expect(summaryBody.perQueue[firstQueueName].totalTokens).toBe(40);
      expect(summaryBody.perQueue[secondQueueName].totalTokens).toBe(25);

      const filteredRes = await fetch(
        `${baseUrl}/usage/summary?window=300000&queues=${encodeURIComponent(firstQueueName)}`,
      );
      expect(filteredRes.status).toBe(200);
      const filteredBody = await filteredRes.json();
      expect(filteredBody.queues).toEqual([firstQueueName]);
      expect(filteredBody.jobCount).toBe(1);
      expect(filteredBody.totalTokens).toBe(40);

      const invalidWindowRes = await fetch(`${baseUrl}/usage/summary?window=1000&windowMs=2000`);
      expect(invalidWindowRes.status).toBe(400);
      const invalidWindowBody = await invalidWindowRes.json();
      expect(invalidWindowBody.error).toContain('window and windowMs must match');
    } finally {
      await firstQueue.close();
      await secondQueue.close();
    }
  });

  it('queue events SSE supports replay via Last-Event-ID', async () => {
    const queueName = uniqueQueue('queue-events');
    await fetch(`${baseUrl}/queues/${queueName}/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: 'replayed-job', data: { ok: true } }),
    });

    const response = await fetch(`${baseUrl}/queues/${queueName}/events`, {
      headers: { 'Last-Event-ID': '0' },
    });
    expect(response.status).toBe(200);

    const reader = createSseReader(response);
    try {
      const event = await reader.nextEvent();
      expect(event.id).toBeTruthy();
      expect(event.event).toBe('added');
      expect(event.data.jobId).toBeTruthy();
      expect(event.data.name).toBe('replayed-job');
    } finally {
      await reader.close();
    }
  });

  it('per-job lifecycle SSE replays matching events only', async () => {
    const queueName = uniqueQueue('job-events');
    const queue = new Queue(queueName, { connection: CONNECTION });
    const worker = new Worker(
      queueName,
      async (job: any) => {
        await job.updateProgress(50);
        return { ok: true };
      },
      { blockTimeout: 500, concurrency: 1, connection: CONNECTION },
    );

    try {
      await worker.waitUntilReady();
      const job = await queue.add('stream-me', { hello: 'world' });

      await waitFor(async () => {
        const fetched = await queue.getJob(job.id);
        return (await fetched?.getState()) === 'completed';
      }, 10000);

      const response = await fetch(`${baseUrl}/queues/${queueName}/jobs/${job.id}/events`, {
        headers: { 'Last-Event-ID': '0' },
      });
      expect(response.status).toBe(200);

      const reader = createSseReader(response);
      const seenEvents: SseEvent[] = [];
      try {
        for (let i = 0; i < 10; i++) {
          const event = await reader.nextEvent();
          seenEvents.push(event);
          if (event.event === 'completed') break;
        }
      } finally {
        await reader.close();
      }

      expect(seenEvents.some((event) => event.event === 'added' && event.data.jobId === job.id)).toBe(true);
      expect(seenEvents.some((event) => event.event === 'progress' && String(event.data.jobId) === job.id)).toBe(true);
      expect(seenEvents.some((event) => event.event === 'completed' && event.data.jobId === job.id)).toBe(true);
      expect(seenEvents.every((event) => String(event.data.jobId) === job.id)).toBe(true);
    } finally {
      await worker.close(true).catch(() => undefined);
      await queue.close();
    }
  });

  it('broadcast publish and SSE fan out to multiple clients on the same subscription', async () => {
    const queueName = uniqueQueue('broadcast');

    const firstResponse = await fetch(`${baseUrl}/broadcast/${queueName}/events?subscription=proxy-sub`);
    const secondResponse = await fetch(
      `${baseUrl}/broadcast/${queueName}/events?subscription=proxy-sub&subjects=orders.*`,
    );
    expect(firstResponse.status).toBe(200);
    expect(secondResponse.status).toBe(200);

    const firstReader = createSseReader(firstResponse);
    const secondReader = createSseReader(secondResponse);

    try {
      await sleep(100);

      const publishRes = await fetch(`${baseUrl}/broadcast/${queueName}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ subject: 'orders.created', data: { orderId: '123' } }),
      });
      expect(publishRes.status).toBe(201);
      const publishBody = await publishRes.json();
      expect(publishBody.subject).toBe('orders.created');

      const [firstEvent, secondEvent] = await Promise.all([
        firstReader.nextEvent(10000),
        secondReader.nextEvent(10000),
      ]);
      expect(firstEvent.event).toBe('message');
      expect(secondEvent.event).toBe('message');
      expect(firstEvent.data.subject).toBe('orders.created');
      expect(secondEvent.data.subject).toBe('orders.created');
      expect(firstEvent.data.data).toEqual({ orderId: '123' });
      expect(secondEvent.data.data).toEqual({ orderId: '123' });
    } finally {
      await Promise.all([firstReader.close(), secondReader.close()]);
    }
  });

  it('GET /health - returns 200 with status, uptime, and queues count', async () => {
    const res = await fetch(`${baseUrl}/health`);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.status).toBe('ok');
    expect(typeof body.uptime).toBe('number');
    expect(typeof body.queues).toBe('number');
  });

  it('missing body fields return 400', async () => {
    const queueName = uniqueQueue('missing');

    // Missing name
    const res = await fetch(`${baseUrl}/queues/${queueName}/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ data: { foo: 'bar' } }),
    });
    expect(res.status).toBe(400);
    const body = await res.json();
    expect(body.error).toContain('name');
  });

  it('POST /queues/:name/jobs/bulk - invalid job in array returns 400', async () => {
    const queueName = uniqueQueue('bulkinvalid');
    const res = await fetch(`${baseUrl}/queues/${queueName}/jobs/bulk`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jobs: [{ name: 'valid', data: {} }, { data: { no_name: true } }],
      }),
    });
    expect(res.status).toBe(400);
    const body = await res.json();
    expect(body.error).toContain('jobs[1]');
  });

  it('POST /queues/:name/jobs/bulk - missing jobs array returns 400', async () => {
    const queueName = uniqueQueue('bulk400');

    const res = await fetch(`${baseUrl}/queues/${queueName}/jobs/bulk`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ notJobs: true }),
    });
    expect(res.status).toBe(400);
    const body = await res.json();
    expect(body.error).toContain('jobs');
  });

  it('POST /queues/:name/jobs/bulk - more than 1000 jobs returns 400', async () => {
    const queueName = uniqueQueue('bulk1001');
    const jobs = Array.from({ length: 1001 }, (_, i) => ({ name: `job-${i}`, data: {} }));

    const res = await fetch(`${baseUrl}/queues/${queueName}/jobs/bulk`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ jobs }),
    });
    expect(res.status).toBe(400);
    const body = await res.json();
    expect(body.error).toContain('1000');
  });

  it('POST /queues/:name/jobs - queue name with curly braces returns 400', async () => {
    const res = await fetch(`${baseUrl}/queues/bad{queue}name/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: 'task', data: {} }),
    });
    expect(res.status).toBe(400);
    const body = await res.json();
    expect(body.error).toBeTruthy();
  });

  it('POST /queues/:name/jobs - queue name with colon returns 400', async () => {
    const res = await fetch(`${baseUrl}/queues/bad:queue/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: 'task', data: {} }),
    });
    expect(res.status).toBe(400);
    const body = await res.json();
    expect(body.error).toBeTruthy();
  });
});

describe('HTTP Proxy - createProxyServer validation', () => {
  it('throws when neither connection nor client provided', () => {
    expect(() => createProxyServer({} as any)).toThrow('connection');
  });
});

describe('HTTP Proxy - Queue Allowlist', () => {
  let server: Server;
  let baseUrl: string;
  let proxyClose: () => Promise<void>;
  let cleanupClient: any;
  const allowedQueue = `proxy-allow-${Date.now()}`;
  const blockedQueue = `proxy-block-${Date.now()}`;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);

    const proxy = createProxyServer({
      connection: CONNECTION,
      queues: [allowedQueue],
    });
    proxyClose = proxy.close;

    await new Promise<void>((resolve) => {
      server = proxy.app.listen(0, () => {
        const addr = server.address();
        if (typeof addr === 'object' && addr) {
          baseUrl = `http://127.0.0.1:${addr.port}`;
        }
        resolve();
      });
    });
  });

  afterAll(async () => {
    await proxyClose();
    await new Promise<void>((resolve, reject) => server.close((err) => (err ? reject(err) : resolve())));
    await flushQueue(cleanupClient, allowedQueue);
    await cleanupClient.close();
  });

  it('allowed queue returns 201', async () => {
    const res = await fetch(`${baseUrl}/queues/${allowedQueue}/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: 'ok', data: {} }),
    });
    expect(res.status).toBe(201);
  });

  it('blocked queue returns 403', async () => {
    const res = await fetch(`${baseUrl}/queues/${blockedQueue}/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: 'blocked', data: {} }),
    });
    expect(res.status).toBe(403);
    const body = await res.json();
    expect(body.error).toContain('not in the allowlist');
  });

  it('blocked queue GET also returns 403', async () => {
    const res = await fetch(`${baseUrl}/queues/${blockedQueue}/jobs/1`);
    expect(res.status).toBe(403);
  });

  it('rejects opts.parent on add endpoint', async () => {
    const res = await fetch(`${baseUrl}/queues/${allowedQueue}/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        name: 'child',
        opts: { parent: { queue: blockedQueue, id: 'parent-1' } },
      }),
    });
    expect(res.status).toBe(400);
    const body = await res.json();
    expect(body.error).toContain('opts.parent is not allowed');
  });

  it('rejects opts.parent on bulk add endpoint', async () => {
    const res = await fetch(`${baseUrl}/queues/${allowedQueue}/jobs/bulk`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jobs: [{ name: 'child', opts: { parent: { queue: blockedQueue, id: 'parent-2' } } }],
      }),
    });
    expect(res.status).toBe(400);
    const body = await res.json();
    expect(body.error).toContain('jobs[0]: opts.parent is not allowed');
  });

  it('health endpoint is not blocked by allowlist', async () => {
    const res = await fetch(`${baseUrl}/health`);
    expect(res.status).toBe(200);
  });

  it('usage summary respects the allowlist', async () => {
    const blockedRes = await fetch(`${baseUrl}/usage/summary?queues=${encodeURIComponent(blockedQueue)}`);
    expect(blockedRes.status).toBe(403);

    const allowedRes = await fetch(`${baseUrl}/usage/summary`);
    expect(allowedRes.status).toBe(200);
    const body = await allowedRes.json();
    expect(Array.isArray(body.queues)).toBe(true);
  });
});

describe('HTTP Proxy - Payload Limits', () => {
  let server: Server;
  let baseUrl: string;
  let proxyClose: () => Promise<void>;
  let cleanupClient: any;
  const queueNames: string[] = [];

  function uniqueQueue(label: string): string {
    const name = `proxy-limit-${Date.now()}-${label}`;
    queueNames.push(name);
    return name;
  }

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);

    const proxy = createProxyServer({ connection: CONNECTION });
    proxyClose = proxy.close;

    await new Promise<void>((resolve) => {
      server = proxy.app.listen(0, () => {
        const addr = server.address();
        if (typeof addr === 'object' && addr) {
          baseUrl = `http://127.0.0.1:${addr.port}`;
        }
        resolve();
      });
    });
  });

  afterAll(async () => {
    await proxyClose();
    await new Promise<void>((resolve, reject) => server.close((err) => (err ? reject(err) : resolve())));
    for (const name of queueNames) {
      await flushQueue(cleanupClient, name);
    }
    await cleanupClient.close();
  });

  it('rejects payloads exceeding 1MB', async () => {
    const queueName = uniqueQueue('large');
    // Generate a string > 1MB
    const largeData = 'x'.repeat(1.1 * 1024 * 1024);

    const res = await fetch(`${baseUrl}/queues/${queueName}/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: 'big', data: { payload: largeData } }),
    });

    // Express will return 413 Payload Too Large (express.json limit)
    // or 500 if glide-mq rejects it - either is acceptable
    expect(res.status).toBeGreaterThanOrEqual(400);
  });
});
