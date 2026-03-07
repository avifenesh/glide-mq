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

import { createCleanupClient, flushQueue, STANDALONE } from './helpers/fixture';

const CONNECTION = STANDALONE;

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

  it('POST /queues/:name/pause + resume - returns 204', async () => {
    const queueName = uniqueQueue('pause');

    // Create queue by adding a job first
    await fetch(`${baseUrl}/queues/${queueName}/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: 'warmup', data: {} }),
    });

    // Pause
    const pauseRes = await fetch(`${baseUrl}/queues/${queueName}/pause`, { method: 'POST' });
    expect(pauseRes.status).toBe(204);

    // Verify paused
    const k = buildKeys(queueName);
    const paused = await cleanupClient.hget(k.meta, 'paused');
    expect(String(paused)).toBe('1');

    // Resume
    const resumeRes = await fetch(`${baseUrl}/queues/${queueName}/resume`, { method: 'POST' });
    expect(resumeRes.status).toBe(204);

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

  it('GET /health - returns 200 with status and uptime, no queues field', async () => {
    const res = await fetch(`${baseUrl}/health`);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.status).toBe('ok');
    expect(typeof body.uptime).toBe('number');
    expect('queues' in body).toBe(false);
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

  it('health endpoint is not blocked by allowlist', async () => {
    const res = await fetch(`${baseUrl}/health`);
    expect(res.status).toBe(200);
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
