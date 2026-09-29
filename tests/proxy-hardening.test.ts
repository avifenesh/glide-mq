/**
 * Proxy hardening: resource cleanup, bounded ranges, error hygiene, and input limits.
 * Requires: valkey-server running on localhost:6379
 *
 * Run: npx vitest run tests/proxy-hardening.test.ts
 */
import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest';
import http from 'http';
import type { Server } from 'http';
import { createCleanupClient, flushQueue, STANDALONE } from './helpers/fixture';

const { createProxyServer } = require('../dist/proxy/index') as typeof import('../src/proxy/index');
const connectionModule = require('../dist/connection') as {
  createBlockingClient: (conn: any) => Promise<any>;
  createClient: (conn: any) => Promise<any>;
};

const CONNECTION = STANDALONE;
const RUN_ID = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(predicate: () => boolean | Promise<boolean>, timeoutMs = 10000, intervalMs = 25): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await sleep(intervalMs);
  }
  throw new Error(`Timed out after ${timeoutMs}ms`);
}

type TrackedClient = { closed: boolean };

/**
 * Hold the next createBlockingClient() call until released, so a test can disconnect the
 * HTTP client while the route is still awaiting its setup.
 */
const originalCreateBlockingClient = connectionModule.createBlockingClient;
let pendingGate: { onReached: (entry: TrackedClient) => void; released: Promise<void> } | null = null;

function gateNextBlockingClient(): { reached: Promise<TrackedClient>; release: () => void } {
  let release!: () => void;
  const released = new Promise<void>((resolve) => {
    release = resolve;
  });
  let onReached!: (entry: TrackedClient) => void;
  const reached = new Promise<TrackedClient>((resolve) => {
    onReached = resolve;
  });
  pendingGate = { onReached, released };
  return { reached, release };
}

function openRawRequest(url: string): http.ClientRequest {
  const req = http.get(url);
  req.on('error', () => undefined);
  return req;
}

async function listen(app: { listen: (port: number, cb: () => void) => Server }): Promise<{
  baseUrl: string;
  server: Server;
}> {
  return new Promise((resolve) => {
    const server = app.listen(0, () => {
      const addr = server.address();
      if (typeof addr !== 'object' || !addr) throw new Error('listen failed');
      resolve({ baseUrl: `http://127.0.0.1:${addr.port}`, server });
    });
  });
}

describe('HTTP proxy hardening', () => {
  let server: Server;
  let baseUrl: string;
  let proxyClose: () => Promise<void>;
  let cleanupClient: any;
  const queueNames: string[] = [];

  function uniqueQueue(label: string): string {
    const name = `proxy-hard-${RUN_ID}-${label}`;
    queueNames.push(name);
    return name;
  }

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
    connectionModule.createBlockingClient = async (conn: any) => {
      const client = await originalCreateBlockingClient(conn);
      const entry: TrackedClient = { closed: false };
      const close = client.close.bind(client);
      client.close = (...args: unknown[]) => {
        entry.closed = true;
        return close(...args);
      };
      const gate = pendingGate;
      if (gate) {
        pendingGate = null;
        gate.onReached(entry);
        await gate.released;
      }
      return client;
    };

    const proxy = createProxyServer({ connection: CONNECTION });
    proxyClose = proxy.close;
    ({ baseUrl, server } = await listen(proxy.app));
  });

  afterEach(() => {
    pendingGate = null;
  });

  afterAll(async () => {
    connectionModule.createBlockingClient = originalCreateBlockingClient;
    server.closeAllConnections?.();
    await proxyClose();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await Promise.allSettled(queueNames.map((name) => flushQueue(cleanupClient, name)));
    cleanupClient?.close();
  }, 30000);

  async function postJson(path: string, body: unknown): Promise<Response> {
    return fetch(`${baseUrl}${path}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
  }

  describe('SSE disconnect during setup', () => {
    it('queue events: closes the blocking client when the client leaves before the stream starts', async () => {
      const queueName = uniqueQueue('sse-queue');
      const gate = gateNextBlockingClient();
      const req = openRawRequest(`${baseUrl}/queues/${queueName}/events`);
      const tracked = await gate.reached;
      req.destroy();
      await sleep(100);
      gate.release();
      await waitFor(() => tracked.closed, 3000);
    });

    it('job events: closes the blocking client when the client leaves before the stream starts', async () => {
      const queueName = uniqueQueue('sse-job');
      const addRes = await postJson(`/queues/${queueName}/jobs`, { name: 'seed', data: {} });
      expect(addRes.status).toBe(201);
      const { id } = await addRes.json();

      const gate = gateNextBlockingClient();
      const req = openRawRequest(`${baseUrl}/queues/${queueName}/jobs/${id}/events`);
      const tracked = await gate.reached;
      req.destroy();
      await sleep(100);
      gate.release();
      await waitFor(() => tracked.closed, 3000);
    });

    it('broadcast: stops the shared worker so messages are not consumed with no listener', async () => {
      const name = uniqueQueue('sse-bcast');
      const subscription = 'hard-sub';
      const gate = gateNextBlockingClient();
      const req = openRawRequest(`${baseUrl}/broadcast/${name}/events?subscription=${subscription}`);
      const tracked = await gate.reached;
      req.destroy();
      await sleep(100);
      gate.release();
      await waitFor(() => tracked.closed, 5000);
      // A closed client's in-flight XREADGROUP BLOCK stays registered server-side until the
      // proxy's 5s block timeout, so wait it out before publishing.
      await sleep(5500);

      const publishRes = await postJson(`/broadcast/${name}`, { subject: 'orders.created', data: { n: 1 } });
      expect(publishRes.status).toBe(201);
      const { id } = await publishRes.json();

      const controller = new AbortController();
      const res = await fetch(`${baseUrl}/broadcast/${name}/events?subscription=${subscription}`, {
        signal: controller.signal,
      });
      expect(res.status).toBe(200);
      const reader = res.body!.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      try {
        const deadline = Date.now() + 10000;
        while (!buffer.includes(`id: ${id}`) && Date.now() < deadline) {
          const chunk = (await Promise.race([
            reader.read(),
            sleep(Math.max(1, deadline - Date.now())).then(() => ({ done: true, value: undefined })),
          ])) as { done: boolean; value?: Uint8Array };
          if (chunk.done) break;
          buffer += decoder.decode(chunk.value, { stream: true });
        }
        expect(buffer).toContain(`id: ${id}`);
      } finally {
        controller.abort();
        await reader.cancel().catch(() => undefined);
      }
    });
  });
});

describe('HTTP proxy hardening - bounded list ranges', () => {
  let server: Server;
  let baseUrl: string;
  let proxyClose: () => Promise<void>;
  let cleanupClient: any;
  const queueName = `proxy-hard-${RUN_ID}-pages`;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
    const proxy = createProxyServer({ connection: CONNECTION, maxPageSize: 3 });
    proxyClose = proxy.close;
    ({ baseUrl, server } = await listen(proxy.app));
    for (let i = 0; i < 5; i++) {
      const res = await fetch(`${baseUrl}/queues/${queueName}/jobs`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: `page-${i}`, data: { i } }),
      });
      expect(res.status).toBe(201);
    }
  });

  afterAll(async () => {
    server.closeAllConnections?.();
    await proxyClose();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await flushQueue(cleanupClient, queueName).catch(() => undefined);
    cleanupClient?.close();
  }, 30000);

  it('rejects an invalid maxPageSize', () => {
    expect(() => createProxyServer({ connection: CONNECTION, maxPageSize: 0 })).toThrow(/maxPageSize/);
    expect(() => createProxyServer({ connection: CONNECTION, maxPageSize: 1.5 })).toThrow(/maxPageSize/);
  });

  it('GET /jobs caps the default and end=-1 page and rejects oversized spans', async () => {
    const defaultRes = await fetch(`${baseUrl}/queues/${queueName}/jobs?state=waiting`);
    expect(defaultRes.status).toBe(200);
    expect((await defaultRes.json()).jobs).toHaveLength(3);

    const openEnded = await fetch(`${baseUrl}/queues/${queueName}/jobs?state=waiting&start=3&end=-1`);
    expect(openEnded.status).toBe(200);
    expect((await openEnded.json()).jobs).toHaveLength(2);

    const withinCap = await fetch(`${baseUrl}/queues/${queueName}/jobs?state=waiting&start=1&end=3`);
    expect(withinCap.status).toBe(200);
    expect((await withinCap.json()).jobs).toHaveLength(3);

    const oversized = await fetch(`${baseUrl}/queues/${queueName}/jobs?state=waiting&start=0&end=3`);
    expect(oversized.status).toBe(400);
    expect((await oversized.json()).error).toMatch(/maximum page size \(3\)/);
  });

  it('dlq, suspended, replay-all, and clean enforce the cap', async () => {
    const dlq = await fetch(`${baseUrl}/queues/${queueName}/dlq?start=0&end=10`);
    expect(dlq.status).toBe(400);

    const suspended = await fetch(`${baseUrl}/queues/${queueName}/suspended?end=3`);
    expect(suspended.status).toBe(400);
    const suspendedDefault = await fetch(`${baseUrl}/queues/${queueName}/suspended`);
    expect(suspendedDefault.status).toBe(200);

    const replayAll = await fetch(`${baseUrl}/queues/${queueName}/dlq/replay-all`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ count: 4 }),
    });
    expect(replayAll.status).toBe(400);

    const clean = await fetch(`${baseUrl}/queues/${queueName}/clean?state=completed&age=0&limit=4`, {
      method: 'DELETE',
    });
    expect(clean.status).toBe(400);
    const cleanOk = await fetch(`${baseUrl}/queues/${queueName}/clean?state=completed&age=0&limit=3`, {
      method: 'DELETE',
    });
    expect(cleanOk.status).toBe(200);
  });
});

describe('HTTP proxy hardening - queue cache connections', () => {
  let cleanupClient: any;
  const queueNames: string[] = [];

  afterAll(async () => {
    await Promise.allSettled(queueNames.map((name) => flushQueue(cleanupClient, name)));
    cleanupClient?.close();
  });

  it('cached queues and broadcasts share one command client instead of one connection per name', async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
    const originalCreateClient = connectionModule.createClient;
    let created = 0;
    connectionModule.createClient = async (conn: any) => {
      created += 1;
      return originalCreateClient(conn);
    };

    const proxy = createProxyServer({ connection: CONNECTION });
    const { baseUrl, server } = await listen(proxy.app);
    try {
      for (let i = 0; i < 5; i++) {
        const name = `proxy-hard-${RUN_ID}-cache-${i}`;
        queueNames.push(name);
        const counts = await fetch(`${baseUrl}/queues/${name}/counts`);
        expect(counts.status).toBe(200);
        const publish = await fetch(`${baseUrl}/broadcast/${name}`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ subject: 'cache.check', data: { i } }),
        });
        expect(publish.status).toBe(201);
      }
      expect(created).toBe(1);

      const health = await fetch(`${baseUrl}/health`);
      expect((await health.json()).queues).toBe(5);
    } finally {
      connectionModule.createClient = originalCreateClient;
      server.closeAllConnections?.();
      await proxy.close();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  });
  it('a failed shared-client connect does not leave an unhandled rejection and is retried', async () => {
    const originalCreateClient = connectionModule.createClient;
    let calls = 0;
    connectionModule.createClient = async (conn: any) => {
      calls += 1;
      if (calls === 1) throw new Error('connect ECONNREFUSED');
      return originalCreateClient(conn);
    };
    const unhandled: unknown[] = [];
    const onUnhandled = (reason: unknown) => unhandled.push(reason);
    process.on('unhandledRejection', onUnhandled);

    const errors: Error[] = [];
    const proxy = createProxyServer({ connection: CONNECTION, onError: (err) => errors.push(err) });
    const { baseUrl, server } = await listen(proxy.app);
    const name = `proxy-hard-${RUN_ID}-connect-fail`;
    queueNames.push(name);
    try {
      const first = await fetch(`${baseUrl}/queues/${name}/counts`);
      expect(first.status).toBe(500);
      const broadcastFirst = await fetch(`${baseUrl}/broadcast/${name}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ subject: 'connect.fail', data: {} }),
      });
      expect(broadcastFirst.status).toBe(201);
      await new Promise((r) => setTimeout(r, 50));
      expect(unhandled).toEqual([]);

      const retry = await fetch(`${baseUrl}/queues/${name}/counts`);
      expect(retry.status).toBe(200);
    } finally {
      process.off('unhandledRejection', onUnhandled);
      connectionModule.createClient = originalCreateClient;
      server.closeAllConnections?.();
      await proxy.close();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  });
});

describe('HTTP proxy hardening - error responses', () => {
  const { Queue: DistQueue } = require('../dist/queue') as typeof import('../src/queue');
  const { Job: DistJob } = require('../dist/job') as typeof import('../src/job');
  let server: Server;
  let baseUrl: string;
  let proxyClose: () => Promise<void>;
  let cleanupClient: any;
  const logged: Array<{ err: Error; queueName: string }> = [];
  const queueName = `proxy-hard-${RUN_ID}-errors`;
  const restores: Array<() => void> = [];

  function patch<T extends object>(target: T, key: keyof T, impl: unknown): void {
    const original = target[key];
    (target as any)[key] = impl;
    restores.push(() => {
      (target as any)[key] = original;
    });
  }

  function restoreAll(): void {
    for (const restore of restores.splice(0).reverse()) restore();
  }

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
    const proxy = createProxyServer({
      connection: CONNECTION,
      onError: (err, name) => {
        logged.push({ err, queueName: name });
      },
    });
    proxyClose = proxy.close;
    ({ baseUrl, server } = await listen(proxy.app));
  });

  afterEach(() => {
    restoreAll();
    logged.length = 0;
  });

  afterAll(async () => {
    server.closeAllConnections?.();
    await proxyClose();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await flushQueue(cleanupClient, queueName).catch(() => undefined);
    cleanupClient?.close();
  }, 30000);

  async function post(path: string, body: unknown): Promise<Response> {
    return fetch(`${baseUrl}${path}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
  }

  it('POST /jobs/wait hides internal error details and logs them through onError', async () => {
    patch(DistQueue.prototype, 'addAndWait', async () => {
      throw new Error('connection lost to 10.0.0.7:6379');
    });
    const res = await post(`/queues/${queueName}/jobs/wait`, { name: 'w', data: {} });
    expect(res.status).toBe(500);
    expect((await res.json()).error).toBe('Internal server error');
    expect(logged).toHaveLength(1);
    expect(logged[0].err.message).toBe('connection lost to 10.0.0.7:6379');
    expect(logged[0].queueName).toBe(queueName);
  });

  it('POST /jobs/wait maps validation errors to 400 instead of 500', async () => {
    patch(DistQueue.prototype, 'addAndWait', async () => {
      throw new Error('ordering.key must be a non-empty string');
    });
    const res = await post(`/queues/${queueName}/jobs/wait`, { name: 'w', data: {} });
    expect(res.status).toBe(400);
    expect((await res.json()).error).toBe('ordering.key must be a non-empty string');
    expect(logged).toHaveLength(0);
  });

  it('job priority/delay/promote return 500 for transport errors and 400 for known rejections', async () => {
    const addRes = await post(`/queues/${queueName}/jobs`, { name: 'mut', data: {}, opts: { delay: 60000 } });
    expect(addRes.status).toBe(201);
    const { id } = await addRes.json();

    const transport = async () => {
      throw new Error('socket hang up 10.0.0.7:6379');
    };
    patch(DistJob.prototype, 'changePriority', transport);
    patch(DistJob.prototype, 'changeDelay', transport);
    patch(DistJob.prototype, 'promote', transport);

    const cases: Array<[string, unknown]> = [
      ['priority', { priority: 1 }],
      ['delay', { delay: 10 }],
      ['promote', {}],
    ];
    for (const [path, body] of cases) {
      const res = await post(`/queues/${queueName}/jobs/${id}/${path}`, body);
      expect(res.status).toBe(500);
      expect((await res.json()).error).toBe('Internal server error');
    }
    expect(logged).toHaveLength(3);

    restoreAll();
    patch(DistJob.prototype, 'promote', async () => {
      throw new Error('Cannot promote: job is not delayed');
    });
    const rejected = await post(`/queues/${queueName}/jobs/${id}/promote`, {});
    expect(rejected.status).toBe(400);
    expect((await rejected.json()).error).toBe('Cannot promote: job is not delayed');
  });
});

describe('HTTP proxy hardening - flow node limit', () => {
  let server: Server;
  let baseUrl: string;
  let proxyClose: () => Promise<void>;
  let cleanupClient: any;
  const queueName = `proxy-hard-${RUN_ID}-flows`;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
    const proxy = createProxyServer({ connection: CONNECTION });
    proxyClose = proxy.close;
    ({ baseUrl, server } = await listen(proxy.app));
  });

  afterAll(async () => {
    server.closeAllConnections?.();
    await proxyClose();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await flushQueue(cleanupClient, queueName).catch(() => undefined);
    cleanupClient?.close();
  }, 30000);

  async function postFlow(body: unknown): Promise<Response> {
    return fetch(`${baseUrl}/flows`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
  }

  it('rejects a tree flow with more than 1000 nodes', async () => {
    const children = Array.from({ length: 1000 }, (_, i) => ({ name: `c${i}`, queueName, data: {} }));
    const res = await postFlow({ flow: { name: 'root', queueName, data: {}, children } });
    expect(res.status).toBe(400);
    expect((await res.json()).error).toBe('Too many flow nodes (max 1000)');
  });

  it('rejects a tree flow nested deeper than the node limit', async () => {
    let flow: any = { name: 'leaf', queueName, data: {} };
    for (let i = 0; i < 1000; i++) {
      flow = { name: `n${i}`, queueName, data: {}, children: [flow] };
    }
    const res = await postFlow({ flow });
    expect(res.status).toBe(400);
    expect((await res.json()).error).toBe('Too many flow nodes (max 1000)');
  });

  it('rejects a DAG with more than 1000 nodes', async () => {
    const nodes = Array.from({ length: 1001 }, (_, i) => ({ name: `d${i}`, queueName, data: {} }));
    const res = await postFlow({ dag: { nodes } });
    expect(res.status).toBe(400);
    expect((await res.json()).error).toBe('Too many dag nodes (max 1000)');
  });
});

describe('HTTP proxy hardening - broadcast publish options', () => {
  let server: Server;
  let baseUrl: string;
  let proxyClose: () => Promise<void>;
  let cleanupClient: any;
  const queueName = `proxy-hard-${RUN_ID}-bcast-opts`;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
    const proxy = createProxyServer({ connection: CONNECTION });
    proxyClose = proxy.close;
    ({ baseUrl, server } = await listen(proxy.app));
  });

  afterAll(async () => {
    server.closeAllConnections?.();
    await proxyClose();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await flushQueue(cleanupClient, queueName).catch(() => undefined);
    cleanupClient?.close();
  }, 30000);

  it.each([[{ priority: 2 }], [{ lifo: true }]])('rejects %j with 400', async (opts) => {
    const res = await fetch(`${baseUrl}/broadcast/${queueName}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ subject: 's', data: {}, opts }),
    });
    expect(res.status).toBe(400);
    expect((await res.json()).error).toMatch(/priority or lifo/);
describe('HTTP proxy hardening - bounded retry', () => {
  const { Queue: DistQueue } = require('../dist/queue') as typeof import('../src/queue');
  const { Worker: DistWorker } = require('../dist/worker') as typeof import('../src/worker');
  let server: Server;
  let baseUrl: string;
  let proxyClose: () => Promise<void>;
  let cleanupClient: any;
  const queueName = `proxy-hard-${RUN_ID}-retry`;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
    const proxy = createProxyServer({ connection: CONNECTION, maxPageSize: 2 });
    proxyClose = proxy.close;
    ({ baseUrl, server } = await listen(proxy.app));

    const worker = new DistWorker(
      queueName,
      async () => {
        throw new Error('boom');
      },
      { connection: CONNECTION, concurrency: 1, blockTimeout: 500 },
    );
    const queue = new DistQueue(queueName, { connection: CONNECTION });
    try {
      await worker.waitUntilReady();
      for (let i = 0; i < 3; i++) {
        await queue.add(`fail-${i}`, {}, { attempts: 1 });
      }
      await waitFor(async () => (await queue.getJobCounts()).failed === 3, 15000);
    } finally {
      await worker.close(true);
      await queue.close();
    }
  }, 30000);

  afterAll(async () => {
    server.closeAllConnections?.();
    await proxyClose();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await flushQueue(cleanupClient, queueName).catch(() => undefined);
    cleanupClient?.close();
  }, 30000);

  async function retry(body?: unknown): Promise<Response> {
    return fetch(`${baseUrl}/queues/${queueName}/retry`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body ?? {}),
    });
  }

  it('rejects a count above maxPageSize and the retry-all sentinel 0', async () => {
    const oversized = await retry({ count: 3 });
    expect(oversized.status).toBe(400);
    expect((await oversized.json()).error).toMatch(/maximum page size \(2\)/);

    const zero = await retry({ count: 0 });
    expect(zero.status).toBe(400);

    const zeroQuery = await fetch(`${baseUrl}/queues/${queueName}/retry?count=0`, { method: 'POST' });
    expect(zeroQuery.status).toBe(400);
  });

  it('retries at most maxPageSize jobs when count is omitted and returns the retried count', async () => {
    const res = await retry();
    expect(res.status).toBe(200);
    expect((await res.json()).retried).toBe(2);

    const counts = await (await fetch(`${baseUrl}/queues/${queueName}/counts`)).json();
    expect(counts.failed).toBe(1);
  });
});

describe('HTTP proxy hardening - shared client for flows and usage', () => {
  let cleanupClient: any;
  const queueName = `proxy-hard-${RUN_ID}-shared`;

  afterAll(async () => {
    await flushQueue(cleanupClient, queueName).catch(() => undefined);
    cleanupClient?.close();
  });

  it('POST /flows and GET /usage/summary reuse the shared command client', async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
    const originalCreateClient = connectionModule.createClient;
    let created = 0;
    connectionModule.createClient = async (conn: any) => {
      created += 1;
      return originalCreateClient(conn);
    };

    const proxy = createProxyServer({ connection: CONNECTION });
    const { baseUrl, server } = await listen(proxy.app);
    try {
      const counts = await fetch(`${baseUrl}/queues/${queueName}/counts`);
      expect(counts.status).toBe(200);

      for (let i = 0; i < 2; i++) {
        const flow = await fetch(`${baseUrl}/flows`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            flow: { name: 'root', queueName, data: {}, children: [{ name: 'child', queueName, data: {} }] },
          }),
        });
        expect(flow.status).toBe(201);
        const dag = await fetch(`${baseUrl}/flows`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ dag: { nodes: [{ name: `d${i}`, queueName, data: {} }] } }),
        });
        expect(dag.status).toBe(201);
        const usage = await fetch(`${baseUrl}/usage/summary?windowMs=60000&queues=${queueName}`);
        expect(usage.status).toBe(200);
      }
      expect(created).toBe(1);
    } finally {
      connectionModule.createClient = originalCreateClient;
      server.closeAllConnections?.();
      await proxy.close();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  });
});
