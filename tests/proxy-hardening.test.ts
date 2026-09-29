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
const connectionModule = require('../dist/connection') as { createBlockingClient: (conn: any) => Promise<any> };

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
