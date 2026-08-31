/**
 * Proxy close() must drain SSE and reject new queue/broadcast work with 503.
 */
import { describe, it, expect, afterAll } from 'vitest';
import type { Server } from 'http';
import { createCleanupClient, flushQueue, STANDALONE } from './helpers/fixture';

const { createProxyServer } = require('../dist/proxy/index') as typeof import('../src/proxy/index');

const CONNECTION = STANDALONE;

async function listen(proxy: { app: { listen: (port: number, cb: () => void) => Server } }): Promise<{
  baseUrl: string;
  server: Server;
}> {
  return new Promise((resolve) => {
    const server = proxy.app.listen(0, () => {
      const addr = server.address();
      if (typeof addr !== 'object' || !addr) {
        throw new Error('proxy listen failed');
      }
      resolve({ baseUrl: `http://127.0.0.1:${addr.port}`, server });
    });
  });
}

describe('HTTP proxy shutdown', () => {
  let cleanupClient: any;
  const queueNames: string[] = [];

  afterAll(async () => {
    for (const name of queueNames) {
      await flushQueue(cleanupClient, name).catch(() => undefined);
    }
    cleanupClient?.close();
  });

  it('close() ends live SSE and returns 503 for new queue and broadcast work', async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
    const queueName = `proxy-shut-${Date.now()}`;
    queueNames.push(queueName);

    const proxy = createProxyServer({ connection: CONNECTION });
    const { baseUrl, server } = await listen(proxy);

    try {
      const addRes = await fetch(`${baseUrl}/queues/${queueName}/jobs`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'seed', data: {} }),
      });
      expect(addRes.status).toBe(201);

      const queueEvents = await fetch(`${baseUrl}/queues/${queueName}/events`);
      expect(queueEvents.status).toBe(200);

      const broadcastEvents = await fetch(`${baseUrl}/broadcast/${queueName}/events?subscription=shut-sub`);
      expect(broadcastEvents.status).toBe(200);

      const inspectBefore = await fetch(`${baseUrl}/flows/missing-id`);
      expect([200, 404]).toContain(inspectBefore.status);

      const concurrentLookups = await Promise.all([
        fetch(`${baseUrl}/flows/missing-a`),
        fetch(`${baseUrl}/flows/missing-b`),
      ]);
      for (const res of concurrentLookups) {
        expect([200, 404]).toContain(res.status);
      }

      await proxy.close();

      const lateJob = await fetch(`${baseUrl}/queues/${queueName}/jobs`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'late', data: {} }),
      });
      expect(lateJob.status).toBe(503);
      expect((await lateJob.json()).error).toContain('shutting down');

      const lateBroadcast = await fetch(`${baseUrl}/broadcast/${queueName}/events?subscription=shut-sub`);
      expect(lateBroadcast.status).toBe(503);

      const lateFlow = await fetch(`${baseUrl}/flows`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ flow: { name: 'late', queueName, data: {} } }),
      });
      expect(lateFlow.status).toBe(503);

      const lateFlowGet = await fetch(`${baseUrl}/flows/missing-id`);
      expect(lateFlowGet.status).toBe(503);

      const latePublish = await fetch(`${baseUrl}/broadcast/${queueName}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ subject: 'late', data: {} }),
      });
      expect(latePublish.status).toBe(503);

      await queueEvents.body?.cancel().catch(() => undefined);
      await broadcastEvents.body?.cancel().catch(() => undefined);
    } finally {
      await proxy.close().catch(() => undefined);
      await new Promise<void>((resolve, reject) => server.close((err) => (err ? reject(err) : resolve())));
    }
  }, 20000);
});
