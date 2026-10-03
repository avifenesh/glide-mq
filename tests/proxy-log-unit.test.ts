import { format } from 'node:util';
import type { Server } from 'node:http';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { Queue } from '../src/queue';
import { createProxyServer } from '../src/proxy';
import type { ProxyOptions } from '../src/proxy';

afterEach(() => vi.restoreAllMocks());

async function requestFailure(name: string, onError?: ProxyOptions['onError']) {
  let error: Error | undefined;
  vi.spyOn(Queue.prototype, 'add').mockImplementation(async (jobName) => {
    error = new Error(`Backend rejected ${jobName}`);
    throw error;
  });
  const proxy = createProxyServer({ client: {} as any, onError });
  let server: Server | undefined;
  try {
    const port = await new Promise<number>((resolve) => {
      server = proxy.app.listen(0, '127.0.0.1', () => {
        const address = server!.address();
        if (address && typeof address === 'object') resolve(address.port);
      });
    });
    const response = await fetch(`http://127.0.0.1:${port}/queues/reports/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name, data: {} }),
    });
    return { response, body: await response.json(), error };
  } finally {
    await proxy.close();
    if (server) await new Promise<void>((resolve) => server!.close(() => resolve()));
  }
}

describe('proxy default error logging', () => {
  it.each(['first\r\n[forged] admin', 'first\u2028[forged] admin\u001b[2J'])(
    'keeps request-derived error details inside one log entry',
    async (name) => {
      const log = vi.spyOn(console, 'error').mockImplementation(() => {});
      const { response, body } = await requestFailure(name);
      expect(response.status).toBe(500);
      expect(body).toEqual({ error: 'Internal server error' });
      expect(log).toHaveBeenCalledOnce();
      const rendered = format(...log.mock.calls[0]);
      expect(rendered).not.toMatch(/(?:^|[\r\n\u2028\u2029])\[forged\]|\u001b/);
      expect(rendered).toContain('Backend rejected');
      expect(rendered).toContain('reports');
    },
  );

  it('preserves ordinary error diagnostics in default logs', async () => {
    const log = vi.spyOn(console, 'error').mockImplementation(() => {});
    const { response } = await requestFailure('ordinary-job');
    expect(response.status).toBe(500);
    const rendered = format(...log.mock.calls[0]);
    expect(rendered).toContain('Backend rejected ordinary-job');
    expect(rendered).toContain('proxy-log-unit.test.ts');
  });

  it('passes the original Error to a custom error callback', async () => {
    const onError = vi.fn();
    const { error } = await requestFailure('custom\njob', onError);
    expect(onError).toHaveBeenCalledExactlyOnceWith(error, 'reports');
    expect(error!.message).toBe('Backend rejected custom\njob');
  });
});
