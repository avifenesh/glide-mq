import { describe, it, expect, vi, beforeAll, afterAll } from 'vitest';
import express from 'express';
import http from 'http';
import { EventEmitter } from 'events';
import { createDashboard } from '../src/dashboard';
import type { Queue } from '../src/queue';
import type { QueueEvents } from '../src/queue-events';

// --- Mock helpers ---

function mockQueue(name: string, overrides: Partial<Queue> = {}): Queue {
  return {
    name,
    getJobCounts: vi.fn().mockResolvedValue({
      waiting: 5, active: 2, delayed: 1, completed: 10, failed: 3,
    }),
    isPaused: vi.fn().mockResolvedValue(false),
    getJobs: vi.fn().mockResolvedValue([
      {
        id: '1', name: 'test-job', data: { x: 1 }, opts: {},
        progress: 0, attemptsMade: 0, failedReason: undefined,
        returnvalue: undefined, timestamp: 1000, processedOn: undefined, finishedOn: undefined,
      },
    ]),
    getJob: vi.fn().mockResolvedValue({
      id: '1', name: 'test-job', data: { x: 1 }, opts: {},
      progress: 50, attemptsMade: 1, failedReason: undefined,
      returnvalue: 'done', timestamp: 1000, processedOn: 1001, finishedOn: 1002,
      getState: vi.fn().mockResolvedValue('completed'),
      remove: vi.fn().mockResolvedValue(undefined),
      retry: vi.fn().mockResolvedValue(undefined),
    }),
    getJobLogs: vi.fn().mockResolvedValue({ logs: ['log line 1', 'log line 2'], count: 2 }),
    pause: vi.fn().mockResolvedValue(undefined),
    resume: vi.fn().mockResolvedValue(undefined),
    obliterate: vi.fn().mockResolvedValue(undefined),
    ...overrides,
  } as unknown as Queue;
}

function mockQueueEvents(name: string): QueueEvents {
  const ee = new EventEmitter();
  (ee as any).name = name;
  return ee as unknown as QueueEvents;
}

// --- Test helpers ---

function request(
  server: http.Server,
  method: string,
  path: string,
): Promise<{ status: number; body: any; raw: string }> {
  return new Promise((resolve, reject) => {
    const url = new URL(path, `http://127.0.0.1:${(server.address() as any).port}`);
    const req = http.request(url, { method }, (res) => {
      let raw = '';
      res.on('data', (chunk) => { raw += chunk; });
      res.on('end', () => {
        let body: any;
        try { body = JSON.parse(raw); } catch { body = raw; }
        resolve({ status: res.statusCode!, body, raw });
      });
    });
    req.on('error', reject);
    req.end();
  });
}

describe('createDashboard', () => {
  let server: http.Server;
  let q1: Queue;
  let q2: Queue;
  let qe1: QueueEvents;

  beforeAll(async () => {
    q1 = mockQueue('orders');
    q2 = mockQueue('emails');
    qe1 = mockQueueEvents('orders');

    const app = express();
    const router = createDashboard([q1, q2], { queueEvents: [qe1] });
    app.use('/dash', router);

    await new Promise<void>((resolve) => {
      server = app.listen(0, '127.0.0.1', () => resolve());
    });
  });

  afterAll(async () => {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  });

  it('returns an express router', () => {
    const router = createDashboard([]);
    expect(router).toBeDefined();
    expect(typeof router).toBe('function');
  });

  it('GET / serves placeholder HTML', async () => {
    const res = await request(server, 'GET', '/dash/');
    expect(res.status).toBe(200);
    expect(res.raw).toContain('Dashboard UI loading...');
    expect(res.raw).toContain('glide-mq Dashboard');
  });

  it('GET /api/queues returns queue data', async () => {
    const res = await request(server, 'GET', '/dash/api/queues');
    expect(res.status).toBe(200);
    expect(Array.isArray(res.body)).toBe(true);
    expect(res.body).toHaveLength(2);
    expect(res.body[0].name).toBe('orders');
    expect(res.body[0].counts).toEqual({
      waiting: 5, active: 2, delayed: 1, completed: 10, failed: 3,
    });
    expect(res.body[0].paused).toBe(false);
    expect(res.body[1].name).toBe('emails');
  });

  it('GET /api/queues/:name/jobs returns jobs', async () => {
    const res = await request(server, 'GET', '/dash/api/queues/orders/jobs?state=completed&start=0&end=20');
    expect(res.status).toBe(200);
    expect(Array.isArray(res.body)).toBe(true);
    expect(res.body[0].id).toBe('1');
    expect(res.body[0].name).toBe('test-job');
    expect(q1.getJobs).toHaveBeenCalledWith('completed', 0, 20);
  });

  it('GET /api/queues/:name/jobs returns 404 for unknown queue', async () => {
    const res = await request(server, 'GET', '/dash/api/queues/unknown/jobs');
    expect(res.status).toBe(404);
    expect(res.body.error).toBe('Queue not found');
  });

  it('GET /api/queues/:name/jobs returns 400 for invalid state', async () => {
    const res = await request(server, 'GET', '/dash/api/queues/orders/jobs?state=bogus');
    expect(res.status).toBe(400);
    expect(res.body.error).toContain('Invalid state');
  });

  it('GET /api/queues/:name/job/:id returns single job with logs and state', async () => {
    const res = await request(server, 'GET', '/dash/api/queues/orders/job/1');
    expect(res.status).toBe(200);
    expect(res.body.id).toBe('1');
    expect(res.body.state).toBe('completed');
    expect(res.body.logs).toEqual(['log line 1', 'log line 2']);
    expect(res.body.progress).toBe(50);
    expect(res.body.returnvalue).toBe('done');
  });

  it('GET /api/queues/:name/job/:id returns 404 for unknown queue', async () => {
    const res = await request(server, 'GET', '/dash/api/queues/nope/job/1');
    expect(res.status).toBe(404);
  });

  it('GET /api/queues/:name/job/:id returns 404 for missing job', async () => {
    const qMissing = mockQueue('missing', { getJob: vi.fn().mockResolvedValue(null) });
    const app2 = express();
    app2.use(createDashboard([qMissing]));
    const srv2 = await new Promise<http.Server>((resolve) => {
      const s = app2.listen(0, '127.0.0.1', () => resolve(s));
    });
    try {
      const res = await request(srv2, 'GET', '/api/queues/missing/job/999');
      expect(res.status).toBe(404);
      expect(res.body.error).toBe('Job not found');
    } finally {
      srv2.close();
    }
  });

  it('POST /api/queues/:name/pause pauses the queue', async () => {
    const res = await request(server, 'POST', '/dash/api/queues/orders/pause');
    expect(res.status).toBe(200);
    expect(res.body.status).toBe('paused');
    expect(q1.pause).toHaveBeenCalled();
  });

  it('POST /api/queues/:name/resume resumes the queue', async () => {
    const res = await request(server, 'POST', '/dash/api/queues/orders/resume');
    expect(res.status).toBe(200);
    expect(res.body.status).toBe('resumed');
    expect(q1.resume).toHaveBeenCalled();
  });

  it('POST /api/queues/:name/obliterate obliterates the queue', async () => {
    const res = await request(server, 'POST', '/dash/api/queues/orders/obliterate');
    expect(res.status).toBe(200);
    expect(res.body.status).toBe('obliterated');
    expect(q1.obliterate).toHaveBeenCalledWith({ force: true });
  });

  it('DELETE /api/queues/:name/jobs/:id removes a job', async () => {
    const res = await request(server, 'DELETE', '/dash/api/queues/orders/jobs/1');
    expect(res.status).toBe(200);
    expect(res.body.status).toBe('removed');
  });

  it('POST /api/queues/:name/jobs/:id/retry retries a job', async () => {
    const res = await request(server, 'POST', '/dash/api/queues/orders/jobs/1/retry');
    expect(res.status).toBe(200);
    expect(res.body.status).toBe('retried');
  });

  it('SSE /api/events streams events from QueueEvents', async () => {
    const port = (server.address() as any).port;
    const received: string[] = [];

    await new Promise<void>((resolve, reject) => {
      const req = http.get(`http://127.0.0.1:${port}/dash/api/events`, (res) => {
        expect(res.statusCode).toBe(200);
        expect(res.headers['content-type']).toBe('text/event-stream');

        res.on('data', (chunk: Buffer) => {
          const line = chunk.toString();
          if (line.startsWith('data: ')) {
            received.push(line.trim().replace('data: ', ''));
          }
        });

        // Give time for event to arrive, then close
        setTimeout(() => {
          // Emit an event from the mock QueueEvents
          (qe1 as unknown as EventEmitter).emit('completed', { jobId: '42', returnvalue: '"ok"' });

          setTimeout(() => {
            res.destroy();
            resolve();
          }, 50);
        }, 50);
      });
      req.on('error', (err) => {
        // ECONNRESET from destroy is expected
        if ((err as any).code !== 'ECONNRESET') reject(err);
      });
    });

    expect(received.length).toBeGreaterThanOrEqual(1);
    const parsed = JSON.parse(received[0]);
    expect(parsed.queue).toBe('orders');
    expect(parsed.event).toBe('completed');
    expect(parsed.payload.jobId).toBe('42');
  });

  it('SSE cleans up listeners on disconnect', async () => {
    // Use a dedicated QueueEvents and server to avoid cross-test interference
    const freshQe = mockQueueEvents('fresh');
    const freshApp = express();
    freshApp.use(createDashboard([mockQueue('fresh')], { queueEvents: [freshQe] }));
    const freshServer = await new Promise<http.Server>((resolve) => {
      const s = freshApp.listen(0, '127.0.0.1', () => resolve(s));
    });
    const freshPort = (freshServer.address() as any).port;
    const ee = freshQe as unknown as EventEmitter;

    try {
      expect(ee.listenerCount('completed')).toBe(0);

      await new Promise<void>((resolve, reject) => {
        const req = http.get(`http://127.0.0.1:${freshPort}/api/events`, (res) => {
          setTimeout(() => {
            expect(ee.listenerCount('completed')).toBe(1);
            res.destroy();
            setTimeout(() => {
              expect(ee.listenerCount('completed')).toBe(0);
              resolve();
            }, 200);
          }, 100);
        });
        req.on('error', (err) => {
          if ((err as any).code !== 'ECONNRESET') reject(err);
        });
      });
    } finally {
      freshServer.close();
    }
  });

  it('pause/resume return 404 for unknown queue', async () => {
    const resPause = await request(server, 'POST', '/dash/api/queues/nope/pause');
    expect(resPause.status).toBe(404);
    const resResume = await request(server, 'POST', '/dash/api/queues/nope/resume');
    expect(resResume.status).toBe(404);
  });

  it('returns 500 when queue method throws', async () => {
    const errQueue = mockQueue('err-queue', {
      getJobCounts: vi.fn().mockRejectedValue(new Error('boom')),
      isPaused: vi.fn().mockRejectedValue(new Error('boom')),
    });
    const app2 = express();
    app2.use(createDashboard([errQueue]));
    const srv2 = await new Promise<http.Server>((resolve) => {
      const s = app2.listen(0, '127.0.0.1', () => resolve(s));
    });
    try {
      const res = await request(srv2, 'GET', '/api/queues');
      expect(res.status).toBe(500);
      expect(res.body.error).toContain('boom');
    } finally {
      srv2.close();
    }
  });
});
