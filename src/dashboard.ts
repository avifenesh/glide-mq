import { readFileSync } from 'fs';
import { join } from 'path';
import type { Router, Request, Response } from 'express';
import type { Queue } from './queue';
import type { QueueEvents } from './queue-events';

let dashboardHtmlCache: string | null = null;
function getDashboardHtml(): string {
  if (dashboardHtmlCache) return dashboardHtmlCache;
  try {
    dashboardHtmlCache = readFileSync(join(__dirname, '..', 'src', 'dashboard-ui.html'), 'utf8');
  } catch {
    try {
      dashboardHtmlCache = readFileSync(join(__dirname, 'dashboard-ui.html'), 'utf8');
    } catch {
      dashboardHtmlCache = PLACEHOLDER_HTML;
    }
  }
  return dashboardHtmlCache;
}

export interface DashboardOptions {
  basePath?: string;
  /** QueueEvents instances to stream SSE events from. One per queue. */
  queueEvents?: QueueEvents[];
}

/** Extract a single string param from Express req.params (handles Express 4 and 5 types). */
function param(req: Request, key: string): string {
  const val = req.params[key];
  return Array.isArray(val) ? val[0] : val;
}

/**
 * Create an Express Router that serves a dashboard UI and REST API
 * for monitoring and managing glide-mq queues.
 */
export function createDashboard(
  queues: Queue[],
  opts?: DashboardOptions,
): Router {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const express = require('express') as typeof import('express');
  const router = express.Router();

  const queueMap = new Map<string, Queue>();
  for (const q of queues) {
    queueMap.set(q.name, q);
  }

  const queueEvents = opts?.queueEvents ?? [];

  // --- HTML dashboard placeholder ---
  router.get('/', (_req: Request, res: Response) => {
    res.type('html').send(getDashboardHtml());
  });

  // --- API: list all queues with job counts ---
  router.get('/api/queues', async (_req: Request, res: Response) => {
    try {
      const result = await Promise.all(
        queues.map(async (q) => {
          const counts = await q.getJobCounts();
          const paused = await q.isPaused();
          return { name: q.name, counts, paused };
        }),
      );
      res.json(result);
    } catch (err) {
      res.status(500).json({ error: String(err) });
    }
  });

  // --- API: get jobs for a queue by state ---
  router.get('/api/queues/:name/jobs', async (req: Request, res: Response) => {
    const queue = queueMap.get(param(req, 'name'));
    if (!queue) {
      res.status(404).json({ error: 'Queue not found' });
      return;
    }

    const state = (req.query.state as string) || 'waiting';
    const validStates = ['waiting', 'active', 'delayed', 'completed', 'failed'];
    if (!validStates.includes(state)) {
      res.status(400).json({ error: `Invalid state: ${state}. Must be one of: ${validStates.join(', ')}` });
      return;
    }

    const start = parseInt(req.query.start as string, 10) || 0;
    const end = parseInt(req.query.end as string, 10);
    const endVal = isNaN(end) ? 20 : end;

    try {
      const jobs = await queue.getJobs(
        state as 'waiting' | 'active' | 'delayed' | 'completed' | 'failed',
        start,
        endVal,
      );
      const serialized = jobs.map((j) => ({
        id: j.id,
        name: j.name,
        data: j.data,
        opts: j.opts,
        progress: j.progress,
        attemptsMade: j.attemptsMade,
        failedReason: j.failedReason,
        returnvalue: j.returnvalue,
        timestamp: j.timestamp,
        processedOn: j.processedOn,
        finishedOn: j.finishedOn,
      }));
      res.json(serialized);
    } catch (err) {
      res.status(500).json({ error: String(err) });
    }
  });

  // --- API: get single job ---
  router.get('/api/queues/:name/job/:id', async (req: Request, res: Response) => {
    const queue = queueMap.get(param(req, 'name'));
    if (!queue) {
      res.status(404).json({ error: 'Queue not found' });
      return;
    }

    const jobId = param(req, 'id');
    try {
      const job = await queue.getJob(jobId);
      if (!job) {
        res.status(404).json({ error: 'Job not found' });
        return;
      }
      const logs = await queue.getJobLogs(jobId);
      const state = await job.getState();
      res.json({
        id: job.id,
        name: job.name,
        data: job.data,
        opts: job.opts,
        progress: job.progress,
        attemptsMade: job.attemptsMade,
        failedReason: job.failedReason,
        returnvalue: job.returnvalue,
        timestamp: job.timestamp,
        processedOn: job.processedOn,
        finishedOn: job.finishedOn,
        state,
        logs: logs.logs,
      });
    } catch (err) {
      res.status(500).json({ error: String(err) });
    }
  });

  // --- API: pause queue ---
  router.post('/api/queues/:name/pause', async (req: Request, res: Response) => {
    const queue = queueMap.get(param(req, 'name'));
    if (!queue) {
      res.status(404).json({ error: 'Queue not found' });
      return;
    }
    try {
      await queue.pause();
      res.json({ status: 'paused' });
    } catch (err) {
      res.status(500).json({ error: String(err) });
    }
  });

  // --- API: resume queue ---
  router.post('/api/queues/:name/resume', async (req: Request, res: Response) => {
    const queue = queueMap.get(param(req, 'name'));
    if (!queue) {
      res.status(404).json({ error: 'Queue not found' });
      return;
    }
    try {
      await queue.resume();
      res.json({ status: 'resumed' });
    } catch (err) {
      res.status(500).json({ error: String(err) });
    }
  });

  // --- API: remove job ---
  router.delete('/api/queues/:name/jobs/:id', async (req: Request, res: Response) => {
    const queue = queueMap.get(param(req, 'name'));
    if (!queue) {
      res.status(404).json({ error: 'Queue not found' });
      return;
    }
    const jobId = param(req, 'id');
    try {
      const job = await queue.getJob(jobId);
      if (!job) {
        res.status(404).json({ error: 'Job not found' });
        return;
      }
      await job.remove();
      res.json({ status: 'removed' });
    } catch (err) {
      res.status(500).json({ error: String(err) });
    }
  });

  // --- API: retry job ---
  router.post('/api/queues/:name/jobs/:id/retry', async (req: Request, res: Response) => {
    const queue = queueMap.get(param(req, 'name'));
    if (!queue) {
      res.status(404).json({ error: 'Queue not found' });
      return;
    }
    const jobId = param(req, 'id');
    try {
      const job = await queue.getJob(jobId);
      if (!job) {
        res.status(404).json({ error: 'Job not found' });
        return;
      }
      await job.retry();
      res.json({ status: 'retried' });
    } catch (err) {
      res.status(500).json({ error: String(err) });
    }
  });

  // --- API: obliterate queue ---
  router.post('/api/queues/:name/obliterate', async (req: Request, res: Response) => {
    const queue = queueMap.get(param(req, 'name'));
    if (!queue) {
      res.status(404).json({ error: 'Queue not found' });
      return;
    }
    try {
      await queue.obliterate({ force: true });
      res.json({ status: 'obliterated' });
    } catch (err) {
      res.status(500).json({ error: String(err) });
    }
  });

  // --- SSE: stream events from all queues ---
  router.get('/api/events', (req: Request, res: Response) => {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      Connection: 'keep-alive',
    });
    res.write('\n');

    const listeners: { qe: QueueEvents; event: string; handler: (payload: unknown) => void }[] = [];
    const eventNames = ['completed', 'failed', 'progress', 'active', 'waiting', 'stalled', 'removed'];

    for (const qe of queueEvents) {
      for (const eventName of eventNames) {
        const handler = (payload: unknown) => {
          const data = JSON.stringify({ queue: qe.name, event: eventName, payload });
          res.write(`data: ${data}\n\n`);
        };
        qe.on(eventName, handler);
        listeners.push({ qe, event: eventName, handler });
      }
    }

    req.on('close', () => {
      for (const { qe, event, handler } of listeners) {
        qe.removeListener(event, handler);
      }
    });
  });

  return router;
}

const PLACEHOLDER_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>glide-mq Dashboard</title>
  <style>
    body { font-family: system-ui, sans-serif; display: flex; align-items: center; justify-content: center; min-height: 100vh; margin: 0; background: #f5f5f5; }
    .container { text-align: center; color: #333; }
  </style>
</head>
<body>
  <div class="container">
    <h1>glide-mq Dashboard</h1>
    <p>Dashboard UI loading...</p>
  </div>
</body>
</html>`;
