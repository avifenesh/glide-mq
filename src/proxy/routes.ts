import type { Router, Request, Response } from 'express';
import { Queue } from '../queue';
import type { ProxyOptions, AddJobRequest, AddJobResponse, AddJobSkippedResponse } from './types';

/** Extract a single string from a route param (Express 5 params can be string | string[]). */
function param(req: Request, key: string): string {
  const v = req.params[key];
  return Array.isArray(v) ? v[0] : v;
}

/**
 * Create an Express Router with all proxy endpoints.
 *
 * The router manages a cache of Queue instances (one per queue name, lazily created).
 * Call the returned `closeQueues()` to shut down all cached Queue instances.
 */
export function createRoutes(opts: ProxyOptions): {
  router: Router;
  closeQueues: () => Promise<void>;
} {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const express = require('express') as typeof import('express');
  const router = express.Router();

  const queueCache = new Map<string, Queue>();
  const startTime = Date.now();
  const allowedQueues = opts.queues ? new Set(opts.queues) : null;

  function getQueue(name: string): Queue {
    let q = queueCache.get(name);
    if (!q) {
      q = new Queue(name, {
        connection: opts.connection,
        client: opts.client,
        prefix: opts.prefix,
        compression: opts.compression,
      });
      queueCache.set(name, q);
    }
    return q;
  }

  function checkAllowlist(req: Request, res: Response): boolean {
    if (!allowedQueues) return true;
    const name = param(req, 'name');
    if (allowedQueues.has(name)) return true;
    res.status(403).json({ error: `Queue "${name}" is not in the allowlist` });
    return false;
  }

  router.post('/queues/:name/jobs', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;

      const body = req.body as AddJobRequest;
      if (!body || typeof body.name !== 'string' || !body.name) {
        res.status(400).json({ error: 'Missing required field: name' });
        return;
      }

      const queue = getQueue(param(req, 'name'));
      const job = await queue.add(body.name, body.data, body.opts);

      if (!job) {
        const skipped: AddJobSkippedResponse = { skipped: true };
        res.status(200).json(skipped);
        return;
      }

      const response: AddJobResponse = {
        id: job.id,
        name: job.name,
        timestamp: job.timestamp,
      };
      res.status(201).json(response);
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Internal server error';
      res.status(500).json({ error: message });
    }
  });

  router.post('/queues/:name/jobs/bulk', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;

      const body = req.body;
      if (!body || !Array.isArray(body.jobs)) {
        res.status(400).json({ error: 'Missing required field: jobs (array)' });
        return;
      }

      const jobs = body.jobs as AddJobRequest[];
      for (let i = 0; i < jobs.length; i++) {
        if (!jobs[i] || typeof jobs[i].name !== 'string' || !jobs[i].name) {
          res.status(400).json({ error: `jobs[${i}]: missing required field: name` });
          return;
        }
      }

      const queue = getQueue(param(req, 'name'));
      const results = await queue.addBulk(
        jobs.map((j) => ({ name: j.name, data: j.data, opts: j.opts })),
      );

      // addBulk filters out skipped/duplicate jobs, so reconcile by index.
      let resultIdx = 0;
      const responseJobs: (AddJobResponse | AddJobSkippedResponse)[] = [];

      for (let i = 0; i < jobs.length; i++) {
        const result = results[resultIdx];
        if (result && resultIdx < results.length) {
          responseJobs.push({
            id: result.id,
            name: result.name,
            timestamp: result.timestamp,
          });
          resultIdx++;
        } else {
          responseJobs.push({ skipped: true });
        }
      }

      res.status(201).json({ jobs: responseJobs });
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Internal server error';
      res.status(500).json({ error: message });
    }
  });

  router.get('/queues/:name/jobs/:id', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;

      const queue = getQueue(param(req, 'name'));
      const job = await queue.getJob(param(req, 'id'));

      if (!job) {
        res.status(404).json({ error: 'Job not found' });
        return;
      }

      res.status(200).json({
        id: job.id,
        name: job.name,
        data: job.data,
        opts: job.opts,
        timestamp: job.timestamp,
        attemptsMade: job.attemptsMade,
        state: await job.getState(),
        progress: job.progress,
        returnvalue: job.returnvalue,
        failedReason: job.failedReason,
        finishedOn: job.finishedOn,
        processedOn: job.processedOn,
        parentId: job.parentId,
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Internal server error';
      res.status(500).json({ error: message });
    }
  });

  router.post('/queues/:name/pause', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;
      const queue = getQueue(param(req, 'name'));
      await queue.pause();
      res.status(204).send();
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Internal server error';
      res.status(500).json({ error: message });
    }
  });

  router.post('/queues/:name/resume', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;
      const queue = getQueue(param(req, 'name'));
      await queue.resume();
      res.status(204).send();
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Internal server error';
      res.status(500).json({ error: message });
    }
  });

  router.get('/queues/:name/counts', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;
      const queue = getQueue(param(req, 'name'));
      const counts = await queue.getJobCounts();
      res.status(200).json(counts);
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Internal server error';
      res.status(500).json({ error: message });
    }
  });

  router.get('/health', (_req: Request, res: Response) => {
    res.status(200).json({
      status: 'ok',
      uptime: Date.now() - startTime,
      queues: [...queueCache.keys()],
    });
  });

  async function closeQueues(): Promise<void> {
    const closers: Promise<void>[] = [];
    for (const q of queueCache.values()) {
      closers.push(q.close());
    }
    await Promise.all(closers);
    queueCache.clear();
  }

  return { router, closeQueues };
}
