import type { Router, Request, Response, NextFunction } from 'express';
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
export function createRoutes(
  opts: ProxyOptions,
  createRouter: () => Router,
): {
  router: Router;
  closeQueues: () => Promise<void>;
} {
  const router = createRouter();

  const queueCache = new Map<string, Queue>();
  const startTime = Date.now();
  const allowedQueues = opts.queues ? new Set(opts.queues) : null;
  let closed = false;

  function getQueue(name: string): Queue {
    if (closed) throw new Error('Proxy is shutting down');
    let q = queueCache.get(name);
    if (!q) {
      q = new Queue(name, {
        connection: opts.connection,
        client: opts.client,
        prefix: opts.prefix,
        compression: opts.compression,
      });
      q.on('error', () => {});
      queueCache.set(name, q);
    }
    return q;
  }

  function checkAllowlist(req: Request, res: Response): boolean {
    if (!allowedQueues) return true;
    const name = param(req, 'name');
    if (allowedQueues.has(name)) return true;
    res.status(403).json({ error: 'Queue is not in the allowlist' });
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
      const job = await queue.add(body.name, body.data ?? null, body.opts);

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
      res.status(500).json({ error: 'Internal server error' });
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
      const results = await Promise.all(
        jobs.map((j) => queue.add(j.name, j.data ?? null, j.opts)),
      );

      const responseJobs: (AddJobResponse | AddJobSkippedResponse)[] = results.map((job) =>
        job
          ? { id: job.id, name: job.name, timestamp: job.timestamp }
          : { skipped: true },
      );

      const anyCreated = results.some((j) => j !== null);
      res.status(anyCreated ? 201 : 200).json({ jobs: responseJobs });
    } catch (err) {
      res.status(500).json({ error: 'Internal server error' });
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
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.post('/queues/:name/pause', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;
      const queue = getQueue(param(req, 'name'));
      await queue.pause();
      res.status(204).send();
    } catch (err) {
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.post('/queues/:name/resume', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;
      const queue = getQueue(param(req, 'name'));
      await queue.resume();
      res.status(204).send();
    } catch (err) {
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  router.get('/queues/:name/counts', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;
      const queue = getQueue(param(req, 'name'));
      const counts = await queue.getJobCounts();
      res.status(200).json(counts);
    } catch (err) {
      res.status(500).json({ error: 'Internal server error' });
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
    closed = true;
    await Promise.allSettled([...queueCache.values()].map((q) => q.close()));
    queueCache.clear();
  }

  return { router, closeQueues };
}
