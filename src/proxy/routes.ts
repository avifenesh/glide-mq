import type { Router, Request, Response } from 'express';
import { Queue } from '../queue';
import type { ProxyOptions, AddJobRequest, AddJobResponse, AddJobSkippedResponse } from './types';
import { validateQueueName } from '../utils';

const MAX_BULK_SIZE = 1000;

/** Extract a single string from a route param (Express 5 params can be string | string[]). */
function param(req: Request, key: string): string {
  const v = req.params[key];
  return Array.isArray(v) ? v[0] : v;
}

const VALIDATION_KEYWORDS = [
  'invalid',
  'missing',
  'required',
  'must be',
  'too many',
  'exceeds',
  'out of bounds',
  'not contain',
  'non-empty',
];

/**
 * Return true when an error is a known validation/input error that should map to 400.
 * Also checks for a .status property set by internal code (e.g. getQueue).
 */
function isValidationError(err: unknown): boolean {
  if (err && typeof err === 'object') {
    const status = (err as any).status;
    if (status === 400) return true;
    if (status === 503) return false; // shutting down - let it 500
  }
  if (!(err instanceof Error)) return false;
  const msg = err.message.toLowerCase();
  return VALIDATION_KEYWORDS.some((kw) => msg.includes(kw));
}

/**
 * Resolve the HTTP status code and message for a caught error.
 * Returns the .status property if set, falls back to 400/500 heuristic.
 */
function errorResponse(err: unknown): { status: number; message: string } {
  if (err && typeof err === 'object') {
    const status = (err as any).status;
    if (typeof status === 'number' && status >= 400) {
      const message = err instanceof Error ? err.message : 'Request error';
      return { status, message };
    }
  }
  if (isValidationError(err)) {
    const message = err instanceof Error ? err.message : 'Bad request';
    return { status: 400, message };
  }
  return { status: 500, message: 'Internal server error' };
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
    if (closed) {
      const err = new Error('Proxy is shutting down');
      (err as any).status = 503;
      throw err;
    }
    try {
      validateQueueName(name);
    } catch (validationErr) {
      const err = validationErr instanceof Error ? validationErr : new Error(String(validationErr));
      (err as any).status = 400;
      throw err;
    }
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

      const optsIn = body.opts;
      if (optsIn) {
        if (optsIn.delay !== undefined && (typeof optsIn.delay !== 'number' || optsIn.delay < 0)) {
          res.status(400).json({ error: 'opts.delay must be a non-negative number' });
          return;
        }
        if (optsIn.priority !== undefined && (typeof optsIn.priority !== 'number' || optsIn.priority < 0)) {
          res.status(400).json({ error: 'opts.priority must be a non-negative number' });
          return;
        }
        if (optsIn.timeout !== undefined && (typeof optsIn.timeout !== 'number' || optsIn.timeout <= 0)) {
          res.status(400).json({ error: 'opts.timeout must be a positive number' });
          return;
        }
        if (optsIn.cost !== undefined && (typeof optsIn.cost !== 'number' || optsIn.cost <= 0)) {
          res.status(400).json({ error: 'opts.cost must be a positive number' });
          return;
        }
        if (optsIn.attempts !== undefined && (typeof optsIn.attempts !== 'number' || optsIn.attempts <= 0)) {
          res.status(400).json({ error: 'opts.attempts must be a positive number' });
          return;
        }
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
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
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
      if (jobs.length > MAX_BULK_SIZE) {
        res.status(400).json({ error: `Too many jobs (max ${MAX_BULK_SIZE})` });
        return;
      }
      for (let i = 0; i < jobs.length; i++) {
        if (!jobs[i] || typeof jobs[i].name !== 'string' || !jobs[i].name) {
          res.status(400).json({ error: `jobs[${i}]: missing required field: name` });
          return;
        }
      }

      const queue = getQueue(param(req, 'name'));
      const results = await Promise.all(jobs.map((j) => queue.add(j.name, j.data ?? null, j.opts)));

      const responseJobs: (AddJobResponse | AddJobSkippedResponse)[] = results.map((job) =>
        job ? { id: job.id, name: job.name, timestamp: job.timestamp } : { skipped: true },
      );

      const anyCreated = results.some((j) => j !== null);
      res.status(anyCreated ? 201 : 200).json({ jobs: responseJobs });
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
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
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.post('/queues/:name/pause', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;
      const queue = getQueue(param(req, 'name'));
      await queue.pause();
      res.status(204).send();
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.post('/queues/:name/resume', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;
      const queue = getQueue(param(req, 'name'));
      await queue.resume();
      res.status(204).send();
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.get('/queues/:name/counts', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;
      const queue = getQueue(param(req, 'name'));
      const counts = await queue.getJobCounts();
      res.status(200).json(counts);
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.get('/health', (_req: Request, res: Response) => {
    res.status(200).json({
      status: 'ok',
      uptime: Date.now() - startTime,
    });
  });

  async function closeQueues(): Promise<void> {
    closed = true;
    await Promise.allSettled([...queueCache.values()].map((q) => q.close()));
    queueCache.clear();
  }

  return { router, closeQueues };
}
