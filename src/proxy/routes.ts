import type { Router, Request, Response } from 'express';
import { Queue } from '../queue';
import type { ProxyOptions, AddJobRequest, AddJobResponse, AddJobSkippedResponse } from './types';
import { validateQueueName, validateJobId } from '../utils';

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
  const queueInitMap = new Map<string, Promise<Queue>>();
  const startTime = Date.now();
  const allowedQueues = opts.queues ? new Set(opts.queues) : null;
  const errorHandler =
    opts.onError ??
    ((err: Error, queueName: string) => {
      console.error(`[glide-mq proxy] queue "${queueName}" error:`, err);
    });
  let draining = false;
  let closed = false;

  function getQueue(name: string): Promise<Queue> {
    if (draining || closed) {
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
    const cached = queueCache.get(name);
    if (cached) return Promise.resolve(cached);
    const pending = queueInitMap.get(name);
    if (pending) return pending;
    const init = (async () => {
      const q = new Queue(name, {
        connection: opts.connection,
        client: opts.client,
        prefix: opts.prefix,
        compression: opts.compression,
      });
      q.on('error', (err) => {
        errorHandler(err, name);
      });
      queueCache.set(name, q);
      return q;
    })();
    queueInitMap.set(name, init);
    init.finally(() => queueInitMap.delete(name));
    return init;
  }

  function checkAllowlist(req: Request, res: Response): boolean {
    if (!allowedQueues) return true;
    const name = param(req, 'name');
    if (name && allowedQueues.has(name)) return true;
    res.status(403).json({ error: 'Queue is not in the allowlist' });
    return false;
  }

  /**
   * Validate job options shared by single-add and bulk-add endpoints.
   * Returns an error string if invalid, or null if valid.
   */
  function validateJobOpts(optsIn: AddJobRequest['opts'], prefix: string): string | null {
    if (!optsIn) return null;
    if (optsIn.jobId !== undefined) {
      if (typeof optsIn.jobId !== 'string' || optsIn.jobId === '') {
        return `${prefix}opts.jobId must be a non-empty string`;
      }
      try {
        validateJobId(optsIn.jobId);
      } catch (e) {
        return `${prefix}${e instanceof Error ? e.message : 'invalid jobId'}`;
      }
    }
    if (
      optsIn.delay !== undefined &&
      (typeof optsIn.delay !== 'number' || !Number.isFinite(optsIn.delay) || optsIn.delay < 0)
    ) {
      return `${prefix}opts.delay must be a non-negative number`;
    }
    if (
      optsIn.priority !== undefined &&
      (typeof optsIn.priority !== 'number' || !Number.isFinite(optsIn.priority) || optsIn.priority < 0)
    ) {
      return `${prefix}opts.priority must be a non-negative number`;
    }
    if (optsIn.priority !== undefined && optsIn.priority > 2048) {
      return `${prefix}opts.priority must not exceed 2048`;
    }
    if (
      optsIn.timeout !== undefined &&
      (typeof optsIn.timeout !== 'number' || !Number.isFinite(optsIn.timeout) || optsIn.timeout <= 0)
    ) {
      return `${prefix}opts.timeout must be a positive number`;
    }
    if (
      optsIn.cost !== undefined &&
      (typeof optsIn.cost !== 'number' || !Number.isFinite(optsIn.cost) || optsIn.cost <= 0)
    ) {
      return `${prefix}opts.cost must be a positive number`;
    }
    if (
      optsIn.attempts !== undefined &&
      (typeof optsIn.attempts !== 'number' || !Number.isFinite(optsIn.attempts) || optsIn.attempts <= 0)
    ) {
      return `${prefix}opts.attempts must be a positive number`;
    }
    if (
      optsIn.ttl !== undefined &&
      (typeof optsIn.ttl !== 'number' || !Number.isFinite(optsIn.ttl) || optsIn.ttl <= 0)
    ) {
      return `${prefix}opts.ttl must be a positive number`;
    }
    if (optsIn.lifo !== undefined && typeof optsIn.lifo !== 'boolean') {
      return `${prefix}opts.lifo must be a boolean`;
    }
    return null;
  }

  router.post('/queues/:name/jobs', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;

      const body = req.body as AddJobRequest;
      if (!body || typeof body.name !== 'string' || body.name === '') {
        res.status(400).json({ error: 'Missing required field: name' });
        return;
      }

      const validationError = validateJobOpts(body.opts, '');
      if (validationError) {
        res.status(400).json({ error: validationError });
        return;
      }

      const queue = await getQueue(param(req, 'name'));
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
        if (!jobs[i] || typeof jobs[i].name !== 'string' || jobs[i].name === '') {
          res.status(400).json({ error: `jobs[${i}]: missing required field: name` });
          return;
        }
        const jobValidationError = validateJobOpts(jobs[i].opts, `jobs[${i}]: `);
        if (jobValidationError) {
          res.status(400).json({ error: jobValidationError });
          return;
        }
      }

      const queue = await getQueue(param(req, 'name'));
      // ⚡ Bolt: Use native addBulk for O(1) network latency instead of Promise.all(add)
      const results = await queue.addBulk(
        jobs.map((j) => ({
          name: j.name,
          data: j.data ?? null,
          opts: j.opts,
        })),
      );

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

      const queue = await getQueue(param(req, 'name'));
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
      const queue = await getQueue(param(req, 'name'));
      await queue.pause();
      res.status(200).json({ paused: true });
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.post('/queues/:name/resume', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;
      const queue = await getQueue(param(req, 'name'));
      await queue.resume();
      res.status(200).json({ paused: false });
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.get('/queues/:name/counts', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;
      const queue = await getQueue(param(req, 'name'));
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
      queues: queueCache.size,
    });
  });

  async function closeQueues(): Promise<void> {
    draining = true;
    await Promise.allSettled([...queueInitMap.values()]);
    await Promise.allSettled([...queueCache.values()].map((q) => q.close()));
    queueCache.clear();
    closed = true;
  }

  return { router, closeQueues };
}
