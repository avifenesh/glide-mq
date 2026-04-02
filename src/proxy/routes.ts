import type { Request, Response, Router } from 'express';
import { randomBytes } from 'crypto';
import { createBlockingClient, createClient } from '../connection';
import { Broadcast } from '../broadcast';
import { BroadcastWorker } from '../broadcast-worker';
import { FlowProducer, type JobNode } from '../flow-producer';
import { Job } from '../job';
import { Queue } from '../queue';
import type { BudgetOptions, Client, DAGFlow, FlowJob, JobTemplate, ScheduleOpts, WorkerInfo } from '../types';
import { buildKeys, compileSubjectMatcher, hashDataToRecord, validateJobId, validateQueueName } from '../utils';
import type { AddJobRequest, AddJobResponse, AddJobSkippedResponse, ProxyOptions } from './types';

const MAX_BULK_SIZE = 1000;
const SSE_BLOCK_MS = 5000;
const SSE_KEEPALIVE_MS = 15000;
type QueueState = 'waiting' | 'active' | 'delayed' | 'completed' | 'failed';
const VALID_QUEUE_STATES = new Set<QueueState>(['waiting', 'active', 'delayed', 'completed', 'failed']);
const VALID_METRIC_TYPES = new Set(['completed', 'failed']);

type BroadcastClient = {
  heartbeat: NodeJS.Timeout;
  matcher: ((subject: string) => boolean) | null;
  res: Response;
};

type SharedBroadcastStream = {
  clients: Set<BroadcastClient>;
  close: () => Promise<void>;
  closing: boolean;
  ready: Promise<void>;
  worker: BroadcastWorker<any, void>;
};

type FlowKind = 'tree' | 'dag';

type FlowJobRef = {
  jobId: string;
  queueName: string;
};

type FlowNodeSummary = ReturnType<typeof serializeJob> & {
  flowId: string;
  queueName: string;
  parentIds?: string[];
  parentQueues?: string[];
};

type FlowTreeNode = FlowNodeSummary & {
  children: FlowTreeNode[];
};

/** Extract a single string from a route param (Express 5 params can be string | string[]). */
function param(req: Request, key: string): string {
  const v = req.params[key];
  return Array.isArray(v) ? v[0] : v;
}

function queryValue(req: Request, key: string): string | undefined {
  const value = req.query[key];
  if (Array.isArray(value)) {
    return value.length > 0 && value[0] != null ? String(value[0]) : undefined;
  }
  return value != null ? String(value) : undefined;
}

function httpError(status: number, message: string): Error {
  const err = new Error(message);
  (err as any).status = status;
  return err;
}

function withStatus(err: unknown, status: number, fallbackMessage: string): Error {
  if (err instanceof Error) {
    (err as any).status = status;
    return err;
  }
  return httpError(status, fallbackMessage);
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
  'one of',
];

/**
 * Return true when an error is a known validation/input error that should map to 400.
 * Also checks for a .status property set by internal code (e.g. getQueue).
 */
function isValidationError(err: unknown): boolean {
  if (err && typeof err === 'object') {
    const status = (err as any).status;
    if (status === 400) return true;
    if (status === 503) return false;
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

function parseInteger(raw: string, label: string, opts?: { allowNegativeOne?: boolean; min?: number }): number {
  if (!/^-?\d+$/.test(raw)) {
    throw httpError(400, `${label} must be an integer`);
  }
  const value = Number(raw);
  if (!Number.isSafeInteger(value)) {
    throw httpError(400, `${label} must be a safe integer`);
  }
  if (opts?.allowNegativeOne && value === -1) {
    return value;
  }
  if (opts?.min != null && value < opts.min) {
    throw httpError(400, `${label} must be >= ${opts.min}`);
  }
  return value;
}

function parseBoolean(raw: string, label: string): boolean {
  if (raw === 'true' || raw === '1') return true;
  if (raw === 'false' || raw === '0') return false;
  throw httpError(400, `${label} must be true or false`);
}

function parseQueueState(req: Request): QueueState {
  const raw = queryValue(req, 'state');
  if (!raw) {
    throw httpError(400, 'Missing required query param: state');
  }
  if (!VALID_QUEUE_STATES.has(raw as QueueState)) {
    throw httpError(400, `state must be one of: ${Array.from(VALID_QUEUE_STATES).join(', ')}`);
  }
  return raw as QueueState;
}

function parseMetricType(req: Request): 'completed' | 'failed' {
  const raw = queryValue(req, 'type');
  if (!raw) {
    throw httpError(400, 'Missing required query param: type');
  }
  if (!VALID_METRIC_TYPES.has(raw)) {
    throw httpError(400, 'type must be one of: completed, failed');
  }
  return raw as 'completed' | 'failed';
}

function parseTerminalState(req: Request): 'completed' | 'failed' {
  const raw = queryValue(req, 'state');
  if (!raw) {
    throw httpError(400, 'Missing required query param: state');
  }
  if (!VALID_METRIC_TYPES.has(raw)) {
    throw httpError(400, 'state must be one of: completed, failed');
  }
  return raw as 'completed' | 'failed';
}

function parseCsvQuery(req: Request, key: string): string[] | undefined {
  const raw = queryValue(req, key);
  if (!raw) return undefined;
  const values = raw
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean);
  return values.length > 0 ? values : undefined;
}

function parseJsonString(value: string): unknown {
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

function decodeEventPayload(payload: Record<string, string>): Record<string, unknown> {
  const decoded: Record<string, unknown> = Object.create(null);
  for (const [field, value] of Object.entries(payload)) {
    if (field === 'returnvalue' || field === 'data') {
      decoded[field] = parseJsonString(value);
      continue;
    }
    if (field === 'delay' || field === 'attemptsMade') {
      const num = Number(value);
      decoded[field] = Number.isFinite(num) ? num : value;
      continue;
    }
    decoded[field] = value;
  }
  return decoded;
}

function writeSse(res: Response, data: unknown, opts?: { event?: string; id?: string }): void {
  let chunk = '';
  if (opts?.id) chunk += `id: ${opts.id}\n`;
  if (opts?.event) chunk += `event: ${opts.event}\n`;
  chunk += `data: ${JSON.stringify(data)}\n\n`;
  res.write(chunk);
}

function writeSseComment(res: Response, comment = 'keepalive'): void {
  res.write(`: ${comment}\n\n`);
}

function startSse(res: Response): void {
  res.writeHead(200, {
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive',
    'Content-Type': 'text/event-stream',
  });
  res.flushHeaders?.();
}

function serializeJob(job: Job<any, any>, state: string) {
  return {
    id: job.id,
    name: job.name,
    data: job.data,
    opts: job.opts as Record<string, unknown>,
    timestamp: job.timestamp,
    attemptsMade: job.attemptsMade,
    state,
    progress: job.progress,
    returnvalue: job.returnvalue,
    failedReason: job.failedReason,
    finishedOn: job.finishedOn,
    processedOn: job.processedOn,
    parentId: job.parentId,
    usage: job.usage,
  };
}

function serializeDeadLetterJob(job: Job<any, any>) {
  const envelope =
    job.data && typeof job.data === 'object' && !Array.isArray(job.data)
      ? (job.data as {
          attemptsMade?: unknown;
          data?: unknown;
          failedReason?: unknown;
          originalJobId?: unknown;
          originalQueue?: unknown;
        })
      : undefined;

  return {
    attemptsMade:
      typeof envelope?.attemptsMade === 'number'
        ? envelope.attemptsMade
        : typeof envelope?.attemptsMade === 'string'
          ? Number(envelope.attemptsMade)
          : job.attemptsMade,
    data: envelope?.data ?? job.data,
    failedReason: typeof envelope?.failedReason === 'string' ? envelope.failedReason : job.failedReason,
    id: job.id,
    name: job.name,
    originalJobId: typeof envelope?.originalJobId === 'string' ? envelope.originalJobId : undefined,
    originalQueue: typeof envelope?.originalQueue === 'string' ? envelope.originalQueue : undefined,
    timestamp: job.timestamp,
  };
}

function flowMetaKey(flowId: string, prefix = 'glide'): string {
  return `${prefix}:flow:${flowId}:meta`;
}

function flowJobsKey(flowId: string, prefix = 'glide'): string {
  return `${prefix}:flow:${flowId}:jobs`;
}

function flowRootsKey(flowId: string, prefix = 'glide'): string {
  return `${prefix}:flow:${flowId}:roots`;
}

function encodeFlowJobRef(ref: FlowJobRef): string {
  return `${ref.queueName}:${ref.jobId}`;
}

function decodeFlowJobRef(raw: string): FlowJobRef | null {
  const separator = raw.indexOf(':');
  if (separator <= 0 || separator === raw.length - 1) return null;
  return {
    jobId: raw.slice(separator + 1),
    queueName: raw.slice(0, separator),
  };
}

function collectFlowQueueNames(flow: FlowJob, acc: Set<string> = new Set()): Set<string> {
  acc.add(flow.queueName);
  for (const child of flow.children ?? []) {
    collectFlowQueueNames(child, acc);
  }
  return acc;
}

function collectDagQueueNames(dag: DAGFlow): Set<string> {
  const names = new Set<string>();
  for (const node of dag.nodes) {
    names.add(node.queueName);
  }
  return names;
}

function buildFlowTreeNodes(
  flowId: string,
  roots: FlowJobRef[],
  nodes: FlowNodeSummary[],
): FlowTreeNode[] {
  const nodeMap = new Map<string, FlowNodeSummary>();
  const childrenByParent = new Map<string, FlowNodeSummary[]>();

  for (const node of nodes) {
    nodeMap.set(encodeFlowJobRef({ jobId: node.id, queueName: node.queueName }), node);

    const parentRefs: FlowJobRef[] = [];
    if (node.parentIds && node.parentQueues && node.parentIds.length === node.parentQueues.length) {
      for (let i = 0; i < node.parentIds.length; i++) {
        parentRefs.push({ jobId: node.parentIds[i], queueName: node.parentQueues[i] });
      }
    } else if (node.parentId) {
      parentRefs.push({ jobId: node.parentId, queueName: node.queueName });
    }

    for (const parentRef of parentRefs) {
      const key = encodeFlowJobRef(parentRef);
      const siblings = childrenByParent.get(key);
      if (siblings) siblings.push(node);
      else childrenByParent.set(key, [node]);
    }
  }

  function visit(ref: FlowJobRef, path: Set<string>): FlowTreeNode {
    const key = encodeFlowJobRef(ref);
    const node = nodeMap.get(key);
    if (!node) {
      return {
        attemptsMade: 0,
        data: null,
        failedReason: undefined,
        finishedOn: undefined,
        flowId,
        id: ref.jobId,
        name: '',
        opts: {},
        parentId: undefined,
        parentIds: undefined,
        parentQueues: undefined,
        processedOn: undefined,
        progress: 0,
        queueName: ref.queueName,
        returnvalue: undefined,
        state: 'missing',
        timestamp: 0,
        usage: undefined,
        children: [],
      };
    }

    const children = (childrenByParent.get(key) ?? [])
      .slice()
      .sort((a, b) => a.timestamp - b.timestamp || a.queueName.localeCompare(b.queueName) || a.id.localeCompare(b.id))
      .map((child) => {
        const childKey = encodeFlowJobRef({ jobId: child.id, queueName: child.queueName });
        if (path.has(childKey)) {
          return { ...child, children: [] };
        }
        const nextPath = new Set(path);
        nextPath.add(childKey);
        return visit({ jobId: child.id, queueName: child.queueName }, nextPath);
      });

    return { ...node, children };
  }

  return roots
    .slice()
    .sort((a, b) => a.queueName.localeCompare(b.queueName) || a.jobId.localeCompare(b.jobId))
    .map((root) => visit(root, new Set([encodeFlowJobRef(root)])));
}

function validateJobOpts(
  optsIn: Record<string, unknown> | undefined,
  prefix: string,
  options?: { allowWaitTimeout?: boolean },
): string | null {
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

  if (optsIn.priority !== undefined && (optsIn.priority as number) > 2048) {
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

  if (optsIn.ttl !== undefined && (typeof optsIn.ttl !== 'number' || !Number.isFinite(optsIn.ttl) || optsIn.ttl <= 0)) {
    return `${prefix}opts.ttl must be a positive number`;
  }

  if (optsIn.lifo !== undefined && typeof optsIn.lifo !== 'boolean') {
    return `${prefix}opts.lifo must be a boolean`;
  }

  if (
    options?.allowWaitTimeout &&
    optsIn.waitTimeout !== undefined &&
    (typeof optsIn.waitTimeout !== 'number' || !Number.isFinite(optsIn.waitTimeout) || optsIn.waitTimeout <= 0)
  ) {
    return `${prefix}opts.waitTimeout must be a positive number`;
  }

  return null;
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
  const broadcastCache = new Map<string, Broadcast>();
  const broadcastInitMap = new Map<string, Promise<Broadcast>>();
  const broadcastStreams = new Map<string, SharedBroadcastStream>();
  const activeQueueEventClosers = new Set<() => void>();
  const startTime = Date.now();
  const allowedQueues = opts.queues ? new Set(opts.queues) : null;
  const flowPrefix = opts.prefix ?? 'glide';
  const errorHandler =
    opts.onError ??
    ((err: Error, queueName: string) => {
      console.error(`[glide-mq proxy] queue "${queueName}" error:`, err);
    });
  let draining = false;
  let closed = false;
  let sharedClient: Client | null = null;
  let sharedClientOwned = false;

  function requireConnection(feature: string) {
    if (!opts.connection) {
      throw httpError(500, `Proxy requires \`connection\` for ${feature}`);
    }
    return opts.connection;
  }

  async function getSharedClient(): Promise<Client> {
    if (sharedClient) return sharedClient;
    if (opts.client) {
      sharedClient = opts.client;
      sharedClientOwned = false;
      return sharedClient;
    }
    if (!opts.connection) {
      throw httpError(500, 'Proxy requires either `client` or `connection`');
    }
    sharedClient = await createClient(opts.connection);
    sharedClientOwned = true;
    return sharedClient;
  }

  function assertAllowedFlowQueues(queueNames: Iterable<string>): void {
    if (!allowedQueues) return;
    for (const queueName of queueNames) {
      if (!allowedQueues.has(queueName)) {
        throw httpError(403, 'Queue is not in the allowlist');
      }
    }
  }

  async function registerFlowRecord(
    flowId: string,
    kind: FlowKind,
    roots: FlowJobRef[],
    jobs: FlowJobRef[],
  ): Promise<void> {
    const client = await getSharedClient();
    const metaKey = flowMetaKey(flowId, flowPrefix);
    const jobsKey = flowJobsKey(flowId, flowPrefix);
    const rootsKey = flowRootsKey(flowId, flowPrefix);
    await client.hset(metaKey, {
      createdAt: Date.now().toString(),
      kind,
    });
    await client.del([jobsKey, rootsKey]);
    if (jobs.length > 0) {
      await client.sadd(
        jobsKey,
        jobs
          .slice()
          .sort((a, b) => a.queueName.localeCompare(b.queueName) || a.jobId.localeCompare(b.jobId))
          .map(encodeFlowJobRef),
      );
      for (const jobRef of jobs) {
        await client.hset(buildKeys(jobRef.queueName, opts.prefix).job(jobRef.jobId), { flowId });
      }
    }
    if (roots.length > 0) {
      await client.sadd(
        rootsKey,
        roots
          .slice()
          .sort((a, b) => a.queueName.localeCompare(b.queueName) || a.jobId.localeCompare(b.jobId))
          .map(encodeFlowJobRef),
      );
    }
  }

  async function loadFlowRecord(flowId: string): Promise<{
    createdAt: number;
    jobs: FlowJobRef[];
    kind: FlowKind;
    roots: FlowJobRef[];
  } | null> {
    const client = await getSharedClient();
    const metaRecord = hashDataToRecord(await client.hgetall(flowMetaKey(flowId, flowPrefix)));
    if (!metaRecord || !metaRecord.kind) return null;

    const jobsRaw = await client.smembers(flowJobsKey(flowId, flowPrefix));
    const rootsRaw = await client.smembers(flowRootsKey(flowId, flowPrefix));
    const jobs = Array.from(jobsRaw ?? [])
      .map((entry) => decodeFlowJobRef(String(entry)))
      .filter((entry): entry is FlowJobRef => entry !== null);
    const roots = Array.from(rootsRaw ?? [])
      .map((entry) => decodeFlowJobRef(String(entry)))
      .filter((entry): entry is FlowJobRef => entry !== null);

    return {
      createdAt: Number(metaRecord.createdAt || '0'),
      jobs,
      kind: metaRecord.kind === 'dag' ? 'dag' : 'tree',
      roots,
    };
  }

  async function deleteFlowRecord(flowId: string): Promise<void> {
    const client = await getSharedClient();
    await client.del([flowMetaKey(flowId, flowPrefix), flowJobsKey(flowId, flowPrefix), flowRootsKey(flowId, flowPrefix)]);
  }

  async function buildFlowSnapshot(flowId: string): Promise<{
    budget: unknown;
    counts: Record<string, number>;
    createdAt: number;
    flowId: string;
    kind: FlowKind;
    nodes: FlowNodeSummary[];
    roots: FlowJobRef[];
    tree: FlowTreeNode[];
    usage: unknown;
  } | null> {
    const record = await loadFlowRecord(flowId);
    if (!record) return null;
    assertAllowedFlowQueues(record.jobs.map((job) => job.queueName));

    const nodes: FlowNodeSummary[] = [];
    const counts: Record<string, number> = Object.create(null);

    for (const ref of record.jobs) {
      const queue = await getQueue(ref.queueName);
      const job = await queue.getJob(ref.jobId);
      if (!job) continue;
      const state = await job.getState();
      counts[state] = (counts[state] || 0) + 1;
      nodes.push({
        ...serializeJob(job, state),
        flowId,
        parentIds: job.parentIds,
        parentQueues: job.parentQueues,
        queueName: ref.queueName,
      });
    }

    let usage: unknown = null;
    let budget: unknown = null;
    if (record.roots.length === 1) {
      const [root] = record.roots;
      const rootQueue = await getQueue(root.queueName);
      try {
        usage = await rootQueue.getFlowUsage(root.jobId);
      } catch {
        usage = null;
      }
      try {
        budget = await rootQueue.getFlowBudget(root.jobId);
      } catch {
        budget = null;
      }
    }

    return {
      budget,
      counts,
      createdAt: record.createdAt,
      flowId,
      kind: record.kind,
      nodes: nodes.sort((a, b) => a.timestamp - b.timestamp || a.queueName.localeCompare(b.queueName) || a.id.localeCompare(b.id)),
      roots: record.roots.slice().sort((a, b) => a.queueName.localeCompare(b.queueName) || a.jobId.localeCompare(b.jobId)),
      tree: buildFlowTreeNodes(flowId, record.roots, nodes),
      usage,
    };
  }

  function getQueue(name: string): Promise<Queue> {
    if (draining || closed) {
      throw httpError(503, 'Proxy is shutting down');
    }
    try {
      validateQueueName(name);
    } catch (validationErr) {
      throw withStatus(validationErr, 400, 'Invalid queue name');
    }

    const cached = queueCache.get(name);
    if (cached) return Promise.resolve(cached);
    const pending = queueInitMap.get(name);
    if (pending) return pending;

    const init = (async () => {
      const queue = new Queue(name, {
        client: opts.client,
        compression: opts.compression,
        connection: opts.connection,
        prefix: opts.prefix,
      });
      queue.on('error', (err) => {
        errorHandler(err, name);
      });
      queueCache.set(name, queue);
      return queue;
    })();

    queueInitMap.set(name, init);
    init.finally(() => queueInitMap.delete(name));
    return init;
  }

  function getBroadcast(name: string): Promise<Broadcast> {
    if (draining || closed) {
      throw httpError(503, 'Proxy is shutting down');
    }
    try {
      validateQueueName(name);
    } catch (validationErr) {
      throw withStatus(validationErr, 400, 'Invalid queue name');
    }

    const cached = broadcastCache.get(name);
    if (cached) return Promise.resolve(cached);
    const pending = broadcastInitMap.get(name);
    if (pending) return pending;

    const init = (async () => {
      const broadcast = new Broadcast(name, {
        client: opts.client,
        compression: opts.compression,
        connection: opts.connection,
        prefix: opts.prefix,
      });
      broadcast.on('error', (err) => {
        errorHandler(err, name);
      });
      broadcastCache.set(name, broadcast);
      return broadcast;
    })();

    broadcastInitMap.set(name, init);
    init.finally(() => broadcastInitMap.delete(name));
    return init;
  }

  function checkAllowlist(req: Request, res: Response): boolean {
    if (!allowedQueues) return true;
    const name = param(req, 'name');
    if (name && allowedQueues.has(name)) return true;
    res.status(403).json({ error: 'Queue is not in the allowlist' });
    return false;
  }

  function removeBroadcastClient(stream: SharedBroadcastStream, client: BroadcastClient, endResponse = true): void {
    if (!stream.clients.delete(client)) return;
    clearInterval(client.heartbeat);
    if (endResponse) {
      try {
        client.res.end();
      } catch {
        /* ignore */
      }
    }
    if (stream.clients.size === 0) {
      void stream.close();
    }
  }

  async function getSharedBroadcastStream(name: string, subscription: string): Promise<SharedBroadcastStream> {
    const cacheKey = `${name}\u0000${subscription}`;
    const cached = broadcastStreams.get(cacheKey);
    if (cached) {
      await cached.ready;
      return cached;
    }

    const connection = requireConnection('broadcast SSE');
    const clients = new Set<BroadcastClient>();

    const stream: SharedBroadcastStream = {
      clients,
      closing: false,
      ready: Promise.resolve(),
      worker: null as unknown as BroadcastWorker<any, void>,
      close: async () => {
        if (stream.closing) return;
        stream.closing = true;
        broadcastStreams.delete(cacheKey);
        for (const client of Array.from(stream.clients)) {
          clearInterval(client.heartbeat);
          try {
            client.res.end();
          } catch {
            /* ignore */
          }
        }
        stream.clients.clear();
        await stream.worker.close();
      },
    };

    const worker = new BroadcastWorker<any, void>(
      name,
      async (job: Job<any, void>) => {
        const payload = {
          data: job.data,
          id: job.id,
          subject: job.name,
          timestamp: job.timestamp,
        };
        for (const client of Array.from(stream.clients)) {
          if (client.matcher && !client.matcher(job.name)) continue;
          try {
            writeSse(client.res, payload, { event: 'message', id: job.id });
          } catch {
            removeBroadcastClient(stream, client);
          }
        }
      },
      {
        blockTimeout: SSE_BLOCK_MS,
        connection,
        prefix: opts.prefix,
        subscription,
      },
    );

    worker.on('error', (err) => {
      errorHandler(err, name);
    });

    stream.worker = worker;
    stream.ready = worker.waitUntilReady();
    broadcastStreams.set(cacheKey, stream);

    try {
      await stream.ready;
      return stream;
    } catch (err) {
      broadcastStreams.delete(cacheKey);
      await worker.close().catch(() => undefined);
      throw err;
    }
  }

  router.post('/queues/:name/jobs', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;

      const body = req.body as AddJobRequest;
      if (!body || typeof body.name !== 'string' || body.name === '') {
        throw httpError(400, 'Missing required field: name');
      }

      const validationError = validateJobOpts(body.opts as Record<string, unknown> | undefined, '');
      if (validationError) {
        throw httpError(400, validationError);
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

      const body = req.body as { jobs?: AddJobRequest[] } | undefined;
      if (!body || !Array.isArray(body.jobs)) {
        throw httpError(400, 'Missing required field: jobs (array)');
      }

      const jobs = body.jobs;
      if (jobs.length > MAX_BULK_SIZE) {
        throw httpError(400, `Too many jobs (max ${MAX_BULK_SIZE})`);
      }
      for (let i = 0; i < jobs.length; i++) {
        if (!jobs[i] || typeof jobs[i].name !== 'string' || jobs[i].name === '') {
          throw httpError(400, `jobs[${i}]: missing required field: name`);
        }
        const jobValidationError = validateJobOpts(jobs[i].opts as Record<string, unknown> | undefined, `jobs[${i}]: `);
        if (jobValidationError) {
          throw httpError(400, jobValidationError);
        }
      }

      const queue = await getQueue(param(req, 'name'));
      const results = await Promise.all(jobs.map((job) => queue.add(job.name, job.data ?? null, job.opts)));

      const responseJobs: (AddJobResponse | AddJobSkippedResponse)[] = results.map((job) =>
        job ? { id: job.id, name: job.name, timestamp: job.timestamp } : { skipped: true },
      );

      const anyCreated = results.some((job) => job !== null);
      res.status(anyCreated ? 201 : 200).json({ jobs: responseJobs });
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.get('/queues/:name/jobs', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;
      const queue = await getQueue(param(req, 'name'));
      const state = parseQueueState(req);
      const startRaw = queryValue(req, 'start');
      const endRaw = queryValue(req, 'end');
      const excludeDataRaw = queryValue(req, 'excludeData');
      const start = startRaw !== undefined ? parseInteger(startRaw, 'start', { min: 0 }) : 0;
      const end = endRaw !== undefined ? parseInteger(endRaw, 'end', { allowNegativeOne: true, min: 0 }) : -1;
      const excludeData = excludeDataRaw !== undefined ? parseBoolean(excludeDataRaw, 'excludeData') : false;

      const jobs = await queue.getJobs(state, start, end, { excludeData });
      res.status(200).json({
        jobs: jobs.map((job) => serializeJob(job, state)),
      });
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.post('/queues/:name/jobs/wait', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;

      const body = req.body as AddJobRequest & { opts?: Record<string, unknown> };
      if (!body || typeof body.name !== 'string' || body.name === '') {
        throw httpError(400, 'Missing required field: name');
      }

      const validationError = validateJobOpts(body.opts, '', { allowWaitTimeout: true });
      if (validationError) {
        throw httpError(400, validationError);
      }

      const queue = await getQueue(param(req, 'name'));
      const result = await queue.addAndWait(body.name, body.data ?? null, body.opts as any);
      res.status(200).json({ result });
    } catch (err) {
      if (err instanceof Error) {
        if (err.message.includes('did not finish within')) {
          (err as any).status = 504;
        } else if (
          err.message.includes('cannot wait on a deduplicated') ||
          err.message.includes('removeOnComplete/removeOnFail') ||
          err.message.includes('waitTimeout')
        ) {
          (err as any).status = 400;
        } else if ((err as any).status == null) {
          (err as any).status = 500;
        }
      }
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
        throw httpError(404, 'Job not found');
      }

      res.status(200).json(serializeJob(job, await job.getState()));
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.get('/queues/:name/dlq', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;
      const queue = await getQueue(param(req, 'name'));
      const startRaw = queryValue(req, 'start');
      const endRaw = queryValue(req, 'end');
      const start = startRaw !== undefined ? parseInteger(startRaw, 'start', { min: 0 }) : 0;
      const end = endRaw !== undefined ? parseInteger(endRaw, 'end', { allowNegativeOne: true, min: 0 }) : -1;
      const jobs = await queue.getDeadLetterJobs(start, end);
      res.status(200).json({
        count: jobs.length,
        jobs: jobs.map((job) => serializeDeadLetterJob(job)),
      });
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.post('/queues/:name/dlq/replay-all', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;
      const body = req.body as { count?: unknown } | undefined;
      const countRaw = typeof body?.count === 'number' ? String(body.count) : queryValue(req, 'count');
      const count = countRaw !== undefined ? parseInteger(countRaw, 'count', { min: 1 }) : undefined;
      const queue = await getQueue(param(req, 'name'));
      const dlqJobs = await queue.getDeadLetterJobs(0, count !== undefined ? count - 1 : -1);
      const replayedJobs: Array<{ id: string; name: string; sourceId: string }> = [];

      for (const job of dlqJobs) {
        const replayed = await queue.replayDeadLetterJob(job.id);
        if (!replayed) continue;
        replayedJobs.push({ id: replayed.id, name: replayed.name, sourceId: job.id });
      }

      res.status(200).json({ jobs: replayedJobs, replayed: replayedJobs.length });
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.get('/queues/:name/dlq/:id', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;
      const queue = await getQueue(param(req, 'name'));
      const job = await queue.getDeadLetterJob(param(req, 'id'));
      if (!job) {
        throw httpError(404, 'DLQ job not found');
      }
      res.status(200).json(serializeDeadLetterJob(job));
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.post('/queues/:name/dlq/:id/replay', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;
      const queue = await getQueue(param(req, 'name'));
      const replayed = await queue.replayDeadLetterJob(param(req, 'id'));
      if (!replayed) {
        throw httpError(404, 'DLQ job not found');
      }
      res.status(200).json({ name: replayed.name, newJobId: replayed.id });
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.delete('/queues/:name/dlq/:id', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;
      const queue = await getQueue(param(req, 'name'));
      const removed = await queue.removeDeadLetterJob(param(req, 'id'));
      if (!removed) {
        throw httpError(404, 'DLQ job not found');
      }
      res.status(200).json({ removed: true });
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.post('/queues/:name/jobs/:id/priority', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;
      const body = req.body as { priority?: unknown } | undefined;
      if (!body || typeof body.priority !== 'number' || !Number.isFinite(body.priority) || body.priority < 0) {
        throw httpError(400, 'Missing required field: priority (non-negative number)');
      }

      const queue = await getQueue(param(req, 'name'));
      const job = await queue.getJob(param(req, 'id'));
      if (!job) {
        throw httpError(404, 'Job not found');
      }

      try {
        await job.changePriority(body.priority);
      } catch (err) {
        throw withStatus(err, 400, 'Cannot change priority');
      }

      res.status(200).json({ priority: body.priority, updated: true });
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.post('/queues/:name/jobs/:id/delay', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;
      const body = req.body as { delay?: unknown } | undefined;
      if (!body || typeof body.delay !== 'number' || !Number.isFinite(body.delay) || body.delay < 0) {
        throw httpError(400, 'Missing required field: delay (non-negative number)');
      }

      const queue = await getQueue(param(req, 'name'));
      const job = await queue.getJob(param(req, 'id'));
      if (!job) {
        throw httpError(404, 'Job not found');
      }

      try {
        await job.changeDelay(body.delay);
      } catch (err) {
        throw withStatus(err, 400, 'Cannot change delay');
      }

      res.status(200).json({ delay: body.delay, updated: true });
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.post('/queues/:name/jobs/:id/promote', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;

      const queue = await getQueue(param(req, 'name'));
      const job = await queue.getJob(param(req, 'id'));
      if (!job) {
        throw httpError(404, 'Job not found');
      }

      try {
        await job.promote();
      } catch (err) {
        throw withStatus(err, 400, 'Cannot promote job');
      }

      res.status(200).json({ promoted: true });
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.get('/queues/:name/jobs/:id/stream', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;

      const queue = await getQueue(param(req, 'name'));
      const jobId = param(req, 'id');

      startSse(res);

      let lastId = (req.headers['last-event-id'] as string) || queryValue(req, 'lastId') || undefined;
      let connectionClosed = false;

      req.on('close', () => {
        connectionClosed = true;
      });

      while (!connectionClosed) {
        const entries = await queue.readStream(jobId, { count: 100, lastId });
        for (const entry of entries) {
          writeSse(res, entry.fields, { id: entry.id });
          lastId = entry.id;
        }

        const job = await queue.getJob(jobId);
        if (!job) break;
        const state = await job.getState();
        if (state === 'completed' || state === 'failed') {
          const trailing = await queue.readStream(jobId, { count: 100, lastId });
          for (const entry of trailing) {
            writeSse(res, entry.fields, { id: entry.id });
          }
          break;
        }

        await new Promise<void>((resolve) => setTimeout(resolve, 500));
      }

      res.end();
    } catch (err) {
      if (!res.headersSent) {
        const { status, message } = errorResponse(err);
        res.status(status).json({ error: message });
      } else {
        res.end();
      }
    }
  });

  router.get('/queues/:name/jobs/:id/events', async (req: Request, res: Response) => {
    let closeConnection: (() => void) | undefined;
    try {
      if (!checkAllowlist(req, res)) return;
      const queue = await getQueue(param(req, 'name'));
      const jobId = param(req, 'id');
      const job = await queue.getJob(jobId);
      if (!job) {
        throw httpError(404, 'Job not found');
      }

      const connection = requireConnection('job events SSE');
      const keys = buildKeys(param(req, 'name'), opts.prefix);
      const client = await createBlockingClient(connection);
      let lastId = (req.headers['last-event-id'] as string) || queryValue(req, 'lastId') || '$';
      let connectionClosed = false;

      closeConnection = () => {
        if (connectionClosed) return;
        connectionClosed = true;
        try {
          client.close();
        } catch {
          /* ignore */
        }
        try {
          res.end();
        } catch {
          /* ignore */
        }
      };

      req.on('close', closeConnection);
      startSse(res);
      writeSseComment(res, 'connected');

      while (!connectionClosed) {
        let result;
        try {
          result = await client.xread({ [keys.events]: lastId }, { block: SSE_BLOCK_MS, count: 100 });
        } catch (err) {
          if (connectionClosed) break;
          throw err;
        }

        if (connectionClosed) break;
        if (!result) {
          const latestJob = await queue.getJob(jobId);
          if (!latestJob) break;
          const state = await latestJob.getState();
          if (state === 'completed' || state === 'failed') break;
          writeSseComment(res);
          continue;
        }

        let sawTerminalEvent = false;
        for (const streamEntry of result) {
          const entries = streamEntry.value;
          for (const [entryId, fieldPairs] of Object.entries(entries)) {
            if (!fieldPairs) continue;

            let eventType: string | undefined;
            const payload: Record<string, string> = Object.create(null);
            for (const [field, value] of fieldPairs) {
              const fieldStr = String(field);
              if (fieldStr === 'event') {
                eventType = String(value);
              } else {
                payload[fieldStr] = String(value);
              }
            }

            lastId = String(entryId);
            if (!eventType) continue;

            const decoded = decodeEventPayload(payload);
            if (String(decoded.jobId ?? '') !== jobId) continue;
            writeSse(res, decoded, { event: eventType, id: lastId });

            if (eventType === 'completed' || eventType === 'failed') {
              sawTerminalEvent = true;
              break;
            }
          }
          if (sawTerminalEvent) break;
        }

        if (sawTerminalEvent) break;
      }

      res.end();
    } catch (err) {
      closeConnection?.();
      if (!res.headersSent) {
        const { status, message } = errorResponse(err);
        res.status(status).json({ error: message });
      } else {
        res.end();
      }
    }
  });

  router.get('/queues/:name/events', async (req: Request, res: Response) => {
    let closeConnection: (() => void) | undefined;
    try {
      if (!checkAllowlist(req, res)) return;
      const connection = requireConnection('queue events SSE');
      const keys = buildKeys(param(req, 'name'), opts.prefix);
      const client = await createBlockingClient(connection);
      let lastId = (req.headers['last-event-id'] as string) || queryValue(req, 'lastId') || '$';
      let connectionClosed = false;

      closeConnection = () => {
        if (connectionClosed) return;
        connectionClosed = true;
        activeQueueEventClosers.delete(closeConnection!);
        try {
          client.close();
        } catch {
          /* ignore */
        }
        try {
          res.end();
        } catch {
          /* ignore */
        }
      };

      activeQueueEventClosers.add(closeConnection);
      req.on('close', closeConnection);

      startSse(res);
      writeSseComment(res, 'connected');

      while (!connectionClosed) {
        let result;
        try {
          result = await client.xread({ [keys.events]: lastId }, { block: SSE_BLOCK_MS, count: 100 });
        } catch (err) {
          if (connectionClosed) break;
          throw err;
        }

        if (connectionClosed) break;
        if (!result) {
          writeSseComment(res);
          continue;
        }

        for (const streamEntry of result) {
          const entries = streamEntry.value;
          for (const [entryId, fieldPairs] of Object.entries(entries)) {
            if (!fieldPairs) continue;

            let eventType: string | undefined;
            const payload: Record<string, string> = Object.create(null);
            for (const [field, value] of fieldPairs) {
              const fieldStr = String(field);
              if (fieldStr === 'event') {
                eventType = String(value);
              } else {
                payload[fieldStr] = String(value);
              }
            }

            if (!eventType) continue;
            lastId = String(entryId);
            writeSse(res, decodeEventPayload(payload), { event: eventType, id: lastId });
          }
        }
      }
    } catch (err) {
      closeConnection?.();
      if (!res.headersSent) {
        const { status, message } = errorResponse(err);
        res.status(status).json({ error: message });
      } else {
        res.end();
      }
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

  router.get('/queues/:name/metrics', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;
      const queue = await getQueue(param(req, 'name'));
      const type = parseMetricType(req);
      const startRaw = queryValue(req, 'start');
      const endRaw = queryValue(req, 'end');
      const start = startRaw !== undefined ? parseInteger(startRaw, 'start') : undefined;
      const end = endRaw !== undefined ? parseInteger(endRaw, 'end', { allowNegativeOne: true }) : undefined;
      const metrics = await queue.getMetrics(
        type,
        start !== undefined || end !== undefined ? { end, start } : undefined,
      );
      res.status(200).json(metrics);
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.get('/queues/:name/workers', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;
      const queue = await getQueue(param(req, 'name'));
      const workers: WorkerInfo[] = await queue.getWorkers();
      res.status(200).json({ workers });
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.get('/queues/:name/suspended', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;
      const queue = await getQueue(param(req, 'name'));
      const startRaw = queryValue(req, 'start');
      const endRaw = queryValue(req, 'end');
      const excludeDataRaw = queryValue(req, 'excludeData');
      const start = startRaw !== undefined ? parseInteger(startRaw, 'start', { min: 0 }) : 0;
      const end = endRaw !== undefined ? parseInteger(endRaw, 'end', { allowNegativeOne: true, min: 0 }) : -1;
      const excludeData = excludeDataRaw !== undefined ? parseBoolean(excludeDataRaw, 'excludeData') : false;
      const jobs = await queue.getSuspendedJobs(start, end, { excludeData });
      const serialized = await Promise.all(
        jobs.map(async (job) => ({
          ...serializeJob(job, 'suspended'),
          suspend: await queue.getSuspendInfo(job.id),
        })),
      );
      res.status(200).json({ jobs: serialized.filter((job) => job.suspend != null) });
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.post('/queues/:name/drain', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;
      const delayedRaw = queryValue(req, 'delayed');
      const delayed = delayedRaw !== undefined ? parseBoolean(delayedRaw, 'delayed') : false;
      const queue = await getQueue(param(req, 'name'));
      await queue.drain(delayed);
      res.status(200).json({ delayed, drained: true });
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.post('/queues/:name/retry', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;
      const body = req.body as { count?: unknown } | undefined;
      const countRaw = typeof body?.count === 'number' ? String(body.count) : queryValue(req, 'count');
      const count = countRaw !== undefined ? parseInteger(countRaw, 'count', { min: 0 }) : undefined;
      const queue = await getQueue(param(req, 'name'));
      const retried = await queue.retryJobs(count !== undefined ? { count } : undefined);
      res.status(200).json({ retried });
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.delete('/queues/:name/clean', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;
      const state = parseTerminalState(req);
      const ageRaw = queryValue(req, 'age');
      if (!ageRaw) {
        throw httpError(400, 'Missing required query param: age');
      }
      const ageSeconds = parseInteger(ageRaw, 'age', { min: 0 });
      const limitRaw = queryValue(req, 'limit');
      const limit = limitRaw !== undefined ? parseInteger(limitRaw, 'limit', { min: 1 }) : 1000;
      const queue = await getQueue(param(req, 'name'));
      const removed = await queue.clean(ageSeconds * 1000, limit, state);
      res.status(200).json({ removed });
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.get('/queues/:name/schedulers', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;
      const queue = await getQueue(param(req, 'name'));
      const schedulers = await queue.getRepeatableJobs();
      res.status(200).json({ schedulers });
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.get('/queues/:name/schedulers/:id', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;
      const queue = await getQueue(param(req, 'name'));
      const name = param(req, 'id');
      const entry = await queue.getJobScheduler(name);
      if (!entry) {
        throw httpError(404, 'Scheduler not found');
      }
      res.status(200).json({ entry, name });
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.put('/queues/:name/schedulers/:id', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;
      const body = req.body as { schedule?: ScheduleOpts; template?: JobTemplate } | undefined;
      if (!body || !body.schedule || typeof body.schedule !== 'object') {
        throw httpError(400, 'Missing required field: schedule');
      }
      const queue = await getQueue(param(req, 'name'));
      await queue.upsertJobScheduler(param(req, 'id'), body.schedule, body.template);
      res.status(200).json({ upserted: true });
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.delete('/queues/:name/schedulers/:id', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;
      const queue = await getQueue(param(req, 'name'));
      const name = param(req, 'id');
      const entry = await queue.getJobScheduler(name);
      if (!entry) {
        throw httpError(404, 'Scheduler not found');
      }
      await queue.removeJobScheduler(name);
      res.status(200).json({ removed: true });
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.get('/queues/:name/flows/:parentId/usage', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;
      const queue = await getQueue(param(req, 'name'));
      const parentId = param(req, 'parentId');
      const job = await queue.getJob(parentId);
      if (!job) {
        throw httpError(404, 'Job not found');
      }
      const usage = await queue.getFlowUsage(parentId);
      res.status(200).json(usage);
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.get('/queues/:name/flows/:flowId/budget', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;
      const queue = await getQueue(param(req, 'name'));
      const budget = await queue.getFlowBudget(param(req, 'flowId'));
      if (!budget) {
        throw httpError(404, 'Flow budget not found');
      }
      res.status(200).json(budget);
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.post('/flows', async (req: Request, res: Response) => {
    try {
      const body = req.body as
        | { budget?: BudgetOptions; dag?: DAGFlow; flow?: FlowJob }
        | undefined;

      if (!body || (!!body.flow && !!body.dag) || (!body.flow && !body.dag)) {
        throw httpError(400, 'Body must include exactly one of: flow, dag');
      }

      const producer = new FlowProducer({
        client: opts.client,
        connection: opts.connection,
        prefix: opts.prefix,
      });

      try {
        if (body.flow) {
          const queueNames = collectFlowQueueNames(body.flow);
          for (const queueName of queueNames) {
            validateQueueName(queueName);
          }
          assertAllowedFlowQueues(queueNames);

          const node = await producer.add(body.flow, body.budget ? { budget: body.budget } : undefined);
          const refs: FlowJobRef[] = [];

          const collectRefs = (flowDef: FlowJob, jobNode: JobNode) => {
            refs.push({ jobId: jobNode.job.id, queueName: flowDef.queueName });
            if (!flowDef.children || !jobNode.children) return;
            for (let i = 0; i < flowDef.children.length && i < jobNode.children.length; i++) {
              collectRefs(flowDef.children[i], jobNode.children[i]);
            }
          };

          collectRefs(body.flow, node);

          const root = { jobId: node.job.id, queueName: body.flow.queueName };
          const flowId = node.job.id;
          await registerFlowRecord(flowId, 'tree', [root], refs);

          res.status(201).json({
            flowId,
            kind: 'tree',
            nodeCount: refs.length,
            root,
            roots: [root],
          });
          return;
        }

        const dag = body.dag!;
        if (body.budget) {
          throw httpError(400, 'budget is currently supported only for tree flows');
        }
        const queueNames = collectDagQueueNames(dag);
        for (const queueName of queueNames) {
          validateQueueName(queueName);
        }
        assertAllowedFlowQueues(queueNames);

        const jobs = await producer.addDAG(dag);
        const flowId = randomBytes(12).toString('hex');
        const refs = dag.nodes.map((dagNode) => {
          const job = jobs.get(dagNode.name);
          if (!job) {
            throw httpError(500, `Missing DAG job for node ${dagNode.name}`);
          }
          return { jobId: job.id, queueName: dagNode.queueName };
        });
        const roots = dag.nodes
          .filter((dagNode) => !dagNode.deps || dagNode.deps.length === 0)
          .map((dagNode) => ({ jobId: jobs.get(dagNode.name)!.id, queueName: dagNode.queueName }));

        await registerFlowRecord(flowId, 'dag', roots, refs);
        res.status(201).json({
          flowId,
          jobs: dag.nodes.map((dagNode) => ({
            id: jobs.get(dagNode.name)!.id,
            name: dagNode.name,
            queueName: dagNode.queueName,
          })),
          kind: 'dag',
          nodeCount: refs.length,
          roots,
        });
      } finally {
        await producer.close().catch(() => undefined);
      }
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.get('/flows/:id', async (req: Request, res: Response) => {
    try {
      const snapshot = await buildFlowSnapshot(param(req, 'id'));
      if (!snapshot) {
        throw httpError(404, 'Flow not found');
      }

      res.status(200).json({
        budget: snapshot.budget,
        counts: snapshot.counts,
        createdAt: snapshot.createdAt,
        flowId: snapshot.flowId,
        kind: snapshot.kind,
        nodes: snapshot.nodes,
        roots: snapshot.roots,
        usage: snapshot.usage,
      });
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.get('/flows/:id/tree', async (req: Request, res: Response) => {
    try {
      const snapshot = await buildFlowSnapshot(param(req, 'id'));
      if (!snapshot) {
        throw httpError(404, 'Flow not found');
      }

      res.status(200).json({
        budget: snapshot.budget,
        counts: snapshot.counts,
        createdAt: snapshot.createdAt,
        flowId: snapshot.flowId,
        kind: snapshot.kind,
        roots: snapshot.roots,
        tree: snapshot.tree,
        usage: snapshot.usage,
      });
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.delete('/flows/:id', async (req: Request, res: Response) => {
    try {
      const flowId = param(req, 'id');
      const record = await loadFlowRecord(flowId);
      if (!record) {
        throw httpError(404, 'Flow not found');
      }
      assertAllowedFlowQueues(record.jobs.map((job) => job.queueName));

      let revoked = 0;
      let flagged = 0;
      let skipped = 0;
      const jobs: Array<{ id: string; queueName: string; state?: string; status: string }> = [];

      for (const ref of record.jobs) {
        const queue = await getQueue(ref.queueName);
        const job = await queue.getJob(ref.jobId);
        if (!job) {
          skipped += 1;
          jobs.push({ id: ref.jobId, queueName: ref.queueName, status: 'missing' });
          continue;
        }

        const state = await job.getState();
        if (state === 'completed' || state === 'failed') {
          skipped += 1;
          jobs.push({ id: ref.jobId, queueName: ref.queueName, state, status: 'skipped' });
          continue;
        }

        const status = await queue.revoke(ref.jobId);
        if (status === 'revoked') revoked += 1;
        else if (status === 'flagged') flagged += 1;
        else skipped += 1;
        jobs.push({ id: ref.jobId, queueName: ref.queueName, state, status });
      }

      await deleteFlowRecord(flowId);
      res.status(200).json({ flagged, flowId, jobs, revoked, skipped });
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.get('/usage/summary', async (req: Request, res: Response) => {
    try {
      const requestedQueues = parseCsvQuery(req, 'queues');
      if (requestedQueues) {
        for (const queueName of requestedQueues) {
          validateQueueName(queueName);
        }
      }

      let queues = requestedQueues;
      if (allowedQueues) {
        if (queues) {
          for (const queueName of queues) {
            if (!allowedQueues.has(queueName)) {
              throw httpError(403, 'Queue is not in the allowlist');
            }
          }
        } else {
          queues = Array.from(allowedQueues);
        }
      }

      const startRaw = queryValue(req, 'start');
      const endRaw = queryValue(req, 'end');
      const windowRaw = queryValue(req, 'window');
      const windowMsRaw = queryValue(req, 'windowMs');
      if (windowRaw !== undefined && windowMsRaw !== undefined && windowRaw !== windowMsRaw) {
        throw httpError(400, 'window and windowMs must match when both are provided');
      }
      const startTime = startRaw !== undefined ? parseInteger(startRaw, 'start', { min: 0 }) : undefined;
      const endTime = endRaw !== undefined ? parseInteger(endRaw, 'end', { min: 0 }) : undefined;
      const effectiveWindowRaw = windowMsRaw ?? windowRaw;
      const windowMs =
        effectiveWindowRaw !== undefined
          ? parseInteger(effectiveWindowRaw, windowMsRaw !== undefined ? 'windowMs' : 'window', { min: 1 })
          : undefined;

      const summary = await Queue.getUsageSummary({
        client: opts.client,
        connection: opts.connection,
        endTime,
        prefix: opts.prefix,
        queues,
        startTime,
        windowMs,
      });

      res.status(200).json(summary);
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.get('/queues/:name/jobs/:id/suspend', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;

      const queue = await getQueue(param(req, 'name'));
      const info = await queue.getSuspendInfo(param(req, 'id'));
      if (!info) {
        throw httpError(404, 'Job is not suspended');
      }

      res.status(200).json(info);
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.post('/queues/:name/jobs/:id/signal', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;

      const queue = await getQueue(param(req, 'name'));
      const jobId = param(req, 'id');
      const body = req.body as { data?: unknown; name?: unknown } | undefined;

      if (!body || !body.name || typeof body.name !== 'string') {
        throw httpError(400, 'Missing required field: name (string)');
      }

      const resumed = await queue.signal(jobId, body.name, body.data);
      res.status(200).json({ resumed });
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.post('/queues/:name/jobs/:id/revoke', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;

      const queue = await getQueue(param(req, 'name'));
      const status = await queue.revoke(param(req, 'id'));
      if (status === 'not_found') {
        throw httpError(404, 'Job not found');
      }

      res.status(200).json({ status });
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.get('/queues/:name/rate-limit', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;
      const queue = await getQueue(param(req, 'name'));
      const rateLimit = await queue.getGlobalRateLimit();
      res.status(200).json({ rateLimit });
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.put('/queues/:name/rate-limit', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;
      const body = req.body as { duration?: unknown; max?: unknown } | undefined;
      if (
        !body ||
        typeof body.max !== 'number' ||
        !Number.isFinite(body.max) ||
        body.max <= 0 ||
        typeof body.duration !== 'number' ||
        !Number.isFinite(body.duration) ||
        body.duration <= 0
      ) {
        throw httpError(400, 'Body must include positive numeric fields: max, duration');
      }

      const queue = await getQueue(param(req, 'name'));
      await queue.setGlobalRateLimit({ duration: body.duration, max: body.max });
      res.status(200).json({ rateLimit: { duration: body.duration, max: body.max } });
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.delete('/queues/:name/rate-limit', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;
      const queue = await getQueue(param(req, 'name'));
      await queue.removeGlobalRateLimit();
      res.status(200).json({ removed: true, rateLimit: null });
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.post('/broadcast/:name', async (req: Request, res: Response) => {
    try {
      if (!checkAllowlist(req, res)) return;
      const body = req.body as { data?: unknown; opts?: Record<string, unknown>; subject?: unknown } | undefined;
      if (!body || typeof body.subject !== 'string' || body.subject === '') {
        throw httpError(400, 'Missing required field: subject');
      }
      const validationError = validateJobOpts(body.opts, '');
      if (validationError) {
        throw httpError(400, validationError);
      }
      const broadcast = await getBroadcast(param(req, 'name'));
      const id = await broadcast.publish(body.subject, body.data ?? null, body.opts as any);
      if (!id) {
        res.status(200).json({ skipped: true });
        return;
      }
      res.status(201).json({ id, subject: body.subject });
    } catch (err) {
      const { status, message } = errorResponse(err);
      res.status(status).json({ error: message });
    }
  });

  router.get('/broadcast/:name/events', async (req: Request, res: Response) => {
    let cleanup: (() => void) | undefined;
    try {
      if (!checkAllowlist(req, res)) return;
      const subscription = queryValue(req, 'subscription');
      if (!subscription) {
        throw httpError(400, 'Missing required query param: subscription');
      }
      const matcher = compileSubjectMatcher(parseCsvQuery(req, 'subjects'));
      const stream = await getSharedBroadcastStream(param(req, 'name'), subscription);

      startSse(res);
      writeSseComment(res, 'connected');

      const client: BroadcastClient = {
        heartbeat: setInterval(() => {
          try {
            writeSseComment(res);
          } catch {
            removeBroadcastClient(stream, client);
          }
        }, SSE_KEEPALIVE_MS),
        matcher,
        res,
      };

      cleanup = () => {
        removeBroadcastClient(stream, client, false);
      };

      stream.clients.add(client);
      req.on('close', cleanup);
    } catch (err) {
      cleanup?.();
      if (!res.headersSent) {
        const { status, message } = errorResponse(err);
        res.status(status).json({ error: message });
      } else {
        res.end();
      }
    }
  });

  router.get('/health', (_req: Request, res: Response) => {
    res.status(200).json({
      queues: queueCache.size,
      status: 'ok',
      uptime: Date.now() - startTime,
    });
  });

  async function closeQueues(): Promise<void> {
    draining = true;

    for (const closeConnection of Array.from(activeQueueEventClosers)) {
      closeConnection();
    }

    await Promise.allSettled(Array.from(queueInitMap.values()));
    await Promise.allSettled(Array.from(broadcastInitMap.values()));
    await Promise.allSettled(Array.from(broadcastStreams.values()).map((stream) => stream.close()));
    await Promise.allSettled(Array.from(queueCache.values()).map((queue) => queue.close()));
    await Promise.allSettled(Array.from(broadcastCache.values()).map((broadcast) => broadcast.close()));
    if (sharedClientOwned && sharedClient) {
      try {
        sharedClient.close();
      } catch {
        /* ignore close errors on shutdown */
      }
    }

    queueCache.clear();
    broadcastCache.clear();
    broadcastStreams.clear();
    sharedClient = null;
    sharedClientOwned = false;
    closed = true;
  }

  return { router, closeQueues };
}
