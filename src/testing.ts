/**
 * In-memory testing mode for glide-mq.
 * TestQueue and TestWorker mimic the real API using plain Maps - no Valkey needed.
 *
 * Usage:
 *   import { TestQueue, TestWorker } from 'glide-mq/testing';
 *
 * See docs/TESTING.md "Known limitations" for the behaviour that is not mirrored.
 */

import { EventEmitter } from 'events';
import path from 'path';
import os from 'os';
import type {
  AddAndWaitOptions,
  JobOptions,
  JobUsage,
  UsageSummary,
  UsageSummaryOptions,
  UsageQueueSummary,
  JobCounts,
  Metrics,
  MetricsOptions,
  MetricsDataPoint,
  GetJobsOptions,
  DeadLetterQueueOptions,
  Processor,
  WorkerInfo,
  SchedulerEntry,
  ScheduleOpts,
  JobTemplate,
  Serializer,
  SignalEntry,
  SuspendOptions,
  JobIndexOptions,
  VectorSearchOptions,
} from './types';
import { JSON_SERIALIZER } from './types';
import { GlideMQError, UnrecoverableError, BatchError, SuspendError, DelayedError } from './errors';
import {
  MAX_JOB_DATA_SIZE,
  MAX_JOB_PRIORITY,
  calculateBackoff,
  computeFollowingSchedulerNextRun,
  computeInitialSchedulerNextRun,
  holdSchedulerModeSwitch,
  computeWeightedTotal,
  floorUsageBucket,
  normalizeScheduleDate,
  isPlainStepPayload,
  USAGE_BUCKET_MS,
  USAGE_RETENTION_MS,
  validateAndResolveUsage,
  validateSchedulerBounds,
  validateSchedulerEvery,
  validateSchedulerTemplate,
  validateTimezone,
  validateJobDataSize,
  validateJobOptions,
  validateJobPriority,
} from './utils';

const MAX_TIMEOUT_DELAY_MS = 2_147_483_647;
const DEFAULT_USAGE_WINDOW_MS = 60 * 60 * 1000;

function createUsageQueueSummary(): UsageQueueSummary {
  return {
    jobCount: 0,
    tokens: Object.create(null) as Record<string, number>,
    totalTokens: 0,
    costs: Object.create(null) as Record<string, number>,
    totalCost: 0,
    costUnit: undefined,
    models: Object.create(null) as Record<string, number>,
  };
}

function createUsageSummary(startTime: number, endTime: number): UsageSummary {
  return {
    startTime,
    endTime,
    bucketSizeMs: USAGE_BUCKET_MS,
    queues: [],
    jobCount: 0,
    tokens: Object.create(null) as Record<string, number>,
    totalTokens: 0,
    costs: Object.create(null) as Record<string, number>,
    totalCost: 0,
    costUnit: undefined,
    models: Object.create(null) as Record<string, number>,
    perQueue: Object.create(null) as Record<string, UsageQueueSummary>,
  };
}

function mergeUsage(target: UsageSummary | UsageQueueSummary, usage: JobUsage): void {
  target.jobCount += 1;
  target.totalTokens += Number.isFinite(usage.totalTokens) ? usage.totalTokens! : 0;
  target.totalCost += Number.isFinite(usage.totalCost) ? usage.totalCost! : 0;
  if (usage.costUnit && !target.costUnit) target.costUnit = usage.costUnit;
  if (usage.model) target.models[usage.model] = (target.models[usage.model] || 0) + 1;

  if (usage.tokens) {
    for (const [key, value] of Object.entries(usage.tokens)) {
      if (Number.isFinite(value)) target.tokens[key] = (target.tokens[key] || 0) + value;
    }
  }

  if (usage.costs) {
    for (const [key, value] of Object.entries(usage.costs)) {
      if (Number.isFinite(value)) target.costs[key] = (target.costs[key] || 0) + value;
    }
  }
}

function resolveUsageWindow(opts?: UsageSummaryOptions): { startTime: number; endTime: number } {
  const endTime = opts?.endTime ?? Date.now();
  if (!Number.isFinite(endTime) || endTime < 0) {
    throw new GlideMQError('endTime must be a finite non-negative number');
  }

  const windowMs = opts?.windowMs ?? DEFAULT_USAGE_WINDOW_MS;
  if (!Number.isFinite(windowMs) || windowMs <= 0) {
    throw new GlideMQError('windowMs must be a finite positive number');
  }

  const requestedStartTime = opts?.startTime ?? Math.max(0, endTime - windowMs);
  if (!Number.isFinite(requestedStartTime) || requestedStartTime < 0) {
    throw new GlideMQError('startTime must be a finite non-negative number');
  }
  if (requestedStartTime > endTime) {
    throw new GlideMQError('startTime must be less than or equal to endTime');
  }
  const earliestRetainedStart = Math.max(0, endTime - USAGE_RETENTION_MS);
  const startTime = Math.max(earliestRetainedStart, requestedStartTime);
  return { startTime, endTime };
}

// ---- Lightweight in-memory Job representation ----

export interface TestJobRecord<D = any, R = any> {
  id: string;
  name: string;
  data: D;
  opts: JobOptions;
  state: 'waiting' | 'prioritized' | 'active' | 'completed' | 'failed' | 'delayed' | 'suspended';
  attemptsMade: number;
  returnvalue: R | undefined;
  failedReason: string | undefined;
  timestamp: number;
  finishedOn: number | undefined;
  processedOn: number | undefined;
  expireAt?: number;
  /** Current position in the fallback chain. */
  fallbackIndex: number;
  /** @internal Per-job streaming channel chunks. */
  streamChunks?: { id: string; fields: Record<string, string> }[];
  /** @internal Counter for synthetic stream entry IDs. */
  streamCounter?: number;
  /** @internal Signals delivered to a suspended job. */
  signals?: SignalEntry[];
  /** @internal Reason for suspension. */
  suspendReason?: string;
  /** @internal When the job was suspended (epoch ms). */
  suspendedAt?: number;
  /** @internal Suspend timeout in ms. */
  suspendTimeout?: number;
  /** @internal Budget key for flow-level budget enforcement. */
  budgetKey?: string;
  /** @internal Usage metadata reported by the processor. */
  usage?: JobUsage;
  /** @internal Epoch ms when usage was last reported. */
  usageReportedAt?: number;
  /** @internal Stored vectors for vector search testing. */
  vectors?: Map<string, number[]>;
  /** @internal Last progress reported via updateProgress. */
  progress?: number | object;
  /** @internal Serializer of the owning queue, used by updateData. */
  serializer?: Serializer;
  /** @internal Owning queue, used by state-changing TestJob methods. */
  queue?: TestQueue<D, R>;
  /** @internal Epoch ms at which a delayed job is due (the time part of the scheduled score). */
  delayedUntil?: number;
  /** @internal Log lines appended by job.log(). */
  logs?: string[];
  /** @internal Name of the repeatAfterComplete scheduler that produced this job. */
  schedulerName?: string;
  /** @internal Set by TestQueue.revoke(). */
  revoked?: boolean;
  /** @internal Error passed to job.moveToFailed() while active, consumed by the worker. */
  movedToFailed?: Error;
}

/**
 * Minimal Job-like object returned by TestQueue methods and passed to processors.
 * Mirrors the public surface of the real Job class.
 */
export class TestJob<D = any, R = any> {
  readonly id: string;
  readonly name: string;
  data: D;
  readonly opts: JobOptions;
  attemptsMade: number;
  returnvalue: R | undefined;
  failedReason: string | undefined;
  progress: number | object;
  timestamp: number;
  finishedOn: number | undefined;
  processedOn: number | undefined;
  expireAt?: number;
  fallbackIndex: number = 0;
  usage?: JobUsage;
  tpmTokens?: number;
  signals: SignalEntry[] = [];
  budgetKey?: string;
  schedulerName?: string;
  /** @internal */ private _record: TestJobRecord<D, R>;

  constructor(record: TestJobRecord<D, R>) {
    this._record = record;
    this.id = record.id;
    this.name = record.name;
    this.data = record.data;
    this.opts = record.opts;
    this.attemptsMade = record.attemptsMade;
    this.returnvalue = record.returnvalue;
    this.failedReason = record.failedReason;
    this.progress = record.progress ?? 0;
    this.timestamp = record.timestamp;
    this.finishedOn = record.finishedOn;
    this.processedOn = record.processedOn;
    this.expireAt = record.expireAt;
    this.fallbackIndex = record.fallbackIndex;
    this.signals = record.signals ?? [];
    this.budgetKey = record.budgetKey;
    this.usage = record.usage;
    this.schedulerName = record.schedulerName;
  }

  get currentFallback(): { model: string; provider?: string; metadata?: Record<string, unknown> } | undefined {
    if (!this.opts.fallbacks || this.fallbackIndex === 0) return undefined;
    return this.opts.fallbacks[this.fallbackIndex - 1];
  }

  /** Append a log line, readable through TestQueue.getJobLogs(). */
  async log(message: string): Promise<void> {
    const byteLen = Buffer.byteLength(message, 'utf8');
    if (byteLen > MAX_JOB_DATA_SIZE) {
      throw new Error(`Log message exceeds maximum size (${byteLen} bytes > ${MAX_JOB_DATA_SIZE})`);
    }
    (this._record.logs ??= []).push(message);
  }

  async updateProgress(p: number | object): Promise<void> {
    const progressStr = typeof p === 'number' ? p.toString() : JSON.stringify(p);
    const byteLen = Buffer.byteLength(progressStr, 'utf8');
    if (byteLen > MAX_JOB_DATA_SIZE) {
      throw new Error(`Progress data exceeds maximum size (${byteLen} bytes > ${MAX_JOB_DATA_SIZE})`);
    }
    this._record.progress = typeof p === 'number' ? p : JSON.parse(progressStr);
    this.progress = p;
  }

  async updateData(data: D): Promise<void> {
    const serializer = this._record.serializer ?? JSON_SERIALIZER;
    const serialized = serializer.serialize(data);
    const byteLen = Buffer.byteLength(serialized, 'utf8');
    if (byteLen > MAX_JOB_DATA_SIZE) {
      throw new Error(`Job data exceeds maximum size (${byteLen} bytes > ${MAX_JOB_DATA_SIZE})`);
    }
    this._record.data = serializer.deserialize(serialized) as D;
    this.data = data;
  }

  discarded = false;

  /**
   * Mirror glidemq_changePriority: a waiting job given a priority moves to
   * prioritized, a prioritized job given 0 returns to waiting, a delayed job
   * keeps its place with the new priority, other states throw.
   */
  async changePriority(newPriority: number): Promise<void> {
    if (newPriority < 0) {
      throw new Error('Priority must be >= 0');
    }
    if (!Number.isInteger(newPriority) || newPriority > MAX_JOB_PRIORITY) {
      throw new Error('Cannot change priority: invalid_priority');
    }
    const queue = this.owningQueue();
    const record = queue.jobs.get(this.id);
    if (!record) throw new Error('Cannot change priority: not_found');
    if (record.state === 'waiting') {
      if (newPriority === 0 && (record.opts.priority ?? 0) === 0) return;
      if (newPriority > 0) record.state = 'prioritized';
    } else if (record.state === 'prioritized') {
      if (newPriority === 0) record.state = 'waiting';
    } else if (record.state !== 'delayed') {
      throw new Error('Cannot change priority: invalid_state');
    }
    this.opts.priority = newPriority;
    queue.emit('priority-changed', this.id, newPriority);
  }

  /**
   * Mirror glidemq_changeDelay: a delayed job is rescheduled (or released when
   * the delay becomes 0), a waiting job is parked in delayed, other states throw.
   */
  async changeDelay(newDelay: number): Promise<void> {
    if (newDelay < 0) {
      throw new Error('Delay must be >= 0');
    }
    const queue = this.owningQueue();
    const record = queue.jobs.get(this.id);
    if (!record) throw new Error('Cannot change delay: not_found');
    if (record.state === 'delayed') {
      if (newDelay === 0) {
        // The job stays parked as prioritized when it has a priority, like the ZADD XX path.
        queue.releaseDelayed(record, (record.opts.priority ?? 0) > 0);
      } else {
        queue.parkDelayed(record, newDelay);
      }
    } else if (record.state === 'waiting' || record.state === 'prioritized') {
      if (newDelay === 0) return;
      queue.parkDelayed(record, newDelay);
    } else {
      throw new Error('Cannot change delay: invalid_state');
    }
    this.opts.delay = newDelay;
    queue.emit('delay-changed', this.id, newDelay);
  }

  /** Mirror glidemq_promoteJob: only a delayed job can be promoted. */
  async promote(): Promise<void> {
    const queue = this.owningQueue();
    const record = queue.jobs.get(this.id);
    if (!record) throw new Error('Cannot promote: not_found');
    if (record.state !== 'delayed') throw new Error('Cannot promote: not_delayed');
    queue.releaseDelayed(record, false);
    this.opts.delay = 0;
  }

  /** Read the current state, 'unknown' once the job has been removed (like Job.getState). */
  async getState(): Promise<string> {
    const record = this._record.queue?.jobs.get(this.id);
    return record ? record.state : 'unknown';
  }

  async isCompleted(): Promise<boolean> {
    return (await this.getState()) === 'completed';
  }

  async isFailed(): Promise<boolean> {
    return (await this.getState()) === 'failed';
  }

  async isDelayed(): Promise<boolean> {
    return (await this.getState()) === 'delayed';
  }

  async isActive(): Promise<boolean> {
    return (await this.getState()) === 'active';
  }

  async isWaiting(): Promise<boolean> {
    return (await this.getState()) === 'waiting';
  }

  async isRevoked(): Promise<boolean> {
    return this._record.queue?.jobs.get(this.id)?.revoked === true;
  }

  /** Poll until completed or failed, like Job.waitUntilFinished. */
  async waitUntilFinished(pollIntervalMs = 500, timeoutMs = 30000): Promise<'completed' | 'failed'> {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const state = await this.getState();
      if (state === 'completed' || state === 'failed') {
        return state;
      }
      await new Promise<void>((r) => setTimeout(r, pollIntervalMs));
    }
    throw new Error(`Job ${this.id} did not finish within ${timeoutMs}ms`);
  }

  /** Retry this failed job, like glidemq_retryJob: only a failed job can be retried. */
  async retry(): Promise<void> {
    const queue = this.owningQueue();
    const record = queue.jobs.get(this.id);
    if (!record) throw new Error('Cannot retry: not_found');
    if (record.state !== 'failed') throw new Error('Cannot retry: not_failed');
    queue.retryRecord(record, Date.now());
    this.attemptsMade = 0;
    this.failedReason = undefined;
    this.finishedOn = undefined;
  }

  /**
   * Fail the active job from inside the processor, like Job.moveToFailed. The
   * worker then applies the attempts/backoff rules instead of completing the job.
   */
  async moveToFailed(err: Error): Promise<void> {
    const record = this.owningQueue().jobs.get(this.id);
    if (!record || record.state !== 'active') {
      throw new GlideMQError('moveToFailed can only be called while job is active in a Worker');
    }
    record.movedToFailed = err;
    this.failedReason = err.message;
  }

  /** Remove this job from the queue, like Job.remove(). */
  async remove(): Promise<void> {
    this.owningQueue().removeJob(this.id);
  }

  private owningQueue(): TestQueue<D, R> {
    const queue = this._record.queue;
    if (!queue) throw new Error('TestJob is not attached to a TestQueue');
    return queue;
  }

  discard(): void {
    this.discarded = true;
  }

  /**
   * Pause an active job and resume it after the given UNIX timestamp in ms,
   * like Job.moveToDelayed. Optionally sets `job.data.step` first.
   * Must be called from inside a TestWorker processor.
   */
  async moveToDelayed(timestamp: number, nextStep?: string): Promise<never> {
    if (!Number.isFinite(timestamp) || timestamp < 0) {
      throw new Error('Timestamp must be a finite Unix millisecond value >= 0');
    }
    if (this._record.state !== 'active') {
      throw new Error('moveToDelayed() can only be used while the job is active in a Worker');
    }
    const delayedUntil = Math.trunc(timestamp);
    if (nextStep !== undefined) {
      if (!isPlainStepPayload(this.data)) {
        throw new Error('moveToDelayed(nextStep) requires plain-object job data');
      }
      await this.updateData({ ...this.data, step: nextStep } as D);
    }
    throw new DelayedError(delayedUntil);
  }

  async reportUsage(usage: JobUsage): Promise<void> {
    const resolved = validateAndResolveUsage(usage);
    this.usage = resolved;
    this._record.usage = resolved;
    this._record.usageReportedAt = Date.now();
  }

  async reportTokens(count: number): Promise<void> {
    if (count < 0) throw new Error('Token count must not be negative');
    this.tpmTokens = count;
  }

  async stream(chunk: Record<string, string>): Promise<string> {
    if (Object.keys(chunk).length === 0) {
      throw new Error('Stream chunk must not be empty');
    }
    let totalBytes = 0;
    for (const key of Object.keys(chunk)) {
      totalBytes += Buffer.byteLength(key, 'utf8') + Buffer.byteLength(chunk[key], 'utf8');
    }
    if (totalBytes > MAX_JOB_DATA_SIZE) {
      throw new Error(`Stream chunk exceeds maximum size (${totalBytes} bytes > ${MAX_JOB_DATA_SIZE})`);
    }
    if (!this._record.streamChunks) this._record.streamChunks = [];
    if (this._record.streamCounter === undefined) this._record.streamCounter = 0;
    const id = `test-${++this._record.streamCounter}`;
    this._record.streamChunks.push({ id, fields: { ...chunk } });
    return id;
  }

  /**
   * Convenience method for streaming typed LLM chunks.
   * Wraps `stream()` with `{ type, content }` fields.
   */
  async streamChunk(type: string, content?: string): Promise<string> {
    const chunk: Record<string, string> = { type };
    if (content !== undefined) chunk.content = content;
    return this.stream(chunk);
  }

  /**
   * Suspend this job. Marks the record as suspended and throws SuspendError.
   */
  async suspend(opts?: SuspendOptions): Promise<never> {
    this._record.state = 'suspended';
    this._record.suspendedAt = Date.now();
    this._record.suspendReason = opts?.reason;
    this._record.suspendTimeout = opts?.timeout;
    if (!this._record.signals) this._record.signals = [];
    throw new SuspendError();
  }

  /**
   * Store a vector embedding on this job (in-memory for testing mode).
   */
  async storeVector(field: string, embedding: number[] | Float32Array): Promise<void> {
    if (!this._record.vectors) this._record.vectors = new Map();
    const arr = embedding instanceof Float32Array ? Array.from(embedding) : embedding;
    this._record.vectors.set(field, arr);
  }
}

// ---- Search options ----

export interface SearchJobsOptions {
  name?: string;
  data?: Record<string, unknown>;
  state?: TestJobRecord['state'];
  /** When true, excludes `data` and `returnvalue` fields from returned jobs. */
  excludeData?: boolean;
}

/**
 * Match a record state against a queried state. 'delayed' reads the scheduled
 * ZSet in production, which also holds prioritized jobs.
 */
function matchesQueueState(state: TestJobRecord['state'], queried: TestJobRecord['state']): boolean {
  return state === queried || (queried === 'delayed' && state === 'prioritized');
}

/** Check if all key-value pairs in filter exist in data (shallow match). */
function matchesData(data: Record<string, unknown>, filter: Record<string, unknown>): boolean {
  for (const [key, value] of Object.entries(filter)) {
    if (data[key] !== value) return false;
  }
  return true;
}

/** Compute cosine similarity between two vectors. Returns value in [-1, 1]. */
function cosineSimilarity(a: number[], b: number[]): number {
  let dot = 0;
  let normA = 0;
  let normB = 0;
  for (let i = 0; i < a.length; i++) {
    dot += a[i] * b[i];
    normA += a[i] * a[i];
    normB += b[i] * b[i];
  }
  const denom = Math.sqrt(normA) * Math.sqrt(normB);
  if (denom === 0) return 0;
  return dot / denom;
}

// ---- TestQueue ----

export interface TestQueueOptions {
  /**
   * Deduplication is always applied when a job sets `deduplication`, like the real Queue.
   * Set to `false` to ignore `deduplication` options. `true` is accepted for backward compatibility.
   */
  dedup?: boolean;
  /** Custom serializer for job data and return values. When provided, values are roundtripped through serialize/deserialize to match production behavior. */
  serializer?: Serializer;
  /**
   * Names the TestQueue that `getDeadLetterJobs()` and the other dead-letter methods read, like
   * QueueOptions.deadLetterQueue. It routes nothing: a TestWorker `deadLetterQueue` option does the copying.
   */
  deadLetterQueue?: DeadLetterQueueOptions;
}

/** Shape of the job data a worker writes into the dead-letter queue, like BaseWorker.moveToDLQ. */
interface DeadLetterEnvelope {
  originalQueue: string;
  originalJobId: string;
  data: unknown;
  failedReason: string;
  attemptsMade: number;
}

/** Budget state stored in-memory for testing mode. */
interface TestBudgetState {
  maxTotalTokens?: number;
  maxTokens?: Record<string, number>;
  tokenWeights?: Record<string, number>;
  maxTotalCost?: number;
  maxCosts?: Record<string, number>;
  costUnit?: string;
  usedTokens: number;
  usedCost: number;
  usedTokensByCategory: Record<string, number>;
  usedCostsByCategory: Record<string, number>;
  exceeded: boolean;
  onExceeded: 'pause' | 'fail';
}

/**
 * In-memory test double for Queue. Suitable for unit tests without Valkey.
 *
 * Known limitations vs real Queue (see docs/TESTING.md):
 * - Ordering keys / concurrency groups are accepted but not enforced
 * - No global concurrency or queue-wide rate limit (use the TestWorker limiter option)
 * - No DAG / parent-child flows
 * - No sandbox ESM processors
 */
export class TestQueue<D = any, R = any> extends EventEmitter {
  private static registry: Map<string, TestQueue<any, any>> = new Map();
  readonly name: string;
  /** @internal */ readonly jobs: Map<string, TestJobRecord<D, R>> = new Map();
  /** @internal dedup id -> job id and add timestamp, like the glidemq dedup hash. */
  readonly dedupEntries: Map<string, { jobId: string; timestamp: number }> = new Map();
  /** @internal */ readonly waitingQueue: TestJobRecord<D, R>[] = [];
  /** @internal */ readonly budgets: Map<string, TestBudgetState> = new Map();
  private idCounter = 0;
  private paused = false;
  private opts: TestQueueOptions;
  /** @internal DLQ name recorded by a worker, like the deadLetterQueueName meta field. */
  deadLetterQueueName: string | undefined;
  /** @internal */ readonly serializer: Serializer;
  /** @internal */ private indexConfig: JobIndexOptions | null = null;

  /** @internal */ readonly metricsData: Map<string, Map<number, { count: number; totalDuration: number }>> = new Map([
    ['completed', new Map()],
    ['failed', new Map()],
  ]);

  /** Workers register themselves here so we can notify on add. */
  /** @internal */ readonly workers: Set<TestWorker<D, R>> = new Set();
  private schedulers: Map<string, SchedulerEntry> = new Map();
  private schedulerTimer: ReturnType<typeof setTimeout> | null = null;
  private schedulerRunning = false;
  private nextSchedulerWakeAt: number | null = null;
  private suspendedTimeoutTimers: Map<string, ReturnType<typeof setTimeout>> = new Map();
  private promotionTimers: Map<string, ReturnType<typeof setTimeout>> = new Map();
  private waitRejectors: Set<(err: Error) => void> = new Set();
  private waitSweepers: Set<() => void> = new Set();

  constructor(name: string, opts?: TestQueueOptions) {
    super();
    this.name = name;
    this.opts = opts ?? {};
    this.serializer = this.opts.serializer ?? JSON_SERIALIZER;
    TestQueue.registry.set(name, this);
  }

  /** Add a single job. Returns null if deduplicated or duplicate custom ID. */
  async add(name: string, data: D, opts?: JobOptions): Promise<TestJob<D, R> | null> {
    validateJobPriority(opts?.priority ?? 0);
    validateJobOptions(opts);
    const serializedData = this.serializer.serialize(data);
    validateJobDataSize(serializedData);
    const customJobId = opts?.jobId ?? '';

    const dedup = this.opts.dedup !== false ? opts?.deduplication : undefined;
    const now = Date.now();
    if (dedup && this.isDeduplicated(dedup, now)) {
      return null;
    }

    let id: string;
    if (customJobId !== '') {
      if (this.jobs.has(customJobId)) {
        return null;
      }
      id = customJobId;
    } else {
      id = this.generateJobId();
    }
    // Record dedup key only after all checks pass (custom ID, etc.)
    if (dedup) {
      this.dedupEntries.set(dedup.id, { jobId: id, timestamp: now });
    }
    return this.insertRecord(name, serializedData, opts ?? {}, id, now, opts?.delay ?? 0);
  }

  /**
   * Add a job and wait for its result, like Queue.addAndWait: resolves with the
   * return value, rejects with the failed reason, the revoke, or a timeout.
   */
  async addAndWait(name: string, data: D, opts?: AddAndWaitOptions): Promise<R> {
    const waitTimeout = opts?.waitTimeout ?? 30000;
    if (!Number.isFinite(waitTimeout) || waitTimeout <= 0) {
      throw new Error('waitTimeout must be a positive finite number');
    }
    if (opts?.removeOnComplete || opts?.removeOnFail) {
      throw new GlideMQError(
        'Queue.addAndWait does not support removeOnComplete/removeOnFail because it may need the job hash as a fallback.',
      );
    }
    const { waitTimeout: _waitTimeout, ...jobOpts } = opts ?? {};
    const job = await this.add(name, data, jobOpts as JobOptions);
    if (!job) {
      throw new GlideMQError(
        'Queue.addAndWait() cannot wait on a deduplicated/skipped/duplicate-ID add that returned null.',
      );
    }
    return this.waitForJobResult(job.id, waitTimeout);
  }

  /**
   * Resolve once every given job has settled, without pausing, draining or closing
   * anything. Accepts the result of addBulk() or several add() calls; null entries
   * (deduplicated or duplicate-id adds) are ignored. A job whose record is gone
   * (removeOnComplete / removeOnFail) counts as settled. Rejects with the first
   * terminal failure (a failed attempt that will retry does not count), on timeout
   * (naming the pending ids), or when the queue closes.
   */
  async waitForJobs(
    jobs: ReadonlyArray<{ id: string } | null | undefined>,
    opts?: { timeout?: number },
  ): Promise<void> {
    const timeout = opts?.timeout ?? 30000;
    if (!Number.isFinite(timeout) || timeout <= 0 || timeout > MAX_TIMEOUT_DELAY_MS) {
      throw new Error(`timeout must be a positive finite number no greater than ${MAX_TIMEOUT_DELAY_MS}`);
    }
    const pending = new Set<string>();
    for (const job of jobs) if (job) pending.add(job.id);
    if (pending.size === 0) return;
    return new Promise<void>((resolve, reject) => {
      const cleanup = () => {
        clearTimeout(timer);
        this.waitRejectors.delete(rejectOnClose);
        this.waitSweepers.delete(sweep);
        this.off('completed', onCompleted);
        this.off('failed', onFailed);
        this.off('revoked', onRevoked);
        this.off('removed', sweep);
        this.off('drained', sweep);
      };
      const rejectOnClose = (err: Error) => {
        cleanup();
        reject(err);
      };
      const settle = (id: string) => {
        if (!pending.delete(id) || pending.size > 0) return;
        cleanup();
        resolve();
      };
      const onCompleted = (job: TestJob<D, R>) => settle(job.id);
      const onFailed = (job: TestJob<D, R>, err: Error) => {
        if (!pending.has(job.id)) return;
        cleanup();
        reject(err);
      };
      // revoke() fails a not-yet-active job without a `failed` event; an active job is only flagged.
      const onRevoked = (id: string) => {
        if (pending.has(id) && this.jobs.get(id)?.state === 'failed') {
          cleanup();
          reject(new Error('revoked'));
        }
      };
      // remove(), drain() and obliterate() delete records without a completed/failed event.
      const sweep = () => {
        for (const id of [...pending]) if (!this.jobs.has(id)) settle(id);
      };
      const timer = setTimeout(() => {
        cleanup();
        reject(new Error(`Jobs did not finish within ${timeout}ms: pending ${[...pending].join(', ')}`));
      }, timeout);
      timer.unref?.();
      this.waitRejectors.add(rejectOnClose);
      this.waitSweepers.add(sweep);
      // Listeners first, then the current state: a job may settle across an await.
      this.on('completed', onCompleted);
      this.on('failed', onFailed);
      this.on('revoked', onRevoked);
      this.on('removed', sweep);
      this.on('drained', sweep);
      for (const id of [...pending]) {
        const record = this.jobs.get(id);
        if (record?.state === 'completed') settle(id);
        else if (record?.state === 'failed') {
          onFailed(new TestJob<D, R>(record), new Error(record.failedReason as string));
          return;
        }
      }
      sweep();
    });
  }

  private waitForJobResult(jobId: string, timeoutMs: number): Promise<R> {
    return new Promise<R>((resolve, reject) => {
      const cleanup = () => {
        clearTimeout(timer);
        this.waitRejectors.delete(rejectOnClose);
        this.off('completed', onCompleted);
        this.off('failed', onFailed);
        this.off('revoked', onRevoked);
      };
      const rejectOnClose = (err: Error) => {
        cleanup();
        reject(err);
      };
      const onCompleted = (job: TestJob<D, R>, result: R) => {
        if (job.id !== jobId) return;
        cleanup();
        resolve(result);
      };
      const onFailed = (job: TestJob<D, R>, err: Error) => {
        if (job.id !== jobId) return;
        cleanup();
        reject(new Error(job.failedReason || err.message));
      };
      const onRevoked = (id: string) => {
        if (id !== jobId) return;
        cleanup();
        reject(new Error('revoked'));
      };
      const timer = setTimeout(() => {
        cleanup();
        reject(new Error(`Job ${jobId} did not finish within ${timeoutMs}ms`));
      }, timeoutMs);
      timer.unref?.();
      this.waitRejectors.add(rejectOnClose);
      this.on('completed', onCompleted);
      this.on('failed', onFailed);
      this.on('revoked', onRevoked);
      // The worker may already have finished the job on the add() microtask.
      const record = this.jobs.get(jobId);
      if (record?.state === 'completed') onCompleted(new TestJob<D, R>(record), record.returnvalue as R);
      else if (record?.state === 'failed') onFailed(new TestJob<D, R>(record), new Error(record.failedReason));
    });
  }

  private generateJobId(): string {
    let id = String(++this.idCounter);
    let retries = 0;
    while (this.jobs.has(id)) {
      if (++retries >= 1000) throw new Error('Failed to generate job ID: too many collisions with custom job IDs');
      id = String(++this.idCounter);
    }
    return id;
  }

  /**
   * Create the job record and enqueue it like glidemq_addJob: delayed when
   * `delay` > 0, prioritized when it has a priority, otherwise waiting.
   */
  private insertRecord(
    name: string,
    serializedData: string,
    opts: JobOptions,
    id: string,
    now: number,
    delay: number,
    dataSerializer: Serializer = this.serializer,
  ): TestJob<D, R> {
    const ttl = opts.ttl ?? 0;
    const priority = opts.priority ?? 0;
    // Roundtrip data through serializer to match production behavior
    const roundtrippedData = dataSerializer.deserialize(serializedData) as D;
    const record: TestJobRecord<D, R> = {
      id,
      name,
      data: roundtrippedData,
      opts,
      state: 'waiting',
      attemptsMade: 0,
      returnvalue: undefined,
      failedReason: undefined,
      timestamp: now,
      finishedOn: undefined,
      processedOn: undefined,
      expireAt: ttl > 0 ? now + ttl : undefined,
      fallbackIndex: 0,
      serializer: this.serializer,
      queue: this,
    };
    this.jobs.set(id, record);
    if (delay > 0) {
      // glidemq_addJob parks the job in the scheduled ZSet until timestamp + delay.
      this.parkDelayed(record, delay);
    } else if (priority > 0) {
      this.enqueuePrioritized(record);
    } else {
      this.enqueueWaiting(record);
    }

    const job = new TestJob<D, R>(record);
    this.emit('added', job);
    return job;
  }

  /**
   * @internal Waiting jobs in the order Queue.getJobs('waiting') reads them:
   * the priority list (lowest priority number first, FIFO within a priority),
   * then the LIFO list (newest first), then the FIFO stream.
   */
  waitingInDispatchOrder(): TestJobRecord<D, R>[] {
    const seen = new Set<string>();
    const live: TestJobRecord<D, R>[] = [];
    for (const record of this.waitingQueue) {
      if (record.state !== 'waiting' || this.jobs.get(record.id) !== record || seen.has(record.id)) continue;
      seen.add(record.id);
      live.push(record);
    }
    for (const record of this.jobs.values()) {
      if (record.state === 'waiting' && !seen.has(record.id)) live.push(record);
    }
    const priority = live
      .filter((r) => !r.opts.lifo && (r.opts.priority ?? 0) > 0)
      .sort((a, b) => (a.opts.priority ?? 0) - (b.opts.priority ?? 0));
    const lifo = live.filter((r) => r.opts.lifo).reverse();
    const fifo = live.filter((r) => !r.opts.lifo && (r.opts.priority ?? 0) === 0);
    return priority.concat(lifo, fifo);
  }

  /**
   * @internal Drop a record's dispatch entry. Called whenever a job leaves the
   * waiting / prioritized states (park, remove, revoke) and before it is
   * enqueued again, so waitingQueue never holds a record twice.
   */
  dequeueRecord(record: TestJobRecord<D, R>): void {
    const q = this.waitingQueue;
    let write = 0;
    for (let read = 0; read < q.length; read++) {
      if (q[read].id !== record.id) q[write++] = q[read];
    }
    q.length = write;
  }

  /** @internal Put a record in 'waiting' and wake the workers, like an XADD / list push. */
  enqueueWaiting(record: TestJobRecord<D, R>): void {
    this.dequeueRecord(record);
    record.state = 'waiting';
    this.waitingQueue.push(record);
    // Notify attached workers (microtask so the add() caller gets the job first)
    if (!this.paused) {
      queueMicrotask(() => {
        for (const w of this.workers) {
          w.onJobAdded();
        }
      });
    }
  }

  /**
   * @internal A priority job without delay waits in the scheduled ZSet as
   * 'prioritized' until a worker's promotion pass moves it to 'waiting'. Wake
   * the workers even when paused: production promotes on the scheduler tick.
   */
  enqueuePrioritized(record: TestJobRecord<D, R>): void {
    this.dequeueRecord(record);
    record.state = 'prioritized';
    this.waitingQueue.push(record);
    queueMicrotask(() => {
      for (const w of this.workers) {
        w.onJobAdded();
      }
    });
  }

  /** @internal Move every prioritized record to waiting, like glidemq_promote on a worker tick. */
  promotePrioritized(): void {
    for (const record of this.waitingQueue) {
      if (record.state !== 'prioritized' || this.jobs.get(record.id) !== record) continue;
      record.state = 'waiting';
      this.emit('promoted', record.id);
    }
  }

  /** @internal Park a record in 'delayed' for delayMs and schedule its promotion. */
  parkDelayed(record: TestJobRecord<D, R>, delayMs: number): void {
    this.dequeueRecord(record);
    record.state = 'delayed';
    this.schedulePromotion(record, delayMs);
  }

  /**
   * @internal Park an active record like glidemq_moveActiveToDelayed (moveToDelayed,
   * budget pause): parkDelayed plus the `delay-changed` event with the delay in ms.
   * Retry backoff and rate-limit parking stay silent, as in production.
   */
  parkActiveDelayed(record: TestJobRecord<D, R>, delayMs: number): void {
    this.parkDelayed(record, delayMs);
    this.emit('delay-changed', record.id, delayMs);
  }

  /**
   * @internal Release a delayed record now. Job.promote() sends it to waiting;
   * changeDelay(0) keeps a priority job parked as prioritized.
   */
  releaseDelayed(record: TestJobRecord<D, R>, toPrioritized: boolean): void {
    this.clearPromotion(record.id);
    record.delayedUntil = undefined;
    if (toPrioritized) {
      this.enqueuePrioritized(record);
      return;
    }
    this.enqueueWaiting(record);
    this.emit('promoted', record.id);
  }

  /** @internal Delete a job and its timers, like glidemq_removeJob. Returns false when absent. */
  removeJob(id: string): boolean {
    const record = this.jobs.get(id);
    if (!record) return false;
    this.clearPromotion(id);
    this.clearSuspendedTimeout(id);
    this.dequeueRecord(record);
    this.jobs.delete(id);
    this.emit('removed', id);
    return true;
  }

  /**
   * Mirror glidemq_dedup. simple: skip while the tracked job exists and is not
   * completed/failed. throttle: skip while inside `ttl` ms of the tracked add
   * (never with ttl 0). debounce: replace a delayed tracked job, skip while it
   * is waiting/active, accept once it finished or is gone.
   */
  private isDeduplicated(dedup: NonNullable<JobOptions['deduplication']>, now: number): boolean {
    const existing = this.dedupEntries.get(dedup.id);
    if (!existing) return false;
    const mode = dedup.mode ?? 'simple';
    if (mode === 'throttle') {
      const ttl = dedup.ttl ?? 0;
      return ttl > 0 && now - existing.timestamp < ttl;
    }
    const record = this.jobs.get(existing.jobId);
    if (!record || record.state === 'completed' || record.state === 'failed') return false;
    if (mode === 'debounce' && (record.state === 'delayed' || record.state === 'prioritized')) {
      this.removeJob(record.id);
      return false;
    }
    return mode === 'simple' || mode === 'debounce';
  }

  /** Add multiple jobs. */
  async addBulk(jobs: { name: string; data: D; opts?: JobOptions }[]): Promise<TestJob<D, R>[]> {
    const results: TestJob<D, R>[] = [];
    for (const entry of jobs) {
      const job = await this.add(entry.name, entry.data, entry.opts);
      if (job) results.push(job);
    }
    return results;
  }

  /** Retrieve a job by ID. */
  async getJob(id: string, opts?: GetJobsOptions): Promise<TestJob<D, R> | null> {
    const record = this.jobs.get(id);
    if (!record) return null;
    const job = new TestJob<D, R>(record);
    if (opts?.excludeData) {
      job.data = undefined as unknown as D;
      job.returnvalue = undefined;
    }
    return job;
  }

  /** Retrieve jobs by state. */
  async getJobs(
    type: 'waiting' | 'active' | 'delayed' | 'completed' | 'failed',
    start = 0,
    end = -1,
    opts?: GetJobsOptions,
  ): Promise<TestJob<D, R>[]> {
    // The scheduled ZSet backs getJobs('delayed') and holds prioritized jobs too.
    const records =
      type === 'waiting'
        ? this.waitingInDispatchOrder()
        : [...this.jobs.values()].filter((r) => matchesQueueState(r.state, type));
    if (type === 'delayed') {
      // Scheduled ZSet order: score = priority * PRIORITY_SHIFT + due time, ties by id.
      records.sort(
        (a, b) =>
          (a.opts.priority ?? 0) - (b.opts.priority ?? 0) ||
          (a.delayedUntil ?? 0) - (b.delayedUntil ?? 0) ||
          (a.id < b.id ? -1 : a.id > b.id ? 1 : 0),
      );
    }
    const sliceEnd = end >= 0 ? end + 1 : undefined;
    return records.slice(start, sliceEnd).map((record) => {
      const job = new TestJob<D, R>(record);
      if (opts?.excludeData) {
        job.data = undefined as unknown as D;
        job.returnvalue = undefined;
      }
      return job;
    });
  }

  /** Get counts by state. */
  async getJobCounts(): Promise<JobCounts> {
    const counts: JobCounts = { waiting: 0, active: 0, delayed: 0, completed: 0, failed: 0 };
    for (const record of this.jobs.values()) {
      const s = record.state;
      if (s === 'waiting') counts.waiting++;
      else if (s === 'active') counts.active++;
      else if (s === 'delayed' || s === 'prioritized') counts.delayed++;
      else if (s === 'completed') counts.completed++;
      else if (s === 'failed') counts.failed++;
      // 'suspended' is not tracked in JobCounts
    }
    return counts;
  }

  /** Alias for getJobCounts(), like Queue.getJobCountByTypes(). */
  async getJobCountByTypes(): Promise<JobCounts> {
    return this.getJobCounts();
  }

  /**
   * Stream length, like Queue.count(): FIFO jobs that are waiting or active.
   * LIFO and priority jobs live in lists, delayed and prioritized jobs in the scheduled ZSet.
   */
  async count(): Promise<number> {
    let n = 0;
    for (const record of this.jobs.values()) {
      if (
        (record.state === 'waiting' || record.state === 'active') &&
        !record.opts.lifo &&
        !(record.opts.priority ?? 0)
      ) {
        n++;
      }
    }
    return n;
  }

  /** Read the lines appended with job.log(), like Queue.getJobLogs() (LRANGE semantics). */
  async getJobLogs(id: string, start = 0, end = -1): Promise<{ logs: string[]; count: number }> {
    const logs = this.jobs.get(id)?.logs ?? [];
    const sliceEnd = end >= 0 ? end + 1 : logs.length + end + 1;
    return { logs: logs.slice(start, Math.max(start, sliceEnd)), count: logs.length };
  }

  /** Suspended jobs ordered by their timeout deadline, like Queue.getSuspendedJobs(). */
  async getSuspendedJobs(start = 0, end = -1, opts?: GetJobsOptions): Promise<TestJob<D, R>[]> {
    const deadline = (r: TestJobRecord<D, R>) =>
      r.suspendTimeout && r.suspendTimeout > 0 ? (r.suspendedAt ?? 0) + r.suspendTimeout : Number.MAX_SAFE_INTEGER;
    const records = [...this.jobs.values()]
      .filter((r) => r.state === 'suspended')
      .sort((a, b) => deadline(a) - deadline(b) || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
    return records.slice(start, end >= 0 ? end + 1 : undefined).map((record) => {
      const job = new TestJob<D, R>(record);
      if (opts?.excludeData) {
        job.data = undefined as unknown as D;
        job.returnvalue = undefined;
      }
      return job;
    });
  }

  /**
   * Mirror glidemq_revoke: a waiting, delayed or prioritized job is failed with
   * reason 'revoked'; any other existing job is only flagged (isRevoked()).
   */
  async revoke(jobId: string): Promise<string> {
    const record = this.jobs.get(jobId);
    if (!record) return 'not_found';
    record.revoked = true;
    if (record.state === 'waiting' || record.state === 'delayed' || record.state === 'prioritized') {
      this.clearPromotion(jobId);
      this.dequeueRecord(record);
      record.delayedUntil = undefined;
      record.state = 'failed';
      record.failedReason = 'revoked';
      record.finishedOn = Date.now();
      this.emit('revoked', jobId);
      return 'revoked';
    }
    this.emit('revoked', jobId);
    return 'flagged';
  }

  /**
   * Remove every job, scheduler, dedup entry, budget and metric of this queue,
   * like Queue.obliterate(). Refuses while jobs are active unless `force` is set.
   */
  async obliterate(opts?: { force?: boolean }): Promise<void> {
    if (!opts?.force) {
      let active = 0;
      for (const record of this.jobs.values()) if (record.state === 'active') active++;
      if (active > 0) {
        throw new Error(
          `Cannot obliterate queue "${this.name}": ${active} active jobs. Use { force: true } to override.`,
        );
      }
    }
    this.clearSchedulerTimer();
    this.clearAllSuspendedTimeouts();
    for (const timer of this.promotionTimers.values()) clearTimeout(timer);
    this.promotionTimers.clear();
    this.jobs.clear();
    for (const sweep of [...this.waitSweepers]) sweep();
    this.dedupEntries.clear();
    this.waitingQueue.length = 0;
    this.schedulers.clear();
    this.budgets.clear();
    for (const buckets of this.metricsData.values()) buckets.clear();
    this.indexConfig = null;
    this.idCounter = 0;
  }

  /** Get metrics for completed or failed jobs with per-minute data points. */
  async getMetrics(type: 'completed' | 'failed', opts?: MetricsOptions): Promise<Metrics> {
    let count = 0;
    for (const record of this.jobs.values()) {
      if (record.state === type) count++;
    }
    const buckets = this.metricsData.get(type)!;
    const data: MetricsDataPoint[] = Array.from(buckets.entries())
      .sort((a, b) => a[0] - b[0])
      .map(([timestamp, b]) => ({
        timestamp,
        count: b.count,
        avgDuration: b.count > 0 ? Math.round(b.totalDuration / b.count) : 0,
      }));

    const start = opts?.start ?? 0;
    const end = opts?.end ?? -1;

    if (!Number.isInteger(start)) throw new TypeError('start must be an integer');
    if (!Number.isInteger(end)) throw new TypeError('end must be an integer');
    if (start >= 0 && end >= 0 && end < start) {
      throw new RangeError('end must be >= start when both are non-negative');
    }

    const sliced = end === -1 ? data.slice(start) : data.slice(start, end + 1);
    return { count, data: sliced, meta: { resolution: 'minute' } };
  }

  /** @internal */
  recordMetric(type: 'completed' | 'failed', processedOn: number | undefined, finishedOn: number): void {
    const minuteTs = finishedOn - (finishedOn % 60000);
    const buckets = this.metricsData.get(type)!;
    let bucket = buckets.get(minuteTs);
    if (!bucket) {
      bucket = { count: 0, totalDuration: 0 };
      buckets.set(minuteTs, bucket);
      // Cap at 1440 buckets (24 hours of per-minute data)
      if (buckets.size > 1440) {
        const oldest = buckets.keys().next().value;
        if (oldest !== undefined) buckets.delete(oldest);
      }
    }
    bucket.count++;
    const duration = processedOn !== undefined ? finishedOn - processedOn : 0;
    if (duration > 0) bucket.totalDuration += duration;
  }

  /** Pause the queue - workers stop picking up new jobs. */
  async pause(): Promise<void> {
    this.paused = true;
  }

  /** Resume the queue. */
  async resume(): Promise<void> {
    this.paused = false;
    // Kick workers to process any waiting jobs
    for (const w of this.workers) {
      w.onJobAdded();
    }
    this.ensureSchedulerLoop();
  }

  /** Check if paused. */
  isPaused(): boolean {
    return this.paused;
  }

  /** Search jobs by name and/or data fields. */
  async searchJobs(opts: SearchJobsOptions): Promise<TestJob<D, R>[]> {
    const results: TestJob<D, R>[] = [];
    for (const record of this.jobs.values()) {
      if (opts.name !== undefined && record.name !== opts.name) continue;
      if (opts.state !== undefined && !matchesQueueState(record.state, opts.state)) continue;
      if (opts.data !== undefined && !matchesData(record.data as Record<string, unknown>, opts.data)) continue;
      const job = new TestJob<D, R>(record);
      if (opts.excludeData) {
        job.data = undefined as unknown as D;
        job.returnvalue = undefined;
      }
      results.push(job);
    }
    return results;
  }

  /** Bulk-remove old completed or failed jobs by age. */
  async clean(grace: number, limit: number, type: 'completed' | 'failed'): Promise<string[]> {
    if (grace < 0) throw new RangeError('grace must be >= 0');
    if (limit <= 0) return [];
    const cutoff = Date.now() - grace;
    const candidates: [string, number][] = [];
    for (const [id, record] of this.jobs) {
      if (record.state === type && record.finishedOn !== undefined && record.finishedOn <= cutoff) {
        candidates.push([id, record.finishedOn]);
      }
    }
    candidates.sort((a, b) => a[1] - b[1]);
    const removed: string[] = [];
    for (const [id] of candidates.slice(0, limit)) {
      removed.push(id);
      this.jobs.delete(id);
    }
    return removed;
  }

  /** Drain the queue: remove all waiting jobs, optionally delayed ones too. */
  async drain(delayed?: boolean): Promise<void> {
    const toRemove: string[] = [];
    for (const [id, record] of this.jobs) {
      if (record.state === 'waiting' || (delayed && (record.state === 'delayed' || record.state === 'prioritized'))) {
        toRemove.push(id);
      }
    }
    for (const id of toRemove) {
      this.clearPromotion(id);
      this.jobs.delete(id);
    }
    // Waiting entries are gone. Prioritized jobs survive a drain without the
    // delayed flag (they sit in the scheduled ZSet in production) and must stay
    // reachable for the next worker promotion pass.
    const survivors = this.waitingQueue.filter((r) => r.state === 'prioritized' && this.jobs.get(r.id) === r);
    this.waitingQueue.splice(0, this.waitingQueue.length, ...survivors);
    if (toRemove.length > 0) {
      this.emit('drained', toRemove.length);
    }
  }

  /**
   * Bulk retry failed jobs.
   * Moves failed jobs back to waiting, resets attemptsMade/failedReason/finishedOn.
   * @param opts.count - Maximum number of jobs to retry. Omit or 0 to retry all.
   * @returns Number of jobs retried.
   */
  async retryJobs(opts?: { count?: number }): Promise<number> {
    if (opts?.count != null && (!Number.isInteger(opts.count) || opts.count < 0)) {
      throw new Error('count must be a non-negative integer');
    }
    const limit = opts?.count ?? 0;
    const now = Date.now();
    let retried = 0;
    for (const record of this.jobs.values()) {
      if (limit > 0 && retried >= limit) break;
      if (record.state !== 'failed') continue;
      this.retryRecord(record, now);
      retried++;
    }
    return retried;
  }

  /**
   * @internal Mirror retryFailedJob: reset the attempt state, re-arm the TTL
   * from the retry time and enqueue the job again.
   */
  retryRecord(record: TestJobRecord<D, R>, now: number): void {
    record.attemptsMade = 0;
    record.failedReason = undefined;
    record.finishedOn = undefined;
    const ttl = record.opts.ttl ?? 0;
    record.expireAt = ttl > 0 ? now + ttl : undefined;
    this.enqueueWaiting(record);
  }

  /** @internal Worker.drain() stops once nothing is waiting, prioritized or delayed. */
  isDrainComplete(): boolean {
    for (const record of this.jobs.values()) {
      if (record.state === 'waiting' || record.state === 'prioritized' || record.state === 'delayed') return false;
    }
    return true;
  }

  /**
   * @internal After a repeatAfterComplete job completes or terminally fails,
   * schedule the next run (BaseWorker.updateSchedulerAfterComplete).
   */
  onSchedulerJobFinished(record: TestJobRecord<D, R>): void {
    const name = record.schedulerName;
    if (!name) return;
    const entry = this.schedulers.get(name);
    if (!entry || !entry.repeatAfterComplete || entry.nextRun !== 0) return;
    const nextRun = computeFollowingSchedulerNextRun(entry, Date.now());
    if (nextRun == null || (entry.limit != null && (entry.iterationCount ?? 0) >= entry.limit)) {
      this.schedulers.delete(name);
    } else {
      entry.nextRun = nextRun;
      this.schedulers.set(name, JSON.parse(JSON.stringify(entry)));
    }
    this.ensureSchedulerLoop();
  }

  /**
   * @internal Mirror BaseWorker.moveToDLQ: add the failure envelope to the TestQueue
   * registered under `dlqName` (created on first use) as a plain waiting job named
   * like the failed one. The envelope is always plain JSON, as in production, so the
   * serializer of the target queue neither alters nor rejects it.
   */
  addDeadLetter(dlqName: string, name: string, envelope: DeadLetterEnvelope): void {
    const target = TestQueue.registry.get(dlqName) ?? new TestQueue<any, any>(dlqName);
    target.insertRecord(name, JSON.stringify(envelope), {}, target.generateJobId(), Date.now(), 0, JSON_SERIALIZER);
  }

  /** The TestQueue holding this queue's dead-letter jobs, or null when no DLQ name is configured or open. */
  private resolveDeadLetterQueue(): TestQueue<any, any> | null {
    const name = this.opts.deadLetterQueue?.name || this.deadLetterQueueName;
    if (!name) return null;
    return TestQueue.registry.get(name) ?? null;
  }

  /** The dead-letter record for `jobId` when it belongs to this queue, like Queue.isDeadLetterJobOwnedByQueue. */
  private findDeadLetterRecord(jobId: string): TestJobRecord<any, any> | null {
    const record = this.resolveDeadLetterQueue()?.jobs.get(jobId);
    return record && (record.data as DeadLetterEnvelope | undefined)?.originalQueue === this.name ? record : null;
  }

  /**
   * List this queue's dead-letter jobs, like Queue.getDeadLetterJobs(). Reads the
   * jobs still waiting or active in the DLQ (the entries production keeps in the
   * DLQ stream), oldest first, and pages after dropping other queues' jobs.
   * The job data is the envelope `{ originalQueue, originalJobId, data, failedReason, attemptsMade }`.
   */
  async getDeadLetterJobs(start = 0, end = -1, opts?: GetJobsOptions): Promise<TestJob<D, R>[]> {
    const dlq = this.resolveDeadLetterQueue();
    if (!dlq) return [];
    const owned = [...dlq.jobs.values()].filter(
      (r) =>
        (r.state === 'waiting' || r.state === 'active') &&
        (r.data as DeadLetterEnvelope | undefined)?.originalQueue === this.name,
    );
    return owned.slice(start, end >= 0 ? end + 1 : undefined).map((r) => this.toDeadLetterJob(r, opts));
  }

  /** Retrieve one of this queue's dead-letter jobs in any state, like Queue.getDeadLetterJob(). */
  async getDeadLetterJob(jobId: string, opts?: GetJobsOptions): Promise<TestJob<D, R> | null> {
    const record = this.findDeadLetterRecord(jobId);
    return record ? this.toDeadLetterJob(record, opts) : null;
  }

  /** Remove one of this queue's dead-letter jobs. Returns false when it does not exist, like Queue.removeDeadLetterJob(). */
  async removeDeadLetterJob(jobId: string): Promise<boolean> {
    if (!this.findDeadLetterRecord(jobId)) return false;
    this.resolveDeadLetterQueue()?.removeJob(jobId);
    return true;
  }

  /**
   * Add a dead-letter job back to this queue, like Queue.replayDeadLetterJob(). The
   * original job's data and options are reused when it still exists (minus `jobId`,
   * `delay`, `deduplication` and `parent`); otherwise the envelope data is replayed
   * with default options. Returns null when the dead-letter job does not exist.
   */
  async replayDeadLetterJob(jobId: string): Promise<TestJob<D, R> | null> {
    const dlqJob = await this.getDeadLetterJob(jobId);
    if (!dlqJob) return null;
    const envelope = dlqJob.data as unknown as DeadLetterEnvelope;
    let replayData: unknown = envelope.data;
    let replayOpts: JobOptions | undefined;
    const original = envelope.originalJobId ? this.jobs.get(envelope.originalJobId) : undefined;
    if (original) {
      replayData = original.data;
      replayOpts = { ...original.opts };
      delete replayOpts.jobId;
      delete replayOpts.delay;
      delete replayOpts.deduplication;
      delete replayOpts.parent;
    }
    const replayed = await this.add(dlqJob.name, (replayData ?? null) as D, replayOpts);
    if (!replayed) {
      throw new GlideMQError('DLQ replay was skipped due to duplicate or deduplicated job constraints');
    }
    await this.removeDeadLetterJob(jobId);
    return replayed;
  }

  private toDeadLetterJob(record: TestJobRecord<any, any>, opts?: GetJobsOptions): TestJob<D, R> {
    const job = new TestJob<D, R>(record);
    if (opts?.excludeData) {
      job.data = undefined as unknown as D;
      job.returnvalue = undefined;
    }
    return job;
  }

  /** List active workers attached to this queue. */
  async getWorkers(): Promise<WorkerInfo[]> {
    const now = Date.now();
    const result: WorkerInfo[] = [];
    for (const w of this.workers) {
      result.push({
        id: w.id,
        addr: os.hostname(),
        pid: process.pid,
        startedAt: w.startedAt,
        age: now - w.startedAt,
        activeJobs: w.getActiveCount(),
        concurrency: w.concurrency,
      });
    }
    result.sort((a, b) => a.startedAt - b.startedAt);
    return result;
  }

  /** Upsert a job scheduler (repeatable/cron job). */
  async upsertJobScheduler(name: string, schedule: ScheduleOpts, template?: JobTemplate): Promise<void> {
    validateSchedulerEvery(schedule.every);
    if (schedule.repeatAfterComplete != null) {
      if (!Number.isSafeInteger(schedule.repeatAfterComplete) || schedule.repeatAfterComplete <= 0) {
        throw new Error('repeatAfterComplete must be a positive safe integer');
      }
    }
    const modeCount = (schedule.pattern ? 1 : 0) + (schedule.every ? 1 : 0) + (schedule.repeatAfterComplete ? 1 : 0);
    if (modeCount === 0) {
      throw new Error('Schedule must have pattern (cron), every (ms interval), or repeatAfterComplete (ms)');
    }
    if (modeCount > 1) {
      throw new Error('Schedule must have only one of: pattern, every, repeatAfterComplete');
    }
    if (schedule.tz) {
      validateTimezone(schedule.tz);
    }
    const startDate = normalizeScheduleDate(schedule.startDate, 'startDate');
    const endDate = normalizeScheduleDate(schedule.endDate, 'endDate');
    validateSchedulerBounds(startDate, endDate, schedule.limit);
    validateSchedulerTemplate(template, this.serializer);
    const now = Date.now();
    let iterationCount = 0;
    let lastRun: number | undefined;
    let nextRun = computeInitialSchedulerNextRun(
      {
        pattern: schedule.pattern,
        every: schedule.every,
        repeatAfterComplete: schedule.repeatAfterComplete,
        tz: schedule.tz,
        startDate,
        endDate,
      },
      now,
    );
    const existing = this.schedulers.get(name);
    if (existing) {
      const scheduleUnchanged =
        existing.pattern === schedule.pattern &&
        existing.every === schedule.every &&
        existing.repeatAfterComplete === schedule.repeatAfterComplete &&
        existing.tz === schedule.tz &&
        existing.startDate === startDate &&
        existing.endDate === endDate;
      if (scheduleUnchanged && existing.nextRun != null) {
        iterationCount = existing.iterationCount ?? 0;
        lastRun = existing.lastRun;
        nextRun = existing.nextRun;
      } else if (nextRun != null && schedule.repeatAfterComplete != null) {
        nextRun = holdSchedulerModeSwitch(existing, nextRun, endDate);
      }
    }
    if (nextRun == null) {
      throw new Error('Schedule has no occurrences within the configured bounds');
    }
    const entry: SchedulerEntry = {
      pattern: schedule.pattern,
      every: schedule.every,
      repeatAfterComplete: schedule.repeatAfterComplete,
      tz: schedule.tz,
      startDate,
      endDate,
      limit: schedule.limit,
      iterationCount,
      template,
      lastRun,
      nextRun,
    };
    // Store via JSON roundtrip to detach from caller references (matches production serialization)
    this.schedulers.set(name, JSON.parse(JSON.stringify(entry)));
    this.ensureSchedulerLoop();
  }

  /** Remove a job scheduler by name. */
  async removeJobScheduler(name: string): Promise<void> {
    this.schedulers.delete(name);
    this.ensureSchedulerLoop();
  }

  /** Get a single job scheduler entry by name. Returns null if not found. */
  async getJobScheduler(name: string): Promise<SchedulerEntry | null> {
    const entry = this.schedulers.get(name);
    if (!entry) return null;
    return JSON.parse(JSON.stringify(entry));
  }

  /** Get all registered job schedulers. */
  async getRepeatableJobs(): Promise<{ name: string; entry: SchedulerEntry }[]> {
    return [...this.schedulers.entries()].map(([name, entry]) => ({
      name,
      entry: JSON.parse(JSON.stringify(entry)),
    }));
  }

  /**
   * Aggregate AI usage metadata across a flow (parent + children).
   * Walks all jobs whose parent matches parentJobId and sums token counts, cost, and model usage.
   */
  async getFlowUsage(parentJobId: string): Promise<{
    tokens: Record<string, number>;
    totalTokens: number;
    costs: Record<string, number>;
    totalCost: number;
    costUnit?: string;
    jobCount: number;
    models: Record<string, number>;
  }> {
    const agg = {
      tokens: Object.create(null) as Record<string, number>,
      totalTokens: 0,
      costs: Object.create(null) as Record<string, number>,
      totalCost: 0,
      costUnit: undefined as string | undefined,
      jobCount: 0,
      models: Object.create(null) as Record<string, number>,
    };
    const parentJob = this.jobs.get(parentJobId);
    if (!parentJob) return agg;

    const mergeUsage = (usage: JobUsage | undefined) => {
      if (!usage) return;
      const totalTokens = Number.isFinite(usage.totalTokens) ? usage.totalTokens! : 0;
      const totalCost = Number.isFinite(usage.totalCost) ? usage.totalCost! : 0;
      agg.totalTokens += totalTokens;
      agg.totalCost += totalCost;
      agg.jobCount++;
      if (usage.costUnit && !agg.costUnit) agg.costUnit = usage.costUnit;
      if (usage.model) agg.models[usage.model] = (agg.models[usage.model] || 0) + 1;
      if (usage.tokens) {
        for (const [k, v] of Object.entries(usage.tokens)) {
          if (Number.isFinite(v)) {
            agg.tokens[k] = (agg.tokens[k] || 0) + v;
          }
        }
      }
      if (usage.costs) {
        for (const [k, v] of Object.entries(usage.costs)) {
          if (Number.isFinite(v)) {
            agg.costs[k] = (agg.costs[k] || 0) + v;
          }
        }
      }
    };

    // Include parent
    mergeUsage(parentJob.usage);

    // Walk all jobs looking for children (testing mode has no deps set, scan by parentId)
    for (const [, record] of this.jobs) {
      if (record.opts?.parent?.id === parentJobId) {
        mergeUsage(record.usage);
      }
    }

    return agg;
  }

  /**
   * Read the budget state for a flow. Returns null if no budget was set.
   */
  async getFlowBudget(flowId: string): Promise<{
    maxTotalTokens?: number;
    maxTokens?: Record<string, number>;
    tokenWeights?: Record<string, number>;
    maxTotalCost?: number;
    maxCosts?: Record<string, number>;
    costUnit?: string;
    usedTokens: number;
    usedCost: number;
    exceeded: boolean;
    onExceeded: 'pause' | 'fail';
  } | null> {
    const budget = this.budgets.get(flowId);
    if (!budget) return null;
    const { usedTokensByCategory: _t, usedCostsByCategory: _c, ...rest } = budget;
    return { ...rest };
  }

  /**
   * Aggregate reported AI usage across all TestQueue instances or a selected subset.
   * Mirrors the production Queue.getUsageSummary() surface.
   */
  async getUsageSummary(opts?: UsageSummaryOptions): Promise<UsageSummary> {
    const { startTime, endTime } = resolveUsageWindow(opts);
    const summary = createUsageSummary(startTime, endTime);
    const selectedQueues = opts?.queues
      ? Array.from(new Set(opts.queues))
          .map((name) => TestQueue.registry.get(name))
          .filter(Boolean)
      : Array.from(TestQueue.registry.values());

    for (const queue of selectedQueues) {
      if (!queue) continue;
      let queueSummary: UsageQueueSummary | undefined;

      for (const record of queue.jobs.values()) {
        if (!record.usage || record.usageReportedAt == null) continue;

        const reportedBucket = floorUsageBucket(record.usageReportedAt);
        if (reportedBucket < floorUsageBucket(startTime) || reportedBucket > floorUsageBucket(endTime)) {
          continue;
        }

        if (!queueSummary) {
          queueSummary = createUsageQueueSummary();
          summary.perQueue[queue.name] = queueSummary;
        }

        mergeUsage(summary, record.usage);
        mergeUsage(queueSummary, record.usage);
      }
    }

    summary.queues = Object.keys(summary.perQueue).sort();
    return summary;
  }

  /**
   * @internal Set a budget for a flow (used by test setup).
   */
  setBudget(
    flowId: string,
    budget: {
      maxTotalTokens?: number;
      maxTokens?: Record<string, number>;
      tokenWeights?: Record<string, number>;
      maxTotalCost?: number;
      maxCosts?: Record<string, number>;
      costUnit?: string;
      onExceeded?: 'pause' | 'fail';
    },
  ): void {
    this.budgets.set(flowId, {
      maxTotalTokens: budget.maxTotalTokens,
      maxTokens: budget.maxTokens,
      tokenWeights: budget.tokenWeights,
      maxTotalCost: budget.maxTotalCost,
      maxCosts: budget.maxCosts,
      costUnit: budget.costUnit,
      usedTokens: 0,
      usedCost: 0,
      usedTokensByCategory: Object.create(null) as Record<string, number>,
      usedCostsByCategory: Object.create(null) as Record<string, number>,
      exceeded: false,
      onExceeded: budget.onExceeded ?? 'fail',
    });
  }

  /**
   * @internal Record usage against a budget and check if exceeded.
   * Supports per-category tracking, weighted totals, and per-category limits.
   * Returns 'ok', 'exceeded', or 'no_budget'.
   */
  recordBudgetUsage(
    budgetKey: string,
    tokens: Record<string, number>,
    costs: Record<string, number>,
    weightedTotal: number,
    totalCost: number,
  ): string {
    const budget = this.budgets.get(budgetKey);
    if (!budget) return 'no_budget';

    budget.usedTokens += weightedTotal;
    budget.usedCost += totalCost;
    for (const [cat, val] of Object.entries(tokens)) {
      budget.usedTokensByCategory[cat] = (budget.usedTokensByCategory[cat] || 0) + val;
    }
    for (const [cat, val] of Object.entries(costs)) {
      budget.usedCostsByCategory[cat] = (budget.usedCostsByCategory[cat] || 0) + val;
    }

    if (TestQueue.budgetLimitsExceeded(budget)) {
      budget.exceeded = true;
      return 'exceeded';
    }
    return 'ok';
  }

  private static budgetLimitsExceeded(budget: TestBudgetState): boolean {
    if ((budget.maxTotalTokens ?? 0) > 0 && budget.usedTokens > budget.maxTotalTokens!) return true;
    if ((budget.maxTotalCost ?? 0) > 0 && budget.usedCost > budget.maxTotalCost!) return true;
    for (const [cat, limit] of Object.entries(budget.maxTokens ?? {})) {
      if (limit > 0 && (budget.usedTokensByCategory[cat] ?? 0) > limit) return true;
    }
    for (const [cat, limit] of Object.entries(budget.maxCosts ?? {})) {
      if (limit > 0 && (budget.usedCostsByCategory[cat] ?? 0) > limit) return true;
    }
    return false;
  }

  /**
   * Change the limits of a flow budget and re-evaluate its exceeded flag.
   * Mirrors Queue.updateFlowBudget(); null deletes a limit.
   */
  async updateFlowBudget(
    flowId: string,
    limits: {
      maxTotalTokens?: number | null;
      maxTokens?: Record<string, number> | null;
      tokenWeights?: Record<string, number> | null;
      maxTotalCost?: number | null;
      maxCosts?: Record<string, number> | null;
      costUnit?: string | null;
      onExceeded?: 'pause' | 'fail';
    },
  ): Promise<Awaited<ReturnType<TestQueue['getFlowBudget']>>> {
    const budget = this.budgets.get(flowId);
    if (!budget) return null;
    for (const key of [
      'maxTotalTokens',
      'maxTokens',
      'tokenWeights',
      'maxTotalCost',
      'maxCosts',
      'costUnit',
    ] as const) {
      const value = limits[key];
      if (value === undefined) continue;
      if (value === null) delete (budget as any)[key];
      else (budget as any)[key] = value;
    }
    if (limits.onExceeded !== undefined) budget.onExceeded = limits.onExceeded;
    budget.exceeded = TestQueue.budgetLimitsExceeded(budget);
    return this.getFlowBudget(flowId);
  }

  /**
   * @internal Check if a budget is exceeded. Returns 'ok', 'exceeded', or 'no_budget'.
   */
  checkBudget(budgetKey: string): string {
    const budget = this.budgets.get(budgetKey);
    if (!budget) return 'no_budget';
    return budget.exceeded ? 'exceeded' : 'ok';
  }

  /** Read entries from a job's streaming channel. */
  async readStream(
    jobId: string,
    opts?: { lastId?: string; count?: number; block?: number },
  ): Promise<{ id: string; fields: Record<string, string> }[]> {
    const record = this.jobs.get(jobId);
    if (!record) return [];
    const lastId = opts?.lastId;
    const count = opts?.count ?? 100;
    const blockMs = opts?.block;

    const readChunks = (): { id: string; fields: Record<string, string> }[] => {
      const chunks = record.streamChunks ?? [];
      let filtered = chunks;
      if (lastId) {
        const idx = chunks.findIndex((c) => c.id === lastId);
        filtered = idx >= 0 ? chunks.slice(idx + 1) : chunks;
      }
      return filtered.slice(0, count);
    };

    const immediate = readChunks();
    if (immediate.length > 0 || !blockMs || blockMs <= 0) {
      return immediate;
    }

    // Simulate blocking: poll at 50ms intervals until timeout
    const deadline = Date.now() + blockMs;
    while (Date.now() < deadline) {
      await new Promise<void>((r) => setTimeout(r, Math.min(50, deadline - Date.now())));
      const result = readChunks();
      if (result.length > 0) return result;
    }
    return [];
  }

  /**
   * Send a signal to a suspended job, resuming it.
   * Returns true if the job was resumed, false if not in suspended state.
   */
  async signal(jobId: string, signalName: string, data?: any): Promise<boolean> {
    const record = this.jobs.get(jobId);
    if (!record || record.state !== 'suspended') return false;
    this.clearSuspendedTimeout(jobId);

    if (!record.signals) record.signals = [];
    record.signals.push({ name: signalName, data: data ?? '', receivedAt: Date.now() });

    record.state = 'waiting';
    this.waitingQueue.push(record);
    this.emit('resumed', jobId, signalName);

    // Notify attached workers so the resumed job gets processed
    queueMicrotask(() => {
      for (const w of this.workers) {
        w.onJobAdded();
      }
    });
    return true;
  }

  /**
   * Get suspension information for a job.
   * Returns null if the job is not in the suspended state.
   */
  async getSuspendInfo(jobId: string): Promise<{
    reason?: string;
    suspendedAt: number;
    timeout?: number;
    signals: SignalEntry[];
  } | null> {
    const record = this.jobs.get(jobId);
    if (!record || record.state !== 'suspended') return null;
    return {
      reason: record.suspendReason,
      suspendedAt: record.suspendedAt ?? 0,
      timeout: record.suspendTimeout,
      signals: record.signals ?? [],
    };
  }

  /**
   * Create a job index (in-memory no-op, stores config for vectorSearch).
   */
  async createJobIndex(opts?: JobIndexOptions): Promise<void> {
    this.indexConfig = opts ?? {};
  }

  /**
   * Drop the job index (clears stored config).
   */
  async dropJobIndex(_name?: string): Promise<void> {
    this.indexConfig = null;
  }

  /**
   * Vector similarity search over jobs with stored vectors (brute-force cosine similarity).
   * Requires prior createJobIndex call.
   */
  async vectorSearch(
    embedding: number[] | Float32Array,
    opts?: VectorSearchOptions,
  ): Promise<{ job: TestJob<D, R>; score: number }[]> {
    if (!this.indexConfig) {
      throw new Error('No index created. Call createJobIndex() first.');
    }
    const k = opts?.k ?? 10;
    const query = Array.from(embedding);
    const vecFieldName = this.indexConfig.vectorField?.name ?? '_vec';

    const candidates: { record: TestJobRecord<D, R>; score: number }[] = [];
    for (const record of this.jobs.values()) {
      // Apply pre-filter if provided
      if (opts?.filter) {
        const stateMatch = opts.filter.match(/@state:\{(\w+)\}/);
        if (stateMatch && record.state !== stateMatch[1]) continue;
      }
      const vectors = record.vectors;
      if (!vectors) continue;
      const vec = vectors.get(vecFieldName);
      if (!vec || vec.length !== query.length) continue;
      const sim = cosineSimilarity(query, vec);
      // Convert similarity to distance (lower = more similar, matching Valkey COSINE behavior)
      const distance = 1 - sim;
      candidates.push({ record, score: distance });
    }

    candidates.sort((a, b) => a.score - b.score);
    const topK = candidates.slice(0, k);

    return topK.map((c) => ({
      job: new TestJob<D, R>(c.record),
      score: c.score,
    }));
  }

  /**
   * @internal Promote a job parked in 'delayed' (delay option, retry backoff or
   * moveToDelayed) back to 'waiting' once its delay elapses: the in-memory
   * equivalent of glidemq_promote.
   */
  schedulePromotion(record: TestJobRecord<D, R>, delayMs: number): void {
    this.clearPromotion(record.id);
    const wait = Math.min(Math.max(0, delayMs), MAX_TIMEOUT_DELAY_MS);
    record.delayedUntil = Date.now() + wait;
    const timer = setTimeout(() => {
      this.promotionTimers.delete(record.id);
      if (this.jobs.get(record.id) !== record || record.state !== 'delayed') return;
      record.delayedUntil = undefined;
      this.dequeueRecord(record);
      record.state = 'waiting';
      this.waitingQueue.push(record);
      this.emit('promoted', record.id);
      if (!this.paused) {
        for (const w of this.workers) w.onJobAdded();
      }
    }, wait);
    timer.unref?.();
    this.promotionTimers.set(record.id, timer);
  }

  private clearPromotion(jobId: string): void {
    const timer = this.promotionTimers.get(jobId);
    if (!timer) return;
    clearTimeout(timer);
    this.promotionTimers.delete(jobId);
  }

  /** Close the queue, clear timers, and detach all workers. */
  async close(): Promise<void> {
    this.clearSchedulerTimer();
    this.clearAllSuspendedTimeouts();
    for (const timer of this.promotionTimers.values()) clearTimeout(timer);
    this.promotionTimers.clear();
    for (const reject of [...this.waitRejectors]) reject(new GlideMQError('Queue is closing'));
    this.waitRejectors.clear();
    this.removeAllListeners();
    this.workers.clear();
    TestQueue.registry.delete(this.name);
  }

  /**
   * @internal Remove and return the next job to dispatch, mirroring the worker
   * fetch order: priority list (lowest priority number first, FIFO within a
   * priority), then LIFO (newest first), then the FIFO stream. A lifo job with a
   * priority is dispatched from the LIFO list, like production promotion.
   */
  takeNextWaiting(): TestJobRecord<D, R> | undefined {
    const best = this.nextWaitingIndex();
    return best < 0 ? undefined : this.waitingQueue.splice(best, 1)[0];
  }

  /** @internal True when takeNextWaiting() would return a job. Does not pop anything. */
  hasWaiting(): boolean {
    return this.nextWaitingIndex() >= 0;
  }

  /** Compact the dispatch queue and return the index of the next job to dispatch, or -1. */
  private nextWaitingIndex(): number {
    this.promotePrioritized();
    const q = this.waitingQueue;
    const seen = new Set<string>();
    let write = 0;
    for (let read = 0; read < q.length; read++) {
      const r = q[read];
      if (r.state === 'waiting' && this.jobs.get(r.id) === r && !seen.has(r.id)) {
        seen.add(r.id);
        q[write++] = r;
      }
    }
    q.length = write;

    let best = -1;
    let bestPriority = Number.POSITIVE_INFINITY;
    let lifo = -1;
    for (let i = 0; i < q.length; i++) {
      const opts = q[i].opts;
      if (opts.lifo) {
        lifo = i;
        continue;
      }
      const priority = opts.priority ?? 0;
      if (priority > 0 && priority < bestPriority) {
        bestPriority = priority;
        best = i;
      }
    }
    if (best < 0) best = lifo;
    if (best < 0) best = q.length > 0 ? 0 : -1;
    return best;
  }

  /**
   * @internal Apply removeOnComplete / removeOnFail retention after a job reaches
   * a terminal state. Mirrors glidemq_complete / glidemq_fail: `true` deletes the
   * job, a number keeps the newest N jobs in that state, `{ age, count }` first
   * drops jobs finished more than `age` seconds ago, then keeps the newest `count`.
   */
  applyRetention(record: TestJobRecord<D, R>, state: 'completed' | 'failed'): void {
    const opt = state === 'completed' ? record.opts.removeOnComplete : record.opts.removeOnFail;
    if (!opt) return;
    if (opt === true) {
      this.jobs.delete(record.id);
      return;
    }
    const now = record.finishedOn ?? Date.now();
    const count = typeof opt === 'number' ? opt : (opt.count ?? 0);
    const age = typeof opt === 'number' ? 0 : (opt.age ?? 0);
    const MAX_REMOVALS = 1000;
    // Sorted like the completed/failed ZSET: score finishedOn, ties by member (job id).
    const inState = (): TestJobRecord<D, R>[] =>
      [...this.jobs.values()]
        .filter((r) => r.state === state)
        .sort((a, b) => (a.finishedOn ?? 0) - (b.finishedOn ?? 0) || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
    if (age > 0) {
      const cutoff = now - age * 1000;
      const old = inState()
        .filter((r) => (r.finishedOn ?? 0) <= cutoff)
        .slice(0, MAX_REMOVALS);
      for (const r of old) this.jobs.delete(r.id);
    }
    if (count > 0) {
      const all = inState();
      if (all.length > count) {
        for (const r of all.slice(0, Math.min(all.length - count, MAX_REMOVALS))) this.jobs.delete(r.id);
      }
    }
  }

  /** @internal Called by TestWorker when it attaches. */
  onWorkerAttached(): void {
    this.ensureSchedulerLoop();
  }

  /** @internal Called by TestWorker when it detaches. */
  onWorkerDetached(): void {
    if (this.workers.size === 0) {
      this.clearSchedulerTimer();
    }
  }

  private clearSchedulerTimer(): void {
    if (this.schedulerTimer) {
      clearTimeout(this.schedulerTimer);
      this.schedulerTimer = null;
    }
    this.nextSchedulerWakeAt = null;
  }

  /** @internal */
  scheduleSuspendedTimeout(record: TestJobRecord<D, R>): void {
    this.clearSuspendedTimeout(record.id);
    if (!record.suspendTimeout || record.suspendTimeout <= 0) return;

    const suspendedAt = record.suspendedAt ?? Date.now();
    let remaining = Math.max(0, suspendedAt + record.suspendTimeout - Date.now());

    const scheduleNextChunk = () => {
      const chunk = Math.min(remaining, MAX_TIMEOUT_DELAY_MS);
      const timer = setTimeout(() => {
        this.suspendedTimeoutTimers.delete(record.id);
        remaining -= chunk;

        if (remaining > 0) {
          scheduleNextChunk();
          return;
        }

        const current = this.jobs.get(record.id);
        if (!current || current.state !== 'suspended') return;

        current.state = 'failed';
        current.failedReason = 'Suspend timeout exceeded';
        current.finishedOn = Date.now();

        const job = new TestJob<D, R>(current);
        job.failedReason = current.failedReason;
        job.finishedOn = current.finishedOn;

        this.recordMetric('failed', current.suspendedAt, current.finishedOn);
        this.emit('failed', job, new Error(current.failedReason));
      }, chunk);
      timer.unref?.();
      this.suspendedTimeoutTimers.set(record.id, timer);
    };

    scheduleNextChunk();
  }

  private clearSuspendedTimeout(jobId: string): void {
    const timer = this.suspendedTimeoutTimers.get(jobId);
    if (!timer) return;
    clearTimeout(timer);
    this.suspendedTimeoutTimers.delete(jobId);
  }

  private clearAllSuspendedTimeouts(): void {
    for (const timer of this.suspendedTimeoutTimers.values()) {
      clearTimeout(timer);
    }
    this.suspendedTimeoutTimers.clear();
  }

  private ensureSchedulerLoop(): void {
    if (this.schedulerRunning || this.workers.size === 0 || this.schedulers.size === 0) {
      return;
    }

    let nextDue = Number.POSITIVE_INFINITY;
    const now = Date.now();
    for (const entry of this.schedulers.values()) {
      // nextRun 0 is the repeatAfterComplete "awaiting completion" sentinel.
      if (entry.repeatAfterComplete && entry.nextRun === 0) continue;
      if (entry.nextRun != null && entry.nextRun < nextDue) {
        nextDue = entry.nextRun;
      }
    }
    if (!Number.isFinite(nextDue)) return;

    if (this.schedulerTimer && this.nextSchedulerWakeAt != null && this.nextSchedulerWakeAt <= nextDue) {
      return;
    }

    this.clearSchedulerTimer();

    const delay = Math.max(0, nextDue - now);
    const clampedDelay = Math.min(delay, MAX_TIMEOUT_DELAY_MS);
    this.nextSchedulerWakeAt = now + clampedDelay;
    this.schedulerTimer = setTimeout(() => {
      this.schedulerTimer = null;
      this.nextSchedulerWakeAt = null;
      void this.runDueSchedulers();
    }, clampedDelay);
  }

  private async runDueSchedulers(): Promise<void> {
    if (this.schedulerRunning || this.workers.size === 0) return;
    this.schedulerRunning = true;

    try {
      const now = Date.now();
      const dueNames: string[] = [];
      for (const [name, entry] of this.schedulers.entries()) {
        if (!entry.pattern && !entry.every && !entry.repeatAfterComplete) {
          dueNames.push(name);
          continue;
        }
        // Skip repeatAfterComplete entries with nextRun=0 (awaiting completion)
        if (entry.repeatAfterComplete && entry.nextRun === 0) continue;

        if (entry.nextRun != null && entry.nextRun <= now) {
          dueNames.push(name);
        }
      }

      for (const name of dueNames) {
        const entry = this.schedulers.get(name);
        if (!entry) continue;

        if (!entry.pattern && !entry.every && !entry.repeatAfterComplete) {
          this.schedulers.delete(name);
          continue;
        }
        if (entry.nextRun == null || entry.nextRun > now) continue;

        const currentIterationCount = entry.iterationCount ?? 0;
        if (
          (entry.limit != null && currentIterationCount >= entry.limit) ||
          (entry.endDate != null && entry.nextRun > entry.endDate)
        ) {
          this.schedulers.delete(name);
          continue;
        }

        const template = entry.template ?? {};
        const jobName = template.name ?? name;
        const jobData = template.data !== undefined ? template.data : ({} as D);
        let serialized: string;
        try {
          serialized = this.serializer.serialize(jobData);
          if (Buffer.byteLength(serialized, 'utf8') > MAX_JOB_DATA_SIZE) {
            this.schedulers.delete(name);
            continue;
          }
        } catch {
          this.schedulers.delete(name);
          continue;
        }
        // Like Scheduler.runSchedulers, a run calls glidemq_addJob directly: no
        // deduplication, no custom jobId and delay 0, whatever the stored template says.
        const created = this.insertRecord(
          jobName,
          serialized,
          (template.opts as JobOptions | undefined) ?? {},
          this.generateJobId(),
          now,
          0,
        );
        const isRepeatAfterComplete = entry.repeatAfterComplete != null && entry.repeatAfterComplete > 0;
        if (isRepeatAfterComplete) {
          const record = this.jobs.get(created.id);
          if (record) record.schedulerName = name;
        }

        entry.lastRun = now;
        entry.iterationCount = currentIterationCount + 1;
        if (entry.limit != null && entry.iterationCount >= entry.limit) {
          this.schedulers.delete(name);
        } else if (isRepeatAfterComplete) {
          // Sentinel: the worker schedules the next run when the job finishes.
          entry.nextRun = 0;
          this.schedulers.set(name, JSON.parse(JSON.stringify(entry)));
        } else {
          const nextRun = computeFollowingSchedulerNextRun(entry, now);
          if (nextRun == null) {
            this.schedulers.delete(name);
          } else {
            entry.nextRun = nextRun;
            this.schedulers.set(name, JSON.parse(JSON.stringify(entry)));
          }
        }
      }
    } finally {
      this.schedulerRunning = false;
      this.ensureSchedulerLoop();
    }
  }
}

// ---- TestWorker ----

export interface TestWorkerOptions {
  concurrency?: number;
  batch?: { size: number; timeout?: number };
  /**
   * Jobs-per-window rate limit, same shape as WorkerOptions.limiter. Mirrors
   * glidemq_rateLimit: at most `max` dispatches per fixed `duration` window.
   * Also the default pause after a processor throws RateLimitError.
   */
  limiter?: { max: number; duration: number };
  /** Token-per-minute rate limiting (in-memory only for testing mode). */
  tokenLimiter?: {
    maxTokens: number;
    duration: number;
  };
  /** Custom backoff strategies keyed by `backoff.type`, same as WorkerOptions.backoffStrategies. */
  backoffStrategies?: Record<string, (attemptsMade: number, err: Error) => number>;
  /**
   * Copy every job that fails terminally into the TestQueue registered under this name, like
   * WorkerOptions.deadLetterQueue. A TestQueue is created for the name when none is open yet.
   */
  deadLetterQueue?: DeadLetterQueueOptions;
}

/** In-memory test double for Worker. Processes jobs from a TestQueue without Valkey. */
export class TestWorker<D = any, R = any> extends EventEmitter {
  private static idCounter = 0;
  readonly id: string;
  readonly startedAt: number;
  private queue: TestQueue<D, R>;
  private processor: Processor<D, R>;
  readonly concurrency: number;
  private activeCount = 0;
  private running = true;
  private paused = false;
  private processing = false;
  private isDrained = true;
  private readonly batchMode: boolean;
  private readonly batchSize: number;
  private readonly batchTimeout: number;
  private readonly batchProcessor: ((jobs: TestJob<D, R>[]) => Promise<R[]>) | null;
  private batchTimer: ReturnType<typeof setTimeout> | null = null;
  private pendingBatch: TestJobRecord<D, R>[] = [];
  private readonly tokenLimiter: TestWorkerOptions['tokenLimiter'];
  private readonly limiter: TestWorkerOptions['limiter'];
  private readonly backoffStrategies: TestWorkerOptions['backoffStrategies'];
  private readonly deadLetterQueue: TestWorkerOptions['deadLetterQueue'];
  private tpmLocalCounter = 0;
  private tpmWindowStart = 0;
  private rateLimitUntil = 0;
  private rateWindowStart = 0;
  private rateWindowCount = 0;
  private rateLimitTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(
    queue: TestQueue<D, R>,
    processor: Processor<D, R> | ((jobs: TestJob<D, R>[]) => Promise<R[]>) | string,
    opts?: TestWorkerOptions,
  ) {
    super();
    if (!(queue instanceof TestQueue)) {
      throw new GlideMQError(
        `TestWorker expects a TestQueue instance as its first argument, got ${queue === null ? 'null' : typeof queue}`,
      );
    }
    this.queue = queue;

    // Batch mode validation
    this.batchMode = !!opts?.batch;
    if (opts?.batch) {
      if (!Number.isInteger(opts.batch.size) || opts.batch.size < 1 || opts.batch.size > 1000) {
        throw new GlideMQError('batch.size must be an integer between 1 and 1000');
      }
      if (opts.batch.timeout !== undefined && (opts.batch.timeout < 0 || !Number.isFinite(opts.batch.timeout))) {
        throw new GlideMQError('batch.timeout must be a non-negative finite number');
      }
      if (typeof processor === 'string') {
        throw new GlideMQError('Batch mode does not support sandbox (file path) processors');
      }
      this.batchSize = opts.batch.size;
      this.batchTimeout = opts.batch.timeout ?? 0;
      this.batchProcessor = processor as (jobs: TestJob<D, R>[]) => Promise<R[]>;
      this.processor = (() => {
        throw new Error('Single-job processor called in batch mode');
      }) as unknown as Processor<D, R>;
    } else {
      this.batchSize = 0;
      this.batchTimeout = 0;
      this.batchProcessor = null;

      if (typeof processor === 'string') {
        const filePath = path.resolve(processor);
        if (filePath.endsWith('.mjs')) {
          throw new GlideMQError(
            'TestWorker does not support ESM (.mjs) processors. Use a .js (CJS) file or an inline function.',
          );
        }
        // eslint-disable-next-line @typescript-eslint/no-require-imports
        const mod = require(filePath);
        const fn = mod.default || mod;
        if (typeof fn !== 'function') {
          throw new GlideMQError(`Processor file ${filePath} does not export a function`);
        }
        this.processor = fn;
      } else {
        this.processor = processor as Processor<D, R>;
      }
    }
    this.concurrency = opts?.concurrency ?? 1;
    this.tokenLimiter = opts?.tokenLimiter;
    this.limiter = opts?.limiter;
    this.backoffStrategies = opts?.backoffStrategies;
    this.deadLetterQueue = opts?.deadLetterQueue;
    if (this.deadLetterQueue?.name) queue.deadLetterQueueName = this.deadLetterQueue.name;
    this.id = `test-worker-${++TestWorker.idCounter}`;
    this.startedAt = Date.now();

    // Register with the queue
    queue.workers.add(this);
    queue.onWorkerAttached();

    // Process any jobs already in the queue
    queueMicrotask(() => this.wake());
  }

  /** @internal Called by TestQueue when a job is added. */
  onJobAdded(): void {
    if (!this.running) return;
    this.wake();
  }

  /** Pull and process waiting jobs up to concurrency. */
  private wake(): void {
    if (this.processing) return;
    this.processing = true;

    // Use microtask to batch multiple onJobAdded calls
    queueMicrotask(() => {
      this.processing = false;
      this.processAvailable();
    });
  }

  private processAvailable(): void {
    if (!this.running) return;
    this.queue.promotePrioritized();
    if (this.queue.isPaused() || this.paused) return;

    if (this.batchMode) {
      this.processAvailableBatch();
      return;
    }

    while (this.activeCount < this.concurrency) {
      const record = this.findNextWaiting();
      if (!record) break;

      record.state = 'active';
      record.processedOn = Date.now();
      this.activeCount++;
      this.isDrained = false;
      const job = new TestJob<D, R>(record);
      this.emit('active', job, record.id);
      this.processJob(record, job);
    }

    if (this.activeCount === 0 && !this.isDrained) {
      this.isDrained = true;
      this.emit('drained');
    }
  }

  /**
   * Take the next waiting job unless the worker is rate limited. Like
   * BaseWorker.waitForRateLimit before a fetch, a manual rateLimit(ms) /
   * RateLimitError pause is honoured first, then the limiter window. While
   * limited nothing is popped, so priority, LIFO and FIFO order are kept, and
   * a wake-up timer resumes dispatch.
   */
  private findNextWaiting(): TestJobRecord<D, R> | undefined {
    if (!this.queue.hasWaiting()) return undefined;
    const wait = this.acquireRateSlot();
    if (wait > 0) {
      this.wakeAfterRateLimit(wait);
      return undefined;
    }
    return this.queue.takeNextWaiting();
  }

  /** Returns 0 and counts a dispatch, or the ms to wait (mirrors glidemq_rateLimit). */
  private acquireRateSlot(): number {
    const now = Date.now();
    if (this.rateLimitUntil > now) return this.rateLimitUntil - now;
    if (!this.limiter || this.limiter.max <= 0) return 0;
    const { max, duration } = this.limiter;
    if (now - this.rateWindowStart >= duration) {
      this.rateWindowStart = now;
      this.rateWindowCount = 1;
      return 0;
    }
    if (this.rateWindowCount >= max) return duration - (now - this.rateWindowStart);
    this.rateWindowCount++;
    return 0;
  }

  private wakeAfterRateLimit(ms: number): void {
    if (this.rateLimitTimer) return;
    this.rateLimitTimer = setTimeout(
      () => {
        this.rateLimitTimer = null;
        if (this.running && !this.queue.isPaused() && !this.paused) this.processAvailable();
      },
      Math.min(Math.max(1, ms), MAX_TIMEOUT_DELAY_MS),
    );
    this.rateLimitTimer.unref?.();
  }

  private processJob(record: TestJobRecord<D, R>, job: TestJob<D, R>): void {
    // Check TTL expiration before processing
    if (record.expireAt && Date.now() > record.expireAt) {
      record.state = 'failed';
      record.failedReason = 'expired';
      record.finishedOn = Date.now();
      job.failedReason = 'expired';
      job.finishedOn = record.finishedOn;
      this.queue.recordMetric('failed', record.processedOn, record.finishedOn);
      const err = new Error('expired');
      this.emit('failed', job, err);
      this.queue.emit('failed', job, err);
      this.activeCount--;
      if (this.running && !this.queue.isPaused()) {
        this.processAvailable();
      }
      return;
    }
    // Pre-dispatch budget check
    if (record.budgetKey) {
      const budgetStatus = this.queue.checkBudget(record.budgetKey);
      if (budgetStatus === 'exceeded') {
        const budget = this.queue.budgets.get(record.budgetKey);
        this.emit('budget-exceeded', job, record.id);
        if (budget?.onExceeded === 'pause') {
          // Like moveActiveToDelayed(now + 24h): parked until the budget is raised and the job promoted.
          this.queue.parkActiveDelayed(record, 86_400_000);
          this.activeCount--;
          if (this.running && !this.queue.isPaused()) {
            this.processAvailable();
          }
          return;
        }
        this.handleFailure(record, job, new Error('Budget exceeded'));
        this.activeCount--;
        if (this.running && !this.queue.isPaused()) {
          this.processAvailable();
        }
        return;
      }
    }
    this.waitForTokenLimitIfNeeded()
      .then(() => this.processor(job as any))
      .then((result) => {
        if (record.movedToFailed) {
          const err = record.movedToFailed;
          record.movedToFailed = undefined;
          this.handleFailure(record, job, err);
          return;
        }
        // Roundtrip returnvalue through serializer to match production behavior
        const s = this.queue.serializer;
        const roundtripped = result !== undefined ? (s.deserialize(s.serialize(result)) as R) : result;
        record.state = 'completed';
        record.returnvalue = roundtripped;
        record.finishedOn = Date.now();
        job.returnvalue = roundtripped;
        job.finishedOn = record.finishedOn;
        this.queue.applyRetention(record, 'completed');
        this.queue.recordMetric('completed', record.processedOn, record.finishedOn);
        this.emit('completed', job, roundtripped);
        this.queue.emit('completed', job, roundtripped);
        this.queue.onSchedulerJobFinished(record);

        // Post-completion TPM tracking
        if (this.tokenLimiter) {
          const tpmTokens = Math.max(job.usage?.totalTokens ?? 0, job.tpmTokens ?? 0);
          this.incrementLocalTpm(tpmTokens);
        }

        // Post-completion budget check
        if (record.budgetKey && job.usage) {
          const usageTokens = job.usage.tokens ?? {};
          const usageCosts = job.usage.costs ?? {};
          const rawTotal = job.usage.totalTokens ?? 0;
          const totalCost = job.usage.totalCost ?? 0;

          if (
            rawTotal > 0 ||
            Object.keys(usageTokens).length > 0 ||
            Object.keys(usageCosts).length > 0 ||
            totalCost > 0
          ) {
            const budgetState = this.queue.budgets.get(record.budgetKey);
            const weights = budgetState?.tokenWeights ?? {};
            const weightedTotal = computeWeightedTotal(usageTokens, weights, rawTotal);

            const budgetResult = this.queue.recordBudgetUsage(
              record.budgetKey,
              usageTokens,
              usageCosts,
              weightedTotal,
              totalCost,
            );
            if (budgetResult === 'exceeded') {
              this.emit('budget-exceeded', job, record.id);
            }
          }
        }
      })
      .catch((err: Error) => {
        // Handle suspend: the job is already marked suspended by TestJob.suspend()
        if (err instanceof SuspendError || err.name === 'SuspendError') {
          this.queue.scheduleSuspendedTimeout(record);
          // State already set to 'suspended' by TestJob.suspend(). Nothing more to do.
          this.queue.emit('suspended', job, record.suspendReason);
          return;
        }
        // moveToDelayed: park without counting an attempt, promote at the timestamp.
        if (err instanceof DelayedError) {
          this.queue.parkActiveDelayed(record, Math.max(0, err.delayedUntil - Date.now()));
          return;
        }

        if (record.movedToFailed) {
          err = record.movedToFailed;
          record.movedToFailed = undefined;
        }
        this.handleFailure(record, job, err);
      })
      .finally(() => {
        this.activeCount--;
        // Try to pick up more work
        if (this.running && !this.queue.isPaused()) {
          this.processAvailable();
        }
      });
  }

  /**
   * Mirror BaseWorker.handleJobFailure + glidemq_fail: every failed attempt sets
   * failedReason and emits the worker 'failed' event. A retryable failure parks
   * the job in 'delayed' for the backoff delay (queue emits 'retrying'); a
   * terminal failure moves it to 'failed', applies removeOnFail and emits the
   * queue 'failed' event. A RateLimitError is not a failure: the job is parked
   * in 'delayed' for the limiter window with failedReason 'rate limited', no
   * attempt is consumed, and the worker pauses dispatch for the same window.
   */
  private handleFailure(record: TestJobRecord<D, R>, job: TestJob<D, R>, err: Error): void {
    const now = Date.now();
    if (TestWorker.isRateLimitError(err)) {
      const delayMs = (err as { delayMs?: number }).delayMs || this.limiter?.duration || 1000;
      this.rateLimitUntil = now + delayMs;
      record.failedReason = 'rate limited';
      job.failedReason = 'rate limited';
      record.processedOn = now;
      this.queue.parkDelayed(record, delayMs);
      this.queue.emit('retrying', job, err);
      return;
    }
    record.attemptsMade++;
    const skipRetry = job.discarded || err instanceof UnrecoverableError || err.name === 'UnrecoverableError';
    const maxAttempts = skipRetry ? 0 : (record.opts.attempts ?? 0);
    record.failedReason = err.message;
    job.failedReason = err.message;

    if (maxAttempts > 0 && record.attemptsMade < maxAttempts) {
      let backoffDelay = 0;
      const backoff = record.opts.backoff;
      if (backoff) {
        const strategyFn = this.backoffStrategies?.[backoff.type];
        backoffDelay = strategyFn
          ? strategyFn(record.attemptsMade, err)
          : calculateBackoff(backoff.type, backoff.delay, record.attemptsMade, backoff.jitter);
      }
      if (record.opts.fallbacks && record.opts.fallbacks.length > 0) {
        record.fallbackIndex++;
      }
      record.processedOn = now;
      this.queue.parkDelayed(record, backoffDelay);
      this.emit('failed', job, err);
      this.queue.emit('retrying', job, err);
      return;
    }

    record.state = 'failed';
    record.finishedOn = now;
    job.finishedOn = now;
    this.queue.applyRetention(record, 'failed');
    this.queue.recordMetric('failed', record.processedOn, record.finishedOn);
    this.moveToDeadLetter(record, job, err);
    this.emit('failed', job, err);
    this.queue.emit('failed', job, err);
    this.queue.onSchedulerJobFinished(record);
  }

  /**
   * Mirror BaseWorker.moveToDLQ: a terminal failure leaves a copy in the dead-letter
   * queue, written before the `failed` events fire. `data` is the processor's
   * `job.data`, which differs from the stored record when the processor replaced it.
   * `attemptsMade` is the count before the failing attempt, as in the worker, whose
   * Job is loaded before the attempt is counted. A write error goes to the `error`
   * event, not the job. Without an `error` listener the production worker would
   * throw an unhandled error; here it becomes a process warning, so the job outcome
   * stays intact and the failure is still visible.
   */
  private moveToDeadLetter(record: TestJobRecord<D, R>, job: TestJob<D, R>, err: Error): void {
    const dlqName = this.deadLetterQueue?.name;
    if (!dlqName) return;
    try {
      this.queue.addDeadLetter(dlqName, record.name, {
        originalQueue: this.queue.name,
        originalJobId: record.id,
        data: job.data,
        failedReason: err.message,
        attemptsMade: record.attemptsMade - 1,
      });
    } catch (dlqErr) {
      if (this.listenerCount('error') > 0) this.emit('error', dlqErr);
      else process.emitWarning(`TestWorker dead-letter write failed: ${(dlqErr as Error).message}`, 'GlideMQWarning');
    }
  }

  // ---- Batch processing ----

  private processAvailableBatch(): void {
    if (this.activeCount >= this.concurrency * this.batchSize) return;

    this.pendingBatch = this.pendingBatch.filter((r) => this.queue.jobs.has(r.id) && r.state === 'waiting');
    while (this.pendingBatch.length < this.batchSize) {
      const record = this.takeWaitingRecord();
      if (!record) break;
      this.pendingBatch.push(record);
    }

    if (this.pendingBatch.length === 0) {
      if (this.activeCount === 0 && !this.isDrained) {
        this.isDrained = true;
        this.emit('drained');
      }
      return;
    }

    if (this.pendingBatch.length >= this.batchSize) {
      this.clearBatchTimer();
      this.executeBatch(this.pendingBatch.splice(0, this.batchSize));
      if (this.pendingBatch.length > 0) this.scheduleBatchFlush();
      else if (this.queue.waitingQueue.length > 0) queueMicrotask(() => this.processAvailable());
      return;
    }

    if (this.batchTimeout > 0) {
      this.scheduleBatchFlush();
      return;
    }

    this.executeBatch(this.pendingBatch.splice(0, this.pendingBatch.length));
  }

  private clearBatchTimer(): void {
    if (this.batchTimer) {
      clearTimeout(this.batchTimer);
      this.batchTimer = null;
    }
  }

  private scheduleBatchFlush(): void {
    if (this.batchTimer || this.batchTimeout <= 0 || this.pendingBatch.length === 0) return;
    this.batchTimer = setTimeout(() => {
      this.batchTimer = null;
      this.flushBatch();
    }, this.batchTimeout);
  }

  private takeWaitingRecord(): TestJobRecord<D, R> | undefined {
    return this.findNextWaiting();
  }

  private flushBatch(): void {
    if (!this.running) return;
    this.pendingBatch = this.pendingBatch.filter((r) => this.queue.jobs.has(r.id) && r.state === 'waiting');
    if (!this.queue.isPaused() && !this.paused) {
      while (this.pendingBatch.length < this.batchSize) {
        const record = this.takeWaitingRecord();
        if (!record) break;
        this.pendingBatch.push(record);
      }
    }
    if (this.pendingBatch.length === 0) return;
    this.executeBatch(this.pendingBatch.splice(0, this.batchSize));
    if (this.pendingBatch.length > 0) this.scheduleBatchFlush();
  }

  private executeBatch(records: TestJobRecord<D, R>[]): void {
    if (!this.batchProcessor) return;

    // Mark all as active
    for (const record of records) {
      record.state = 'active';
      record.processedOn = Date.now();
    }
    this.activeCount += records.length;
    this.isDrained = false;

    const jobs = records.map((r) => new TestJob<D, R>(r));
    for (const job of jobs) {
      this.emit('active', job, job.id);
    }

    this.batchProcessor(jobs)
      .then((results) => {
        if (results.length !== records.length) {
          throw new Error(`Batch processor returned ${results.length} results but batch had ${records.length} jobs`);
        }
        const s = this.queue.serializer;
        for (let i = 0; i < records.length; i++) {
          const record = records[i];
          const job = jobs[i];
          if (this.settleMovedToFailed(record, job)) continue;
          const result = results[i];
          const roundtripped = result !== undefined ? (s.deserialize(s.serialize(result)) as R) : result;
          record.state = 'completed';
          record.returnvalue = roundtripped;
          record.finishedOn = Date.now();
          job.returnvalue = roundtripped;
          job.finishedOn = record.finishedOn;
          this.queue.applyRetention(record, 'completed');
          this.queue.recordMetric('completed', record.processedOn, record.finishedOn);
          this.emit('completed', job, roundtripped);
          this.queue.emit('completed', job, roundtripped);
          this.queue.onSchedulerJobFinished(record);
        }
      })
      .catch((err: Error) => {
        if (err instanceof BatchError || err.name === 'BatchError') {
          const batchErr = err as BatchError;
          for (let i = 0; i < records.length; i++) {
            const record = records[i];
            const job = jobs[i];
            if (this.settleMovedToFailed(record, job)) continue;
            const result = i < batchErr.results.length ? batchErr.results[i] : new Error('No result in BatchError');

            if (result instanceof Error) {
              this.handleFailure(record, job, result);
            } else {
              const s = this.queue.serializer;
              const roundtripped =
                result !== undefined ? (s.deserialize(s.serialize(result as R)) as R) : (result as R);
              record.state = 'completed';
              record.returnvalue = roundtripped;
              record.finishedOn = Date.now();
              job.returnvalue = roundtripped;
              job.finishedOn = record.finishedOn;
              this.queue.applyRetention(record, 'completed');
              this.queue.recordMetric('completed', record.processedOn, record.finishedOn);
              this.emit('completed', job, roundtripped);
              this.queue.emit('completed', job, roundtripped);
              this.queue.onSchedulerJobFinished(record);
            }
          }
        } else {
          // All jobs fail
          for (let i = 0; i < records.length; i++) {
            const record = records[i];
            const job = jobs[i];
            if (this.settleMovedToFailed(record, job)) continue;
            this.handleFailure(record, job, err);
          }
        }
      })
      .finally(() => {
        this.activeCount -= records.length;
        if (this.running && !this.queue.isPaused()) {
          this.processAvailable();
        }
      });
  }

  /**
   * Like BaseWorker.skipMovedToFailed: a job that called moveToFailed() inside
   * the processor is settled through the failure path, whatever the batch
   * outcome for it. Returns true when the job was handled here.
   */
  private settleMovedToFailed(record: TestJobRecord<D, R>, job: TestJob<D, R>): boolean {
    const err = record.movedToFailed;
    if (!err) return false;
    record.movedToFailed = undefined;
    this.handleFailure(record, job, err);
    return true;
  }

  /** Wait if the TPM counter exceeds the limit for the current window. */
  private async waitForTokenLimitIfNeeded(): Promise<void> {
    if (!this.tokenLimiter) return;
    const tl = this.tokenLimiter;

    while (true) {
      const now = Date.now();
      // Reset window if expired
      if (now >= this.tpmWindowStart + tl.duration) {
        this.tpmLocalCounter = 0;
        this.tpmWindowStart = now - (now % tl.duration);
      }
      if (this.tpmLocalCounter < tl.maxTokens) break;
      const sleepMs = this.tpmWindowStart + tl.duration - now;
      if (sleepMs <= 0) continue;
      await new Promise<void>((resolve) => setTimeout(resolve, sleepMs));
    }
  }

  /** Increment the local TPM counter after a job completes. */
  private incrementLocalTpm(tokens: number): void {
    if (tokens <= 0 || !this.tokenLimiter) return;
    const now = Date.now();
    if (now >= this.tpmWindowStart + this.tokenLimiter.duration) {
      this.tpmLocalCounter = 0;
      this.tpmWindowStart = now - (now % this.tokenLimiter.duration);
    }
    this.tpmLocalCounter += tokens;
  }

  /** Return the number of jobs currently being processed. */
  getActiveCount(): number {
    return this.activeCount;
  }

  /** Pause dispatch for the given duration, like Worker.rateLimit(ms). */
  async rateLimit(ms: number): Promise<void> {
    this.rateLimitUntil = Date.now() + ms;
  }

  /** Resolves immediately: a TestWorker has no connection to wait for. */
  async waitUntilReady(): Promise<void> {}

  /** True while the worker is open and not paused, like Worker.isRunning(). */
  isRunning(): boolean {
    return this.running && !this.paused;
  }

  isPaused(): boolean {
    return this.paused;
  }

  /** Stop taking jobs. Without `force`, resolves once the active jobs finish, like Worker.pause(). */
  async pause(force?: boolean): Promise<void> {
    this.paused = true;
    if (!force) await this.waitForActiveJobs();
  }

  /** Resume taking jobs after pause(). */
  async resume(): Promise<void> {
    this.paused = false;
    this.wake();
  }

  /** Process everything that is waiting, prioritized or delayed, then close, like Worker.drain(). */
  async drain(): Promise<void> {
    while (this.running) {
      await this.waitForActiveJobs();
      if (this.activeCount === 0 && this.queue.isDrainComplete()) break;
      await new Promise<void>((r) => setTimeout(r, 10));
    }
    await this.close();
  }

  private async waitForActiveJobs(): Promise<void> {
    while (this.activeCount > 0) {
      await new Promise<void>((r) => setTimeout(r, 5));
    }
  }

  /** Stop processing and detach from the queue. */
  async close(): Promise<void> {
    this.running = false;
    this.clearBatchTimer();
    if (this.rateLimitTimer) {
      clearTimeout(this.rateLimitTimer);
      this.rateLimitTimer = null;
    }
    if (this.pendingBatch.length > 0) {
      const handoff = this.pendingBatch.filter((r) => this.queue.jobs.has(r.id) && r.state === 'waiting');
      this.pendingBatch = [];
      if (handoff.length > 0) this.queue.waitingQueue.unshift(...handoff);
    }
    this.queue.workers.delete(this);
    this.queue.onWorkerDetached();
    for (const peer of this.queue.workers) {
      peer.onJobAdded();
    }
    this.removeAllListeners();
  }

  /** Same check as Worker.isRateLimitError: instance or `name === 'RateLimitError'`. */
  static isRateLimitError(error: Error): boolean {
    return error instanceof TestWorker.RateLimitError || error.name === 'RateLimitError';
  }

  /** Throw from a processor to requeue the job after the limiter window, like Worker.RateLimitError. */
  static RateLimitError = class extends Error {
    delayMs?: number;
    constructor() {
      super('Rate limit exceeded');
      this.name = 'RateLimitError';
    }
  };
}
