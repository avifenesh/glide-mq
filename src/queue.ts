import { EventEmitter } from 'events';
import { randomBytes } from 'crypto';
import { InfBoundary, Batch, ClusterBatch, ClusterScanCursor, GlideFt, SortOrder } from '@glidemq/speedkey';
import type { GlideClient, GlideClusterClient, Field, FtCreateOptions } from '@glidemq/speedkey';
import type {
  QueueOptions,
  JobOptions,
  AddAndWaitOptions,
  Client,
  ScheduleOpts,
  JobTemplate,
  SchedulerEntry,
  Metrics,
  MetricsOptions,
  MetricsDataPoint,
  JobCounts,
  SearchJobsOptions,
  GetJobsOptions,
  ReadStreamOptions,
  RateLimitConfig,
  WorkerInfo,
  Serializer,
  SignalEntry,
  IndexCreateOptions,
  SearchQueryOptions,
  JobIndexOptions,
  VectorSearchOptions,
  VectorSearchResult,
  UsageSummary,
  UsageSummaryOptions,
  UsageQueueSummary,
} from './types';
import { JSON_SERIALIZER } from './types';
import { Job } from './job';
import {
  buildKeys,
  keyPrefix,
  keyPrefixPattern,
  nextReconnectDelay,
  parseJsonRecord,
  validateTimezone,
  validateSchedulerEvery,
  normalizeScheduleDate,
  validateSchedulerBounds,
  computeInitialSchedulerNextRun,
  hashDataToRecord,
  hmgetArrayToRecord,
  extractJobIdsFromStreamEntries,
  compress,
  MAX_JOB_DATA_SIZE,
  JOB_METADATA_FIELDS,
  validateQueueName,
  MAX_ORDERING_KEY_LENGTH,
  validateJobId,
  floorUsageBucket,
  usageQueuesKey,
  USAGE_BUCKET_MS,
  USAGE_RETENTION_MS,
} from './utils';
import {
  createBlockingClient,
  createClient,
  ensureFunctionLibrary,
  ensureFunctionLibraryOnce,
  isClusterClient,
} from './connection';
import { GlideMQError } from './errors';
import {
  LIBRARY_SOURCE,
  CONSUMER_GROUP,
  addJob,
  dedup,
  pause,
  resume,
  revokeJob,
  tryLock,
  unlock,
  searchByName,
  cleanJobs,
  drainQueue,
  retryJobs,
  removeJob,
  rateLimitGroupExternal,
  signalJob,
  sweepSuspended,
  getActiveListJobIds,
} from './functions/index';
import type { QueueKeys } from './functions/index';
import { withSpan } from './telemetry';

const PIPELINE_CHUNK_SIZE = 1000;
const SCHEDULER_LOCK_TTL_MS = 5000;
const SCHEDULER_LOCK_RETRY_DELAY_MS = 25;
const SCHEDULER_LOCK_MAX_ATTEMPTS = Math.ceil(SCHEDULER_LOCK_TTL_MS / SCHEDULER_LOCK_RETRY_DELAY_MS);
const SUSPEND_SWEEP_INTERVAL_MS = 1000;
const SUSPEND_SWEEP_IDLE_POLL_MS = 5000;
const SUSPEND_SWEEP_LOCK_TTL_MS = 5000;
const SUSPEND_NO_TIMEOUT_SCORE = 9_999_999_999_999;
const DEFAULT_USAGE_WINDOW_MS = 60 * 60 * 1000;
const MAX_USAGE_SUMMARY_KEY_READS = 100_000;
const USAGE_MODEL_FIELD_PREFIX = 'models:';
const USAGE_TOKEN_FIELD_PREFIX = 'tokens:';
const USAGE_COST_FIELD_PREFIX = 'costs:';

type UsageAccumulator = Pick<
  UsageSummary,
  'jobCount' | 'tokens' | 'totalTokens' | 'costs' | 'totalCost' | 'costUnit' | 'models'
>;

function newClientBatch(client: Client): InstanceType<typeof Batch> | InstanceType<typeof ClusterBatch> {
  return isClusterClient(client) ? new ClusterBatch(false) : new Batch(false);
}

function parseUsageNumber(raw: string | undefined): number {
  if (raw == null) return 0;
  const value = parseFloat(raw);
  return Number.isFinite(value) ? value : 0;
}

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

function createUsageSummaryResult(startTime: number, endTime: number): UsageSummary {
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

function mergeUsageBucketFields(target: UsageAccumulator, fields: Record<string, string>): void {
  target.jobCount += parseUsageNumber(fields.jobCount);
  target.totalTokens += parseUsageNumber(fields.totalTokens);
  target.totalCost += parseUsageNumber(fields.totalCost);

  if (fields.costUnit && !target.costUnit) {
    target.costUnit = fields.costUnit;
  }

  for (const [field, rawValue] of Object.entries(fields)) {
    const value = parseUsageNumber(rawValue);
    if (!Number.isFinite(value) || value === 0) continue;

    if (field.startsWith(USAGE_MODEL_FIELD_PREFIX)) {
      const model = field.slice(USAGE_MODEL_FIELD_PREFIX.length);
      if (model) target.models[model] = (target.models[model] || 0) + value;
      continue;
    }

    if (field.startsWith(USAGE_TOKEN_FIELD_PREFIX)) {
      const tokenKey = field.slice(USAGE_TOKEN_FIELD_PREFIX.length);
      if (tokenKey) target.tokens[tokenKey] = (target.tokens[tokenKey] || 0) + value;
      continue;
    }

    if (field.startsWith(USAGE_COST_FIELD_PREFIX)) {
      const costKey = field.slice(USAGE_COST_FIELD_PREFIX.length);
      if (costKey) target.costs[costKey] = (target.costs[costKey] || 0) + value;
    }
  }
}

function hasUsageBucketData(fields: Record<string, string>): boolean {
  for (const [field, rawValue] of Object.entries(fields)) {
    if (field === 'costUnit') continue;
    if (parseUsageNumber(rawValue) !== 0) return true;
  }
  return false;
}

function resolveUsageWindow(opts?: UsageSummaryOptions): {
  startTime: number;
  endTime: number;
  bucketStarts: number[];
} {
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

  const bucketStarts: number[] = [];
  for (let ts = floorUsageBucket(startTime); ts <= floorUsageBucket(endTime); ts += USAGE_BUCKET_MS) {
    bucketStarts.push(ts);
  }

  return { startTime, endTime, bucketStarts };
}

function validateOrderingKey(orderingKey: string): void {
  if (orderingKey.length > MAX_ORDERING_KEY_LENGTH) {
    throw new Error(`Ordering key exceeds maximum length (${orderingKey.length} > ${MAX_ORDERING_KEY_LENGTH}).`);
  }
  if (orderingKey === '__') {
    throw new Error("Ordering key '__' is reserved as an internal sentinel.");
  }
}

/** Check if all key-value pairs in filter exist in data (shallow match). */
function matchesData(data: Record<string, unknown>, filter: Record<string, unknown>): boolean {
  for (const [key, value] of Object.entries(filter)) {
    if (data[key] !== value) return false;
  }
  return true;
}

/** Map glide-mq IndexCreateOptions to speedkey FtCreateOptions (minus dataType/prefixes). */
function mapIndexCreateOptions(opts: IndexCreateOptions): Omit<FtCreateOptions, 'dataType' | 'prefixes'> {
  return { ...opts };
}

/** Map glide-mq SearchQueryOptions to speedkey FtSearchOptions format. */
function mapSearchQueryOptions(opts: SearchQueryOptions): Record<string, any> {
  const result: Record<string, any> = { ...opts };
  if (opts.sortby) {
    result.sortby = {
      field: opts.sortby.field,
      ...(opts.sortby.order ? { order: opts.sortby.order === 'ASC' ? SortOrder.ASC : SortOrder.DESC } : {}),
    };
  }
  return result;
}

/**
 * Queue manages job submission, retrieval, scheduling, and lifecycle operations.
 * Connects to Valkey/Redis and uses server-side functions for atomic operations.
 */
export class Queue<D = any, R = any> extends EventEmitter {
  readonly name: string;
  private opts: QueueOptions;
  private client: Client | null = null;
  private clientInitPromise: Promise<Client> | null = null;
  private clientOwned = true;
  private queueMetaConfigSynced = false;
  private _clusterMode: boolean | undefined;
  private closing = false;
  private waitClients: Set<Client> = new Set();
  private waitRejectors: Set<(err: Error) => void> = new Set();
  private keys: QueueKeys;
  private suspendedSweepTimer: ReturnType<typeof setTimeout> | null = null;
  private suspendedSweepWakeAt = 0;
  private suspendedSweepInFlight = false;
  private suspendedSweepQueued = false;
  private suspendedSweepTasks: Set<Promise<void>> = new Set();

  private serializer: Serializer;
  private skipEvents: boolean;
  private searchModuleAvailable: boolean | null = null;

  constructor(name: string, opts: QueueOptions) {
    super();
    if (!opts.connection && !opts.client) {
      throw new GlideMQError('Either `connection` or `client` must be provided.');
    }
    validateQueueName(name);
    this.name = name;
    this.opts = opts;
    this.serializer = opts.serializer ?? JSON_SERIALIZER;
    this.skipEvents = opts.events === false;
    this.keys = buildKeys(name, opts.prefix);
    if (opts.connection) {
      this._clusterMode = opts.connection.clusterMode ?? false;
    }
  }

  /** Resolve clusterMode (cached after first resolution). */
  private get clusterMode(): boolean {
    if (this._clusterMode !== undefined) return this._clusterMode;
    if (this.client) {
      this._clusterMode = isClusterClient(this.client);
      return this._clusterMode;
    }
    return false;
  }

  /** @internal Create a Batch appropriate for the client type. */
  private newBatch(): InstanceType<typeof Batch> | InstanceType<typeof ClusterBatch> {
    return this.clusterMode ? new ClusterBatch(false) : new Batch(false);
  }

  private schedulerLockKey(): string {
    return `${this.keys.schedulers}:lock:__tick__`;
  }

  private async acquireSchedulerMutationLock(client: Client): Promise<{ key: string; token: string }> {
    const key = this.schedulerLockKey();
    for (let attempt = 0; attempt < SCHEDULER_LOCK_MAX_ATTEMPTS; attempt++) {
      const token = randomBytes(8).toString('hex');
      const acquired = await tryLock(client, key, token, SCHEDULER_LOCK_TTL_MS);
      if (acquired) return { key, token };
      await new Promise<void>((resolve) => setTimeout(resolve, SCHEDULER_LOCK_RETRY_DELAY_MS));
    }
    throw new GlideMQError('Scheduler is busy; try again');
  }

  private async releaseSchedulerMutationLock(client: Client, lock: { key: string; token: string }): Promise<void> {
    try {
      await unlock(client, lock.key, lock.token);
    } catch {
      // Best effort - the lock has a TTL fallback.
    }
  }

  /**
   * @internal Resolve active job IDs from pending entry IDs via pipelined xrange.
   * Replaces the N+1 serial xrange pattern with a single Batch pipeline.
   */
  private async resolveActiveJobIds(client: Client, entryIds: string[]): Promise<string[]> {
    if (entryIds.length === 0) return [];
    const batch = this.newBatch();
    for (const eid of entryIds) {
      (batch as any).xrange(this.keys.stream, { value: eid }, { value: eid }, 1);
    }
    const results = await client.exec(batch as any, false);
    const jobIds: string[] = [];
    if (!results) return jobIds;
    // Speedkey's batch xrange returns Array<{ key: string; value: [field, value][] }>
    // per entry. The standalone xrange returns Record<entryId, fields>. Handle both.
    for (const result of results) {
      if (!result) continue;
      if (Array.isArray(result)) {
        for (const entry of result as Array<{ key?: unknown; value?: [unknown, unknown][] }>) {
          const fieldPairs = entry?.value;
          if (!Array.isArray(fieldPairs)) continue;
          for (const [field, value] of fieldPairs) {
            if (String(field) === 'jobId') {
              jobIds.push(String(value));
              break;
            }
          }
        }
      } else if (typeof result === 'object') {
        jobIds.push(...extractJobIdsFromStreamEntries(result as any));
      }
    }
    return jobIds;
  }

  private suspendSweepLockKey(): string {
    return `${this.keys.suspended}:lock:__sweep__`;
  }

  private shouldStartSuspendSweepLoop(client: Client): boolean {
    return typeof (client as any)?.fcall === 'function';
  }

  private ensureSuspendSweepLoop(client: Client): void {
    if (
      this.suspendedSweepTimer ||
      this.suspendedSweepInFlight ||
      this.closing ||
      !this.shouldStartSuspendSweepLoop(client)
    ) {
      return;
    }

    this.scheduleNextSuspendSweep(client, SUSPEND_SWEEP_INTERVAL_MS);
  }

  private clearSuspendSweepLoop(): void {
    if (this.suspendedSweepTimer) {
      clearTimeout(this.suspendedSweepTimer);
      this.suspendedSweepTimer = null;
    }
    this.suspendedSweepWakeAt = 0;
    this.suspendedSweepQueued = false;
    if (this.suspendedSweepTasks.size === 0) {
      this.suspendedSweepInFlight = false;
    }
  }

  private trackSuspendSweepTask(task: Promise<void>): Promise<void> {
    this.suspendedSweepTasks.add(task);
    void task.finally(() => {
      this.suspendedSweepTasks.delete(task);
    });
    return task;
  }

  private scheduleNextSuspendSweep(client: Client, delayMs: number): void {
    if (this.closing) return;

    const delay = Math.max(0, Math.trunc(delayMs));
    const target = Date.now() + delay;
    if (this.suspendedSweepTimer && this.suspendedSweepWakeAt > 0 && this.suspendedSweepWakeAt <= target) {
      return;
    }

    if (this.suspendedSweepTimer) {
      clearTimeout(this.suspendedSweepTimer);
      this.suspendedSweepTimer = null;
    }

    this.suspendedSweepWakeAt = target;
    this.suspendedSweepTimer = setTimeout(() => {
      this.suspendedSweepTimer = null;
      this.suspendedSweepWakeAt = 0;
      this.runSuspendSweep(client);
    }, delay);
    this.suspendedSweepTimer.unref?.();
  }

  private async nextSuspendedSweepDelay(client: Client, acquiredLock: boolean): Promise<number> {
    if (!acquiredLock) {
      return SUSPEND_SWEEP_INTERVAL_MS;
    }
    const nextDue = await this.nextSuspendedDueAt(client);
    if (nextDue == null || nextDue >= SUSPEND_NO_TIMEOUT_SCORE) {
      // Keep a low-frequency poll alive so this queue can pick up new suspended
      // timeouts created by other processes after it becomes idle.
      return SUSPEND_SWEEP_IDLE_POLL_MS;
    }
    const delta = nextDue - Date.now();
    if (delta <= 0) {
      return SUSPEND_SWEEP_INTERVAL_MS;
    }
    return Math.min(SUSPEND_SWEEP_IDLE_POLL_MS, delta);
  }

  private async nextSuspendedDueAt(client: Client): Promise<number | null> {
    const members = await client.zrange(this.keys.suspended, { start: 0, end: 0 });
    const firstMember = Array.isArray(members) ? members[0] : null;
    if (firstMember == null) {
      return null;
    }
    const score = await client.zscore(this.keys.suspended, firstMember);
    if (score == null) {
      return null;
    }
    const numericScore = typeof score === 'number' ? score : Number(score);
    return Number.isFinite(numericScore) ? numericScore : null;
  }

  private runSuspendSweep(client: Client): void {
    if (this.closing) return;
    if (this.suspendedSweepInFlight) {
      this.suspendedSweepQueued = true;
      return;
    }

    this.suspendedSweepInFlight = true;
    void this.trackSuspendSweepTask(
      (async () => {
        let acquiredLock = false;
        try {
          acquiredLock = await this.sweepExpiredSuspended(client);
        } catch (err) {
          if (this.listenerCount('error') > 0) {
            this.emit('error', err instanceof Error ? err : new Error(String(err)));
          }
        } finally {
          this.suspendedSweepInFlight = false;
          if (this.closing) return;
          if (this.suspendedSweepQueued) {
            this.suspendedSweepQueued = false;
            this.scheduleNextSuspendSweep(client, 0);
            return;
          }
          try {
            const delay = await this.nextSuspendedSweepDelay(client, acquiredLock);
            this.scheduleNextSuspendSweep(client, delay);
          } catch (err) {
            if (this.listenerCount('error') > 0) {
              this.emit('error', err instanceof Error ? err : new Error(String(err)));
            }
            this.scheduleNextSuspendSweep(client, SUSPEND_SWEEP_INTERVAL_MS);
          }
        }
      })(),
    );
  }

  private async sweepExpiredSuspended(client: Client): Promise<boolean> {
    const lockKey = this.suspendSweepLockKey();
    const token = randomBytes(8).toString('hex');
    const acquired = await tryLock(client, lockKey, token, SUSPEND_SWEEP_LOCK_TTL_MS);
    if (!acquired) return false;

    try {
      const keyPrefix = this.keys.id.slice(0, -2);
      await sweepSuspended(client, this.keys, Date.now(), keyPrefix);
      return true;
    } finally {
      try {
        await unlock(client, lockKey, token);
      } catch {
        // Best effort - the TTL ensures the sweep lock eventually expires.
      }
    }
  }

  /** @internal */
  async getClient(): Promise<Client> {
    if (this.closing) {
      throw new GlideMQError('Queue is closing');
    }
    if (!this.client) {
      if (!this.clientInitPromise) {
        this.clientInitPromise = (async () => {
          if (this.opts.client) {
            const injected = this.opts.client;
            try {
              await ensureFunctionLibraryOnce(
                injected,
                LIBRARY_SOURCE,
                this.opts.connection?.clusterMode ?? isClusterClient(injected),
              );
            } catch (err) {
              this.emit('error', err);
              throw err;
            }
            this.client = injected;
            this.clientOwned = false;
            return injected;
          }

          let client: Client;
          try {
            client = await createClient(this.opts.connection!);
            await ensureFunctionLibrary(client, LIBRARY_SOURCE, this.opts.connection!.clusterMode ?? false);
          } catch (err) {
            // Don't cache a failed client - next getClient() call will retry
            this.emit('error', err);
            throw err;
          }
          this.client = client;
          this.clientOwned = true;
          return client;
        })().finally(() => {
          this.clientInitPromise = null;
        });
      }
      await this.clientInitPromise;
    }
    const client = this.client;
    if (!client) {
      throw new GlideMQError('Queue client initialization failed');
    }
    if (!this.queueMetaConfigSynced) {
      await this.syncQueueMetaConfig(client);
      this.queueMetaConfigSynced = true;
    }
    this.ensureSuspendSweepLoop(client);
    return client;
  }

  private async syncQueueMetaConfig(client: Client): Promise<void> {
    if (!this.opts.deadLetterQueue?.name) return;
    await client.hset(this.keys.meta, {
      deadLetterQueueName: this.opts.deadLetterQueue.name,
    });
  }

  private async resolveDeadLetterQueueName(client: Client): Promise<string | null> {
    if (this.opts.deadLetterQueue?.name) {
      return this.opts.deadLetterQueue.name;
    }
    const stored = await client.hget(this.keys.meta, 'deadLetterQueueName');
    return stored ? String(stored) : null;
  }

  private async loadJobsByIdsForKeys<TD = D, TR = R>(
    client: Client,
    queueKeys: QueueKeys,
    jobIds: string[],
    opts?: { excludeData?: boolean; serializer?: Serializer },
  ): Promise<Job<TD, TR>[]> {
    if (jobIds.length === 0) return [];

    const excludeData = opts?.excludeData === true;
    const jobs: Job<TD, TR>[] = [];

    for (let offset = 0; offset < jobIds.length; offset += PIPELINE_CHUNK_SIZE) {
      const chunk = jobIds.slice(offset, offset + PIPELINE_CHUNK_SIZE);
      const batch = this.newBatch();
      if (excludeData) {
        for (const id of chunk) (batch as any).hmget(queueKeys.job(id), JOB_METADATA_FIELDS);
      } else {
        for (const id of chunk) (batch as any).hgetall(queueKeys.job(id));
      }
      const batchResults = await client.exec(batch as any, false);
      if (!batchResults) continue;

      for (let i = 0; i < batchResults.length; i++) {
        const hash = excludeData
          ? hmgetArrayToRecord(batchResults[i] as (unknown | null)[], JOB_METADATA_FIELDS)
          : hashDataToRecord(batchResults[i] as any);
        if (!hash) continue;
        jobs.push(Job.fromHash<TD, TR>(client, queueKeys, chunk[i], hash, opts?.serializer, excludeData));
      }
    }

    return jobs;
  }

  /**
   * Add a single job to the queue.
   * Uses the glidemq_addJob server function to atomically create the job hash
   * and enqueue it to the stream (or scheduled ZSet if delayed/prioritized).
   */
  async add(name: string, data: D, opts?: JobOptions): Promise<Job<D, R> | null> {
    const delay = opts?.delay ?? 0;
    const priority = opts?.priority ?? 0;
    if (priority > 2048) {
      throw new Error('Priority must be <= 2048');
    }

    return withSpan('glide-mq.queue.add', async (span) => {
      span.setAttribute('glide-mq.queue', this.name);
      span.setAttribute('glide-mq.job.name', name);
      span.setAttribute('glide-mq.job.delay', delay);
      span.setAttribute('glide-mq.job.priority', priority);
      const client = await this.getClient();
      const timestamp = Date.now();
      const parentId = opts?.parent ? opts.parent.id : '';
      const parentQueue = opts?.parent ? opts.parent.queue : '';
      const maxAttempts = opts?.attempts ?? 0;
      const orderingKey = opts?.ordering?.key ?? '';
      const groupRateMax = opts?.ordering?.rateLimit?.max ?? 0;
      const groupRateDuration = opts?.ordering?.rateLimit?.duration ?? 0;
      const tb = opts?.ordering?.tokenBucket;
      let tbCapacity = 0;
      let tbRefillRate = 0;
      if (tb) {
        if (!Number.isFinite(tb.capacity) || tb.capacity <= 0)
          throw new Error('tokenBucket.capacity must be a positive finite number');
        if (!Number.isFinite(tb.refillRate) || tb.refillRate <= 0)
          throw new Error('tokenBucket.refillRate must be a positive finite number');
        tbCapacity = Math.round(tb.capacity * 1000);
        tbRefillRate = Math.round(tb.refillRate * 1000);
      }
      let jobCost = 0;
      if (opts?.cost != null) {
        if (!Number.isFinite(opts.cost) || opts.cost < 0) throw new Error('cost must be a non-negative finite number');
        jobCost = Math.round(opts.cost * 1000);
      }
      let groupConcurrency = opts?.ordering?.concurrency ?? 0;
      if (orderingKey && groupConcurrency < 1) {
        groupConcurrency = 1;
      }
      validateOrderingKey(orderingKey);

      if (opts?.lifo && orderingKey) {
        throw new Error('lifo and ordering.key cannot be used together');
      }
      const lifo = opts?.lifo ?? false;

      const customJobId = opts?.jobId ?? '';
      if (customJobId !== '') validateJobId(customJobId);

      if (opts?.ttl != null) {
        if (!Number.isFinite(opts.ttl) || opts.ttl < 0) throw new Error('ttl must be a non-negative finite number');
      }

      // Payload size validation - prevent DoS via oversized jobs
      let serialized = this.serializer.serialize(data);
      // UTF-8 worst case: 4 bytes per char. Skip Buffer.byteLength for small strings.
      if (serialized.length > MAX_JOB_DATA_SIZE / 4) {
        const byteLen = Buffer.byteLength(serialized, 'utf8');
        if (byteLen > MAX_JOB_DATA_SIZE) {
          throw new Error(
            `Job data exceeds maximum size (${byteLen} bytes > ${MAX_JOB_DATA_SIZE} bytes). Use smaller payloads or store large data externally.`,
          );
        }
      }

      if (this.opts.compression === 'gzip') {
        serialized = compress(serialized);
      }

      let jobId: string;

      const ttl = opts?.ttl ?? 0;

      // Compute parent deps key for same-queue parent (atomic SADD in Lua)
      let parentDepsKey = '';
      if (parentId && parentQueue && parentQueue === this.name) {
        parentDepsKey = this.keys.deps(parentId);
      }

      if (opts?.deduplication) {
        const dedupOpts = opts.deduplication;
        const result = await dedup(
          client,
          this.keys,
          dedupOpts.id,
          dedupOpts.ttl ?? 0,
          dedupOpts.mode ?? 'simple',
          name,
          serialized,
          JSON.stringify(opts),
          timestamp,
          delay,
          priority,
          parentId,
          maxAttempts,
          orderingKey,
          groupConcurrency,
          groupRateMax,
          groupRateDuration,
          tbCapacity,
          tbRefillRate,
          jobCost,
          ttl,
          customJobId,
          lifo ? 1 : 0,
          parentQueue,
          parentDepsKey,
          this.skipEvents,
        );
        if (result === 'skipped' || result === 'duplicate') {
          return null;
        }
        if (result === 'ERR:COST_EXCEEDS_CAPACITY') {
          throw new Error('Job cost exceeds token bucket capacity');
        }
        if (result === 'ERR:ID_EXHAUSTED') {
          throw new Error('Failed to generate job ID: too many collisions with custom job IDs');
        }
        jobId = result;

        // Cross-queue parent: register dedup child in parent deps separately
        if (parentId && parentQueue && parentQueue !== this.name) {
          const parentKeys = buildKeys(parentQueue, this.opts.prefix);
          const prefix = keyPrefix(this.opts.prefix ?? 'glide', this.name);
          const depsMember = `${prefix}:${jobId}`;
          await client.sadd(parentKeys.deps(parentId), [depsMember]);
        }
      } else {
        const result = await addJob(
          client,
          this.keys,
          name,
          serialized,
          opts != null ? JSON.stringify(opts) : '{}',
          timestamp,
          delay,
          priority,
          parentId,
          maxAttempts,
          orderingKey,
          groupConcurrency,
          groupRateMax,
          groupRateDuration,
          tbCapacity,
          tbRefillRate,
          jobCost,
          ttl,
          customJobId,
          lifo ? 1 : 0,
          parentQueue,
          parentDepsKey,
          '',
          this.skipEvents,
        );
        if (result === 'duplicate') {
          return null;
        }
        if (result === 'ERR:COST_EXCEEDS_CAPACITY') {
          throw new Error('Job cost exceeds token bucket capacity');
        }
        if (result === 'ERR:ID_EXHAUSTED') {
          throw new Error('Failed to generate job ID: too many collisions with custom job IDs');
        }
        jobId = result;

        // Cross-queue parent: register child in parent deps separately
        if (parentId && parentQueue && parentQueue !== this.name) {
          const parentKeys = buildKeys(parentQueue, this.opts.prefix);
          const prefix = keyPrefix(this.opts.prefix ?? 'glide', this.name);
          const depsMember = `${prefix}:${jobId}`;
          await client.sadd(parentKeys.deps(parentId), [depsMember]);
        }
      }

      span.setAttribute('glide-mq.job.id', String(jobId));

      const job = new Job<D, R>(client, this.keys, String(jobId), name, data, opts ?? {}, this.serializer);
      job.timestamp = timestamp;
      job.parentId = parentId || undefined;
      job.parentQueue = parentQueue || undefined;
      return job;
    });
  }

  /**
   * Add a job and wait for its completed/failed event using the queue events stream.
   * Captures the current tail entry ID before enqueue so fast completions are not missed.
   */
  async addAndWait(name: string, data: D, opts?: AddAndWaitOptions): Promise<R> {
    if (!this.opts.connection) {
      throw new GlideMQError(
        'Queue.addAndWait requires `connection` because it uses a dedicated blocking connection to wait for events.',
      );
    }

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
    const client = await this.getClient();
    const eventCursor = await this.latestEventCursor(client);
    const blockingClient = await createBlockingClient(this.opts.connection!);
    if (this.closing) {
      blockingClient.close();
      throw new GlideMQError('Queue is closing');
    }
    this.waitClients.add(blockingClient);
    try {
      const job = await this.add(name, data, jobOpts);
      if (!job) {
        throw new GlideMQError(
          'Queue.addAndWait() cannot wait on a deduplicated/skipped/duplicate-ID add that returned null.',
        );
      }
      if (this.closing) {
        throw new GlideMQError('Queue is closing');
      }
      // Ownership of blockingClient transfers to waitForJobResult, which
      // handles cleanup (including reconnection) in its own finally block.
      // The await ensures that if waitForJobResult rejects, the error
      // propagates through this try/catch for proper handling.
      return await this.waitForJobResult(blockingClient, job.id, eventCursor, waitTimeout);
    } catch (err) {
      // Re-throw after ensuring cleanup runs in finally
      throw err;
    } finally {
      // waitForJobResult may have reconnected (replacing the original client),
      // so it manages its own client lifecycle. We only clean up here if the
      // client is still registered (i.e., waitForJobResult never ran or the
      // original client was never replaced).
      if (this.waitClients.has(blockingClient)) {
        this.waitClients.delete(blockingClient);
        try {
          blockingClient.close();
        } catch (closeErr) {
          if (this.listenerCount('error') > 0) {
            this.emit('error', closeErr as Error);
          }
        }
      }
    }
  }

  /**
   * Add multiple jobs to the queue in a pipeline.
   * Uses GLIDE's Batch API to pipeline all addJob FCALL commands in a single round trip.
   * Non-atomic: each job is independent, but all are sent together for efficiency.
   */
  async addBulk(jobs: { name: string; data: D; opts?: JobOptions }[]): Promise<Job<D, R>[]> {
    if (jobs.length === 0) return [];

    const client = await this.getClient();
    const isCluster = this.clusterMode;
    const timestamp = Date.now();

    // Prepare job metadata for each entry
    const prepared = jobs.map((entry) => {
      const opts = entry.opts ?? {};
      const delay = opts.delay ?? 0;
      const priority = opts.priority ?? 0;
      const parentId = opts.parent ? opts.parent.id : '';
      const parentQueue = opts.parent ? opts.parent.queue : '';
      const maxAttempts = opts.attempts ?? 0;
      const orderingKey = opts.ordering?.key ?? '';
      validateOrderingKey(orderingKey);

      if (opts.lifo && orderingKey) {
        throw new Error('lifo and ordering.key cannot be used together');
      }
      if (opts.ttl != null) {
        if (!Number.isFinite(opts.ttl) || opts.ttl < 0) throw new Error('ttl must be a non-negative finite number');
      }
      const deduplication = opts.deduplication;
      const customJobId = opts.jobId ?? '';
      if (customJobId !== '') validateJobId(customJobId);

      let serializedData = this.serializer.serialize(entry.data);
      if (serializedData.length > MAX_JOB_DATA_SIZE / 4) {
        const byteLen = Buffer.byteLength(serializedData, 'utf8');
        if (byteLen > MAX_JOB_DATA_SIZE) {
          throw new Error(
            `Job data exceeds maximum size (${byteLen} bytes > ${MAX_JOB_DATA_SIZE} bytes). Use smaller payloads or store large data externally.`,
          );
        }
      }
      if (this.opts.compression === 'gzip') {
        serializedData = compress(serializedData);
      }

      const groupRateMax = opts.ordering?.rateLimit?.max ?? 0;
      const groupRateDuration = opts.ordering?.rateLimit?.duration ?? 0;
      const bulkTb = opts.ordering?.tokenBucket;
      let tbCapacity = 0;
      let tbRefillRate = 0;
      if (bulkTb) {
        if (!Number.isFinite(bulkTb.capacity) || bulkTb.capacity <= 0)
          throw new Error('tokenBucket.capacity must be a positive finite number');
        if (!Number.isFinite(bulkTb.refillRate) || bulkTb.refillRate <= 0)
          throw new Error('tokenBucket.refillRate must be a positive finite number');
        tbCapacity = Math.round(bulkTb.capacity * 1000);
        tbRefillRate = Math.round(bulkTb.refillRate * 1000);
      }
      let jobCost = 0;
      if (opts.cost != null) {
        if (!Number.isFinite(opts.cost) || opts.cost < 0) throw new Error('cost must be a non-negative finite number');
        jobCost = Math.round(opts.cost * 1000);
      }
      let groupConcurrency = opts.ordering?.concurrency ?? 0;
      if (orderingKey && groupConcurrency < 1) {
        groupConcurrency = 1;
      }
      return {
        entry,
        opts,
        delay,
        priority,
        parentId,
        parentQueue,
        maxAttempts,
        orderingKey,
        groupConcurrency,
        groupRateMax,
        groupRateDuration,
        tbCapacity,
        tbRefillRate,
        jobCost,
        ttl: opts.ttl ?? 0,
        deduplication,
        serializedData,
        customJobId,
        lifo: opts.lifo ?? false,
      };
    });

    // Build a batch with all fcall commands
    const keys = [this.keys.id, this.keys.stream, this.keys.scheduled, this.keys.events];
    const dedupKeys = [this.keys.dedup, this.keys.id, this.keys.stream, this.keys.scheduled, this.keys.events];
    const batch = isCluster ? new ClusterBatch(false) : new Batch(false);

    for (const p of prepared) {
      if (p.deduplication) {
        let dKeys = dedupKeys;
        if (p.parentId && p.parentQueue && p.parentQueue === this.name) {
          dKeys = [...dedupKeys, this.keys.deps(p.parentId)];
        }
        batch.fcall('glidemq_dedup', dKeys, [
          p.deduplication.id,
          String(p.deduplication.ttl ?? 0),
          p.deduplication.mode ?? 'simple',
          p.entry.name,
          p.serializedData,
          JSON.stringify(p.opts),
          timestamp.toString(),
          p.delay.toString(),
          p.priority.toString(),
          p.parentId,
          p.maxAttempts.toString(),
          p.orderingKey,
          p.groupConcurrency.toString(),
          p.groupRateMax.toString(),
          p.groupRateDuration.toString(),
          p.tbCapacity.toString(),
          p.tbRefillRate.toString(),
          p.jobCost.toString(),
          p.ttl.toString(),
          p.customJobId,
          String(p.lifo ? 1 : 0),
          p.parentQueue,
          this.skipEvents ? '1' : '0',
        ]);
      } else {
        let jobKeys = keys;
        if (p.parentId && p.parentQueue && p.parentQueue === this.name) {
          jobKeys = [...keys, this.keys.deps(p.parentId)];
        }
        batch.fcall('glidemq_addJob', jobKeys, [
          p.entry.name,
          p.serializedData,
          JSON.stringify(p.opts),
          timestamp.toString(),
          p.delay.toString(),
          p.priority.toString(),
          p.parentId,
          p.maxAttempts.toString(),
          p.orderingKey,
          p.groupConcurrency.toString(),
          p.groupRateMax.toString(),
          p.groupRateDuration.toString(),
          p.tbCapacity.toString(),
          p.tbRefillRate.toString(),
          p.jobCost.toString(),
          p.ttl.toString(),
          p.customJobId,
          String(p.lifo ? 1 : 0),
          p.parentQueue,
          '',
          this.skipEvents ? '1' : '0',
        ]);
      }
    }

    const rawResults = isCluster
      ? await (client as GlideClusterClient).exec(batch as ClusterBatch, true)
      : await (client as GlideClient).exec(batch as Batch, true);

    const builtJobs = await this.buildBulkJobs(client, prepared, rawResults, timestamp);

    // Handle cross-queue parent deps registration (non-atomic, after batch)
    for (let i = 0; i < prepared.length; i++) {
      const p = prepared[i];
      if (p.parentId && p.parentQueue && p.parentQueue !== this.name && builtJobs[i]) {
        const parentKeys = buildKeys(p.parentQueue, this.opts.prefix);
        const prefix = keyPrefix(this.opts.prefix ?? 'glide', this.name);
        const depsMember = `${prefix}:${builtJobs[i].id}`;
        await client.sadd(parentKeys.deps(p.parentId), [depsMember]);
      }
    }

    return builtJobs;
  }

  private async latestEventCursor(client: Client): Promise<string> {
    const entries = await client.xrevrange(
      this.keys.events,
      InfBoundary.PositiveInfinity,
      InfBoundary.NegativeInfinity,
      { count: 1 },
    );
    const latestId = Object.keys(entries ?? {})[0];
    return latestId ?? '0-0';
  }

  private async waitForJobResult(
    blockingClient: Client,
    jobId: string,
    lastId: string,
    waitTimeout: number,
  ): Promise<R> {
    let cursor = lastId;
    const deadline = Date.now() + waitTimeout;
    let reconnectBackoff = 0;
    let clientClosed = false;
    let rejectOnClose: ((err: Error) => void) | null = null;
    const closePromise = new Promise<never>((_, reject) => {
      rejectOnClose = reject;
      this.waitRejectors.add(reject);
    });

    try {
      while (Date.now() < deadline) {
        let result;
        try {
          const remaining = Math.max(1, deadline - Date.now());
          result = await Promise.race([
            blockingClient.xread({ [this.keys.events]: cursor }, { block: remaining, count: 100 }),
            closePromise,
          ]);
          reconnectBackoff = 0;
        } catch (err) {
          this.waitClients.delete(blockingClient);
          try {
            blockingClient.close();
          } catch (closeErr) {
            if (this.listenerCount('error') > 0) {
              this.emit('error', closeErr as Error);
            }
          }
          clientClosed = true;
          if (this.closing) {
            throw new GlideMQError('Queue is closing');
          }
          const remaining = deadline - Date.now();
          if (remaining <= 0) {
            throw err;
          }
          const delay = Math.min(nextReconnectDelay(reconnectBackoff), remaining);
          reconnectBackoff = delay;
          await Promise.race([new Promise<void>((resolve) => setTimeout(resolve, delay)), closePromise]);
          if (this.closing) {
            throw new GlideMQError('Queue is closing');
          }
          while (true) {
            try {
              blockingClient = await createBlockingClient(this.opts.connection!);
              this.waitClients.add(blockingClient);
              clientClosed = false;
              break;
            } catch (createErr) {
              const reconnectRemaining = deadline - Date.now();
              if (this.closing) {
                throw new GlideMQError('Queue is closing');
              }
              if (reconnectRemaining <= 0) {
                throw createErr;
              }
              const reconnectDelay = Math.min(nextReconnectDelay(reconnectBackoff), reconnectRemaining);
              reconnectBackoff = reconnectDelay;
              await Promise.race([new Promise<void>((resolve) => setTimeout(resolve, reconnectDelay)), closePromise]);
              if (this.closing) {
                throw new GlideMQError('Queue is closing');
              }
            }
          }
          continue;
        }

        if (!result) break;

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

            cursor = String(entryId);
            if (!eventType || payload.jobId !== jobId) continue;

            if (eventType === 'completed') {
              const raw = payload.returnvalue ?? 'null';
              return this.serializer.deserialize(raw) as R;
            }

            if (eventType === 'failed' || eventType === 'expired' || eventType === 'revoked') {
              throw new Error(payload.failedReason || `Job ${jobId} ended with event '${eventType}'`);
            }
          }
        }
      }
    } finally {
      if (rejectOnClose) {
        this.waitRejectors.delete(rejectOnClose);
      }
      if (!clientClosed) {
        this.waitClients.delete(blockingClient);
        try {
          blockingClient.close();
        } catch (closeErr) {
          if (this.listenerCount('error') > 0) {
            this.emit('error', closeErr as Error);
          }
        }
      }
    }

    const client = await this.getClient();
    const state = await client.hget(this.keys.job(jobId), 'state');
    if (state && String(state) === 'completed') {
      const raw = await client.hget(this.keys.job(jobId), 'returnvalue');
      return this.serializer.deserialize(raw != null ? String(raw) : 'null') as R;
    }
    if (state && String(state) === 'failed') {
      const reason = await client.hget(this.keys.job(jobId), 'failedReason');
      throw new Error(reason ? String(reason) : `Job ${jobId} failed`);
    }

    throw new Error(`Job ${jobId} did not finish within ${waitTimeout}ms`);
  }

  /** @internal Build Job objects from batch exec results. */
  private buildBulkJobs(
    client: Client,
    prepared: { entry: { name: string; data: D; opts?: JobOptions }; opts: JobOptions; parentId: string }[],
    rawResults: unknown[] | null,
    timestamp: number,
  ): Job<D, R>[] {
    if (!rawResults || rawResults.length !== prepared.length) {
      throw new Error(`addBulk batch returned ${rawResults?.length ?? 'null'} results, expected ${prepared.length}`);
    }
    return prepared.flatMap((p, i) => {
      const raw = String(rawResults[i]);
      if (raw === 'skipped' || raw === 'duplicate') return [];
      if (raw === 'ERR:COST_EXCEEDS_CAPACITY') {
        throw new Error('Job cost exceeds token bucket capacity');
      }
      if (raw === 'ERR:ID_EXHAUSTED') {
        throw new Error('Failed to generate job ID: too many collisions with custom job IDs');
      }
      const jobId = raw;
      const job = new Job<D, R>(client, this.keys, jobId, p.entry.name, p.entry.data, p.opts, this.serializer);
      job.timestamp = timestamp;
      job.parentId = p.parentId || undefined;
      return [job];
    });
  }

  /**
   * Retrieve a job by ID from the queue.
   * Returns null if the job does not exist.
   * @param id - The job ID
   * @param opts - Set `excludeData: true` to omit `data` and `returnvalue` fields
   */
  async getJob(id: string, opts?: GetJobsOptions): Promise<Job<D, R> | null> {
    const client = await this.getClient();

    if (opts?.excludeData) {
      const values = await client.hmget(this.keys.job(id), JOB_METADATA_FIELDS as string[]);
      const hash = hmgetArrayToRecord(values, JOB_METADATA_FIELDS);
      if (!hash) return null;
      return Job.fromHash<D, R>(client, this.keys, id, hash, this.serializer, true);
    }

    const hashData = await client.hgetall(this.keys.job(id));
    const hash = hashDataToRecord(hashData);
    if (!hash) return null;
    return Job.fromHash<D, R>(client, this.keys, id, hash, this.serializer);
  }

  /**
   * Pause the queue. Workers will stop picking up new jobs.
   */
  async pause(): Promise<void> {
    const client = await this.getClient();
    await pause(client, this.keys);
  }

  /**
   * Resume the queue after a pause.
   */
  async resume(): Promise<void> {
    const client = await this.getClient();
    await resume(client, this.keys);
  }

  /**
   * Revoke a job by ID.
   * If the job is waiting/delayed, it is immediately moved to the failed set with reason 'revoked'.
   * If the job is currently being processed, a revoked flag is set on the hash -
   * the worker will check this flag cooperatively and fire the AbortSignal.
   * Returns 'revoked', 'flagged', or 'not_found'.
   */
  async revoke(jobId: string): Promise<string> {
    const client = await this.getClient();
    return revokeJob(client, this.keys, jobId, Date.now());
  }

  /**
   * Set the global concurrency limit for this queue.
   * When set, workers will not pick up new jobs if the total number of
   * pending (active) jobs across all workers meets or exceeds this limit.
   * Set to 0 to remove the limit.
   */
  async setGlobalConcurrency(n: number): Promise<void> {
    const client = await this.getClient();
    await client.hset(this.keys.meta, { globalConcurrency: n.toString() });
  }

  /**
   * Set a global rate limit for this queue.
   * All workers will respect this limit dynamically (picked up within one scheduler tick).
   * Takes precedence over WorkerOptions.limiter when set.
   */
  async setGlobalRateLimit(config: RateLimitConfig): Promise<void> {
    const client = await this.getClient();
    await client.hset(this.keys.meta, {
      rateLimitMax: config.max.toString(),
      rateLimitDuration: config.duration.toString(),
    });
  }

  /**
   * Remove the global rate limit for this queue.
   * Workers fall back to their local WorkerOptions.limiter if configured.
   */
  async removeGlobalRateLimit(): Promise<void> {
    const client = await this.getClient();
    await client.hdel(this.keys.meta, ['rateLimitMax', 'rateLimitDuration']);
  }

  /**
   * Get the current global rate limit for this queue.
   * Returns null if no global rate limit is configured.
   */
  async getGlobalRateLimit(): Promise<RateLimitConfig | null> {
    const client = await this.getClient();
    const fields = await client.hmget(this.keys.meta, ['rateLimitMax', 'rateLimitDuration']);
    const max = fields?.[0];
    const duration = fields?.[1];
    if (max == null || duration == null) return null;
    return { max: Number(String(max)), duration: Number(String(duration)) };
  }

  /**
   * List all active workers for this queue.
   * Workers register themselves with TTL-based keys; only live workers appear.
   * Returns an array of WorkerInfo sorted by startedAt (oldest first).
   */
  async getWorkers(): Promise<WorkerInfo[]> {
    const client = await this.getClient();
    const pfx = keyPrefixPattern(this.opts.prefix ?? 'glide', this.name);
    const pattern = `${pfx}:w:*`;
    const keys = await this.scanKeys(client, pattern);
    if (keys.length === 0) return [];

    const now = Date.now();
    const workers: WorkerInfo[] = [];
    const CHUNK = 500;
    for (let offset = 0; offset < keys.length; offset += CHUNK) {
      const chunk = keys.slice(offset, offset + CHUNK);
      const batch = this.newBatch();
      for (const key of chunk) {
        (batch as any).get(key);
      }
      const results = await client.exec(batch as any, false);
      if (!results) continue;
      for (let i = 0; i < chunk.length; i++) {
        const val = results[i];
        if (!val) continue;
        try {
          const data = JSON.parse(String(val));
          if (typeof data.addr !== 'string' || typeof data.pid !== 'number' || typeof data.startedAt !== 'number') {
            continue;
          }
          const idx = chunk[i].lastIndexOf(':w:');
          if (idx < 0) continue;
          workers.push({
            id: chunk[i].substring(idx + 3),
            addr: data.addr,
            pid: data.pid,
            startedAt: data.startedAt,
            age: Math.max(0, now - data.startedAt),
            activeJobs: typeof data.activeJobs === 'number' ? data.activeJobs : 0,
          });
        } catch {
          // Malformed JSON - skip entry
        }
      }
    }

    workers.sort((a, b) => a.startedAt - b.startedAt);
    return workers;
  }

  /**
   * Upsert a job scheduler (repeatable/cron job).
   * Stores the scheduler config in the schedulers hash.
   * Computes the initial nextRun based on the schedule.
   */
  async upsertJobScheduler(name: string, schedule: ScheduleOpts, template?: JobTemplate): Promise<void> {
    const client = await this.getClient();

    if (schedule.tz) {
      validateTimezone(schedule.tz);
    }
    validateSchedulerEvery(schedule.every);
    if (schedule.repeatAfterComplete != null) {
      if (!Number.isSafeInteger(schedule.repeatAfterComplete) || schedule.repeatAfterComplete <= 0) {
        throw new Error('repeatAfterComplete must be a positive safe integer');
      }
    }
    const modeCount = [schedule.pattern, schedule.every, schedule.repeatAfterComplete].filter(Boolean).length;
    if (modeCount === 0) {
      throw new Error('Schedule must have pattern (cron), every (ms interval), or repeatAfterComplete (ms)');
    }
    if (modeCount > 1) {
      throw new Error('Schedule options pattern, every, and repeatAfterComplete are mutually exclusive');
    }
    const startDate = normalizeScheduleDate(schedule.startDate, 'startDate');
    const endDate = normalizeScheduleDate(schedule.endDate, 'endDate');
    validateSchedulerBounds(startDate, endDate, schedule.limit);
    if (template?.opts?.lifo && template.opts.ordering?.key) {
      throw new Error('Scheduler template: lifo and ordering.key cannot be used together');
    }

    const lock = await this.acquireSchedulerMutationLock(client);
    try {
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
      const existingRaw = await client.hget(this.keys.schedulers, name);
      if (existingRaw != null) {
        try {
          const existing = JSON.parse(String(existingRaw)) as SchedulerEntry;
          const scheduleUnchanged =
            existing.pattern === schedule.pattern &&
            existing.every === schedule.every &&
            existing.repeatAfterComplete === schedule.repeatAfterComplete &&
            existing.tz === schedule.tz &&
            existing.startDate === startDate &&
            existing.endDate === endDate;
          if (scheduleUnchanged && existing.nextRun) {
            iterationCount = existing.iterationCount ?? 0;
            lastRun = existing.lastRun;
            nextRun = existing.nextRun;
          }
        } catch {
          // Ignore malformed existing state and treat as a fresh upsert.
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

      await client.hset(this.keys.schedulers, { [name]: JSON.stringify(entry) });
    } finally {
      await this.releaseSchedulerMutationLock(client, lock);
    }
  }

  /**
   * Remove a job scheduler by name.
   */
  async removeJobScheduler(name: string): Promise<void> {
    const client = await this.getClient();
    const lock = await this.acquireSchedulerMutationLock(client);
    try {
      await client.hdel(this.keys.schedulers, [name]);
    } finally {
      await this.releaseSchedulerMutationLock(client, lock);
    }
  }

  /**
   * Get metrics for completed or failed jobs.
   * Returns total count and per-minute time-series data points with throughput and avg duration.
   */
  async getMetrics(type: 'completed' | 'failed', opts?: MetricsOptions): Promise<Metrics> {
    const client = await this.getClient();
    const key = type === 'completed' ? this.keys.completed : this.keys.failed;
    const metricsKey = type === 'completed' ? this.keys.metricsCompleted : this.keys.metricsFailed;

    const [count, rawHash] = await Promise.all([client.zcard(key), client.hgetall(metricsKey)]);

    const buckets = new Map<number, { count: number; totalDuration: number }>();
    const hashRecord = hashDataToRecord(rawHash);
    if (hashRecord) {
      for (const [field, value] of Object.entries(hashRecord)) {
        const match = field.match(/^m:(\d+):([cd])$/);
        if (!match) continue;
        const ts = parseInt(match[1], 10);
        if (isNaN(ts)) continue;
        const kind = match[2];
        let bucket = buckets.get(ts);
        if (!bucket) {
          bucket = { count: 0, totalDuration: 0 };
          buckets.set(ts, bucket);
        }
        const numValue = parseInt(value, 10);
        if (isNaN(numValue)) continue;
        if (kind === 'c') bucket.count = numValue;
        else bucket.totalDuration = numValue;
      }
    }

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

  /**
   * Bulk-remove old completed or failed jobs by age.
   * @param grace - Minimum age in milliseconds. Jobs finished more recently than this are kept.
   * @param limit - Maximum number of jobs to remove in one call.
   * @param type - Which job state to clean: 'completed' or 'failed'.
   * @returns Array of removed job IDs.
   */
  async clean(grace: number, limit: number, type: 'completed' | 'failed'): Promise<string[]> {
    if (grace < 0) throw new RangeError('grace must be >= 0');
    if (limit <= 0) return [];
    const client = await this.getClient();
    return cleanJobs(client, this.keys, type, grace, limit, Date.now());
  }

  /**
   * Drain the queue: remove all waiting jobs without touching active jobs.
   * When delayed=true, also removes all delayed/scheduled jobs.
   * Deletes associated job hashes and emits a 'drained' event.
   */
  async drain(delayed?: boolean): Promise<void> {
    const client = await this.getClient();
    await drainQueue(client, this.keys, delayed ?? false);
  }

  /**
   * Bulk retry failed jobs.
   * Moves jobs from the failed set to the scheduled ZSet (delayed state).
   * Resets attemptsMade, failedReason, and finishedOn on each retried job.
   * @param opts.count - Maximum number of jobs to retry. Omit or 0 to retry all.
   * @returns Number of jobs retried.
   */
  async retryJobs(opts?: { count?: number }): Promise<number> {
    if (opts?.count != null && (!Number.isInteger(opts.count) || opts.count < 0)) {
      throw new Error('count must be a non-negative integer');
    }
    const client = await this.getClient();
    return retryJobs(client, this.keys, opts?.count ?? 0, Date.now());
  }

  /**
   * Get job counts by state.
   * - waiting: stream length minus stream-active entries, plus LIFO and priority list lengths
   * - active: stream PEL count (XPENDING) plus list-active counter
   * - delayed: scheduled ZSet cardinality (includes both delayed and prioritized)
   * - completed: completed ZSet cardinality
   * - failed: failed ZSet cardinality
   */
  async getJobCounts(): Promise<JobCounts> {
    const client = await this.getClient();

    const [streamLen, completedCount, failedCount, scheduledCount, listActiveRaw, lifoLen, priorityLen] =
      await Promise.all([
        client.xlen(this.keys.stream),
        client.zcard(this.keys.completed),
        client.zcard(this.keys.failed),
        client.zcard(this.keys.scheduled),
        typeof client.get === 'function' ? client.get(this.keys.listActive) : null,
        typeof client.llen === 'function' ? client.llen(this.keys.lifo) : 0,
        typeof client.llen === 'function' ? client.llen(this.keys.priority) : 0,
      ]);

    const listActive = Number(listActiveRaw) || 0;

    // XPENDING returns [pendingCount, minId, maxId, consumers[]]
    let streamActive = 0;
    try {
      const pendingInfo = await client.xpending(this.keys.stream, CONSUMER_GROUP);
      streamActive = Number(pendingInfo[0]) || 0;
    } catch {
      // Consumer group may not exist yet
    }

    const activeCount = streamActive + listActive;
    const waiting = Math.max(0, streamLen - streamActive) + lifoLen + priorityLen;

    return {
      waiting,
      active: activeCount,
      delayed: scheduledCount,
      completed: completedCount,
      failed: failedCount,
    };
  }

  /**
   * Remove all data associated with this queue from the server.
   * If force=false (default), fails if there are active jobs.
   * If force=true, deletes everything regardless of active jobs.
   */
  async obliterate(opts?: { force?: boolean }): Promise<void> {
    const client = await this.getClient();
    const force = opts?.force ?? false;

    // Check for active jobs if not forcing
    if (!force) {
      try {
        const pendingInfo = await client.xpending(this.keys.stream, CONSUMER_GROUP);
        const activeCount = Number(pendingInfo[0]) || 0;
        if (activeCount > 0) {
          throw new Error(
            `Cannot obliterate queue "${this.name}": ${activeCount} active jobs. Use { force: true } to override.`,
          );
        }
      } catch (err) {
        // If the error is our own active-jobs check, re-throw
        if (err instanceof Error && err.message.includes('Cannot obliterate')) {
          throw err;
        }
        // Consumer group doesn't exist yet means no active jobs - continue
      }
    }

    // Delete all known static keys
    const staticKeys = [
      this.keys.id,
      this.keys.stream,
      this.keys.scheduled,
      this.keys.completed,
      this.keys.failed,
      this.keys.events,
      this.keys.meta,
      this.keys.dedup,
      this.keys.rate,
      this.keys.schedulers,
      this.keys.ordering,
      this.keys.ratelimited,
      this.keys.metricsCompleted,
      this.keys.metricsFailed,
      this.keys.lifo,
    ];
    await client.del(staticKeys);

    // Scan and delete job hashes and deps sets
    // Use escaped prefix to prevent glob injection from queue names containing * ? [ ]
    const pfx = keyPrefixPattern(this.opts.prefix ?? 'glide', this.name);
    const jobPattern = `${pfx}:job:*`;
    const logPattern = `${pfx}:log:*`;
    const depsPattern = `${pfx}:deps:*`;
    const groupPattern = `${pfx}:group:*`;
    const groupqPattern = `${pfx}:groupq:*`;
    const orderPendingPattern = `${pfx}:orderdone:pending:*`;

    const workerPattern = `${pfx}:w:*`;

    for (const pattern of [
      jobPattern,
      logPattern,
      depsPattern,
      groupPattern,
      groupqPattern,
      orderPendingPattern,
      workerPattern,
    ]) {
      await this.scanAndDelete(client, pattern);
    }
  }

  /**
   * Scan for keys matching a pattern and return them.
   * Handles both standalone (GlideClient) and cluster (GlideClusterClient) scan APIs.
   * @internal
   */
  private async scanKeys(client: Client, pattern: string): Promise<string[]> {
    const seen = new Set<string>();
    if (this.clusterMode) {
      const clusterClient = client as GlideClusterClient;
      let cursor = new ClusterScanCursor();
      while (!cursor.isFinished()) {
        const [nextCursor, keys] = await clusterClient.scan(cursor, { match: pattern, count: 100 });
        cursor = nextCursor;
        for (const k of keys) seen.add(String(k));
      }
    } else {
      let cursor = '0';
      do {
        const result = await (client as GlideClient).scan(cursor, { match: pattern, count: 100 });
        cursor = result[0] as string;
        for (const k of result[1]) seen.add(String(k));
      } while (cursor !== '0');
    }
    return [...seen];
  }

  /**
   * Scan for keys matching a pattern and delete them in batches.
   * Deletes during iteration to avoid accumulating all keys in memory.
   * @internal
   */
  private async scanAndDelete(client: Client, pattern: string): Promise<void> {
    if (this.clusterMode) {
      const clusterClient = client as GlideClusterClient;
      let cursor = new ClusterScanCursor();
      while (!cursor.isFinished()) {
        const [nextCursor, keys] = await clusterClient.scan(cursor, { match: pattern, count: 100 });
        cursor = nextCursor;
        if (keys.length > 0) await client.del(keys);
      }
    } else {
      let cursor = '0';
      do {
        const result = await (client as GlideClient).scan(cursor, { match: pattern, count: 100 });
        cursor = result[0] as string;
        const keys = result[1];
        if (keys.length > 0) await client.del(keys);
      } while (cursor !== '0');
    }
  }

  /**
   * Retrieve jobs by state with optional pagination.
   * @param type - The job state to query
   * @param start - Start index for pagination (default 0)
   * @param end - End index for pagination (default -1, meaning all)
   * @param opts - Set `excludeData: true` to omit `data` and `returnvalue` fields
   */
  async getJobs(
    type: 'waiting' | 'active' | 'delayed' | 'completed' | 'failed',
    start = 0,
    end = -1,
    opts?: GetJobsOptions,
  ): Promise<Job<D, R>[]> {
    const client = await this.getClient();
    let jobIds: string[];

    switch (type) {
      case 'waiting': {
        // Note: stream pagination reads from start to end+1, then slices.
        // For large offsets this is O(end) rather than O(end-start).
        // Consider cursor-based pagination for better performance at scale.
        const entries = await client.xrange(
          this.keys.stream,
          InfBoundary.NegativeInfinity,
          InfBoundary.PositiveInfinity,
          end >= 0 ? { count: end + 1 } : undefined,
        );
        if (!entries) return [];
        const allIds = extractJobIdsFromStreamEntries(entries);
        jobIds = allIds.slice(start, end >= 0 ? end + 1 : undefined);
        break;
      }
      case 'active': {
        // Active jobs come from two sources: stream PEL (XPENDING) for stream-backed
        // jobs, and the per-queue job hashes for list-backed jobs (priority/LIFO),
        // which are invisible to XPENDING. List-backed IDs come from a SCAN-based
        // FCALL (glidemq_getActiveListJobIds).
        let streamJobIds: string[] = [];
        try {
          const pendingEntries = await client.xpendingWithOptions(this.keys.stream, CONSUMER_GROUP, {
            start: InfBoundary.NegativeInfinity,
            end: InfBoundary.PositiveInfinity,
            count: 10000,
          });
          const entryIds = pendingEntries.map((e) => String(e[0]));
          streamJobIds = await this.resolveActiveJobIds(client, entryIds);
        } catch {
          streamJobIds = [];
        }
        const listJobIds = await getActiveListJobIds(client, this.keys, 0, -1);
        const all = streamJobIds.concat(listJobIds);
        jobIds = all.slice(start, end >= 0 ? end + 1 : undefined);
        break;
      }
      case 'delayed':
      case 'completed':
      case 'failed': {
        const members = await client.zrange(this.zsetKeyForState(type), { start: start, end: end >= 0 ? end : -1 });
        jobIds = members.map((m) => String(m));
        break;
      }
    }

    if (jobIds.length === 0) return [];
    const excludeData = opts?.excludeData === true;
    const jobs: Job<D, R>[] = [];
    for (let offset = 0; offset < jobIds.length; offset += PIPELINE_CHUNK_SIZE) {
      const chunk = jobIds.slice(offset, offset + PIPELINE_CHUNK_SIZE);
      const batch = this.newBatch();
      if (excludeData) {
        for (const id of chunk) (batch as any).hmget(this.keys.job(id), JOB_METADATA_FIELDS);
      } else {
        for (const id of chunk) (batch as any).hgetall(this.keys.job(id));
      }
      const batchResults = await client.exec(batch as any, false);
      if (batchResults) {
        for (let i = 0; i < batchResults.length; i++) {
          const hash = excludeData
            ? hmgetArrayToRecord(batchResults[i] as (unknown | null)[], JOB_METADATA_FIELDS)
            : hashDataToRecord(batchResults[i] as any);
          if (hash) jobs.push(Job.fromHash<D, R>(client, this.keys, chunk[i], hash, this.serializer, excludeData));
        }
      }
    }
    return jobs;
  }

  /**
   * Search for jobs matching the given criteria.
   * Supports filtering by state, name (exact match), and data fields (shallow key-value match).
   * If state is provided, searches only within that state's data structure.
   * If no state is provided, SCANs all job hashes matching the queue prefix.
   * Default limit: 100.
   */
  async searchJobs(opts: SearchJobsOptions): Promise<Job<D, R>[]> {
    const client = await this.getClient();
    const limit = opts.limit ?? 100;
    const pfx = keyPrefix(this.opts.prefix ?? 'glide', this.name);

    let jobIds: string[];

    if (opts.state && opts.name) {
      // Use Lua function for name-based filtering within a state
      jobIds = await this.searchByNameInState(client, opts.state, opts.name, limit, pfx);
    } else if (opts.state) {
      // Get all IDs from the state, will filter by data below
      jobIds = await this.getJobIdsForState(client, opts.state, limit);
    } else {
      // No state: SCAN all job hashes
      jobIds = await this.scanJobIds(client, pfx, opts.name, limit);
    }

    const hasDataFilter = opts.data && Object.keys(opts.data).length > 0;
    const excludeData = opts.excludeData === true && !hasDataFilter;
    const jobs: Job<D, R>[] = [];
    const CHUNK = 100;
    for (let offset = 0; offset < jobIds.length && jobs.length < limit; offset += CHUNK) {
      const chunk = jobIds.slice(offset, offset + CHUNK);
      const batch = this.newBatch();
      if (excludeData) {
        for (const id of chunk) (batch as any).hmget(this.keys.job(id), JOB_METADATA_FIELDS);
      } else {
        for (const id of chunk) (batch as any).hgetall(this.keys.job(id));
      }
      const batchResults = await client.exec(batch as any, false);
      if (!batchResults) continue;

      for (let i = 0; i < chunk.length && jobs.length < limit; i++) {
        const hash = excludeData
          ? hmgetArrayToRecord(batchResults[i] as (unknown | null)[], JOB_METADATA_FIELDS)
          : hashDataToRecord(batchResults[i] as any);
        if (!hash) continue;

        const job = Job.fromHash<D, R>(client, this.keys, chunk[i], hash, this.serializer, excludeData);

        // Apply name filter if we used a non-Lua path
        if (opts.name && !opts.state && job.name !== opts.name) continue;

        // Apply data filter (shallow key-value match)
        if (opts.data && !matchesData(job.data as Record<string, unknown>, opts.data)) continue;

        // Strip data after filtering if caller originally requested excludeData
        if (opts.excludeData && !excludeData) {
          job.data = undefined as unknown as D;
          job.returnvalue = undefined;
        }

        jobs.push(job);
      }
    }

    return jobs;
  }

  /** @internal Map a terminal/zset state to its corresponding key. */
  private zsetKeyForState(state: 'delayed' | 'completed' | 'failed'): string {
    switch (state) {
      case 'delayed':
        return this.keys.scheduled;
      case 'completed':
        return this.keys.completed;
      case 'failed':
        return this.keys.failed;
    }
  }

  /** @internal Get job IDs from a state's data structure. */
  private async getJobIdsForState(
    client: Client,
    state: 'waiting' | 'active' | 'delayed' | 'completed' | 'failed',
    limit: number,
  ): Promise<string[]> {
    switch (state) {
      case 'waiting': {
        const entries = await client.xrange(
          this.keys.stream,
          InfBoundary.NegativeInfinity,
          InfBoundary.PositiveInfinity,
          { count: limit },
        );
        if (!entries) return [];
        return extractJobIdsFromStreamEntries(entries);
      }
      case 'active': {
        try {
          const pendingEntries = await client.xpendingWithOptions(this.keys.stream, CONSUMER_GROUP, {
            start: InfBoundary.NegativeInfinity,
            end: InfBoundary.PositiveInfinity,
            count: limit,
          });
          const entryIds = pendingEntries.map((e) => String(e[0]));
          return this.resolveActiveJobIds(client, entryIds);
        } catch {
          return [];
        }
      }
      case 'delayed':
      case 'completed':
      case 'failed': {
        const members = await client.zrange(this.zsetKeyForState(state), { start: 0, end: limit - 1 });
        return members.map((m) => String(m));
      }
    }
  }

  /** @internal Use Lua searchByName within a specific state. */
  private async searchByNameInState(
    client: Client,
    state: 'waiting' | 'active' | 'delayed' | 'completed' | 'failed',
    name: string,
    limit: number,
    pfx: string,
  ): Promise<string[]> {
    if (state === 'active') {
      const allIds = await this.getJobIdsForState(client, 'active', 10000);
      if (allIds.length === 0) return [];
      const batch = this.newBatch();
      for (const id of allIds) (batch as any).hget(this.keys.job(id), 'name');
      const names = await client.exec(batch as any, false);
      const matched: string[] = [];
      for (let i = 0; i < allIds.length && matched.length < limit; i++) {
        if (String(names?.[i]) === name) matched.push(allIds[i]);
      }
      return matched;
    }

    if (state === 'waiting') {
      return searchByName(client, this.keys.stream, 'stream', name, limit, pfx + ':');
    }

    return searchByName(client, this.zsetKeyForState(state), 'zset', name, limit, pfx + ':');
  }

  /** @internal SCAN all job hashes and optionally filter by name. */
  private async scanJobIds(
    client: Client,
    pfx: string,
    nameFilter: string | undefined,
    limit: number,
  ): Promise<string[]> {
    const pattern = `${pfx}:job:*`;
    const jobIds: string[] = [];
    const prefixLen = `${pfx}:job:`.length;

    const collectKeys = async (keys: unknown[]): Promise<void> => {
      const keyStrs = keys.map((k) => String(k));
      if (keyStrs.length === 0) return;
      if (!nameFilter) {
        for (const k of keyStrs) {
          if (jobIds.length >= limit) break;
          jobIds.push(k.substring(prefixLen));
        }
        return;
      }
      const batch = this.newBatch();
      for (const k of keyStrs) (batch as any).hget(k, 'name');
      const names = await client.exec(batch as any, false);
      for (let i = 0; i < keyStrs.length && jobIds.length < limit; i++) {
        if (String(names?.[i]) === nameFilter) jobIds.push(keyStrs[i].substring(prefixLen));
      }
    };

    if (this.clusterMode) {
      const clusterClient = client as GlideClusterClient;
      let cursor = new ClusterScanCursor();
      while (!cursor.isFinished() && jobIds.length < limit) {
        const [nextCursor, keys] = await clusterClient.scan(cursor, { match: pattern, count: 100 });
        cursor = nextCursor;
        await collectKeys(keys);
      }
    } else {
      let cursor = '0';
      do {
        const result = await (client as GlideClient).scan(cursor, { match: pattern, count: 100 });
        cursor = result[0] as string;
        await collectKeys(result[1]);
      } while (cursor !== '0' && jobIds.length < limit);
    }

    return jobIds;
  }

  /**
   * Get job counts by types. Alias for getJobCounts().
   */
  async getJobCountByTypes(): Promise<JobCounts> {
    return this.getJobCounts();
  }

  /**
   * Check if the queue is paused.
   */
  async isPaused(): Promise<boolean> {
    const client = await this.getClient();
    const val = await client.hget(this.keys.meta, 'paused');
    return String(val) === '1';
  }

  /**
   * Get the count of waiting jobs (stream length).
   */
  async count(): Promise<number> {
    const client = await this.getClient();
    return client.xlen(this.keys.stream);
  }

  /**
   * Get all registered job schedulers (repeatable jobs).
   */
  async getRepeatableJobs(): Promise<{ name: string; entry: SchedulerEntry }[]> {
    const client = await this.getClient();
    const hashData = await client.hgetall(this.keys.schedulers);
    if (!hashData || hashData.length === 0) return [];
    const result: { name: string; entry: SchedulerEntry }[] = [];
    for (const item of hashData) {
      try {
        result.push({
          name: String(item.field),
          entry: JSON.parse(String(item.value)),
        });
      } catch {
        // Malformed JSON - skip entry
      }
    }
    return result;
  }

  /**
   * Get a single job scheduler entry by name.
   * Returns null if no scheduler with that name exists or if stored data is malformed.
   */
  async getJobScheduler(name: string): Promise<SchedulerEntry | null> {
    const client = await this.getClient();
    const raw = await client.hget(this.keys.schedulers, name);
    if (raw == null) return null;
    try {
      return JSON.parse(String(raw)) as SchedulerEntry;
    } catch {
      return null;
    }
  }

  /**
   * Retrieve log entries for a job by ID.
   */
  async getJobLogs(id: string, start = 0, end = -1): Promise<{ logs: string[]; count: number }> {
    const client = await this.getClient();
    const batch = this.newBatch();
    const logKey = this.keys.log(id);
    (batch as any).lrange(logKey, start, end);
    (batch as any).llen(logKey);
    const results = await client.exec(batch as any, false);
    const logs = ((results?.[0] as any[]) ?? []).map((l: any) => String(l));
    const count = Number(results?.[1] ?? 0);
    return { logs, count };
  }

  /**
   * Aggregate AI usage metadata across a flow (parent + children).
   * Walks the deps set of the parent job and sums token counts, cost, and model usage.
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
    const client = await this.getClient();
    const usageFields = [
      'usage:model',
      'usage:tokens',
      'usage:costs',
      'usage:totalTokens',
      'usage:totalCost',
      'usage:costUnit',
    ];
    const agg = {
      tokens: Object.create(null) as Record<string, number>,
      totalTokens: 0,
      costs: Object.create(null) as Record<string, number>,
      totalCost: 0,
      costUnit: undefined as string | undefined,
      jobCount: 0,
      models: Object.create(null) as Record<string, number>,
    };

    const mergeJob = (values: (unknown | null)[]) => {
      const model = values[0] ? String(values[0]) : undefined;
      const tokensStr = values[1] ? String(values[1]) : undefined;
      const costsStr = values[2] ? String(values[2]) : undefined;
      const rawTotal = values[3] ? parseFloat(String(values[3])) : 0;
      const totalTokens = Number.isFinite(rawTotal) ? rawTotal : 0;
      const rawCost = values[4] ? parseFloat(String(values[4])) : 0;
      const totalCost = Number.isFinite(rawCost) ? rawCost : 0;
      const costUnit = values[5] ? String(values[5]) : undefined;

      if (!model && !tokensStr && !costsStr && !totalTokens && !totalCost) return;

      agg.totalTokens += totalTokens;
      agg.totalCost += totalCost;
      agg.jobCount++;
      if (costUnit && !agg.costUnit) agg.costUnit = costUnit;
      if (model) agg.models[model] = (agg.models[model] || 0) + 1;

      const tokens = tokensStr ? parseJsonRecord(tokensStr) : undefined;
      if (tokens) {
        for (const [k, v] of Object.entries(tokens)) {
          if (Number.isFinite(v)) {
            agg.tokens[k] = (agg.tokens[k] || 0) + v;
          }
        }
      }
      const costs = costsStr ? parseJsonRecord(costsStr) : undefined;
      if (costs) {
        for (const [k, v] of Object.entries(costs)) {
          if (Number.isFinite(v)) {
            agg.costs[k] = (agg.costs[k] || 0) + v;
          }
        }
      }
    };

    // Include the parent job itself
    const parentValues = await client.hmget(this.keys.job(parentJobId), usageFields);
    if (parentValues) mergeJob(parentValues);

    // Walk children via deps set
    const members = await client.smembers(this.keys.deps(parentJobId));
    if (!members || members.size === 0) return agg;

    const batch = this.newBatch();
    const memberList: string[] = [];
    for (const member of members) {
      const memberStr = String(member);
      const lastColon = memberStr.lastIndexOf(':');
      if (lastColon === -1) continue;
      const queuePrefix = memberStr.substring(0, lastColon);
      const childId = memberStr.substring(lastColon + 1);
      const jobKey = `${queuePrefix}:job:${childId}`;
      (batch as any).hmget(jobKey, usageFields);
      memberList.push(memberStr);
    }

    if (memberList.length === 0) return agg;
    const batchResults = await client.exec(batch as any, false);
    if (!batchResults) return agg;

    for (let i = 0; i < memberList.length; i++) {
      const values = batchResults[i] as any[] | null;
      if (values) mergeJob(values);
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
    const client = await this.getClient();
    const budgetKey = this.keys.budget(flowId);
    const raw = await client.hgetall(budgetKey);
    if (!raw || (Array.isArray(raw) && raw.length === 0)) return null;
    const fields = hashDataToRecord(raw as any);
    if (!fields) return null;

    return {
      maxTotalTokens: fields.maxTotalTokens ? parseFloat(fields.maxTotalTokens) : undefined,
      maxTokens: fields.maxTokens ? parseJsonRecord(fields.maxTokens) : undefined,
      tokenWeights: fields.tokenWeights ? parseJsonRecord(fields.tokenWeights) : undefined,
      maxTotalCost: fields.maxTotalCost ? parseFloat(fields.maxTotalCost) : undefined,
      maxCosts: fields.maxCosts ? parseJsonRecord(fields.maxCosts) : undefined,
      costUnit: fields.costUnit || undefined,
      usedTokens: parseFloat(fields.usedTokens || '0'),
      usedCost: parseFloat(fields.usedCost || '0'),
      exceeded: fields.exceeded === '1',
      onExceeded: (fields.onExceeded as 'pause' | 'fail') || 'fail',
    };
  }

  /**
   * Aggregate reported AI usage across queues for a rolling time window.
   * Uses per-minute buckets recorded by job.reportUsage(), avoiding job-hash scans.
   */
  async getUsageSummary(opts?: UsageSummaryOptions): Promise<UsageSummary> {
    const client = await this.getClient();
    return Queue.getUsageSummary({
      ...opts,
      client,
      prefix: this.opts.prefix,
    });
  }

  /**
   * Aggregate reported AI usage across queues for a rolling time window.
   * Pass either an existing client or connection options for a temporary client.
   */
  static async getUsageSummary(
    opts: UsageSummaryOptions & Pick<QueueOptions, 'client' | 'connection' | 'prefix'>,
  ): Promise<UsageSummary> {
    const { startTime, endTime, bucketStarts } = resolveUsageWindow(opts);
    const summary = createUsageSummaryResult(startTime, endTime);

    let client = opts.client;
    let clientOwned = false;
    if (!client) {
      if (!opts.connection) {
        throw new GlideMQError('getUsageSummary requires either `client` or `connection`');
      }
      client = await createClient(opts.connection);
      clientOwned = true;
    }

    try {
      const prefix = opts.prefix;
      const requestedQueues = opts.queues
        ? Array.from(
            new Set(
              opts.queues.map((name) => {
                validateQueueName(name);
                return name;
              }),
            ),
          )
        : null;

      const queues =
        requestedQueues ??
        Array.from(await client.smembers(usageQueuesKey(prefix))).map((queueName) => String(queueName));

      if (queues.length === 0 || bucketStarts.length === 0) {
        return summary;
      }

      const totalBucketReads = queues.length * bucketStarts.length;
      if (totalBucketReads > MAX_USAGE_SUMMARY_KEY_READS) {
        throw new GlideMQError(
          `usage summary request exceeds maximum bucket reads (${totalBucketReads} > ${MAX_USAGE_SUMMARY_KEY_READS}); reduce queues or windowMs`,
        );
      }

      for (const queueName of queues) {
        const queueKeys = buildKeys(queueName, prefix);
        let queueSummary: UsageQueueSummary | undefined;

        for (let offset = 0; offset < bucketStarts.length; offset += PIPELINE_CHUNK_SIZE) {
          const chunk = bucketStarts.slice(offset, offset + PIPELINE_CHUNK_SIZE);
          const batch = newClientBatch(client);
          for (const bucketTs of chunk) {
            (batch as any).hgetall(queueKeys.usageBucket(bucketTs));
          }

          const results = await client.exec(batch as any, false);
          if (!results) continue;

          for (let i = 0; i < chunk.length && i < results.length; i++) {
            const fields = hashDataToRecord(results[i] as any);
            if (!fields || !hasUsageBucketData(fields)) continue;

            if (!queueSummary) {
              queueSummary = createUsageQueueSummary();
              summary.perQueue[queueName] = queueSummary;
            }

            mergeUsageBucketFields(summary, fields);
            mergeUsageBucketFields(queueSummary, fields);
          }
        }
      }

      summary.queues = Object.keys(summary.perQueue).sort();
      return summary;
    } finally {
      if (clientOwned) {
        try {
          await client.close();
        } catch {
          // Best effort for temporary clients used by static summary reads.
        }
      }
    }
  }

  /**
   * Read entries from a job's streaming channel.
   * Uses XRANGE for non-blocking reads by default.
   * When `block` is set and > 0, uses XREAD with BLOCK for long-polling.
   * Pass lastId to resume from a known position.
   */
  async readStream(jobId: string, opts?: ReadStreamOptions): Promise<{ id: string; fields: Record<string, string> }[]> {
    const client = await this.getClient();
    const lastId = opts?.lastId;
    const count = opts?.count ?? 100;
    const blockMs = opts?.block;

    if (blockMs !== undefined && blockMs > 0) {
      // Use XREAD with BLOCK for long-polling
      const streamId = lastId ?? '0-0';
      const xreadResult = await client.xread({ [this.keys.jstream(jobId)]: streamId }, { block: blockMs, count });
      if (!xreadResult) return [];
      const result: { id: string; fields: Record<string, string> }[] = [];
      for (const streamEntry of xreadResult) {
        const entries = streamEntry.value;
        for (const entryId in entries) {
          if (!Object.prototype.hasOwnProperty.call(entries, entryId)) continue;
          const fieldPairs = entries[entryId];
          if (!fieldPairs) continue;
          const fields: Record<string, string> = Object.create(null);
          for (const [k, v] of fieldPairs) {
            fields[String(k)] = String(v);
          }
          result.push({ id: String(entryId), fields });
        }
      }
      return result;
    }

    // Non-blocking: use XRANGE
    const start = lastId ? ({ value: lastId, isInclusive: false } as const) : InfBoundary.NegativeInfinity;
    const entries = await client.xrange(this.keys.jstream(jobId), start, InfBoundary.PositiveInfinity, { count });
    if (!entries) return [];
    const result: { id: string; fields: Record<string, string> }[] = [];
    for (const entryId of Object.keys(entries)) {
      const pairs = entries[entryId];
      const fields: Record<string, string> = Object.create(null);
      for (const [k, v] of pairs) {
        fields[String(k)] = String(v);
      }
      result.push({ id: entryId, fields });
    }
    return result;
  }

  /**
   * Retrieve jobs from the dead letter queue configured for this queue.
   * Returns an empty array if no DLQ is configured.
   * @param start - Start index (default 0)
   * @param end - End index (default -1, meaning all)
   * @param opts - Set `excludeData: true` to omit `data` and `returnvalue` fields
   */
  async getDeadLetterJobs(start = 0, end = -1, opts?: GetJobsOptions): Promise<Job<D, R>[]> {
    const client = await this.getClient();
    const dlqName = await this.resolveDeadLetterQueueName(client);
    if (!dlqName) return [];
    const dlqKeys = buildKeys(dlqName, this.opts.prefix);
    const targetCount = end >= 0 ? end - start + 1 : Number.POSITIVE_INFINITY;
    const loadPagedJobs = async (candidateJobIds: string[]): Promise<Job<D, R>[]> => {
      if (candidateJobIds.length === 0) return [];

      const selected: Job<D, R>[] = [];
      let seenExisting = 0;

      for (let offset = 0; offset < candidateJobIds.length; offset += PIPELINE_CHUNK_SIZE) {
        const chunkJobIds = candidateJobIds.slice(offset, offset + PIPELINE_CHUNK_SIZE);
        // Always load data internally so the ownership envelope is available, then
        // strip data on the way out when the caller asked for excludeData.
        const chunkJobs = await this.loadJobsByIdsForKeys(client, dlqKeys, chunkJobIds, {
          excludeData: false,
          serializer: undefined,
        });
        const scopedChunkJobs = chunkJobs.filter((job) => this.isDeadLetterJobOwnedByQueue(job));

        if (seenExisting + scopedChunkJobs.length <= start) {
          seenExisting += scopedChunkJobs.length;
          continue;
        }

        const sliceStart = Math.max(0, start - seenExisting);
        const remaining = Number.isFinite(targetCount) ? targetCount - selected.length : undefined;
        const pageSlice = scopedChunkJobs.slice(sliceStart, remaining != null ? sliceStart + remaining : undefined);
        if (opts?.excludeData) {
          for (const job of pageSlice) {
            (job as { data: unknown }).data = undefined;
            (job as { returnvalue: unknown }).returnvalue = undefined;
          }
        }
        selected.push(...pageSlice);
        seenExisting += scopedChunkJobs.length;

        if (Number.isFinite(targetCount) && selected.length >= targetCount) {
          break;
        }
      }

      return selected;
    };

    const limitedEntries = await client.xrange(
      dlqKeys.stream,
      InfBoundary.NegativeInfinity,
      InfBoundary.PositiveInfinity,
      end >= 0 ? { count: end + 1 } : undefined,
    );
    if (!limitedEntries) return [];

    let jobs = await loadPagedJobs(extractJobIdsFromStreamEntries(limitedEntries));
    if (!Number.isFinite(targetCount) || jobs.length >= targetCount) {
      return jobs;
    }

    const allEntries = await client.xrange(dlqKeys.stream, InfBoundary.NegativeInfinity, InfBoundary.PositiveInfinity);
    if (!allEntries) return jobs;

    jobs = await loadPagedJobs(extractJobIdsFromStreamEntries(allEntries));
    return jobs;
  }

  /**
   * Retrieve a single job from the configured dead letter queue.
   * Returns null when no DLQ is configured or the DLQ job does not exist.
   */
  async getDeadLetterJob(jobId: string, opts?: GetJobsOptions): Promise<Job<D, R> | null> {
    const client = await this.getClient();
    const dlqName = await this.resolveDeadLetterQueueName(client);
    if (!dlqName) return null;
    const dlqKeys = buildKeys(dlqName, this.opts.prefix);
    // Always load data internally so the ownership envelope is available; strip it
    // afterwards when the caller asked for excludeData.
    const jobs = await this.loadJobsByIdsForKeys(client, dlqKeys, [jobId], {
      excludeData: false,
      serializer: undefined,
    });
    const job = jobs[0] ?? null;
    if (!job || !this.isDeadLetterJobOwnedByQueue(job)) return null;
    if (opts?.excludeData) {
      (job as { data: unknown }).data = undefined;
      (job as { returnvalue: unknown }).returnvalue = undefined;
    }
    return job;
  }

  /**
   * Remove a single job from the configured dead letter queue.
   * Returns false when no DLQ is configured or the DLQ job does not exist.
   */
  async removeDeadLetterJob(jobId: string): Promise<boolean> {
    const client = await this.getClient();
    const dlqName = await this.resolveDeadLetterQueueName(client);
    if (!dlqName) return false;
    const dlqKeys = buildKeys(dlqName, this.opts.prefix);
    // Ownership check requires the envelope (originalQueue lives in data); excludeData
    // is honoured inside getDeadLetterJob, which still loads the envelope internally.
    const existing = await this.getDeadLetterJob(jobId, { excludeData: true });
    if (!existing) return false;
    await removeJob(client, dlqKeys, jobId);
    return true;
  }

  /**
   * Replay a job from the dead letter queue back to its original queue.
   * Returns the newly added job, or null when the DLQ job does not exist.
   */
  async replayDeadLetterJob(jobId: string): Promise<Job<any, any> | null> {
    const dlqJob = await this.getDeadLetterJob(jobId);
    if (!dlqJob) return null;

    const originalQueueName = this.getDeadLetterOriginalQueue(dlqJob);
    if (!originalQueueName) {
      throw new GlideMQError('DLQ entry is missing originalQueue metadata');
    }
    validateQueueName(originalQueueName);

    const client = await this.getClient();
    const originalQueue = new Queue<any, any>(originalQueueName, {
      client,
      compression: this.opts.compression,
      connection: this.opts.connection,
      prefix: this.opts.prefix,
      serializer: this.serializer,
    });

    try {
      const envelope = dlqJob.data as { originalJobId?: string; data?: unknown } | undefined;
      let replayData = envelope?.data;
      let replayOpts: JobOptions | undefined;
      if (envelope?.originalJobId) {
        const originalJob = await originalQueue.getJob(envelope.originalJobId);
        if (originalJob) {
          replayData = originalJob.data;
          replayOpts = { ...originalJob.opts };
        }
      }

      if (replayOpts) {
        delete replayOpts.jobId;
        delete replayOpts.delay;
        delete replayOpts.deduplication;
        delete replayOpts.parent;
      }

      const replayed = await originalQueue.add(dlqJob.name, replayData ?? null, replayOpts);
      if (!replayed) {
        throw new GlideMQError('DLQ replay was skipped due to duplicate or deduplicated job constraints');
      }

      await this.removeDeadLetterJob(jobId);
      return replayed;
    } finally {
      await originalQueue.close().catch(() => undefined);
    }
  }

  private getDeadLetterOriginalQueue(job: Job<any, any>): string | null {
    const data = job.data as { originalQueue?: unknown } | undefined;
    return typeof data?.originalQueue === 'string' ? data.originalQueue : null;
  }

  private isDeadLetterJobOwnedByQueue(job: Job<any, any>): boolean {
    return this.getDeadLetterOriginalQueue(job) === this.name;
  }

  /**
   * Retrieve jobs currently in the suspended state.
   * Results are ordered by the suspended ZSet score (timeout deadline).
   */
  async getSuspendedJobs(start = 0, end = -1, opts?: GetJobsOptions): Promise<Job<D, R>[]> {
    const client = await this.getClient();
    const members = await client.zrange(this.keys.suspended, { start, end: end >= 0 ? end : -1 });
    const jobIds = members.map((member) => String(member));
    return this.loadJobsByIdsForKeys(client, this.keys, jobIds, {
      excludeData: opts?.excludeData,
      serializer: this.serializer,
    });
  }

  /**
   * Rate-limit a specific ordering group from outside the worker processor.
   * Registers the group in the ratelimited ZADD — the scheduler will unblock it after duration.
   * Any in-flight job for the group continues; new activations are blocked until resumeAt.
   */
  async rateLimitGroup(groupKey: string, duration: number, opts?: { extend?: 'max' | 'replace' }): Promise<number> {
    if (!groupKey) throw new Error('groupKey must be a non-empty string');
    if (!Number.isFinite(duration) || duration <= 0) throw new Error('duration must be a positive finite number');
    const client = await this.getClient();
    return rateLimitGroupExternal(client, this.keys, groupKey, duration, Date.now(), opts?.extend ?? 'max');
  }

  /**
   * Send a signal to a suspended job, resuming it.
   * The job moves back to waiting state and re-enters the stream.
   * Returns true if the job was resumed, false if it was not in suspended state.
   */
  async signal(jobId: string, signalName: string, data?: any): Promise<boolean> {
    const client = await this.getClient();
    const serialized = data !== undefined ? JSON.stringify(data) : '';
    const result = await signalJob(client, this.keys, jobId, signalName, serialized, Date.now());
    return String(result) === 'ok';
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
    const client = await this.getClient();
    const values = await client.hmget(this.keys.job(jobId), [
      'state',
      'suspendReason',
      'suspendedAt',
      'suspendTimeout',
      'signals',
    ]);
    if (!values || String(values[0]) !== 'suspended') return null;
    let signals: SignalEntry[] = [];
    if (values[4]) {
      try {
        const raw = JSON.parse(String(values[4])) as any[];
        signals = raw.map((s) => ({
          ...s,
          data:
            typeof s.data === 'string'
              ? (() => {
                  try {
                    return JSON.parse(s.data);
                  } catch {
                    return s.data;
                  }
                })()
              : s.data,
        }));
      } catch {
        // ignore parse errors
      }
    }
    return {
      reason: values[1] ? String(values[1]) : undefined,
      suspendedAt: values[2] ? parseInt(String(values[2]), 10) : 0,
      timeout: values[3] ? parseInt(String(values[3]), 10) : undefined,
      signals,
    };
  }

  // ---- Valkey Search / Vector Index ----

  /**
   * Create a Valkey Search index over job hashes for this queue.
   * Auto-includes base fields: name (TAG), state (TAG), timestamp (NUMERIC), priority (NUMERIC).
   *
   * The index uses a queue-specific prefix that uniquely matches this queue's job hashes.
   * Requires the valkey-search module to be loaded on the server (standalone mode).
   */
  async createJobIndex(opts?: JobIndexOptions): Promise<void> {
    const client = await this.getClient();
    await this.ensureSearchModule(client);
    const indexName = opts?.name ?? `${this.name}-idx`;
    // Valkey Search rejects complete hash tags ({...}) in prefixes.
    // Use a partial prefix that includes the opening brace and queue name
    // but omits the closing brace. This uniquely matches this queue's keys
    // (e.g. "glide:{myqueue" matches "glide:{myqueue}:job:*") without
    // triggering the hash tag validator.
    const pfxBase = this.opts.prefix ?? 'glide';
    const searchPrefix = `${pfxBase}:{${this.name}`;

    const schema: Field[] = [
      { type: 'TAG', name: 'name' },
      { type: 'TAG', name: 'state' },
      { type: 'NUMERIC', name: 'timestamp' },
      { type: 'NUMERIC', name: 'priority' },
    ];

    if (opts?.fields) {
      schema.push(...opts.fields);
    }

    if (opts?.vectorField) {
      const vf = opts.vectorField;
      const algorithm = vf.algorithm ?? 'HNSW';
      const distanceMetric = vf.distanceMetric ?? 'COSINE';
      if (algorithm === 'HNSW') {
        schema.push({
          type: 'VECTOR',
          name: vf.name,
          attributes: {
            algorithm: 'HNSW',
            dimensions: vf.dimensions,
            distanceMetric,
            type: 'FLOAT32',
          },
        });
      } else {
        schema.push({
          type: 'VECTOR',
          name: vf.name,
          attributes: {
            algorithm: 'FLAT',
            dimensions: vf.dimensions,
            distanceMetric,
            type: 'FLOAT32',
          },
        });
      }
    } else {
      // valkey-search requires at least one vector field; add a minimal placeholder
      schema.push({
        type: 'VECTOR',
        name: '_vec',
        attributes: {
          algorithm: 'FLAT',
          dimensions: 2,
          distanceMetric: 'COSINE',
          type: 'FLOAT32',
        },
      });
    }

    await GlideFt.create(client, indexName, schema, {
      ...(opts?.createOptions ? mapIndexCreateOptions(opts.createOptions) : {}),
      dataType: 'HASH',
      prefixes: [searchPrefix],
    });
  }

  /**
   * Drop a Valkey Search index. Indexed document keys (job hashes) are not affected.
   */
  async dropJobIndex(name?: string): Promise<void> {
    const client = await this.getClient();
    const indexName = name ?? `${this.name}-idx`;
    await GlideFt.dropindex(client, indexName);
  }

  /**
   * Search for jobs by vector similarity (KNN) using a Valkey Search index.
   * Requires a prior call to createJobIndex with a vectorField configured.
   *
   * The search is automatically scoped to this queue via the index prefix.
   *
   * @param embedding - The query vector (number[] or Float32Array).
   * @param opts - Search options (index name, k, pre-filter, return fields, score field).
   * @returns Array of { job, score } sorted by similarity (best first).
   */
  async vectorSearch(
    embedding: number[] | Float32Array,
    opts?: VectorSearchOptions,
  ): Promise<VectorSearchResult<D, R>[]> {
    const client = await this.getClient();
    const indexName = opts?.indexName ?? `${this.name}-idx`;
    const k = opts?.k ?? 10;
    const filter = opts?.filter ?? '*';
    const scoreField = opts?.scoreField ?? '__score';

    // Determine vector field name from index info.
    // The info response format varies across valkey-search versions:
    // - v1.x: fields in info.attributes as array of arrays
    // - later: fields in info.fields as array of objects
    const info = await GlideFt.info(client, indexName);
    let vecFieldName = '_vec';
    const attrs = (info.attributes ?? info.fields) as any;
    if (Array.isArray(attrs)) {
      for (const attr of attrs) {
        if (Array.isArray(attr)) {
          // v1.x format: ["identifier","vec","attribute","vec","type","VECTOR",...]
          const typeIdx = attr.indexOf('type');
          if (typeIdx >= 0 && String(attr[typeIdx + 1]) === 'VECTOR') {
            const idIdx = attr.indexOf('identifier');
            if (idIdx >= 0) vecFieldName = String(attr[idIdx + 1]);
            break;
          }
        } else if (typeof attr === 'object' && attr !== null) {
          // Object format: { type: 'VECTOR', identifier: '...', ... }
          if (String(attr.type) === 'VECTOR') {
            vecFieldName = String(attr.field_name || attr.identifier);
            break;
          }
        }
      }
    }

    // Convert embedding to Buffer
    const arr = embedding instanceof Float32Array ? embedding : new Float32Array(embedding);
    const vecBuf = Buffer.from(arr.buffer, arr.byteOffset, arr.byteLength);

    const query = `${filter}=>[KNN ${k} @${vecFieldName} $BLOB AS ${scoreField}]`;
    const searchOpts: Record<string, any> = {
      ...(opts?.searchOptions ? mapSearchQueryOptions(opts.searchOptions) : {}),
      params: [{ key: 'BLOB', value: vecBuf }],
      dialect: 2,
      // Request only the score field from the search result to avoid
      // binary vector data causing UTF-8 decode errors. Full job data
      // is fetched separately via HMGET.
      returnFields: [{ fieldIdentifier: scoreField }],
    };

    const [totalCount, records] = await GlideFt.search(client, indexName, query, searchOpts);

    const results: VectorSearchResult<D, R>[] = [];
    if (totalCount === 0 || !records || records.length === 0) return results;

    // Fetch job data using HMGET to avoid reading binary vector fields
    // (HGETALL would include vector buffers that fail UTF-8 string decoding).
    const JOB_TEXT_FIELDS = ['data', 'returnvalue', ...JOB_METADATA_FIELDS] as string[];
    const batch = this.newBatch();
    const scoreMap = new Map<string, number>();
    const jobIds: string[] = [];

    for (const record of records) {
      const key = String(record.key);
      const lastColon = key.lastIndexOf(':');
      if (lastColon === -1) continue;
      const jobId = key.substring(lastColon + 1);

      let score = 0;
      for (const field of record.value) {
        if (String(field.key) === scoreField) {
          score = parseFloat(String(field.value));
          break;
        }
      }

      jobIds.push(jobId);
      scoreMap.set(jobId, score);
      (batch as any).hmget(this.keys.job(jobId), JOB_TEXT_FIELDS);
    }

    const batchResults = await client.exec(batch as any, false);
    if (!batchResults) return results;

    for (let i = 0; i < jobIds.length; i++) {
      const hash = hmgetArrayToRecord(batchResults[i] as (unknown | null)[], JOB_TEXT_FIELDS as readonly string[]);
      if (!hash) continue;
      const job = Job.fromHash<D, R>(client, this.keys, jobIds[i], hash, this.serializer);
      results.push({ job, score: scoreMap.get(jobIds[i]) ?? 0 });
    }

    return results;
  }

  /**
   * Check that the valkey-search module is available. Caches the result.
   * Throws a clear error if the module is not loaded.
   * @internal
   */
  private async ensureSearchModule(client: Client): Promise<void> {
    if (this.searchModuleAvailable === true) return;
    try {
      await GlideFt.list(client);
      this.searchModuleAvailable = true;
    } catch {
      this.searchModuleAvailable = false;
      throw new GlideMQError(
        'Valkey Search module is not available. Install valkey-bundle or load the search module to use index features.',
      );
    }
  }

  /**
   * Close the queue and release the underlying client connection.
   * Idempotent: safe to call multiple times.
   */
  async close(): Promise<void> {
    if (this.closing) return;
    this.closing = true;
    this.clearSuspendSweepLoop();
    if (this.suspendedSweepTasks.size > 0) {
      await Promise.allSettled(this.suspendedSweepTasks);
    }
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
    const handleCloseError = (err: unknown) => {
      // Speedkey can reject close() if the transport is already shutting down.
      // Queue.close() is intentionally idempotent, so treat that as best-effort cleanup.
      if ((err as { name?: string } | null)?.name === 'ClosingError') return;
      if (this.listenerCount('error') > 0) {
        this.emit('error', err as Error);
      }
    };
    for (const rejectWaiter of this.waitRejectors) {
      try {
        rejectWaiter(new GlideMQError('Queue is closing'));
      } catch {
        /* ignore */
      }
    }
    this.waitRejectors.clear();
    const closePromises: Promise<void>[] = [];
    for (const waitClient of this.waitClients) {
      closePromises.push(
        Promise.resolve()
          .then(() => waitClient.close())
          .catch(handleCloseError),
      );
    }
    this.waitClients.clear();
    if (this.client) {
      const client = this.client;
      this.client = null;
      if (this.clientOwned) {
        closePromises.push(
          Promise.resolve()
            .then(() => client.close())
            .catch(handleCloseError),
        );
      }
    }
    await Promise.all(closePromises);
  }
}
