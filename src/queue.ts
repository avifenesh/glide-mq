import { EventEmitter } from 'events';
import { InfBoundary, Batch, ClusterBatch, ClusterScanCursor } from '@glidemq/speedkey';
import type { GlideClient, GlideClusterClient } from '@glidemq/speedkey';
import type {
  QueueOptions,
  JobOptions,
  Client,
  ScheduleOpts,
  JobTemplate,
  SchedulerEntry,
  Metrics,
  JobCounts,
  SearchJobsOptions,
  RateLimitConfig,
} from './types';
import { Job } from './job';
import {
  buildKeys,
  keyPrefix,
  keyPrefixPattern,
  nextCronOccurrence,
  hashDataToRecord,
  extractJobIdsFromStreamEntries,
  compress,
} from './utils';
import { createClient, ensureFunctionLibrary, ensureFunctionLibraryOnce, isClusterClient } from './connection';
import { GlideMQError } from './errors';
import {
  LIBRARY_SOURCE,
  CONSUMER_GROUP,
  addJob,
  dedup,
  pause,
  resume,
  revokeJob,
  searchByName,
  cleanJobs,
  drainQueue,
  retryJobs,
} from './functions/index';
import type { QueueKeys } from './functions/index';
import { withSpan } from './telemetry';

const MAX_ORDERING_KEY_LENGTH = 256;

function validateOrderingKey(orderingKey: string): void {
  if (orderingKey.length > MAX_ORDERING_KEY_LENGTH) {
    throw new Error(`Ordering key exceeds maximum length (${orderingKey.length} > ${MAX_ORDERING_KEY_LENGTH}).`);
  }
}

/** Check if all key-value pairs in filter exist in data (shallow match). */
function matchesData(data: Record<string, unknown>, filter: Record<string, unknown>): boolean {
  for (const [key, value] of Object.entries(filter)) {
    if (data[key] !== value) return false;
  }
  return true;
}

export class Queue<D = any, R = any> extends EventEmitter {
  readonly name: string;
  private opts: QueueOptions;
  private client: Client | null = null;
  private clientOwned = true;
  private _clusterMode: boolean | undefined;
  private closing = false;
  private keys: QueueKeys;

  constructor(name: string, opts: QueueOptions) {
    super();
    if (!opts.connection && !opts.client) {
      throw new GlideMQError('Either `connection` or `client` must be provided.');
    }
    this.name = name;
    this.opts = opts;
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
    if (results) {
      for (const result of results) {
        if (result) jobIds.push(...extractJobIdsFromStreamEntries(result as any));
      }
    }
    return jobIds;
  }

  /** @internal */
  async getClient(): Promise<Client> {
    if (this.closing) {
      throw new GlideMQError('Queue is closing');
    }
    if (!this.client) {
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
      } else {
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
      }
    }
    return this.client;
  }

  /**
   * Add a single job to the queue.
   * Uses the glidemq_addJob server function to atomically create the job hash
   * and enqueue it to the stream (or scheduled ZSet if delayed/prioritized).
   */
  async add(name: string, data: D, opts?: JobOptions): Promise<Job<D, R> | null> {
    const delay = opts?.delay ?? 0;
    const priority = opts?.priority ?? 0;

    return withSpan(
      'glide-mq.queue.add',
      {
        'glide-mq.queue': this.name,
        'glide-mq.job.name': name,
        'glide-mq.job.delay': delay,
        'glide-mq.job.priority': priority,
      },
      async (span) => {
        const client = await this.getClient();
        const timestamp = Date.now();
        const parentId = opts?.parent ? opts.parent.id : '';
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
          if (!Number.isFinite(opts.cost) || opts.cost < 0)
            throw new Error('cost must be a non-negative finite number');
          jobCost = Math.round(opts.cost * 1000);
        }
        let groupConcurrency = opts?.ordering?.concurrency ?? 0;
        // Force group path when rate limit or token bucket is set
        if ((groupRateMax > 0 || tbCapacity > 0) && groupConcurrency < 1) {
          groupConcurrency = 1;
        }
        validateOrderingKey(orderingKey);

        // Payload size validation - prevent DoS via oversized jobs
        let serialized = JSON.stringify(data);
        if (serialized.length > 1_048_576) {
          throw new Error(
            `Job data exceeds maximum size (${serialized.length} bytes > 1MB). Use smaller payloads or store large data externally.`,
          );
        }

        if (this.opts.compression === 'gzip') {
          serialized = compress(serialized);
        }

        let jobId: string;

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
          );
          if (result === 'skipped') {
            return null;
          }
          if (result === 'ERR:COST_EXCEEDS_CAPACITY') {
            throw new Error('Job cost exceeds token bucket capacity');
          }
          jobId = result;
        } else {
          const result = await addJob(
            client,
            this.keys,
            name,
            serialized,
            JSON.stringify(opts ?? {}),
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
          );
          if (result === 'ERR:COST_EXCEEDS_CAPACITY') {
            throw new Error('Job cost exceeds token bucket capacity');
          }
          jobId = result;
        }

        span.setAttribute('glide-mq.job.id', String(jobId));

        const job = new Job<D, R>(client, this.keys, String(jobId), name, data, opts ?? {});
        job.timestamp = timestamp;
        job.parentId = parentId || undefined;
        return job;
      },
    );
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
      const maxAttempts = opts.attempts ?? 0;
      const orderingKey = opts.ordering?.key ?? '';
      validateOrderingKey(orderingKey);
      const deduplication = opts.deduplication;

      let serializedData = JSON.stringify(entry.data);
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
      if ((groupRateMax > 0 || tbCapacity > 0) && groupConcurrency < 1) {
        groupConcurrency = 1;
      }
      return {
        entry,
        opts,
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
        deduplication,
        serializedData,
      };
    });

    // Build a batch with all fcall commands
    const keys = [this.keys.id, this.keys.stream, this.keys.scheduled, this.keys.events];
    const dedupKeys = [this.keys.dedup, this.keys.id, this.keys.stream, this.keys.scheduled, this.keys.events];
    const batch = isCluster ? new ClusterBatch(false) : new Batch(false);

    for (const p of prepared) {
      if (p.deduplication) {
        batch.fcall('glidemq_dedup', dedupKeys, [
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
        ]);
      } else {
        batch.fcall('glidemq_addJob', keys, [
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
        ]);
      }
    }

    const rawResults = isCluster
      ? await (client as GlideClusterClient).exec(batch as ClusterBatch, true)
      : await (client as GlideClient).exec(batch as Batch, true);

    return this.buildBulkJobs(client, prepared, rawResults, timestamp);
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
      if (raw === 'skipped') return [];
      if (raw === 'ERR:COST_EXCEEDS_CAPACITY') {
        throw new Error('Job cost exceeds token bucket capacity');
      }
      const jobId = raw;
      const job = new Job<D, R>(client, this.keys, jobId, p.entry.name, p.entry.data, p.opts);
      job.timestamp = timestamp;
      job.parentId = p.parentId || undefined;
      return [job];
    });
  }

  /**
   * Retrieve a job by ID from the queue.
   * Returns null if the job does not exist.
   */
  async getJob(id: string): Promise<Job<D, R> | null> {
    const client = await this.getClient();
    const hashData = await client.hgetall(this.keys.job(id));
    const hash = hashDataToRecord(hashData);
    if (!hash) return null;

    return Job.fromHash<D, R>(client, this.keys, id, hash);
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
   * Upsert a job scheduler (repeatable/cron job).
   * Stores the scheduler config in the schedulers hash.
   * Computes the initial nextRun based on the schedule.
   */
  async upsertJobScheduler(name: string, schedule: ScheduleOpts, template?: JobTemplate): Promise<void> {
    const client = await this.getClient();
    const now = Date.now();

    let nextRun: number;
    if (schedule.pattern) {
      nextRun = nextCronOccurrence(schedule.pattern, now);
    } else if (schedule.every) {
      nextRun = now + schedule.every;
    } else {
      throw new Error('Schedule must have either pattern (cron) or every (ms interval)');
    }

    const entry: SchedulerEntry = {
      pattern: schedule.pattern,
      every: schedule.every,
      template,
      nextRun,
    };

    await client.hset(this.keys.schedulers, { [name]: JSON.stringify(entry) });
  }

  /**
   * Remove a job scheduler by name.
   */
  async removeJobScheduler(name: string): Promise<void> {
    const client = await this.getClient();
    await client.hdel(this.keys.schedulers, [name]);
  }

  /**
   * Get metrics for completed or failed jobs.
   * Returns the count of entries in the corresponding ZSet.
   */
  async getMetrics(type: 'completed' | 'failed'): Promise<Metrics> {
    const client = await this.getClient();
    const key = type === 'completed' ? this.keys.completed : this.keys.failed;
    const count = await client.zcard(key);
    return { count };
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
   * - waiting: stream length minus active (pending) entries
   * - active: PEL count from XPENDING
   * - delayed: scheduled ZSet cardinality (includes both delayed and prioritized)
   * - completed: completed ZSet cardinality
   * - failed: failed ZSet cardinality
   */
  async getJobCounts(): Promise<JobCounts> {
    const client = await this.getClient();

    const [streamLen, completedCount, failedCount, scheduledCount] = await Promise.all([
      client.xlen(this.keys.stream),
      client.zcard(this.keys.completed),
      client.zcard(this.keys.failed),
      client.zcard(this.keys.scheduled),
    ]);

    // XPENDING returns [pendingCount, minId, maxId, consumers[]]
    let activeCount = 0;
    try {
      const pendingInfo = await client.xpending(this.keys.stream, CONSUMER_GROUP);
      activeCount = Number(pendingInfo[0]) || 0;
    } catch {
      // Consumer group may not exist yet
    }

    const waiting = Math.max(0, streamLen - activeCount);

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

    for (const pattern of [jobPattern, logPattern, depsPattern, groupPattern, groupqPattern, orderPendingPattern]) {
      await this.scanAndDelete(client, pattern);
    }
  }

  /**
   * Scan for keys matching a pattern and delete them in batches.
   * Handles both standalone (GlideClient) and cluster (GlideClusterClient) scan APIs.
   * @internal
   */
  private async scanAndDelete(client: Client, pattern: string): Promise<void> {
    if (this.clusterMode) {
      const clusterClient = client as GlideClusterClient;
      let cursor = new ClusterScanCursor();
      while (!cursor.isFinished()) {
        const [nextCursor, keys] = await clusterClient.scan(cursor, { match: pattern, count: 100 });
        cursor = nextCursor;
        if (keys.length > 0) {
          await client.del(keys);
        }
      }
    } else {
      let cursor = '0';
      do {
        const result = await (client as GlideClient).scan(cursor, { match: pattern, count: 100 });
        cursor = result[0] as string;
        const keys = result[1];
        if (keys.length > 0) {
          await client.del(keys);
        }
      } while (cursor !== '0');
    }
  }

  /**
   * Retrieve jobs by state with optional pagination.
   * @param type - The job state to query
   * @param start - Start index for pagination (default 0)
   * @param end - End index for pagination (default -1, meaning all)
   */
  async getJobs(
    type: 'waiting' | 'active' | 'delayed' | 'completed' | 'failed',
    start = 0,
    end = -1,
  ): Promise<Job<D, R>[]> {
    const client = await this.getClient();
    let jobIds: string[];

    switch (type) {
      case 'waiting': {
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
        try {
          const pendingEntries = await client.xpendingWithOptions(this.keys.stream, CONSUMER_GROUP, {
            start: InfBoundary.NegativeInfinity,
            end: InfBoundary.PositiveInfinity,
            count: end >= 0 ? end + 1 : 10000,
          });
          const entryIds = pendingEntries.slice(start, end >= 0 ? end + 1 : undefined).map((e) => String(e[0]));
          jobIds = await this.resolveActiveJobIds(client, entryIds);
        } catch {
          jobIds = [];
        }
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
    const batch = this.newBatch();
    for (const id of jobIds) (batch as any).hgetall(this.keys.job(id));
    const batchResults = await client.exec(batch as any, false);
    const jobs: Job<D, R>[] = [];
    if (batchResults) {
      for (let i = 0; i < batchResults.length; i++) {
        const hash = hashDataToRecord(batchResults[i] as any);
        if (hash) jobs.push(Job.fromHash<D, R>(client, this.keys, jobIds[i], hash));
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

    // Fetch full job objects
    const jobs: Job<D, R>[] = [];
    for (const id of jobIds) {
      if (jobs.length >= limit) break;
      const job = await this.getJob(id);
      if (!job) continue;

      // Apply name filter if we used a non-Lua path
      if (opts.name && !opts.state && job.name !== opts.name) continue;

      // Apply data filter (shallow key-value match)
      if (opts.data && !matchesData(job.data as Record<string, unknown>, opts.data)) continue;

      jobs.push(job);
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
    return val === '1';
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
      result.push({
        name: String(item.field),
        entry: JSON.parse(String(item.value)),
      });
    }
    return result;
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
   * Retrieve jobs from the dead letter queue configured for this queue.
   * Returns an empty array if no DLQ is configured.
   * @param start - Start index (default 0)
   * @param end - End index (default -1, meaning all)
   */
  async getDeadLetterJobs(start = 0, end = -1): Promise<Job<D, R>[]> {
    if (!this.opts.deadLetterQueue) return [];
    const client = await this.getClient();
    const dlqKeys = buildKeys(this.opts.deadLetterQueue.name, this.opts.prefix);

    const entries = await client.xrange(
      dlqKeys.stream,
      InfBoundary.NegativeInfinity,
      InfBoundary.PositiveInfinity,
      end >= 0 ? { count: end + 1 } : undefined,
    );
    if (!entries) return [];

    const jobIds = extractJobIdsFromStreamEntries(entries);
    const sliced = jobIds.slice(start, end >= 0 ? end + 1 : undefined);
    if (sliced.length === 0) return [];
    const batch = this.newBatch();
    for (const id of sliced) (batch as any).hgetall(dlqKeys.job(id));
    const batchResults = await client.exec(batch as any, false);
    const jobs: Job<D, R>[] = [];
    if (batchResults) {
      for (let i = 0; i < batchResults.length; i++) {
        const hash = hashDataToRecord(batchResults[i] as any);
        if (!hash) continue;
        jobs.push(Job.fromHash<D, R>(client, dlqKeys, sliced[i], hash));
      }
    }
    return jobs;
  }

  /**
   * Close the queue and release the underlying client connection.
   * Idempotent: safe to call multiple times.
   */
  async close(): Promise<void> {
    if (this.closing) return;
    this.closing = true;
    if (this.client) {
      if (this.clientOwned) {
        this.client.close();
      }
      this.client = null;
    }
  }
}
