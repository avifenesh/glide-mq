import { EventEmitter } from 'events';
import { randomBytes } from 'crypto';
import { InfBoundary, Batch, ClusterBatch, ClusterScanCursor } from '@glidemq/speedkey';
import type { GlideClient, GlideClusterClient } from '@glidemq/speedkey';
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
  RateLimitConfig,
  WorkerInfo,
  Serializer,
} from './types';
import { JSON_SERIALIZER } from './types';
import { Job } from './job';
import {
  buildKeys,
  keyPrefix,
  keyPrefixPattern,
  nextReconnectDelay,
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
} from './functions/index';
import type { QueueKeys } from './functions/index';
import { withSpan } from './telemetry';

const MAX_ORDERING_KEY_LENGTH = 256;
const PIPELINE_CHUNK_SIZE = 1000;
const SCHEDULER_LOCK_TTL_MS = 5000;
const SCHEDULER_LOCK_RETRY_DELAY_MS = 25;
const SCHEDULER_LOCK_MAX_ATTEMPTS = Math.ceil(SCHEDULER_LOCK_TTL_MS / SCHEDULER_LOCK_RETRY_DELAY_MS);

function validateOrderingKey(orderingKey: string): void {
  if (orderingKey.length > MAX_ORDERING_KEY_LENGTH) {
    throw new Error(`Ordering key exceeds maximum length (${orderingKey.length} > ${MAX_ORDERING_KEY_LENGTH}).`);
  }
}

const INVALID_JOB_ID_CHARS = /[\x00-\x1f\x7f{}:]/;
function validateJobId(jobId: string): void {
  if (jobId.length > 256) throw new Error('jobId must be at most 256 characters');
  if (INVALID_JOB_ID_CHARS.test(jobId)) {
    throw new Error('jobId must not contain control characters, curly braces, or colons');
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
  private waitClients: Set<Client> = new Set();
  private waitRejectors: Set<(err: Error) => void> = new Set();
  private keys: QueueKeys;

  private serializer: Serializer;

  constructor(name: string, opts: QueueOptions) {
    super();
    if (!opts.connection && !opts.client) {
      throw new GlideMQError('Either `connection` or `client` must be provided.');
    }
    this.name = name;
    this.opts = opts;
    this.serializer = opts.serializer ?? JSON_SERIALIZER;
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
        const byteLen = Buffer.byteLength(serialized, 'utf8');
        if (byteLen > MAX_JOB_DATA_SIZE) {
          throw new Error(
            `Job data exceeds maximum size (${byteLen} bytes > ${MAX_JOB_DATA_SIZE} bytes). Use smaller payloads or store large data externally.`,
          );
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
            ttl,
            customJobId,
            lifo ? 1 : 0,
            parentQueue,
            parentDepsKey,
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
      },
    );
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
    let handedOff = false;
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
      handedOff = true;
      return this.waitForJobResult(blockingClient, job.id, eventCursor, waitTimeout);
    } finally {
      if (!handedOff) {
        this.waitClients.delete(blockingClient);
        blockingClient.close();
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
      const byteLen = Buffer.byteLength(serializedData, 'utf8');
      if (byteLen > MAX_JOB_DATA_SIZE) {
        throw new Error(
          `Job data exceeds maximum size (${byteLen} bytes > ${MAX_JOB_DATA_SIZE} bytes). Use smaller payloads or store large data externally.`,
        );
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
      if ((groupRateMax > 0 || tbCapacity > 0) && groupConcurrency < 1) {
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
          for (const entryId in entries) {
            if (!Object.prototype.hasOwnProperty.call(entries, entryId)) continue;
            const fieldPairs = entries[entryId];
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
      this.waitClients.delete(blockingClient);
      try {
        blockingClient.close();
      } catch (closeErr) {
        if (this.listenerCount('error') > 0) {
          this.emit('error', closeErr as Error);
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
   * Retrieve jobs from the dead letter queue configured for this queue.
   * Returns an empty array if no DLQ is configured.
   * @param start - Start index (default 0)
   * @param end - End index (default -1, meaning all)
   * @param opts - Set `excludeData: true` to omit `data` and `returnvalue` fields
   */
  async getDeadLetterJobs(start = 0, end = -1, opts?: GetJobsOptions): Promise<Job<D, R>[]> {
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
    const excludeData = opts?.excludeData === true;
    const jobs: Job<D, R>[] = [];
    for (let offset = 0; offset < sliced.length; offset += PIPELINE_CHUNK_SIZE) {
      const chunk = sliced.slice(offset, offset + PIPELINE_CHUNK_SIZE);
      const batch = this.newBatch();
      if (excludeData) {
        for (const id of chunk) (batch as any).hmget(dlqKeys.job(id), JOB_METADATA_FIELDS);
      } else {
        for (const id of chunk) (batch as any).hgetall(dlqKeys.job(id));
      }
      const batchResults = await client.exec(batch as any, false);
      if (batchResults) {
        for (let i = 0; i < batchResults.length; i++) {
          const hash = excludeData
            ? hmgetArrayToRecord(batchResults[i] as (unknown | null)[], JOB_METADATA_FIELDS)
            : hashDataToRecord(batchResults[i] as any);
          if (!hash) continue;
          // DLQ envelope is always JSON (written by Worker.moveToDLQ with JSON.stringify),
          // regardless of the queue's custom serializer.
          jobs.push(Job.fromHash<D, R>(client, dlqKeys, chunk[i], hash, undefined, excludeData));
        }
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
    for (const rejectWaiter of this.waitRejectors) {
      try {
        rejectWaiter(new GlideMQError('Queue is closing'));
      } catch {
        /* ignore */
      }
    }
    this.waitRejectors.clear();
    for (const waitClient of this.waitClients) {
      try {
        waitClient.close();
      } catch (closeErr) {
        if (this.listenerCount('error') > 0) {
          this.emit('error', closeErr as Error);
        }
      }
    }
    this.waitClients.clear();
    if (this.client) {
      if (this.clientOwned) {
        this.client.close();
      }
      this.client = null;
    }
  }
}
