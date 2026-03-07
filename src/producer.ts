/**
 * Lightweight Producer for serverless/edge environments.
 *
 * Reuses the same addJob/dedup FCALL functions as Queue but without EventEmitter,
 * Job instances, or state tracking. Returns plain string IDs.
 */
import { Batch, ClusterBatch } from '@glidemq/speedkey';
import type { GlideClient, GlideClusterClient } from '@glidemq/speedkey';
import type { ConnectionOptions, JobOptions, Client, Serializer } from './types';
import { JSON_SERIALIZER } from './types';
import { buildKeys, keyPrefix, compress, MAX_JOB_DATA_SIZE } from './utils';
import { createClient, ensureFunctionLibraryOnce, isClusterClient } from './connection';
import { GlideMQError } from './errors';
import { LIBRARY_SOURCE, addJob, dedup } from './functions/index';
import type { QueueKeys } from './functions/index';

const MAX_ORDERING_KEY_LENGTH = 256;

const INVALID_JOB_ID_CHARS = /[\x00-\x1f\x7f{}:]/;

function validateOrderingKey(orderingKey: string): void {
  if (orderingKey.length > MAX_ORDERING_KEY_LENGTH) {
    throw new GlideMQError(`Ordering key exceeds maximum length (${orderingKey.length} > ${MAX_ORDERING_KEY_LENGTH}).`);
  }
}

function validateJobId(jobId: string): void {
  if (jobId.length > 256) throw new GlideMQError('jobId must be at most 256 characters');
  if (INVALID_JOB_ID_CHARS.test(jobId)) {
    throw new GlideMQError('jobId must not contain control characters, curly braces, or colons');
  }
}

export interface ProducerOptions {
  /** Connection options for creating a new client. Required unless `client` is provided. */
  connection?: ConnectionOptions;
  /** Pre-existing GLIDE client. When provided, the Producer does NOT own this client - close() will not destroy it. */
  client?: Client;
  /** Key prefix. Default: 'glide'. */
  prefix?: string;
  /** Enable transparent compression of job data. Default: 'none'. */
  compression?: 'none' | 'gzip';
  /** Custom serializer for job data. Default: JSON. */
  serializer?: Serializer;
}

interface PreparedJob {
  jobName: string;
  serializedData: string;
  optsJson: string;
  timestamp: number;
  delay: number;
  priority: number;
  parentId: string;
  parentQueue: string;
  maxAttempts: number;
  orderingKey: string;
  groupConcurrency: number;
  groupRateMax: number;
  groupRateDuration: number;
  tbCapacity: number;
  tbRefillRate: number;
  jobCost: number;
  ttl: number;
  customJobId: string;
  parentDepsKey: string;
  deduplication?: { id: string; ttl?: number; mode?: 'simple' | 'throttle' | 'debounce' };
}

export class Producer<D = any> {
  readonly name: string;
  private opts: ProducerOptions;
  private client: Client | null = null;
  private clientOwned = true;
  private _clusterMode: boolean | undefined;
  private closing = false;
  private keys: QueueKeys;
  private serializer: Serializer;

  constructor(name: string, opts: ProducerOptions) {
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

  private get clusterMode(): boolean {
    if (this._clusterMode !== undefined) return this._clusterMode;
    if (this.client) {
      this._clusterMode = isClusterClient(this.client);
      return this._clusterMode;
    }
    return false;
  }

  private async getClient(): Promise<Client> {
    if (this.closing) {
      throw new GlideMQError('Producer is closed');
    }
    if (!this.client) {
      if (this.opts.client) {
        const injected = this.opts.client;
        await ensureFunctionLibraryOnce(
          injected,
          LIBRARY_SOURCE,
          this.opts.connection?.clusterMode ?? isClusterClient(injected),
        );
        this.client = injected;
        this.clientOwned = false;
      } else {
        const client = await createClient(this.opts.connection!);
        await ensureFunctionLibraryOnce(client, LIBRARY_SOURCE, this.opts.connection!.clusterMode ?? false);
        this.client = client;
        this.clientOwned = true;
      }
    }
    return this.client;
  }

  /**
   * Register a job as a child in a cross-queue parent's dependency set.
   */
  private async registerCrossQueueParent(parentId: string, parentQueue: string, jobId: string): Promise<void> {
    const client = await this.getClient();
    const parentKeys = buildKeys(parentQueue, this.opts.prefix);
    const pfx = keyPrefix(this.opts.prefix ?? 'glide', this.name);
    const depsMember = `${pfx}:${jobId}`;
    await client.sadd(parentKeys.deps(parentId), [depsMember]);
  }

  /**
   * Validate and prepare job parameters. Shared between add() and addBulk().
   */
  private prepareJobParams(jobName: string, data: D, opts?: JobOptions): PreparedJob {
    const delay = opts?.delay ?? 0;
    const priority = opts?.priority ?? 0;
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
        throw new GlideMQError('tokenBucket.capacity must be a positive finite number');
      if (!Number.isFinite(tb.refillRate) || tb.refillRate <= 0)
        throw new GlideMQError('tokenBucket.refillRate must be a positive finite number');
      tbCapacity = Math.round(tb.capacity * 1000);
      tbRefillRate = Math.round(tb.refillRate * 1000);
    }
    let jobCost = 0;
    if (opts?.cost != null) {
      if (!Number.isFinite(opts.cost) || opts.cost < 0)
        throw new GlideMQError('cost must be a non-negative finite number');
      jobCost = Math.round(opts.cost * 1000);
    }
    let groupConcurrency = opts?.ordering?.concurrency ?? 0;
    if ((groupRateMax > 0 || tbCapacity > 0) && groupConcurrency < 1) {
      groupConcurrency = 1;
    }
    validateOrderingKey(orderingKey);

    const customJobId = opts?.jobId ?? '';
    if (customJobId !== '') validateJobId(customJobId);

    if (opts?.ttl != null) {
      if (!Number.isFinite(opts.ttl) || opts.ttl < 0) throw new GlideMQError('ttl must be a non-negative finite number');
    }

    let serialized = this.serializer.serialize(data);
    const byteLen = Buffer.byteLength(serialized, 'utf8');
    if (byteLen > MAX_JOB_DATA_SIZE) {
      throw new GlideMQError(
        `Job data exceeds maximum size (${byteLen} bytes > ${MAX_JOB_DATA_SIZE} bytes). Use smaller payloads or store large data externally.`,
      );
    }

    if (this.opts.compression === 'gzip') {
      serialized = compress(serialized);
    }

    const ttl = opts?.ttl ?? 0;

    let parentDepsKey = '';
    if (parentId && parentQueue && parentQueue === this.name) {
      parentDepsKey = this.keys.deps(parentId);
    }

    return {
      jobName,
      serializedData: serialized,
      optsJson: JSON.stringify(opts ?? {}),
      timestamp: Date.now(),
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
      ttl,
      customJobId,
      parentDepsKey,
      deduplication: opts?.deduplication,
    };
  }

  /**
   * Add a single job to the queue.
   * Returns the job ID (string) or null if deduplicated/collision.
   */
  async add(name: string, data: D, opts?: JobOptions): Promise<string | null> {
    const client = await this.getClient();
    const p = this.prepareJobParams(name, data, opts);

    let jobId: string;

    if (p.deduplication) {
      const result = await dedup(
        client,
        this.keys,
        p.deduplication.id,
        p.deduplication.ttl ?? 0,
        p.deduplication.mode ?? 'simple',
        p.jobName,
        p.serializedData,
        p.optsJson,
        p.timestamp,
        p.delay,
        p.priority,
        p.parentId,
        p.maxAttempts,
        p.orderingKey,
        p.groupConcurrency,
        p.groupRateMax,
        p.groupRateDuration,
        p.tbCapacity,
        p.tbRefillRate,
        p.jobCost,
        p.ttl,
        p.customJobId,
        p.parentQueue,
        p.parentDepsKey,
      );
      if (result === 'skipped' || result === 'duplicate') {
        return null;
      }
      if (result === 'ERR:COST_EXCEEDS_CAPACITY') {
        throw new GlideMQError('Job cost exceeds token bucket capacity');
      }
      if (result === 'ERR:ID_EXHAUSTED') {
        throw new GlideMQError('Failed to generate job ID: too many collisions with custom job IDs');
      }
      jobId = result;

      // Cross-queue parent: register dedup child in parent deps separately
      if (p.parentId && p.parentQueue && p.parentQueue !== this.name) {
        await this.registerCrossQueueParent(p.parentId, p.parentQueue, jobId);
      }
    } else {
      const result = await addJob(
        client,
        this.keys,
        p.jobName,
        p.serializedData,
        p.optsJson,
        p.timestamp,
        p.delay,
        p.priority,
        p.parentId,
        p.maxAttempts,
        p.orderingKey,
        p.groupConcurrency,
        p.groupRateMax,
        p.groupRateDuration,
        p.tbCapacity,
        p.tbRefillRate,
        p.jobCost,
        p.ttl,
        p.customJobId,
        p.parentQueue,
        p.parentDepsKey,
      );
      if (result === 'duplicate') {
        return null;
      }
      if (result === 'ERR:COST_EXCEEDS_CAPACITY') {
        throw new GlideMQError('Job cost exceeds token bucket capacity');
      }
      if (result === 'ERR:ID_EXHAUSTED') {
        throw new GlideMQError('Failed to generate job ID: too many collisions with custom job IDs');
      }
      jobId = result;

      // Cross-queue parent: register child in parent deps separately
      if (p.parentId && p.parentQueue && p.parentQueue !== this.name) {
        await this.registerCrossQueueParent(p.parentId, p.parentQueue, jobId);
      }
    }

    return String(jobId);
  }

  /**
   * Add multiple jobs in a single pipeline round trip.
   * Returns an array of job IDs (string or null for dedup/collision).
   */
  async addBulk(jobs: { name: string; data: D; opts?: JobOptions }[]): Promise<(string | null)[]> {
    if (jobs.length === 0) return [];

    const client = await this.getClient();
    const isCluster = this.clusterMode;
    const timestamp = Date.now();

    const prepared = jobs.map((entry) => {
      const p = this.prepareJobParams(entry.name, entry.data, entry.opts);
      // Use a shared timestamp for the batch
      p.timestamp = timestamp;
      return p;
    });

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
          p.jobName,
          p.serializedData,
          p.optsJson,
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
          p.parentQueue,
        ]);
      } else {
        let jobKeys = keys;
        if (p.parentId && p.parentQueue && p.parentQueue === this.name) {
          jobKeys = [...keys, this.keys.deps(p.parentId)];
        }
        batch.fcall('glidemq_addJob', jobKeys, [
          p.jobName,
          p.serializedData,
          p.optsJson,
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
          p.parentQueue,
        ]);
      }
    }

    const rawResults = isCluster
      ? await (client as GlideClusterClient).exec(batch as ClusterBatch, true)
      : await (client as GlideClient).exec(batch as Batch, true);

    const results: (string | null)[] = [];
    const crossQueueParents: { parentId: string; parentQueue: string; jobId: string }[] = [];

    for (let i = 0; i < prepared.length; i++) {
      const raw = rawResults ? String(rawResults[i]) : '';
      if (raw === 'skipped' || raw === 'duplicate') {
        results.push(null);
      } else if (raw === 'ERR:COST_EXCEEDS_CAPACITY') {
        throw new Error('Job cost exceeds token bucket capacity');
      } else if (raw === 'ERR:ID_EXHAUSTED') {
        throw new Error('Failed to generate job ID: too many collisions with custom job IDs');
      } else {
        results.push(raw);

        // Collect cross-queue parent deps for batch registration
        const p = prepared[i];
        if (p.parentId && p.parentQueue && p.parentQueue !== this.name && raw) {
          crossQueueParents.push({ parentId: p.parentId, parentQueue: p.parentQueue, jobId: raw });
        }
      }
    }

    // Batch all cross-queue parent registrations
    if (crossQueueParents.length > 0) {
      const parentBatch = isCluster ? new ClusterBatch(false) : new Batch(false);
      const pfx = keyPrefix(this.opts.prefix ?? 'glide', this.name);

      for (const { parentId, parentQueue, jobId } of crossQueueParents) {
        const parentKeys = buildKeys(parentQueue, this.opts.prefix);
        const depsMember = `${pfx}:${jobId}`;
        parentBatch.sadd(parentKeys.deps(parentId), [depsMember]);
      }

      const executeBatch = isCluster
        ? (client as GlideClusterClient).exec(parentBatch as ClusterBatch, true)
        : (client as GlideClient).exec(parentBatch as Batch, true);
      await executeBatch;
    }

    return results;
  }

  /**
   * Close the producer. If the client was created by this producer, it is destroyed.
   * If an external client was provided, it is not closed.
   */
  async close(): Promise<void> {
    if (this.closing) return;
    this.closing = true;
    if (this.client && this.clientOwned) {
      this.client.close();
    }
    this.client = null;
  }
}
