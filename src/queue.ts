import type { QueueOptions, JobOptions, Client } from './types';
import { Job } from './job';
import { buildKeys } from './utils';
import { createClient, ensureFunctionLibrary } from './connection';
import { addJob, dedup, pause, resume, removeJob } from './functions/index';
import type { QueueKeys } from './functions/index';
import { LIBRARY_SOURCE } from './functions/index';

export class Queue<D = any, R = any> {
  readonly name: string;
  private opts: QueueOptions;
  private client: Client | null = null;
  private keys: QueueKeys;

  constructor(name: string, opts: QueueOptions) {
    this.name = name;
    this.opts = opts;
    this.keys = buildKeys(name, opts.prefix);
  }

  /** @internal */
  async getClient(): Promise<Client> {
    if (!this.client) {
      this.client = await createClient(this.opts.connection);
      await ensureFunctionLibrary(
        this.client,
        LIBRARY_SOURCE,
        this.opts.connection.clusterMode ?? false,
      );
    }
    return this.client;
  }

  /**
   * Add a single job to the queue.
   * Uses the glidemq_addJob server function to atomically create the job hash
   * and enqueue it to the stream (or scheduled ZSet if delayed/prioritized).
   */
  async add(name: string, data: D, opts?: JobOptions): Promise<Job<D, R> | null> {
    const client = await this.getClient();
    const timestamp = Date.now();
    const delay = opts?.delay ?? 0;
    const priority = opts?.priority ?? 0;
    const parentId = opts?.parent ? opts.parent.id : '';
    const maxAttempts = opts?.attempts ?? 0;

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
        JSON.stringify(data),
        JSON.stringify(opts),
        timestamp,
        delay,
        priority,
        parentId,
        maxAttempts,
      );
      if (result === 'skipped') {
        return null;
      }
      jobId = result;
    } else {
      jobId = await addJob(
        client,
        this.keys,
        name,
        JSON.stringify(data),
        JSON.stringify(opts ?? {}),
        timestamp,
        delay,
        priority,
        parentId,
        maxAttempts,
      );
    }

    const job = new Job<D, R>(
      client,
      this.keys,
      String(jobId),
      name,
      data,
      opts ?? {},
    );
    job.timestamp = timestamp;
    job.parentId = parentId || undefined;
    return job;
  }

  /**
   * Add multiple jobs to the queue in a pipeline.
   * Each job is added via a separate addJob FCALL (non-atomic across jobs).
   */
  async addBulk(
    jobs: { name: string; data: D; opts?: JobOptions }[],
  ): Promise<Job<D, R>[]> {
    const client = await this.getClient();
    const results: Job<D, R>[] = [];
    for (const entry of jobs) {
      const timestamp = Date.now();
      const opts = entry.opts ?? {};
      const delay = opts.delay ?? 0;
      const priority = opts.priority ?? 0;
      const parentId = opts.parent ? opts.parent.id : '';
      const maxAttempts = opts.attempts ?? 0;

      const jobId = await addJob(
        client,
        this.keys,
        entry.name,
        JSON.stringify(entry.data),
        JSON.stringify(opts),
        timestamp,
        delay,
        priority,
        parentId,
        maxAttempts,
      );

      const job = new Job<D, R>(
        client,
        this.keys,
        String(jobId),
        entry.name,
        entry.data,
        opts,
      );
      job.timestamp = timestamp;
      job.parentId = parentId || undefined;
      results.push(job);
    }
    return results;
  }

  /**
   * Retrieve a job by ID from the queue.
   * Returns null if the job does not exist.
   */
  async getJob(id: string): Promise<Job<D, R> | null> {
    const client = await this.getClient();
    const hashData = await client.hgetall(this.keys.job(id));

    // hgetall returns HashDataType which is { field: GlideString, value: GlideString }[]
    // An empty array means the key does not exist.
    if (!hashData || hashData.length === 0) {
      return null;
    }

    // Convert HashDataType array to a plain Record<string, string>
    const hash: Record<string, string> = {};
    for (const entry of hashData) {
      hash[String(entry.field)] = String(entry.value);
    }

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
   * Close the queue and release the underlying client connection.
   */
  async close(): Promise<void> {
    if (this.client) {
      this.client.close();
      this.client = null;
    }
  }
}
