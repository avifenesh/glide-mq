import type { JobOptions, Client } from './types';
import type { QueueKeys } from './functions/index';
import { removeJob, failJob } from './functions/index';
import { calculateBackoff } from './utils';

export class Job<D = any, R = any> {
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
  parentId?: string;

  /**
   * Stream entry ID assigned when the job was added to the stream.
   * Used by Worker to XACK after processing.
   * @internal
   */
  entryId?: string;

  /** @internal */
  private client: Client;
  /** @internal */
  private queueKeys: QueueKeys;

  /** @internal */
  constructor(
    client: Client,
    queueKeys: QueueKeys,
    id: string,
    name: string,
    data: D,
    opts: JobOptions,
  ) {
    this.client = client;
    this.queueKeys = queueKeys;
    this.id = id;
    this.name = name;
    this.data = data;
    this.opts = opts;
    this.attemptsMade = 0;
    this.progress = 0;
    this.timestamp = Date.now();
  }

  /**
   * Update the progress of this job. Persists to the job hash and emits a progress event.
   */
  async updateProgress(progress: number | object): Promise<void> {
    const progressStr = typeof progress === 'number'
      ? progress.toString()
      : JSON.stringify(progress);
    await this.client.hset(this.queueKeys.job(this.id), {
      progress: progressStr,
    });
    await this.client.xadd(this.queueKeys.events, [
      ['event', 'progress'],
      ['jobId', this.id],
      ['data', progressStr],
    ]);
    this.progress = progress;
  }

  /**
   * Replace the data payload of this job.
   */
  async updateData(data: D): Promise<void> {
    await this.client.hset(this.queueKeys.job(this.id), {
      data: JSON.stringify(data),
    });
    this.data = data;
  }

  /**
   * Read return values from all child jobs (for flow/parent-child patterns).
   */
  async getChildrenValues(): Promise<Record<string, R>> {
    const depsKey = this.queueKeys.deps(this.id);
    const childIds = await this.client.smembers(depsKey);
    const result: Record<string, R> = {};
    for (const childId of childIds) {
      const val = await this.client.hget(
        this.queueKeys.job(String(childId)),
        'returnvalue',
      );
      if (val != null) {
        result[String(childId)] = JSON.parse(String(val));
      }
    }
    return result;
  }

  /**
   * Move this job to the failed state.
   * If attempts remain and backoff is configured, retries via the scheduled ZSet.
   * Requires entryId to be set (set by Worker when processing).
   */
  async moveToFailed(err: Error): Promise<void> {
    const maxAttempts = this.opts.attempts ?? 0;
    let backoffDelay = 0;
    if (this.opts.backoff) {
      backoffDelay = calculateBackoff(
        this.opts.backoff.type,
        this.opts.backoff.delay,
        this.attemptsMade + 1,
        this.opts.backoff.jitter,
      );
    }
    const entryId = this.entryId ?? '0-0';
    const result = await failJob(
      this.client,
      this.queueKeys,
      this.id,
      entryId,
      err.message,
      Date.now(),
      maxAttempts,
      backoffDelay,
    );
    this.failedReason = err.message;
    if (result === 'retrying') {
      this.attemptsMade += 1;
    }
  }

  /**
   * Remove this job from all data structures.
   */
  async remove(): Promise<void> {
    await removeJob(this.client, this.queueKeys, this.id);
  }

  /**
   * Retry this job by moving it back to the scheduled ZSet with a score of now
   * (so it gets promoted immediately on the next promote cycle).
   */
  async retry(): Promise<void> {
    const now = Date.now();
    const priority = this.opts.priority ?? 0;
    const PRIORITY_SHIFT = 2 ** 42;
    const score = priority * PRIORITY_SHIFT + now;
    await this.client.zadd(this.queueKeys.scheduled, [
      { element: this.id, score },
    ]);
    await this.client.hset(this.queueKeys.job(this.id), {
      state: 'delayed',
      failedReason: '',
    });
  }

  /**
   * Construct a Job instance from a hash returned by HGETALL.
   * @internal
   */
  static fromHash<D, R>(
    client: Client,
    queueKeys: QueueKeys,
    id: string,
    hash: Record<string, string>,
  ): Job<D, R> {
    const job = new Job<D, R>(
      client,
      queueKeys,
      id,
      hash.name || '',
      JSON.parse(hash.data || '{}'),
      JSON.parse(hash.opts || '{}'),
    );
    job.attemptsMade = parseInt(hash.attemptsMade || '0', 10);
    job.timestamp = parseInt(hash.timestamp || '0', 10);
    job.processedOn = hash.processedOn ? parseInt(hash.processedOn, 10) : undefined;
    job.finishedOn = hash.finishedOn ? parseInt(hash.finishedOn, 10) : undefined;
    job.returnvalue = hash.returnvalue ? JSON.parse(hash.returnvalue) : undefined;
    job.failedReason = hash.failedReason || undefined;
    job.parentId = hash.parentId || undefined;
    if (hash.progress) {
      try {
        job.progress = JSON.parse(hash.progress);
      } catch {
        job.progress = parseInt(hash.progress, 10) || 0;
      }
    }
    return job;
  }
}
