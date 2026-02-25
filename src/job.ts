import { Batch, ClusterBatch } from '@glidemq/speedkey';
import type { GlideClient, GlideClusterClient } from '@glidemq/speedkey';
import type { JobOptions, Client } from './types';
import type { QueueKeys } from './functions/index';
import { removeJob, failJob, changePriority, changeDelay, promoteJob, updateProgress } from './functions/index';
import { calculateBackoff, decompress } from './utils';
import { isClusterClient } from './connection';

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
  parentQueue?: string;
  orderingKey?: string;
  orderingSeq?: number;
  groupKey?: string;
  cost?: number;

  /**
   * AbortSignal that fires when this job is revoked during processing.
   * The processor should check signal.aborted cooperatively.
   * Only set when the job is being processed by a Worker.
   */
  abortSignal?: AbortSignal;

  /**
   * When true, the job will not be retried on failure regardless of attempts config.
   * Set by calling `discard()` inside the processor.
   */
  discarded = false;

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
  constructor(client: Client, queueKeys: QueueKeys, id: string, name: string, data: D, opts: JobOptions) {
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
   * Append a log line to this job's log list.
   */
  async log(message: string): Promise<void> {
    await this.client.rpush(this.queueKeys.log(this.id), [message]);
  }

  /**
   * Update the progress of this job. Persists to the job hash and emits a progress event.
   */
  async updateProgress(progress: number | object): Promise<void> {
    await updateProgress(this.client, this.queueKeys, this.id, progress);
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
   * Mark this job so it will not be retried on failure.
   * Call inside the processor before throwing to skip all remaining attempts.
   */
  discard(): void {
    this.discarded = true;
  }

  /**
   * Read return values from all child jobs (for flow/parent-child patterns).
   */
  async getChildrenValues(): Promise<Record<string, R>> {
    const depsKey = this.queueKeys.deps(this.id);
    const members = await this.client.smembers(depsKey);
    const result: Record<string, R> = {};
    if (!members || members.size === 0) return result;

    const isCluster = isClusterClient(this.client);
    const batch = isCluster ? new ClusterBatch(false) : new Batch(false);
    const memberKeys: string[] = [];

    for (const member of members) {
      const memberStr = String(member);
      // Deps member format: "queuePrefix:childId" e.g. "glide:{q}:3"
      // Job hash key: "queuePrefix:job:childId" e.g. "glide:{q}:job:3"
      const lastColon = memberStr.lastIndexOf(':');
      if (lastColon === -1) continue;
      const queuePrefix = memberStr.substring(0, lastColon);
      const childId = memberStr.substring(lastColon + 1);
      const jobKey = `${queuePrefix}:job:${childId}`;

      (batch as any).hget(jobKey, 'returnvalue');
      memberKeys.push(memberStr);
    }

    if (memberKeys.length === 0) return result;

    const results = isCluster
      ? await (this.client as GlideClusterClient).exec(batch as ClusterBatch, false)
      : await (this.client as GlideClient).exec(batch as Batch, false);

    if (results) {
      for (let i = 0; i < results.length; i++) {
        const val = results[i];
        if (val != null) {
          result[memberKeys[i]] = JSON.parse(String(val));
        }
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
   * Change the priority of this job. Supports waiting, prioritized, and delayed states.
   * Setting priority to 0 moves a prioritized job back to the stream (waiting).
   * Throws if the job is in an invalid state (active, completed, failed).
   */
  async changePriority(newPriority: number): Promise<void> {
    if (newPriority < 0) {
      throw new Error('Priority must be >= 0');
    }
    const result = await changePriority(this.client, this.queueKeys, this.id, newPriority);
    if (result.startsWith('error:')) {
      const reason = result.slice(6);
      throw new Error(`Cannot change priority: ${reason}`);
    }
    if (result === 'ok' || result === 'no_op') {
      this.opts.priority = newPriority;
    }
  }

  /**
   * Change the delay of this job. Supports delayed, waiting, and prioritized states.
   * Setting delay to 0 promotes a delayed job immediately.
   * Setting delay > 0 on a waiting/prioritized job moves it to the scheduled ZSet.
   * Throws if the job is in an invalid state (active, completed, failed).
   */
  async changeDelay(newDelay: number): Promise<void> {
    if (newDelay < 0) {
      throw new Error('Delay must be >= 0');
    }
    const result = await changeDelay(this.client, this.queueKeys, this.id, newDelay);
    if (result.startsWith('error:')) {
      const reason = result.slice(6);
      throw new Error(`Cannot change delay: ${reason}`);
    }
    if (result === 'ok' || result === 'no_op') {
      this.opts.delay = newDelay;
    }
  }

  /**
   * Promote a delayed job to waiting immediately.
   * Removes from the scheduled ZSet, adds to the stream, sets state to 'waiting'.
   * Throws if the job is not in the delayed state or does not exist.
   */
  async promote(): Promise<void> {
    const result = await promoteJob(this.client, this.queueKeys, this.id);
    if (result.startsWith('error:')) {
      const reason = result.slice(6);
      throw new Error(`Cannot promote: ${reason}`);
    }
    if (result === 'ok') {
      this.opts.delay = 0;
    }
  }

  /**
   * Retry this job by moving it back to the scheduled ZSet with a score of now
   * (so it gets promoted immediately on the next promote cycle).
   * Removes the job from the failed ZSet first to prevent dual membership.
   */
  async retry(): Promise<void> {
    const now = Date.now();
    const priority = this.opts.priority ?? 0;
    const PRIORITY_SHIFT = 2 ** 42;
    const score = priority * PRIORITY_SHIFT + now;

    const isCluster = isClusterClient(this.client);
    const batch = isCluster ? new ClusterBatch(false) : new Batch(false);

    batch.zrem(this.queueKeys.failed, [this.id]);
    batch.zadd(this.queueKeys.scheduled, { [this.id]: score });
    batch.hset(this.queueKeys.job(this.id), {
      state: 'delayed',
      attemptsMade: '0',
      failedReason: '',
      finishedOn: '',
    });

    if (isCluster) {
      await (this.client as GlideClusterClient).exec(batch as ClusterBatch, false);
    } else {
      await (this.client as GlideClient).exec(batch as Batch, false);
    }

    this.attemptsMade = 0;
    this.failedReason = undefined;
    this.finishedOn = undefined;
  }

  /**
   * Check if this job is in the completed state.
   */
  async isCompleted(): Promise<boolean> {
    return (await this.getState()) === 'completed';
  }

  /**
   * Check if this job is in the failed state.
   */
  async isFailed(): Promise<boolean> {
    return (await this.getState()) === 'failed';
  }

  /**
   * Check if this job is in the delayed state.
   */
  async isDelayed(): Promise<boolean> {
    return (await this.getState()) === 'delayed';
  }

  /**
   * Check if this job is in the active state.
   */
  async isActive(): Promise<boolean> {
    return (await this.getState()) === 'active';
  }

  /**
   * Check if this job is in the waiting state.
   */
  async isWaiting(): Promise<boolean> {
    return (await this.getState()) === 'waiting';
  }

  /**
   * Check if this job has been revoked.
   */
  async isRevoked(): Promise<boolean> {
    const val = await this.client.hget(this.queueKeys.job(this.id), 'revoked');
    return String(val) === '1';
  }

  /**
   * Read the current state from the job hash.
   */
  async getState(): Promise<string> {
    const state = await this.client.hget(this.queueKeys.job(this.id), 'state');
    return state ? String(state) : 'unknown';
  }

  /**
   * Wait until the job reaches a terminal state (completed or failed).
   * Polls the job hash state at the given interval.
   * Returns the final state.
   */
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

  /**
   * Construct a Job instance from a hash returned by HGETALL.
   * @internal
   */
  static fromHash<D, R>(client: Client, queueKeys: QueueKeys, id: string, hash: Record<string, string>): Job<D, R> {
    let data: D;
    let opts: JobOptions;
    let returnvalue: R | undefined;
    try {
      data = JSON.parse(decompress(hash.data || '{}'));
    } catch {
      data = {} as D;
    }
    try {
      opts = JSON.parse(hash.opts || '{}');
    } catch {
      opts = {};
    }
    try {
      returnvalue = hash.returnvalue ? JSON.parse(hash.returnvalue) : undefined;
    } catch {
      returnvalue = undefined;
    }

    const job = new Job<D, R>(client, queueKeys, id, hash.name || '', data, opts);
    job.attemptsMade = parseInt(hash.attemptsMade || '0', 10);
    job.timestamp = parseInt(hash.timestamp || '0', 10);
    job.processedOn = hash.processedOn ? parseInt(hash.processedOn, 10) : undefined;
    job.finishedOn = hash.finishedOn ? parseInt(hash.finishedOn, 10) : undefined;
    job.returnvalue = returnvalue;
    job.failedReason = hash.failedReason || undefined;
    job.parentId = hash.parentId || undefined;
    job.parentQueue = hash.parentQueue || undefined;
    job.orderingKey = hash.orderingKey || undefined;
    job.orderingSeq = hash.orderingSeq ? parseInt(hash.orderingSeq, 10) : undefined;
    job.groupKey = hash.groupKey || undefined;
    job.cost = hash.cost ? parseInt(hash.cost, 10) : undefined;
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
