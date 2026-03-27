import { Batch, ClusterBatch } from '@glidemq/speedkey';
import type { GlideClient, GlideClusterClient } from '@glidemq/speedkey';
import type { JobOptions, JobUsage, Client, Serializer, SuspendOptions, SignalEntry } from './types';
import { JSON_SERIALIZER } from './types';
import type { QueueKeys } from './functions/index';
import { removeJob, failJob, changePriority, changeDelay, promoteJob } from './functions/index';
import { GlideMQError, DelayedError, WaitingChildrenError, GroupRateLimitError, SuspendError } from './errors';
import type { GroupRateLimitOptions } from './errors';
import { calculateBackoff, decompress, isPlainStepPayload, MAX_JOB_DATA_SIZE } from './utils';
import { isClusterClient } from './connection';

/** Try to parse a string as JSON; return the original string if parsing fails. */
function tryParseJson(s: string): any {
  try {
    return JSON.parse(s);
  } catch {
    return s;
  }
}

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
  /** Additional parent IDs for DAG multi-parent jobs. */
  parentIds?: string[];
  /** Additional parent queues for DAG multi-parent jobs (parallel array to parentIds). */
  parentQueues?: string[];
  orderingKey?: string;
  orderingSeq?: number;
  groupKey?: string;
  cost?: number;
  expireAt?: number;
  schedulerName?: string;

  /** Budget key for flow-level budget enforcement. Set when the job belongs to a budgeted flow. */
  budgetKey?: string;

  /** Current position in the fallback chain. 0 = original request, 1+ = fallback entries. */
  fallbackIndex: number = 0;

  /** AI-specific usage metadata reported via reportUsage(). */
  usage?: JobUsage;

  /** Tokens reported via reportTokens() for TPM rate limiting. */
  tpmTokens?: number;

  /**
   * AbortSignal that fires when this job is revoked during processing.
   * The processor should check signal.aborted cooperatively.
   * Only set when the job is being processed by a Worker.
   */
  abortSignal?: AbortSignal;

  /** Signals delivered to this job while it was suspended. */
  signals: SignalEntry[] = [];

  /**
   * When true, the job will not be retried on failure regardless of attempts config.
   * Set by calling `discard()` inside the processor.
   */
  discarded = false;

  /** @internal Request captured by moveToDelayed() while inside the worker. */
  moveToDelayedRequest?: { delayedUntil: number; serializedData?: string; nextData?: D };

  /** @internal Request captured by moveToWaitingChildren() while inside the worker. */
  moveToWaitingChildrenRequest?: boolean;

  /** @internal Request captured by suspend() while inside the worker. */
  suspendRequest?: { reason?: string; timeout?: number; onResume?: (signals: SignalEntry[]) => Promise<any> };

  /**
   * Set to true when data or returnvalue could not be deserialized from Valkey.
   * This typically indicates a serializer mismatch between the producer and consumer.
   * When true, `data` is set to `{} as D` and `returnvalue` to `undefined`.
   */
  deserializationFailed = false;

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
  private serializer: Serializer;

  /** @internal */
  constructor(
    client: Client,
    queueKeys: QueueKeys,
    id: string,
    name: string,
    data: D,
    opts: JobOptions,
    serializer?: Serializer,
  ) {
    this.client = client;
    this.queueKeys = queueKeys;
    this.serializer = serializer ?? JSON_SERIALIZER;
    this.id = id;
    this.name = name;
    this.data = data;
    this.opts = opts;
    this.attemptsMade = 0;
    this.progress = 0;
    this.timestamp = Date.now();
  }

  /**
   * The current fallback entry, or undefined when running the original request.
   * fallbackIndex=0 means original (no fallback). On first failure, fallbackIndex
   * becomes 1 and currentFallback returns fallbacks[0], etc.
   */
  get currentFallback(): { model: string; provider?: string; metadata?: Record<string, unknown> } | undefined {
    if (!this.opts.fallbacks || this.fallbackIndex === 0) return undefined;
    return this.opts.fallbacks[this.fallbackIndex - 1];
  }

  /**
   * Append a log line to this job's log list.
   */
  async log(message: string): Promise<void> {
    const byteLen = Buffer.byteLength(message, 'utf8');
    if (byteLen > MAX_JOB_DATA_SIZE) {
      throw new Error(`Log message exceeds maximum size (${byteLen} bytes > ${MAX_JOB_DATA_SIZE})`);
    }
    await this.client.rpush(this.queueKeys.log(this.id), [message]);
  }

  /**
   * Update the progress of this job. Persists to the job hash and emits a progress event.
   */
  async updateProgress(progress: number | object): Promise<void> {
    const progressStr = typeof progress === 'number' ? progress.toString() : JSON.stringify(progress);
    const byteLen = Buffer.byteLength(progressStr, 'utf8');
    if (byteLen > MAX_JOB_DATA_SIZE) {
      throw new Error(`Progress data exceeds maximum size (${byteLen} bytes > ${MAX_JOB_DATA_SIZE})`);
    }
    const isCluster = isClusterClient(this.client);
    const batch = isCluster ? new ClusterBatch(false) : new Batch(false);

    batch.hset(this.queueKeys.job(this.id), {
      progress: progressStr,
    });
    batch.xadd(this.queueKeys.events, [
      ['event', 'progress'],
      ['jobId', this.id],
      ['data', progressStr],
    ]);

    if (isCluster) {
      await (this.client as GlideClusterClient).exec(batch as ClusterBatch, false);
    } else {
      await (this.client as GlideClient).exec(batch as Batch, false);
    }

    this.progress = progress;
  }

  /**
   * Replace the data payload of this job.
   */
  async updateData(data: D): Promise<void> {
    const serialized = this.serializeData(data);
    await this.client.hset(this.queueKeys.job(this.id), {
      data: serialized,
    });
    this.data = data;
  }

  /**
   * Report AI-specific usage metadata for this job. Persists to the job hash
   * and emits a 'usage' event on the events stream.
   *
   * Callable from any context (inside a processor, externally via getJob(), etc.).
   * If `totalTokens` is not provided, it is auto-computed as the sum of all values in `tokens`.
   * Calling multiple times overwrites the previous usage data.
   */
  async reportUsage(usage: JobUsage): Promise<void> {
    if (usage.tokens) {
      for (const [key, val] of Object.entries(usage.tokens)) {
        if (!Number.isFinite(val) || val < 0) {
          throw new Error(`Token count for '${key}' must be a finite non-negative number`);
        }
      }
    }
    if (usage.totalTokens !== undefined && (!Number.isFinite(usage.totalTokens) || usage.totalTokens < 0)) {
      throw new Error('totalTokens must be a finite non-negative number');
    }
    if (usage.costs) {
      for (const [key, val] of Object.entries(usage.costs)) {
        if (!Number.isFinite(val) || val < 0) {
          throw new Error(`Cost for '${key}' must be a finite non-negative number`);
        }
      }
    }
    if (usage.totalCost !== undefined && (!Number.isFinite(usage.totalCost) || usage.totalCost < 0)) {
      throw new Error('totalCost must be a finite non-negative number');
    }

    const resolved: JobUsage = { ...usage };
    if (resolved.totalTokens === undefined && resolved.tokens && Object.keys(resolved.tokens).length > 0) {
      resolved.totalTokens = Object.values(resolved.tokens).reduce((sum, v) => sum + v, 0);
    }
    if (resolved.totalCost === undefined && resolved.costs && Object.keys(resolved.costs).length > 0) {
      resolved.totalCost = Object.values(resolved.costs).reduce((sum, v) => sum + v, 0);
    }

    const fields: Record<string, string> = {};
    if (resolved.model !== undefined) fields['usage:model'] = resolved.model;
    if (resolved.provider !== undefined) fields['usage:provider'] = resolved.provider;
    if (resolved.tokens !== undefined) fields['usage:tokens'] = JSON.stringify(resolved.tokens);
    if (resolved.totalTokens !== undefined) fields['usage:totalTokens'] = resolved.totalTokens.toString();
    if (resolved.costs !== undefined) fields['usage:costs'] = JSON.stringify(resolved.costs);
    if (resolved.totalCost !== undefined) fields['usage:totalCost'] = resolved.totalCost.toString();
    if (resolved.costUnit !== undefined) fields['usage:costUnit'] = resolved.costUnit;
    if (resolved.latencyMs !== undefined) fields['usage:latencyMs'] = resolved.latencyMs.toString();
    if (resolved.cached !== undefined) fields['usage:cached'] = resolved.cached ? '1' : '0';

    const isCluster = isClusterClient(this.client);
    const batch = isCluster ? new ClusterBatch(false) : new Batch(false);

    batch.hset(this.queueKeys.job(this.id), fields);
    batch.xadd(this.queueKeys.events, [
      ['event', 'usage'],
      ['jobId', this.id],
      ['data', JSON.stringify(resolved)],
    ]);

    if (isCluster) {
      await (this.client as GlideClusterClient).exec(batch as ClusterBatch, false);
    } else {
      await (this.client as GlideClient).exec(batch as Batch, false);
    }

    this.usage = resolved;
  }

  /**
   * Report tokens consumed by this job for TPM (tokens-per-minute) tracking.
   * The count is stored in the job hash field `tpmTokens`.
   * After job completion, the Worker reads this value and increments the TPM counter
   * if a tokenLimiter is configured.
   *
   * Calling multiple times overwrites the previous value.
   */
  async reportTokens(count: number): Promise<void> {
    if (count < 0) throw new Error('Token count must not be negative');
    await this.client.hset(this.queueKeys.job(this.id), { tpmTokens: count.toString() });
    this.tpmTokens = count;
  }

  /**
   * Append a chunk to this job's streaming channel.
   * Each chunk is a flat string-keyed object appended via XADD to a per-job stream.
   * Returns the Valkey stream entry ID.
   */
  async stream(chunk: Record<string, string>): Promise<string> {
    const fieldNames = Object.keys(chunk);
    if (fieldNames.length === 0) {
      throw new Error('Stream chunk must not be empty');
    }
    const entries: [string, string][] = [];
    let totalBytes = 0;
    for (const key of fieldNames) {
      const val = chunk[key];
      totalBytes += Buffer.byteLength(key, 'utf8') + Buffer.byteLength(val, 'utf8');
      entries.push([key, val]);
    }
    if (totalBytes > MAX_JOB_DATA_SIZE) {
      throw new Error(`Stream chunk exceeds maximum size (${totalBytes} bytes > ${MAX_JOB_DATA_SIZE})`);
    }
    const entryId = await this.client.xadd(this.queueKeys.jstream(this.id), entries);
    return String(entryId);
  }

  /**
   * Convenience method for streaming typed LLM chunks.
   * Wraps `stream()` with `{ type, content }` fields.
   *
   * @example
   *   await job.streamChunk('reasoning', 'Let me think about this...');
   *   await job.streamChunk('content', 'The answer is 42.');
   *   await job.streamChunk('done');
   */
  async streamChunk(type: string, content?: string): Promise<string> {
    const chunk: Record<string, string> = { type };
    if (content !== undefined) chunk.content = content;
    return this.stream(chunk);
  }

  /**
   * Mark this job so it will not be retried on failure.
   * Call inside the processor before throwing to skip all remaining attempts.
   */
  discard(): void {
    this.discarded = true;
  }

  /** @internal */
  requestMoveToDelayed(timestamp: number, nextData?: D): void {
    this.moveToDelayedRequest = {
      delayedUntil: Math.trunc(timestamp),
      ...(nextData !== undefined ? { serializedData: this.serializeData(nextData), nextData } : {}),
    };
    if (nextData !== undefined) {
      this.data = nextData;
    }
  }

  /** @internal */
  consumeMoveToDelayedRequest():
    | {
        delayedUntil: number;
        serializedData?: string;
        nextData?: D;
      }
    | undefined {
    const requested = this.moveToDelayedRequest;
    this.moveToDelayedRequest = undefined;
    return requested;
  }

  /**
   * Pause an active job and wait for dynamically-added child jobs to complete.
   * When all children finish, this job resumes and the processor is invoked again.
   *
   * This method must be called from inside a Worker processor.
   */
  async moveToWaitingChildren(): Promise<never> {
    if (!this.entryId) {
      throw new Error('moveToWaitingChildren() can only be used while the job is active in a Worker');
    }
    this.moveToWaitingChildrenRequest = true;
    throw new WaitingChildrenError();
  }

  /** @internal */
  consumeMoveToWaitingChildrenRequest(): boolean {
    const requested = this.moveToWaitingChildrenRequest ?? false;
    this.moveToWaitingChildrenRequest = undefined;
    return requested;
  }

  /**
   * Suspend this job to wait for an external signal (human-in-the-loop).
   * The processor is interrupted and the job moves to 'suspended' state.
   * When a signal arrives via Queue.signal(), the job re-enters the stream
   * and the processor is re-invoked from scratch with job.signals populated.
   *
   * Optionally provide an onResume callback that runs instead of the main
   * processor when the job resumes on the same worker (best-effort).
   *
   * This method must be called from inside a Worker processor.
   */
  async suspend(opts?: SuspendOptions & { onResume?: (signals: SignalEntry[]) => Promise<any> }): Promise<never> {
    if (!this.entryId) {
      throw new Error('suspend() can only be used while the job is active in a Worker');
    }
    this.suspendRequest = {
      reason: opts?.reason,
      timeout: opts?.timeout,
      onResume: opts?.onResume,
    };
    throw new SuspendError();
  }

  /** @internal */
  consumeSuspendRequest():
    | { reason?: string; timeout?: number; onResume?: (signals: SignalEntry[]) => Promise<any> }
    | undefined {
    const req = this.suspendRequest;
    this.suspendRequest = undefined;
    return req;
  }

  /**
   * Read return values from all child jobs (for flow/parent-child patterns).
   */
  async getChildrenValues(): Promise<Record<string, R>> {
    const depsKey = this.queueKeys.deps(this.id);
    const members = await this.client.smembers(depsKey);
    const result: Record<string, R> = Object.create(null);
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
          result[memberKeys[i]] = this.serializer.deserialize(String(val)) as R;
        }
      }
    }
    return result;
  }

  /**
   * Read all parent references for this job (for DAG multi-parent patterns).
   * Returns an array of { queue, id } for each parent.
   * For single-parent jobs, returns an array with one element.
   * For jobs with no parent, returns an empty array.
   */
  async getParents(): Promise<Array<{ queue: string; id: string }>> {
    const result: Array<{ queue: string; id: string }> = [];

    // Check the parents SET first (multi-parent DAG jobs)
    const parentsKey = this.queueKeys.parents(this.id);
    const members = await this.client.smembers(parentsKey);
    if (members && members.size > 0) {
      for (const member of members) {
        const memberStr = String(member);
        // Format: "queue:parentId"
        const sepIdx = memberStr.indexOf(':');
        if (sepIdx !== -1) {
          result.push({
            queue: memberStr.substring(0, sepIdx),
            id: memberStr.substring(sepIdx + 1),
          });
        }
      }
      return result;
    }

    // Fall back to single-parent fields
    let parentId = this.parentId;
    let parentQueue = this.parentQueue;
    if (!parentId || !parentQueue) {
      const [refreshedParentId, refreshedParentQueue] = await this.client.hmget(this.queueKeys.job(this.id), [
        'parentId',
        'parentQueue',
      ]);
      parentId = refreshedParentId ? String(refreshedParentId) : parentId;
      parentQueue = refreshedParentQueue ? String(refreshedParentQueue) : parentQueue;
    }
    if (parentId && parentQueue) {
      result.push({ queue: parentQueue, id: parentId });
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
    if (!this.entryId) {
      throw new GlideMQError('moveToFailed can only be called while job is active in a Worker');
    }
    const entryId = this.entryId;
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
   * Pause an active job and resume it after the given UNIX timestamp in ms.
   * Optionally updates `job.data.step` before yielding back to the worker.
   *
   * This method must be called from inside a Worker processor.
   */
  async moveToDelayed(timestamp: number, nextStep?: string): Promise<never> {
    if (!Number.isFinite(timestamp) || timestamp < 0) {
      throw new Error('Timestamp must be a finite Unix millisecond value >= 0');
    }
    if (!this.entryId) {
      throw new Error('moveToDelayed() can only be used while the job is active in a Worker');
    }

    const delayedUntil = Math.trunc(timestamp);
    if (nextStep !== undefined) {
      if (!isPlainStepPayload(this.data)) {
        throw new Error('moveToDelayed(nextStep) requires plain-object job data');
      }
      this.requestMoveToDelayed(delayedUntil, { ...this.data, step: nextStep } as D);
    } else {
      this.requestMoveToDelayed(delayedUntil);
    }
    throw new DelayedError(delayedUntil);
  }

  /**
   * Rate-limit this job's ordering group for the given duration (milliseconds).
   * The current job is re-parked in the group queue (by default at the front)
   * and the entire group is paused until the duration expires.
   *
   * Can only be called from inside a Worker processor.
   * Throws GroupRateLimitError which the worker catches internally.
   */
  async rateLimitGroup(duration: number, opts?: GroupRateLimitOptions): Promise<never> {
    if (!Number.isFinite(duration) || duration <= 0) {
      throw new Error('duration must be a positive finite number (milliseconds)');
    }
    if (!this.groupKey) {
      throw new Error('rateLimitGroup() requires a group key (set via ordering.key option)');
    }
    throw new GroupRateLimitError(Math.trunc(duration), opts);
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
   * Construct a Job instance from a hash returned by HGETALL or HMGET.
   * @internal
   */
  static fromHash<D, R>(
    client: Client,
    queueKeys: QueueKeys,
    id: string,
    hash: Record<string, string>,
    serializer?: Serializer,
    excludeData?: boolean,
  ): Job<D, R> {
    const s = serializer ?? JSON_SERIALIZER;
    let data: D;
    let opts: JobOptions;
    let returnvalue: R | undefined;
    let deserializationFailed = false;

    if (excludeData) {
      data = undefined as unknown as D;
      returnvalue = undefined;
    } else {
      try {
        data = hash.data ? (s.deserialize(decompress(hash.data)) as D) : ({} as D);
      } catch {
        data = {} as D;
        deserializationFailed = true;
      }
      try {
        returnvalue = hash.returnvalue ? (s.deserialize(hash.returnvalue) as R) : undefined;
      } catch {
        returnvalue = undefined;
        deserializationFailed = true;
      }
    }

    try {
      opts = JSON.parse(hash.opts || '{}');
    } catch {
      opts = {};
    }

    const job = new Job<D, R>(client, queueKeys, id, hash.name || '', data, opts, s);
    job.deserializationFailed = deserializationFailed;
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
    job.expireAt = hash.expireAt ? parseInt(hash.expireAt, 10) : undefined;
    job.schedulerName = hash.schedulerName || undefined;
    job.budgetKey = hash.budgetKey || undefined;
    job.fallbackIndex = hash.fallbackIndex ? parseInt(hash.fallbackIndex, 10) : 0;
    job.tpmTokens = hash.tpmTokens ? parseInt(hash.tpmTokens, 10) : undefined;
    if (hash.parentIds) {
      try {
        job.parentIds = JSON.parse(hash.parentIds);
      } catch {
        job.parentIds = undefined;
      }
    }
    if (hash.parentQueues) {
      try {
        job.parentQueues = JSON.parse(hash.parentQueues);
      } catch {
        job.parentQueues = undefined;
      }
    }
    if (hash.progress) {
      try {
        job.progress = JSON.parse(hash.progress);
      } catch {
        job.progress = parseInt(hash.progress, 10) || 0;
      }
    }
    if (
      hash['usage:model'] ||
      hash['usage:tokens'] ||
      hash['usage:provider'] ||
      hash['usage:costs'] ||
      hash['usage:totalTokens'] ||
      hash['usage:totalCost'] ||
      hash['usage:costUnit']
    ) {
      let tokens: Record<string, number> | undefined;
      let costs: Record<string, number> | undefined;
      if (hash['usage:tokens']) {
        try {
          const p = JSON.parse(hash['usage:tokens']);
          if (p && typeof p === 'object') tokens = p;
        } catch {
          /* ignore */
        }
      }
      if (hash['usage:costs']) {
        try {
          const p = JSON.parse(hash['usage:costs']);
          if (p && typeof p === 'object') costs = p;
        } catch {
          /* ignore */
        }
      }
      job.usage = {
        model: hash['usage:model'] || undefined,
        provider: hash['usage:provider'] || undefined,
        tokens,
        totalTokens: hash['usage:totalTokens'] ? parseInt(hash['usage:totalTokens'], 10) : undefined,
        costs,
        totalCost: hash['usage:totalCost'] ? parseFloat(hash['usage:totalCost']) : undefined,
        costUnit: hash['usage:costUnit'] || undefined,
        latencyMs: hash['usage:latencyMs'] ? parseInt(hash['usage:latencyMs'], 10) : undefined,
        cached: hash['usage:cached'] === '1' ? true : hash['usage:cached'] === '0' ? false : undefined,
      };
    }
    if (hash.signals) {
      try {
        const raw = JSON.parse(hash.signals) as any[];
        job.signals = raw.map((s) => ({
          ...s,
          data: typeof s.data === 'string' ? tryParseJson(s.data) : s.data,
        }));
      } catch {
        job.signals = [];
      }
    }
    return job;
  }

  /**
   * Store a vector embedding in this job's hash field.
   * The vector is stored as a raw Float32Array buffer suitable for Valkey Search vector indexing.
   * @param field - Hash field name where the vector is stored (must match the index schema).
   * @param embedding - The vector as a number[] or Float32Array.
   */
  async storeVector(field: string, embedding: number[] | Float32Array): Promise<void> {
    const arr = embedding instanceof Float32Array ? embedding : new Float32Array(embedding);
    const buf = Buffer.from(arr.buffer, arr.byteOffset, arr.byteLength);
    await this.client.hset(this.queueKeys.job(this.id), { [field]: buf });
  }

  private serializeData(data: D): string {
    const serialized = this.serializer.serialize(data);
    const byteLen = Buffer.byteLength(serialized, 'utf8');
    if (byteLen > MAX_JOB_DATA_SIZE) {
      throw new Error(`Job data exceeds maximum size (${byteLen} bytes > ${MAX_JOB_DATA_SIZE})`);
    }
    return serialized;
  }
}
