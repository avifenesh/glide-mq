import { EventEmitter } from 'events';
import type { WorkerOptions, Processor, Client } from './types';
import { Job } from './job';
import { buildKeys, calculateBackoff, keyPrefix, nextReconnectDelay, reconnectWithBackoff } from './utils';
import { createClient, createBlockingClient, ensureFunctionLibrary, createConsumerGroup } from './connection';
import {
  CONSUMER_GROUP,
  completeJob,
  completeAndFetchNext,
  failJob,
  addJob,
  rateLimit as rateLimitFn,
  checkConcurrency,
  moveToActive,
  deferActive,
} from './functions/index';
import type { QueueKeys } from './functions/index';
import { Scheduler } from './scheduler';
export type WorkerEvent = 'completed' | 'failed' | 'error' | 'stalled' | 'closing' | 'closed';

export class Worker<D = any, R = any> extends EventEmitter {
  readonly name: string;
  private opts: WorkerOptions;
  private processor: Processor<D, R>;
  private commandClient: Client | null = null;
  private blockingClient: Client | null = null;
  private running = false;
  private paused = false;
  private closing = false;
  private closed = false;
  private queueKeys: ReturnType<typeof buildKeys>;
  private consumerId: string;
  private activeCount = 0;
  private activePromises: Set<Promise<void>> = new Set();
  private activeAbortControllers: Map<string, AbortController> = new Map();
  private scheduler: Scheduler | null = null;
  private initPromise: Promise<void>;
  private rateLimitUntil = 0;
  private reconnectBackoff = 0;

  // Configurable defaults
  private concurrency: number;
  private prefetch: number;
  private blockTimeout: number;
  private stalledInterval: number;
  private maxStalledCount: number;
  private lockDuration: number;
  private heartbeatIntervals: Map<string, ReturnType<typeof setInterval>> = new Map();
  private globalConcurrencyEnabled = false;

  constructor(name: string, processor: Processor<D, R>, opts: WorkerOptions) {
    super();
    this.name = name;
    this.processor = processor;
    this.opts = opts;
    this.queueKeys = buildKeys(name, opts.prefix);
    this.consumerId = `worker-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

    this.concurrency = opts.concurrency ?? 1;
    this.prefetch = opts.prefetch ?? this.concurrency;
    this.blockTimeout = opts.blockTimeout ?? 5000;
    this.stalledInterval = opts.stalledInterval ?? 30000;
    this.maxStalledCount = opts.maxStalledCount ?? 1;
    this.lockDuration = opts.lockDuration ?? 30000;

    // Auto-init: start the worker immediately
    this.initPromise = this.init();
  }

  /**
   * Wait for the worker to be fully initialized and connected.
   */
  async waitUntilReady(): Promise<void> {
    return this.initPromise;
  }

  private async init(): Promise<void> {
    this.commandClient = await createClient(this.opts.connection);
    this.blockingClient = await createBlockingClient(this.opts.connection);
    await ensureFunctionLibrary(
      this.commandClient,
      undefined,
      this.opts.connection.clusterMode ?? false,
    );

    // Create consumer group on the stream (idempotent)
    await createConsumerGroup(
      this.commandClient,
      this.queueKeys.stream,
      CONSUMER_GROUP,
    );

    // Check if global concurrency is configured (read once, avoid per-poll FCALL)
    const gcVal = await this.commandClient.hget(this.queueKeys.meta, 'globalConcurrency');
    this.globalConcurrencyEnabled = gcVal != null && Number(gcVal) > 0;

    // Start the internal scheduler for delayed promotion + stalled recovery
    this.scheduler = new Scheduler(this.commandClient, this.queueKeys, {
      promotionInterval: this.opts.promotionInterval,
      stalledInterval: this.stalledInterval,
      maxStalledCount: this.maxStalledCount,
      consumerId: this.consumerId,
    });
    this.scheduler.start();

    this.running = true;
    this.pollLoop();
  }

  /**
   * Main poll loop: XREADGROUP BLOCK on the stream, dispatch jobs to the processor.
   * Respects concurrency limits by only requesting (prefetch - activeCount) entries.
   * On connection errors, uses exponential backoff (1s, 2s, 4s, 8s, max 30s) and reconnects.
   */
  private async pollLoop(): Promise<void> {
    while (this.running && !this.paused && !this.closing) {
      try {
        await this.pollOnce();
        this.reconnectBackoff = 0;
      } catch (err) {
        if (this.running && !this.closing) {
          this.emit('error', err);
          this.reconnectBackoff = nextReconnectDelay(this.reconnectBackoff);
          await this.reconnectAndResume();
          return; // reconnectAndResume restarts the loop
        }
      }
    }
  }

  private reconnectCtx = {
    isActive: () => this.running && !this.closing,
    getBackoff: () => this.reconnectBackoff,
    setBackoff: (ms: number) => { this.reconnectBackoff = ms; },
    onError: (err: unknown) => { this.emit('error', err); },
  };

  /**
   * Attempt to reconnect clients and resume polling after a connection error.
   */
  private async reconnectAndResume(): Promise<void> {
    await reconnectWithBackoff(
      this.reconnectCtx,
      async () => {
        // Close stale clients
        if (this.commandClient) {
          try { this.commandClient.close(); } catch { /* ignore */ }
          this.commandClient = null;
        }
        if (this.blockingClient) {
          try { this.blockingClient.close(); } catch { /* ignore */ }
          this.blockingClient = null;
        }

        // Recreate clients
        this.commandClient = await createClient(this.opts.connection);
        this.blockingClient = await createBlockingClient(this.opts.connection);
        await ensureFunctionLibrary(
          this.commandClient,
          undefined,
          this.opts.connection.clusterMode ?? false,
        );

        // Re-ensure consumer group
        await createConsumerGroup(
          this.commandClient,
          this.queueKeys.stream,
          CONSUMER_GROUP,
        );

        // Restart scheduler with the new client
        if (this.scheduler) {
          this.scheduler.stop();
        }
        this.scheduler = new Scheduler(this.commandClient, this.queueKeys, {
          promotionInterval: this.opts.promotionInterval,
          stalledInterval: this.stalledInterval,
          maxStalledCount: this.maxStalledCount,
          consumerId: this.consumerId,
        });
        this.scheduler.start();
      },
      () => this.pollLoop(),
    );
  }

  private async pollOnce(): Promise<void> {
    if (!this.blockingClient || !this.commandClient) return;

    // Calculate how many jobs we can fetch without exceeding concurrency
    const available = this.prefetch - this.activeCount;
    if (available <= 0) {
      // At capacity - yield to let active jobs finish, then retry
      await new Promise<void>((resolve) => setTimeout(resolve, 5));
      return;
    }

    let fetchCount = available;

    // Only check global concurrency if configured. Skipping this FCALL entirely
    // saves one Valkey round trip per poll cycle (~0.2ms).
    if (this.globalConcurrencyEnabled) {
      const gcRemaining = await checkConcurrency(
        this.commandClient,
        this.queueKeys,
        CONSUMER_GROUP,
      );
      if (gcRemaining === 0) {
        await new Promise<void>((resolve) => setTimeout(resolve, 20));
        return;
      }
      if (gcRemaining > 0) {
        fetchCount = Math.min(available, gcRemaining);
      }
    }

    // XREADGROUP GROUP {group} {consumerId} COUNT {fetchCount} BLOCK {blockTimeout}
    // STREAMS {streamKey} >
    const result = await this.blockingClient.xreadgroup(
      CONSUMER_GROUP,
      this.consumerId,
      { [this.queueKeys.stream]: '>' },
      { count: fetchCount, block: this.blockTimeout },
    );

    if (!result) {
      // null means no messages (timeout expired) - just loop again
      return;
    }

    // result is GlideRecord<Record<string, [GlideString, GlideString][] | null>>
    // i.e. { key, value }[] where value is Record<entryId, fieldPairs | null>
    for (const streamEntry of result) {
      const entries = streamEntry.value;
      for (const [entryId, fieldPairs] of Object.entries(entries)) {
        if (!fieldPairs) continue; // deleted entry

        // Parse the stream entry fields to extract jobId
        let jobId: string | null = null;
        for (const [field, value] of fieldPairs) {
          if (String(field) === 'jobId') {
            jobId = String(value);
            break;
          }
        }

        if (!jobId) continue;

        if (this.concurrency === 1) {
          // c=1 fast path: process inline (blocks poll loop, no activeCount tracking needed)
          await this.processJob(jobId, String(entryId));
        } else {
          this.dispatchJob(jobId, String(entryId));
        }
      }
    }
  }

  /**
   * Dispatch a single job for processing.
   * Increments activeCount, runs the processor, then completes or fails the job.
   */
  private dispatchJob(jobId: string, entryId: string): void {
    this.activeCount++;

    const promise = this.processJob(jobId, entryId).finally(() => {
      this.activeCount--;
      this.activePromises.delete(promise);
    });

    this.activePromises.add(promise);
  }

  // ---- Job processing helpers ----

  /**
   * Handle a moveToActive result that is not a valid hash (null or REVOKED).
   * Returns true if the result was handled (caller should return), false if the hash is valid.
   */
  private async handleMoveToActiveEdgeCase(
    moveResult: Record<string, string> | 'REVOKED' | null,
    jobId: string,
    entryId: string,
  ): Promise<boolean> {
    if (!this.commandClient) return true;
    if (moveResult === null) {
      try { await completeJob(this.commandClient, this.queueKeys, jobId, entryId, 'null', Date.now(), CONSUMER_GROUP); } catch (err) { this.emit('error', err); }
      return true;
    }
    if (moveResult === 'REVOKED') {
      try { await failJob(this.commandClient, this.queueKeys, jobId, entryId, 'revoked', Date.now(), 0, 0, CONSUMER_GROUP); } catch (err) { this.emit('error', err); }
      return true;
    }
    return false;
  }

  /**
   * Run the processor with optional timeout, AbortController, and heartbeat.
   * Returns { result, error } - exactly one will be set.
   */
  private async runProcessor(
    job: Job<D, R>,
    jobId: string,
  ): Promise<{ result?: R; error?: Error; aborted: boolean }> {
    if (this.opts.limiter) await this.waitForRateLimit();

    const ac = new AbortController();
    this.activeAbortControllers.set(jobId, ac);
    job.abortSignal = ac.signal;
    this.startHeartbeat(jobId);

    let result: R | undefined;
    let error: Error | undefined;

    try {
      const timeoutMs = job.opts.timeout;
      if (timeoutMs && timeoutMs > 0) {
        result = await Promise.race([
          this.processor(job),
          new Promise<never>((_, reject) => setTimeout(() => reject(new Error('Job timeout exceeded')), timeoutMs)),
        ]);
      } else {
        result = await this.processor(job);
      }
    } catch (err) {
      error = err instanceof Error ? err : new Error(String(err));
    } finally {
      this.stopHeartbeat(jobId);
      this.activeAbortControllers.delete(jobId);
    }

    return { result, error, aborted: ac.signal.aborted };
  }

  /**
   * Handle a failed job: applies rate limiting, backoff, DLQ, and emits 'failed'.
   * Returns true if the error was a RateLimitError (caller should skip the 'failed' event).
   */
  private async handleJobFailure(
    job: Job<D, R>,
    jobId: string,
    entryId: string,
    error: Error,
  ): Promise<boolean> {
    if (!this.commandClient) {
      job.failedReason = error.message;
      this.emit('failed', job, error);
      return true;
    }

    if (error instanceof Worker.RateLimitError) {
      const delayMs = (error as any).delayMs || (this.opts.limiter?.duration ?? 1000);
      this.rateLimitUntil = Date.now() + delayMs;
      try { await failJob(this.commandClient, this.queueKeys, jobId, entryId, 'rate limited', Date.now(), job.attemptsMade + 2, delayMs, CONSUMER_GROUP); } catch (e) { this.emit('error', e); }
      return false;
    }

    const maxAttempts = job.opts.attempts ?? 0;
    let backoffDelay = 0;
    if (maxAttempts > 0 && job.opts.backoff) {
      const strategyFn = this.opts.backoffStrategies?.[job.opts.backoff.type];
      backoffDelay = strategyFn
        ? strategyFn(job.attemptsMade + 1, error)
        : calculateBackoff(job.opts.backoff.type, job.opts.backoff.delay, job.attemptsMade + 1, job.opts.backoff.jitter);
    }

    const failResult = await failJob(
      this.commandClient, this.queueKeys, jobId, entryId, error.message,
      Date.now(), maxAttempts, backoffDelay, CONSUMER_GROUP, job.opts.removeOnFail,
    );

    if (failResult === 'failed' && this.opts.deadLetterQueue && this.commandClient) {
      await this.moveToDLQ(job, error);
    }
    job.failedReason = error.message;
    this.emit('failed', job, error);
    return failResult === 'failed';
  }

  /**
   * Build parent dependency info for complete/completeAndFetchNext calls.
   */
  private buildParentInfo(
    job: Job<D, R>,
    jobId: string,
  ): { depsMember: string; parentId: string; parentKeys: QueueKeys } | undefined {
    if (!job.parentId || !job.parentQueue) return undefined;
    return {
      depsMember: `${keyPrefix(this.opts.prefix ?? 'glide', this.name)}:${jobId}`,
      parentId: job.parentId,
      parentKeys: buildKeys(job.parentQueue, this.opts.prefix),
    };
  }

  private orderingMetaField(job: Job<D, R>): string | null {
    if (!job.orderingKey || !job.orderingSeq || job.orderingSeq <= 0) return null;
    return `orderdone:${job.orderingKey}`;
  }

  /**
   * Checks whether this job can run now under per-key ordering.
   * Returns false when an earlier sequence for the same key is still pending.
   */
  private async isOrderingTurn(job: Job<D, R>): Promise<boolean> {
    if (!this.commandClient) return false;
    const field = this.orderingMetaField(job);
    if (!field) return true;

    const targetSeq = job.orderingSeq as number;
    const lastDoneRaw = await this.commandClient.hget(this.queueKeys.meta, field);
    const lastDone = lastDoneRaw ? Number(lastDoneRaw) : 0;
    return targetSeq <= (Number.isFinite(lastDone) ? lastDone : 0) + 1;
  }

  /**
   * Re-enqueue out-of-order jobs instead of holding an active slot.
   */
  private async deferOutOfOrderJob(jobId: string, entryId: string): Promise<void> {
    if (!this.commandClient) return;
    await deferActive(this.commandClient, this.queueKeys, jobId, entryId, CONSUMER_GROUP);
  }

  private async markOrderingDone(job: Job<D, R>): Promise<void> {
    if (!this.commandClient) return;
    const field = this.orderingMetaField(job);
    if (!field) return;
    const seq = job.orderingSeq as number;
    const lastDoneRaw = await this.commandClient.hget(this.queueKeys.meta, field);
    const lastDone = lastDoneRaw ? Number(lastDoneRaw) : 0;
    if (seq > (Number.isFinite(lastDone) ? lastDone : 0)) {
      await this.commandClient.hset(this.queueKeys.meta, { [field]: String(seq) });
    }
  }

  // ---- Main processing path ----

  /**
   * Process a job through its full lifecycle: activate, run processor, complete, fetch next.
   * Used for both c=1 (inline, blocking poll loop) and c>1 (dispatched via dispatchJob).
   * Chains into the next job via completeAndFetchNext to reuse the same dispatch slot.
   */
  private async processJob(jobId: string, entryId: string): Promise<void> {
    if (!this.commandClient) return;

    let currentJobId = jobId;
    let currentEntryId = entryId;
    let currentHash: Record<string, string> | null = null;

    // Loop: process current job, then chain into next via completeAndFetchNext.
    // This reuses the same dispatch slot (activeCount) for sequential jobs.
    while (this.running && !this.closing && this.commandClient) {
      // Activate the job (skip if we already have a pre-fetched hash from completeAndFetchNext)
      if (!currentHash) {
        const moveResult = await moveToActive(this.commandClient, this.queueKeys, currentJobId, Date.now());
        if (await this.handleMoveToActiveEdgeCase(moveResult, currentJobId, currentEntryId)) return;
        currentHash = moveResult as Record<string, string>;
      }

      const job = Job.fromHash<D, R>(this.commandClient, this.queueKeys, currentJobId, currentHash);
      job.entryId = currentEntryId;

      const orderingReady = await this.isOrderingTurn(job);
      if (!orderingReady) {
        await this.deferOutOfOrderJob(currentJobId, currentEntryId);
        return;
      }

      const { result: processResult, error: processError, aborted } = await this.runProcessor(job, currentJobId);

      if (processError || aborted) {
        const terminalFailure = await this.handleJobFailure(
          job,
          currentJobId,
          currentEntryId,
          aborted ? new Error('revoked') : processError!,
        );
        if (terminalFailure) {
          await this.markOrderingDone(job);
        }
        return;
      }

      if (!this.commandClient) return;

      const returnvalue = processResult !== undefined ? JSON.stringify(processResult) : 'null';
      const parentInfo = this.buildParentInfo(job, currentJobId);

      const fetchResult = await completeAndFetchNext(
        this.commandClient, this.queueKeys, currentJobId, currentEntryId,
        returnvalue, Date.now(), CONSUMER_GROUP, this.consumerId,
        job.opts.removeOnComplete, parentInfo,
      );

      job.returnvalue = processResult;
      job.finishedOn = Date.now();
      await this.markOrderingDone(job);
      this.emit('completed', job, processResult);

      // No next job - return to poll loop
      if (fetchResult.next === false) return;

      if (fetchResult.next === 'REVOKED') {
        if (fetchResult.nextJobId && fetchResult.nextEntryId) {
          try { await failJob(this.commandClient, this.queueKeys, fetchResult.nextJobId, fetchResult.nextEntryId, 'revoked', Date.now(), 0, 0, CONSUMER_GROUP); } catch (err) { this.emit('error', err); }
        }
        return;
      }

      // Chain into the next job within the same dispatch slot
      currentJobId = fetchResult.nextJobId!;
      currentEntryId = fetchResult.nextEntryId!;
      currentHash = fetchResult.next as unknown as Record<string, string>;
    }
  }

  /**
   * Abort a job that is currently being processed by this worker.
   * The processor receives the abort signal via job.abortSignal and must check it cooperatively.
   * Returns true if the job was found and aborted, false if not currently active.
   */
  abortJob(jobId: string): boolean {
    const ac = this.activeAbortControllers.get(jobId);
    if (ac) {
      ac.abort();
      return true;
    }
    return false;
  }

  private startHeartbeat(jobId: string): void {
    if (!this.commandClient) return;
    // Only start periodic heartbeat for long lockDurations where stall detection matters.
    // moveToActive already writes the initial lastActive - protects against immediate stall reclaim.
    // For the default 30s lockDuration with 30s stalledInterval, the heartbeat fires at 15s.
    // Skip entirely if lockDuration >= stalledInterval (initial write is sufficient for one cycle).
    if (this.lockDuration >= this.stalledInterval) return;
    const interval = this.lockDuration / 2;
    const client = this.commandClient;
    const jobKey = this.queueKeys.job(jobId);
    const timer = setInterval(() => {
      client.hset(jobKey, { lastActive: Date.now().toString() }).catch(() => {});
    }, interval);
    this.heartbeatIntervals.set(jobId, timer);
  }

  private stopHeartbeat(jobId: string): void {
    const timer = this.heartbeatIntervals.get(jobId);
    if (timer) {
      clearInterval(timer);
      this.heartbeatIntervals.delete(jobId);
    }
  }

  private async moveToDLQ(job: Job<D, R>, error: Error): Promise<void> {
    if (!this.commandClient || !this.opts.deadLetterQueue) return;
    const dlqName = this.opts.deadLetterQueue.name;
    const dlqKeys = buildKeys(dlqName, this.opts.prefix);
    try {
      const dlqData = JSON.stringify({
        originalQueue: this.name,
        originalJobId: job.id,
        data: job.data,
        failedReason: error.message,
        attemptsMade: job.attemptsMade,
      });
      await addJob(
        this.commandClient,
        dlqKeys,
        job.name,
        dlqData,
        JSON.stringify({}),
        Date.now(),
        0,
        0,
        '',
        0,
      );
    } catch (dlqErr) {
      this.emit('error', dlqErr);
    }
  }

  /**
   * Check the server-side rate limiter and wait if the limit is exceeded.
   * Also respects any manual rate limit set via rateLimit(ms).
   */
  private async waitForRateLimit(): Promise<void> {
    if (!this.commandClient || !this.opts.limiter) return;

    // First, respect any manual rate limit
    const now = Date.now();
    if (this.rateLimitUntil > now) {
      await new Promise<void>((resolve) => setTimeout(resolve, this.rateLimitUntil - now));
    }

    // Server-side sliding window check
    // eslint-disable-next-line no-constant-condition
    while (true) {
      const delayMs = await rateLimitFn(
        this.commandClient,
        this.queueKeys,
        this.opts.limiter.max,
        this.opts.limiter.duration,
        Date.now(),
      );

      if (delayMs <= 0) break;

      // Wait for the delay, then re-check
      await new Promise<void>((resolve) => setTimeout(resolve, delayMs));
    }
  }

  /**
   * Check if the worker is currently running and not paused.
   */
  isRunning(): boolean {
    return this.running && !this.paused;
  }

  /**
   * Check if the worker is currently paused.
   */
  isPaused(): boolean {
    return this.paused;
  }

  /**
   * Manually trigger a rate limit pause for the given duration.
   * Subsequent jobs will wait until the pause expires.
   */
  async rateLimit(ms: number): Promise<void> {
    this.rateLimitUntil = Date.now() + ms;
  }

  /**
   * Pause the worker. If force=false (default), waits for active jobs to finish.
   */
  async pause(force?: boolean): Promise<void> {
    this.paused = true;
    if (!force) {
      await this.waitForActiveJobs();
    }
  }

  /**
   * Resume the worker after a pause.
   */
  async resume(): Promise<void> {
    await this.initPromise;
    this.paused = false;
    this.pollLoop();
  }

  /**
   * Process all remaining jobs in the queue, then stop gracefully.
   * Keeps polling until both the stream and scheduled ZSet are empty,
   * then closes the worker.
   */
  async drain(): Promise<void> {
    await this.initPromise;

    // Poll until everything is empty
    // eslint-disable-next-line no-constant-condition
    while (true) {
      if (!this.commandClient) break;

      // Wait for active jobs to complete
      await this.waitForActiveJobs();

      // Check if stream and scheduled set are both empty
      const streamLen = await this.commandClient.xlen(this.queueKeys.stream);
      const scheduledLen = await this.commandClient.zcard(this.queueKeys.scheduled);

      if (streamLen === 0 && scheduledLen === 0 && this.activeCount === 0) {
        break;
      }

      // Small delay before re-checking
      await new Promise<void>((resolve) => setTimeout(resolve, 100));
    }

    await this.close();
  }

  /**
   * Close the worker. If force=false (default), waits for active jobs to finish.
   * Idempotent: safe to call multiple times.
   */
  async close(force?: boolean): Promise<void> {
    if (this.closed) return;
    if (this.closing) {
      // Already closing - wait for init to settle, then return
      await this.initPromise.catch(() => {});
      return;
    }

    this.closing = true;
    this.running = false;
    this.emit('closing');

    // Wait for init to complete so clients are available for cleanup
    await this.initPromise.catch(() => {});

    if (this.scheduler) {
      this.scheduler.stop();
      this.scheduler = null;
    }

    if (!force) {
      await this.waitForActiveJobs();
    }

    // Clear all active heartbeats
    for (const [, timer] of this.heartbeatIntervals) {
      clearInterval(timer);
    }
    this.heartbeatIntervals.clear();

    if (this.commandClient) {
      this.commandClient.close();
      this.commandClient = null;
    }
    if (this.blockingClient) {
      this.blockingClient.close();
      this.blockingClient = null;
    }

    this.closed = true;
    this.emit('closed');
  }

  private async waitForActiveJobs(): Promise<void> {
    if (this.activePromises.size > 0) {
      await Promise.allSettled([...this.activePromises]);
    }
  }

  static RateLimitError = class extends Error {
    constructor() {
      super('Rate limit exceeded');
      this.name = 'RateLimitError';
    }
  };
}
