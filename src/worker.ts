import { EventEmitter } from 'events';
import type { WorkerOptions, Processor, Client } from './types';
import { Job } from './job';
import { buildKeys, calculateBackoff, keyPrefix } from './utils';
import { createClient, createBlockingClient, ensureFunctionLibrary, createConsumerGroup } from './connection';
import { CONSUMER_GROUP } from './functions/index';
import { completeJob, failJob, rateLimit as rateLimitFn, checkConcurrency } from './functions/index';
import { Scheduler } from './scheduler';
import { withSpan, withChildSpan } from './telemetry';
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
  private pollLoop(): void {
    if (!this.running || this.paused || this.closing) return;

    this.pollOnce()
      .then(() => {
        // Successful poll resets backoff
        this.reconnectBackoff = 0;
        this.pollLoop();
      })
      .catch((err) => {
        if (this.running && !this.closing) {
          this.emit('error', err);
          // Exponential backoff: 1s, 2s, 4s, 8s, max 30s
          const delay = this.reconnectBackoff === 0
            ? 1000
            : Math.min(this.reconnectBackoff * 2, 30000);
          this.reconnectBackoff = delay;
          setTimeout(() => this.reconnectAndResume(), delay);
        }
      });
  }

  /**
   * Attempt to reconnect clients and resume polling after a connection error.
   */
  private async reconnectAndResume(): Promise<void> {
    if (!this.running || this.closing) return;

    try {
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

      this.reconnectBackoff = 0;
      this.pollLoop();
    } catch (err) {
      if (this.running && !this.closing) {
        this.emit('error', err);
        const delay = this.reconnectBackoff === 0
          ? 1000
          : Math.min(this.reconnectBackoff * 2, 30000);
        this.reconnectBackoff = delay;
        setTimeout(() => this.reconnectAndResume(), delay);
      }
    }
  }

  private async pollOnce(): Promise<void> {
    if (!this.blockingClient || !this.commandClient) return;

    // Calculate how many jobs we can fetch without exceeding concurrency
    const available = this.prefetch - this.activeCount;
    if (available <= 0) {
      // At capacity - wait a bit for active jobs to finish
      await new Promise<void>((resolve) => setTimeout(resolve, 50));
      return;
    }

    // Check global concurrency limit before fetching new jobs.
    // Returns -1 (no limit), 0 (blocked), or positive remaining capacity.
    const gcRemaining = await checkConcurrency(
      this.commandClient,
      this.queueKeys,
      CONSUMER_GROUP,
    );
    if (gcRemaining === 0) {
      await new Promise<void>((resolve) => setTimeout(resolve, 100));
      return;
    }

    // Limit fetch count to the smaller of local available and global remaining
    const fetchCount = gcRemaining > 0 ? Math.min(available, gcRemaining) : available;

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

        // Dispatch the job for processing (non-blocking)
        this.dispatchJob(jobId, String(entryId));
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

  private async processJob(jobId: string, entryId: string): Promise<void> {
    if (!this.commandClient) return;

    // Fetch job data from hash
    const hashData = await this.commandClient.hgetall(this.queueKeys.job(jobId));
    if (!hashData || hashData.length === 0) {
      // Job hash missing - ACK and skip
      try {
        await completeJob(
          this.commandClient,
          this.queueKeys,
          jobId,
          entryId,
          'null',
          Date.now(),
          CONSUMER_GROUP,
        );
      } catch {
        // Best effort
      }
      return;
    }

    // Convert HashDataType ({ field, value }[]) to Record<string, string>
    const hash: Record<string, string> = {};
    for (const entry of hashData) {
      hash[String(entry.field)] = String(entry.value);
    }

    const job = Job.fromHash<D, R>(this.commandClient, this.queueKeys, jobId, hash);

    // Attach entryId for completion/failure XACK
    (job as any)._entryId = entryId;

    // Rate limiting: check before processing
    if (this.opts.limiter) {
      await this.waitForRateLimit();
    }

    await withSpan(
      'glide-mq.worker.process',
      {
        'glide-mq.queue': this.name,
        'glide-mq.job.id': jobId,
        'glide-mq.job.name': job.name,
        'glide-mq.job.attemptsMade': job.attemptsMade,
      },
      async () => {
        try {
          const result = await this.processor(job);
          const returnvalue = result !== undefined ? JSON.stringify(result) : 'null';

          // Build parent info if this job is a child in a flow
          let parentInfo: Parameters<typeof completeJob>[8];
          if (job.parentId && job.parentQueue) {
            const parentKeys = buildKeys(job.parentQueue, this.opts.prefix);
            const childQueuePrefix = keyPrefix(this.opts.prefix ?? 'glide', this.name);
            parentInfo = {
              depsMember: `${childQueuePrefix}:${jobId}`,
              parentId: job.parentId,
              parentKeys,
            };
          }

          if (!this.commandClient) return;
          await withChildSpan(
            'glide-mq.worker.complete',
            { 'glide-mq.job.id': jobId },
            async () => {
              await completeJob(
                this.commandClient!,
                this.queueKeys,
                jobId,
                entryId,
                returnvalue,
                Date.now(),
                CONSUMER_GROUP,
                job.opts.removeOnComplete,
                parentInfo,
              );
            },
          );

          job.returnvalue = result;
          job.finishedOn = Date.now();

          this.emit('completed', job, result);
        } catch (err) {
          // If the processor threw RateLimitError, apply manual rate limit delay
          // and re-queue the job as delayed instead of failing it.
          if (err instanceof Worker.RateLimitError) {
            const delayMs = (err as any).delayMs || (this.opts.limiter?.duration ?? 1000);
            this.rateLimitUntil = Date.now() + delayMs;
            if (!this.commandClient) return;
            try {
              await failJob(
                this.commandClient,
                this.queueKeys,
                jobId,
                entryId,
                'rate limited',
                Date.now(),
                job.attemptsMade + 2,
                delayMs,
                CONSUMER_GROUP,
              );
            } catch (failErr) {
              this.emit('error', failErr);
            }
            return;
          }

          const error = err instanceof Error ? err : new Error(String(err));
          const maxAttempts = job.opts.attempts ?? 0;
          let backoffDelay = 0;

          if (maxAttempts > 0 && job.opts.backoff) {
            backoffDelay = calculateBackoff(
              job.opts.backoff.type,
              job.opts.backoff.delay,
              job.attemptsMade + 1,
              job.opts.backoff.jitter,
            );
          }

          if (!this.commandClient) {
            job.failedReason = error.message;
            this.emit('failed', job, error);
            return;
          }

          await withChildSpan(
            'glide-mq.worker.fail',
            { 'glide-mq.job.id': jobId, 'glide-mq.error': error.message },
            async () => {
              await failJob(
                this.commandClient!,
                this.queueKeys,
                jobId,
                entryId,
                error.message,
                Date.now(),
                maxAttempts,
                backoffDelay,
                CONSUMER_GROUP,
                job.opts.removeOnFail,
              );
            },
          );

          job.failedReason = error.message;
          this.emit('failed', job, error);
        }
      },
    );
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
