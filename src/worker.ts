import { EventEmitter } from 'events';
import type { WorkerOptions, Processor, Client } from './types';
import { Job } from './job';
import { buildKeys, calculateBackoff } from './utils';
import { createClient, createBlockingClient, ensureFunctionLibrary, createConsumerGroup } from './connection';
import { CONSUMER_GROUP } from './functions/index';
import { completeJob, failJob } from './functions/index';
import { Scheduler } from './scheduler';
export type WorkerEvent = 'completed' | 'failed' | 'error' | 'stalled';

export class Worker<D = any, R = any> extends EventEmitter {
  readonly name: string;
  private opts: WorkerOptions;
  private processor: Processor<D, R>;
  private commandClient: Client | null = null;
  private blockingClient: Client | null = null;
  private running = false;
  private paused = false;
  private closing = false;
  private queueKeys: ReturnType<typeof buildKeys>;
  private consumerId: string;
  private activeCount = 0;
  private activePromises: Set<Promise<void>> = new Set();
  private scheduler: Scheduler | null = null;
  private initPromise: Promise<void>;

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
    await ensureFunctionLibrary(this.commandClient);

    // Create consumer group on the stream (idempotent)
    await createConsumerGroup(
      this.commandClient,
      this.queueKeys.stream,
      CONSUMER_GROUP,
    );

    // Start the internal scheduler for delayed promotion + stalled recovery
    this.scheduler = new Scheduler(this.commandClient, this.queueKeys, {
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
   */
  private pollLoop(): void {
    if (!this.running || this.paused || this.closing) return;

    this.pollOnce()
      .then(() => {
        // Continue polling
        this.pollLoop();
      })
      .catch((err) => {
        if (this.running && !this.closing) {
          this.emit('error', err);
          // Back off slightly on errors, then retry
          setTimeout(() => this.pollLoop(), 1000);
        }
      });
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

    // XREADGROUP GROUP {group} {consumerId} COUNT {available} BLOCK {blockTimeout}
    // STREAMS {streamKey} >
    const result = await this.blockingClient.xreadgroup(
      CONSUMER_GROUP,
      this.consumerId,
      { [this.queueKeys.stream]: '>' },
      { count: available, block: this.blockTimeout },
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

    try {
      const result = await this.processor(job);
      const returnvalue = result !== undefined ? JSON.stringify(result) : 'null';

      await completeJob(
        this.commandClient,
        this.queueKeys,
        jobId,
        entryId,
        returnvalue,
        Date.now(),
        CONSUMER_GROUP,
      );

      job.returnvalue = result;
      job.finishedOn = Date.now();

      this.emit('completed', job, result);
    } catch (err) {
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

      try {
        await failJob(
          this.commandClient,
          this.queueKeys,
          jobId,
          entryId,
          error.message,
          Date.now(),
          maxAttempts,
          backoffDelay,
          CONSUMER_GROUP,
        );
      } catch (failErr) {
        this.emit('error', failErr);
      }

      job.failedReason = error.message;
      this.emit('failed', job, error);
    }
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
   * Close the worker. If force=false (default), waits for active jobs to finish.
   */
  async close(force?: boolean): Promise<void> {
    this.closing = true;
    this.running = false;

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
