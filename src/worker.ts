import { EventEmitter } from 'events';
import { randomBytes } from 'crypto';
import os from 'os';
import { TimeUnit } from '@glidemq/speedkey';
import type { WorkerOptions, Processor, BatchProcessor, Client, Serializer, SchedulerEntry } from './types';
import { JSON_SERIALIZER } from './types';
import { Job } from './job';
import {
  buildKeys,
  calculateBackoff,
  computeFollowingSchedulerNextRun,
  keyPrefix,
  nextReconnectDelay,
  reconnectWithBackoff,
  MAX_JOB_DATA_SIZE,
} from './utils';
import { createSandboxedProcessor } from './sandbox';
import {
  createClient,
  createBlockingClient,
  ensureFunctionLibrary,
  ensureFunctionLibraryOnce,
  createConsumerGroup,
} from './connection';
import {
  GlideMQError,
  ConnectionError,
  DelayedError,
  WaitingChildrenError,
  UnrecoverableError,
  BatchError,
} from './errors';
import {
  CONSUMER_GROUP,
  completeJob,
  completeAndFetchNext,
  failJob,
  addJob,
  rateLimit as rateLimitFn,
  checkConcurrency,
  rpopAndReserve,
  moveToActive,
  moveActiveToDelayed,
  moveToWaitingChildren,
  deferActive,
} from './functions/index';
import type { QueueKeys } from './functions/index';
import { Scheduler } from './scheduler';
export type WorkerEvent = 'completed' | 'failed' | 'error' | 'stalled' | 'closing' | 'closed' | 'active' | 'drained';

export class Worker<D = any, R = any> extends EventEmitter {
  readonly name: string;
  private opts: WorkerOptions;
  private processor: Processor<D, R>;
  private commandClient: Client | null = null;
  private commandClientOwned = true;
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
  private isDrained = true;
  private reconnectBackoff = 0;
  private internalEvents = new EventEmitter();

  // Configurable defaults
  private concurrency: number;
  private prefetch: number;
  private blockTimeout: number;
  private stalledInterval: number;
  private maxStalledCount: number;
  private lockDuration: number;
  private heartbeatIntervals: Map<string, ReturnType<typeof setInterval>> = new Map();
  private xreadStreams: Record<string, string> = Object.create(null);
  private globalConcurrencyEnabled = false;
  private globalRateLimitEnabled = false;
  private cachedRateLimitMax = 0;
  private cachedRateLimitDuration = 0;
  private sandboxClose?: (force?: boolean) => Promise<void>;
  private workerHeartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private pollLoopPromise: Promise<void> | null = null;
  private readonly startedAt = Date.now();
  private readonly hostname = os.hostname();
  private serializer: Serializer;
  private readonly batchMode: boolean;
  private readonly batchSize: number;
  private readonly batchTimeout: number;
  private readonly batchProcessor: BatchProcessor<D, R> | null;

  constructor(name: string, processor: Processor<D, R> | BatchProcessor<D, R> | string, opts: WorkerOptions) {
    super();

    // Validate client injection options
    if (opts.client && opts.commandClient) {
      throw new GlideMQError('Provide either `client` or `commandClient`, not both.');
    }
    const injectedClient = opts.commandClient ?? opts.client;
    if (!opts.connection && !injectedClient) {
      throw new GlideMQError('Either `connection` or `client`/`commandClient` must be provided.');
    }
    if (!opts.connection && injectedClient) {
      throw new GlideMQError(
        'Worker requires `connection` even when a shared client is provided, ' +
          'because the blocking client for XREADGROUP must be auto-created.',
      );
    }

    this.name = name;

    // Batch mode validation
    this.batchMode = !!opts.batch;
    if (opts.batch) {
      if (!Number.isInteger(opts.batch.size) || opts.batch.size < 1 || opts.batch.size > 1000) {
        throw new GlideMQError('batch.size must be an integer between 1 and 1000');
      }
      if (opts.batch.timeout !== undefined && (opts.batch.timeout < 0 || !Number.isFinite(opts.batch.timeout))) {
        throw new GlideMQError('batch.timeout must be a non-negative finite number');
      }
      if (typeof processor === 'string') {
        throw new GlideMQError('Batch mode does not support sandbox (file path) processors');
      }
      this.batchSize = opts.batch.size;
      this.batchTimeout = opts.batch.timeout ?? 0;
      this.batchProcessor = processor as BatchProcessor<D, R>;
    } else {
      this.batchSize = 0;
      this.batchTimeout = 0;
      this.batchProcessor = null;
    }

    if (this.batchMode) {
      // In batch mode, processor is stored as batchProcessor above.
      this.processor = (() => {
        throw new Error('Single-job processor called in batch mode');
      }) as unknown as Processor<D, R>;
    } else if (typeof processor === 'string') {
      const concurrency = opts.concurrency ?? 1;
      const sandbox = createSandboxedProcessor<D, R>(processor, opts.sandbox, concurrency);
      this.processor = sandbox.processor;
      this.sandboxClose = (force?: boolean) => sandbox.close(force);
    } else {
      this.processor = processor as Processor<D, R>;
    }
    this.opts = opts;
    this.serializer = opts.serializer ?? JSON_SERIALIZER;
    this.queueKeys = buildKeys(name, opts.prefix);
    this.consumerId = `worker-${Date.now()}-${randomBytes(4).toString('hex')}`;

    this.concurrency = opts.concurrency ?? 1;
    this.prefetch = opts.prefetch ?? (this.batchMode ? this.concurrency * this.batchSize : this.concurrency);
    this.blockTimeout = opts.blockTimeout ?? 5000;
    this.stalledInterval = opts.stalledInterval ?? 30000;
    this.maxStalledCount = opts.maxStalledCount ?? 1;
    this.lockDuration = opts.lockDuration ?? 30000;

    // Auto-init: start the worker immediately
    this.initPromise = this.init();
    this.initPromise.catch((err) => {
      if (!this.closing && !this.closed) {
        this.emit('error', err);
      }
    });
  }

  /**
   * Wait for the worker to be fully initialized and connected.
   */
  async waitUntilReady(): Promise<void> {
    return this.initPromise;
  }

  private async init(): Promise<void> {
    const injectedClient = this.opts.commandClient ?? this.opts.client;
    const clusterMode = this.opts.connection!.clusterMode ?? false;
    if (injectedClient) {
      await ensureFunctionLibraryOnce(injectedClient, undefined, clusterMode);
      this.commandClient = injectedClient;
      this.commandClientOwned = false;
    } else {
      const client = await createClient(this.opts.connection!);
      await ensureFunctionLibrary(client, undefined, clusterMode);
      this.commandClient = client;
      this.commandClientOwned = true;
    }
    this.blockingClient = await createBlockingClient(this.opts.connection!);

    this.xreadStreams = { [this.queueKeys.stream]: '>' };

    // Create consumer group on the stream (idempotent)
    await createConsumerGroup(this.commandClient, this.queueKeys.stream, CONSUMER_GROUP);

    // Check if global concurrency / rate limit are configured (refreshed on scheduler tick)
    await this.refreshMetaFlags();

    // Start the internal scheduler for delayed promotion + stalled recovery
    this.scheduler = new Scheduler(this.commandClient, this.queueKeys, {
      promotionInterval: this.opts.promotionInterval,
      stalledInterval: this.stalledInterval,
      maxStalledCount: this.maxStalledCount,
      consumerId: this.consumerId,
      queuePrefix: keyPrefix(this.opts.prefix ?? 'glide', this.name),
      onPromotionTick: () => this.refreshMetaFlags(),
      onError: (err) => {
        if (!this.closing) {
          this.emit('error', err);
        }
      },
      serializer: this.serializer,
    });
    this.scheduler.start();

    // Register this worker and start periodic heartbeat
    await this.registerWorker();
    const heartbeatMs = Math.max(1000, Math.floor(this.stalledInterval / 2));
    this.workerHeartbeatTimer = setInterval(() => {
      void this.registerWorker();
    }, heartbeatMs);

    this.running = true;
    this.pollLoopPromise = this.pollLoop();
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
    setBackoff: (ms: number) => {
      this.reconnectBackoff = ms;
    },
    onError: (err: unknown) => {
      this.emit('error', err);
    },
  };

  /**
   * Attempt to reconnect clients and resume polling after a connection error.
   */
  private async reconnectAndResume(): Promise<void> {
    await reconnectWithBackoff(
      this.reconnectCtx,
      async () => {
        // Close stale blocking client (always owned)
        if (this.blockingClient) {
          try {
            this.blockingClient.close();
          } catch {
            /* ignore */
          }
          this.blockingClient = null;
        }

        if (this.commandClientOwned) {
          // Close and recreate owned command client
          if (this.commandClient) {
            try {
              this.commandClient.close();
            } catch {
              /* ignore */
            }
            this.commandClient = null;
          }
          const client = await createClient(this.opts.connection!);
          await ensureFunctionLibrary(client, undefined, this.opts.connection!.clusterMode ?? false);
          this.commandClient = client;
        } else {
          // Injected command client - verify liveness and re-ensure library
          try {
            await this.commandClient!.ping();
            // Re-ensure function library in case of failover/topology change
            await ensureFunctionLibrary(this.commandClient!, undefined, this.opts.connection!.clusterMode ?? false);
          } catch (err) {
            this.emit(
              'error',
              new ConnectionError('Shared command client is unreachable. The client owner must handle reconnection.'),
            );
            throw err;
          }
        }

        this.blockingClient = await createBlockingClient(this.opts.connection!);

        // Re-ensure consumer group
        await createConsumerGroup(this.commandClient!, this.queueKeys.stream, CONSUMER_GROUP);

        // Restart scheduler with the (possibly same) client
        if (this.scheduler) {
          this.scheduler.stop();
        }
        this.scheduler = new Scheduler(this.commandClient!, this.queueKeys, {
          promotionInterval: this.opts.promotionInterval,
          stalledInterval: this.stalledInterval,
          maxStalledCount: this.maxStalledCount,
          consumerId: this.consumerId,
          queuePrefix: keyPrefix(this.opts.prefix ?? 'glide', this.name),
          onPromotionTick: () => this.refreshMetaFlags(),
          onError: (err) => {
            if (!this.closing) {
              this.emit('error', err);
            }
          },
          serializer: this.serializer,
        });
        this.scheduler.start();

        // Re-register worker and restart heartbeat timer after reconnect
        if (this.workerHeartbeatTimer) {
          clearInterval(this.workerHeartbeatTimer);
        }
        await this.registerWorker();
        const hbMs = Math.max(1000, Math.floor(this.stalledInterval / 2));
        this.workerHeartbeatTimer = setInterval(() => {
          void this.registerWorker();
        }, hbMs);
      },
      () => {
        this.pollLoopPromise = this.pollLoop();
        return this.pollLoopPromise;
      },
    );
  }

  private async waitForSlot(): Promise<void> {
    if (this.prefetch - this.activeCount > 0) return;

    return new Promise<void>((resolve) => {
      // eslint-disable-next-line prefer-const
      let timer: ReturnType<typeof setTimeout>;

      const done = () => {
        this.internalEvents.off('slotFree', done);
        this.off('closing', done);
        clearTimeout(timer);
        resolve();
      };

      this.internalEvents.once('slotFree', done);
      this.once('closing', done);
      timer = setTimeout(done, 100);
    });
  }

  private async pollOnce(): Promise<void> {
    if (!this.blockingClient || !this.commandClient) return;

    // Calculate how many jobs we can fetch without exceeding concurrency
    const available = this.prefetch - this.activeCount;
    if (available <= 0) {
      // At capacity - wait for a slot to free up
      await this.waitForSlot();
      return;
    }

    let fetchCount = available;

    // Only check global concurrency if configured. Skipping this FCALL entirely
    // saves one Valkey round trip per poll cycle (~0.2ms).
    if (this.globalConcurrencyEnabled) {
      const gcRemaining = await checkConcurrency(this.commandClient, this.queueKeys, CONSUMER_GROUP);
      if (gcRemaining === 0) {
        await new Promise<void>((resolve) => setTimeout(resolve, 20));
        return;
      }
      if (gcRemaining > 0) {
        fetchCount = Math.min(available, gcRemaining);
      }
    }

    // Check priority list first (priority > LIFO > FIFO), then LIFO, before blocking on stream
    for (const [listKey, label] of [
      [this.queueKeys.priority, 'priority'],
      [this.queueKeys.lifo, 'LIFO'],
    ] as [string, string][]) {
      try {
        // With gc enabled, use atomic rpopAndReserve (single slot).
        // Without gc, batch pop up to fetchCount to reduce RTTs at concurrency > 1.
        const popCount = this.concurrency === 1 ? 1 : fetchCount;
        let jobIds: string[];
        if (this.globalConcurrencyEnabled) {
          const id = await rpopAndReserve(this.commandClient, this.queueKeys, listKey, CONSUMER_GROUP);
          jobIds = id ? [id] : [];
        } else if (popCount === 1) {
          const id = await this.commandClient.rpop(listKey);
          jobIds = id ? [String(id)] : [];
        } else {
          const ids = await this.commandClient.rpopCount(listKey, popCount);
          jobIds = ids ? ids.map(String) : [];
        }
        if (jobIds.length > 0) {
          // Always INCR list-active so complete/fail Lua DECRs stay balanced.
          // rpopAndReserve already did the INCR atomically; for non-gc paths do it here.
          if (!this.globalConcurrencyEnabled && jobIds.length > 0) {
            await this.commandClient.incrBy(this.queueKeys.listActive, jobIds.length);
          }
          for (const jobId of jobIds) {
            if (this.concurrency === 1) {
              this.activeCount++;
              const promise = this.processJob(jobId, '');
              this.activePromises.add(promise);
              try {
                await promise;
              } finally {
                this.activeCount--;
                this.activePromises.delete(promise);
              }
            } else {
              this.dispatchJob(jobId, '');
            }
          }
          return;
        }
      } catch (err) {
        this.emit('error', new Error(`${label} fetch error`, { cause: err }));
      }
    }

    // XREADGROUP GROUP {group} {consumerId} COUNT {fetchCount} BLOCK {blockTimeout}
    // STREAMS {streamKey} >
    const result = await this.blockingClient.xreadgroup(CONSUMER_GROUP, this.consumerId, this.xreadStreams, {
      count: fetchCount,
      block: this.blockTimeout,
    });

    if (!result) {
      // Stream empty - check priority and LIFO lists for jobs added while we were blocking
      if (this.commandClient) {
        for (const [listKey, label] of [
          [this.queueKeys.priority, 'priority'],
          [this.queueKeys.lifo, 'LIFO'],
        ] as [string, string][]) {
          try {
            const popCount = this.concurrency === 1 ? 1 : fetchCount;
            let jobIds: string[];
            if (this.globalConcurrencyEnabled) {
              const id = await rpopAndReserve(this.commandClient, this.queueKeys, listKey, CONSUMER_GROUP);
              jobIds = id ? [id] : [];
            } else if (popCount === 1) {
              const id = await this.commandClient.rpop(listKey);
              jobIds = id ? [String(id)] : [];
            } else {
              const ids = await this.commandClient.rpopCount(listKey, popCount);
              jobIds = ids ? ids.map(String) : [];
            }
            if (jobIds.length > 0) {
              if (!this.globalConcurrencyEnabled && jobIds.length > 0) {
                await this.commandClient.incrBy(this.queueKeys.listActive, jobIds.length);
              }
              for (const jobId of jobIds) {
                if (this.concurrency === 1) {
                  this.activeCount++;
                  const promise = this.processJob(jobId, '');
                  this.activePromises.add(promise);
                  try {
                    await promise;
                  } finally {
                    this.activeCount--;
                    this.activePromises.delete(promise);
                  }
                } else {
                  this.dispatchJob(jobId, '');
                }
              }
              return;
            }
          } catch (err) {
            this.emit('error', new Error(`${label} fetch error`, { cause: err }));
          }
        }
      }

      if (!this.isDrained && this.activeCount === 0) {
        this.isDrained = true;
        this.emit('drained');
      }
      return;
    }

    // Batch mode: collect entries and process as a batch
    if (this.batchMode) {
      await this.collectAndProcessBatch(result);
      return;
    }

    // result is GlideRecord<Record<string, [GlideString, GlideString][] | null>>
    // i.e. { key, value }[] where value is Record<entryId, fieldPairs | null>
    for (const streamEntry of result) {
      const entries = streamEntry.value;
      for (const entryId in entries) {
        if (!Object.prototype.hasOwnProperty.call(entries, entryId)) continue;
        const fieldPairs = entries[entryId];
        if (!fieldPairs) continue; // deleted entry

        // Parse the stream entry fields to extract jobId
        let jobId: string | null = null;
        for (let i = 0; i < fieldPairs.length; i++) {
          const field = fieldPairs[i][0];
          const value = fieldPairs[i][1];
          if (String(field) === 'jobId') {
            jobId = String(value);
            break;
          }
        }

        if (!jobId) continue;

        if (this.concurrency === 1) {
          // c=1 fast path: process inline (blocks poll loop).
          // Track in activePromises so close(false) can wait for it.
          this.activeCount++;
          const promise = this.processJob(jobId, String(entryId));
          this.activePromises.add(promise);
          try {
            await promise;
          } finally {
            this.activeCount--;
            this.activePromises.delete(promise);
          }
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

    const promise = this.processJob(jobId, entryId)
      .catch((err) => {
        // Force close can interrupt in-flight commands and reject these promises.
        // Consume rejections to avoid unhandled promise warnings during shutdown.
        if (!this.closing && this.running) {
          this.emit('error', err);
        }
      })
      .finally(() => {
        this.activeCount--;
        this.activePromises.delete(promise);
        this.internalEvents.emit('slotFree');
      });

    this.activePromises.add(promise);
  }

  // ---- Batch processing ----

  /**
   * Collect entries from XREADGROUP result and optionally wait for more
   * entries (if batch.timeout is set), then process the batch.
   */
  private async collectAndProcessBatch(
    initialResult: NonNullable<Awaited<ReturnType<Client['xreadgroup']>>>,
  ): Promise<void> {
    if (!this.commandClient || !this.blockingClient) return;

    // Collect {jobId, entryId} tuples from the initial XREADGROUP result
    const collected: { jobId: string; entryId: string }[] = [];

    for (const streamEntry of initialResult) {
      const entries = streamEntry.value;
      for (const entryId in entries) {
        if (!Object.prototype.hasOwnProperty.call(entries, entryId)) continue;
        const fieldPairs = entries[entryId];
        if (!fieldPairs) continue;

        let jobId: string | null = null;
        for (let i = 0; i < fieldPairs.length; i++) {
          if (String(fieldPairs[i][0]) === 'jobId') {
            jobId = String(fieldPairs[i][1]);
            break;
          }
        }
        if (jobId) {
          collected.push({ jobId, entryId: String(entryId) });
          if (collected.length >= this.batchSize) break;
        }
      }
      if (collected.length >= this.batchSize) break;
    }

    // If timeout is set and batch is not full, fetch more
    if (this.batchTimeout > 0 && collected.length < this.batchSize) {
      const deadline = Date.now() + this.batchTimeout;
      while (collected.length < this.batchSize && this.running && !this.closing) {
        const remaining = deadline - Date.now();
        if (remaining <= 0) break;

        const blockMs = Math.min(remaining, this.blockTimeout);
        const moreResult = await this.blockingClient.xreadgroup(CONSUMER_GROUP, this.consumerId, this.xreadStreams, {
          count: this.batchSize - collected.length,
          block: blockMs,
        });

        if (!moreResult) continue;
        for (const streamEntry of moreResult) {
          const entries = streamEntry.value;
          for (const entryId in entries) {
            if (!Object.prototype.hasOwnProperty.call(entries, entryId)) continue;
            const fieldPairs = entries[entryId];
            if (!fieldPairs) continue;
            let jobId: string | null = null;
            for (let i = 0; i < fieldPairs.length; i++) {
              if (String(fieldPairs[i][0]) === 'jobId') {
                jobId = String(fieldPairs[i][1]);
                break;
              }
            }
            if (jobId) collected.push({ jobId, entryId: String(entryId) });
          }
        }
      }
    }

    if (collected.length === 0) return;

    // Activate jobs and build batch
    const batch: { jobId: string; entryId: string; job: Job<D, R> }[] = [];
    for (const entry of collected) {
      if (!this.commandClient) break;
      const moveResult = await moveToActive(
        this.commandClient,
        this.queueKeys,
        entry.jobId,
        Date.now(),
        this.queueKeys.stream,
        entry.entryId,
        CONSUMER_GROUP,
      );
      if (await this.handleMoveToActiveEdgeCase(moveResult, entry.jobId, entry.entryId)) continue;
      const hash = moveResult as Record<string, string>;
      const job = Job.fromHash<D, R>(this.commandClient, this.queueKeys, entry.jobId, hash, this.serializer);
      job.entryId = entry.entryId;

      // Check ordering - if not ready, defer and skip
      const orderingReady = await this.isOrderingTurn(job);
      if (!orderingReady) {
        await this.deferOutOfOrderJob(entry.jobId, entry.entryId);
        continue;
      }

      this.startHeartbeat(entry.jobId);
      batch.push({ jobId: entry.jobId, entryId: entry.entryId, job });
    }

    if (batch.length === 0) return;

    // Process in the appropriate concurrency mode
    if (this.concurrency === 1) {
      // c=1: process inline (blocks poll loop)
      this.activeCount += batch.length;
      const promise = this.processBatch(batch);
      this.activePromises.add(promise);
      try {
        await promise;
      } finally {
        this.activeCount -= batch.length;
        this.activePromises.delete(promise);
        if (!this.isDrained && this.activeCount === 0) {
          this.isDrained = true;
          this.emit('drained');
        }
      }
    } else {
      this.dispatchBatch(batch);
    }
  }

  /**
   * Dispatch a batch for processing (c>1 mode).
   */
  private dispatchBatch(batch: { jobId: string; entryId: string; job: Job<D, R> }[]): void {
    this.activeCount += batch.length;

    const promise = this.processBatch(batch)
      .catch((err) => {
        if (!this.closing && this.running) {
          this.emit('error', err);
        }
      })
      .finally(() => {
        this.activeCount -= batch.length;
        this.activePromises.delete(promise);
        this.internalEvents.emit('slotFree');
        if (!this.isDrained && this.activeCount === 0) {
          this.isDrained = true;
          this.emit('drained');
        }
      });

    this.activePromises.add(promise);
  }

  /**
   * Process a batch of jobs through the batch processor.
   * Handles completion, failure, and partial failure (BatchError) for each job individually.
   */
  private async processBatch(batch: { jobId: string; entryId: string; job: Job<D, R> }[]): Promise<void> {
    if (!this.commandClient || !this.batchProcessor) return;

    // Emit 'active' for each job
    this.isDrained = false;
    for (const entry of batch) {
      this.emit('active', entry.job, entry.jobId);
    }

    // Rate limit check (once per batch)
    if (this.opts.limiter || this.globalRateLimitEnabled) await this.waitForRateLimit();

    // Set up abort controllers for all jobs
    const batchAc = new AbortController();
    for (const entry of batch) {
      this.activeAbortControllers.set(entry.jobId, batchAc);
      entry.job.abortSignal = batchAc.signal;
    }

    let results: R[] | undefined;
    let batchError: BatchError | undefined;
    let thrownError: Error | undefined;

    try {
      // Calculate batch timeout: max timeout across all jobs in the batch
      let maxTimeout = 0;
      for (const entry of batch) {
        const t = entry.job.opts.timeout;
        if (t && t > 0 && t > maxTimeout) maxTimeout = t;
      }

      const jobs = batch.map((e) => e.job);
      if (maxTimeout > 0) {
        let timer: ReturnType<typeof setTimeout> | undefined;
        try {
          results = await Promise.race([
            this.batchProcessor(jobs),
            new Promise<never>((_, reject) => {
              timer = setTimeout(() => {
                batchAc.abort();
                reject(new Error('Batch timeout exceeded'));
              }, maxTimeout);
            }),
          ]);
        } finally {
          if (timer !== undefined) clearTimeout(timer);
        }
      } else {
        results = await this.batchProcessor(jobs);
      }
    } catch (err) {
      if (err instanceof BatchError || (err instanceof Error && err.name === 'BatchError')) {
        batchError = err as BatchError;
      } else {
        thrownError = err instanceof Error ? err : new Error(String(err));
      }
    } finally {
      // Stop heartbeats and clean up abort controllers
      for (const entry of batch) {
        this.stopHeartbeat(entry.jobId);
        this.activeAbortControllers.delete(entry.jobId);
      }
    }

    if (!this.commandClient) return;

    if (results) {
      // Success path: validate results is array with correct length
      if (!Array.isArray(results) || results.length !== batch.length) {
        const len = Array.isArray(results) ? results.length : 'non-array';
        const err = new Error(`Batch processor returned ${len} results but batch had ${batch.length} jobs`);
        for (const entry of batch) {
          await this.handleJobFailure(entry.job, entry.jobId, entry.entryId, err);
        }
        return;
      }

      for (let i = 0; i < batch.length; i++) {
        const entry = batch[i];
        const result = results[i];

        let returnvalue: string;
        try {
          returnvalue = result !== undefined ? this.serializer.serialize(result) : 'null';
        } catch (serializeErr) {
          const err = serializeErr instanceof Error ? serializeErr : new Error(String(serializeErr));
          await this.handleJobFailure(
            entry.job,
            entry.jobId,
            entry.entryId,
            new Error(`Serializer failed on return value: ${err.message}`),
          );
          continue;
        }
        const byteLen = Buffer.byteLength(returnvalue, 'utf8');
        if (byteLen > MAX_JOB_DATA_SIZE) {
          await this.handleJobFailure(
            entry.job,
            entry.jobId,
            entry.entryId,
            new Error(`Return value exceeds maximum size (${byteLen} bytes > ${MAX_JOB_DATA_SIZE} bytes).`),
          );
          continue;
        }

        const parentInfo = await this.buildParentInfo(entry.job, entry.jobId);

        await completeJob(
          this.commandClient!,
          this.queueKeys,
          entry.jobId,
          entry.entryId,
          returnvalue,
          Date.now(),
          CONSUMER_GROUP,
          entry.job.opts.removeOnComplete,
          parentInfo,
        );

        entry.job.returnvalue = result;
        entry.job.finishedOn = Date.now();
        this.emit('completed', entry.job, result);
      }
    } else if (batchError) {
      // Partial failure: process each result individually
      const batchResults = batchError.results;
      for (let i = 0; i < batch.length; i++) {
        const entry = batch[i];
        const result = i < batchResults.length ? batchResults[i] : new Error('No result in BatchError');

        if (result instanceof Error) {
          await this.handleJobFailure(entry.job, entry.jobId, entry.entryId, result);
        } else {
          let returnvalue: string;
          try {
            returnvalue = result !== undefined ? this.serializer.serialize(result as R) : 'null';
          } catch (serializeErr) {
            const err = serializeErr instanceof Error ? serializeErr : new Error(String(serializeErr));
            await this.handleJobFailure(
              entry.job,
              entry.jobId,
              entry.entryId,
              new Error(`Serializer failed on return value: ${err.message}`),
            );
            continue;
          }
          const byteLen = Buffer.byteLength(returnvalue, 'utf8');
          if (byteLen > MAX_JOB_DATA_SIZE) {
            await this.handleJobFailure(
              entry.job,
              entry.jobId,
              entry.entryId,
              new Error(`Return value exceeds maximum size (${byteLen} bytes > ${MAX_JOB_DATA_SIZE} bytes).`),
            );
            continue;
          }

          const parentInfo = await this.buildParentInfo(entry.job, entry.jobId);

          await completeJob(
            this.commandClient!,
            this.queueKeys,
            entry.jobId,
            entry.entryId,
            returnvalue,
            Date.now(),
            CONSUMER_GROUP,
            entry.job.opts.removeOnComplete,
            parentInfo,
          );

          entry.job.returnvalue = result as R;
          entry.job.finishedOn = Date.now();
          this.emit('completed', entry.job, result);
        }
      }
    } else if (thrownError) {
      // All jobs fail
      const aborted = batchAc.signal.aborted;
      const err = aborted ? new Error('revoked') : thrownError;
      for (const entry of batch) {
        await this.handleJobFailure(entry.job, entry.jobId, entry.entryId, err);
      }
    }
  }

  // ---- Job processing helpers ----

  /**
   * Handle a moveToActive result that is not a valid hash (null or REVOKED).
   * Returns true if the result was handled (caller should return), false if the hash is valid.
   */
  private async handleMoveToActiveEdgeCase(
    moveResult:
      | Record<string, string>
      | 'REVOKED'
      | 'EXPIRED'
      | 'GROUP_FULL'
      | 'GROUP_RATE_LIMITED'
      | 'GROUP_TOKEN_LIMITED'
      | 'ERR:COST_EXCEEDS_CAPACITY'
      | null,
    jobId: string,
    entryId: string,
  ): Promise<boolean> {
    if (!this.commandClient) return true;
    if (moveResult === null) {
      try {
        await completeJob(this.commandClient, this.queueKeys, jobId, entryId, 'null', Date.now(), CONSUMER_GROUP);
      } catch (err) {
        this.emit('error', err);
      }
      return true;
    }
    if (moveResult === 'REVOKED') {
      try {
        await failJob(this.commandClient, this.queueKeys, jobId, entryId, 'revoked', Date.now(), 0, 0, CONSUMER_GROUP);
      } catch (err) {
        this.emit('error', err);
      }
      return true;
    }
    if (moveResult === 'EXPIRED') {
      // Already handled server-side by checkExpired in Lua
      return true;
    }
    if (
      moveResult === 'GROUP_FULL' ||
      moveResult === 'GROUP_RATE_LIMITED' ||
      moveResult === 'GROUP_TOKEN_LIMITED' ||
      moveResult === 'ERR:COST_EXCEEDS_CAPACITY'
    ) {
      return true;
    }
    return false;
  }

  /**
   * Run the processor with optional timeout, AbortController, and heartbeat.
   * Returns { result, error } - exactly one will be set.
   */
  private async runProcessor(job: Job<D, R>, jobId: string): Promise<{ result?: R; error?: Error; aborted: boolean }> {
    if (this.opts.limiter || this.globalRateLimitEnabled) await this.waitForRateLimit();

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
   * Returns true when the job reached a terminal failed state, false when it will retry.
   */
  private async handleJobFailure(job: Job<D, R>, jobId: string, entryId: string, error: Error): Promise<boolean> {
    if (!this.commandClient) {
      job.failedReason = error.message;
      this.emit('failed', job, error);
      return true;
    }

    if (error instanceof Worker.RateLimitError) {
      const delayMs = (error as any).delayMs || (this.opts.limiter?.duration ?? 1000);
      this.rateLimitUntil = Date.now() + delayMs;
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
      } catch (e) {
        this.emit('error', e);
      }
      return false;
    }

    const configuredAttempts = job.opts.attempts ?? 0;
    // .name fallback handles cross-realm errors or sandbox IPC where instanceof may fail
    const skipRetry = job.discarded || error instanceof UnrecoverableError || error.name === 'UnrecoverableError';
    const maxAttempts = skipRetry ? 0 : configuredAttempts;
    let backoffDelay = 0;
    if (maxAttempts > 0 && job.opts.backoff) {
      const strategyFn = this.opts.backoffStrategies?.[job.opts.backoff.type];
      backoffDelay = strategyFn
        ? strategyFn(job.attemptsMade + 1, error)
        : calculateBackoff(
            job.opts.backoff.type,
            job.opts.backoff.delay,
            job.attemptsMade + 1,
            job.opts.backoff.jitter,
          );
    }

    const failResult = await failJob(
      this.commandClient,
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

    if (failResult === 'failed' && this.opts.deadLetterQueue && this.commandClient) {
      await this.moveToDLQ(job, error);
    }
    job.failedReason = error.message;
    this.emit('failed', job, error);

    // Terminal failure: schedule next run for repeatAfterComplete schedulers
    if (failResult === 'failed' && job.schedulerName) {
      await this.updateSchedulerAfterComplete(job.schedulerName, Date.now());
    }

    return failResult === 'failed';
  }

  /**
   * Move an active job back into delayed state after the processor requests a pause.
   */
  private async handleMoveToDelayed(
    job: Job<D, R>,
    jobId: string,
    entryId: string,
    request: { delayedUntil: number; serializedData?: string; nextData?: D },
  ): Promise<void> {
    if (!this.commandClient) return;

    const result = await moveActiveToDelayed(
      this.commandClient,
      this.queueKeys,
      jobId,
      entryId,
      request.delayedUntil,
      request.serializedData,
      Date.now(),
      CONSUMER_GROUP,
    );
    if (result.startsWith('error:')) {
      const reason = result.slice(6);
      throw new Error(`Cannot move to delayed: ${reason}`);
    }

    if (request.nextData !== undefined) {
      job.data = request.nextData;
    }
    job.opts.delay = Math.max(0, request.delayedUntil - Date.now());
  }

  /**
   * After a repeatAfterComplete job completes or terminally fails,
   * update the scheduler entry so the next job is scheduled.
   *
   * KNOWN LIMITATIONS:
   * 1. Non-atomic: This update happens after the job completion transaction,
   *    so a worker crash between completion and this call will leave the scheduler
   *    stuck at nextRun=0 (awaiting completion sentinel) indefinitely.
   * 2. Non-worker failures: Jobs that reach terminal failure outside the worker
   *    path (e.g., revoked jobs, expired jobs in moveToActive, stalled terminal
   *    failures in glidemq_reclaimStalled) never trigger this update, leaving
   *    the scheduler permanently stuck.
   * 3. Race conditions: The idempotency check (nextRun === 0) prevents duplicate
   *    updates from stalled reclaim, but doesn't prevent races with concurrent
   *    upsertJobScheduler/removeJobScheduler (those use scheduler lock, this doesn't).
   *
   * MITIGATION: Run multiple workers for redundancy. Manually remove/re-add the
   * scheduler to recover from stuck state.
   *
   * FUTURE WORK: Move scheduler update into Lua completion/failure functions to
   * make it atomic and handle all terminal failure paths.
   */
  private async updateSchedulerAfterComplete(schedulerName: string, now: number): Promise<void> {
    if (!this.commandClient) return;
    try {
      const raw = await this.commandClient.hget(this.queueKeys.schedulers, schedulerName);
      if (raw == null) return; // scheduler was deleted while job was in flight

      let config: SchedulerEntry;
      try {
        config = JSON.parse(String(raw));
      } catch {
        return;
      }

      if (!config.repeatAfterComplete) return;

      // Idempotency: only update if nextRun is 0 (awaiting completion sentinel).
      // This prevents duplicate updates from stalled reclaim or double-processing.
      if (config.nextRun !== 0) return;

      const nextRun = computeFollowingSchedulerNextRun(config, now);
      if (nextRun == null || (config.limit != null && (config.iterationCount ?? 0) >= config.limit)) {
        await this.commandClient.hdel(this.queueKeys.schedulers, [schedulerName]);
      } else {
        config.nextRun = nextRun;
        // Don't overwrite lastRun - it was set by runSchedulers when the job was enqueued
        await this.commandClient.hset(this.queueKeys.schedulers, { [schedulerName]: JSON.stringify(config) });
      }
    } catch (err) {
      this.emit('error', err instanceof Error ? err : new Error(String(err)));
    }
  }

  /**
   * Build parent dependency info for complete/completeAndFetchNext calls.
   */
  private async buildParentInfo(
    job: Job<D, R>,
    jobId: string,
  ): Promise<{ depsMember: string; parentId: string; parentKeys: QueueKeys } | undefined> {
    let parentId = job.parentId;
    let parentQueue = job.parentQueue;

    if ((!parentId || !parentQueue) && this.commandClient) {
      const [refreshedParentId, refreshedParentQueue] = await this.commandClient.hmget(this.queueKeys.job(jobId), [
        'parentId',
        'parentQueue',
      ]);
      parentId = refreshedParentId ? String(refreshedParentId) : parentId;
      parentQueue = refreshedParentQueue ? String(refreshedParentQueue) : parentQueue;
    }

    if (!parentId || !parentQueue) return undefined;
    return {
      depsMember: `${keyPrefix(this.opts.prefix ?? 'glide', this.name)}:${jobId}`,
      parentId,
      parentKeys: buildKeys(parentQueue, this.opts.prefix),
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
        const moveResult = await moveToActive(
          this.commandClient,
          this.queueKeys,
          currentJobId,
          Date.now(),
          this.queueKeys.stream,
          currentEntryId,
          CONSUMER_GROUP,
        );
        if (await this.handleMoveToActiveEdgeCase(moveResult, currentJobId, currentEntryId)) return;
        currentHash = moveResult as Record<string, string>;
      }

      const job = Job.fromHash<D, R>(this.commandClient, this.queueKeys, currentJobId, currentHash, this.serializer);
      job.entryId = currentEntryId;

      const orderingReady = await this.isOrderingTurn(job);
      if (!orderingReady) {
        await this.deferOutOfOrderJob(currentJobId, currentEntryId);
        return;
      }
      const completionHints = {
        orderingKey: job.orderingKey,
        orderingSeq: job.orderingSeq,
        groupKey: job.groupKey,
      };

      this.isDrained = false;
      this.emit('active', job, currentJobId);

      const { result: processResult, error: processError, aborted } = await this.runProcessor(job, currentJobId);

      const delayedRequest = job.consumeMoveToDelayedRequest();
      const delayedError = processError instanceof DelayedError ? processError : undefined;
      if (delayedError) {
        try {
          await this.handleMoveToDelayed(job, currentJobId, currentEntryId, {
            delayedUntil: delayedRequest?.delayedUntil ?? delayedError.delayedUntil,
            serializedData: delayedRequest?.serializedData,
            nextData: delayedRequest?.nextData,
          });
        } catch (delayErr) {
          const err = delayErr instanceof Error ? delayErr : new Error(String(delayErr));
          await this.handleJobFailure(job, currentJobId, currentEntryId, err);
        }
        return;
      }

      const waitingChildrenRequest = job.consumeMoveToWaitingChildrenRequest();
      if (processError instanceof WaitingChildrenError || waitingChildrenRequest) {
        if (!this.commandClient) return;
        try {
          const wtcResult = await moveToWaitingChildren(
            this.commandClient,
            this.queueKeys,
            currentJobId,
            currentEntryId,
            CONSUMER_GROUP,
          );
          if (typeof wtcResult === 'string' && wtcResult.startsWith('error:')) {
            const reason = wtcResult.slice(6);
            throw new Error(`Cannot move to waiting-children: ${reason}`);
          }
        } catch (wtcErr) {
          const err = wtcErr instanceof Error ? wtcErr : new Error(String(wtcErr));
          await this.handleJobFailure(job, currentJobId, currentEntryId, err);
        }
        return;
      }

      if (processError || aborted) {
        await this.handleJobFailure(job, currentJobId, currentEntryId, aborted ? new Error('revoked') : processError!);
        return;
      }

      if (!this.commandClient) return;

      let returnvalue: string;
      try {
        returnvalue = processResult !== undefined ? this.serializer.serialize(processResult) : 'null';
      } catch (serializeErr) {
        const err = serializeErr instanceof Error ? serializeErr : new Error(String(serializeErr));
        await this.handleJobFailure(
          job,
          currentJobId,
          currentEntryId,
          new Error(`Serializer failed on return value: ${err.message}`),
        );
        return;
      }
      const byteLen = Buffer.byteLength(returnvalue, 'utf8');
      if (byteLen > MAX_JOB_DATA_SIZE) {
        await this.handleJobFailure(
          job,
          currentJobId,
          currentEntryId,
          new Error(
            `Return value exceeds maximum size (${byteLen} bytes > ${MAX_JOB_DATA_SIZE} bytes). Use smaller return values or store large data externally.`,
          ),
        );
        return;
      }
      const parentInfo = await this.buildParentInfo(job, currentJobId);

      const fetchResult = await completeAndFetchNext(
        this.commandClient,
        this.queueKeys,
        currentJobId,
        currentEntryId,
        returnvalue,
        Date.now(),
        CONSUMER_GROUP,
        this.consumerId,
        job.opts.removeOnComplete,
        parentInfo,
        completionHints,
      );

      job.returnvalue = processResult;
      job.finishedOn = Date.now();
      this.emit('completed', job, processResult);

      if (job.schedulerName) {
        await this.updateSchedulerAfterComplete(job.schedulerName, Date.now());
      }

      // No next job - return to poll loop
      if (fetchResult.next === false) {
        if (!this.isDrained && this.activeCount <= 1) {
          this.isDrained = true;
          this.emit('drained');
        }
        return;
      }

      if (fetchResult.next === 'REVOKED') {
        if (fetchResult.nextJobId && fetchResult.nextEntryId) {
          try {
            await failJob(
              this.commandClient,
              this.queueKeys,
              fetchResult.nextJobId,
              fetchResult.nextEntryId,
              'revoked',
              Date.now(),
              0,
              0,
              CONSUMER_GROUP,
            );
          } catch (err) {
            this.emit('error', err);
          }
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
      // DLQ envelope is always JSON. The data field is the already-deserialized
      // job.data embedded directly - this means non-JSON types (Date, Map, Set)
      // undergo lossy JSON conversion. BigInt will throw, caught by outer catch.
      // A future major version could change this to use the queue's serializer.
      const dlqData = JSON.stringify({
        originalQueue: this.name,
        originalJobId: job.id,
        data: job.data,
        failedReason: error.message,
        attemptsMade: job.attemptsMade,
      });
      await addJob(this.commandClient, dlqKeys, job.name, dlqData, JSON.stringify({}), Date.now(), 0, 0, '', 0);
    } catch (dlqErr) {
      this.emit('error', dlqErr);
    }
  }

  /**
   * Check the server-side rate limiter and wait if the limit is exceeded.
   * Also respects any manual rate limit set via rateLimit(ms).
   */
  private async waitForRateLimit(): Promise<void> {
    if (!this.commandClient) return;

    // First, respect any manual rate limit
    const now = Date.now();
    if (this.rateLimitUntil > now) {
      await new Promise<void>((resolve) => setTimeout(resolve, this.rateLimitUntil - now));
    }

    // Determine effective rate limit config.
    // Valkey-stored (dynamic) config takes precedence over local WorkerOptions.
    // Values are cached from meta by refreshMetaFlags (runs each scheduler tick).
    let max: number;
    let duration: number;

    if (this.globalRateLimitEnabled && this.cachedRateLimitMax > 0) {
      max = this.cachedRateLimitMax;
      duration = this.cachedRateLimitDuration;
    } else if (this.opts.limiter) {
      max = this.opts.limiter.max;
      duration = this.opts.limiter.duration;
    } else {
      return;
    }

    // Server-side sliding window check
    while (true) {
      const delayMs = await rateLimitFn(this.commandClient, this.queueKeys, max, duration, Date.now());

      if (delayMs <= 0) break;

      // Wait for the delay, then re-check
      await new Promise<void>((resolve) => setTimeout(resolve, delayMs));
    }
  }

  /** Refresh cached meta flags from Valkey. Called on init and each scheduler tick. */
  private async refreshMetaFlags(): Promise<void> {
    if (!this.commandClient) return;
    try {
      // Read only the 3 specific fields we need - avoids O(N) on orderdone:* fields
      const vals = await this.commandClient.hmget(this.queueKeys.meta, [
        'globalConcurrency',
        'rateLimitMax',
        'rateLimitDuration',
      ]);
      const gcVal = vals?.[0] != null ? String(vals[0]) : null;
      const rlMax = vals?.[1] != null ? String(vals[1]) : null;
      const rlDur = vals?.[2] != null ? String(vals[2]) : null;
      this.globalConcurrencyEnabled = gcVal != null && Number(gcVal) > 0;
      this.globalRateLimitEnabled = rlMax != null && Number(rlMax) > 0;
      this.cachedRateLimitMax = Number(rlMax) || 0;
      this.cachedRateLimitDuration = Number(rlDur) || 0;
    } catch {
      // Transient error - next tick will retry
    }
  }

  /**
   * Register this worker in Valkey with a TTL-based heartbeat key.
   * The key expires after stalledInterval ms; a periodic timer refreshes it at half that interval.
   * Registration failure is non-fatal - the worker can still process jobs.
   */
  private async registerWorker(): Promise<void> {
    if (!this.commandClient) return;
    try {
      const payload = JSON.stringify({
        addr: this.hostname,
        pid: process.pid,
        startedAt: this.startedAt,
        activeJobs: this.activeCount,
      });
      const workerKey = this.queueKeys.worker(this.consumerId);
      await this.commandClient.set(workerKey, payload, {
        expiry: { type: TimeUnit.Milliseconds, count: this.stalledInterval },
      });
    } catch {
      // Non-fatal: next heartbeat tick will retry
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
      if (!force) {
        await this.scheduler.waitForIdle();
      }
      this.scheduler = null;
    }

    if (!force) {
      await this.waitForActiveJobs();
    }

    // Shut down sandbox worker pool
    if (this.sandboxClose) {
      await this.sandboxClose(force);
    }

    // Clear worker registration heartbeat
    if (this.workerHeartbeatTimer) {
      clearInterval(this.workerHeartbeatTimer);
      this.workerHeartbeatTimer = null;
    }
    // Best-effort deregistration
    if (this.commandClient) {
      try {
        await this.commandClient.del([this.queueKeys.worker(this.consumerId)]);
      } catch {
        // Ignore - TTL will clean up
      }
    }

    // Clear all active heartbeats
    for (const [, timer] of this.heartbeatIntervals) {
      clearInterval(timer);
    }
    this.heartbeatIntervals.clear();

    if (this.blockingClient) {
      this.blockingClient.close();
      this.blockingClient = null;
    }
    void this.pollLoopPromise?.catch(() => {});
    this.pollLoopPromise = null;

    if (this.commandClient) {
      const commandClient = this.commandClient;
      this.commandClient = null;
      if (this.commandClientOwned) {
        commandClient.close();
      }
    }

    this.closed = true;
    this.internalEvents.removeAllListeners();
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
