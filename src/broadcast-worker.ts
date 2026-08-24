import type { BroadcastWorkerOptions, Processor, BatchProcessor, Client } from './types';
import { Job } from './job';
import { GlideMQError } from './errors';
import { checkConcurrency } from './functions/index';
import { BaseWorker } from './base-worker';
import { compileSubjectMatcher } from './utils';
export type { WorkerEvent } from './base-worker';

export class BroadcastWorker<D = any, R = any> extends BaseWorker<D, R> {
  private subjectMatcher: ((subject: string) => boolean) | null;

  constructor(name: string, processor: Processor<D, R> | BatchProcessor<D, R> | string, opts: BroadcastWorkerOptions) {
    // Validate subscription field (required for BroadcastWorker)
    if (!opts.subscription || typeof opts.subscription !== 'string' || opts.subscription.trim() === '') {
      throw new GlideMQError('BroadcastWorker requires a `subscription` name (consumer group name).');
    }

    super(name, processor, opts, {
      consumerGroup: opts.subscription,
      broadcastMode: true,
      startFrom: opts.startFrom ?? '$',
    });

    this.subjectMatcher = compileSubjectMatcher(opts.subjects);
  }

  protected async pollOnce(): Promise<void> {
    if (!this.blockingClient || !this.commandClient) return;
    if (this.paused || this.closing) return;
    if (await this.waitIfQueuePaused()) return;
    if (this.paused || this.closing) return;

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
      const gcRemaining = await checkConcurrency(this.commandClient, this.queueKeys, this.consumerGroup);
      if (gcRemaining === 0) {
        await new Promise<void>((resolve) => setTimeout(resolve, 20));
        return;
      }
      if (gcRemaining > 0) {
        fetchCount = Math.min(available, gcRemaining);
      }
    }

    const parkedResult = await this.recoverPausedBroadcastEntries(fetchCount);
    if (parkedResult) {
      await this.processReadResult(parkedResult);
      return;
    }

    // XREADGROUP GROUP {group} {consumerId} COUNT {fetchCount} BLOCK {blockTimeout}
    // STREAMS {streamKey} >
    const result = await this.blockingClient.xreadgroup(this.consumerGroup, this.consumerId, this.xreadStreams, {
      count: fetchCount,
      block: this.blockTimeout,
    });

    if (!result) {
      if (!this.isDrained && this.activeCount === 0) {
        this.isDrained = true;
        this.emit('drained');
      }
      return;
    }

    await this.processReadResult(result);
  }

  /**
   * Recover only broadcast entries parked by this worker during a queue-pause
   * activation race. XREADGROUP with `0` would redeliver all of this
   * consumer's PEL, including live processing, so target known IDs with
   * XCLAIM instead.
   */
  private async recoverPausedBroadcastEntries(
    count: number,
  ): Promise<NonNullable<Awaited<ReturnType<Client['xreadgroup']>>> | null> {
    if (!this.commandClient || this.pausedBroadcastEntries.size === 0) return null;

    const entryIds = [...this.pausedBroadcastEntries].slice(0, count);
    try {
      const entries = await this.commandClient.xclaim(
        this.queueKeys.stream,
        this.consumerGroup,
        this.consumerId,
        0,
        entryIds,
      );
      for (const entryId of entryIds) this.pausedBroadcastEntries.delete(entryId);
      if (Object.keys(entries).length === 0) return null;
      return [{ key: this.queueKeys.stream, value: entries }] as NonNullable<Awaited<ReturnType<Client['xreadgroup']>>>;
    } catch (err) {
      this.emit('error', err);
      return null;
    }
  }

  private async processReadResult(result: NonNullable<Awaited<ReturnType<Client['xreadgroup']>>>): Promise<void> {
    // Batch mode: collect entries and process as a batch
    if (this.batchMode) {
      await this.collectAndProcessBatch(result);
      return;
    }

    // result is GlideRecord<Record<string, [GlideString, GlideString][] | null>>
    // i.e. { key, value }[] where value is Record<entryId, fieldPairs | null>
    for (const streamEntry of result) {
      const entries = streamEntry.value;
      for (const [entryId, fieldPairs] of Object.entries(entries)) {
        if (!fieldPairs) continue; // deleted entry

        // Parse the stream entry fields to extract jobId and name
        let jobId: string | null = null;
        let jobName: string | null = null;
        let retryGroup: string | null = null;
        for (let i = 0; i < fieldPairs.length; i++) {
          const field = String(fieldPairs[i][0]);
          const value = String(fieldPairs[i][1]);
          if (field === 'jobId') jobId = value;
          else if (field === 'name') jobName = value;
          else if (field === 'retryGroup') retryGroup = value;
        }

        if (!jobId) continue;

        if (retryGroup && retryGroup !== this.consumerGroup) {
          await this.commandClient!.xack(this.queueKeys.stream, this.consumerGroup, [entryId]);
          continue;
        }

        // Subject filtering: auto-ACK and skip non-matching messages
        if (this.subjectMatcher && (!jobName || !this.subjectMatcher(jobName))) {
          await this.commandClient!.xack(this.queueKeys.stream, this.consumerGroup, [entryId]);
          continue;
        }

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
      for (const [entryId, fieldPairs] of Object.entries(entries)) {
        if (!fieldPairs) continue;

        let jobId: string | null = null;
        let jobName: string | null = null;
        let retryGroup: string | null = null;
        for (let i = 0; i < fieldPairs.length; i++) {
          const field = String(fieldPairs[i][0]);
          const value = String(fieldPairs[i][1]);
          if (field === 'jobId') jobId = value;
          else if (field === 'name') jobName = value;
          else if (field === 'retryGroup') retryGroup = value;
        }
        if (!jobId) continue;
        if (retryGroup && retryGroup !== this.consumerGroup) {
          await this.commandClient!.xack(this.queueKeys.stream, this.consumerGroup, [entryId]);
          continue;
        }
        // Subject filtering: auto-ACK and skip non-matching messages
        if (this.subjectMatcher && (!jobName || !this.subjectMatcher(jobName))) {
          await this.commandClient!.xack(this.queueKeys.stream, this.consumerGroup, [entryId]);
          continue;
        }
        collected.push({ jobId, entryId: String(entryId) });
        if (collected.length >= this.batchSize) break;
      }
      if (collected.length >= this.batchSize) break;
    }

    // If timeout is set and batch is not full, fetch more
    if (this.batchTimeout > 0 && collected.length < this.batchSize) {
      const deadline = Date.now() + this.batchTimeout;
      while (collected.length < this.batchSize && this.running && !this.closing) {
        const remaining = deadline - Date.now();
        if (remaining <= 0) break;

        // Queue.pause() can race with the initial read while a batch waits for
        // its timeout. Refresh before every refill so a paused queue does not
        // claim another entry during that secondary read.
        await this.refreshMetaFlags();
        if (this.queuePaused) break;

        const blockMs = Math.min(remaining, this.blockTimeout);
        const moreResult = await this.blockingClient.xreadgroup(
          this.consumerGroup,
          this.consumerId,
          this.xreadStreams,
          {
            count: this.batchSize - collected.length,
            block: blockMs,
          },
        );

        if (!moreResult) continue;
        for (const streamEntry of moreResult) {
          const entries = streamEntry.value;
          for (const [entryId, fieldPairs] of Object.entries(entries)) {
            if (!fieldPairs) continue;
            let jobId: string | null = null;
            let jobName: string | null = null;
            let retryGroup: string | null = null;
            for (let i = 0; i < fieldPairs.length; i++) {
              const field = String(fieldPairs[i][0]);
              const value = String(fieldPairs[i][1]);
              if (field === 'jobId') jobId = value;
              else if (field === 'name') jobName = value;
              else if (field === 'retryGroup') retryGroup = value;
            }
            if (!jobId) continue;
            if (retryGroup && retryGroup !== this.consumerGroup) {
              await this.commandClient!.xack(this.queueKeys.stream, this.consumerGroup, [entryId]);
              continue;
            }
            if (this.subjectMatcher && (!jobName || !this.subjectMatcher(jobName))) {
              await this.commandClient!.xack(this.queueKeys.stream, this.consumerGroup, [entryId]);
              continue;
            }
            collected.push({ jobId, entryId: String(entryId) });
          }
        }
      }
    }

    await this.activateAndProcessBatch(collected);
  }

  /**
   * Resolve the number of attempts made for a job.
   * In broadcast mode, attemptsMade is tracked per-subscription in :sub: hash.
   */
  protected async getAttemptsMade(job: Job<D, R>, jobId: string): Promise<number> {
    if (!this.commandClient) return job.attemptsMade;
    const subAttemptStr = await this.commandClient.hget(`${this.queueKeys.job(jobId)}:sub:${this.consumerGroup}`, 'a');
    return subAttemptStr !== null ? Number(subAttemptStr) : job.attemptsMade;
  }

  /**
   * In broadcast mode, stream entries are never XDEL'd (intentional retention for fan-out).
   * Only wait for active processing to finish and the scheduled set to empty.
   */
  protected async isDrainComplete(): Promise<boolean> {
    if (!this.commandClient) return true;
    const scheduledLen = await this.commandClient.zcard(this.queueKeys.scheduled);
    return scheduledLen === 0 && this.activeCount === 0;
  }

  /** Backward-compatible static RateLimitError from BaseWorker. */
  static RateLimitError = BaseWorker.RateLimitError;
}
