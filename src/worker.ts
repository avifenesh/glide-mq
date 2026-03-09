import type { WorkerOptions, Processor, BatchProcessor, Client } from './types';
import { CONSUMER_GROUP, checkConcurrency, rpopAndReserve, popLists } from './functions/index';
import { BaseWorker } from './base-worker';
export type { WorkerEvent } from './base-worker';

export class Worker<D = any, R = any> extends BaseWorker<D, R> {
  constructor(name: string, processor: Processor<D, R> | BatchProcessor<D, R> | string, opts: WorkerOptions) {
    super(name, processor, opts, {
      consumerGroup: CONSUMER_GROUP,
      broadcastMode: false,
      startFrom: '0',
    });
  }

  protected async pollOnce(): Promise<void> {
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
    if (await this.tryPopFromLists(fetchCount)) return;

    // XREADGROUP GROUP {group} {consumerId} COUNT {fetchCount} BLOCK {blockTimeout}
    // STREAMS {streamKey} >
    const result = await this.blockingClient.xreadgroup(CONSUMER_GROUP, this.consumerId, this.xreadStreams, {
      count: fetchCount,
      block: this.blockTimeout,
    });

    if (!result) {
      // Stream empty - check priority and LIFO lists for jobs added while we were blocking
      if (this.commandClient) {
        if (await this.tryPopFromLists(fetchCount)) return;
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
   * Try to pop jobs from priority and LIFO lists.
   * Returns true if any jobs were popped and dispatched, false otherwise.
   */
  private async tryPopFromLists(fetchCount: number): Promise<boolean> {
    if (!this.commandClient) return false;

    try {
      const popCount = this.concurrency === 1 ? 1 : fetchCount;
      let jobIds: string[];

      if (this.globalConcurrencyEnabled) {
        // With gc, must use atomic rpopAndReserve per list (preserves slot accounting)
        jobIds = [];
        for (const listKey of [this.queueKeys.priority, this.queueKeys.lifo]) {
          try {
            const id = await rpopAndReserve(this.commandClient, this.queueKeys, listKey, CONSUMER_GROUP);
            if (id) {
              jobIds.push(id);
              break;
            }
          } catch (err) {
            const listType = listKey === this.queueKeys.priority ? 'priority' : 'LIFO';
            this.emit('error', new Error(`${listType} fetch error`, { cause: err }));
          }
        }
      } else {
        // Single FCALL checks both priority and LIFO lists (1 RTT instead of 2)
        jobIds = await popLists(this.commandClient, this.queueKeys, popCount);
      }

      if (jobIds.length > 0) {
        // INCR list-active so complete/fail Lua DECRs stay balanced.
        // rpopAndReserve already did the INCR atomically; for non-gc paths do it here.
        if (!this.globalConcurrencyEnabled) {
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
        return true;
      }
    } catch (err) {
      this.emit('error', new Error(`popLists fetch error`, { cause: err }));
    }
    return false;
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

    await this.activateAndProcessBatch(collected);
  }

  /** Backward-compatible static RateLimitError from BaseWorker. */
  static RateLimitError = BaseWorker.RateLimitError;
}
