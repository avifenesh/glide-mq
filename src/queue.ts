import { EventEmitter } from 'events';
import { InfBoundary } from 'speedkey';
import type { GlideClient, GlideClusterClient } from 'speedkey';
import type { QueueOptions, JobOptions, Client, ScheduleOpts, JobTemplate, SchedulerEntry, Metrics, JobCounts } from './types';
import { Job } from './job';
import { buildKeys, keyPrefix, nextCronOccurrence } from './utils';
import { createClient, ensureFunctionLibrary } from './connection';
import { addJob, dedup, pause, resume, removeJob, revokeJob, CONSUMER_GROUP } from './functions/index';
import type { QueueKeys } from './functions/index';
import { LIBRARY_SOURCE } from './functions/index';
import { withSpan } from './telemetry';

export class Queue<D = any, R = any> extends EventEmitter {
  readonly name: string;
  private opts: QueueOptions;
  private client: Client | null = null;
  private closing = false;
  private keys: QueueKeys;

  constructor(name: string, opts: QueueOptions) {
    super();
    this.name = name;
    this.opts = opts;
    this.keys = buildKeys(name, opts.prefix);
  }

  /** @internal */
  async getClient(): Promise<Client> {
    if (this.closing) {
      throw new Error('Queue is closing');
    }
    if (!this.client) {
      let client: Client;
      try {
        client = await createClient(this.opts.connection);
        await ensureFunctionLibrary(
          client,
          LIBRARY_SOURCE,
          this.opts.connection.clusterMode ?? false,
        );
      } catch (err) {
        // Don't cache a failed client - next getClient() call will retry
        this.emit('error', err);
        throw err;
      }
      this.client = client;
    }
    return this.client;
  }

  /**
   * Add a single job to the queue.
   * Uses the glidemq_addJob server function to atomically create the job hash
   * and enqueue it to the stream (or scheduled ZSet if delayed/prioritized).
   */
  async add(name: string, data: D, opts?: JobOptions): Promise<Job<D, R> | null> {
    const delay = opts?.delay ?? 0;
    const priority = opts?.priority ?? 0;

    return withSpan(
      'glide-mq.queue.add',
      {
        'glide-mq.queue': this.name,
        'glide-mq.job.name': name,
        'glide-mq.job.delay': delay,
        'glide-mq.job.priority': priority,
      },
      async (span) => {
        const client = await this.getClient();
        const timestamp = Date.now();
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

        span.setAttribute('glide-mq.job.id', String(jobId));

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
      },
    );
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
   * Revoke a job by ID.
   * If the job is waiting/delayed, it is immediately moved to the failed set with reason 'revoked'.
   * If the job is currently being processed, a revoked flag is set on the hash -
   * the worker will check this flag cooperatively and fire the AbortSignal.
   * Returns 'revoked', 'flagged', or 'not_found'.
   */
  async revoke(jobId: string): Promise<string> {
    const client = await this.getClient();
    return revokeJob(client, this.keys, jobId, Date.now());
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
   * Upsert a job scheduler (repeatable/cron job).
   * Stores the scheduler config in the schedulers hash.
   * Computes the initial nextRun based on the schedule.
   */
  async upsertJobScheduler(
    name: string,
    schedule: ScheduleOpts,
    template?: JobTemplate,
  ): Promise<void> {
    const client = await this.getClient();
    const now = Date.now();

    let nextRun: number;
    if (schedule.pattern) {
      nextRun = nextCronOccurrence(schedule.pattern, now);
    } else if (schedule.every) {
      nextRun = now + schedule.every;
    } else {
      throw new Error('Schedule must have either pattern (cron) or every (ms interval)');
    }

    const entry: SchedulerEntry = {
      pattern: schedule.pattern,
      every: schedule.every,
      template,
      nextRun,
    };

    await client.hset(this.keys.schedulers, { [name]: JSON.stringify(entry) });
  }

  /**
   * Remove a job scheduler by name.
   */
  async removeJobScheduler(name: string): Promise<void> {
    const client = await this.getClient();
    await client.hdel(this.keys.schedulers, [name]);
  }

  /**
   * Get metrics for completed or failed jobs.
   * Returns the count of entries in the corresponding ZSet.
   */
  async getMetrics(type: 'completed' | 'failed'): Promise<Metrics> {
    const client = await this.getClient();
    const key = type === 'completed' ? this.keys.completed : this.keys.failed;
    const count = await client.zcard(key);
    return { count };
  }

  /**
   * Get job counts by state.
   * - waiting: stream length minus active (pending) entries
   * - active: PEL count from XPENDING
   * - delayed: scheduled ZSet cardinality (includes both delayed and prioritized)
   * - completed: completed ZSet cardinality
   * - failed: failed ZSet cardinality
   */
  async getJobCounts(): Promise<JobCounts> {
    const client = await this.getClient();

    const [streamLen, completedCount, failedCount, scheduledCount] = await Promise.all([
      client.xlen(this.keys.stream),
      client.zcard(this.keys.completed),
      client.zcard(this.keys.failed),
      client.zcard(this.keys.scheduled),
    ]);

    // XPENDING returns [pendingCount, minId, maxId, consumers[]]
    let activeCount = 0;
    try {
      const pendingInfo = await client.xpending(this.keys.stream, CONSUMER_GROUP);
      activeCount = Number(pendingInfo[0]) || 0;
    } catch {
      // Consumer group may not exist yet
    }

    const waiting = Math.max(0, streamLen - activeCount);

    return {
      waiting,
      active: activeCount,
      delayed: scheduledCount,
      completed: completedCount,
      failed: failedCount,
    };
  }

  /**
   * Remove all data associated with this queue from the server.
   * If force=false (default), fails if there are active jobs.
   * If force=true, deletes everything regardless of active jobs.
   */
  async obliterate(opts?: { force?: boolean }): Promise<void> {
    const client = await this.getClient();
    const force = opts?.force ?? false;

    // Check for active jobs if not forcing
    if (!force) {
      try {
        const pendingInfo = await client.xpending(this.keys.stream, CONSUMER_GROUP);
        const activeCount = Number(pendingInfo[0]) || 0;
        if (activeCount > 0) {
          throw new Error(`Cannot obliterate queue "${this.name}": ${activeCount} active jobs. Use { force: true } to override.`);
        }
      } catch (err) {
        // If the error is our own active-jobs check, re-throw
        if (err instanceof Error && err.message.includes('Cannot obliterate')) {
          throw err;
        }
        // Consumer group doesn't exist yet means no active jobs - continue
      }
    }

    // Delete all known static keys
    const staticKeys = [
      this.keys.id, this.keys.stream, this.keys.scheduled,
      this.keys.completed, this.keys.failed, this.keys.events,
      this.keys.meta, this.keys.dedup, this.keys.rate, this.keys.schedulers,
    ];
    await client.del(staticKeys);

    // Scan and delete job hashes and deps sets
    const prefix = keyPrefix(this.opts.prefix ?? 'glide', this.name);
    const jobPattern = `${prefix}:job:*`;
    const logPattern = `${prefix}:log:*`;
    const depsPattern = `${prefix}:deps:*`;

    for (const pattern of [jobPattern, logPattern, depsPattern]) {
      await this.scanAndDelete(client, pattern);
    }
  }

  /**
   * Scan for keys matching a pattern and delete them in batches.
   * Handles both standalone (GlideClient) and cluster (GlideClusterClient) scan APIs.
   * @internal
   */
  private async scanAndDelete(client: Client, pattern: string): Promise<void> {
    if (this.opts.connection.clusterMode) {
      const { ClusterScanCursor } = await import('speedkey');
      const clusterClient = client as GlideClusterClient;
      let cursor = new ClusterScanCursor();
      while (!cursor.isFinished()) {
        const [nextCursor, keys] = await clusterClient.scan(cursor, { match: pattern, count: 100 });
        cursor = nextCursor;
        if (keys.length > 0) {
          await client.del(keys);
        }
      }
    } else {
      let cursor = '0';
      do {
        const result = await (client as GlideClient).scan(cursor, { match: pattern, count: 100 });
        cursor = result[0] as string;
        const keys = result[1];
        if (keys.length > 0) {
          await client.del(keys);
        }
      } while (cursor !== '0');
    }
  }

  /**
   * Retrieve jobs by state with optional pagination.
   * @param type - The job state to query
   * @param start - Start index for pagination (default 0)
   * @param end - End index for pagination (default -1, meaning all)
   */
  async getJobs(
    type: 'waiting' | 'active' | 'delayed' | 'completed' | 'failed',
    start = 0,
    end = -1,
  ): Promise<Job<D, R>[]> {
    const client = await this.getClient();
    let jobIds: string[];

    switch (type) {
      case 'waiting': {
        // XRANGE on the stream to get waiting entries, extract jobId fields
        const entries = await client.xrange(
          this.keys.stream,
          InfBoundary.NegativeInfinity,
          InfBoundary.PositiveInfinity,
          end >= 0 ? { count: end + 1 } : undefined,
        );
        if (!entries) return [];
        const allIds: string[] = [];
        for (const fieldPairs of Object.values(entries)) {
          for (const [field, value] of fieldPairs) {
            if (String(field) === 'jobId') {
              allIds.push(String(value));
            }
          }
        }
        jobIds = allIds.slice(start, end >= 0 ? end + 1 : undefined);
        break;
      }
      case 'active': {
        // XPENDING extended form to get active job entry IDs, then read jobId from entries
        try {
          const pendingEntries = await client.xpendingWithOptions(
            this.keys.stream,
            CONSUMER_GROUP,
            {
              start: InfBoundary.NegativeInfinity,
              end: InfBoundary.PositiveInfinity,
              count: end >= 0 ? end + 1 : 10000,
            },
          );
          const entryIds = pendingEntries.slice(start, end >= 0 ? end + 1 : undefined).map(e => String(e[0]));
          // Read each entry from the stream to get the jobId
          jobIds = [];
          for (const entryId of entryIds) {
            const entryData = await client.xrange(
              this.keys.stream,
              { value: entryId },
              { value: entryId },
              { count: 1 },
            );
            if (entryData) {
              for (const fieldPairs of Object.values(entryData)) {
                for (const [field, value] of fieldPairs) {
                  if (String(field) === 'jobId') {
                    jobIds.push(String(value));
                  }
                }
              }
            }
          }
        } catch {
          // Consumer group may not exist
          jobIds = [];
        }
        break;
      }
      case 'delayed': {
        const members = await client.zrange(
          this.keys.scheduled,
          { start: start, end: end >= 0 ? end : -1 },
        );
        jobIds = members.map(m => String(m));
        break;
      }
      case 'completed': {
        const members = await client.zrange(
          this.keys.completed,
          { start: start, end: end >= 0 ? end : -1 },
        );
        jobIds = members.map(m => String(m));
        break;
      }
      case 'failed': {
        const members = await client.zrange(
          this.keys.failed,
          { start: start, end: end >= 0 ? end : -1 },
        );
        jobIds = members.map(m => String(m));
        break;
      }
    }

    // Fetch job hashes for each ID
    const jobs: Job<D, R>[] = [];
    for (const id of jobIds) {
      const job = await this.getJob(id);
      if (job) jobs.push(job);
    }
    return jobs;
  }

  /**
   * Get job counts by types. Alias for getJobCounts().
   */
  async getJobCountByTypes(): Promise<JobCounts> {
    return this.getJobCounts();
  }

  /**
   * Check if the queue is paused.
   */
  async isPaused(): Promise<boolean> {
    const client = await this.getClient();
    const val = await client.hget(this.keys.meta, 'paused');
    return val === '1';
  }

  /**
   * Get the count of waiting jobs (stream length).
   */
  async count(): Promise<number> {
    const client = await this.getClient();
    return client.xlen(this.keys.stream);
  }

  /**
   * Get all registered job schedulers (repeatable jobs).
   */
  async getRepeatableJobs(): Promise<{ name: string; entry: SchedulerEntry }[]> {
    const client = await this.getClient();
    const hashData = await client.hgetall(this.keys.schedulers);
    if (!hashData || hashData.length === 0) return [];
    const result: { name: string; entry: SchedulerEntry }[] = [];
    for (const item of hashData) {
      result.push({
        name: String(item.field),
        entry: JSON.parse(String(item.value)),
      });
    }
    return result;
  }

  /**
   * Retrieve log entries for a job by ID.
   */
  async getJobLogs(id: string, start = 0, end = -1): Promise<{ logs: string[]; count: number }> {
    const client = await this.getClient();
    const logs = await client.lrange(this.keys.log(id), start, end);
    const count = await client.llen(this.keys.log(id));
    return {
      logs: logs.map((l) => String(l)),
      count,
    };
  }

  /**
   * Retrieve jobs from the dead letter queue configured for this queue.
   * Returns an empty array if no DLQ is configured.
   * @param start - Start index (default 0)
   * @param end - End index (default -1, meaning all)
   */
  async getDeadLetterJobs(start = 0, end = -1): Promise<Job<D, R>[]> {
    if (!this.opts.deadLetterQueue) return [];
    const client = await this.getClient();
    const dlqKeys = buildKeys(this.opts.deadLetterQueue.name, this.opts.prefix);

    // DLQ jobs are added via addJob, so they appear in the stream.
    // Read all entries from the DLQ stream.
    const { InfBoundary } = await import('speedkey');
    const entries = await client.xrange(
      dlqKeys.stream,
      InfBoundary.NegativeInfinity,
      InfBoundary.PositiveInfinity,
      end >= 0 ? { count: end + 1 } : undefined,
    );
    if (!entries) return [];

    const jobIds: string[] = [];
    for (const fieldPairs of Object.values(entries)) {
      for (const [field, value] of fieldPairs) {
        if (String(field) === 'jobId') {
          jobIds.push(String(value));
        }
      }
    }

    const sliced = jobIds.slice(start, end >= 0 ? end + 1 : undefined);
    const jobs: Job<D, R>[] = [];
    for (const id of sliced) {
      const hashData = await client.hgetall(dlqKeys.job(id));
      if (!hashData || hashData.length === 0) continue;
      const hash: Record<string, string> = {};
      for (const entry of hashData) {
        hash[String(entry.field)] = String(entry.value);
      }
      jobs.push(Job.fromHash<D, R>(client, dlqKeys, id, hash));
    }
    return jobs;
  }

  /**
   * Close the queue and release the underlying client connection.
   * Idempotent: safe to call multiple times.
   */
  async close(): Promise<void> {
    if (this.closing) return;
    this.closing = true;
    if (this.client) {
      this.client.close();
      this.client = null;
    }
  }
}
