/**
 * In-memory testing mode for glide-mq.
 * TestQueue and TestWorker mimic the real API using plain Maps - no Valkey needed.
 *
 * Usage:
 *   import { TestQueue, TestWorker } from 'glide-mq/testing';
 *
 * LIMITATION: repeatAfterComplete schedulers are accepted but behave like `every` schedulers
 * in testing mode (fire at regular intervals, not based on job completion). This is because
 * the in-memory implementation doesn't track job-to-scheduler linkage. For full repeatAfterComplete
 * behavior, use the real Queue/Worker with a Valkey instance.
 */

import { EventEmitter } from 'events';
import path from 'path';
import os from 'os';
import type {
  JobOptions,
  JobCounts,
  Metrics,
  MetricsOptions,
  MetricsDataPoint,
  GetJobsOptions,
  Processor,
  WorkerInfo,
  SchedulerEntry,
  ScheduleOpts,
  JobTemplate,
  Serializer,
} from './types';
import { JSON_SERIALIZER } from './types';
import { GlideMQError, UnrecoverableError, BatchError } from './errors';
import {
  MAX_JOB_DATA_SIZE,
  computeFollowingSchedulerNextRun,
  computeInitialSchedulerNextRun,
  normalizeScheduleDate,
  validateSchedulerBounds,
  validateSchedulerEvery,
  validateTimezone,
  validateJobId,
} from './utils';

const MAX_TIMEOUT_DELAY_MS = 2_147_483_647;

// ---- Lightweight in-memory Job representation ----

export interface TestJobRecord<D = any, R = any> {
  id: string;
  name: string;
  data: D;
  opts: JobOptions;
  state: 'waiting' | 'active' | 'completed' | 'failed' | 'delayed';
  attemptsMade: number;
  returnvalue: R | undefined;
  failedReason: string | undefined;
  timestamp: number;
  finishedOn: number | undefined;
  processedOn: number | undefined;
  expireAt?: number;
}

/**
 * Minimal Job-like object returned by TestQueue methods and passed to processors.
 * Mirrors the public surface of the real Job class.
 */
export class TestJob<D = any, R = any> {
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
  expireAt?: number;

  constructor(record: TestJobRecord<D, R>) {
    this.id = record.id;
    this.name = record.name;
    this.data = record.data;
    this.opts = record.opts;
    this.attemptsMade = record.attemptsMade;
    this.returnvalue = record.returnvalue;
    this.failedReason = record.failedReason;
    this.progress = 0;
    this.timestamp = record.timestamp;
    this.finishedOn = record.finishedOn;
    this.processedOn = record.processedOn;
    this.expireAt = record.expireAt;
  }

  async log(_message: string): Promise<void> {
    // no-op in test mode
  }

  async updateProgress(p: number | object): Promise<void> {
    this.progress = p;
  }

  async updateData(data: D): Promise<void> {
    this.data = data;
  }

  discarded = false;

  async changePriority(newPriority: number): Promise<void> {
    if (newPriority < 0) {
      throw new Error('Priority must be >= 0');
    }
    this.opts.priority = newPriority;
  }

  async changeDelay(newDelay: number): Promise<void> {
    if (newDelay < 0) {
      throw new Error('Delay must be >= 0');
    }
    this.opts.delay = newDelay;
  }

  async promote(): Promise<void> {
    this.opts.delay = 0;
  }

  discard(): void {
    this.discarded = true;
  }
}

// ---- Search options ----

export interface SearchJobsOptions {
  name?: string;
  data?: Record<string, unknown>;
  state?: TestJobRecord['state'];
  /** When true, excludes `data` and `returnvalue` fields from returned jobs. */
  excludeData?: boolean;
}

/** Check if all key-value pairs in filter exist in data (shallow match). */
function matchesData(data: Record<string, unknown>, filter: Record<string, unknown>): boolean {
  for (const [key, value] of Object.entries(filter)) {
    if (data[key] !== value) return false;
  }
  return true;
}

// ---- TestQueue ----

export interface TestQueueOptions {
  /** Enable deduplication in 'simple' mode. */
  dedup?: boolean;
  /** Custom serializer for job data and return values. When provided, values are roundtripped through serialize/deserialize to match production behavior. */
  serializer?: Serializer;
}

export class TestQueue<D = any, R = any> extends EventEmitter {
  readonly name: string;
  /** @internal */ readonly jobs: Map<string, TestJobRecord<D, R>> = new Map();
  /** @internal */ readonly dedupSet: Set<string> = new Set();
  /** @internal */ readonly waitingQueue: TestJobRecord<D, R>[] = [];
  private idCounter = 0;
  private paused = false;
  private opts: TestQueueOptions;
  /** @internal */ readonly serializer: Serializer;

  /** @internal */ readonly metricsData: Map<string, Map<number, { count: number; totalDuration: number }>> = new Map([
    ['completed', new Map()],
    ['failed', new Map()],
  ]);

  /** Workers register themselves here so we can notify on add. */
  /** @internal */ readonly workers: Set<TestWorker<D, R>> = new Set();
  private schedulers: Map<string, SchedulerEntry> = new Map();
  private schedulerTimer: ReturnType<typeof setTimeout> | null = null;
  private schedulerRunning = false;
  private nextSchedulerWakeAt: number | null = null;

  constructor(name: string, opts?: TestQueueOptions) {
    super();
    this.name = name;
    this.opts = opts ?? {};
    this.serializer = this.opts.serializer ?? JSON_SERIALIZER;
  }

  /** Add a single job. Returns null if deduplicated or duplicate custom ID. */
  async add(name: string, data: D, opts?: JobOptions): Promise<TestJob<D, R> | null> {
    const customJobId = opts?.jobId ?? '';
    if (customJobId !== '') validateJobId(customJobId);

    if (opts?.deduplication && this.opts.dedup) {
      const dedupId = opts.deduplication.id;
      if (this.dedupSet.has(dedupId)) {
        return null;
      }
    }

    let id: string;
    if (customJobId !== '') {
      if (this.jobs.has(customJobId)) {
        return null;
      }
      id = customJobId;
    } else {
      id = String(++this.idCounter);
      let retries = 0;
      while (this.jobs.has(id)) {
        if (++retries >= 1000) throw new Error('Failed to generate job ID: too many collisions with custom job IDs');
        id = String(++this.idCounter);
      }
    }
    // Record dedup key only after all checks pass (custom ID, etc.)
    if (opts?.deduplication && this.opts.dedup) {
      this.dedupSet.add(opts.deduplication.id);
    }
    const now = Date.now();
    const ttl = opts?.ttl ?? 0;
    // Roundtrip data through serializer to match production behavior
    const roundtrippedData = this.serializer.deserialize(this.serializer.serialize(data)) as D;
    const record: TestJobRecord<D, R> = {
      id,
      name,
      data: roundtrippedData,
      opts: opts ?? {},
      state: 'waiting',
      attemptsMade: 0,
      returnvalue: undefined,
      failedReason: undefined,
      timestamp: now,
      finishedOn: undefined,
      processedOn: undefined,
      expireAt: ttl > 0 ? now + ttl : undefined,
    };
    this.jobs.set(id, record);
    this.waitingQueue.push(record);

    const job = new TestJob<D, R>(record);
    this.emit('added', job);

    // Notify attached workers (microtask so the add() caller gets the job first)
    if (!this.paused) {
      queueMicrotask(() => {
        for (const w of this.workers) {
          w.onJobAdded();
        }
      });
    }

    return job;
  }

  /** Add multiple jobs. */
  async addBulk(jobs: { name: string; data: D; opts?: JobOptions }[]): Promise<TestJob<D, R>[]> {
    const results: TestJob<D, R>[] = [];
    for (const entry of jobs) {
      const job = await this.add(entry.name, entry.data, entry.opts);
      if (job) results.push(job);
    }
    return results;
  }

  /** Retrieve a job by ID. */
  async getJob(id: string, opts?: GetJobsOptions): Promise<TestJob<D, R> | null> {
    const record = this.jobs.get(id);
    if (!record) return null;
    const job = new TestJob<D, R>(record);
    if (opts?.excludeData) {
      job.data = undefined as unknown as D;
      job.returnvalue = undefined;
    }
    return job;
  }

  /** Retrieve jobs by state. */
  async getJobs(
    type: 'waiting' | 'active' | 'delayed' | 'completed' | 'failed',
    start = 0,
    end = -1,
    opts?: GetJobsOptions,
  ): Promise<TestJob<D, R>[]> {
    const matching: TestJob<D, R>[] = [];
    for (const record of this.jobs.values()) {
      if (record.state === type) {
        const job = new TestJob<D, R>(record);
        if (opts?.excludeData) {
          job.data = undefined as unknown as D;
          job.returnvalue = undefined;
        }
        matching.push(job);
      }
    }
    const sliceEnd = end >= 0 ? end + 1 : undefined;
    return matching.slice(start, sliceEnd);
  }

  /** Get counts by state. */
  async getJobCounts(): Promise<JobCounts> {
    const counts: JobCounts = { waiting: 0, active: 0, delayed: 0, completed: 0, failed: 0 };
    for (const record of this.jobs.values()) {
      counts[record.state]++;
    }
    return counts;
  }

  /** Get metrics for completed or failed jobs with per-minute data points. */
  async getMetrics(type: 'completed' | 'failed', opts?: MetricsOptions): Promise<Metrics> {
    let count = 0;
    for (const record of this.jobs.values()) {
      if (record.state === type) count++;
    }
    const buckets = this.metricsData.get(type)!;
    const data: MetricsDataPoint[] = Array.from(buckets.entries())
      .sort((a, b) => a[0] - b[0])
      .map(([timestamp, b]) => ({
        timestamp,
        count: b.count,
        avgDuration: b.count > 0 ? Math.round(b.totalDuration / b.count) : 0,
      }));

    const start = opts?.start ?? 0;
    const end = opts?.end ?? -1;

    if (!Number.isInteger(start)) throw new TypeError('start must be an integer');
    if (!Number.isInteger(end)) throw new TypeError('end must be an integer');
    if (start >= 0 && end >= 0 && end < start) {
      throw new RangeError('end must be >= start when both are non-negative');
    }

    const sliced = end === -1 ? data.slice(start) : data.slice(start, end + 1);
    return { count, data: sliced, meta: { resolution: 'minute' } };
  }

  /** @internal */
  recordMetric(type: 'completed' | 'failed', processedOn: number | undefined, finishedOn: number): void {
    const minuteTs = finishedOn - (finishedOn % 60000);
    const buckets = this.metricsData.get(type)!;
    let bucket = buckets.get(minuteTs);
    if (!bucket) {
      bucket = { count: 0, totalDuration: 0 };
      buckets.set(minuteTs, bucket);
      // Cap at 1440 buckets (24 hours of per-minute data)
      if (buckets.size > 1440) {
        const oldest = buckets.keys().next().value;
        if (oldest !== undefined) buckets.delete(oldest);
      }
    }
    bucket.count++;
    const duration = processedOn !== undefined ? finishedOn - processedOn : 0;
    if (duration > 0) bucket.totalDuration += duration;
  }

  /** Pause the queue - workers stop picking up new jobs. */
  async pause(): Promise<void> {
    this.paused = true;
  }

  /** Resume the queue. */
  async resume(): Promise<void> {
    this.paused = false;
    // Kick workers to process any waiting jobs
    for (const w of this.workers) {
      w.onJobAdded();
    }
    this.ensureSchedulerLoop();
  }

  /** Check if paused. */
  isPaused(): boolean {
    return this.paused;
  }

  /** Search jobs by name and/or data fields. */
  async searchJobs(opts: SearchJobsOptions): Promise<TestJob<D, R>[]> {
    const results: TestJob<D, R>[] = [];
    for (const record of this.jobs.values()) {
      if (opts.name !== undefined && record.name !== opts.name) continue;
      if (opts.state !== undefined && record.state !== opts.state) continue;
      if (opts.data !== undefined && !matchesData(record.data as Record<string, unknown>, opts.data)) continue;
      const job = new TestJob<D, R>(record);
      if (opts.excludeData) {
        job.data = undefined as unknown as D;
        job.returnvalue = undefined;
      }
      results.push(job);
    }
    return results;
  }

  /** Bulk-remove old completed or failed jobs by age. */
  async clean(grace: number, limit: number, type: 'completed' | 'failed'): Promise<string[]> {
    if (grace < 0) throw new RangeError('grace must be >= 0');
    if (limit <= 0) return [];
    const cutoff = Date.now() - grace;
    const candidates: [string, number][] = [];
    for (const [id, record] of this.jobs) {
      if (record.state === type && record.finishedOn !== undefined && record.finishedOn <= cutoff) {
        candidates.push([id, record.finishedOn]);
      }
    }
    candidates.sort((a, b) => a[1] - b[1]);
    const removed: string[] = [];
    for (const [id] of candidates.slice(0, limit)) {
      removed.push(id);
      this.jobs.delete(id);
    }
    return removed;
  }

  /** Drain the queue: remove all waiting jobs, optionally delayed ones too. */
  async drain(delayed?: boolean): Promise<void> {
    const toRemove: string[] = [];
    for (const [id, record] of this.jobs) {
      if (record.state === 'waiting' || (delayed && record.state === 'delayed')) {
        toRemove.push(id);
      }
    }
    for (const id of toRemove) {
      this.jobs.delete(id);
    }
    // Clear waitingQueue - stale entries will be skipped by findNextWaiting,
    // but draining waiting jobs means the queue should be empty.
    this.waitingQueue.length = 0;
    if (toRemove.length > 0) {
      this.emit('drained', toRemove.length);
    }
  }

  /**
   * Bulk retry failed jobs.
   * Moves failed jobs back to waiting, resets attemptsMade/failedReason/finishedOn.
   * @param opts.count - Maximum number of jobs to retry. Omit or 0 to retry all.
   * @returns Number of jobs retried.
   */
  async retryJobs(opts?: { count?: number }): Promise<number> {
    if (opts?.count != null && (!Number.isInteger(opts.count) || opts.count < 0)) {
      throw new Error('count must be a non-negative integer');
    }
    const limit = opts?.count ?? 0;
    let retried = 0;
    for (const record of this.jobs.values()) {
      if (limit > 0 && retried >= limit) break;
      if (record.state !== 'failed') continue;
      record.state = 'waiting';
      record.attemptsMade = 0;
      record.failedReason = undefined;
      record.finishedOn = undefined;
      this.waitingQueue.push(record);
      retried++;
    }
    // Notify workers so retried jobs get picked up
    if (retried > 0 && !this.paused) {
      queueMicrotask(() => {
        for (const w of this.workers) {
          w.onJobAdded();
        }
      });
    }
    return retried;
  }

  /** List active workers attached to this queue. */
  async getWorkers(): Promise<WorkerInfo[]> {
    const now = Date.now();
    const result: WorkerInfo[] = [];
    for (const w of this.workers) {
      result.push({
        id: w.id,
        addr: os.hostname(),
        pid: process.pid,
        startedAt: w.startedAt,
        age: now - w.startedAt,
        activeJobs: w.getActiveCount(),
      });
    }
    result.sort((a, b) => a.startedAt - b.startedAt);
    return result;
  }

  async upsertJobScheduler(name: string, schedule: ScheduleOpts, template?: JobTemplate): Promise<void> {
    validateSchedulerEvery(schedule.every);
    if (schedule.repeatAfterComplete != null) {
      if (!Number.isSafeInteger(schedule.repeatAfterComplete) || schedule.repeatAfterComplete <= 0) {
        throw new Error('repeatAfterComplete must be a positive safe integer');
      }
    }
    const modeCount = (schedule.pattern ? 1 : 0) + (schedule.every ? 1 : 0) + (schedule.repeatAfterComplete ? 1 : 0);
    if (modeCount === 0) {
      throw new Error('Schedule must have pattern (cron), every (ms interval), or repeatAfterComplete (ms)');
    }
    if (modeCount > 1) {
      throw new Error('Schedule must have only one of: pattern, every, repeatAfterComplete');
    }
    if (schedule.tz) {
      validateTimezone(schedule.tz);
    }
    const startDate = normalizeScheduleDate(schedule.startDate, 'startDate');
    const endDate = normalizeScheduleDate(schedule.endDate, 'endDate');
    validateSchedulerBounds(startDate, endDate, schedule.limit);
    const now = Date.now();
    let iterationCount = 0;
    let lastRun: number | undefined;
    let nextRun = computeInitialSchedulerNextRun(
      {
        pattern: schedule.pattern,
        every: schedule.every,
        repeatAfterComplete: schedule.repeatAfterComplete,
        tz: schedule.tz,
        startDate,
        endDate,
      },
      now,
    );
    const existing = this.schedulers.get(name);
    if (existing) {
      const scheduleUnchanged =
        existing.pattern === schedule.pattern &&
        existing.every === schedule.every &&
        existing.repeatAfterComplete === schedule.repeatAfterComplete &&
        existing.tz === schedule.tz &&
        existing.startDate === startDate &&
        existing.endDate === endDate;
      if (scheduleUnchanged && existing.nextRun != null) {
        iterationCount = existing.iterationCount ?? 0;
        lastRun = existing.lastRun;
        nextRun = existing.nextRun;
      }
    }
    if (nextRun == null) {
      throw new Error('Schedule has no occurrences within the configured bounds');
    }
    const entry: SchedulerEntry = {
      pattern: schedule.pattern,
      every: schedule.every,
      repeatAfterComplete: schedule.repeatAfterComplete,
      tz: schedule.tz,
      startDate,
      endDate,
      limit: schedule.limit,
      iterationCount,
      template,
      lastRun,
      nextRun,
    };
    // Store via JSON roundtrip to detach from caller references (matches production serialization)
    this.schedulers.set(name, JSON.parse(JSON.stringify(entry)));
    this.ensureSchedulerLoop();
  }

  async removeJobScheduler(name: string): Promise<void> {
    this.schedulers.delete(name);
    this.ensureSchedulerLoop();
  }

  async getJobScheduler(name: string): Promise<SchedulerEntry | null> {
    const entry = this.schedulers.get(name);
    if (!entry) return null;
    return JSON.parse(JSON.stringify(entry));
  }

  async getRepeatableJobs(): Promise<{ name: string; entry: SchedulerEntry }[]> {
    return [...this.schedulers.entries()].map(([name, entry]) => ({
      name,
      entry: JSON.parse(JSON.stringify(entry)),
    }));
  }

  /** Close the queue. */
  async close(): Promise<void> {
    this.clearSchedulerTimer();
    this.removeAllListeners();
    this.workers.clear();
  }

  /** @internal Called by TestWorker when it attaches. */
  onWorkerAttached(): void {
    this.ensureSchedulerLoop();
  }

  /** @internal Called by TestWorker when it detaches. */
  onWorkerDetached(): void {
    if (this.workers.size === 0) {
      this.clearSchedulerTimer();
    }
  }

  private clearSchedulerTimer(): void {
    if (this.schedulerTimer) {
      clearTimeout(this.schedulerTimer);
      this.schedulerTimer = null;
    }
    this.nextSchedulerWakeAt = null;
  }

  private ensureSchedulerLoop(): void {
    if (this.schedulerRunning || this.workers.size === 0 || this.schedulers.size === 0) {
      return;
    }

    let nextDue = Number.POSITIVE_INFINITY;
    const now = Date.now();
    for (const entry of this.schedulers.values()) {
      if (entry.nextRun != null && entry.nextRun < nextDue) {
        nextDue = entry.nextRun;
      }
    }
    if (!Number.isFinite(nextDue)) return;

    if (this.schedulerTimer && this.nextSchedulerWakeAt != null && this.nextSchedulerWakeAt <= nextDue) {
      return;
    }

    this.clearSchedulerTimer();

    const delay = Math.max(0, nextDue - now);
    const clampedDelay = Math.min(delay, MAX_TIMEOUT_DELAY_MS);
    this.nextSchedulerWakeAt = now + clampedDelay;
    this.schedulerTimer = setTimeout(() => {
      this.schedulerTimer = null;
      this.nextSchedulerWakeAt = null;
      void this.runDueSchedulers();
    }, clampedDelay);
  }

  private async runDueSchedulers(): Promise<void> {
    if (this.schedulerRunning || this.workers.size === 0) return;
    this.schedulerRunning = true;

    try {
      const now = Date.now();
      const dueNames: string[] = [];
      for (const [name, entry] of this.schedulers.entries()) {
        if (!entry.pattern && !entry.every && !entry.repeatAfterComplete) {
          dueNames.push(name);
          continue;
        }
        // Skip repeatAfterComplete entries with nextRun=0 (awaiting completion)
        if (entry.repeatAfterComplete && entry.nextRun === 0) continue;

        if (entry.nextRun != null && entry.nextRun <= now) {
          dueNames.push(name);
        }
      }

      for (const name of dueNames) {
        const entry = this.schedulers.get(name);
        if (!entry) continue;

        if (!entry.pattern && !entry.every && !entry.repeatAfterComplete) {
          this.schedulers.delete(name);
          continue;
        }
        if (entry.nextRun == null || entry.nextRun > now) continue;

        const currentIterationCount = entry.iterationCount ?? 0;
        if (
          (entry.limit != null && currentIterationCount >= entry.limit) ||
          (entry.endDate != null && entry.nextRun > entry.endDate)
        ) {
          this.schedulers.delete(name);
          continue;
        }

        const template = entry.template ?? {};
        const jobName = template.name ?? name;
        let jobData = template.data !== undefined ? template.data : ({} as D);
        try {
          const serialized = this.serializer.serialize(jobData);
          const byteLen = Buffer.byteLength(serialized, 'utf8');
          if (byteLen > MAX_JOB_DATA_SIZE) {
            this.schedulers.delete(name);
            continue;
          }
          jobData = this.serializer.deserialize(serialized) as D;
        } catch {
          this.schedulers.delete(name);
          continue;
        }
        await this.add(jobName, jobData, template.opts as JobOptions | undefined);

        entry.lastRun = now;
        entry.iterationCount = currentIterationCount + 1;
        if (entry.limit != null && entry.iterationCount >= entry.limit) {
          this.schedulers.delete(name);
        } else {
          const nextRun = computeFollowingSchedulerNextRun(entry, now);
          if (nextRun == null) {
            this.schedulers.delete(name);
          } else {
            entry.nextRun = nextRun;
            this.schedulers.set(name, JSON.parse(JSON.stringify(entry)));
          }
        }
      }
    } finally {
      this.schedulerRunning = false;
      this.ensureSchedulerLoop();
    }
  }
}

// ---- TestWorker ----

export interface TestWorkerOptions {
  concurrency?: number;
  batch?: { size: number; timeout?: number };
}

export class TestWorker<D = any, R = any> extends EventEmitter {
  private static idCounter = 0;
  readonly id: string;
  readonly startedAt: number;
  private queue: TestQueue<D, R>;
  private processor: Processor<D, R>;
  private concurrency: number;
  private activeCount = 0;
  private running = true;
  private processing = false;
  private isDrained = true;
  private readonly batchMode: boolean;
  private readonly batchSize: number;
  private readonly batchTimeout: number;
  private readonly batchProcessor: ((jobs: TestJob<D, R>[]) => Promise<R[]>) | null;
  private batchTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(
    queue: TestQueue<D, R>,
    processor: Processor<D, R> | ((jobs: TestJob<D, R>[]) => Promise<R[]>) | string,
    opts?: TestWorkerOptions,
  ) {
    super();
    this.queue = queue;

    // Batch mode validation
    this.batchMode = !!opts?.batch;
    if (opts?.batch) {
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
      this.batchProcessor = processor as (jobs: TestJob<D, R>[]) => Promise<R[]>;
      this.processor = (() => {
        throw new Error('Single-job processor called in batch mode');
      }) as unknown as Processor<D, R>;
    } else {
      this.batchSize = 0;
      this.batchTimeout = 0;
      this.batchProcessor = null;

      if (typeof processor === 'string') {
        const filePath = path.resolve(processor);
        if (filePath.endsWith('.mjs')) {
          throw new GlideMQError(
            'TestWorker does not support ESM (.mjs) processors. Use a .js (CJS) file or an inline function.',
          );
        }
        // eslint-disable-next-line @typescript-eslint/no-require-imports
        const mod = require(filePath);
        const fn = mod.default || mod;
        if (typeof fn !== 'function') {
          throw new GlideMQError(`Processor file ${filePath} does not export a function`);
        }
        this.processor = fn;
      } else {
        this.processor = processor as Processor<D, R>;
      }
    }
    this.concurrency = opts?.concurrency ?? 1;
    this.id = `test-worker-${++TestWorker.idCounter}`;
    this.startedAt = Date.now();

    // Register with the queue
    queue.workers.add(this);
    queue.onWorkerAttached();

    // Process any jobs already in the queue
    queueMicrotask(() => this.drain());
  }

  /** @internal Called by TestQueue when a job is added. */
  onJobAdded(): void {
    if (!this.running) return;
    this.drain();
  }

  /** Pull and process waiting jobs up to concurrency. */
  private drain(): void {
    if (this.processing) return;
    this.processing = true;

    // Use microtask to batch multiple onJobAdded calls
    queueMicrotask(() => {
      this.processing = false;
      this.processAvailable();
    });
  }

  private processAvailable(): void {
    if (!this.running || this.queue.isPaused()) return;

    if (this.batchMode) {
      this.processAvailableBatch();
      return;
    }

    while (this.activeCount < this.concurrency) {
      const record = this.findNextWaiting();
      if (!record) break;

      record.state = 'active';
      record.processedOn = Date.now();
      this.activeCount++;
      this.isDrained = false;
      const job = new TestJob<D, R>(record);
      this.emit('active', job, record.id);
      this.processJob(record, job);
    }

    if (this.activeCount === 0 && !this.isDrained) {
      this.isDrained = true;
      this.emit('drained');
    }
  }

  private findNextWaiting(): TestJobRecord<D, R> | undefined {
    // Shift from the front of the waitingQueue, skipping stale entries
    // (records whose state changed out of 'waiting' since they were enqueued).
    while (this.queue.waitingQueue.length > 0) {
      const record = this.queue.waitingQueue.shift()!;
      if (record.state === 'waiting') return record;
    }
    return undefined;
  }

  private processJob(record: TestJobRecord<D, R>, job: TestJob<D, R>): void {
    // Check TTL expiration before processing
    if (record.expireAt && Date.now() > record.expireAt) {
      record.state = 'failed';
      record.failedReason = 'expired';
      record.finishedOn = Date.now();
      job.failedReason = 'expired';
      job.finishedOn = record.finishedOn;
      this.queue.recordMetric('failed', record.processedOn, record.finishedOn);
      const err = new Error('expired');
      this.emit('failed', job, err);
      this.queue.emit('failed', job, err);
      this.activeCount--;
      if (this.running && !this.queue.isPaused()) {
        this.processAvailable();
      }
      return;
    }
    this.processor(job as any)
      .then((result) => {
        // Roundtrip returnvalue through serializer to match production behavior
        const s = this.queue.serializer;
        const roundtripped = result !== undefined ? (s.deserialize(s.serialize(result)) as R) : result;
        record.state = 'completed';
        record.returnvalue = roundtripped;
        record.finishedOn = Date.now();
        job.returnvalue = roundtripped;
        job.finishedOn = record.finishedOn;
        this.queue.recordMetric('completed', record.processedOn, record.finishedOn);
        this.emit('completed', job, roundtripped);
        this.queue.emit('completed', job, roundtripped);
      })
      .catch((err: Error) => {
        record.attemptsMade++;
        const maxAttempts = record.opts.attempts ?? 0;

        const skipRetry = job.discarded || err instanceof UnrecoverableError || err.name === 'UnrecoverableError';
        if (maxAttempts > 0 && record.attemptsMade < maxAttempts && !skipRetry) {
          // Retry: put back to waiting
          record.state = 'waiting';
          this.queue.waitingQueue.push(record);
          // Schedule a re-drain so the retry gets picked up
          queueMicrotask(() => this.processAvailable());
        } else {
          record.state = 'failed';
          record.failedReason = err.message;
          record.finishedOn = Date.now();
          job.failedReason = err.message;
          job.finishedOn = record.finishedOn;
          this.queue.recordMetric('failed', record.processedOn, record.finishedOn);
          this.emit('failed', job, err);
          this.queue.emit('failed', job, err);
        }
      })
      .finally(() => {
        this.activeCount--;
        // Try to pick up more work
        if (this.running && !this.queue.isPaused()) {
          this.processAvailable();
        }
      });
  }

  // ---- Batch processing ----

  private processAvailableBatch(): void {
    // Respect concurrency: don't start a new batch if at capacity
    if (this.activeCount >= this.concurrency * this.batchSize) return;

    // Collect up to batchSize waiting jobs from the waitingQueue
    const records: TestJobRecord<D, R>[] = [];
    while (this.queue.waitingQueue.length > 0 && records.length < this.batchSize) {
      const record = this.queue.waitingQueue[0];
      if (record.state !== 'waiting') {
        this.queue.waitingQueue.shift();
        continue;
      }
      this.queue.waitingQueue.shift();
      records.push(record);
    }

    if (records.length === 0) {
      if (this.activeCount === 0 && !this.isDrained) {
        this.isDrained = true;
        this.emit('drained');
      }
      return;
    }

    // Batch is full - process immediately even if timer is running
    if (records.length >= this.batchSize) {
      if (this.batchTimer) {
        clearTimeout(this.batchTimer);
        this.batchTimer = null;
      }
      this.executeBatch(records);
      return;
    }

    if (this.batchTimeout > 0) {
      // Start a timer to flush the partial batch after timeout
      if (!this.batchTimer) {
        this.batchTimer = setTimeout(() => {
          this.batchTimer = null;
          this.flushBatch();
        }, this.batchTimeout);
      }
      return;
    }

    // No timeout - process partial batch immediately
    this.executeBatch(records);
  }

  private flushBatch(): void {
    if (!this.running) return;
    // Re-collect from waitingQueue, skipping stale entries
    const records: TestJobRecord<D, R>[] = [];
    while (this.queue.waitingQueue.length > 0 && records.length < this.batchSize) {
      const record = this.queue.waitingQueue[0];
      if (record.state !== 'waiting') {
        this.queue.waitingQueue.shift();
        continue;
      }
      this.queue.waitingQueue.shift();
      records.push(record);
    }
    if (records.length > 0) {
      this.executeBatch(records);
    }
  }

  private executeBatch(records: TestJobRecord<D, R>[]): void {
    if (!this.batchProcessor) return;

    // Mark all as active
    for (const record of records) {
      record.state = 'active';
      record.processedOn = Date.now();
    }
    this.activeCount += records.length;
    this.isDrained = false;

    const jobs = records.map((r) => new TestJob<D, R>(r));
    for (const job of jobs) {
      this.emit('active', job, job.id);
    }

    this.batchProcessor(jobs)
      .then((results) => {
        if (results.length !== records.length) {
          throw new Error(`Batch processor returned ${results.length} results but batch had ${records.length} jobs`);
        }
        const s = this.queue.serializer;
        for (let i = 0; i < records.length; i++) {
          const record = records[i];
          const job = jobs[i];
          const result = results[i];
          const roundtripped = result !== undefined ? (s.deserialize(s.serialize(result)) as R) : result;
          record.state = 'completed';
          record.returnvalue = roundtripped;
          record.finishedOn = Date.now();
          job.returnvalue = roundtripped;
          job.finishedOn = record.finishedOn;
          this.queue.recordMetric('completed', record.processedOn, record.finishedOn);
          this.emit('completed', job, roundtripped);
          this.queue.emit('completed', job, roundtripped);
        }
      })
      .catch((err: Error) => {
        if (err instanceof BatchError || err.name === 'BatchError') {
          const batchErr = err as BatchError;
          for (let i = 0; i < records.length; i++) {
            const record = records[i];
            const job = jobs[i];
            const result = i < batchErr.results.length ? batchErr.results[i] : new Error('No result in BatchError');

            if (result instanceof Error) {
              record.attemptsMade++;
              const maxAttempts = record.opts.attempts ?? 0;
              const skipRetry =
                job.discarded || result instanceof UnrecoverableError || result.name === 'UnrecoverableError';
              if (maxAttempts > 0 && record.attemptsMade < maxAttempts && !skipRetry) {
                record.state = 'waiting';
                this.queue.waitingQueue.push(record);
                queueMicrotask(() => this.processAvailable());
              } else {
                record.state = 'failed';
                record.failedReason = result.message;
                record.finishedOn = Date.now();
                job.failedReason = result.message;
                job.finishedOn = record.finishedOn;
                this.queue.recordMetric('failed', record.processedOn, record.finishedOn);
                this.emit('failed', job, result);
                this.queue.emit('failed', job, result);
              }
            } else {
              const s = this.queue.serializer;
              const roundtripped =
                result !== undefined ? (s.deserialize(s.serialize(result as R)) as R) : (result as R);
              record.state = 'completed';
              record.returnvalue = roundtripped;
              record.finishedOn = Date.now();
              job.returnvalue = roundtripped;
              job.finishedOn = record.finishedOn;
              this.queue.recordMetric('completed', record.processedOn, record.finishedOn);
              this.emit('completed', job, roundtripped);
              this.queue.emit('completed', job, roundtripped);
            }
          }
        } else {
          // All jobs fail
          for (let i = 0; i < records.length; i++) {
            const record = records[i];
            const job = jobs[i];
            record.attemptsMade++;
            const maxAttempts = record.opts.attempts ?? 0;
            const skipRetry = job.discarded || err instanceof UnrecoverableError || err.name === 'UnrecoverableError';
            if (maxAttempts > 0 && record.attemptsMade < maxAttempts && !skipRetry) {
              record.state = 'waiting';
              this.queue.waitingQueue.push(record);
              queueMicrotask(() => this.processAvailable());
            } else {
              record.state = 'failed';
              record.failedReason = err.message;
              record.finishedOn = Date.now();
              job.failedReason = err.message;
              job.finishedOn = record.finishedOn;
              this.queue.recordMetric('failed', record.processedOn, record.finishedOn);
              this.emit('failed', job, err);
              this.queue.emit('failed', job, err);
            }
          }
        }
      })
      .finally(() => {
        this.activeCount -= records.length;
        if (this.running && !this.queue.isPaused()) {
          this.processAvailable();
        }
      });
  }

  /** Return the number of jobs currently being processed. */
  getActiveCount(): number {
    return this.activeCount;
  }

  /** Stop processing and detach from the queue. */
  async close(): Promise<void> {
    this.running = false;
    if (this.batchTimer) {
      clearTimeout(this.batchTimer);
      this.batchTimer = null;
    }
    this.queue.workers.delete(this);
    this.queue.onWorkerDetached();
    this.removeAllListeners();
  }
}
