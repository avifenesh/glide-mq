/**
 * In-memory testing mode for glide-mq.
 * TestQueue and TestWorker mimic the real API using plain Maps - no Valkey needed.
 *
 * Usage:
 *   import { TestQueue, TestWorker } from 'glide-mq/testing';
 */

import { EventEmitter } from 'events';
import path from 'path';
import os from 'os';
import type {
  JobOptions,
  JobCounts,
  Processor,
  WorkerInfo,
  SchedulerEntry,
  ScheduleOpts,
  JobTemplate,
  Serializer,
} from './types';
import { JSON_SERIALIZER } from './types';
import { GlideMQError, UnrecoverableError } from './errors';
import {
  MAX_JOB_DATA_SIZE,
  computeFollowingSchedulerNextRun,
  computeInitialSchedulerNextRun,
  normalizeScheduleDate,
  validateSchedulerBounds,
  validateSchedulerEvery,
  validateTimezone,
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
  private idCounter = 0;
  private paused = false;
  private opts: TestQueueOptions;
  /** @internal */ readonly serializer: Serializer;

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

  /** Add a single job. Returns null if deduplicated. */
  async add(name: string, data: D, opts?: JobOptions): Promise<TestJob<D, R> | null> {
    if (opts?.deduplication && this.opts.dedup) {
      const dedupId = opts.deduplication.id;
      if (this.dedupSet.has(dedupId)) {
        return null;
      }
      this.dedupSet.add(dedupId);
    }

    const id = String(++this.idCounter);
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
  async getJob(id: string): Promise<TestJob<D, R> | null> {
    const record = this.jobs.get(id);
    if (!record) return null;
    return new TestJob<D, R>(record);
  }

  /** Retrieve jobs by state. */
  async getJobs(
    type: 'waiting' | 'active' | 'delayed' | 'completed' | 'failed',
    start = 0,
    end = -1,
  ): Promise<TestJob<D, R>[]> {
    const matching: TestJob<D, R>[] = [];
    for (const record of this.jobs.values()) {
      if (record.state === type) {
        matching.push(new TestJob<D, R>(record));
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
      results.push(new TestJob<D, R>(record));
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
    if (!schedule.pattern && !schedule.every) {
      throw new Error('Schedule must have either pattern (cron) or every (ms interval)');
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
        existing.tz === schedule.tz &&
        existing.startDate === startDate &&
        existing.endDate === endDate;
      if (scheduleUnchanged && existing.nextRun) {
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
        if (!entry.pattern && !entry.every) {
          dueNames.push(name);
          continue;
        }
        if (entry.nextRun != null && entry.nextRun <= now) {
          dueNames.push(name);
        }
      }

      for (const name of dueNames) {
        const entry = this.schedulers.get(name);
        if (!entry) continue;

        if (!entry.pattern && !entry.every) {
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
        const job = await this.add(jobName, jobData, template.opts as JobOptions | undefined);
        if (!job) continue;

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

  constructor(queue: TestQueue<D, R>, processor: Processor<D, R> | string, opts?: TestWorkerOptions) {
    super();
    this.queue = queue;
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
      this.processor = processor;
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
    for (const record of this.queue.jobs.values()) {
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
          // Schedule a re-drain so the retry gets picked up
          queueMicrotask(() => this.processAvailable());
        } else {
          record.state = 'failed';
          record.failedReason = err.message;
          record.finishedOn = Date.now();
          job.failedReason = err.message;
          job.finishedOn = record.finishedOn;
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

  /** Return the number of jobs currently being processed. */
  getActiveCount(): number {
    return this.activeCount;
  }

  /** Stop processing and detach from the queue. */
  async close(): Promise<void> {
    this.running = false;
    this.queue.workers.delete(this);
    this.queue.onWorkerDetached();
    this.removeAllListeners();
  }
}
