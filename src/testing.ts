/**
 * In-memory testing mode for glide-mq.
 * TestQueue and TestWorker mimic the real API using plain Maps - no Valkey needed.
 *
 * Usage:
 *   import { TestQueue, TestWorker } from 'glide-mq/testing';
 */

import { EventEmitter } from 'events';
import path from 'path';
import type { JobOptions, JobCounts, Processor } from './types';
import { GlideMQError, UnrecoverableError } from './errors';

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
}

export class TestQueue<D = any, R = any> extends EventEmitter {
  readonly name: string;
  /** @internal */ readonly jobs: Map<string, TestJobRecord<D, R>> = new Map();
  /** @internal */ readonly dedupSet: Set<string> = new Set();
  private idCounter = 0;
  private paused = false;
  private opts: TestQueueOptions;

  /** Workers register themselves here so we can notify on add. */
  /** @internal */ readonly workers: Set<TestWorker<D, R>> = new Set();

  constructor(name: string, opts?: TestQueueOptions) {
    super();
    this.name = name;
    this.opts = opts ?? {};
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
    const record: TestJobRecord<D, R> = {
      id,
      name,
      data,
      opts: opts ?? {},
      state: 'waiting',
      attemptsMade: 0,
      returnvalue: undefined,
      failedReason: undefined,
      timestamp: Date.now(),
      finishedOn: undefined,
      processedOn: undefined,
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

  /** Close the queue. */
  async close(): Promise<void> {
    this.removeAllListeners();
    this.workers.clear();
  }
}

// ---- TestWorker ----

export interface TestWorkerOptions {
  concurrency?: number;
}

export class TestWorker<D = any, R = any> extends EventEmitter {
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

    // Register with the queue
    queue.workers.add(this);

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
    this.processor(job as any)
      .then((result) => {
        record.state = 'completed';
        record.returnvalue = result;
        record.finishedOn = Date.now();
        job.returnvalue = result;
        job.finishedOn = record.finishedOn;
        this.emit('completed', job, result);
        this.queue.emit('completed', job, result);
      })
      .catch((err: Error) => {
        record.attemptsMade++;
        const maxAttempts = record.opts.attempts ?? 0;

        if (maxAttempts > 0 && record.attemptsMade < maxAttempts && !job.discarded && !(err instanceof UnrecoverableError)) {
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

  /** Stop processing and detach from the queue. */
  async close(): Promise<void> {
    this.running = false;
    this.queue.workers.delete(this);
    this.removeAllListeners();
  }
}
