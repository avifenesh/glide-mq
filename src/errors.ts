/** Base error class for all glide-mq errors. */
export class GlideMQError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'GlideMQError';
  }
}

/** Thrown when a Valkey/Redis connection cannot be established. */
export class ConnectionError extends GlideMQError {
  constructor(message: string) {
    super(message);
    this.name = 'ConnectionError';
  }
}

/** Thrown to signal a job failure that should never be retried, regardless of attempts config. */
export class UnrecoverableError extends GlideMQError {
  constructor(message: string) {
    super(message);
    this.name = 'UnrecoverableError';
  }
}

/** Thrown by batch processors to report per-job results (mixed success/failure). */
export class BatchError extends GlideMQError {
  readonly results: (unknown | Error)[];

  constructor(results: (unknown | Error)[]) {
    super('Batch processor reported per-job results');
    this.name = 'BatchError';
    this.results = results;
  }
}

/** Internal control-flow error thrown by `job.moveToDelayed()`. Caught by the Worker. */
export class DelayedError extends GlideMQError {
  readonly delayedUntil: number;

  constructor(delayedUntil: number, message = 'Job moved to delayed state') {
    if (!Number.isFinite(delayedUntil) || delayedUntil < 0) {
      throw new GlideMQError('DelayedError requires a finite Unix millisecond timestamp >= 0');
    }
    super(message);
    this.name = 'DelayedError';
    this.delayedUntil = delayedUntil;
  }
}

/** Internal control-flow error thrown by `job.moveToWaitingChildren()`. Caught by the Worker. */
export class WaitingChildrenError extends GlideMQError {
  constructor(message = 'Job moved to waiting-children state') {
    super(message);
    this.name = 'WaitingChildrenError';
  }
}

/** Internal control-flow error thrown by `job.suspend()`. Caught by the Worker. */
export class SuspendError extends GlideMQError {
  constructor() {
    super('Job suspended');
    this.name = 'SuspendError';
  }
}

/** Options controlling behavior when `job.rateLimitGroup()` is called. */
export interface GroupRateLimitOptions {
  /** What happens to the current job. Default: 'requeue' (re-parks without consuming retry). */
  currentJob?: 'requeue' | 'fail';
  /** Where to re-park the job in the group queue. Default: 'front' (resumes first). */
  requeuePosition?: 'front' | 'back';
  /** How to handle existing rate limit. Default: 'max' (never shortens). */
  extend?: 'max' | 'replace';
}

/** Internal control-flow error thrown by `job.rateLimitGroup()`. Caught by the Worker. */
export class GroupRateLimitError extends GlideMQError {
  readonly delayMs: number;
  readonly opts: Required<GroupRateLimitOptions>;

  constructor(delayMs: number, opts?: GroupRateLimitOptions) {
    if (!Number.isFinite(delayMs) || delayMs <= 0) {
      throw new GlideMQError('GroupRateLimitError requires a positive finite duration in milliseconds');
    }
    super('group rate limited');
    this.name = 'GroupRateLimitError';
    this.delayMs = delayMs;
    this.opts = {
      currentJob: opts?.currentJob ?? 'requeue',
      requeuePosition: opts?.requeuePosition ?? 'front',
      extend: opts?.extend ?? 'max',
    };
  }
}
