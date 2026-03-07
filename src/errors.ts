export class GlideMQError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'GlideMQError';
  }
}

export class ConnectionError extends GlideMQError {
  constructor(message: string) {
    super(message);
    this.name = 'ConnectionError';
  }
}

export class ScriptError extends GlideMQError {
  constructor(message: string) {
    super(message);
    this.name = 'ScriptError';
  }
}

export class UnrecoverableError extends GlideMQError {
  constructor(message: string) {
    super(message);
    this.name = 'UnrecoverableError';
  }
}

export class BatchError extends GlideMQError {
  readonly results: (unknown | Error)[];

  constructor(results: (unknown | Error)[]) {
    super('Batch processor reported per-job results');
    this.name = 'BatchError';
    this.results = results;
  }
}

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

export class WaitingChildrenError extends GlideMQError {
  constructor(message = 'Job moved to waiting-children state') {
    super(message);
    this.name = 'WaitingChildrenError';
  }
}
