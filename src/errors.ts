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
