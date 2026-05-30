/**
 * Unit tests for the error classes in src/errors.ts.
 * No Valkey connection needed.
 */
import { describe, it, expect } from 'vitest';

const {
  GlideMQError,
  ConnectionError,
  UnrecoverableError,
  BatchError,
  DelayedError,
  WaitingChildrenError,
  SuspendError,
  GroupRateLimitError,
} = require('../dist/errors') as typeof import('../src/errors');

describe('GlideMQError', () => {
  it('is an Error with the correct name and message', () => {
    const err = new GlideMQError('boom');
    expect(err).toBeInstanceOf(Error);
    expect(err).toBeInstanceOf(GlideMQError);
    expect(err.name).toBe('GlideMQError');
    expect(err.message).toBe('boom');
    expect(err.stack).toBeDefined();
  });
});

describe('ConnectionError', () => {
  it('extends GlideMQError with its own name', () => {
    const err = new ConnectionError('cannot connect');
    expect(err).toBeInstanceOf(GlideMQError);
    expect(err).toBeInstanceOf(ConnectionError);
    expect(err.name).toBe('ConnectionError');
    expect(err.message).toBe('cannot connect');
  });
});

describe('UnrecoverableError', () => {
  it('extends GlideMQError and signals no-retry', () => {
    const err = new UnrecoverableError('do not retry');
    expect(err).toBeInstanceOf(GlideMQError);
    expect(err).toBeInstanceOf(UnrecoverableError);
    expect(err.name).toBe('UnrecoverableError');
    expect(err.message).toBe('do not retry');
  });
});

describe('BatchError', () => {
  it('carries per-job results', () => {
    const results = [{ ok: true }, new Error('job 2 failed'), 'plain'];
    const err = new BatchError(results);
    expect(err).toBeInstanceOf(GlideMQError);
    expect(err).toBeInstanceOf(BatchError);
    expect(err.name).toBe('BatchError');
    expect(err.message).toBe('Batch processor reported per-job results');
    expect(err.results).toBe(results);
    expect(err.results).toHaveLength(3);
    expect(err.results[1]).toBeInstanceOf(Error);
  });

  it('accepts an empty results array', () => {
    const err = new BatchError([]);
    expect(err.results).toEqual([]);
  });
});

describe('DelayedError', () => {
  it('uses the default message and stores delayedUntil', () => {
    const err = new DelayedError(1234);
    expect(err).toBeInstanceOf(GlideMQError);
    expect(err).toBeInstanceOf(DelayedError);
    expect(err.name).toBe('DelayedError');
    expect(err.message).toBe('Job moved to delayed state');
    expect(err.delayedUntil).toBe(1234);
  });

  it('accepts a custom message', () => {
    const err = new DelayedError(0, 'custom');
    expect(err.message).toBe('custom');
    expect(err.delayedUntil).toBe(0);
  });

  it('rejects a negative timestamp', () => {
    expect(() => new DelayedError(-1)).toThrow(GlideMQError);
    expect(() => new DelayedError(-1)).toThrow(/finite Unix millisecond timestamp/);
  });

  it('rejects non-finite timestamps', () => {
    expect(() => new DelayedError(Number.NaN)).toThrow(GlideMQError);
    expect(() => new DelayedError(Number.POSITIVE_INFINITY)).toThrow(GlideMQError);
  });
});

describe('WaitingChildrenError', () => {
  it('uses the default message', () => {
    const err = new WaitingChildrenError();
    expect(err).toBeInstanceOf(GlideMQError);
    expect(err).toBeInstanceOf(WaitingChildrenError);
    expect(err.name).toBe('WaitingChildrenError');
    expect(err.message).toBe('Job moved to waiting-children state');
  });

  it('accepts a custom message', () => {
    const err = new WaitingChildrenError('children pending');
    expect(err.message).toBe('children pending');
  });
});

describe('SuspendError', () => {
  it('has a fixed message and name', () => {
    const err = new SuspendError();
    expect(err).toBeInstanceOf(GlideMQError);
    expect(err).toBeInstanceOf(SuspendError);
    expect(err.name).toBe('SuspendError');
    expect(err.message).toBe('Job suspended');
  });
});

describe('GroupRateLimitError', () => {
  it('stores delayMs and applies default options', () => {
    const err = new GroupRateLimitError(500);
    expect(err).toBeInstanceOf(GlideMQError);
    expect(err).toBeInstanceOf(GroupRateLimitError);
    expect(err.name).toBe('GroupRateLimitError');
    expect(err.message).toBe('group rate limited');
    expect(err.delayMs).toBe(500);
    expect(err.opts).toEqual({ currentJob: 'requeue', requeuePosition: 'front', extend: 'max' });
  });

  it('honors explicit options', () => {
    const err = new GroupRateLimitError(1000, {
      currentJob: 'fail',
      requeuePosition: 'back',
      extend: 'replace',
    });
    expect(err.opts).toEqual({ currentJob: 'fail', requeuePosition: 'back', extend: 'replace' });
  });

  it('fills only the omitted options with defaults', () => {
    const err = new GroupRateLimitError(1000, { currentJob: 'fail' });
    expect(err.opts).toEqual({ currentJob: 'fail', requeuePosition: 'front', extend: 'max' });
  });

  it('rejects a non-positive duration', () => {
    expect(() => new GroupRateLimitError(0)).toThrow(GlideMQError);
    expect(() => new GroupRateLimitError(-5)).toThrow(/positive finite duration/);
  });

  it('rejects a non-finite duration', () => {
    expect(() => new GroupRateLimitError(Number.NaN)).toThrow(GlideMQError);
    expect(() => new GroupRateLimitError(Number.POSITIVE_INFINITY)).toThrow(GlideMQError);
  });
});
