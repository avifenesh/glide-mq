import { describe, it, expect } from 'vitest';
import { gzipSync } from 'zlib';
import { nextCronOccurrence, decompress, MAX_JOB_DATA_SIZE } from '../src/utils';

describe('decompress', () => {
  it('rejects decompression bombs exceeding MAX_JOB_DATA_SIZE', () => {
    const huge = Buffer.alloc(MAX_JOB_DATA_SIZE + 1, 0x41);
    const compressed = gzipSync(huge);
    const payload = 'gz:' + compressed.toString('base64');
    expect(() => decompress(payload)).toThrow();
  });
});

describe('nextCronOccurrence', () => {
  // --- Input validation (from #56) ---

  it('should throw error for invalid step 0', () => {
    const now = Date.now();
    expect(() => nextCronOccurrence('* */0 * * *', now)).toThrow('Invalid cron step: 0');
  });

  it('should throw error for negative step value', () => {
    const now = Date.now();
    expect(() => nextCronOccurrence('*/-1 * * * *', now)).toThrow();
  });

  it('should throw error for out of bounds range', () => {
    const now = Date.now();
    expect(() => nextCronOccurrence('0-60 * * * *', now)).toThrow('Cron range out of bounds: 0-60');
  });

  it('should throw error for out of bounds value', () => {
    const now = Date.now();
    expect(() => nextCronOccurrence('60 * * * *', now)).toThrow('Cron value out of bounds: 60');
  });

  it('should throw error for reversed in-bounds range', () => {
    const now = Date.now();
    expect(() => nextCronOccurrence('10-5 * * * *', now)).toThrow('Cron range reversed: 10-5');
  });

  it('should throw error for malformed numeric token', () => {
    const now = Date.now();
    expect(() => nextCronOccurrence('5foo * * * *', now)).toThrow('Invalid cron token: 5foo');
  });

  // --- Algorithmic correctness (from #59) ---

  it('returns next minute for * * * * *', () => {
    const now = new Date('2024-01-01T12:00:00Z').getTime();
    const next = nextCronOccurrence('* * * * *', now);
    expect(new Date(next).toISOString()).toBe('2024-01-01T12:01:00.000Z');
  });

  it('returns specific minute for 5 * * * *', () => {
    const now = new Date('2024-01-01T12:00:00Z').getTime();
    const next = nextCronOccurrence('5 * * * *', now);
    expect(new Date(next).toISOString()).toBe('2024-01-01T12:05:00.000Z');
  });

  it('wraps to next hour for 5 * * * * if past', () => {
    const now = new Date('2024-01-01T12:06:00Z').getTime();
    const next = nextCronOccurrence('5 * * * *', now);
    expect(new Date(next).toISOString()).toBe('2024-01-01T13:05:00.000Z');
  });

  it('wraps to next day for 0 0 * * *', () => {
    const now = new Date('2024-01-01T12:00:00Z').getTime();
    const next = nextCronOccurrence('0 0 * * *', now);
    expect(new Date(next).toISOString()).toBe('2024-01-02T00:00:00.000Z');
  });

  it('wraps to next month for 0 0 1 * *', () => {
    const now = new Date('2024-01-02T00:00:00Z').getTime();
    const next = nextCronOccurrence('0 0 1 * *', now);
    expect(new Date(next).toISOString()).toBe('2024-02-01T00:00:00.000Z');
  });

  it('wraps to next year for 0 0 1 1 *', () => {
    const now = new Date('2024-01-02T00:00:00Z').getTime();
    const next = nextCronOccurrence('0 0 1 1 *', now);
    expect(new Date(next).toISOString()).toBe('2025-01-01T00:00:00.000Z');
  });

  it('handles leap year (Feb 29)', () => {
    const now = new Date('2024-01-01T00:00:00Z').getTime();
    const next = nextCronOccurrence('0 0 29 2 *', now);
    expect(new Date(next).toISOString()).toBe('2024-02-29T00:00:00.000Z');
  });

  it('skips non-leap year for Feb 29', () => {
    const now = new Date('2025-01-01T00:00:00Z').getTime();
    const next = nextCronOccurrence('0 0 29 2 *', now);
    expect(new Date(next).toISOString()).toBe('2028-02-29T00:00:00.000Z');
  });

  it('handles century non-leap-year gap for Feb 29', () => {
    // 2100 is NOT a leap year (divisible by 100 but not 400)
    // Next Feb 29 after 2097 is 2104
    const now = new Date('2097-03-01T00:00:00Z').getTime();
    const next = nextCronOccurrence('0 0 29 2 *', now);
    expect(new Date(next).toISOString()).toBe('2104-02-29T00:00:00.000Z');
  });

  it('matches day of week (Monday)', () => {
    // 2024-01-01 is Monday
    const now = new Date('2024-01-01T12:00:00Z').getTime();
    const next = nextCronOccurrence('0 0 * * 1', now);
    expect(new Date(next).toISOString()).toBe('2024-01-08T00:00:00.000Z');
  });

  it('matches specific date AND day of week', () => {
    // 1st of month AND Monday - Apr 1 2024 is the next match
    const now = new Date('2024-01-01T12:00:00Z').getTime();
    const next = nextCronOccurrence('0 0 1 * 1', now);
    expect(new Date(next).toISOString()).toBe('2024-04-01T00:00:00.000Z');
  });

  it('throws error for impossible date (Feb 30)', () => {
    const now = new Date('2024-01-01T00:00:00Z').getTime();
    expect(() => nextCronOccurrence('0 0 30 2 *', now)).toThrow();
  });

  it('handles complex pattern (minute 30 past every hour)', () => {
    const now = new Date('2024-01-01T10:00:00Z').getTime();
    const next = nextCronOccurrence('30 * * * *', now);
    expect(new Date(next).toISOString()).toBe('2024-01-01T10:30:00.000Z');
  });
});
