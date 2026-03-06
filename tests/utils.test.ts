import { describe, it, expect } from 'vitest';
import { gzipSync } from 'zlib';
import { nextCronOccurrence, validateTimezone, decompress, MAX_JOB_DATA_SIZE, hmgetArrayToRecord, JOB_METADATA_FIELDS } from '../src/utils';

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

  it('aligns interval schedulers from a past startDate instead of delaying by a full extra interval', async () => {
    const { computeInitialSchedulerNextRun } = await import('../src/utils');
    const startDate = 1_000;
    const now = 1_550;
    const next = computeInitialSchedulerNextRun({ every: 250, startDate }, now);
    expect(next).toBe(1_750);
  });

  it('keeps an interval scheduler immediately eligible when now is exactly on a startDate-aligned slot', async () => {
    const { computeInitialSchedulerNextRun } = await import('../src/utils');
    const startDate = 1_000;
    const now = 1_500;
    const next = computeInitialSchedulerNextRun({ every: 250, startDate, endDate: 1_500 }, now);
    expect(next).toBe(1_500);
  });
});

describe('validateTimezone', () => {
  it('accepts valid IANA timezone', () => {
    expect(() => validateTimezone('America/New_York')).not.toThrow();
    expect(() => validateTimezone('Europe/London')).not.toThrow();
    expect(() => validateTimezone('Asia/Tokyo')).not.toThrow();
    expect(() => validateTimezone('UTC')).not.toThrow();
  });

  it('rejects invalid timezone string', () => {
    expect(() => validateTimezone('Fake/Timezone')).toThrow('Invalid timezone: Fake/Timezone');
    expect(() => validateTimezone('')).toThrow('Invalid timezone');
    expect(() => validateTimezone('Not_A_Zone')).toThrow('Invalid timezone');
  });
});

describe('nextCronOccurrence with timezone', () => {
  // America/New_York is UTC-5 in winter (EST), UTC-4 in summer (EDT)
  // Asia/Tokyo is always UTC+9 (no DST)

  it('evaluates cron in the specified timezone (EST winter)', () => {
    // "0 9 * * *" in America/New_York = 9:00 AM EST = 14:00 UTC
    // afterMs: 2024-01-15T13:00:00Z (8:00 AM in New York - before 9:00 AM)
    const now = new Date('2024-01-15T13:00:00Z').getTime();
    const next = nextCronOccurrence('0 9 * * *', now, 'America/New_York');
    expect(new Date(next).toISOString()).toBe('2024-01-15T14:00:00.000Z');
  });

  it('evaluates cron in the specified timezone (EDT summer)', () => {
    // "0 9 * * *" in America/New_York = 9:00 AM EDT = 13:00 UTC
    // afterMs: 2024-07-15T12:00:00Z (8:00 AM in New York - before 9:00 AM)
    const now = new Date('2024-07-15T12:00:00Z').getTime();
    const next = nextCronOccurrence('0 9 * * *', now, 'America/New_York');
    expect(new Date(next).toISOString()).toBe('2024-07-15T13:00:00.000Z');
  });

  it('evaluates cron in Asia/Tokyo (UTC+9, no DST)', () => {
    // "0 9 * * *" in Asia/Tokyo = 9:00 JST = 00:00 UTC
    // afterMs: 2024-06-01T14:00:00Z (23:00 JST June 1 - before 9:00 AM JST June 2)
    const now = new Date('2024-06-01T14:00:00Z').getTime();
    const next = nextCronOccurrence('0 9 * * *', now, 'Asia/Tokyo');
    expect(new Date(next).toISOString()).toBe('2024-06-02T00:00:00.000Z');
  });

  it('wraps to next day in target timezone', () => {
    // "0 9 * * *" in America/New_York, afterMs when it's already past 9 AM in New York
    // 2024-01-15T15:00:00Z = 10:00 AM EST (already past 9 AM)
    const now = new Date('2024-01-15T15:00:00Z').getTime();
    const next = nextCronOccurrence('0 9 * * *', now, 'America/New_York');
    // Next occurrence is Jan 16 at 9:00 AM EST = 14:00 UTC
    expect(new Date(next).toISOString()).toBe('2024-01-16T14:00:00.000Z');
  });

  it('without tz parameter, cron runs in UTC', () => {
    // "0 9 * * *" without tz = 09:00 UTC
    const now = new Date('2024-01-15T08:00:00Z').getTime();
    const next = nextCronOccurrence('0 9 * * *', now);
    expect(new Date(next).toISOString()).toBe('2024-01-15T09:00:00.000Z');
  });

  // --- DST transitions ---

  it('spring-forward: skips nonexistent wall-clock time', () => {
    // In America/New_York, 2024-03-10: clocks spring forward 2:00 AM -> 3:00 AM
    // "30 2 * * *" = 2:30 AM - does not exist on March 10
    // Next valid occurrence is March 11 at 2:30 AM EDT = 06:30 UTC
    const now = new Date('2024-03-10T06:00:00Z').getTime(); // 1:00 AM EST
    const next = nextCronOccurrence('30 2 * * *', now, 'America/New_York');
    // March 10 2:30 AM doesn't exist, so it should fire March 11 at 2:30 AM EDT
    // March 11 2:30 AM EDT = 06:30 UTC
    expect(new Date(next).toISOString()).toBe('2024-03-11T06:30:00.000Z');
  });

  it('fall-back: picks first (earlier) UTC instant for ambiguous time', () => {
    // In America/New_York, 2024-11-03: clocks fall back 2:00 AM -> 1:00 AM
    // "30 1 * * *" = 1:30 AM - this time occurs twice (EDT and EST)
    // We should pick the first (EDT) occurrence: 1:30 AM EDT = 05:30 UTC
    // afterMs: right before the DST transition
    const now = new Date('2024-11-03T04:00:00Z').getTime(); // midnight EDT
    const next = nextCronOccurrence('30 1 * * *', now, 'America/New_York');
    // 1:30 AM EDT = 05:30 UTC (the earlier of the two possible interpretations)
    expect(new Date(next).toISOString()).toBe('2024-11-03T05:30:00.000Z');
  });

  it('midnight cron in positive-offset timezone', () => {
    // "0 0 * * *" in Asia/Kolkata (UTC+5:30) = previous day 18:30 UTC
    const now = new Date('2024-06-15T17:00:00Z').getTime(); // 22:30 IST
    const next = nextCronOccurrence('0 0 * * *', now, 'Asia/Kolkata');
    // Next midnight IST = June 16 00:00 IST = June 15 18:30 UTC
    expect(new Date(next).toISOString()).toBe('2024-06-15T18:30:00.000Z');
  });

  it('handles day-of-week matching in timezone', () => {
    // "0 9 * * 1" = 9:00 AM on Mondays in America/Chicago (UTC-6 CST / UTC-5 CDT)
    // 2024-01-15 is a Monday
    // afterMs: 2024-01-14T00:00:00Z (Sunday in Chicago)
    const now = new Date('2024-01-14T00:00:00Z').getTime();
    const next = nextCronOccurrence('0 9 * * 1', now, 'America/Chicago');
    // Monday Jan 15 at 9:00 AM CST = 15:00 UTC
    expect(new Date(next).toISOString()).toBe('2024-01-15T15:00:00.000Z');
  });

  it('validates invalid timezone via nextCronOccurrence dispatch', () => {
    const now = Date.now();
    // nextCronOccurrenceTz uses getFormatter which calls Intl.DateTimeFormat
    // An invalid tz string will throw
    expect(() => nextCronOccurrence('0 9 * * *', now, 'Invalid/Zone')).toThrow();
  });
});

describe('hmgetArrayToRecord', () => {
  it('converts an array of values to a Record keyed by field names', () => {
    const fields = ['a', 'b', 'c'];
    const values = ['1', '2', '3'];
    const result = hmgetArrayToRecord(values, fields);
    expect(result).toEqual({ a: '1', b: '2', c: '3' });
  });

  it('skips null values', () => {
    const fields = ['a', 'b', 'c'];
    const values = ['1', null, '3'];
    const result = hmgetArrayToRecord(values, fields);
    expect(result).toEqual({ a: '1', c: '3' });
  });

  it('returns null when all values are null', () => {
    const fields = ['a', 'b'];
    const values = [null, null];
    const result = hmgetArrayToRecord(values, fields);
    expect(result).toBeNull();
  });

  it('converts non-string values to strings', () => {
    const fields = ['num', 'buf'];
    const values = [42, Buffer.from('hello')];
    const result = hmgetArrayToRecord(values, fields);
    expect(result).not.toBeNull();
    expect(result!.num).toBe('42');
    expect(typeof result!.buf).toBe('string');
  });
});

describe('JOB_METADATA_FIELDS', () => {
  it('does not include data or returnvalue', () => {
    expect(JOB_METADATA_FIELDS).not.toContain('data');
    expect(JOB_METADATA_FIELDS).not.toContain('returnvalue');
  });

  it('includes essential metadata fields', () => {
    for (const field of ['id', 'name', 'opts', 'timestamp', 'attemptsMade', 'state']) {
      expect(JOB_METADATA_FIELDS).toContain(field);
    }
  });
});
