import { describe, it, expect } from 'vitest';
import { gzipSync } from 'zlib';
import {
  nextCronOccurrence,
  validateTimezone,
  decompress,
  MAX_JOB_DATA_SIZE,
  hmgetArrayToRecord,
  hashDataToRecord,
  JOB_METADATA_FIELDS,
  encodeScore,
  validateQueueName,
  sanitizeForLog,
} from '../src/utils';

describe('hashDataToRecord', () => {
  it('treats a non-array batch result (per-command error) as missing', () => {
    expect(hashDataToRecord(new Error('WRONGTYPE') as any)).toBeNull();
    expect(hashDataToRecord({} as any)).toBeNull();
    expect(hashDataToRecord([{ key: 'name', value: 'x' }])).toEqual({ name: 'x' });
  });
});

describe('sanitizeForLog', () => {
  it('removes control characters that could forge a log line', () => {
    expect(sanitizeForLog('queue\n[ERROR] forged\r\t')).toBe('queue[ERROR] forged');
    expect(sanitizeForLog('queue\u2028[ERROR]\u2029 forged\u0085')).toBe('queue[ERROR] forged');
  });

  it('caps caller-supplied text', () => {
    expect(sanitizeForLog('abcdef', 4)).toBe('abcd…');
  });
});

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

  it('matches day of month OR day of week when both are restricted', () => {
    // Standard cron: '0 0 1 * 1' fires on every 1st AND on every Monday
    let t = new Date('2024-01-22T12:00:00Z').getTime();
    const runs: string[] = [];
    for (let i = 0; i < 4; i++) {
      t = nextCronOccurrence('0 0 1 * 1', t);
      runs.push(new Date(t).toISOString().slice(0, 10));
    }
    // Jan 29 Mon, Feb 1 Thu, Feb 5 Mon, Feb 12 Mon
    expect(runs).toEqual(['2024-01-29', '2024-02-01', '2024-02-05', '2024-02-12']);
  });

  it('treats * and */1 as unrestricted day fields (AND with the other field)', () => {
    const now = new Date('2024-01-01T12:00:00Z').getTime();
    // dom '*/1' is unrestricted: only Mondays match
    expect(new Date(nextCronOccurrence('0 0 */1 * 1', now)).toISOString()).toBe('2024-01-08T00:00:00.000Z');
    // dow '*/1' is unrestricted: only the 5th matches
    expect(new Date(nextCronOccurrence('0 0 5 * */1', now)).toISOString()).toBe('2024-01-05T00:00:00.000Z');
    // dom '*/2' is restricted: odd days OR Mondays
    expect(new Date(nextCronOccurrence('0 0 */2 * 1', now)).toISOString()).toBe('2024-01-03T00:00:00.000Z');
    // dow '0-6' is restricted (cron-parser and vixie): the 5th OR any weekday, so every day
    expect(new Date(nextCronOccurrence('0 0 5 * 0-6', now)).toISOString()).toBe('2024-01-02T00:00:00.000Z');
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

  it('keeps interval scheduler nextRun anchored when scheduler ticks late', async () => {
    const { computeFollowingSchedulerNextRun } = await import('../src/utils');
    const every = 1_000;

    const secondRun = computeFollowingSchedulerNextRun({ every, nextRun: 1_000 }, 1_001);
    const thirdRun = computeFollowingSchedulerNextRun({ every, nextRun: secondRun! }, 2_001);
    const fourthRun = computeFollowingSchedulerNextRun({ every, nextRun: thirdRun! }, 3_001);
    const afterPause = computeFollowingSchedulerNextRun({ every, nextRun: 1_000 }, 4_501);

    expect([secondRun, thirdRun, fourthRun]).toEqual([2_000, 3_000, 4_000]);
    expect(afterPause).toBe(5_000);
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

  it('spring-forward: fixed time inside the gap fires at the first instant after it', () => {
    // In America/New_York, 2024-03-10: clocks spring forward 2:00 AM -> 3:00 AM
    // "30 2 * * *" = 2:30 AM does not exist on March 10; like vixie cron it runs at 3:00 AM EDT = 07:00 UTC
    const now = new Date('2024-03-10T06:00:00Z').getTime(); // 1:00 AM EST
    const next = nextCronOccurrence('30 2 * * *', now, 'America/New_York');
    expect(new Date(next).toISOString()).toBe('2024-03-10T07:00:00.000Z');
    // The day after is back to 2:30 AM EDT = 06:30 UTC
    expect(new Date(nextCronOccurrence('30 2 * * *', next, 'America/New_York')).toISOString()).toBe(
      '2024-03-11T06:30:00.000Z',
    );
  });

  it('spring-forward: several fixed times inside the gap coalesce into one run', () => {
    const now = new Date('2024-03-10T06:00:00Z').getTime();
    const first = nextCronOccurrence('0,30 2 * * *', now, 'America/New_York');
    expect(new Date(first).toISOString()).toBe('2024-03-10T07:00:00.000Z');
    expect(new Date(nextCronOccurrence('0,30 2 * * *', first, 'America/New_York')).toISOString()).toBe(
      '2024-03-11T06:00:00.000Z',
    );
  });

  it('spring-forward: fixed time in a 30-minute gap (Australia/Lord_Howe)', () => {
    // 2026-10-04 02:00 LHST (+10:30) -> 02:30 LHDT (+11); 02:15 does not exist
    const now = new Date('2026-10-03T15:00:00Z').getTime(); // 01:30 LHST
    const next = nextCronOccurrence('15 2 * * *', now, 'Australia/Lord_Howe');
    expect(new Date(next).toISOString()).toBe('2026-10-03T15:30:00.000Z'); // 02:30 LHDT
  });

  it('spring-forward: wildcard pattern skips the missing times', () => {
    const now = new Date('2024-03-10T06:30:00Z').getTime(); // 1:30 AM EST
    expect(walkCron('*/30 * * * *', '2024-03-10T06:30:00Z', 3, 'America/New_York')).toEqual([
      '2024-03-10T07:00:00.000Z', // 3:00 AM EDT
      '2024-03-10T07:30:00.000Z',
      '2024-03-10T08:00:00.000Z',
    ]);
    expect(nextCronOccurrence('*/30 * * * *', now, 'America/New_York')).toBe(
      new Date('2024-03-10T07:00:00Z').getTime(),
    );
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

  function walkCron(pattern: string, fromIso: string, count: number, tz: string): string[] {
    let t = new Date(fromIso).getTime();
    const out: string[] = [];
    for (let i = 0; i < count; i++) {
      t = nextCronOccurrence(pattern, t, tz);
      out.push(new Date(t).toISOString());
    }
    return out;
  }

  it('fall-back: sub-hourly wildcard pattern fires in both instances of the repeated hour', () => {
    // 2026-11-01 America/New_York: 01:00-01:59 happens twice (EDT 05:xx UTC, then EST 06:xx UTC)
    expect(walkCron('*/15 * * * *', '2026-11-01T05:30:00Z', 7, 'America/New_York')).toEqual([
      '2026-11-01T05:45:00.000Z', // 01:45 EDT
      '2026-11-01T06:00:00.000Z', // 01:00 EST
      '2026-11-01T06:15:00.000Z',
      '2026-11-01T06:30:00.000Z',
      '2026-11-01T06:45:00.000Z',
      '2026-11-01T07:00:00.000Z', // 02:00 EST
      '2026-11-01T07:15:00.000Z',
    ]);
  });

  it('fall-back: hourly wildcard pattern fires every elapsed hour', () => {
    expect(walkCron('0 * * * *', '2026-11-01T04:30:00Z', 4, 'America/New_York')).toEqual([
      '2026-11-01T05:00:00.000Z', // 01:00 EDT
      '2026-11-01T06:00:00.000Z', // 01:00 EST
      '2026-11-01T07:00:00.000Z', // 02:00 EST
      '2026-11-01T08:00:00.000Z',
    ]);
  });

  it('fall-back: fixed-time pattern fires once, including when resumed inside the repeated hour', () => {
    // After the 01:30 EDT run, the 01:30 EST repeat is not fired again
    expect(walkCron('30 1 * * *', '2026-11-01T04:00:00Z', 2, 'America/New_York')).toEqual([
      '2026-11-01T05:30:00.000Z',
      '2026-11-02T06:30:00.000Z',
    ]);
    // Searching from 01:10 EST (second instance) goes to the next day
    expect(walkCron('30 1 * * *', '2026-11-01T06:10:00Z', 1, 'America/New_York')).toEqual(['2026-11-02T06:30:00.000Z']);
  });

  it('fall-back: ambiguous fixed time resolves to the earlier instant in positive-offset zones', () => {
    // 2026-10-25 Europe/Berlin: 02:30 CEST = 00:30 UTC, 02:30 CET = 01:30 UTC
    expect(walkCron('30 2 * * *', '2026-10-25T00:00:00Z', 1, 'Europe/Berlin')).toEqual(['2026-10-25T00:30:00.000Z']);
    // 2026-04-05 Australia/Sydney: 02:30 AEDT = 15:30 UTC (Apr 4), 02:30 AEST = 16:30 UTC
    expect(walkCron('30 2 * * *', '2026-04-04T14:00:00Z', 2, 'Australia/Sydney')).toEqual([
      '2026-04-04T15:30:00.000Z',
      '2026-04-05T16:30:00.000Z',
    ]);
  });

  it('fall-back: wildcard pattern in a positive-offset zone keeps a steady cadence', () => {
    expect(walkCron('*/30 * * * *', '2026-10-24T23:00:00Z', 6, 'Europe/Berlin')).toEqual([
      '2026-10-24T23:30:00.000Z',
      '2026-10-25T00:00:00.000Z', // 02:00 CEST
      '2026-10-25T00:30:00.000Z',
      '2026-10-25T01:00:00.000Z', // 02:00 CET
      '2026-10-25T01:30:00.000Z',
      '2026-10-25T02:00:00.000Z',
    ]);
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

describe('nextCronOccurrence extended syntax', () => {
  function walk(pattern: string, fromIso: string, count: number, tz?: string): string[] {
    let t = new Date(fromIso).getTime();
    const out: string[] = [];
    for (let i = 0; i < count; i++) {
      t = nextCronOccurrence(pattern, t, tz);
      out.push(new Date(t).toISOString());
    }
    return out;
  }

  describe('names', () => {
    it('accepts weekday names, case-insensitive, in ranges', () => {
      expect(walk('0 9 * * MON-FRI', '2024-01-13T00:00:00Z', 1)).toEqual(['2024-01-15T09:00:00.000Z']);
      expect(walk('0 9 * * mon-fri', '2024-01-13T00:00:00Z', 1)).toEqual(['2024-01-15T09:00:00.000Z']);
      expect(walk('0 0 * * Sun', '2024-01-15T00:00:00Z', 1)).toEqual(['2024-01-21T00:00:00.000Z']);
    });

    it('accepts weekday names in lists', () => {
      expect(walk('0 0 * * SUN,SAT', '2024-01-15T00:00:00Z', 2)).toEqual([
        '2024-01-20T00:00:00.000Z',
        '2024-01-21T00:00:00.000Z',
      ]);
    });

    it('accepts month names in values, ranges and steps', () => {
      expect(walk('0 0 1 DEC *', '2024-01-01T00:00:00Z', 1)).toEqual(['2024-12-01T00:00:00.000Z']);
      expect(walk('0 9 1 JAN-MAR/2 *', '2024-02-10T00:00:00Z', 2)).toEqual([
        '2024-03-01T09:00:00.000Z',
        '2025-01-01T09:00:00.000Z',
      ]);
      expect(walk('0 0 1 jan,jul *', '2024-02-10T00:00:00Z', 1)).toEqual(['2024-07-01T00:00:00.000Z']);
    });

    it('rejects unknown names and names in the wrong field', () => {
      const now = Date.now();
      expect(() => nextCronOccurrence('0 0 * * FOO', now)).toThrow('Invalid cron token: FOO');
      expect(() => nextCronOccurrence('0 0 * MON *', now)).toThrow('Invalid cron token: MON');
      expect(() => nextCronOccurrence('0 JAN * * *', now)).toThrow('Invalid cron token: JAN');
    });
  });

  describe('day-of-week 7', () => {
    it('treats 7 as Sunday', () => {
      expect(walk('0 0 * * 7', '2024-01-15T00:00:00Z', 1)).toEqual(['2024-01-21T00:00:00.000Z']);
      expect(walk('0 0 * * 5-7', '2024-01-15T00:00:00Z', 4)).toEqual([
        '2024-01-19T00:00:00.000Z',
        '2024-01-20T00:00:00.000Z',
        '2024-01-21T00:00:00.000Z',
        '2024-01-26T00:00:00.000Z',
      ]);
      expect(walk('0 0 * * 1-7/3', '2024-01-15T00:00:00Z', 3)).toEqual([
        '2024-01-18T00:00:00.000Z',
        '2024-01-21T00:00:00.000Z',
        '2024-01-22T00:00:00.000Z',
      ]);
    });

    it('still rejects 8', () => {
      expect(() => nextCronOccurrence('0 0 * * 8', Date.now())).toThrow('Cron value out of bounds: 8');
      expect(() => nextCronOccurrence('0 0 * * 5-8', Date.now())).toThrow('Cron range out of bounds: 5-8');
    });
  });

  describe('? in day fields', () => {
    it('is an unrestricted day field', () => {
      expect(walk('0 0 ? * 1', '2024-01-13T00:00:00Z', 1)).toEqual(['2024-01-15T00:00:00.000Z']);
      expect(walk('0 0 15 * ?', '2024-01-13T00:00:00Z', 2)).toEqual([
        '2024-01-15T00:00:00.000Z',
        '2024-02-15T00:00:00.000Z',
      ]);
      expect(walk('0 0 ? * ?', '2024-01-13T00:00:00Z', 1)).toEqual(['2024-01-14T00:00:00.000Z']);
    });

    it('is rejected outside the day fields', () => {
      expect(() => nextCronOccurrence('? 0 * * *', Date.now())).toThrow('Invalid cron token: ?');
      expect(() => nextCronOccurrence('0 ? * * *', Date.now())).toThrow('Invalid cron token: ?');
      expect(() => nextCronOccurrence('0 0 * ? *', Date.now())).toThrow('Invalid cron token: ?');
    });
  });

  describe('start/step', () => {
    it('reads N/step as N-max/step', () => {
      expect(walk('5/15 * * * *', '2024-01-01T12:00:00Z', 5)).toEqual([
        '2024-01-01T12:05:00.000Z',
        '2024-01-01T12:20:00.000Z',
        '2024-01-01T12:35:00.000Z',
        '2024-01-01T12:50:00.000Z',
        '2024-01-01T13:05:00.000Z',
      ]);
      expect(walk('0 9 5/10 * *', '2024-01-13T00:00:00Z', 2)).toEqual([
        '2024-01-15T09:00:00.000Z',
        '2024-01-25T09:00:00.000Z',
      ]);
    });

    it('rejects malformed steps', () => {
      expect(() => nextCronOccurrence('5/ * * * *', Date.now())).toThrow('Invalid cron token: 5/');
      expect(() => nextCronOccurrence('*/5/2 * * * *', Date.now())).toThrow('Invalid cron token: */5/2');
      expect(() => nextCronOccurrence('1-5-9 * * * *', Date.now())).toThrow('Invalid cron token: 1-5-9');
    });
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

describe('encodeScore (T3)', () => {
  it('throws for priority > 2048', () => {
    expect(() => encodeScore(2049, Date.now())).toThrow('Priority must be <= 2048');
    expect(() => encodeScore(9999, Date.now())).toThrow('Priority must be <= 2048');
  });

  it('throws for negative or fractional priority', () => {
    expect(() => encodeScore(-1, Date.now())).toThrow('Priority must be an integer between 0 and 2048');
    expect(() => encodeScore(1.5, Date.now())).toThrow('Priority must be an integer between 0 and 2048');
  });

  it('accepts priority <= 2048', () => {
    expect(() => encodeScore(0, Date.now())).not.toThrow();
    expect(() => encodeScore(1024, Date.now())).not.toThrow();
    expect(() => encodeScore(2048, Date.now())).not.toThrow();
  });

  it('returns correct score for priority 0 (just the timestamp)', () => {
    const ts = 1000000;
    expect(encodeScore(0, ts)).toBe(ts);
  });
});

describe('validateQueueName (T3)', () => {
  it('accepts valid queue names', () => {
    expect(() => validateQueueName('my-queue')).not.toThrow();
    expect(() => validateQueueName('queue_1')).not.toThrow();
    expect(() => validateQueueName('emailQueue')).not.toThrow();
  });

  it('rejects names with curly braces', () => {
    expect(() => validateQueueName('{queue}')).toThrow();
    expect(() => validateQueueName('queue{name}')).toThrow();
    expect(() => validateQueueName('{bad')).toThrow();
  });

  it('rejects names with colons', () => {
    expect(() => validateQueueName('queue:name')).toThrow();
    expect(() => validateQueueName('a:b')).toThrow();
  });

  it('rejects empty string', () => {
    expect(() => validateQueueName('')).toThrow();
  });
});
