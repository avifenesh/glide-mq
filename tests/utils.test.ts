
import { describe, it, expect } from 'vitest';
import { nextCronOccurrence } from '../src/utils';

describe('utils', () => {
  describe('nextCronOccurrence', () => {
    it('should calculate next occurrence for simple cron', () => {
      // 2024-01-01 00:00:00 UTC
      // 1704067200000
      const now = 1704067200000;
      // * * * * * -> next minute (timestamp + 60s)
      // nextCronOccurrence starts searching from next minute.
      // If we are exactly at 00:00:00, it starts checking 00:01:00.
      // So it returns 00:01:00.
      const next = nextCronOccurrence('* * * * *', now);
      expect(next).toBe(now + 60000);
    });

    it('should throw error for invalid step 0', () => {
      const now = Date.now();
      expect(() => nextCronOccurrence('* */0 * * *', now)).toThrow('Invalid cron step: 0');
    });

    it('should throw error for out of bounds range', () => {
      const now = Date.now();
      // Minutes 0-59
      expect(() => nextCronOccurrence('0-60 * * * *', now)).toThrow('Cron range out of bounds: 0-60');
    });

    it('should throw error for out of bounds value', () => {
      const now = Date.now();
      expect(() => nextCronOccurrence('60 * * * *', now)).toThrow('Cron value out of bounds: 60');
    });

    it('should handle complex patterns correctly', () => {
      const now = new Date('2024-01-01T10:00:00Z').getTime();
      // At minute 30 past every hour
      const next = nextCronOccurrence('30 * * * *', now);
      expect(next).toBe(new Date('2024-01-01T10:30:00Z').getTime());
    });
  });
});
