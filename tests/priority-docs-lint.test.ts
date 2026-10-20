import { describe, expect, it } from 'vitest';

const { findViolations } = require('../scripts/check-priority-docs.cjs') as {
  findViolations(text: string): { line: number; reason: string }[];
};

describe('priority documentation lint', () => {
  it.each([
    'Priority: 0 is highest priority.',
    'Non-delayed priority jobs use timestamp 0 so they promote immediately.',
    'Priority jobs are always consumed first.',
    'Priority 0 runs after prioritized jobs.',
    'The priority job starts no later than the first completion.',
    'A high-priority job promoted later runs after an earlier job.',
    'Priority: lower number = higher priority (default: 0)',
    "await queue.add('urgent', data, { priority: 0 });",
    'Low priority (processed last)',
    'glide-mq runs them after any waiting job with priority > 0.',
  ])('rejects the incorrect claim: %s', (text) => {
    expect(findViolations(text)).toHaveLength(1);
  });

  it.each([
    'Non-delayed priority jobs do not promote immediately; scheduler promotion is required.',
    'Priority 0 means no priority and does not always run after prioritized jobs.',
  ])('permits correct negated guidance: %s', (text) => {
    expect(findViolations(text)).toEqual([]);
  });

  it('preserves line numbers and permits qualified retrieval guidance', () => {
    expect(findViolations('Promotion is scheduler-driven.\nPriority: 0 is highest.')[0].line).toBe(2);
    expect(
      findViolations('0 means no priority. Promoted work is checked first; admission gates can defer execution.'),
    ).toEqual([]);
    expect(findViolations('BullMQ: 0 is highest priority.')).toEqual([]);
    expect(findViolations('glide-mq differs from BullMQ: 0 is highest priority.')).toHaveLength(1);
  });
});
