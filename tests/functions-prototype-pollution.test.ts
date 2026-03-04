import { describe, expect, it, vi } from 'vitest';
import type { Client } from '../src/types';
import { CONSUMER_GROUP, completeAndFetchNext, moveToActive } from '../src/functions/index';
import { buildKeys } from '../src/utils';

function makeMockClient(rawResult: string): Client {
  return {
    fcall: vi.fn().mockResolvedValue(rawResult),
  } as unknown as Client;
}

describe('function hash parsing hardening', () => {
  const keys = buildKeys('prototype-pollution');

  it('completeAndFetchNext builds a null-prototype hash for external keys', async () => {
    const client = makeMockClient(
      JSON.stringify({
        completed: 'job-1',
        next: ['__proto__', 'polluted', 'safe', 'value'],
        nextJobId: 'job-2',
        nextEntryId: '1-0',
      }),
    );

    const result = await completeAndFetchNext(
      client,
      keys,
      'job-1',
      '0-1',
      '"ok"',
      Date.now(),
      CONSUMER_GROUP,
      'worker-1',
    );

    expect(result.next).not.toBe(false);
    expect(result.next).not.toBe('REVOKED');
    const hash = result.next as Record<string, string>;
    expect(Object.getPrototypeOf(hash)).toBeNull();
    expect(Object.prototype.hasOwnProperty.call(hash, '__proto__')).toBe(true);
    expect(hash['__proto__']).toBe('polluted');
    expect(hash.safe).toBe('value');
    expect(({} as { polluted?: string }).polluted).toBeUndefined();
  });

  it('moveToActive builds a null-prototype hash for external keys', async () => {
    const client = makeMockClient(JSON.stringify(['__proto__', 'polluted', 'state', 'active']));

    const result = await moveToActive(client, keys, 'job-2', Date.now());
    expect(result).not.toBeNull();
    expect(result).not.toBe('REVOKED');
    expect(result).not.toBe('GROUP_FULL');
    expect(result).not.toBe('GROUP_RATE_LIMITED');
    expect(result).not.toBe('GROUP_TOKEN_LIMITED');
    expect(result).not.toBe('ERR:COST_EXCEEDS_CAPACITY');

    const hash = result as Record<string, string>;
    expect(Object.getPrototypeOf(hash)).toBeNull();
    expect(Object.prototype.hasOwnProperty.call(hash, '__proto__')).toBe(true);
    expect(hash['__proto__']).toBe('polluted');
    expect(hash.state).toBe('active');
    expect(({} as { polluted?: string }).polluted).toBeUndefined();
  });
});
