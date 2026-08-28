/**
 * Unit coverage for workflow helpers: auto-close owned clients,
 * keep injected clients open. No Valkey required.
 *
 * Run: npx vitest run tests/workflows.test.ts
 */
import { beforeEach, describe, expect, it, vi } from 'vitest';

const add = vi.fn();
const addDAG = vi.fn();
const close = vi.fn(async () => undefined);

vi.mock('../src/flow-producer', () => ({
  FlowProducer: class {
    add = add;
    addDAG = addDAG;
    close = close;
  },
}));

import { chain, chord, dag, group } from '../src/workflows';

const CONN = { addresses: [{ host: 'localhost', port: 6379 }] };
const NODE = { job: { id: '1' } };

describe('workflow helpers client ownership', () => {
  beforeEach(() => {
    add.mockReset();
    addDAG.mockReset();
    close.mockReset();
    close.mockResolvedValue(undefined);
  });

  it('chain auto-closes an owned producer on success', async () => {
    add.mockResolvedValue(NODE);
    const node = await chain('q', [{ name: 'only', data: {} }], CONN);
    expect(close).toHaveBeenCalledTimes(1);
    await node.close();
    expect(close).toHaveBeenCalledTimes(1);
  });

  it('chain keeps a shared client open so returned jobs stay usable', async () => {
    add.mockResolvedValue(NODE);
    const client = { close: vi.fn() } as any;
    const node = await chain('q', [{ name: 'only', data: {} }], { ...CONN, client });
    expect(close).not.toHaveBeenCalled();
    await node.close();
    expect(close).toHaveBeenCalledTimes(1);
  });

  it('chain closes the producer when add fails', async () => {
    add.mockRejectedValue(new Error('boom'));
    await expect(chain('q', [{ name: 'only', data: {} }], CONN)).rejects.toThrow('boom');
    expect(close).toHaveBeenCalledTimes(1);
  });

  it('group/chord/dag keep the client on success and close it on failure', async () => {
    add.mockResolvedValue(NODE);
    addDAG.mockResolvedValue(new Map());
    const client = { close: vi.fn() } as any;
    const shared = { ...CONN, client };

    const g = await group('q', [{ name: 'c', data: {} }], shared);
    const c = await chord('q', [{ name: 'm', data: {} }], { name: 'cb', data: {} }, shared);
    const d = await dag([{ name: 'A', queueName: 'q', data: {} }], shared);
    expect(close).not.toHaveBeenCalled();
    await g.close();
    await c.close();
    await d.close();
    expect(close).toHaveBeenCalledTimes(3);

    add.mockRejectedValue(new Error('g'));
    await expect(group('q', [{ name: 'c', data: {} }], CONN)).rejects.toThrow('g');
    add.mockRejectedValue(new Error('c'));
    await expect(chord('q', [{ name: 'm', data: {} }], { name: 'cb', data: {} }, CONN)).rejects.toThrow('c');
    addDAG.mockRejectedValue(new Error('d'));
    await expect(dag([{ name: 'A', queueName: 'q', data: {} }], CONN)).rejects.toThrow('d');
    expect(close).toHaveBeenCalledTimes(6);
  });
});
