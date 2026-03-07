/**
 * Unit tests for DAG cycle detection and topological sort.
 * No Valkey connection needed.
 */
import { describe, it, expect } from 'vitest';

const { validateDAG, topoSort, CycleError } = require('../dist/dag-utils') as typeof import('../src/dag-utils');

describe('validateDAG', () => {
  it('accepts a valid DAG with no cycles', () => {
    expect(() =>
      validateDAG([
        { name: 'A', queueName: 'q', data: {} },
        { name: 'B', queueName: 'q', data: {}, deps: ['A'] },
        { name: 'C', queueName: 'q', data: {}, deps: ['A'] },
        { name: 'D', queueName: 'q', data: {}, deps: ['B', 'C'] },
      ]),
    ).not.toThrow();
  });

  it('accepts a single node with no deps', () => {
    expect(() => validateDAG([{ name: 'X', queueName: 'q', data: {} }])).not.toThrow();
  });

  it('accepts nodes with no deps array', () => {
    expect(() =>
      validateDAG([
        { name: 'A', queueName: 'q', data: {} },
        { name: 'B', queueName: 'q', data: {} },
      ]),
    ).not.toThrow();
  });

  it('throws CycleError for direct cycle A -> B -> A', () => {
    expect(() =>
      validateDAG([
        { name: 'A', queueName: 'q', data: {}, deps: ['B'] },
        { name: 'B', queueName: 'q', data: {}, deps: ['A'] },
      ]),
    ).toThrow(CycleError);
  });

  it('throws CycleError for self-edge', () => {
    expect(() => validateDAG([{ name: 'A', queueName: 'q', data: {}, deps: ['A'] }])).toThrow(CycleError);
  });

  it('throws CycleError for indirect cycle A -> B -> C -> A', () => {
    expect(() =>
      validateDAG([
        { name: 'A', queueName: 'q', data: {}, deps: ['C'] },
        { name: 'B', queueName: 'q', data: {}, deps: ['A'] },
        { name: 'C', queueName: 'q', data: {}, deps: ['B'] },
      ]),
    ).toThrow(CycleError);
  });

  it('throws Error for missing dependency reference', () => {
    expect(() => validateDAG([{ name: 'A', queueName: 'q', data: {}, deps: ['nonexistent'] }])).toThrow(/unknown node/);
  });

  it('throws Error for duplicate node names', () => {
    expect(() =>
      validateDAG([
        { name: 'A', queueName: 'q', data: {} },
        { name: 'A', queueName: 'q', data: {} },
      ]),
    ).toThrow(/Duplicate node name/);
  });

  it('handles a large DAG (1000 nodes, linear chain) without error', () => {
    const nodes = [];
    for (let i = 0; i < 1000; i++) {
      nodes.push({
        name: `node-${i}`,
        queueName: 'q',
        data: {},
        deps: i > 0 ? [`node-${i - 1}`] : [],
      });
    }
    expect(() => validateDAG(nodes)).not.toThrow();
  });
});

describe('topoSort', () => {
  it('returns nodes in topological order (leaves first)', () => {
    const sorted = topoSort([
      { name: 'D', queueName: 'q', data: {}, deps: ['B', 'C'] },
      { name: 'B', queueName: 'q', data: {}, deps: ['A'] },
      { name: 'C', queueName: 'q', data: {}, deps: ['A'] },
      { name: 'A', queueName: 'q', data: {} },
    ]);

    const names = sorted.map((n: any) => n.name);
    // A must come before B and C; B and C must come before D
    expect(names.indexOf('A')).toBeLessThan(names.indexOf('B'));
    expect(names.indexOf('A')).toBeLessThan(names.indexOf('C'));
    expect(names.indexOf('B')).toBeLessThan(names.indexOf('D'));
    expect(names.indexOf('C')).toBeLessThan(names.indexOf('D'));
  });

  it('returns correct order for linear chain', () => {
    const sorted = topoSort([
      { name: 'C', queueName: 'q', data: {}, deps: ['B'] },
      { name: 'A', queueName: 'q', data: {} },
      { name: 'B', queueName: 'q', data: {}, deps: ['A'] },
    ]);

    const names = sorted.map((n: any) => n.name);
    expect(names).toEqual(['A', 'B', 'C']);
  });

  it('returns all nodes for independent nodes', () => {
    const sorted = topoSort([
      { name: 'X', queueName: 'q', data: {} },
      { name: 'Y', queueName: 'q', data: {} },
      { name: 'Z', queueName: 'q', data: {} },
    ]);
    expect(sorted).toHaveLength(3);
  });

  it('throws CycleError for cycle', () => {
    expect(() =>
      topoSort([
        { name: 'A', queueName: 'q', data: {}, deps: ['B'] },
        { name: 'B', queueName: 'q', data: {}, deps: ['A'] },
      ]),
    ).toThrow(CycleError);
  });

  it('handles fan-in DAG correctly', () => {
    // A, B, C all flow into D
    const sorted = topoSort([
      { name: 'D', queueName: 'q', data: {}, deps: ['A', 'B', 'C'] },
      { name: 'A', queueName: 'q', data: {} },
      { name: 'B', queueName: 'q', data: {} },
      { name: 'C', queueName: 'q', data: {} },
    ]);

    const names = sorted.map((n: any) => n.name);
    expect(names.indexOf('D')).toBe(3); // D must be last
  });
});
