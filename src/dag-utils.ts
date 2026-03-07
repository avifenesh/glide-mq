/**
 * DAG (Directed Acyclic Graph) utilities for multi-parent dependency flows.
 * Provides cycle detection and topological sort using Kahn's algorithm.
 */

import type { DAGNode } from './types';

export class CycleError extends Error {
  readonly cycle: string[];

  constructor(cycle: string[]) {
    super(`DAG contains a cycle: ${cycle.join(' -> ')}`);
    this.name = 'CycleError';
    this.cycle = cycle;
  }
}

/**
 * Build the dependency graph for a set of DAG nodes.
 * Validates node names and dependencies.
 * Returns { nodeMap, inDegree, adjacency }.
 */
function buildGraph(nodes: DAGNode[]): {
  nodeMap: Map<string, DAGNode>;
  inDegree: Map<string, number>;
  adjacency: Map<string, string[]>;
} {
  const nodeMap = new Map<string, DAGNode>();
  for (const node of nodes) {
    if (nodeMap.has(node.name)) {
      throw new Error(`Duplicate node name: ${node.name}`);
    }
    nodeMap.set(node.name, node);
  }

  // Validate all dependencies reference existing nodes
  for (const node of nodes) {
    if (node.deps) {
      for (const dep of node.deps) {
        if (!nodeMap.has(dep)) {
          throw new Error(`Node "${node.name}" depends on unknown node "${dep}"`);
        }
        if (dep === node.name) {
          throw new CycleError([node.name, node.name]);
        }
      }
    }
  }

  const inDegree = new Map<string, number>();
  const adjacency = new Map<string, string[]>();

  for (const node of nodes) {
    inDegree.set(node.name, 0);
    adjacency.set(node.name, []);
  }

  // Build graph: edge from dep -> node (dep must complete before node)
  for (const node of nodes) {
    if (node.deps) {
      for (const dep of node.deps) {
        adjacency.get(dep)!.push(node.name);
        inDegree.set(node.name, inDegree.get(node.name)! + 1);
      }
    }
  }

  return { nodeMap, inDegree, adjacency };
}

/**
 * Validate that a set of DAG nodes forms a valid DAG (no cycles).
 * Throws CycleError if a cycle is detected.
 * Throws Error if a node references a non-existent dependency.
 */
export function validateDAG(nodes: DAGNode[]): void {
  const { inDegree, adjacency } = buildGraph(nodes);

  // Kahn's algorithm for cycle detection
  const queue: string[] = [];
  for (const [name, degree] of inDegree) {
    if (degree === 0) {
      queue.push(name);
    }
  }

  let processed = 0;
  while (queue.length > 0) {
    const current = queue.shift()!;
    processed++;
    for (const neighbor of adjacency.get(current)!) {
      const newDegree = inDegree.get(neighbor)! - 1;
      inDegree.set(neighbor, newDegree);
      if (newDegree === 0) {
        queue.push(neighbor);
      }
    }
  }

  if (processed < nodes.length) {
    // Find a cycle for the error message
    const remaining = nodes.filter((n) => inDegree.get(n.name)! > 0).map((n) => n.name);
    const cyclePath = findCyclePath(remaining, adjacency);
    throw new CycleError(cyclePath);
  }
}

/**
 * Topological sort of DAG nodes using Kahn's algorithm.
 * Returns nodes in submission order (leaves first, roots last).
 * Throws CycleError if a cycle is detected.
 */
export function topoSort(nodes: DAGNode[]): DAGNode[] {
  const { nodeMap, inDegree, adjacency } = buildGraph(nodes);

  const queue: string[] = [];
  for (const [name, degree] of inDegree) {
    if (degree === 0) {
      queue.push(name);
    }
  }

  const sorted: DAGNode[] = [];
  let processed = 0;
  while (queue.length > 0) {
    const current = queue.shift()!;
    sorted.push(nodeMap.get(current)!);
    processed++;
    for (const neighbor of adjacency.get(current)!) {
      const newDegree = inDegree.get(neighbor)! - 1;
      inDegree.set(neighbor, newDegree);
      if (newDegree === 0) {
        queue.push(neighbor);
      }
    }
  }

  if (processed < nodes.length) {
    // Find a cycle for the error message
    const remaining = nodes.filter((n) => inDegree.get(n.name)! > 0).map((n) => n.name);
    const cyclePath = findCyclePath(remaining, adjacency);
    throw new CycleError(cyclePath);
  }

  return sorted;
}

/**
 * Find a cycle path from remaining nodes with non-zero in-degree.
 */
function findCyclePath(remaining: string[], adjacency: Map<string, string[]>): string[] {
  const remainingSet = new Set(remaining);
  const visited = new Set<string>();
  const path: string[] = [];

  // DFS cycle path reconstruction:
  // - path tracks the current exploration stack (nodes entered but not yet exited).
  // - When we encounter a node already in path, we have found the cycle entry point.
  // - We push it again so the path array reads "A -> B -> C -> A" (closed loop),
  //   then splice off any prefix before the cycle start so only the cycle is returned.
  // - visited is reset per root to allow fresh DFS from each candidate root.
  function dfs(node: string): boolean {
    if (path.includes(node)) {
      const cycleStart = path.indexOf(node);
      path.push(node);
      path.splice(0, cycleStart);
      return true;
    }
    if (visited.has(node)) return false;
    visited.add(node);
    path.push(node);

    for (const neighbor of adjacency.get(node) ?? []) {
      if (remainingSet.has(neighbor) && dfs(neighbor)) {
        return true;
      }
    }
    path.pop();
    return false;
  }

  for (const node of remaining) {
    visited.clear();
    path.length = 0;
    if (dfs(node)) return path;
  }

  return remaining.slice(0, 2);
}
