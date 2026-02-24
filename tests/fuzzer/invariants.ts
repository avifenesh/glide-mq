/**
 * Invariant checkers for fuzzer scenarios.
 * Each function returns an array of violation descriptions (empty = all good).
 */

/**
 * Check that every added job ID reached a terminal state.
 * Violation: a job was added but never completed or failed.
 */
export function checkNoJobLost(addedIds: Set<string>, terminalIds: Set<string>): string[] {
  const violations: string[] = [];
  for (const id of addedIds) {
    if (!terminalIds.has(id)) {
      violations.push(`Job ${id} was added but never reached a terminal state`);
    }
  }
  return violations;
}

/**
 * Check that no job was processed more than once.
 * Violation: a jobId appears multiple times in the process log.
 */
export function checkNoDuplicates(processLog: { jobId: string }[]): string[] {
  const violations: string[] = [];
  const seen = new Map<string, number>();
  for (const entry of processLog) {
    const count = (seen.get(entry.jobId) ?? 0) + 1;
    seen.set(entry.jobId, count);
    if (count === 2) {
      violations.push(`Job ${entry.jobId} was processed more than once`);
    }
  }
  return violations;
}

/**
 * Check that job data survived the roundtrip (add -> process) intact.
 * Uses JSON serialization for deep comparison.
 */
export function checkDataIntegrity(addedData: Map<string, any>, processedData: Map<string, any>): string[] {
  const violations: string[] = [];
  for (const [id, original] of addedData) {
    const received = processedData.get(id);
    if (received === undefined) continue; // Missing jobs checked by checkNoJobLost
    const origJson = JSON.stringify(original);
    const recvJson = JSON.stringify(received);
    if (origJson !== recvJson) {
      violations.push(`Job ${id} data mismatch: sent ${origJson.slice(0, 100)}, got ${recvJson.slice(0, 100)}`);
    }
  }
  return violations;
}

/**
 * Check that job state counts are consistent with expected totals.
 * The sum of all states + removed jobs should equal the total added.
 */
export function checkCountConsistency(
  counts: {
    waiting: number;
    active: number;
    delayed: number;
    completed: number;
    failed: number;
  },
  expectedTotal: number,
  removedCount: number,
): string[] {
  const violations: string[] = [];
  const actual = counts.waiting + counts.active + counts.delayed + counts.completed + counts.failed + removedCount;
  if (actual !== expectedTotal) {
    violations.push(
      `Count inconsistency: states sum to ${actual} (w=${counts.waiting} a=${counts.active} ` +
        `d=${counts.delayed} c=${counts.completed} f=${counts.failed} removed=${removedCount}) ` +
        `but expected ${expectedTotal}`,
    );
  }
  return violations;
}

/**
 * Check that all resources were properly closed.
 * Violation: a resource is still open after cleanup.
 */
export function checkCleanShutdown(openResources: any[]): string[] {
  const violations: string[] = [];
  if (openResources.length > 0) {
    violations.push(`${openResources.length} resource(s) still open after cleanup`);
  }
  return violations;
}
