/**
 * Scenario: flow-producer (Valkey only)
 * Create random flow tree: parent with 1-4 children, some children may have
 * sub-children (depth 2). Process all with a single worker. Verify: all jobs
 * processed, parent processes last (after all children). Use getChildrenValues()
 * on parent to verify child return values.
 */

import type { ScenarioContext, ScenarioResult } from '../types';

function waitForCount(target: number, counter: { value: number }, timeoutMs = 25000): Promise<void> {
  return new Promise((resolve) => {
    const start = Date.now();
    const check = () => {
      if (counter.value >= target) return resolve();
      if (Date.now() - start > timeoutMs) return resolve();
      setTimeout(check, 50);
    };
    check();
  });
}

export async function flowProducer(ctx: ScenarioContext): Promise<ScenarioResult> {
  const { rng } = ctx;
  const queueName = ctx.uid();
  const queue = ctx.createQueue(queueName);

  const violations: string[] = [];
  const processOrder: string[] = [];
  const counter = { value: 0 };
  let parentChildrenValues: Record<string, any> | undefined;

  // Create worker - processes all jobs including parent and children
  ctx.createWorker(
    queueName,
    async (job: any) => {
      processOrder.push(job.name);
      counter.value++;

      // If this is the parent, grab children values
      if (job.name === 'parent') {
        try {
          parentChildrenValues = await job.getChildrenValues();
        } catch {
          // May fail if children not set up properly
        }
      }

      // Return a value so getChildrenValues() has something to read
      return { processed: job.name, ts: Date.now() };
    },
    { concurrency: 5 },
  );

  // Build flow tree
  const fp = ctx.createFlowProducer();
  const numChildren = rng.nextInt(1, 4);

  const children: any[] = [];
  let expectedTotal = 1; // parent

  for (let i = 0; i < numChildren; i++) {
    const hasSubChildren = rng.chance(0.4);
    if (hasSubChildren) {
      const numSub = rng.nextInt(1, 3);
      const subChildren: any[] = [];
      for (let s = 0; s < numSub; s++) {
        subChildren.push({
          name: `child-${i}-sub-${s}`,
          queueName,
          data: { childIndex: i, subIndex: s },
        });
        expectedTotal++;
      }
      children.push({
        name: `child-${i}`,
        queueName,
        data: { childIndex: i, hasSubChildren: true },
        children: subChildren,
      });
    } else {
      children.push({
        name: `child-${i}`,
        queueName,
        data: { childIndex: i },
      });
    }
    expectedTotal++;
  }

  await fp.add({
    name: 'parent',
    queueName,
    data: { role: 'parent', childCount: numChildren },
    children,
  });

  // Wait for all jobs to complete
  await waitForCount(expectedTotal, counter);

  // Verify parent processed last (after all children)
  const parentIdx = processOrder.indexOf('parent');
  if (parentIdx === -1) {
    violations.push('Parent job was never processed');
  } else {
    // All child names should appear before the parent
    for (let i = 0; i < numChildren; i++) {
      const childIdx = processOrder.indexOf(`child-${i}`);
      if (childIdx === -1) {
        violations.push(`Child ${i} was never processed`);
      } else if (childIdx > parentIdx) {
        violations.push(`Child ${i} processed after parent (child@${childIdx} > parent@${parentIdx})`);
      }
    }
  }

  // Verify getChildrenValues returned data for the direct children
  if (parentChildrenValues !== undefined) {
    const childKeys = Object.keys(parentChildrenValues);
    if (childKeys.length === 0 && numChildren > 0) {
      violations.push('getChildrenValues() returned empty but there were children');
    }
  } else if (parentIdx !== -1) {
    violations.push('getChildrenValues() was not captured on parent');
  }

  return {
    added: expectedTotal,
    processed: counter.value,
    failed: 0,
    violations,
  };
}
