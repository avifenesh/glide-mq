/**
 * Scenario: workflows (Valkey only)
 * Test chain, group, and chord workflow helpers.
 * - chain: 2-5 jobs processed in reverse order (last is root)
 * - group: 2-6 jobs processed in parallel, synthetic __group__ parent
 * - chord: 2-4 group jobs + callback, callback after group
 */

import type { ScenarioContext, ScenarioResult } from '../types';

const { chain, group, chord } = require('../../../dist') as typeof import('../../../src/index');

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

export async function workflows(ctx: ScenarioContext): Promise<ScenarioResult> {
  const { rng } = ctx;
  const violations: string[] = [];
  let totalAdded = 0;
  let totalProcessed = 0;

  // Build connection config based on mode
  const connection =
    ctx.mode === 'cluster'
      ? { addresses: [{ host: '127.0.0.1', port: 7000 }], clusterMode: true }
      : { addresses: [{ host: 'localhost', port: 6379 }] };

  // ---- Phase 1: chain ----
  const chainQueueName = ctx.uid();
  ctx.createQueue(chainQueueName);
  const chainOrder: string[] = [];
  const chainCounter = { value: 0 };
  const chainSize = rng.nextInt(2, 5);
  const chainExpected = chainSize;

  ctx.createWorker(
    chainQueueName,
    async (job: any) => {
      chainOrder.push(job.name);
      chainCounter.value++;
      return { step: job.name };
    },
    { concurrency: 1 },
  );

  const chainJobs = Array.from({ length: chainSize }, (_, i) => ({
    name: `chain-step-${i}`,
    data: { step: i },
  }));

  await chain(chainQueueName, chainJobs, connection);
  totalAdded += chainExpected;

  await waitForCount(chainExpected, chainCounter);
  totalProcessed += chainCounter.value;

  // In a chain, jobs[last] runs first, jobs[0] runs last.
  // Verify the last element of chainJobs was processed first.
  if (chainOrder.length >= 2) {
    const lastJobName = `chain-step-${chainSize - 1}`;
    if (chainOrder[0] !== lastJobName) {
      violations.push(`Chain: expected ${lastJobName} to process first but got ${chainOrder[0]}`);
    }
  }

  // ---- Phase 2: group ----
  const groupQueueName = ctx.uid();
  ctx.createQueue(groupQueueName);
  const groupNames = new Set<string>();
  const groupCounter = { value: 0 };
  const groupSize = rng.nextInt(2, 6);
  // group adds N children + 1 synthetic __group__ parent
  const groupExpected = groupSize + 1;

  ctx.createWorker(
    groupQueueName,
    async (job: any) => {
      groupNames.add(job.name);
      groupCounter.value++;
      return { member: job.name };
    },
    { concurrency: 5 },
  );

  const groupJobs = Array.from({ length: groupSize }, (_, i) => ({
    name: `group-member-${i}`,
    data: { index: i },
  }));

  await group(groupQueueName, groupJobs, connection);
  totalAdded += groupExpected;

  await waitForCount(groupExpected, groupCounter);
  totalProcessed += groupCounter.value;

  // Verify all group members processed
  for (let i = 0; i < groupSize; i++) {
    if (!groupNames.has(`group-member-${i}`)) {
      violations.push(`Group: member group-member-${i} was not processed`);
    }
  }
  // Verify __group__ parent was processed
  if (!groupNames.has('__group__')) {
    violations.push('Group: synthetic __group__ parent was not processed');
  }

  // ---- Phase 3: chord ----
  const chordQueueName = ctx.uid();
  ctx.createQueue(chordQueueName);
  const chordOrder: string[] = [];
  const chordCounter = { value: 0 };
  const chordGroupSize = rng.nextInt(2, 4);
  // chord adds N children + 1 callback parent
  const chordExpected = chordGroupSize + 1;

  ctx.createWorker(
    chordQueueName,
    async (job: any) => {
      chordOrder.push(job.name);
      chordCounter.value++;
      return { result: job.name };
    },
    { concurrency: 5 },
  );

  const chordGroupJobs = Array.from({ length: chordGroupSize }, (_, i) => ({
    name: `chord-member-${i}`,
    data: { index: i },
  }));
  const callbackDef = { name: 'chord-callback', data: { isCallback: true } };

  await chord(chordQueueName, chordGroupJobs, callbackDef, connection);
  totalAdded += chordExpected;

  await waitForCount(chordExpected, chordCounter);
  totalProcessed += chordCounter.value;

  // Verify callback processed after all group members
  const cbIdx = chordOrder.indexOf('chord-callback');
  if (cbIdx === -1) {
    violations.push('Chord: callback was not processed');
  } else {
    for (let i = 0; i < chordGroupSize; i++) {
      const memberIdx = chordOrder.indexOf(`chord-member-${i}`);
      if (memberIdx === -1) {
        violations.push(`Chord: member chord-member-${i} was not processed`);
      } else if (memberIdx > cbIdx) {
        violations.push(`Chord: member chord-member-${i} processed after callback (member@${memberIdx} > cb@${cbIdx})`);
      }
    }
  }

  return {
    added: totalAdded,
    processed: totalProcessed,
    failed: 0,
    violations,
  };
}
