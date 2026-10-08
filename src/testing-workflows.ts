/**
 * chain, group, chord and dag for glide-mq/testing: the signatures of src/workflows.ts, built on
 * TestFlowProducer. The `connection` argument is accepted and ignored, so a call written against
 * production runs unchanged without a connection.
 */

import type { DAGNode, FlowJob } from './types';
import type { ClosableWorkflow, WorkflowConnection, WorkflowJobDef } from './workflows';
import { TestFlowProducer, type TestJobNode } from './testing-flow';
import type { TestJob } from './testing';

function closable<T extends object>(value: T): ClosableWorkflow<T> {
  const handle = value as ClosableWorkflow<T>;
  handle.close = async () => {};
  return handle;
}

function toFlowJob(queueName: string, def: WorkflowJobDef, children?: FlowJob[]): FlowJob {
  return { name: def.name, queueName, data: def.data, opts: def.opts, ...(children && { children }) };
}

/**
 * Chain: run jobs one after another. Each step is a child of the previous one in the array, so the last
 * job runs first and jobs[0] is the root. Returns the JobNode tree.
 */
export async function chain(
  queueName: string,
  jobs: WorkflowJobDef[],
  _connection?: WorkflowConnection,
  prefix?: string,
): Promise<ClosableWorkflow<TestJobNode>> {
  if (jobs.length === 0) {
    throw new Error('chain() requires at least one job');
  }
  let flow = toFlowJob(queueName, jobs[jobs.length - 1]);
  for (let i = jobs.length - 2; i >= 0; i--) flow = toFlowJob(queueName, jobs[i], [flow]);
  return closable(await new TestFlowProducer({ prefix }).add(flow));
}

/**
 * Group: run jobs in parallel under a synthetic `__group__` parent that waits for all of them and can
 * read their results with getChildrenValues(). Returns the JobNode tree rooted at the group parent.
 */
export async function group(
  queueName: string,
  jobs: WorkflowJobDef[],
  _connection?: WorkflowConnection,
  prefix?: string,
): Promise<ClosableWorkflow<TestJobNode>> {
  if (jobs.length === 0) {
    throw new Error('group() requires at least one job');
  }
  const parent: WorkflowJobDef = { name: '__group__', data: {} };
  const flow = toFlowJob(
    queueName,
    parent,
    jobs.map((def) => toFlowJob(queueName, def)),
  );
  return closable(await new TestFlowProducer({ prefix }).add(flow));
}

/**
 * Chord: run a group of jobs in parallel, then the callback with their results. The callback is the
 * parent. Returns the JobNode tree rooted at the callback.
 */
export async function chord(
  queueName: string,
  groupJobs: WorkflowJobDef[],
  callback: WorkflowJobDef,
  _connection?: WorkflowConnection,
  prefix?: string,
): Promise<ClosableWorkflow<TestJobNode>> {
  if (groupJobs.length === 0) {
    throw new Error('chord() requires at least one group job');
  }
  const flow = toFlowJob(
    queueName,
    callback,
    groupJobs.map((def) => toFlowJob(queueName, def)),
  );
  return closable(await new TestFlowProducer({ prefix }).add(flow));
}

/**
 * DAG: submit jobs that each depend on any number of other jobs. The graph is validated for cycles.
 * Returns a Map of node name to job.
 */
export async function dag(
  nodes: DAGNode[],
  _connection?: WorkflowConnection,
  prefix?: string,
): Promise<ClosableWorkflow<Map<string, TestJob>>> {
  if (nodes.length === 0) {
    throw new Error('dag() requires at least one node');
  }
  return closable(await new TestFlowProducer({ prefix }).addDAG({ nodes }));
}
