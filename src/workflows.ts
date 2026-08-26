import { FlowProducer, JobNode } from './flow-producer';
import type { ConnectionOptions, FlowJob, JobOptions, DAGNode } from './types';
import type { Job } from './job';

export interface WorkflowJobDef {
  name: string;
  data: any;
  opts?: JobOptions;
}

/** Returned workflow tree or DAG map plus close() for the owned client. */
export type ClosableWorkflow<T> = T & { close(): Promise<void> };

function withOwnedFlow<T extends object>(flow: FlowProducer, value: T): ClosableWorkflow<T> {
  const handle = value as ClosableWorkflow<T>;
  handle.close = () => flow.close();
  return handle;
}

/**
 * Chain: execute jobs sequentially. Each step becomes a child of the next,
 * so step N+1 only runs after step N completes. The last job in the array
 * runs first; the first job in the array runs last and is the top-level parent.
 *
 * Returns the JobNode tree. The top-level job (jobs[0]) is the root.
 * Call close() when finished with the returned jobs to release the client.
 */
export async function chain(
  queueName: string,
  jobs: WorkflowJobDef[],
  connection: ConnectionOptions,
  prefix?: string,
): Promise<ClosableWorkflow<JobNode>> {
  if (jobs.length === 0) {
    throw new Error('chain() requires at least one job');
  }

  const flow = new FlowProducer({ connection, prefix });

  try {
    if (jobs.length === 1) {
      return withOwnedFlow(
        flow,
        await flow.add({
          name: jobs[0].name,
          queueName,
          data: jobs[0].data,
          opts: jobs[0].opts,
        }),
      );
    }

    let flowJob: FlowJob = {
      name: jobs[jobs.length - 1].name,
      queueName,
      data: jobs[jobs.length - 1].data,
      opts: jobs[jobs.length - 1].opts,
    };

    for (let i = jobs.length - 2; i >= 0; i--) {
      flowJob = {
        name: jobs[i].name,
        queueName,
        data: jobs[i].data,
        opts: jobs[i].opts,
        children: [flowJob],
      };
    }

    return withOwnedFlow(flow, await flow.add(flowJob));
  } catch (err) {
    await flow.close();
    throw err;
  }
}

/**
 * Group: execute jobs in parallel. All jobs run concurrently.
 * A synthetic parent job (name: '__group__') waits for all children.
 * When complete, the parent's processor receives all children's results
 * via getChildrenValues().
 *
 * Returns the JobNode tree. The root is the group parent.
 * Call close() when finished with the returned jobs to release the client.
 */
export async function group(
  queueName: string,
  jobs: WorkflowJobDef[],
  connection: ConnectionOptions,
  prefix?: string,
): Promise<ClosableWorkflow<JobNode>> {
  if (jobs.length === 0) {
    throw new Error('group() requires at least one job');
  }

  const flow = new FlowProducer({ connection, prefix });

  try {
    const children: FlowJob[] = jobs.map((j) => ({
      name: j.name,
      queueName,
      data: j.data,
      opts: j.opts,
    }));

    return withOwnedFlow(
      flow,
      await flow.add({
        name: '__group__',
        queueName,
        data: {},
        children,
      }),
    );
  } catch (err) {
    await flow.close();
    throw err;
  }
}

/**
 * Chord: run a group of jobs in parallel, then execute a callback job
 * with the results. The callback is the parent, the group members are children.
 *
 * Returns the JobNode tree. The root is the callback job.
 * Call close() when finished with the returned jobs to release the client.
 */
export async function chord(
  queueName: string,
  groupJobs: WorkflowJobDef[],
  callback: WorkflowJobDef,
  connection: ConnectionOptions,
  prefix?: string,
): Promise<ClosableWorkflow<JobNode>> {
  if (groupJobs.length === 0) {
    throw new Error('chord() requires at least one group job');
  }

  const flow = new FlowProducer({ connection, prefix });

  try {
    const children: FlowJob[] = groupJobs.map((j) => ({
      name: j.name,
      queueName,
      data: j.data,
      opts: j.opts,
    }));

    return withOwnedFlow(
      flow,
      await flow.add({
        name: callback.name,
        queueName,
        data: callback.data,
        opts: callback.opts,
        children,
      }),
    );
  } catch (err) {
    await flow.close();
    throw err;
  }
}

/**
 * DAG: submit a directed acyclic graph of jobs where each job can depend on
 * multiple other jobs. The graph is validated for cycles and submitted in
 * topological order (leaves first).
 *
 * Returns a Map of node name to Job instance.
 * Call close() when finished with the returned jobs to release the client.
 */
export async function dag(
  nodes: DAGNode[],
  connection: ConnectionOptions,
  prefix?: string,
): Promise<ClosableWorkflow<Map<string, Job>>> {
  if (nodes.length === 0) {
    throw new Error('dag() requires at least one node');
  }

  const flow = new FlowProducer({ connection, prefix });

  try {
    return withOwnedFlow(flow, await flow.addDAG({ nodes }));
  } catch (err) {
    await flow.close();
    throw err;
  }
}
