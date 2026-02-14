import { FlowProducer, JobNode } from './flow-producer';
import type { ConnectionOptions, FlowJob, JobOptions } from './types';

export interface WorkflowJobDef {
  name: string;
  data: any;
  opts?: JobOptions;
}

/**
 * Chain: execute jobs sequentially. Each step becomes a child of the next,
 * so step N+1 only runs after step N completes. The last job in the array
 * runs first; the first job in the array runs last and is the top-level parent.
 *
 * Returns the JobNode tree. The top-level job (jobs[0]) is the root.
 * When the chain completes, the root's processor can call getChildrenValues()
 * to access results from children.
 */
export async function chain(
  queueName: string,
  jobs: WorkflowJobDef[],
  connection: ConnectionOptions,
  prefix?: string,
): Promise<JobNode> {
  if (jobs.length === 0) {
    throw new Error('chain() requires at least one job');
  }

  const flow = new FlowProducer({ connection, prefix });

  try {
    if (jobs.length === 1) {
      // Single job - just add it directly
      return await flow.add({
        name: jobs[0].name,
        queueName,
        data: jobs[0].data,
        opts: jobs[0].opts,
      });
    }

    // Build a nested flow: jobs[0] is the root parent, jobs[1] is its child,
    // jobs[1] is parent of jobs[2], etc.
    // The deepest job (last in array) runs first.
    // Build bottom-up: start from the last job and wrap each as parent.
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

    return await flow.add(flowJob);
  } finally {
    await flow.close();
  }
}

/**
 * Group: execute jobs in parallel. All jobs run concurrently.
 * A synthetic parent job (name: '__group__') waits for all children.
 * When complete, the parent's processor receives all children's results
 * via getChildrenValues().
 *
 * Returns the JobNode tree. The root is the group parent.
 */
export async function group(
  queueName: string,
  jobs: WorkflowJobDef[],
  connection: ConnectionOptions,
  prefix?: string,
): Promise<JobNode> {
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

    return await flow.add({
      name: '__group__',
      queueName,
      data: {},
      children,
    });
  } finally {
    await flow.close();
  }
}

/**
 * Chord: run a group of jobs in parallel, then execute a callback job
 * with the results. The callback is the parent, the group members are children.
 *
 * Returns the JobNode tree. The root is the callback job.
 */
export async function chord(
  queueName: string,
  groupJobs: WorkflowJobDef[],
  callback: WorkflowJobDef,
  connection: ConnectionOptions,
  prefix?: string,
): Promise<JobNode> {
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

    return await flow.add({
      name: callback.name,
      queueName,
      data: callback.data,
      opts: callback.opts,
      children,
    });
  } finally {
    await flow.close();
  }
}
