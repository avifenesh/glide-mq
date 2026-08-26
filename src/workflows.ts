import { FlowProducer, JobNode } from './flow-producer';
import type { Client, ConnectionOptions, FlowJob, JobOptions, DAGNode } from './types';
import type { Job } from './job';

export interface WorkflowJobDef {
  name: string;
  data: any;
  opts?: JobOptions;
}

/** Connection for helpers. Pass `client` to keep returned jobs usable. */
export type WorkflowConnection = ConnectionOptions & { client?: Client };

/** Returned workflow tree or DAG map plus close() for an injected client. */
export type ClosableWorkflow<T> = T & { close(): Promise<void> };

function withOwnedFlow<T extends object>(flow: FlowProducer, value: T): ClosableWorkflow<T> {
  const handle = value as ClosableWorkflow<T>;
  handle.close = () => flow.close();
  return handle;
}

async function withFlow<T extends object>(
  connection: WorkflowConnection,
  prefix: string | undefined,
  run: (flow: FlowProducer) => Promise<T>,
): Promise<ClosableWorkflow<T>> {
  const { client, ...conn } = connection;
  const flow = new FlowProducer({ connection: conn, client, prefix });
  const owned = !client;
  try {
    const value = await run(flow);
    if (owned) {
      await flow.close();
      (value as ClosableWorkflow<T>).close = async () => {};
      return value as ClosableWorkflow<T>;
    }
    return withOwnedFlow(flow, value);
  } catch (err) {
    await flow.close();
    throw err;
  }
}

/**
 * Chain: execute jobs sequentially. Each step becomes a child of the next,
 * so step N+1 only runs after step N completes. The last job in the array
 * runs first; the first job in the array runs last and is the top-level parent.
 *
 * Returns the JobNode tree. The top-level job (jobs[0]) is the root.
 * Pass `{ client }` on the connection to keep returned jobs usable; otherwise
 * the owned client is closed after submit. Call close() when using a shared client.
 */
export async function chain(
  queueName: string,
  jobs: WorkflowJobDef[],
  connection: WorkflowConnection,
  prefix?: string,
): Promise<ClosableWorkflow<JobNode>> {
  if (jobs.length === 0) {
    throw new Error('chain() requires at least one job');
  }

  return withFlow(connection, prefix, async (flow) => {
    if (jobs.length === 1) {
      return flow.add({
        name: jobs[0].name,
        queueName,
        data: jobs[0].data,
        opts: jobs[0].opts,
      });
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

    return flow.add(flowJob);
  });
}

/**
 * Group: execute jobs in parallel. All jobs run concurrently.
 * A synthetic parent job (name: '__group__') waits for all children.
 * When complete, the parent's processor receives all children's results
 * via getChildrenValues().
 *
 * Returns the JobNode tree. The root is the group parent.
 * Pass `{ client }` on the connection to keep returned jobs usable.
 */
export async function group(
  queueName: string,
  jobs: WorkflowJobDef[],
  connection: WorkflowConnection,
  prefix?: string,
): Promise<ClosableWorkflow<JobNode>> {
  if (jobs.length === 0) {
    throw new Error('group() requires at least one job');
  }

  return withFlow(connection, prefix, (flow) => {
    const children: FlowJob[] = jobs.map((j) => ({
      name: j.name,
      queueName,
      data: j.data,
      opts: j.opts,
    }));

    return flow.add({
      name: '__group__',
      queueName,
      data: {},
      children,
    });
  });
}

/**
 * Chord: run a group of jobs in parallel, then execute a callback job
 * with the results. The callback is the parent, the group members are children.
 *
 * Returns the JobNode tree. The root is the callback job.
 * Pass `{ client }` on the connection to keep returned jobs usable.
 */
export async function chord(
  queueName: string,
  groupJobs: WorkflowJobDef[],
  callback: WorkflowJobDef,
  connection: WorkflowConnection,
  prefix?: string,
): Promise<ClosableWorkflow<JobNode>> {
  if (groupJobs.length === 0) {
    throw new Error('chord() requires at least one group job');
  }

  return withFlow(connection, prefix, (flow) => {
    const children: FlowJob[] = groupJobs.map((j) => ({
      name: j.name,
      queueName,
      data: j.data,
      opts: j.opts,
    }));

    return flow.add({
      name: callback.name,
      queueName,
      data: callback.data,
      opts: callback.opts,
      children,
    });
  });
}

/**
 * DAG: submit a directed acyclic graph of jobs where each job can depend on
 * multiple other jobs. The graph is validated for cycles and submitted in
 * topological order (leaves first).
 *
 * Returns a Map of node name to Job instance.
 * Pass `{ client }` on the connection to keep returned jobs usable.
 */
export async function dag(
  nodes: DAGNode[],
  connection: WorkflowConnection,
  prefix?: string,
): Promise<ClosableWorkflow<Map<string, Job>>> {
  if (nodes.length === 0) {
    throw new Error('dag() requires at least one node');
  }

  return withFlow(connection, prefix, (flow) => flow.addDAG({ nodes }));
}
