/**
 * In-memory FlowProducer for glide-mq/testing. Flows are built over the open TestQueue
 * instances, looked up by name, so a parent and its children may live in different queues.
 */

import type { BudgetOptions, DAGFlow, DAGNode, FlowJob } from './types';
import { GlideMQError } from './errors';
import { validateQueueName } from './utils';
import { topoSort, validateDAG } from './dag-utils';
import { TestQueue, type FlowJobInit, type TestJob } from './testing';

/** Shape returned by TestFlowProducer.add(), like the production JobNode. */
export interface TestJobNode {
  job: TestJob;
  children?: TestJobNode[];
}

export interface TestFlowProducerOptions {
  /** Prefix of the deps keys returned by getChildrenValues(). Default: 'glide'. */
  prefix?: string;
}

/**
 * Creates parent-child job flows and DAG workflows over TestQueue, with no connection.
 * A parent starts in `waiting-children` and moves to `waiting` when its last child completes.
 */
export class TestFlowProducer {
  private readonly prefix: string;
  private closing = false;

  constructor(opts?: TestFlowProducerOptions) {
    this.prefix = opts?.prefix ?? 'glide';
  }

  /** Add a flow atomically. With `budget`, every job of the flow shares one budget keyed by the root job id. */
  async add(flow: FlowJob, flowOpts?: { budget?: BudgetOptions }): Promise<TestJobNode> {
    this.assertOpen();
    const reserved = new Map<string, Set<string>>();
    this.checkTree(flow, reserved);
    const node = this.buildTree(flow, reserved);
    if (flowOpts?.budget) {
      this.queueOf(flow.queueName).setBudget(node.job.id, flowOpts.budget);
      this.shareBudget(node, flow, flow.queueName, node.job.id);
    }
    return node;
  }

  /** Add multiple independent flows. Every flow is validated before the first is added. */
  async addBulk(flows: FlowJob[]): Promise<TestJobNode[]> {
    this.assertOpen();
    const reserved = new Map<string, Set<string>>();
    for (const flow of flows) this.checkTree(flow, reserved);
    return flows.map((flow) => this.buildTree(flow, reserved));
  }

  /**
   * Add a DAG where a job can wait for several others: a node with `deps` waits in
   * `waiting-children` until all of them complete. Returns the jobs by node name.
   */
  async addDAG(dag: DAGFlow): Promise<Map<string, TestJob>> {
    this.assertOpen();
    validateDAG(dag.nodes);
    const reserved = new Map<string, Set<string>>();
    for (const node of dag.nodes) this.checkJob(node.queueName, node.data, node.opts, reserved);
    const dependents = new Map<string, DAGNode[]>(dag.nodes.map((node) => [node.name, []]));
    for (const node of dag.nodes) for (const dep of new Set(node.deps)) dependents.get(dep)!.push(node);
    const jobs = new Map<string, TestJob>();
    // Dependents first, so each job is created with its parents and appears in their deps.
    for (const node of [...topoSort(dag.nodes)].reverse()) {
      const parents = dependents.get(node.name)!.map((dependent) => ({
        queue: dependent.queueName,
        id: jobs.get(dependent.name)!.id,
      }));
      const wait = (node.deps?.length ?? 0) > 0;
      const init = this.flowInit(node.queueName, wait, reserved, parents);
      jobs.set(node.name, this.queueOf(node.queueName).addFlowJob(node.name, node.data, node.opts ?? {}, init));
    }
    return jobs;
  }

  /** Mark the producer closed; later adds throw. */
  async close(): Promise<void> {
    this.closing = true;
  }

  private assertOpen(): void {
    if (this.closing) throw new GlideMQError('FlowProducer is closing');
  }

  private queueOf(name: string): TestQueue<any, any> {
    const queue = TestQueue.lookup(name);
    if (!queue) throw new GlideMQError(`TestQueue "${name}" is not open. Create it before adding a flow.`);
    return queue;
  }

  private checkJob(queueName: string, data: unknown, opts: FlowJob['opts'], reserved: Map<string, Set<string>>): void {
    validateQueueName(queueName);
    this.queueOf(queueName).checkFlowJob(data, opts ?? {});
    if (opts?.jobId) {
      const ids = reserved.get(queueName) ?? new Set<string>();
      if (ids.has(opts.jobId)) throw new Error('Duplicate job ID in flow');
      reserved.set(queueName, ids.add(opts.jobId));
    }
  }

  private checkTree(flow: FlowJob, reserved: Map<string, Set<string>>): void {
    this.checkJob(flow.queueName, flow.data, flow.opts, reserved);
    for (const child of flow.children ?? []) this.checkTree(child, reserved);
  }

  private flowInit(
    queueName: string,
    wait: boolean,
    reserved: Map<string, Set<string>>,
    parents?: { queue: string; id: string }[],
  ): FlowJobInit {
    return { wait, prefix: this.prefix, reserved: reserved.get(queueName), parents };
  }

  /**
   * Sub-flows are created first, then the parent, then its leaf children, like FlowProducer. A leaf is
   * created with its parent; a sub-flow root exists before its parent, so it is linked afterwards.
   */
  private buildTree(flow: FlowJob, reserved: Map<string, Set<string>>): TestJobNode {
    const queue = this.queueOf(flow.queueName);
    const defs = flow.children ?? [];
    if (defs.length === 0) {
      return {
        job: queue.addFlowJob(flow.name, flow.data, flow.opts ?? {}, this.flowInit(flow.queueName, false, reserved)),
      };
    }
    const subFlows = defs.map((def) => (def.children?.length ? this.buildTree(def, reserved) : undefined));
    const parent = queue.addFlowJob(
      flow.name,
      flow.data,
      flow.opts ?? {},
      this.flowInit(flow.queueName, true, reserved),
    );
    const parentRef = { queue: flow.queueName, id: parent.id };
    const children = defs.map((def, i): TestJobNode => {
      const subFlow = subFlows[i];
      if (subFlow) {
        const childQueue = this.queueOf(def.queueName);
        childQueue.attachParent(childQueue.jobs.get(subFlow.job.id)!, parentRef);
        subFlow.job.parentId = parentRef.id;
        subFlow.job.parentQueue = parentRef.queue;
        return subFlow;
      }
      const init = this.flowInit(def.queueName, false, reserved, [parentRef]);
      return { job: this.queueOf(def.queueName).addFlowJob(def.name, def.data, def.opts ?? {}, init) };
    });
    return { job: parent, children };
  }

  /**
   * Point every job of the flow at the root budget, keyed by the root job id on the root queue. A job
   * in another queue reaches the same state under `rootQueue:rootId`, so unrelated budgets never clash.
   */
  private shareBudget(node: TestJobNode, def: FlowJob, rootQueue: string, rootId: string): void {
    const queue = this.queueOf(def.queueName);
    let key = rootId;
    if (def.queueName !== rootQueue) {
      key = `${rootQueue}:${rootId}`;
      queue.budgets.set(key, this.queueOf(rootQueue).budgets.get(rootId)!);
    }
    queue.jobs.get(node.job.id)!.budgetKey = key;
    node.job.budgetKey = key;
    (node.children ?? []).forEach((child, i) => this.shareBudget(child, def.children![i], rootQueue, rootId));
  }
}
