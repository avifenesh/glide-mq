/**
 * In-memory FlowProducer for glide-mq/testing. Flows are built over the open TestQueue
 * instances, looked up by name, so a parent and its children may live in different queues.
 */

import type { BudgetOptions, DAGFlow, FlowJob } from './types';
import { GlideMQError } from './errors';
import { validateQueueName } from './utils';
import { topoSort, validateDAG } from './dag-utils';
import { TestQueue, type TestJob } from './testing';

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
    this.checkTree(flow, new Set());
    const node = this.buildTree(flow);
    if (flowOpts?.budget) {
      this.queueOf(flow.queueName).setBudget(node.job.id, flowOpts.budget);
      this.shareBudget(node, flow, flow.queueName, node.job.id);
    }
    return node;
  }

  /** Add multiple independent flows. Every flow is validated before the first is added. */
  async addBulk(flows: FlowJob[]): Promise<TestJobNode[]> {
    this.assertOpen();
    const taken = new Set<string>();
    for (const flow of flows) this.checkTree(flow, taken);
    return flows.map((flow) => this.buildTree(flow));
  }

  /**
   * Add a DAG where a job can wait for several others: a node with `deps` waits in
   * `waiting-children` until all of them complete. Returns the jobs by node name.
   */
  async addDAG(dag: DAGFlow): Promise<Map<string, TestJob>> {
    this.assertOpen();
    validateDAG(dag.nodes);
    const taken = new Set<string>();
    for (const node of dag.nodes) this.checkJob(node.queueName, node.data, node.opts, taken);
    const jobs = new Map<string, TestJob>();
    // Dependents first, so every parent exists when its deps are wired to it.
    for (const node of [...topoSort(dag.nodes)].reverse()) {
      const waits = (node.deps?.length ?? 0) > 0;
      jobs.set(node.name, this.queueOf(node.queueName).addFlowJob(node.name, node.data, node.opts ?? {}, waits));
    }
    const queueByName = new Map(dag.nodes.map((node) => [node.name, node.queueName]));
    for (const node of dag.nodes) {
      for (const dep of node.deps ?? []) {
        this.link(jobs.get(dep)!, queueByName.get(dep)!, { queue: node.queueName, id: jobs.get(node.name)!.id });
      }
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

  private checkJob(queueName: string, data: unknown, opts: FlowJob['opts'], taken: Set<string>): void {
    validateQueueName(queueName);
    this.queueOf(queueName).checkFlowJob(data, opts ?? {});
    if (opts?.jobId) {
      const key = `${queueName}:${opts.jobId}`;
      if (taken.has(key)) throw new Error('Duplicate job ID in flow');
      taken.add(key);
    }
  }

  private checkTree(flow: FlowJob, taken: Set<string>): void {
    this.checkJob(flow.queueName, flow.data, flow.opts, taken);
    for (const child of flow.children ?? []) this.checkTree(child, taken);
  }

  /** Sub-flows are created first, then the parent, then its leaf children, like FlowProducer. */
  private buildTree(flow: FlowJob): TestJobNode {
    const queue = this.queueOf(flow.queueName);
    const defs = flow.children ?? [];
    if (defs.length === 0) return { job: queue.addFlowJob(flow.name, flow.data, flow.opts ?? {}, false) };
    const subFlows = defs.map((def) => (def.children?.length ? this.buildTree(def) : undefined));
    const parent = queue.addFlowJob(flow.name, flow.data, flow.opts ?? {}, true);
    const children = defs.map((def, i) => {
      const node = subFlows[i] ?? {
        job: this.queueOf(def.queueName).addFlowJob(def.name, def.data, def.opts ?? {}, false),
      };
      this.link(node.job, def.queueName, { queue: flow.queueName, id: parent.id });
      return node;
    });
    return { job: parent, children };
  }

  /** Make `parent` a parent of `job`: the job hash fields and the parent's deps. */
  private link(job: TestJob, queueName: string, parent: { queue: string; id: string }): void {
    const queue = this.queueOf(queueName);
    queue.attachParent(queue.jobs.get(job.id)!, parent, this.prefix);
    job.parentId ??= parent.id;
    job.parentQueue ??= parent.queue;
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
