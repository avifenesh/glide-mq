import type { FlowProducerOptions, FlowJob, Client } from './types';
import { Job } from './job';
import { buildKeys, keyPrefix } from './utils';
import { createClient, ensureFunctionLibrary } from './connection';
import { LIBRARY_SOURCE, addFlow } from './functions/index';
import { withSpan } from './telemetry';

export interface JobNode {
  job: Job;
  children?: JobNode[];
}

export class FlowProducer {
  private opts: FlowProducerOptions;
  private client: Client | null = null;
  private closing = false;

  constructor(opts: FlowProducerOptions) {
    this.opts = opts;
  }

  /** @internal */
  private async getClient(): Promise<Client> {
    if (!this.client) {
      this.client = await createClient(this.opts.connection);
      await ensureFunctionLibrary(
        this.client,
        LIBRARY_SOURCE,
        this.opts.connection.clusterMode ?? false,
      );
    }
    return this.client;
  }

  /**
   * Add a flow (parent with children) atomically.
   * Children can have their own children (recursive flows), which are flattened
   * into multiple addFlow calls (one per level with children).
   */
  async add(flow: FlowJob): Promise<JobNode> {
    return withSpan(
      'glide-mq.flow.add',
      {
        'glide-mq.queue': flow.queueName,
        'glide-mq.flow.name': flow.name,
        'glide-mq.flow.childCount': flow.children?.length ?? 0,
      },
      async () => {
        const client = await this.getClient();
        return this.addFlowRecursive(client, flow);
      },
    );
  }

  /**
   * Add multiple independent flows.
   */
  async addBulk(flows: FlowJob[]): Promise<JobNode[]> {
    const client = await this.getClient();
    const results: JobNode[] = [];
    for (const flow of flows) {
      results.push(await this.addFlowRecursive(client, flow));
    }
    return results;
  }

  /**
   * Recursively add a flow. If children themselves have children,
   * those sub-flows are added first (bottom-up), and the resulting
   * child jobs are used as direct children of the current parent.
   */
  private async addFlowRecursive(client: Client, flow: FlowJob): Promise<JobNode> {
    const prefix = this.opts.prefix ?? 'glide';
    const parentQueueName = flow.queueName;
    const parentKeys = buildKeys(parentQueueName, prefix);

    // If no children, this is a leaf - add as a regular job (not a flow)
    if (!flow.children || flow.children.length === 0) {
      const { addJob } = await import('./functions/index');
      const timestamp = Date.now();
      const opts = flow.opts ?? {};
      const jobId = await addJob(
        client,
        parentKeys,
        flow.name,
        JSON.stringify(flow.data),
        JSON.stringify(opts),
        timestamp,
        opts.delay ?? 0,
        opts.priority ?? 0,
        '',
        opts.attempts ?? 0,
      );
      const job = new Job(
        client,
        parentKeys,
        String(jobId),
        flow.name,
        flow.data,
        opts,
      );
      job.timestamp = timestamp;
      return { job };
    }

    // Recursively process children that themselves have children (bottom-up).
    const childNodeMap: Map<number, JobNode> = new Map();
    for (let i = 0; i < flow.children.length; i++) {
      const child = flow.children[i];
      if (child.children && child.children.length > 0) {
        childNodeMap.set(i, await this.addFlowRecursive(client, child));
      }
    }

    // Build leaf children data for addFlow (children without sub-children)
    const childrenForLua = flow.children
      .filter((_, i) => !childNodeMap.has(i))
      .map((child) => {
        const childOpts = child.opts ?? {};
        return {
          name: child.name,
          data: JSON.stringify(child.data),
          opts: JSON.stringify(childOpts),
          delay: childOpts.delay ?? 0,
          priority: childOpts.priority ?? 0,
          maxAttempts: childOpts.attempts ?? 0,
          keys: buildKeys(child.queueName, prefix),
          queuePrefix: keyPrefix(prefix, child.queueName),
          parentQueueName: parentQueueName,
        };
      });

    // Build extra deps for sub-flow children (already created recursively)
    const extraDeps: string[] = [];
    for (const [i, subNode] of childNodeMap.entries()) {
      const child = flow.children[i];
      extraDeps.push(`${keyPrefix(prefix, child.queueName)}:${subNode.job.id}`);
    }

    const timestamp = Date.now();
    const parentOpts = flow.opts ?? {};

    const ids = await addFlow(
      client,
      parentKeys,
      flow.name,
      JSON.stringify(flow.data),
      JSON.stringify(parentOpts),
      timestamp,
      parentOpts.delay ?? 0,
      parentOpts.priority ?? 0,
      parentOpts.attempts ?? 0,
      childrenForLua,
      extraDeps,
    );

    const parentId = ids[0];

    // Set parentId and parentQueue on pre-existing sub-flow children
    for (const [i, subNode] of childNodeMap.entries()) {
      const child = flow.children[i];
      const childKeys = buildKeys(child.queueName, prefix);
      await client.hset(childKeys.job(subNode.job.id), {
        parentId: parentId,
        parentQueue: parentQueueName,
      });
      subNode.job.parentId = parentId;
      subNode.job.parentQueue = parentQueueName;
    }

    // Build JobNode tree - interleave sub-flow and leaf child nodes in original order
    const childNodes: JobNode[] = [];
    let leafIdx = 0;
    for (let i = 0; i < flow.children.length; i++) {
      if (childNodeMap.has(i)) {
        childNodes.push(childNodeMap.get(i)!);
      } else {
        const childId = ids[1 + leafIdx];
        const child = flow.children[i];
        const childJob = new Job(
          client,
          buildKeys(child.queueName, prefix),
          childId,
          child.name,
          child.data,
          child.opts ?? {},
        );
        childJob.timestamp = timestamp;
        childJob.parentId = parentId;
        childJob.parentQueue = parentQueueName;
        childNodes.push({ job: childJob });
        leafIdx++;
      }
    }

    const parentJob = new Job(
      client,
      parentKeys,
      parentId,
      flow.name,
      flow.data,
      parentOpts,
    );
    parentJob.timestamp = timestamp;

    return { job: parentJob, children: childNodes };
  }

  /**
   * Close the FlowProducer and release the underlying client connection.
   * Idempotent: safe to call multiple times.
   */
  async close(): Promise<void> {
    if (this.closing) return;
    this.closing = true;
    if (this.client) {
      this.client.close();
      this.client = null;
    }
  }
}
