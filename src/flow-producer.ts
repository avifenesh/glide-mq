import type { FlowProducerOptions, FlowJob, Client } from './types';
import { Job } from './job';
import { buildKeys, keyPrefix } from './utils';
import { createClient, ensureFunctionLibrary } from './connection';
import { LIBRARY_SOURCE, addFlow } from './functions/index';
import type { QueueKeys } from './functions/index';
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

    // First, recursively process children that themselves have children.
    // We need to build the direct children list for the addFlow call.
    // Children with sub-children become sub-flows (added bottom-up).
    const directChildren: FlowJob[] = [];
    const childNodeMap: Map<number, JobNode> = new Map();

    for (let i = 0; i < flow.children.length; i++) {
      const child = flow.children[i];
      if (child.children && child.children.length > 0) {
        // Sub-flow: add bottom-up recursively, then wire as dep of current parent.
        const subNode = await this.addFlowRecursive(client, child);
        childNodeMap.set(i, subNode);
      }
      directChildren.push(child);
    }

    // Build children data for addFlow - only include children without sub-children
    // (children with sub-children were already created recursively)
    const leafChildren: { index: number; child: FlowJob }[] = [];
    for (let i = 0; i < flow.children.length; i++) {
      if (!childNodeMap.has(i)) {
        leafChildren.push({ index: i, child: flow.children[i] });
      }
    }

    const timestamp = Date.now();
    const parentOpts = flow.opts ?? {};

    if (leafChildren.length > 0) {
      // Use addFlow for the parent + leaf children
      const childrenForLua = leafChildren.map(({ child }) => {
        const childKeys = buildKeys(child.queueName, prefix);
        const childOpts = child.opts ?? {};
        return {
          name: child.name,
          data: JSON.stringify(child.data),
          opts: JSON.stringify(childOpts),
          delay: childOpts.delay ?? 0,
          priority: childOpts.priority ?? 0,
          maxAttempts: childOpts.attempts ?? 0,
          keys: childKeys,
          queuePrefix: keyPrefix(prefix, child.queueName),
          parentQueueName: parentQueueName,
        };
      });

      // Build extra deps for sub-flow children (already created recursively)
      const extraDeps: string[] = [];
      for (const [i, subNode] of childNodeMap.entries()) {
        const child = flow.children[i];
        const childPrefix = keyPrefix(prefix, child.queueName);
        extraDeps.push(`${childPrefix}:${subNode.job.id}`);
      }

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

      // Build JobNode tree
      const parentJob = new Job(
        client,
        parentKeys,
        parentId,
        flow.name,
        flow.data,
        parentOpts,
      );
      parentJob.timestamp = timestamp;

      const childNodes: JobNode[] = [];
      let leafIdx = 0;
      for (let i = 0; i < flow.children.length; i++) {
        if (childNodeMap.has(i)) {
          childNodes.push(childNodeMap.get(i)!);
        } else {
          const childId = ids[1 + leafIdx];
          const child = flow.children[i];
          const childKeys = buildKeys(child.queueName, prefix);
          const childJob = new Job(
            client,
            childKeys,
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

      return { job: parentJob, children: childNodes };
    } else {
      // All children were sub-flows (already created).
      // Use addFlow with 0 leaf children but with extraDeps to create
      // the parent atomically with its deps set.
      const extraDeps: string[] = [];
      for (const [i, subNode] of childNodeMap.entries()) {
        const child = flow.children[i];
        const childPrefix = keyPrefix(prefix, child.queueName);
        extraDeps.push(`${childPrefix}:${subNode.job.id}`);
      }

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
        [],
        extraDeps,
      );

      const parentIdStr = ids[0];

      // Set parentId and parentQueue on pre-existing sub-flow children
      const childNodes: JobNode[] = [];
      for (const [i, subNode] of childNodeMap.entries()) {
        const child = flow.children[i];
        const childKeys = buildKeys(child.queueName, prefix);
        await client.hset(childKeys.job(subNode.job.id), {
          parentId: parentIdStr,
          parentQueue: parentQueueName,
        });
        subNode.job.parentId = parentIdStr;
        subNode.job.parentQueue = parentQueueName;
        childNodes.push(subNode);
      }

      const parentJob = new Job(
        client,
        parentKeys,
        parentIdStr,
        flow.name,
        flow.data,
        parentOpts,
      );
      parentJob.timestamp = timestamp;

      return { job: parentJob, children: childNodes };
    }
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
