import type { FlowProducerOptions, FlowJob, Client, Serializer } from './types';
import { JSON_SERIALIZER } from './types';
import { Job } from './job';
import { buildKeys, keyPrefix, MAX_JOB_DATA_SIZE } from './utils';
import { createClient, ensureFunctionLibrary, ensureFunctionLibraryOnce, isClusterClient } from './connection';
import { GlideMQError } from './errors';
import { LIBRARY_SOURCE, addFlow, addJob, completeChild } from './functions/index';
import { withSpan } from './telemetry';

export interface JobNode {
  job: Job;
  children?: JobNode[];
}

const INVALID_JOB_ID_CHARS = /[\x00-\x1f\x7f{}:]/;
function validateJobId(jobId: string): void {
  if (jobId.length > 256) throw new Error('jobId must be at most 256 characters');
  if (INVALID_JOB_ID_CHARS.test(jobId)) {
    throw new Error('jobId must not contain control characters, curly braces, or colons');
  }
}

const LATE_PARENT_RECONCILE_ATTEMPTS = 5;
const LATE_PARENT_RECONCILE_DELAY_MS = 10;

export class FlowProducer {
  private opts: FlowProducerOptions;
  private client: Client | null = null;
  private clientOwned = true;
  private closing = false;
  private serializer: Serializer;

  constructor(opts: FlowProducerOptions) {
    if (!opts.connection && !opts.client) {
      throw new GlideMQError('Either `connection` or `client` must be provided.');
    }
    this.opts = opts;
    this.serializer = opts.serializer ?? JSON_SERIALIZER;
  }

  /** @internal */
  private async getClient(): Promise<Client> {
    if (this.closing) {
      throw new GlideMQError('FlowProducer is closing');
    }
    if (!this.client) {
      if (this.opts.client) {
        const injected = this.opts.client;
        const clusterMode = this.opts.connection?.clusterMode ?? isClusterClient(injected);
        await ensureFunctionLibraryOnce(injected, LIBRARY_SOURCE, clusterMode);
        this.client = injected;
        this.clientOwned = false;
      } else {
        this.client = await createClient(this.opts.connection!);
        this.clientOwned = true;
        await ensureFunctionLibrary(this.client, LIBRARY_SOURCE, this.opts.connection!.clusterMode ?? false);
      }
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
      const timestamp = Date.now();
      const opts = flow.opts ?? {};
      const groupRateMax = opts.ordering?.rateLimit?.max ?? 0;
      const groupRateDuration = opts.ordering?.rateLimit?.duration ?? 0;
      const tbCapacity = opts.ordering?.tokenBucket ? Math.round(opts.ordering.tokenBucket.capacity * 1000) : 0;
      const tbRefillRate = opts.ordering?.tokenBucket ? Math.round(opts.ordering.tokenBucket.refillRate * 1000) : 0;
      const jobCost = opts.cost != null ? Math.round(opts.cost * 1000) : 0;
      if (opts.ttl != null && (!Number.isFinite(opts.ttl) || opts.ttl < 0)) {
        throw new Error('ttl must be a non-negative finite number');
      }
      let groupConcurrency = opts.ordering?.concurrency ?? 0;
      // Force group path when rate limit or token bucket is set
      if ((groupRateMax > 0 || tbCapacity > 0) && groupConcurrency < 1) {
        groupConcurrency = 1;
      }
      const serializedData = this.serializer.serialize(flow.data);
      const dataByteLen = Buffer.byteLength(serializedData, 'utf8');
      if (dataByteLen > MAX_JOB_DATA_SIZE) {
        throw new Error(
          `Job data exceeds maximum size (${dataByteLen} bytes > ${MAX_JOB_DATA_SIZE} bytes). Use smaller payloads or store large data externally.`,
        );
      }
      const customJobId = opts.jobId ?? '';
      if (customJobId !== '') validateJobId(customJobId);
      const jobId = await addJob(
        client,
        parentKeys,
        flow.name,
        serializedData,
        JSON.stringify(opts),
        timestamp,
        opts.delay ?? 0,
        opts.priority ?? 0,
        '',
        opts.attempts ?? 0,
        opts.ordering?.key ?? '',
        groupConcurrency,
        groupRateMax,
        groupRateDuration,
        tbCapacity,
        tbRefillRate,
        jobCost,
        opts.ttl ?? 0,
        customJobId,
      );
      if (String(jobId) === 'duplicate') {
        throw new Error('Duplicate job ID in flow');
      }
      if (String(jobId) === 'ERR:ID_EXHAUSTED') {
        throw new Error('Failed to generate job ID: too many collisions with custom job IDs');
      }
      if (String(jobId) === 'ERR:COST_EXCEEDS_CAPACITY') {
        throw new Error('Job cost exceeds token bucket capacity');
      }
      const job = new Job(client, parentKeys, String(jobId), flow.name, flow.data, opts, this.serializer);
      job.timestamp = timestamp;
      return { job };
    }

    // Early check: if parent has a custom ID, verify it doesn't already exist
    // before recursing into children (to avoid orphaning sub-flow children).
    const parentCustomId = (flow.opts ?? {}).jobId ?? '';
    if (parentCustomId !== '') {
      validateJobId(parentCustomId);
      const exists = await client.exists([parentKeys.job(parentCustomId)]);
      if (exists > 0) {
        throw new Error('Duplicate job ID in flow');
      }
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
        const childData = this.serializer.serialize(child.data);
        const childByteLen = Buffer.byteLength(childData, 'utf8');
        if (childByteLen > MAX_JOB_DATA_SIZE) {
          throw new Error(
            `Job data exceeds maximum size (${childByteLen} bytes > ${MAX_JOB_DATA_SIZE} bytes). Use smaller payloads or store large data externally.`,
          );
        }
        const childCustomId = childOpts.jobId ?? '';
        if (childCustomId !== '') validateJobId(childCustomId);
        return {
          name: child.name,
          data: childData,
          opts: JSON.stringify(childOpts),
          delay: childOpts.delay ?? 0,
          priority: childOpts.priority ?? 0,
          maxAttempts: childOpts.attempts ?? 0,
          keys: buildKeys(child.queueName, prefix),
          queuePrefix: keyPrefix(prefix, child.queueName),
          parentQueueName: parentQueueName,
          customId: childCustomId,
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

    const parentData = this.serializer.serialize(flow.data);
    const parentByteLen = Buffer.byteLength(parentData, 'utf8');
    if (parentByteLen > MAX_JOB_DATA_SIZE) {
      throw new Error(
        `Job data exceeds maximum size (${parentByteLen} bytes > ${MAX_JOB_DATA_SIZE} bytes). Use smaller payloads or store large data externally.`,
      );
    }
    const ids = await addFlow(
      client,
      parentKeys,
      flow.name,
      parentData,
      JSON.stringify(parentOpts),
      timestamp,
      parentOpts.delay ?? 0,
      parentOpts.priority ?? 0,
      parentOpts.attempts ?? 0,
      childrenForLua,
      extraDeps,
      parentCustomId,
    );

    if (ids[0] === 'duplicate') {
      throw new Error('Duplicate job ID in flow');
    }
    if (ids[0] === 'ERR:ID_EXHAUSTED') {
      throw new Error('Failed to generate job ID: too many collisions with custom job IDs');
    }
    const parentId = ids[0];

    // Set parentId and parentQueue on pre-existing sub-flow children
    for (const [i, subNode] of childNodeMap.entries()) {
      const child = flow.children[i];
      const childKeys = buildKeys(child.queueName, prefix);
      const depsMember = `${keyPrefix(prefix, child.queueName)}:${subNode.job.id}`;
      await client.hset(childKeys.job(subNode.job.id), {
        parentId: parentId,
        parentQueue: parentQueueName,
      });
      let state = await client.hget(childKeys.job(subNode.job.id), 'state');
      for (
        let attempt = 0;
        state && String(state) !== 'completed' && attempt < LATE_PARENT_RECONCILE_ATTEMPTS;
        attempt++
      ) {
        await new Promise<void>((resolve) => setTimeout(resolve, LATE_PARENT_RECONCILE_DELAY_MS));
        state = await client.hget(childKeys.job(subNode.job.id), 'state');
      }
      if (state && String(state) === 'completed') {
        await completeChild(client, parentKeys, parentId, depsMember);
      }
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
          this.serializer,
        );
        childJob.timestamp = timestamp;
        childJob.parentId = parentId;
        childJob.parentQueue = parentQueueName;
        childNodes.push({ job: childJob });
        leafIdx++;
      }
    }

    const parentJob = new Job(client, parentKeys, parentId, flow.name, flow.data, parentOpts, this.serializer);
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
      if (this.clientOwned) {
        this.client.close();
      }
      this.client = null;
    }
  }
}
