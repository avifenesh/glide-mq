import type { FlowProducerOptions, FlowJob, DAGFlow, Client, Serializer } from './types';
import { JSON_SERIALIZER } from './types';
import { Job } from './job';
import { buildKeys, keyPrefix, MAX_JOB_DATA_SIZE, validateJobId, validateQueueName } from './utils';
import { createClient, ensureFunctionLibrary, ensureFunctionLibraryOnce, isClusterClient } from './connection';
import { GlideMQError } from './errors';
import { LIBRARY_SOURCE, addFlow, addJob, completeChild, registerParent } from './functions/index';
import { withSpan } from './telemetry';
import { validateDAG, topoSort } from './dag-utils';

export interface JobNode {
  job: Job;
  children?: JobNode[];
}

/**
 * Race-condition constants for late-parent reconciliation in addFlowRecursive.
 *
 * Children are created before the parent job exists. Between child creation and
 * parent creation, a worker may pick up and complete a child job. When that
 * happens, the child's completion Lua script cannot notify the parent (it does
 * not exist yet). The reconcile loop below polls the child state after parent
 * creation; if the child has already completed, it manually calls completeChild
 * so the parent's dependency counter is incremented and the parent can proceed.
 */
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
   * Uses Promise.all to process independent flow trees concurrently,
   * eliminating sequential I/O bottlenecks when adding many flows.
   */
  async addBulk(flows: FlowJob[]): Promise<JobNode[]> {
    const client = await this.getClient();
    return Promise.all(flows.map((flow) => this.addFlowRecursive(client, flow)));
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
      if (opts.lifo && opts.ordering?.key) {
        throw new Error('lifo and ordering.key cannot be used together');
      }
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
        opts.lifo ? 1 : 0,
        '',
        '',
        '',
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
        if (childOpts.lifo && childOpts.ordering?.key) {
          throw new Error(`Flow child "${child.name}": lifo and ordering.key cannot be used together`);
        }
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
   * Add a DAG (Directed Acyclic Graph) flow where jobs can have multiple parents.
   * Validates the graph for cycles, performs topological sort, and submits nodes
   * bottom-up (leaves first). For nodes with multiple parents, registers each
   * parent dependency.
   *
   * Returns a map of node name to Job instance.
   */
  async addDAG(dag: DAGFlow): Promise<Map<string, Job>> {
    return withSpan(
      'glide-mq.flow.addDAG',
      {
        'glide-mq.flow.nodeCount': dag.nodes.length,
      },
      async () => {
        // Validate the DAG and get submission order (leaves first)
        validateDAG(dag.nodes);
        const sorted = topoSort(dag.nodes);

        const client = await this.getClient();
        const prefix = this.opts.prefix ?? 'glide';
        const result = new Map<string, Job>();

        // Track which nodes have dependents (children) - they become "parents" in waiting-children
        const hasChildren = new Set<string>();
        for (const node of dag.nodes) {
          if (node.deps) {
            for (const dep of node.deps) {
              hasChildren.add(dep);
            }
          }
        }

        // Submit nodes in topological order (leaves first)
        // Nodes with 0 deps: simple addJob
        // Nodes with 1 dep: addJob with parent (traditional single-parent)
        // Nodes with 2+ deps: addJob with first parent, then registerParent for rest
        // Nodes that ARE parents (have children depending on them): created in waiting-children
        for (const node of sorted) {
          validateQueueName(node.queueName);
          const queueKeys = buildKeys(node.queueName, prefix);
          const opts = node.opts ?? {};
          const timestamp = Date.now();
          const serializedData = this.serializer.serialize(node.data);
          const dataByteLen = Buffer.byteLength(serializedData, 'utf8');
          if (dataByteLen > MAX_JOB_DATA_SIZE) {
            throw new Error(
              `Job data exceeds maximum size (${dataByteLen} bytes > ${MAX_JOB_DATA_SIZE} bytes). Use smaller payloads or store large data externally.`,
            );
          }

          const customJobId = opts.jobId ?? '';
          if (customJobId !== '') {
            if (customJobId.length > 256) throw new Error('jobId must be at most 256 characters');
            if (/[\x00-\x1f\x7f{}:]/.test(customJobId)) {
              throw new Error('jobId must not contain control characters, curly braces, or colons');
            }
          }

          const deps = node.deps ?? [];
          const isParent = hasChildren.has(node.name);

          if (opts.lifo && opts.ordering?.key) {
            throw new GlideMQError('lifo and ordering.key cannot be combined in a DAG node');
          }

          // Validate ordering.key if present (reserved chars: {, }, :)
          const orderingKey = opts.ordering?.key;
          if (orderingKey) {
            validateQueueName(orderingKey);
            if (orderingKey === '__') {
              throw new GlideMQError("Ordering key '__' is reserved as an internal sentinel.");
            }
          }

          // Extract rate-limit and token bucket params
          const groupConcurrency = opts.ordering?.concurrency ?? 0;
          const groupRateMax = opts.ordering?.rateLimit?.max ?? 0;
          const groupRateDuration = opts.ordering?.rateLimit?.duration ?? 0;
          const tbCapacity = opts.ordering?.tokenBucket ? Math.round(opts.ordering.tokenBucket.capacity * 1000) : 0;
          const tbRefillRate = opts.ordering?.tokenBucket ? Math.round(opts.ordering.tokenBucket.refillRate * 1000) : 0;
          const jobCost = opts.cost != null ? Math.round(opts.cost * 1000) : 0;

          if (isParent && deps.length === 0) {
            // Node is a parent with no deps of its own - use addFlow with no children (empty flow)
            // Actually, we create it as waiting-children and let children register later
            // Use addFlow with 0 children to set state=waiting-children
            const ids = await addFlow(
              client,
              queueKeys,
              node.name,
              serializedData,
              JSON.stringify(opts),
              timestamp,
              opts.delay ?? 0,
              opts.priority ?? 0,
              opts.attempts ?? 0,
              [],
              [],
              customJobId,
            );
            if (ids[0] === 'duplicate') throw new Error('Duplicate job ID in DAG');
            if (ids[0] === 'ERR:ID_EXHAUSTED') throw new Error('Failed to generate job ID');
            const job = new Job(client, queueKeys, ids[0], node.name, node.data, opts, this.serializer);
            job.timestamp = timestamp;
            result.set(node.name, job);
          } else if (isParent && deps.length > 0) {
            // Node is both a child (has deps) and a parent (has dependents)
            // Create as waiting-children via addFlow with 0 children
            const ids = await addFlow(
              client,
              queueKeys,
              node.name,
              serializedData,
              JSON.stringify(opts),
              timestamp,
              opts.delay ?? 0,
              opts.priority ?? 0,
              opts.attempts ?? 0,
              [],
              [],
              customJobId,
            );
            if (ids[0] === 'duplicate') throw new Error('Duplicate job ID in DAG');
            if (ids[0] === 'ERR:ID_EXHAUSTED') throw new Error('Failed to generate job ID');
            const jobId = ids[0];
            const job = new Job(client, queueKeys, jobId, node.name, node.data, opts, this.serializer);
            job.timestamp = timestamp;
            result.set(node.name, job);

            // Register all parent dependencies
            for (const depName of deps) {
              const parentJob = result.get(depName)!;
              const parentNode = dag.nodes.find((n) => n.name === depName)!;
              const parentQueueKeys = buildKeys(parentNode.queueName, prefix);
              const depsMember = `${keyPrefix(prefix, node.queueName)}:${jobId}`;

              let registerResult: string;
              try {
                registerResult = await registerParent(
                  client,
                  queueKeys,
                  jobId,
                  parentJob.id,
                  keyPrefix(prefix, parentNode.queueName),
                  parentQueueKeys,
                  depsMember,
                );
              } catch (regErr) {
                const msg = regErr instanceof Error ? regErr.message : String(regErr);
                throw new GlideMQError(
                  `DAG partially submitted: registerParent failed for ${depName}->${node.name}: ${msg}. Graph may be in inconsistent state.`,
                );
              }

              if (registerResult.startsWith('error:')) {
                throw new GlideMQError(`Failed to register parent ${depName} for node ${node.name}: ${registerResult}`);
              }
            }

            // Store parent info on the job
            const pIds = deps.map((d) => result.get(d)!.id);
            const pQueues = deps.map((d) => dag.nodes.find((n) => n.name === d)!.queueName);
            job.parentId = pIds[0];
            job.parentQueue = pQueues[0];
            if (deps.length > 1) {
              job.parentIds = pIds;
              job.parentQueues = pQueues;
              await client.hset(queueKeys.job(jobId), {
                parentId: pIds[0],
                parentQueue: pQueues[0],
                parentIds: JSON.stringify(pIds),
                parentQueues: JSON.stringify(pQueues),
              });
            } else {
              await client.hset(queueKeys.job(jobId), {
                parentId: pIds[0],
                parentQueue: pQueues[0],
              });
            }
          } else if (deps.length === 0) {
            // Leaf node with no deps and no children - simple addJob
            const jobId = await addJob(
              client,
              queueKeys,
              node.name,
              serializedData,
              JSON.stringify(opts),
              timestamp,
              opts.delay ?? 0,
              opts.priority ?? 0,
              '',
              opts.attempts ?? 0,
              orderingKey ?? '',
              groupConcurrency,
              groupRateMax,
              groupRateDuration,
              tbCapacity,
              tbRefillRate,
              jobCost,
              opts.ttl ?? 0,
              customJobId,
              opts.lifo ? 1 : 0,
              '',
              '',
              '',
            );
            if (String(jobId) === 'duplicate') throw new Error('Duplicate job ID in DAG');
            if (String(jobId) === 'ERR:ID_EXHAUSTED') throw new Error('Failed to generate job ID');
            const job = new Job(client, queueKeys, String(jobId), node.name, node.data, opts, this.serializer);
            job.timestamp = timestamp;
            result.set(node.name, job);
          } else {
            // Non-parent node with deps (terminal/sink node) - add as regular job
            // Dependency semantics: deps are the PARENTS this child waits for.
            // The child (current node) becomes a dependency of the parent via the deps SET.
            // Use first dep as the primary parent for backward compatibility
            const firstDepName = deps[0];
            const firstParentJob = result.get(firstDepName)!;
            const firstParentNode = dag.nodes.find((n) => n.name === firstDepName)!;
            const firstParentKeys = buildKeys(firstParentNode.queueName, prefix);
            const queuePrefix = keyPrefix(prefix, node.queueName);

            const jobId = await addJob(
              client,
              queueKeys,
              node.name,
              serializedData,
              JSON.stringify(opts),
              timestamp,
              opts.delay ?? 0,
              opts.priority ?? 0,
              firstParentJob.id,
              opts.attempts ?? 0,
              orderingKey ?? '',
              groupConcurrency,
              groupRateMax,
              groupRateDuration,
              tbCapacity,
              tbRefillRate,
              jobCost,
              opts.ttl ?? 0,
              customJobId,
              0,
              firstParentNode.queueName,
              firstParentKeys.deps(firstParentJob.id),
            );
            if (String(jobId) === 'duplicate') throw new Error('Duplicate job ID in DAG');
            if (String(jobId) === 'ERR:ID_EXHAUSTED') throw new Error('Failed to generate job ID');
            const jid = String(jobId);
            const job = new Job(client, queueKeys, jid, node.name, node.data, opts, this.serializer);
            job.timestamp = timestamp;
            job.parentId = firstParentJob.id;
            job.parentQueue = firstParentNode.queueName;
            result.set(node.name, job);

            // Register additional parents (2nd, 3rd, etc.)
            if (deps.length > 1) {
              const pIds = deps.map((d) => result.get(d)!.id);
              const pQueues = deps.map((d) => dag.nodes.find((n) => n.name === d)!.queueName);
              job.parentIds = pIds;
              job.parentQueues = pQueues;
              await client.hset(queueKeys.job(jid), {
                parentIds: JSON.stringify(pIds),
                parentQueues: JSON.stringify(pQueues),
              });

              for (let p = 1; p < deps.length; p++) {
                const depName = deps[p];
                const parentJob = result.get(depName)!;
                const parentNode = dag.nodes.find((n) => n.name === depName)!;
                const parentQueueKeys = buildKeys(parentNode.queueName, prefix);
                const depsMember = `${queuePrefix}:${jid}`;

                let registerResult: string;
                try {
                  registerResult = await registerParent(
                    client,
                    queueKeys,
                    jid,
                    parentJob.id,
                    keyPrefix(prefix, parentNode.queueName),
                    parentQueueKeys,
                    depsMember,
                  );
                } catch (regErr) {
                  const msg = regErr instanceof Error ? regErr.message : String(regErr);
                  throw new GlideMQError(
                    `DAG partially submitted: registerParent failed for ${depName}->${node.name}: ${msg}. Graph may be in inconsistent state.`,
                  );
                }

                if (registerResult.startsWith('error:')) {
                  throw new GlideMQError(
                    `Failed to register parent ${depName} for node ${node.name}: ${registerResult}`,
                  );
                }
              }
            }
          }
        }

        return result;
      },
    );
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
