import type { FlowProducerOptions, FlowJob, DAGFlow, DAGNode, Client, Serializer, BudgetOptions } from './types';
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

/**
 * Creates parent-child job flows and DAG workflows.
 * Children are processed first; the parent runs after all children complete.
 */
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
   *
   * When `flowOpts.budget` is provided, a budget hash is created in Valkey and
   * a `budgetKey` field is written to the parent and all child job hashes.
   * Workers check this key before processing and after completion to enforce
   * token and cost caps across the entire flow.
   */
  async add(flow: FlowJob, flowOpts?: { budget?: BudgetOptions }): Promise<JobNode> {
    return withSpan(
      'glide-mq.flow.add',
      {
        'glide-mq.queue': flow.queueName,
        'glide-mq.flow.name': flow.name,
        'glide-mq.flow.childCount': flow.children?.length ?? 0,
      },
      async () => {
        const client = await this.getClient();
        const result = await this.addFlowRecursive(client, flow);

        if (flowOpts?.budget) {
          const prefix = this.opts.prefix ?? 'glide';
          const budgetKey = `${prefix}:{${flow.queueName}}:budget:${result.job.id}`;
          const budgetFields: Record<string, string> = {
            usedTokens: '0',
            usedCost: '0',
            exceeded: '0',
            onExceeded: flowOpts.budget.onExceeded ?? 'fail',
          };
          if (flowOpts.budget.maxTotalTokens != null) {
            budgetFields.maxTotalTokens = flowOpts.budget.maxTotalTokens.toString();
          }
          if (flowOpts.budget.maxTokens != null) {
            budgetFields.maxTokens = JSON.stringify(flowOpts.budget.maxTokens);
          }
          if (flowOpts.budget.tokenWeights != null) {
            budgetFields.tokenWeights = JSON.stringify(flowOpts.budget.tokenWeights);
          }
          if (flowOpts.budget.maxTotalCost != null) {
            budgetFields.maxTotalCost = flowOpts.budget.maxTotalCost.toString();
          }
          if (flowOpts.budget.maxCosts != null) {
            budgetFields.maxCosts = JSON.stringify(flowOpts.budget.maxCosts);
          }
          if (flowOpts.budget.costUnit != null) {
            budgetFields.costUnit = flowOpts.budget.costUnit;
          }
          await client.hset(budgetKey, budgetFields);

          // Write budgetKey to all jobs in the flow
          await this.propagateBudgetKey(client, result, flow, budgetKey, prefix);
        }

        return result;
      },
    );
  }

  /**
   * Recursively write budgetKey to every job in a flow tree.
   * Walks the FlowJob tree in parallel with the JobNode tree to access queue names.
   */
  private async propagateBudgetKey(
    client: Client,
    node: JobNode,
    flowDef: FlowJob,
    budgetKey: string,
    prefix: string,
  ): Promise<void> {
    const keys = buildKeys(flowDef.queueName, prefix);
    await client.hset(keys.job(node.job.id), { budgetKey });
    node.job.budgetKey = budgetKey;
    if (node.children && flowDef.children) {
      for (let i = 0; i < node.children.length; i++) {
        const childNode = node.children[i];
        const childFlow = flowDef.children[i];
        await this.propagateBudgetKey(client, childNode, childFlow, budgetKey, prefix);
      }
    }
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
      if (opts.ordering?.key && groupConcurrency < 1) {
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

        // Semantic: DAGNode.deps means "nodes that must complete before this
        // node runs". Mapping to BullMQ flow primitives:
        //   - A node WITH deps is a state=waiting-children PARENT that
        //     aggregates its deps as CHILDREN. Children run first; when all
        //     complete they notify the parent and the parent transitions to
        //     runnable.
        //   - A node WITHOUT deps is a leaf-or-root that runs immediately.
        //
        // Build a reverse-deps map: dependents[X] = nodes whose deps contain X.
        // Those are X's BullMQ-parents (they wait for X). A leaf node uses its
        // first dependent as its addJob parentId so it can notify on completion;
        // additional dependents are wired via registerParent.
        const dependents = new Map<string, string[]>();
        for (const node of dag.nodes) {
          dependents.set(node.name, []);
        }
        for (const node of dag.nodes) {
          if (node.deps) {
            for (const dep of node.deps) {
              dependents.get(dep)!.push(node.name);
            }
          }
        }

        // Submit in REVERSE topological order so each node's BullMQ-parents
        // (its dependents in user terms) already exist by the time we wire the
        // notification edge via registerParent. topoSort returns deps-first
        // (in-degree 0 first); reversing gives most-deps first.
        for (const node of [...sorted].reverse()) {
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
          let groupConcurrency = opts.ordering?.concurrency ?? 0;
          if (opts.ordering?.key && groupConcurrency < 1) {
            groupConcurrency = 1;
          }
          const groupRateMax = opts.ordering?.rateLimit?.max ?? 0;
          const groupRateDuration = opts.ordering?.rateLimit?.duration ?? 0;
          const tbCapacity = opts.ordering?.tokenBucket ? Math.round(opts.ordering.tokenBucket.capacity * 1000) : 0;
          const tbRefillRate = opts.ordering?.tokenBucket ? Math.round(opts.ordering.tokenBucket.refillRate * 1000) : 0;
          const jobCost = opts.cost != null ? Math.round(opts.cost * 1000) : 0;

          const myDependents = dependents.get(node.name) ?? [];

          if (deps.length > 0) {
            // Node has deps -> it's a waiting-children PARENT that aggregates
            // its deps as children. addFlow with 0 children sets
            // state=waiting-children; deps register themselves as children when
            // they're submitted later in this loop (they come AFTER us in the
            // reversed topo order).
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

            // If this node is itself someone else's dep, those dependents
            // already exist (reverse-topo). Register us as a child of each
            // dependent so when we complete we notify them.
            if (myDependents.length > 0) {
              await wireDependents(node, jobId, myDependents);
              // Set parentIds metadata so completeAndFetchNext knows to read
              // the parents SET (it short-circuits the SMEMBERS when this is
              // empty, and addFlow doesn't set parentId/parentIds itself).
              const pIds = myDependents.map((d) => result.get(d)!.id);
              const pQueues = myDependents.map((d) => dag.nodes.find((n) => n.name === d)!.queueName);
              job.parentIds = pIds;
              job.parentQueues = pQueues;
              await client.hset(queueKeys.job(jobId), {
                parentIds: JSON.stringify(pIds),
                parentQueues: JSON.stringify(pQueues),
              });
            }
          } else if (myDependents.length > 0) {
            // Leaf in user terms (no deps -> runs immediately) but other
            // nodes wait for it. Use the first dependent as the addJob
            // parentId so completion notifies it; registerParent for the rest.
            const firstDepName = myDependents[0];
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
              opts.lifo ? 1 : 0,
              firstParentNode.queueName,
              firstParentKeys.deps(firstParentJob.id),
              '',
            );
            if (String(jobId) === 'duplicate') throw new Error('Duplicate job ID in DAG');
            if (String(jobId) === 'ERR:ID_EXHAUSTED') throw new Error('Failed to generate job ID');
            const jid = String(jobId);
            const job = new Job(client, queueKeys, jid, node.name, node.data, opts, this.serializer);
            job.timestamp = timestamp;
            job.parentId = firstParentJob.id;
            job.parentQueue = firstParentNode.queueName;
            result.set(node.name, job);

            // Wire any additional dependents (2nd+) and persist parent metadata.
            if (myDependents.length > 1) {
              const pIds = myDependents.map((d) => result.get(d)!.id);
              const pQueues = myDependents.map((d) => dag.nodes.find((n) => n.name === d)!.queueName);
              job.parentIds = pIds;
              job.parentQueues = pQueues;
              await client.hset(queueKeys.job(jid), {
                parentIds: JSON.stringify(pIds),
                parentQueues: JSON.stringify(pQueues),
              });

              for (let p = 1; p < myDependents.length; p++) {
                const depName = myDependents[p];
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
                    `DAG partially submitted: registerParent failed for ${node.name}->${depName}: ${msg}. Graph may be in inconsistent state.`,
                  );
                }
                if (registerResult.startsWith('error:')) {
                  throw new GlideMQError(`Failed to register dependent ${depName} for node ${node.name}: ${registerResult}`);
                }
              }
            }
          } else {
            // Standalone node: no deps, no dependents - simple addJob.
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
          }
        }

        async function wireDependents(node: DAGNode, jobId: string, myDependents: string[]) {
          if (myDependents.length === 0) return;
          const queueKeys = buildKeys(node.queueName, prefix);
          const queuePrefix = keyPrefix(prefix, node.queueName);
          for (const depName of myDependents) {
            const parentJob = result.get(depName)!;
            const parentNode = dag.nodes.find((n) => n.name === depName)!;
            const parentQueueKeys = buildKeys(parentNode.queueName, prefix);
            const depsMember = `${queuePrefix}:${jobId}`;
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
                `DAG partially submitted: registerParent failed for ${node.name}->${depName}: ${msg}. Graph may be in inconsistent state.`,
              );
            }
            if (registerResult.startsWith('error:')) {
              throw new GlideMQError(`Failed to register dependent ${depName} for node ${node.name}: ${registerResult}`);
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
