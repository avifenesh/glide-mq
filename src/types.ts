import type { GlideClient, GlideClusterClient, ReadFrom } from '@glidemq/speedkey';

export type Client = GlideClient | GlideClusterClient;

export type { ReadFrom } from '@glidemq/speedkey';

/** Standard password-based credentials. */
export interface PasswordCredentials {
  username?: string;
  password: string;
}

/** IAM authentication credentials for AWS ElastiCache/MemoryDB. */
export interface IamCredentials {
  type: 'iam';
  /** ElastiCache or MemoryDB. */
  serviceType: 'elasticache' | 'memorydb';
  /** AWS region (e.g. 'us-east-1'). */
  region: string;
  /** The IAM user ID used for authentication. Maps to username in Valkey AUTH. */
  userId: string;
  /** The ElastiCache/MemoryDB cluster name. */
  clusterName: string;
  /** Token refresh interval in seconds. Defaults to 300 (5 min). */
  refreshIntervalSeconds?: number;
}

export interface ConnectionOptions {
  addresses: { host: string; port: number }[];
  useTLS?: boolean;
  credentials?: PasswordCredentials | IamCredentials;
  clusterMode?: boolean;
  /**
   * Read strategy for the client. Controls how read commands are routed.
   * - 'primary': Always read from primary (default).
   * - 'preferReplica': Round-robin across replicas, fallback to primary.
   * - 'AZAffinity': Route reads to replicas in the same availability zone.
   * - 'AZAffinityReplicasAndPrimary': Route reads to any node in the same AZ.
   *
   * AZ-based strategies require `clientAz` to be set.
   */
  readFrom?: ReadFrom;
  /**
   * Availability zone of the client (e.g., 'us-east-1a').
   * Used with readFrom 'AZAffinity' or 'AZAffinityReplicasAndPrimary' to route
   * read commands to nodes in the same AZ, reducing cross-AZ latency and cost.
   */
  clientAz?: string;
  /**
   * Maximum concurrent in-flight requests per client connection.
   * Passed through to GLIDE. Default: 1000.
   */
  inflightRequestsLimit?: number;
}

export interface DeadLetterQueueOptions {
  /** Queue name to use as the dead letter queue. */
  name: string;
  /** Max retries before moving to DLQ. If not set, uses the job's own attempts config. */
  maxRetries?: number;
}

export interface QueueOptions {
  /** Connection options for creating a new client. Required unless `client` is provided. */
  connection?: ConnectionOptions;
  /**
   * Pre-existing GLIDE client for non-blocking commands.
   * When provided, the component does NOT own this client - close() will not destroy it.
   * Must not be used for blocking reads (XREADGROUP BLOCK / XREAD BLOCK).
   */
  client?: Client;
  prefix?: string;
  /** Dead letter queue configuration. Jobs that exhaust retries are moved here. */
  deadLetterQueue?: DeadLetterQueueOptions;
  /** Enable transparent compression of job data. Default: 'none'. */
  compression?: 'none' | 'gzip';
  /**
   * Custom serializer for job data and return values. Default: JSON.
   *
   * **Important**: The same serializer must be used across all Queue, Worker,
   * and FlowProducer instances that operate on the same queue. A mismatch
   * causes silent data corruption - the consumer will see `{}` and the job's
   * `deserializationFailed` flag will be `true`.
   */
  serializer?: Serializer;
}

export interface SandboxOptions {
  /** Use worker_threads (default: true). When false, uses child_process.fork. */
  useWorkerThreads?: boolean;
  /** Maximum number of concurrent sandbox workers. Defaults to the Worker concurrency. */
  maxWorkers?: number;
}

export interface WorkerOptions extends QueueOptions {
  /**
   * Pre-existing GLIDE client for non-blocking commands (alias for `client`).
   * The blocking client for XREADGROUP is always auto-created from `connection`.
   * `connection` is required even when this is set.
   * Provide either `commandClient` or `client`, not both.
   */
  commandClient?: Client;
  concurrency?: number;
  globalConcurrency?: number;
  prefetch?: number;
  blockTimeout?: number;
  stalledInterval?: number;
  maxStalledCount?: number;
  promotionInterval?: number;
  limiter?: { max: number; duration: number };
  backoffStrategies?: Record<string, (attemptsMade: number, err: Error) => number>;
  /** Lock duration in ms. The worker sends a heartbeat every lockDuration/2.
   *  Jobs with a recent heartbeat are not reclaimed as stalled.
   *  Default: 30000 (30s). */
  lockDuration?: number;
  /** Sandbox options for file-path processors. Only used when processor is a string. */
  sandbox?: SandboxOptions;
  /** Enable batch processing. When set, the processor receives an array of jobs. */
  batch?: BatchOptions;
  /** Emit events to Valkey event stream on job completion/activation. Default: true.
   *  Set to false to skip XADD events in hot path (~1 fewer redis.call per job).
   *  TS-side EventEmitter ('completed', 'failed', etc.) is unaffected. */
  events?: boolean;
  /** Record per-minute timing metrics in Valkey on job completion. Default: true.
   *  Set to false to skip HINCRBY metrics recording (~1-2 fewer redis.call per job). */
  metrics?: boolean;
}

export interface BroadcastOptions extends QueueOptions {
  /** Max messages to retain in stream (must be a positive integer). Trimmed exactly (hard limit) on each publish. Opt-in; no trimming by default. */
  maxMessages?: number;
}

export interface BroadcastWorkerOptions extends WorkerOptions {
  /** Subscription name - becomes the consumer group name. Required for broadcast workers. */
  subscription: string;
  /**
   * Stream ID to start from when creating this subscription.
   * - '$': Only new messages (default)
   * - '0-0': All history (backfill)
   * - '<stream-id>': Start from specific ID
   */
  startFrom?: string;
  /**
   * Subject patterns to filter messages. Only messages whose subject matches
   * at least one pattern are delivered to the processor. Non-matching messages
   * are auto-acknowledged and skipped (zero wasted HGETALL calls).
   *
   * Pattern syntax (dot-separated segments):
   * - `*` matches exactly one segment
   * - `>` matches one or more trailing segments (must be last token)
   * - Literal segments match exactly
   *
   * Examples:
   * - `'projects.>'` matches `'projects.1'`, `'projects.1.issues.2'`
   * - `'projects.*'` matches `'projects.1'` but not `'projects.1.issues.2'`
   * - `'projects.*.issues.>'` matches `'projects.1.issues.2'`
   *
   * When omitted, all messages are delivered (no filtering).
   */
  subjects?: string[];
}

export interface JobOptions {
  /**
   * Custom job ID. Max 256 characters, must not contain control characters,
   * curly braces, or colons. If a job with this ID already exists, Queue.add returns null
   * and FlowProducer.add throws. When combined with deduplication, the dedup
   * check runs first.
   */
  jobId?: string;
  delay?: number;
  priority?: number;
  /** Process jobs in LIFO (last-in-first-out) order. Cannot be combined with ordering keys. */
  lifo?: boolean;
  /**
   * Per-key ordering and group concurrency control.
   * Jobs sharing the same key are constrained to run at most `concurrency`
   * instances simultaneously across all workers.
   * When concurrency is 1 (default), jobs run sequentially in enqueue order.
   * When concurrency > 1, up to N jobs per key run in parallel.
   */
  ordering?: {
    key: string;
    /** Max concurrent jobs for this ordering key. Default: 1 (sequential). */
    concurrency?: number;
    /** Per-group rate limit: max N jobs per time window for this ordering key. */
    rateLimit?: RateLimitConfig;
    /** Cost-based token bucket: capacity + refill rate. Jobs consume tokens based on cost. */
    tokenBucket?: TokenBucketConfig;
  };
  /** Job cost in tokens for token bucket rate limiting. Default: 1. */
  cost?: number;
  attempts?: number;
  backoff?: { type: 'fixed' | 'exponential' | string; delay: number; jitter?: number };
  timeout?: number;
  removeOnComplete?: boolean | number | { age: number; count: number };
  removeOnFail?: boolean | number | { age: number; count: number };
  deduplication?: { id: string; ttl?: number; mode?: 'simple' | 'throttle' | 'debounce' };
  parent?: { queue: string; id: string };
  /**
   * Multiple parent dependencies for DAG flows.
   * When set, this job waits for ALL parents to complete before it can run.
   * Each parent tracks this job as a child in its deps SET.
   * Mutually exclusive with `parent` - use one or the other.
   */
  parents?: Array<{ queue: string; id: string }>;
  /** Time-to-live in milliseconds. Jobs not processed within this window are failed as 'expired'. */
  ttl?: number;
}

export interface AddAndWaitOptions extends JobOptions {
  /** Maximum time to wait for a completed/failed event before rejecting. Default: 30000ms. */
  waitTimeout?: number;
}

export interface RateLimitConfig {
  /** Maximum jobs allowed within the time window. */
  max: number;
  /** Time window in milliseconds. */
  duration: number;
}

export interface TokenBucketConfig {
  /** Maximum bucket capacity in tokens (burst size). */
  capacity: number;
  /** Refill rate in tokens per second. */
  refillRate: number;
}

/**
 * Custom serializer for job data and return values.
 *
 * Implementations must satisfy the roundtrip invariant:
 * `deserialize(serialize(value))` must produce a value equivalent to `value`
 * for all values the application stores in jobs.
 *
 * Both methods must be synchronous. If `serialize` throws, the job is treated
 * as a processor failure (in Worker) or skipped (in Scheduler).
 */
export interface Serializer {
  /** Serialize a value to a string for storage in Valkey. */
  serialize(data: unknown): string;
  /** Deserialize a string from Valkey back to a value. */
  deserialize(raw: string): unknown;
}

/** Default JSON serializer used when no custom serializer is provided. */
export const JSON_SERIALIZER: Serializer = {
  serialize: (data) => JSON.stringify(data),
  deserialize: (raw) =>
    JSON.parse(raw, (k, v) => (k === '__proto__' || k === 'constructor' || k === 'prototype' ? undefined : v)),
};

export interface JobData {
  [key: string]: unknown;
}

export type Processor<D = any, R = any> = (job: import('./job').Job<D, R>) => Promise<R>;

export interface BatchOptions {
  /** Maximum number of jobs to collect before invoking the batch processor. */
  size: number;
  /** Maximum time in ms to wait for a full batch. If not set, processes whatever is available immediately. */
  timeout?: number;
}

export type BatchProcessor<D = any, R = any> = (jobs: import('./job').Job<D, R>[]) => Promise<R[]>;

export interface FlowJob {
  name: string;
  queueName: string;
  data: any;
  opts?: JobOptions;
  children?: FlowJob[];
}

export interface FlowProducerOptions {
  /** Connection options for creating a new client. Required unless `client` is provided. */
  connection?: ConnectionOptions;
  /**
   * Pre-existing GLIDE client for non-blocking commands.
   * When provided, the component does NOT own this client - close() will not destroy it.
   */
  client?: Client;
  prefix?: string;
  /**
   * Custom serializer for job data and return values. Default: JSON.
   *
   * **Important**: Must match the serializer used by the corresponding Queue
   * and Worker. A mismatch causes silent data corruption.
   */
  serializer?: Serializer;
}

export interface QueueEventsOptions {
  connection: ConnectionOptions;
  prefix?: string;
  /** Starting stream ID. Defaults to '$' (new events only). Use '0' for historical replay. */
  lastEventId?: string;
  /** XREAD BLOCK timeout in milliseconds. Defaults to 5000. */
  blockTimeout?: number;
}

export interface ScheduleOpts {
  /** Cron pattern (5 fields: minute hour dayOfMonth month dayOfWeek) */
  pattern?: string;
  /** Repeat interval in milliseconds */
  every?: number;
  /**
   * Schedule next job N ms after the current one completes (or terminally fails).
   * Mutually exclusive with `pattern` and `every`.
   */
  repeatAfterComplete?: number;
  /** IANA timezone for cron patterns (e.g. 'America/New_York'). Defaults to UTC. */
  tz?: string;
  /** Earliest time the scheduler may create a job. Accepts a Date or epoch milliseconds. */
  startDate?: Date | number;
  /** Latest scheduled run time allowed before the scheduler auto-removes itself. */
  endDate?: Date | number;
  /** Maximum number of jobs to create before the scheduler auto-removes itself. */
  limit?: number;
}

export interface JobTemplate {
  name?: string;
  data?: any;
  opts?: Omit<JobOptions, 'delay' | 'deduplication' | 'parent'>;
}

export interface SchedulerEntry {
  pattern?: string;
  every?: number;
  /** Delay in ms after completion before scheduling the next job. */
  repeatAfterComplete?: number;
  /** IANA timezone for cron patterns (e.g. 'America/New_York'). Defaults to UTC. */
  tz?: string;
  startDate?: number;
  endDate?: number;
  limit?: number;
  iterationCount?: number;
  template?: JobTemplate;
  lastRun?: number;
  nextRun: number;
}

export interface MetricsDataPoint {
  /** Minute-bucket epoch ms (floored to start of minute). */
  timestamp: number;
  /** Number of jobs completed/failed in this bucket. */
  count: number;
  /** Average processing duration in ms for this bucket. */
  avgDuration: number;
}

export interface MetricsOptions {
  /** Start index for data points (default 0). */
  start?: number;
  /** End index for data points (default -1 = all). */
  end?: number;
}

export interface Metrics {
  /** Total count of completed or failed jobs. */
  count: number;
  /** Per-minute data points sorted oldest-first. */
  data: MetricsDataPoint[];
  /** Resolution metadata. */
  meta: { resolution: 'minute' };
}

export interface JobCounts {
  waiting: number;
  active: number;
  delayed: number;
  completed: number;
  failed: number;
}

export interface GetJobsOptions {
  /** When true, excludes `data` and `returnvalue` fields from returned jobs. */
  excludeData?: boolean;
}

export interface SearchJobsOptions {
  state?: 'waiting' | 'active' | 'delayed' | 'completed' | 'failed';
  name?: string;
  data?: Record<string, unknown>;
  limit?: number;
  /** When true, excludes `data` and `returnvalue` fields from returned jobs. */
  excludeData?: boolean;
}

export interface WorkerInfo {
  id: string;
  addr: string;
  pid: number;
  startedAt: number;
  age: number;
  activeJobs: number;
}

/**
 * A node in a DAG flow. Each node is a job with optional dependencies on other nodes.
 * The `deps` array lists the names of nodes that must complete before this node can run.
 */
export interface DAGNode {
  /** Unique name within this DAG submission. Used as reference in `deps` arrays. */
  name: string;
  /** Queue to add this job to. */
  queueName: string;
  /** Job data payload. */
  data: any;
  /** Job options (delay, priority, etc.). `parent` and `parents` are managed automatically. */
  opts?: Omit<JobOptions, 'parent' | 'parents'>;
  /** Names of other nodes in this DAG that must complete before this node runs. */
  deps?: string[];
}

/**
 * A complete DAG flow definition for submission via FlowProducer.addDAG().
 */
export interface DAGFlow {
  /** The nodes of the DAG. Order does not matter - topological sort is applied. */
  nodes: DAGNode[];
}
