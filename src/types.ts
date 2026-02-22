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
}

export interface JobOptions {
  delay?: number;
  priority?: number;
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

export interface JobData {
  [key: string]: unknown;
}

export type Processor<D = any, R = any> = (job: import('./job').Job<D, R>) => Promise<R>;

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
}

export interface QueueEventsOptions {
  connection: ConnectionOptions;
  /** @internal Not supported - QueueEvents uses blocking XREAD and requires a dedicated connection. */
  client?: never;
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
}

export interface JobTemplate {
  name?: string;
  data?: any;
  opts?: Omit<JobOptions, 'delay' | 'deduplication' | 'parent'>;
}

export interface SchedulerEntry {
  pattern?: string;
  every?: number;
  template?: JobTemplate;
  lastRun?: number;
  nextRun: number;
}

export interface Metrics {
  /** Total count of completed or failed jobs */
  count: number;
}

export interface JobCounts {
  waiting: number;
  active: number;
  delayed: number;
  completed: number;
  failed: number;
}

export interface SearchJobsOptions {
  state?: 'waiting' | 'active' | 'delayed' | 'completed' | 'failed';
  name?: string;
  data?: Record<string, unknown>;
  limit?: number;
}
