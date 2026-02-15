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
}

export interface DeadLetterQueueOptions {
  /** Queue name to use as the dead letter queue. */
  name: string;
  /** Max retries before moving to DLQ. If not set, uses the job's own attempts config. */
  maxRetries?: number;
}

export interface QueueOptions {
  connection: ConnectionOptions;
  prefix?: string;
  /** Dead letter queue configuration. Jobs that exhaust retries are moved here. */
  deadLetterQueue?: DeadLetterQueueOptions;
  /** Enable transparent compression of job data. Default: 'none'. */
  compression?: 'none' | 'gzip';
}

export interface WorkerOptions extends QueueOptions {
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
}

export interface JobOptions {
  delay?: number;
  priority?: number;
  attempts?: number;
  backoff?: { type: 'fixed' | 'exponential' | string; delay: number; jitter?: number };
  timeout?: number;
  removeOnComplete?: boolean | number | { age: number; count: number };
  removeOnFail?: boolean | number | { age: number; count: number };
  deduplication?: { id: string; ttl?: number; mode?: 'simple' | 'throttle' | 'debounce' };
  parent?: { queue: string; id: string };
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
  connection: ConnectionOptions;
  prefix?: string;
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
