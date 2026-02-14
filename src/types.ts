import type { GlideClient, GlideClusterClient } from 'speedkey';

export type Client = GlideClient | GlideClusterClient;

export interface ConnectionOptions {
  addresses: { host: string; port: number }[];
  useTLS?: boolean;
  credentials?: { username?: string; password: string };
  clusterMode?: boolean;
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
