import type {
  ConnectionOptions,
  JobOptions,
  JobTemplate,
  JobUsage,
  Metrics,
  ScheduleOpts,
  SchedulerEntry,
  UsageSummary,
  WorkerInfo,
} from '../types';
import type { Client } from '../types';

/**
 * Options for the HTTP proxy server.
 *
 * No built-in rate limiting is provided. When exposing the proxy to untrusted
 * networks, deploy behind express-rate-limit or equivalent middleware.
 */
export interface ProxyOptions {
  /** Connection options for creating internal Queue instances. Required unless `client` is provided. */
  connection?: ConnectionOptions;
  /** Pre-existing GLIDE client shared by all internal Queue instances. */
  client?: Client;
  /** Key prefix for all queues. Default: 'glide'. */
  prefix?: string;
  /** Allowlist of queue names. When set, requests to unlisted queues return 403. */
  queues?: string[];
  /** Enable transparent compression for job data. Default: 'none'. */
  compression?: 'none' | 'gzip';
  /** Callback for queue-level errors. Defaults to console.error if not provided. */
  onError?: (err: Error, queueName: string) => void;
}

/**
 * Request body for POST /queues/:name/jobs.
 */
export interface AddJobRequest {
  /** Job name (required). */
  name: string;
  /** Job data payload. Defaults to null if omitted. */
  data?: unknown;
  /** Job options (delay, priority, etc.). */
  opts?: Pick<
    JobOptions,
    | 'jobId'
    | 'delay'
    | 'priority'
    | 'attempts'
    | 'backoff'
    | 'timeout'
    | 'removeOnComplete'
    | 'removeOnFail'
    | 'deduplication'
    | 'ttl'
    | 'ordering'
    | 'cost'
    | 'lifo'
  >;
}

/**
 * Response body for POST /queues/:name/jobs.
 */
export interface AddJobResponse {
  id: string;
  name: string;
  timestamp: number;
}

/**
 * Response body when a job is deduplicated or has a duplicate custom ID.
 */
export interface AddJobSkippedResponse {
  skipped: true;
}

/**
 * Request body for POST /queues/:name/jobs/bulk.
 */
export interface AddBulkRequest {
  jobs: AddJobRequest[];
}

/**
 * Response body for POST /queues/:name/jobs/bulk.
 */
export interface AddBulkResponse {
  jobs: (AddJobResponse | AddJobSkippedResponse)[];
}

/**
 * Response body for GET /queues/:name/jobs/:id.
 */
export interface GetJobResponse {
  id: string;
  name: string;
  data?: unknown;
  opts: Record<string, unknown>;
  timestamp: number;
  attemptsMade: number;
  state: string;
  progress: number | object;
  returnvalue?: unknown;
  failedReason?: string;
  finishedOn?: number;
  processedOn?: number;
  parentId?: string;
  usage?: JobUsage;
}

/** Response body for GET /queues/:name/jobs. */
export interface ListJobsResponse {
  jobs: GetJobResponse[];
}

/**
 * Response body for GET /queues/:name/counts.
 */
export interface JobCountsResponse {
  waiting: number;
  active: number;
  delayed: number;
  completed: number;
  failed: number;
}

/**
 * Response body for GET /health.
 */
export interface HealthResponse {
  status: 'ok';
  uptime: number;
  queues: number;
}

/**
 * Error response body.
 */
export interface ErrorResponse {
  error: string;
}

/** Request body for POST /queues/:name/jobs/wait. */
export interface WaitJobRequest extends Omit<AddJobRequest, 'opts'> {
  opts?: AddJobRequest['opts'] & { waitTimeout?: number };
}

/** Response body for POST /queues/:name/jobs/wait. */
export interface WaitJobResponse {
  result: unknown;
}

/** Response body for GET /queues/:name/metrics. */
export type MetricsResponse = Metrics;

/** Response body for GET /queues/:name/workers. */
export interface WorkersResponse {
  workers: WorkerInfo[];
}

/** Response body for POST /queues/:name/drain. */
export interface DrainResponse {
  delayed: boolean;
  drained: true;
}

/** Response body for POST /queues/:name/retry. */
export interface RetryJobsResponse {
  retried: number;
}

/** Response body for DELETE /queues/:name/clean. */
export interface CleanJobsResponse {
  removed: string[];
}

/** Request body for PUT /queues/:name/schedulers/:id. */
export interface UpsertSchedulerRequest {
  schedule: ScheduleOpts;
  template?: JobTemplate;
}

/** Response body for GET /queues/:name/schedulers. */
export interface ListSchedulersResponse {
  schedulers: Array<{ name: string; entry: SchedulerEntry }>;
}

/** Response body for GET /queues/:name/schedulers/:id. */
export interface GetSchedulerResponse {
  entry: SchedulerEntry;
  name: string;
}

/** Response body for scheduler write endpoints. */
export interface SchedulerMutationResponse {
  removed?: true;
  upserted?: true;
}

/** Request body for POST /broadcast/:name. */
export interface BroadcastPublishRequest {
  subject: string;
  data?: unknown;
  opts?: AddJobRequest['opts'];
}

/** Response body for POST /broadcast/:name when a message is published. */
export interface BroadcastPublishResponse {
  id: string;
  subject: string;
}

/** Response body for GET /usage/summary. */
export type UsageSummaryResponse = UsageSummary;
