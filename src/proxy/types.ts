import type { ConnectionOptions, JobOptions } from '../types';
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
  data: unknown;
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
