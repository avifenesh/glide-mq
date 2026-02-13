import type { GlideClient, GlideClusterClient } from 'speedkey';

export type Client = GlideClient | GlideClusterClient;

export interface ConnectionOptions {
  addresses: { host: string; port: number }[];
  useTLS?: boolean;
  credentials?: { username?: string; password: string };
  clusterMode?: boolean;
}

export interface QueueOptions {
  connection: ConnectionOptions;
  prefix?: string;
}

export interface WorkerOptions extends QueueOptions {
  concurrency?: number;
  globalConcurrency?: number;
  prefetch?: number;
  blockTimeout?: number;
  stalledInterval?: number;
  maxStalledCount?: number;
  limiter?: { max: number; duration: number };
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
}
