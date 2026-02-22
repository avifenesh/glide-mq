import type { Job } from '../job';
import type { JobOptions } from '../types';

/**
 * Serializable subset of Job data that can be passed over IPC
 * to a worker thread or child process.
 */
export interface SerializedJob {
  id: string;
  name: string;
  data: unknown;
  opts: JobOptions;
  attemptsMade: number;
  timestamp: number;
  progress: number | object;
  processedOn?: number;
  parentId?: string;
  parentQueue?: string;
  orderingKey?: string;
  orderingSeq?: number;
  groupKey?: string;
  cost?: number;
}

/** Messages sent from the main thread to the child. */
export type MainToChild =
  | { type: 'process'; id: string; job: SerializedJob }
  | { type: 'abort'; id: string }
  | { type: 'proxy-response'; id: string; result?: unknown; error?: string };

/** Messages sent from the child back to the main thread. */
export type ChildToMain =
  | { type: 'completed'; id: string; result: unknown }
  | { type: 'failed'; id: string; error: string; stack?: string; errorName?: string; discarded?: boolean }
  | {
      type: 'proxy-request';
      id: string;
      method: 'log' | 'updateProgress' | 'updateData' | 'discard';
      args: unknown[];
    };

/**
 * Convert a real Job instance into a serializable object for IPC.
 */
export function toSerializedJob(job: Job): SerializedJob {
  return {
    id: job.id,
    name: job.name,
    data: job.data,
    opts: job.opts,
    attemptsMade: job.attemptsMade,
    timestamp: job.timestamp,
    progress: job.progress,
    processedOn: job.processedOn,
    parentId: job.parentId,
    parentQueue: job.parentQueue,
    orderingKey: job.orderingKey,
    orderingSeq: job.orderingSeq,
    groupKey: job.groupKey,
    cost: job.cost,
  };
}
