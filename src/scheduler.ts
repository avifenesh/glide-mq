import type { Client } from './types';
import { promote, reclaimStalled } from './functions/index';
import { CONSUMER_GROUP } from './functions/index';
import type { buildKeys } from './utils';

export interface SchedulerOptions {
  promotionInterval?: number;
  stalledInterval?: number;
  maxStalledCount?: number;
  consumerId?: string;
}

/**
 * Internal scheduler that runs inside a Worker.
 *
 * Responsibilities:
 * 1. Promote delayed/priority jobs from scheduled ZSet to stream (via glidemq_promote)
 * 2. Reclaim stalled jobs via XAUTOCLAIM (via glidemq_reclaimStalled)
 */
export class Scheduler {
  private client: Client;
  private queueKeys: ReturnType<typeof buildKeys>;
  private promotionInterval: number;
  private stalledInterval: number;
  private maxStalledCount: number;
  private consumerId: string;
  private promotionTimer: ReturnType<typeof setInterval> | null = null;
  private stalledTimer: ReturnType<typeof setInterval> | null = null;
  private running = false;

  constructor(
    client: Client,
    queueKeys: ReturnType<typeof buildKeys>,
    opts: SchedulerOptions = {},
  ) {
    this.client = client;
    this.queueKeys = queueKeys;
    this.promotionInterval = opts.promotionInterval ?? 5000;
    this.stalledInterval = opts.stalledInterval ?? 30000;
    this.maxStalledCount = opts.maxStalledCount ?? 1;
    this.consumerId = opts.consumerId ?? 'scheduler';
  }

  start(): void {
    if (this.running) return;
    this.running = true;

    // Run both immediately, then on their intervals
    this.runPromotion();
    this.runStalledRecovery();

    this.promotionTimer = setInterval(() => {
      this.runPromotion();
    }, this.promotionInterval);

    this.stalledTimer = setInterval(() => {
      this.runStalledRecovery();
    }, this.stalledInterval);
  }

  stop(): void {
    this.running = false;
    if (this.promotionTimer) {
      clearInterval(this.promotionTimer);
      this.promotionTimer = null;
    }
    if (this.stalledTimer) {
      clearInterval(this.stalledTimer);
      this.stalledTimer = null;
    }
  }

  private runPromotion(): void {
    this.promoteDelayed().catch(() => {
      // Swallow errors in background promotion loop
    });
  }

  private runStalledRecovery(): void {
    this.reclaimStalledJobs().catch(() => {
      // Swallow errors in background stalled recovery loop
    });
  }

  /**
   * Promote delayed/prioritized jobs whose scheduled time has passed.
   * Calls FCALL glidemq_promote with current timestamp.
   */
  async promoteDelayed(): Promise<number> {
    return promote(this.client, this.queueKeys, Date.now());
  }

  /**
   * Reclaim stalled jobs whose consumers haven't ACKed within the stalled interval.
   * Calls FCALL glidemq_reclaimStalled via XAUTOCLAIM semantics in Lua.
   */
  async reclaimStalledJobs(): Promise<number> {
    return reclaimStalled(
      this.client,
      this.queueKeys,
      this.consumerId,
      this.stalledInterval,
      this.maxStalledCount,
      Date.now(),
      CONSUMER_GROUP,
    );
  }
}
