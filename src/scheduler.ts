import { Batch, ClusterBatch, GlideClient, GlideClusterClient } from '@glidemq/speedkey';
import type { Client, SchedulerEntry } from './types';
import { CONSUMER_GROUP, promote, promoteRateLimited, reclaimStalled } from './functions/index';
import type { buildKeys } from './utils';
import { nextCronOccurrence } from './utils';

export interface SchedulerOptions {
  promotionInterval?: number;
  stalledInterval?: number;
  maxStalledCount?: number;
  consumerId?: string;
  /** @deprecated No longer needed - prefix is derived from Valkey keys server-side. */
  queuePrefix?: string;
  /** Called at the end of each promotion tick to refresh cached meta flags. */
  onPromotionTick?: () => void;
}

/**
 * Internal scheduler that runs inside a Worker.
 *
 * Responsibilities:
 * 1. Promote delayed/priority jobs from scheduled ZSet to stream (via glidemq_promote)
 * 2. Reclaim stalled jobs via XAUTOCLAIM (via glidemq_reclaimStalled)
 * 3. Fire job schedulers (repeatable/cron) when their nextRun time has passed
 */
export class Scheduler {
  private client: Client;
  private queueKeys: ReturnType<typeof buildKeys>;
  private promotionInterval: number;
  private stalledInterval: number;
  private maxStalledCount: number;
  private consumerId: string;
  private onPromotionTick?: () => void;
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
    this.onPromotionTick = opts.onPromotionTick;
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
    this.promoteDelayed()
      .then(() => this.promoteRateLimitedGroups())
      .then(() => this.runSchedulers())
      .then(() => { this.onPromotionTick?.(); })
      .catch(() => {
        // Scheduler has no EventEmitter - errors are transient connection issues
        // that self-heal on the next interval tick. Worker reconnect handles the rest.
      });
  }

  private runStalledRecovery(): void {
    this.reclaimStalledJobs().catch(() => {
      // Scheduler has no EventEmitter - errors are transient connection issues
      // that self-heal on the next interval tick. Worker reconnect handles the rest.
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
   * Promote rate-limited groups whose time window has expired.
   * Moves waiting jobs from group queues back into the stream.
   */
  async promoteRateLimitedGroups(): Promise<number> {
    return promoteRateLimited(this.client, this.queueKeys, Date.now());
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

  /**
   * Check all scheduler entries in the schedulers hash. For any whose nextRun <= now,
   * create a job from the template and update lastRun/nextRun.
   */
  async runSchedulers(): Promise<number> {
    const now = Date.now();
    const allEntries = await this.client.hgetall(this.queueKeys.schedulers);

    // hgetall returns { field, value }[] — empty array means no schedulers
    if (!allEntries || allEntries.length === 0) return 0;

    const pendingAdditions: {
      jobName: string;
      jobData: string;
      jobOpts: string;
      priority: number;
      maxAttempts: number;
    }[] = [];
    const pendingUpdates: Record<string, string> = {};
    const pendingDeletions: string[] = [];

    let fired = 0;
    for (const entry of allEntries) {
      const schedulerName = String(entry.field);
      let config: SchedulerEntry;
      try {
        config = JSON.parse(String(entry.value));
      } catch {
        continue; // Skip malformed entries
      }

      if (!config.nextRun || config.nextRun > now) continue;

      // Compute the job name and data from the template
      const template = config.template ?? {};
      const jobName = template.name ?? schedulerName;
      const jobData = template.data !== undefined ? JSON.stringify(template.data) : '{}';
      const jobOpts = template.opts ? JSON.stringify(template.opts) : '{}';
      const priority = template.opts?.priority ?? 0;
      const maxAttempts = template.opts?.attempts ?? 0;

      pendingAdditions.push({ jobName, jobData, jobOpts, priority, maxAttempts });

      // Compute next run
      let nextRun: number;
      if (config.pattern) {
        nextRun = nextCronOccurrence(config.pattern, now);
      } else if (config.every) {
        nextRun = now + config.every;
      } else {
        // No repeat config - remove the scheduler entry
        pendingDeletions.push(schedulerName);
        fired++;
        continue;
      }

      config.lastRun = now;
      config.nextRun = nextRun;
      pendingUpdates[schedulerName] = JSON.stringify(config);
      fired++;
    }

    if (fired === 0) return 0;

    const isCluster = this.client instanceof GlideClusterClient;
    const batch = isCluster ? new ClusterBatch(false) : new Batch(false);
    const k = this.queueKeys;
    const addJobKeys = [k.id, k.stream, k.scheduled, k.events];

    for (const job of pendingAdditions) {
      batch.fcall('glidemq_addJob', addJobKeys, [
        job.jobName,
        job.jobData,
        job.jobOpts,
        now.toString(),
        '0', // no delay
        job.priority.toString(),
        '', // no parent
        job.maxAttempts.toString(),
        '', // orderingKey
        '0', // groupConcurrency
        '0', // groupRateMax
        '0', // groupRateDuration
        '0', // tbCapacity
        '0', // tbRefillRate
        '0', // jobCost
      ]);
    }

    if (Object.keys(pendingUpdates).length > 0) {
      batch.hset(k.schedulers, pendingUpdates);
    }

    if (pendingDeletions.length > 0) {
      batch.hdel(k.schedulers, pendingDeletions);
    }

    if (isCluster) {
      await (this.client as GlideClusterClient).exec(batch as ClusterBatch, true);
    } else {
      await (this.client as GlideClient).exec(batch as Batch, true);
    }

    return fired;
  }
}
