import { Batch, ClusterBatch } from '@glidemq/speedkey';
import { randomBytes } from 'crypto';
import type { Client, SchedulerEntry, Serializer } from './types';
import { JSON_SERIALIZER } from './types';
import {
  CONSUMER_GROUP,
  promote,
  promoteRateLimited,
  reclaimStalled,
  reclaimStalledListJobs as reclaimStalledListJobsCmd,
  addJobArgs,
  nextDueAt,
  tryLock,
  renewLock,
  unlock,
  healListActive,
} from './functions/index';
import type { buildKeys } from './utils';
import { computeFollowingSchedulerNextRun, isValidSchedulerEvery, MAX_JOB_DATA_SIZE, jsonReviver } from './utils';
import { isClusterClient } from './connection';

export interface SchedulerOptions {
  promotionInterval?: number;
  stalledInterval?: number;
  maxStalledCount?: number;
  consumerId?: string;
  /** Called at the end of each promotion tick to refresh cached meta flags. */
  onPromotionTick?: () => void;
  /** Surface scheduler errors through the parent worker instead of swallowing them silently. */
  onError?: (err: Error) => void;
  /** Serializer for job template data. Inherited from the parent Worker/Queue. */
  serializer?: Serializer;
  /** Consumer group name for stalled job reclamation. Defaults to 'workers'. */
  consumerGroup?: string;
  /** When true, stalled reclaim skips XDEL so other consumer groups can still consume the entry. */
  broadcastMode?: boolean;
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
  private consumerGroup: string;
  private broadcastMode: boolean;
  private onPromotionTick?: () => void;
  private onError?: (err: Error) => void;
  private serializer: Serializer;
  private promotionTimer: ReturnType<typeof setInterval> | null = null;
  private promotionWakeTimer: ReturnType<typeof setTimeout> | null = null;
  private nextPromotionWakeAt = 0;
  private stalledTimer: ReturnType<typeof setInterval> | null = null;
  private running = false;
  private promotionInFlight = false;
  private promotionQueued = false;
  private pendingRuns = new Set<Promise<unknown>>();
  private tickCount = 0;

  constructor(client: Client, queueKeys: ReturnType<typeof buildKeys>, opts: SchedulerOptions = {}) {
    this.client = client;
    this.queueKeys = queueKeys;
    this.promotionInterval = opts.promotionInterval ?? 5000;
    this.stalledInterval = opts.stalledInterval ?? 30000;
    this.maxStalledCount = opts.maxStalledCount ?? 1;
    this.consumerId = opts.consumerId ?? 'scheduler';
    this.consumerGroup = opts.consumerGroup ?? CONSUMER_GROUP;
    this.broadcastMode = opts.broadcastMode ?? false;
    this.onPromotionTick = opts.onPromotionTick;
    this.onError = opts.onError;
    this.serializer = opts.serializer ?? JSON_SERIALIZER;
  }

  private reportError(err: unknown): void {
    this.onError?.(err instanceof Error ? err : new Error(String(err)));
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
    this.promotionQueued = false;
    if (this.promotionTimer) {
      clearInterval(this.promotionTimer);
      this.promotionTimer = null;
    }
    if (this.promotionWakeTimer) {
      clearTimeout(this.promotionWakeTimer);
      this.promotionWakeTimer = null;
      this.nextPromotionWakeAt = 0;
    }
    if (this.stalledTimer) {
      clearInterval(this.stalledTimer);
      this.stalledTimer = null;
    }
  }

  async waitForIdle(): Promise<void> {
    while (this.pendingRuns.size > 0) {
      const results = await Promise.allSettled(this.pendingRuns);
      const errors = results.filter((r): r is PromiseRejectedResult => r.status === 'rejected').map((r) => r.reason);
      if (errors.length > 0) {
        throw errors[0];
      }
    }
  }

  private trackRun(run: Promise<unknown>): void {
    this.pendingRuns.add(run);
    void run.finally(() => {
      this.pendingRuns.delete(run);
    });
  }

  private runPromotion(): void {
    if (!this.running) return;
    if (this.promotionInFlight) {
      this.promotionQueued = true;
      return;
    }

    this.promotionInFlight = true;
    this.trackRun(
      this.promoteDelayed()
        .then(() => this.promoteRateLimitedGroups())
        .then(() => this.runSchedulers())
        .then(() => {
          this.onPromotionTick?.();
        })
        .then(() => this.healListActiveCounter())
        .then(() => this.scheduleNextPromotionWake())
        .catch((err) => {
          // Scheduler has no EventEmitter - errors are transient connection issues
          // that self-heal on the next interval tick. Worker reconnect handles the rest.
          this.reportError(err);
        })
        .finally(() => {
          this.promotionInFlight = false;
          if (this.running && this.promotionQueued) {
            this.promotionQueued = false;
            this.runPromotion();
          }
        }),
    );
  }

  private async scheduleNextPromotionWake(): Promise<void> {
    if (!this.running) return;

    const dueAt = await nextDueAt(this.client, this.queueKeys);
    if (dueAt == null) {
      if (this.promotionWakeTimer) {
        clearTimeout(this.promotionWakeTimer);
        this.promotionWakeTimer = null;
      }
      this.nextPromotionWakeAt = 0;
      return;
    }

    const now = Date.now();
    const delay = dueAt <= now ? 1 : dueAt - now;

    // Keep promotionInterval as the coarse polling ceiling; only schedule
    // an early wakeup when the next due job should fire sooner.
    if (delay >= this.promotionInterval) {
      if (this.promotionWakeTimer) {
        clearTimeout(this.promotionWakeTimer);
        this.promotionWakeTimer = null;
      }
      this.nextPromotionWakeAt = 0;
      return;
    }

    const target = now + delay;
    if (this.promotionWakeTimer && this.nextPromotionWakeAt > 0 && this.nextPromotionWakeAt <= target) {
      return;
    }

    if (this.promotionWakeTimer) {
      clearTimeout(this.promotionWakeTimer);
      this.promotionWakeTimer = null;
    }

    this.nextPromotionWakeAt = target;
    this.promotionWakeTimer = setTimeout(() => {
      this.promotionWakeTimer = null;
      this.nextPromotionWakeAt = 0;
      this.runPromotion();
    }, delay);
  }

  private runStalledRecovery(): void {
    this.trackRun(
      Promise.all([this.reclaimStalledJobs(), this.reclaimStalledListJobs()]).catch((err) => {
        this.reportError(err);
      }),
    );
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
   * Every 10th promotion tick, verify the list-active counter against the actual
   * count of active list-sourced jobs. Corrects upward drift caused by worker crashes.
   */
  private async healListActiveCounter(): Promise<void> {
    this.tickCount++;
    if (this.tickCount % 10 !== 0) return;
    try {
      await healListActive(this.client, this.queueKeys);
    } catch (err) {
      this.reportError(err);
    }
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
      this.consumerGroup,
      this.broadcastMode,
    );
  }

  /**
   * Reclaim stalled list-sourced jobs (LIFO/priority) invisible to XAUTOCLAIM.
   * Uses bounded SCAN to detect active list jobs with stale lastActive.
   */
  private async reclaimStalledListJobs(): Promise<number> {
    return reclaimStalledListJobsCmd(
      this.client,
      this.queueKeys,
      this.stalledInterval,
      this.maxStalledCount,
      Date.now(),
    );
  }

  private schedulerLockKey(name: string): string {
    return `${this.queueKeys.schedulers}:lock:${name}`;
  }

  private schedulerLockTtlMs(): number {
    return Math.max(1000, Math.min(this.promotionInterval * 2, 5000));
  }

  private async acquireSchedulerLock(name: string): Promise<{ key: string; token: string } | null> {
    const key = this.schedulerLockKey(name);
    const token = randomBytes(8).toString('hex');
    const ttlMs = this.schedulerLockTtlMs();
    const acquired = await tryLock(this.client, key, token, ttlMs);
    if (!acquired) return null;
    return { key, token };
  }

  private async releaseSchedulerLock(lock: { key: string; token: string }): Promise<void> {
    try {
      await unlock(this.client, lock.key, lock.token);
    } catch {
      // Best effort - the TTL is a fallback if release fails.
    }
  }

  /**
   * Check all scheduler entries in the schedulers hash. For any whose nextRun <= now,
   * create a job from the template and update lastRun/nextRun.
   */
  async runSchedulers(): Promise<number> {
    const now = Date.now();
    const tickLock = await this.acquireSchedulerLock('__tick__');
    if (!tickLock) return 0;
    const renewEveryMs = Math.max(250, Math.floor(this.schedulerLockTtlMs() / 3));
    let lockLost = false;
    const markTickLockLost = () => {
      if (lockLost) return;
      lockLost = true;
      this.reportError(new Error('Lost scheduler tick lock while processing schedulers'));
    };
    const renewTimer = setInterval(() => {
      void renewLock(this.client, tickLock.key, tickLock.token, this.schedulerLockTtlMs())
        .then((renewed) => {
          if (!renewed) {
            markTickLockLost();
          }
        })
        .catch((err) => {
          markTickLockLost();
          this.reportError(err);
        });
    }, renewEveryMs);

    let fired = 0;
    const pendingJobs: { keys: string[]; args: string[] }[] = [];
    const pendingUpdates: Record<string, string> = Object.create(null);
    const pendingDeletions: string[] = [];
    let pendingUpdateCount = 0;

    try {
      const allEntries = await this.client.hgetall(this.queueKeys.schedulers);

      // hgetall returns { field, value }[] — empty array means no schedulers
      if (!allEntries || allEntries.length === 0) {
        return 0;
      }

      for (const entry of allEntries) {
        if (lockLost) {
          break;
        }

        const schedulerName = String(entry.field);
        let config: SchedulerEntry;
        try {
          config = JSON.parse(String(entry.value), jsonReviver);
        } catch {
          continue;
        }

        const invalid =
          !config.pattern && !isValidSchedulerEvery(config.every) && !isValidSchedulerEvery(config.repeatAfterComplete);
        if (invalid) {
          pendingDeletions.push(schedulerName);
          continue;
        }

        if (!config.nextRun || config.nextRun > now) {
          continue;
        }

        const currentIterationCount = config.iterationCount ?? 0;
        if (
          (config.limit != null && currentIterationCount >= config.limit) ||
          (config.endDate != null && config.nextRun > config.endDate)
        ) {
          pendingDeletions.push(schedulerName);
          continue;
        }

        let preparedNextRun: number | null | undefined;
        if (config.pattern) {
          try {
            preparedNextRun = computeFollowingSchedulerNextRun(config, now);
          } catch {
            pendingDeletions.push(schedulerName);
            continue;
          }
        }

        const template = config.template ?? {};
        const jobName = template.name ?? schedulerName;
        let jobData: string;
        try {
          jobData = template.data !== undefined ? this.serializer.serialize(template.data) : '{}';
        } catch {
          continue;
        }
        const byteLen = Buffer.byteLength(jobData, 'utf8');
        if (byteLen > MAX_JOB_DATA_SIZE) {
          continue;
        }

        const jobOpts = template.opts ? JSON.stringify(template.opts) : '{}';
        const priority = template.opts?.priority ?? 0;
        const maxAttempts = template.opts?.attempts ?? 0;
        const jobTtl = template.opts?.ttl ?? 0;
        const lifo = template.opts?.lifo ? 1 : 0;

        const isRepeatAfterComplete = isValidSchedulerEvery(config.repeatAfterComplete);

        pendingJobs.push(
          addJobArgs(
            this.queueKeys,
            jobName,
            jobData,
            jobOpts,
            now,
            0,
            priority,
            '',
            maxAttempts,
            '',
            0,
            0,
            0,
            0,
            0,
            0,
            jobTtl,
            '',
            lifo,
            '',
            '',
            isRepeatAfterComplete ? schedulerName : '',
          ),
        );

        config.lastRun = now;
        config.iterationCount = currentIterationCount + 1;
        if (config.limit != null && config.iterationCount >= config.limit) {
          pendingDeletions.push(schedulerName);
        } else if (isRepeatAfterComplete) {
          // Set nextRun to 0 (sentinel: awaiting completion). The worker will
          // update nextRun after the job completes or terminally fails.
          config.nextRun = 0;
          pendingUpdates[schedulerName] = JSON.stringify(config);
          pendingUpdateCount++;
        } else {
          const nextRun = preparedNextRun ?? computeFollowingSchedulerNextRun(config, now);
          if (nextRun == null) {
            pendingDeletions.push(schedulerName);
          } else {
            config.nextRun = nextRun;
            pendingUpdates[schedulerName] = JSON.stringify(config);
            pendingUpdateCount++;
          }
        }
        fired++;
      }

      if (pendingJobs.length === 0 && pendingDeletions.length === 0 && pendingUpdateCount === 0) {
        return 0;
      }

      if (lockLost) {
        return 0;
      }

      const lockStillHeld = await renewLock(this.client, tickLock.key, tickLock.token, this.schedulerLockTtlMs());
      if (!lockStillHeld) {
        markTickLockLost();
        return 0;
      }

      // This batch intentionally uses MULTI/EXEC so job creation and scheduler
      // state updates stay in one transaction. All queue keys share the same hash
      // tag, so the transactional batch remains single-slot in cluster mode.
      const batch = isClusterClient(this.client) ? new ClusterBatch(true) : new Batch(true);

      for (const { keys, args } of pendingJobs) {
        batch.fcall('glidemq_addJob', keys, args);
      }
      if (pendingUpdateCount > 0) {
        batch.hset(this.queueKeys.schedulers, pendingUpdates);
      }
      if (pendingDeletions.length > 0) {
        batch.hdel(this.queueKeys.schedulers, pendingDeletions);
      }
      await this.client.exec(batch as any, false);
      return fired;
    } finally {
      clearInterval(renewTimer);
      await this.releaseSchedulerLock(tickLock);
    }
  }
}
