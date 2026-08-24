import type { GlideReturnType } from '@glidemq/speedkey';
import type { Client } from '../types';
import { librarySourceFrom, loadLibraryFile } from './load-library-source';

export const LIBRARY_NAME = 'glidemq';
// Version 44: Added metrics recording (time-series data for getMetrics).
// Version 45: DAG multi-parent dependencies - glidemq_registerParent, multi-parent completion notification.
// Version 46: Added lifo parameter to glidemq_addJob (arg 18) and glidemq_dedup (arg 21) for LIFO mode.
// Version 47: Priority-promoted jobs routed to dedicated priority list (priority > LIFO > FIFO ordering).
// Version 48: Use LPUSH for priority list so RPOP returns highest-priority (lowest score) job first.
// Version 49: Fix Phase 1.0/1.5 in completeAndFetchNext - HSET before HGETALL, add lastActive.
// Version 50: glidemq_drain cleans LIFO and priority list keys and their job hashes.
// Version 51: Broadcast fan-out safety: broadcastMode flag in glidemq_complete/completeAndFetchNext/fail/reclaimStalled skips XDEL and per-subscription retry tracking.
// Version 52: Skip removeOnFail job-hash deletion in glidemq_fail when broadcastMode=1.
// Version 53: Fix Lua scope: move recordMetrics definition before releaseGroupSlotAndPromote/expireJob.
// Version 54: Guard XDEL in glidemq_moveToActive/deferActive/moveToWaitingChildren with broadcastMode flag.
// Version 55: Guard XDEL in glidemq_moveActiveToDelayed with broadcastMode flag.
// Version 56: glidemq_checkConcurrency includes list-active counter; complete/fail DECR on list jobs; rpopAndReserve.
// Version 57: Clamp groupConcurrency >= 1 in Lua for rolling upgrade safety.
// Version 57: glidemq_addFlow routes child jobs with lifo:true to LIFO list.
// Version 58: XADD to job stream includes 'name' field for subject-based filtering in BroadcastWorker.
// Version 59: tbRefill early exit, DAG pattern match, key assertions, metrics scan 1000.
// Version 60: glidemq_healListActive - self-healing for list-active counter drift on worker crash.
// Version 61: glidemq_popLists - atomic pop from priority + LIFO lists in a single FCALL; timestamp caching in processJob.
// Version 62: buildParentInfo fast path - skip HMGET for non-parent jobs.
// Version 63: completeAndFetchNext accepts processedOn hint, '__' sentinels for ordering/group keys, hasParents flag.
// Version 64: completeAndFetchNext skipEvents/skipMetrics args - opt-out XADD events + HINCRBY metrics on hot path.
// Version 65: rpopAndReserve accepts count arg for batch popping under globalConcurrency; deferActive DECRs list-active for list-sourced jobs.
// Version 66: glidemq_reclaimStalledListJobs - stall detection for list-sourced jobs via bounded SCAN.
// Version 67: reclaimStalledListJobs - increase SCAN bounds (maxIter 100->1000, COUNT 50->500) for large DBs.
// Version 68: completeAndFetchNext HMGET optimization - merge 4 separate hash lookups into 1 HMGET (13→10 redis.call()s on hot path).
// Version 69: Remove auto-ID EXISTS check - monotonic INCR can't collide; custom numeric IDs advance counter to prevent future conflicts.
// Version 70: Unify ordering path: ZSET groupq, nextSeq tracking, ordering gates in all activation paths.
// Version 71: Step-job slot retention, returning step-job bypass, GROUP_ORDERED sentinel.
// Version 72: Runtime per-group rate limiting: glidemq_rateLimitGroup + glidemq_rateLimitGroupExternal.
// Version 73: Fix review findings: groupqScore string ID fallback, promoteRateLimited loop, step-job gates in Phase 1/3, retry slot retention, addFlow routing.
// Version 74: glidemq_removeJob/clean/drain delete per-job streaming channel keys (jstream:).
// Version 75: Suspend/resume with signals: glidemq_suspend, glidemq_signal, glidemq_sweepSuspended.
// Version 76: glidemq_clean and glidemq_drain delete signals: keys to prevent key leaks.
// Version 77: Per-job lockDuration: reclaimStalled/reclaimStalledListJobs read lockDuration from job opts for per-job stall threshold.
// Version 78: glidemq_fail increments fallbackIndex on retry when job has fallbacks configured.
// Version 79: Budget middleware: glidemq_checkBudget, glidemq_recordUsageAndCheckBudget.
// Version 80: Budget v2: per-category token/cost tracking, weighted totals, per-category limits.
// Version 81: Fix debounce + ordering nextSeq deadlock: advancePastSkips() helper, skip markers in all ordering gates.
// Version 82: Guard every list-active DECR with > 0 check via decrListActive() helper - prevents counter underflow on duplicate complete/fail/suspend, reclaim races, etc. (#217).
// Version 83: glidemq_getActiveListJobIds - bounded SCAN that returns active list-sourced jobIds for getJobs('active') visibility (#213).
// Version 84: glidemq_reclaimStalled / glidemq_reclaimStalledListJobs accept workerLockDuration arg - per-entry threshold falls back to it before minIdleMs, so per-job opts.lockDuration overrides still fire under XAUTOCLAIM gating (#213).
// Version 85: extractLockDurationFromOpts clamps to >=1000ms (1s) to prevent tight-heartbeat DoS via per-job lockDuration; sub-second values fall back to worker lockDuration / minIdleMs (#225).
// Version 86: glidemq_reclaimStalledListJobs refreshes lastActive on detection to dedupe stalled-recovery across concurrent worker schedulers - same stale list job counted at most once per interval (#228).
// Version 87: releaseGroupSlotAndPromote caps maxConcurrency promotion budget at 1000 to prevent unbounded Lua loop on large maxConcurrency settings (#236).
// Version 88: glidemq_promote counts expired scheduled jobs toward MAX_PROMOTIONS budget - prevents unbounded scans when many expired jobs sit in scheduled set (#235).
// Version 89: Bound skip-marker advancement work per FCALL via MAX_SKIP_ADVANCE_STEPS=128 to prevent long-running ordered-group loops (#222).
// Version 90: glidemq_addFlow rechecks EXISTS for custom child IDs and skips auto-INCR'd parent/child IDs that collide with existing custom-ID jobs - mirrors the addJob auto-ID collision guard so flows survive shared idKey state (#234).
// Version 91: Stalled-recovery redispatches under-threshold jobs back to the stream / lifo / priority list so a healthy worker can pick them up; jobs only fail once stalledCount > maxStalledCount. Aligns with the at-least-once redelivery promise in DURABILITY.md and matches BullMQ semantics (#242).
// Version 92: Replace DEL with UNLINK on every multi-key / large-collection delete (job hashes, retention purge, glidemq_clean batches, glidemq_drain stream/zset/lifo/priority sweeps). UNLINK keeps in-script atomicity - the keyspace removal is still synchronous from the script's view - but defers memory reclamation to the bio thread, so obliterate / retention / drain stop blocking the server thread on MB-sized job hashes. Small-key DELs (lockKey) kept as DEL (#243).
// Version 93: glidemq_completeAndFetchNext always SMEMBERS the child parents SET. The previous hasParents gate sourced its truth from the worker's snapshot of the job hash, which is stale when DAG wiring (hset parentIds + registerParent) lands between the worker's job fetch and the completion FCALL. The SMEMBERS cost on an empty set is negligible; the dropped optimization was unsound (#246).
// Version 101: complete/CAF enqueue cross-queue parent notifies on a same-slot pending set so a later scheduler tick can retry if the worker dies between complete and completeChild.
// Version 102: completeChild ignores stale notifications for deleted parent hashes.
export const LIBRARY_VERSION = '102';

// Consumer group name used by workers
export const CONSUMER_GROUP = 'workers';

// Lua library source loaded via FUNCTION LOAD.
// Source of truth is src/functions/glidemq.lua (copied to dist/ on build).
// loadLibraryFile reads that sibling when present; bundlers that omit the
// .lua file fall back to the generated glidemq.embedded.json snapshot.
export const LIBRARY_SOURCE = librarySourceFrom(loadLibraryFile(), LIBRARY_VERSION);

// ---- Key set type ----

export type QueueKeys = ReturnType<typeof import('../utils').buildKeys>;

// ---- Typed FCALL wrappers ----

/**
 * Build the keys and args arrays for glidemq_addJob, shared by addJob() and Batch callers.
 */
export function addJobArgs(
  k: QueueKeys,
  jobName: string,
  data: string,
  opts: string,
  timestamp: number,
  delay: number,
  priority: number,
  parentId: string,
  maxAttempts: number,
  orderingKey: string = '',
  groupConcurrency: number = 0,
  groupRateMax: number = 0,
  groupRateDuration: number = 0,
  tbCapacity: number = 0,
  tbRefillRate: number = 0,
  jobCost: number = 0,
  ttl: number = 0,
  customJobId: string = '',
  lifo: number = 0,
  parentQueue: string = '',
  parentDepsKey: string = '',
  schedulerName: string = '',
  skipEvents: boolean = false,
): { keys: string[]; args: string[] } {
  const keys = [k.id, k.stream, k.scheduled, k.events];
  if (parentDepsKey) {
    keys.push(parentDepsKey);
  }
  return {
    keys,
    args: [
      jobName,
      data,
      opts,
      timestamp.toString(),
      delay.toString(),
      priority.toString(),
      parentId,
      maxAttempts.toString(),
      orderingKey,
      groupConcurrency.toString(),
      groupRateMax.toString(),
      groupRateDuration.toString(),
      tbCapacity.toString(),
      tbRefillRate.toString(),
      jobCost.toString(),
      ttl.toString(),
      customJobId,
      lifo.toString(),
      parentQueue,
      schedulerName,
      skipEvents ? '1' : '0',
    ],
  };
}

export async function addJob(
  client: Client,
  k: QueueKeys,
  jobName: string,
  data: string,
  opts: string,
  timestamp: number,
  delay: number,
  priority: number,
  parentId: string,
  maxAttempts: number,
  orderingKey: string = '',
  groupConcurrency: number = 0,
  groupRateMax: number = 0,
  groupRateDuration: number = 0,
  tbCapacity: number = 0,
  tbRefillRate: number = 0,
  jobCost: number = 0,
  ttl: number = 0,
  customJobId: string = '',
  lifo: number = 0,
  parentQueue: string = '',
  parentDepsKey: string = '',
  schedulerName: string = '',
  skipEvents: boolean = false,
): Promise<string> {
  const { keys, args } = addJobArgs(
    k,
    jobName,
    data,
    opts,
    timestamp,
    delay,
    priority,
    parentId,
    maxAttempts,
    orderingKey,
    groupConcurrency,
    groupRateMax,
    groupRateDuration,
    tbCapacity,
    tbRefillRate,
    jobCost,
    ttl,
    customJobId,
    lifo,
    parentQueue,
    parentDepsKey,
    schedulerName,
    skipEvents,
  );
  const result = await client.fcall('glidemq_addJob', keys, args);
  return result as string;
}

/**
 * Add a job with deduplication. Checks the dedup hash and either skips or adds the job.
 * Returns "skipped" if deduplicated, otherwise the new job ID (string).
 */
export async function dedup(
  client: Client,
  k: QueueKeys,
  dedupId: string,
  ttlMs: number,
  mode: string,
  jobName: string,
  data: string,
  opts: string,
  timestamp: number,
  delay: number,
  priority: number,
  parentId: string,
  maxAttempts: number,
  orderingKey: string = '',
  groupConcurrency: number = 0,
  groupRateMax: number = 0,
  groupRateDuration: number = 0,
  tbCapacity: number = 0,
  tbRefillRate: number = 0,
  jobCost: number = 0,
  jobTtl: number = 0,
  customJobId: string = '',
  lifo: number = 0,
  parentQueue: string = '',
  parentDepsKey: string = '',
  skipEvents: boolean = false,
): Promise<string> {
  const keys = [k.dedup, k.id, k.stream, k.scheduled, k.events];
  if (parentDepsKey) {
    keys.push(parentDepsKey);
  }
  const result = await client.fcall('glidemq_dedup', keys, [
    dedupId,
    ttlMs.toString(),
    mode,
    jobName,
    data,
    opts,
    timestamp.toString(),
    delay.toString(),
    priority.toString(),
    parentId,
    maxAttempts.toString(),
    orderingKey,
    groupConcurrency.toString(),
    groupRateMax.toString(),
    groupRateDuration.toString(),
    tbCapacity.toString(),
    tbRefillRate.toString(),
    jobCost.toString(),
    jobTtl.toString(),
    customJobId,
    lifo.toString(),
    parentQueue,
    skipEvents ? '1' : '0',
  ]);
  return result as string;
}

/**
 * Promote delayed/prioritized jobs whose score <= now from scheduled ZSet to stream.
 * Returns the number of jobs promoted.
 */
export async function promote(client: Client, k: QueueKeys, timestamp: number): Promise<number> {
  const result = await client.fcall('glidemq_promote', [k.scheduled, k.stream, k.events], [timestamp.toString()]);
  return result as number;
}

/**
 * Returns the earliest known due timestamp for delayed/priority promotion work.
 * - delayed/prioritized jobs come from the scheduled ZSet (decoded score timestamp)
 * - group rate/token wakeups come from the ratelimited ZSet (raw score timestamp)
 *
 * Returns null when no pending due work exists.
 */
export async function nextDueAt(client: Client, k: QueueKeys): Promise<number | null> {
  const result = await client.fcall('glidemq_nextDue', [k.scheduled, k.ratelimited], []);
  const ts = Number(result);
  if (!Number.isFinite(ts) || ts < 0) {
    return null;
  }
  return ts;
}

export async function tryLock(client: Client, lockKey: string, token: string, ttlMs: number): Promise<boolean> {
  const result = await client.fcall('glidemq_tryLock', [lockKey], [token, ttlMs.toString()]);
  return Number(result) === 1;
}

export async function unlock(client: Client, lockKey: string, token: string): Promise<boolean> {
  const result = await client.fcall('glidemq_unlock', [lockKey], [token]);
  return Number(result) === 1;
}

export async function renewLock(client: Client, lockKey: string, token: string, ttlMs: number): Promise<boolean> {
  const result = await client.fcall('glidemq_renewLock', [lockKey], [token, ttlMs.toString()]);
  return Number(result) === 1;
}

/**
 * Encode a removeOnComplete/removeOnFail option into Lua args.
 */
const RETENTION_NONE = { mode: '0', count: 0, age: 0 } as const;
const RETENTION_TRUE = { mode: 'true', count: 0, age: 0 } as const;

function encodeRetention(opt?: boolean | number | { age: number; count: number }): {
  mode: string;
  count: number;
  age: number;
} {
  if (opt === true) {
    return RETENTION_TRUE;
  }
  if (typeof opt === 'number') {
    return { mode: 'count', count: opt, age: 0 };
  }
  if (opt && typeof opt === 'object') {
    return { mode: 'age_count', count: opt.count ?? 0, age: opt.age ?? 0 };
  }
  return RETENTION_NONE;
}

/**
 * Complete a job: XACK, move to completed ZSet, update job hash, emit event.
 * Optionally applies retention cleanup based on removeOnComplete.
 * If the job has a parent (depsMember and parentId provided), also handles
 * the completeChild logic inline: removes from parent deps, re-queues parent when all children done.
 */
export async function completeJob(
  client: Client,
  k: QueueKeys,
  jobId: string,
  entryId: string,
  returnvalue: string,
  timestamp: number,
  group: string = CONSUMER_GROUP,
  removeOnComplete?: boolean | number | { age: number; count: number },
  parentInfo?: { depsMember: string; parentId: string; parentKeys: QueueKeys },
  broadcastMode?: boolean,
): Promise<GlideReturnType> {
  const { mode, count, age } = encodeRetention(removeOnComplete);

  const keys: string[] = [k.stream, k.completed, k.events, k.job(jobId), k.metricsCompleted];
  const args: string[] = [
    jobId,
    entryId,
    returnvalue,
    timestamp.toString(),
    group,
    mode,
    count.toString(),
    age.toString(),
  ];

  if (parentInfo) {
    const pk = parentInfo.parentKeys;
    keys.push(pk.deps(parentInfo.parentId), pk.job(parentInfo.parentId), pk.stream, pk.events);
    args.push(parentInfo.depsMember, parentInfo.parentId);
  } else {
    args.push('', '');
  }

  if (broadcastMode) args.push('1');

  return client.fcall('glidemq_complete', keys, args);
}

/**
 * Complete current job AND fetch+activate the next job in a single round trip.
 * In steady state (jobs available), this reduces per-job overhead from 2 RTTs to 1.
 *
 * Returns:
 * - { completed, next: false } if no more jobs in the stream
 * - { completed, next: 'REVOKED', nextJobId, nextEntryId } if next job is revoked
 * - { completed, next: Record<string,string>, nextJobId, nextEntryId } with next job hash fields
 */
export interface CompleteAndFetchResult {
  completed: string;
  next: false | 'REVOKED' | Record<string, string>;
  nextJobId?: string;
  nextEntryId?: string;
}

export interface CompleteAndFetchHints {
  orderingKey?: string;
  orderingSeq?: number;
  groupKey?: string;
}

export async function completeAndFetchNext(
  client: Client,
  k: QueueKeys,
  jobId: string,
  entryId: string,
  returnvalue: string,
  timestamp: number,
  group: string,
  consumer: string,
  removeOnComplete?: boolean | number | { age: number; count: number },
  parentInfo?: { depsMember: string; parentId: string; parentKeys: QueueKeys },
  hints?: CompleteAndFetchHints,
  broadcastMode?: boolean,
  processedOn?: number,
  hasParents?: boolean,
  skipEvents?: boolean,
  skipMetrics?: boolean,
): Promise<CompleteAndFetchResult> {
  const { mode, count, age } = encodeRetention(removeOnComplete);

  const keys: string[] = [k.stream, k.completed, k.events, k.job(jobId), k.metricsCompleted];
  const args: string[] = [
    jobId,
    entryId,
    returnvalue,
    timestamp.toString(),
    group,
    consumer,
    mode,
    count.toString(),
    age.toString(),
  ];

  if (parentInfo) {
    const pk = parentInfo.parentKeys;
    keys.push(pk.deps(parentInfo.parentId), pk.job(parentInfo.parentId), pk.stream, pk.events);
    args.push(parentInfo.depsMember, parentInfo.parentId);
  } else {
    args.push('', '');
  }

  const orderingSeqHint =
    hints?.orderingSeq != null && Number.isFinite(hints.orderingSeq) ? Math.trunc(hints.orderingSeq).toString() : '';
  // '__' sentinel = confirmed absent (skip Lua call entirely); '' = unknown (Lua may HGET internally)
  args.push(
    hints?.orderingKey === undefined ? '__' : hints.orderingKey,
    orderingSeqHint,
    hints?.groupKey === undefined ? '__' : hints.groupKey,
  );
  args.push(broadcastMode ? '1' : '0');
  args.push(processedOn != null ? processedOn.toString() : '');
  args.push(hasParents ? '1' : '0');
  args.push(skipEvents ? '1' : '0');
  args.push(skipMetrics ? '1' : '0');

  const raw = await client.fcall('glidemq_completeAndFetchNext', keys, args);

  // Fast path: array protocol from Lua function
  if (Array.isArray(raw)) {
    const tag = String(raw[0]);
    if (tag === 'NEXT_NONE') {
      return { completed: raw[1] != null ? String(raw[1]) : jobId, next: false };
    }
    if (tag === 'NEXT_REVOKED') {
      return {
        completed: raw[1] != null ? String(raw[1]) : jobId,
        next: 'REVOKED',
        nextJobId: String(raw[2]),
        nextEntryId: String(raw[3]),
      };
    }
    if (tag === 'NEXT_HASH') {
      const hash: Record<string, string> = Object.create(null);
      for (let i = 4; i + 1 < raw.length; i += 2) {
        hash[String(raw[i])] = String(raw[i + 1]);
      }
      return {
        completed: raw[1] != null ? String(raw[1]) : jobId,
        next: hash,
        nextJobId: String(raw[2]),
        nextEntryId: String(raw[3]),
      };
    }
    throw new Error(`Unexpected glidemq_completeAndFetchNext tag: ${tag}`);
  }

  // Backward compatibility: JSON protocol (older library versions)
  const parsed = JSON.parse(String(raw));
  if (!parsed.next || parsed.next === false) {
    return { completed: parsed.completed, next: false };
  }
  if (parsed.next === 'REVOKED') {
    return {
      completed: parsed.completed,
      next: 'REVOKED',
      nextJobId: parsed.nextJobId,
      nextEntryId: parsed.nextEntryId,
    };
  }
  const parsedHash = parsed.next as string[];
  const hash: Record<string, string> = Object.create(null);
  for (let i = 0; i < parsedHash.length; i += 2) {
    hash[String(parsedHash[i])] = String(parsedHash[i + 1]);
  }
  return {
    completed: parsed.completed,
    next: hash,
    nextJobId: parsed.nextJobId,
    nextEntryId: parsed.nextEntryId,
  };
}

/**
 * Fail a job: XACK, retry with backoff if attempts remain, else move to failed ZSet.
 * Optionally applies retention cleanup based on removeOnFail.
 * Returns "failed" or "retrying".
 */
export async function failJob(
  client: Client,
  k: QueueKeys,
  jobId: string,
  entryId: string,
  failedReason: string,
  timestamp: number,
  maxAttempts: number,
  backoffDelay: number,
  group: string = CONSUMER_GROUP,
  removeOnFail?: boolean | number | { age: number; count: number },
  broadcastMode?: boolean,
): Promise<string> {
  const { mode, count, age } = encodeRetention(removeOnFail);
  const result = await client.fcall(
    'glidemq_fail',
    [k.stream, k.failed, k.scheduled, k.events, k.job(jobId), k.metricsFailed],
    [
      jobId,
      entryId,
      failedReason,
      timestamp.toString(),
      maxAttempts.toString(),
      backoffDelay.toString(),
      group,
      mode,
      count.toString(),
      age.toString(),
      ...(broadcastMode ? ['1'] : []),
    ],
  );
  return result as string;
}

/**
 * Reclaim stalled jobs via XAUTOCLAIM. Jobs exceeding maxStalledCount are moved to failed.
 * Returns the number of jobs reclaimed.
 */
export async function reclaimStalled(
  client: Client,
  k: QueueKeys,
  consumer: string,
  minIdleMs: number,
  maxStalledCount: number,
  timestamp: number,
  group: string = CONSUMER_GROUP,
  broadcastMode?: boolean,
  workerLockDuration: number = 0,
): Promise<number> {
  const result = await client.fcall(
    'glidemq_reclaimStalled',
    [k.stream, k.events],
    [
      group,
      consumer,
      minIdleMs.toString(),
      maxStalledCount.toString(),
      timestamp.toString(),
      k.failed,
      broadcastMode ? '1' : '0',
      workerLockDuration.toString(),
    ],
  );
  return result as number;
}

/**
 * Reclaim stalled list-sourced jobs (LIFO/priority) that are invisible to XAUTOCLAIM.
 * Uses bounded SCAN to find active list jobs with stale lastActive, then applies stall logic.
 */
export async function reclaimStalledListJobs(
  client: Client,
  k: QueueKeys,
  minIdleMs: number,
  maxStalledCount: number,
  timestamp: number,
  workerLockDuration: number = 0,
): Promise<number> {
  const result = await client.fcall(
    'glidemq_reclaimStalledListJobs',
    [k.stream, k.events],
    [minIdleMs.toString(), maxStalledCount.toString(), timestamp.toString(), k.failed, workerLockDuration.toString()],
  );
  return result as number;
}

/**
 * Pause a queue: sets paused=1 in meta hash, emits event.
 */
export async function pause(client: Client, k: QueueKeys): Promise<void> {
  await client.fcall('glidemq_pause', [k.meta, k.events], []);
}

/**
 * Resume a queue: sets paused=0 in meta hash, emits event.
 */
export async function resume(client: Client, k: QueueKeys): Promise<void> {
  await client.fcall('glidemq_resume', [k.meta, k.events], []);
}

/**
 * Check and enforce rate limiting using a sliding window counter.
 * Returns 0 if the job is allowed, or a positive number of ms to wait.
 */
export async function rateLimit(
  client: Client,
  k: QueueKeys,
  maxPerWindow: number,
  windowDuration: number,
  timestamp: number,
): Promise<number> {
  const result = await client.fcall(
    'glidemq_rateLimit',
    [k.rate, k.meta],
    [maxPerWindow.toString(), windowDuration.toString(), timestamp.toString()],
  );
  return result as number;
}

/**
 * Check global concurrency: returns -1 if no limit is set, 0 if blocked
 * (pending >= globalConcurrency), or a positive number indicating remaining
 * capacity (globalConcurrency - pending).
 */
export async function checkConcurrency(client: Client, k: QueueKeys, group: string = CONSUMER_GROUP): Promise<number> {
  const result = await client.fcall('glidemq_checkConcurrency', [k.meta, k.stream, k.listActive], [group]);
  return result as number;
}

/**
 * Atomically check global concurrency capacity, RPOP up to `count` jobs from a list,
 * and INCRBY list-active counter by the number popped.
 * Returns an array of popped jobIds (may be empty).
 */
export async function rpopAndReserve(
  client: Client,
  k: QueueKeys,
  listKey: string,
  group: string = CONSUMER_GROUP,
  count: number = 1,
): Promise<string[]> {
  const safeCount = Math.max(1, Math.min(Math.floor(count) || 1, 1000));
  const result = await client.fcall(
    'glidemq_rpopAndReserve',
    [k.meta, k.stream, k.listActive, listKey],
    [group, safeCount.toString()],
  );
  if (!Array.isArray(result) || result.length === 0) return [];
  return result.map((v) => String(v));
}

/**
 * Pop from priority and LIFO lists in a single FCALL (1 RTT instead of 2).
 * Returns job IDs popped, or empty array if both lists are empty.
 */
export async function popLists(client: Client, k: QueueKeys, count: number): Promise<string[]> {
  const result = await client.fcall('glidemq_popLists', [k.priority, k.lifo], [count.toString()]);
  if (!Array.isArray(result) || result.length === 0) return [];
  return result.map((v) => String(v));
}

/**
 * Move a job to active state in a single round trip.
 * Reads the full job hash, checks revoked flag, sets state=active + processedOn + lastActive.
 * For group-concurrency jobs, checks if the group has capacity. If not, parks the job
 * in the group wait list and returns 'GROUP_FULL'.
 * For rate-limited groups, parks the job and returns 'GROUP_RATE_LIMITED'.
 * Returns:
 * - null if job hash doesn't exist
 * - 'REVOKED' if the job's revoked flag is set
 * - 'GROUP_FULL' if the job's group is at max concurrency (job was parked)
 * - 'GROUP_RATE_LIMITED' if the job's group exceeded its rate limit (job was parked)
 * - 'GROUP_TOKEN_LIMITED' if the job's group has insufficient tokens (job was parked)
 * - 'ERR:COST_EXCEEDS_CAPACITY' if the job cost exceeds token bucket capacity (job was failed)
 * - Record<string, string> with all job fields otherwise
 */
export async function moveToActive(
  client: Client,
  k: QueueKeys,
  jobId: string,
  timestamp: number,
  streamKey: string = '',
  entryId: string = '',
  group: string = '',
  broadcastMode?: boolean,
): Promise<
  | Record<string, string>
  | 'REVOKED'
  | 'EXPIRED'
  | 'GROUP_FULL'
  | 'GROUP_RATE_LIMITED'
  | 'GROUP_TOKEN_LIMITED'
  | 'GROUP_ORDERED'
  | 'ERR:COST_EXCEEDS_CAPACITY'
  | null
> {
  const keys: string[] = [k.job(jobId)];
  const args: string[] = [timestamp.toString()];
  if (streamKey) {
    keys.push(streamKey);
    args.push(entryId, group, jobId);
    if (broadcastMode) args.push('1');
  }
  const result = await client.fcall('glidemq_moveToActive', keys, args);

  if (Array.isArray(result)) {
    if (result.length === 0) return null;
    const hash: Record<string, string> = Object.create(null);
    for (let i = 0; i + 1 < result.length; i += 2) {
      hash[String(result[i])] = String(result[i + 1]);
    }
    return hash;
  }

  const str = String(result);
  if (str === '' || str === 'null') return null;
  if (str === 'REVOKED') return 'REVOKED';
  if (str === 'EXPIRED') return 'EXPIRED';
  if (str === 'GROUP_FULL') return 'GROUP_FULL';
  if (str === 'GROUP_RATE_LIMITED') return 'GROUP_RATE_LIMITED';
  if (str === 'GROUP_TOKEN_LIMITED') return 'GROUP_TOKEN_LIMITED';
  if (str === 'GROUP_ORDERED') return 'GROUP_ORDERED';
  if (str === 'ERR:COST_EXCEEDS_CAPACITY') return 'ERR:COST_EXCEEDS_CAPACITY';
  // Backward compatibility: older library returns cjson string
  const arr = JSON.parse(str) as string[];
  const hash: Record<string, string> = Object.create(null);
  for (let i = 0; i < arr.length; i += 2) {
    hash[String(arr[i])] = String(arr[i + 1]);
  }
  return hash;
}

/**
 * Promote rate-limited groups whose window has expired.
 * Moves waiting jobs from the group queue back into the stream.
 * Returns the number of jobs promoted.
 */
export async function promoteRateLimited(client: Client, k: QueueKeys, timestamp: number): Promise<number> {
  const result = await client.fcall('glidemq_promoteRateLimited', [k.ratelimited, k.stream], [timestamp.toString()]);
  return Number(result) || 0;
}

/**
 * Rate-limit a specific ordering group from inside a worker processor.
 * Re-parks the current job in the group queue and registers the group
 * in the ratelimited ZADD for the scheduler to unblock after duration.
 * Returns the resumeAt timestamp as a string, or 'error:...' on failure.
 */
export async function rateLimitGroup(
  client: Client,
  k: QueueKeys,
  jobId: string,
  entryId: string,
  duration: number,
  timestamp: number,
  group: string,
  currentJob: 'requeue' | 'fail' = 'requeue',
  requeuePosition: 'front' | 'back' = 'front',
  extend: 'max' | 'replace' = 'max',
  broadcastMode?: boolean,
): Promise<string> {
  const result = await client.fcall(
    'glidemq_rateLimitGroup',
    [k.job(jobId), k.stream],
    [
      jobId,
      entryId,
      group,
      duration.toString(),
      timestamp.toString(),
      broadcastMode ? '1' : '0',
      currentJob,
      requeuePosition,
      extend,
    ],
  );
  return String(result);
}

/**
 * Rate-limit a group from outside the worker (e.g. from a webhook or health check).
 * Registers the group in the ratelimited ZADD without re-parking a specific job.
 * Returns the resumeAt timestamp.
 */
export async function rateLimitGroupExternal(
  client: Client,
  k: QueueKeys,
  groupKey: string,
  duration: number,
  timestamp: number,
  extend: 'max' | 'replace' = 'max',
): Promise<number> {
  const result = await client.fcall(
    'glidemq_rateLimitGroupExternal',
    [k.ratelimited, k.events],
    [groupKey, duration.toString(), timestamp.toString(), extend],
  );
  return Number(result) || 0;
}

/**
 * Defers an active job back to waiting by acknowledging + deleting the current
 * stream entry and re-enqueuing the same jobId to the stream tail.
 * If the job hash no longer exists, it only removes the stream entry.
 */
export async function deferActive(
  client: Client,
  k: QueueKeys,
  jobId: string,
  entryId: string,
  group: string = CONSUMER_GROUP,
  broadcastMode?: boolean,
): Promise<void> {
  const args = [jobId, entryId, group];
  if (broadcastMode) args.push('1');
  await client.fcall('glidemq_deferActive', [k.stream, k.job(jobId), k.listActive], args);
}

/**
 * Remove a job from all data structures (hash, stream, scheduled, completed, failed).
 * Returns 1 if removed, 0 if not found.
 */
export async function removeJob(client: Client, k: QueueKeys, jobId: string): Promise<number> {
  const result = await client.fcall(
    'glidemq_removeJob',
    [k.job(jobId), k.stream, k.scheduled, k.completed, k.failed, k.events, k.log(jobId)],
    [jobId],
  );
  return result as number;
}

/**
 * Bulk-remove old completed or failed jobs by age.
 * Removes job hashes, log keys, and ZSet entries for jobs older than cutoff.
 * Returns an array of removed job IDs.
 */
export async function cleanJobs(
  client: Client,
  k: QueueKeys,
  type: 'completed' | 'failed',
  grace: number,
  limit: number,
  timestamp: number,
): Promise<string[]> {
  if (type !== 'completed' && type !== 'failed') {
    throw new TypeError(`clean type must be 'completed' or 'failed', got '${type}'`);
  }
  const cutoff = timestamp - grace;
  const setKey = type === 'completed' ? k.completed : k.failed;
  const result = await client.fcall('glidemq_clean', [setKey, k.events, k.id], [cutoff.toString(), limit.toString()]);
  return Array.isArray(result) ? result.map((r) => String(r)) : [];
}

/**
 * Drain the queue: remove all waiting jobs from the stream (skipping active ones).
 * Optionally also remove all delayed/scheduled jobs.
 * Deletes associated job/log/deps hashes. Emits 'drained' event.
 * Returns the number of removed jobs.
 */
export async function drainQueue(
  client: Client,
  k: QueueKeys,
  delayed: boolean,
  group: string = CONSUMER_GROUP,
): Promise<number> {
  const result = await client.fcall(
    'glidemq_drain',
    [k.stream, k.scheduled, k.events, k.id, k.lifo, k.priority],
    [delayed ? '1' : '0', group],
  );
  return Number(result) || 0;
}

/**
 * Bulk retry failed jobs.
 * Moves jobs from the failed ZSet to the scheduled ZSet for re-processing.
 * The promote cycle picks them up immediately (score = priority * PRIORITY_SHIFT + now).
 * Resets attemptsMade, failedReason, and finishedOn on each job hash.
 * Emits a single 'retried' event with the total count.
 * @param count - Maximum number of jobs to retry. 0 means all.
 * @returns The number of jobs retried.
 */
export async function retryJobs(client: Client, k: QueueKeys, count: number, timestamp: number): Promise<number> {
  const result = await client.fcall(
    'glidemq_retryJobs',
    [k.failed, k.scheduled, k.events, k.id],
    [count.toString(), timestamp.toString()],
  );
  return Number(result) || 0;
}

/**
 * Revoke a job. Sets 'revoked' flag on the job hash.
 * If the job is waiting/delayed/prioritized, removes from stream/scheduled and moves to failed.
 * If the job is active (being processed), just sets the flag - worker checks it cooperatively.
 * Returns 'revoked' (moved to failed), 'flagged' (flag set, job is active), or 'not_found'.
 */
export async function revokeJob(
  client: Client,
  k: QueueKeys,
  jobId: string,
  timestamp: number,
  group: string = CONSUMER_GROUP,
): Promise<string> {
  const result = await client.fcall(
    'glidemq_revoke',
    [k.job(jobId), k.stream, k.scheduled, k.failed, k.events],
    [jobId, timestamp.toString(), group],
  );
  return result as string;
}

/**
 * Change the priority of a job after enqueue.
 * Handles waiting, prioritized, and delayed states. Returns 'ok', 'no_op',
 * or an error string for invalid states.
 */
export async function changePriority(
  client: Client,
  k: QueueKeys,
  jobId: string,
  newPriority: number,
  group: string = CONSUMER_GROUP,
): Promise<string> {
  const result = await client.fcall(
    'glidemq_changePriority',
    [k.job(jobId), k.stream, k.scheduled, k.events],
    [jobId, newPriority.toString(), group],
  );
  return result as string;
}

/**
 * Change the delay of a job after enqueue.
 * Handles delayed, waiting, and prioritized states. Returns 'ok', 'no_op',
 * or an error string for invalid states.
 */
export async function changeDelay(
  client: Client,
  k: QueueKeys,
  jobId: string,
  newDelay: number,
  group: string = CONSUMER_GROUP,
): Promise<string> {
  const result = await client.fcall(
    'glidemq_changeDelay',
    [k.job(jobId), k.stream, k.scheduled, k.events],
    [jobId, newDelay.toString(), Date.now().toString(), group],
  );
  return result as string;
}

/**
 * Promote a delayed job to waiting immediately.
 * Removes from the scheduled ZSet, adds to the stream, sets state to 'waiting'.
 * Returns 'ok', 'error:not_found', or 'error:not_delayed'.
 */
export async function promoteJob(client: Client, k: QueueKeys, jobId: string): Promise<string> {
  const result = await client.fcall('glidemq_promoteJob', [k.job(jobId), k.stream, k.scheduled, k.events], [jobId]);
  return result as string;
}

/**
 * Move an active job back into the delayed/scheduled set.
 * Acknowledges the current stream entry and releases the active slot.
 */
export async function moveActiveToDelayed(
  client: Client,
  k: QueueKeys,
  jobId: string,
  entryId: string,
  delayedUntil: number,
  serializedData?: string,
  timestamp: number = Date.now(),
  group: string = CONSUMER_GROUP,
  broadcastMode?: boolean,
): Promise<string> {
  const args = [jobId, entryId, timestamp.toString(), delayedUntil.toString(), group, serializedData ?? ''];
  if (broadcastMode) args.push('1');
  const result = await client.fcall(
    'glidemq_moveActiveToDelayed',
    [k.job(jobId), k.stream, k.scheduled, k.events],
    args,
  );
  return result as string;
}

/**
 * Move an active job to waiting-children state.
 * The job pauses execution and waits for dynamically-added child jobs to complete.
 * Returns 'ok', 'completed' (all children already done), or 'error:*'.
 */
export async function moveToWaitingChildren(
  client: Client,
  k: QueueKeys,
  jobId: string,
  entryId: string,
  group: string = CONSUMER_GROUP,
  timestamp: number = Date.now(),
  broadcastMode?: boolean,
): Promise<string> {
  const args = [jobId, entryId, group, timestamp.toString()];
  if (broadcastMode) args.push('1');
  const result = await client.fcall('glidemq_moveToWaitingChildren', [k.job(jobId), k.stream, k.events], args);
  return result as string;
}

/**
 * Search for jobs by name within a specific state structure.
 * For ZSet states (completed, failed, delayed): iterates members and checks name.
 * For stream state (waiting): iterates stream entries and checks name.
 * Returns an array of matching job IDs.
 */
export async function searchByName(
  client: Client,
  stateKey: string,
  stateType: 'zset' | 'stream',
  nameFilter: string,
  limit: number,
  keyPrefix: string,
): Promise<string[]> {
  const result = await client.fcall(
    'glidemq_searchByName',
    [stateKey],
    [stateType, nameFilter, limit.toString(), keyPrefix],
  );
  if (!result) return [];
  if (Array.isArray(result)) {
    return result.map((r) => String(r));
  }
  return [];
}

/**
 * Atomically create a parent job (waiting-children) and its child jobs.
 * Returns a JSON array: [parentId, childId1, childId2, ...].
 */
export async function addFlow(
  client: Client,
  parentKeys: QueueKeys,
  parentName: string,
  parentData: string,
  parentOpts: string,
  timestamp: number,
  parentDelay: number,
  parentPriority: number,
  parentMaxAttempts: number,
  children: {
    name: string;
    data: string;
    opts: string;
    delay: number;
    priority: number;
    maxAttempts: number;
    keys: QueueKeys;
    queuePrefix: string;
    parentQueueName: string;
    customId: string;
  }[],
  extraDeps: string[] = [],
  parentCustomId: string = '',
): Promise<string[]> {
  const keys: string[] = [parentKeys.id, parentKeys.stream, parentKeys.scheduled, parentKeys.events];
  const args: string[] = [
    parentName,
    parentData,
    parentOpts,
    timestamp.toString(),
    parentDelay.toString(),
    parentPriority.toString(),
    parentMaxAttempts.toString(),
    children.length.toString(),
    parentCustomId,
  ];

  for (const child of children) {
    keys.push(child.keys.id, child.keys.stream, child.keys.scheduled, child.keys.events);
    args.push(
      child.name,
      child.data,
      child.opts,
      child.delay.toString(),
      child.priority.toString(),
      child.maxAttempts.toString(),
      child.queuePrefix,
      child.parentQueueName,
      child.customId,
    );
  }

  // Extra deps: pre-existing sub-flow children to add to deps set atomically
  args.push(extraDeps.length.toString());
  for (const dep of extraDeps) {
    args.push(dep);
  }

  const result = await client.fcall('glidemq_addFlow', keys, args);
  return JSON.parse(result as string) as string[];
}

/**
 * Remove a child from the parent's deps set. If all children are done, re-queues the parent.
 * Returns the number of remaining children (0 means parent was re-queued).
 */
export async function completeChild(
  client: Client,
  parentKeys: QueueKeys,
  parentId: string,
  depsMember: string,
): Promise<number> {
  const result = await client.fcall(
    'glidemq_completeChild',
    [parentKeys.deps(parentId), parentKeys.job(parentId), parentKeys.stream, parentKeys.events],
    [depsMember, parentId],
  );
  return result as number;
}

/**
 * Register an additional parent for an existing child job (DAG multi-parent).
 * If the child has already completed, triggers parent notification immediately.
 * Returns 'ok', 'already_completed', or 'error:child_not_found'.
 */
export async function registerParent(
  client: Client,
  childKeys: QueueKeys,
  childJobId: string,
  parentId: string,
  parentQueue: string,
  parentKeys: QueueKeys,
  depsMember: string,
): Promise<string> {
  const result = await client.fcall(
    'glidemq_registerParent',
    [
      childKeys.job(childJobId),
      childKeys.parents(childJobId),
      parentKeys.deps(parentId),
      parentKeys.job(parentId),
      parentKeys.stream,
      parentKeys.events,
    ],
    [childJobId, parentId, parentQueue, depsMember],
  );
  return result as string;
}

/**
 * Self-heal the list-active counter by comparing its value against
 * the actual count of active list-sourced jobs (LIFO or priority).
 * Returns the drift amount corrected (0 if no correction was needed).
 */
export async function healListActive(client: Client, keys: QueueKeys): Promise<number> {
  const result = await client.fcall('glidemq_healListActive', [keys.id], []);
  return Number(result) || 0;
}

/**
 * Return active list-sourced jobIds (priority or LIFO) via bounded SCAN.
 * Pagination follows the same convention as Queue.getJobs: inclusive 0-indexed
 * bounds; end < 0 means unbounded.
 * Used by Queue.getJobs('active') to merge list-active jobs with the stream PEL.
 */
export async function getActiveListJobIds(
  client: Client,
  keys: QueueKeys,
  start: number,
  end: number,
): Promise<string[]> {
  const result = await client.fcall('glidemq_getActiveListJobIds', [keys.id], [start.toString(), end.toString()]);
  if (!Array.isArray(result)) return [];
  return result.map((v) => String(v));
}

/**
 * Suspend an active job. Moves it from active to the suspended sorted set.
 * Returns 'ok', or 'error:not_found' / 'error:not_active'.
 */
export async function suspendJob(
  client: Client,
  k: QueueKeys,
  jobId: string,
  entryId: string,
  group: string,
  timestamp: number,
  reason?: string,
  timeout?: number,
  broadcastMode?: boolean,
): Promise<string> {
  const args = [jobId, entryId, group, timestamp.toString(), reason || '', (timeout || 0).toString()];
  if (broadcastMode) args.push('1');
  const result = await client.fcall('glidemq_suspend', [k.job(jobId), k.stream, k.events, k.suspended], args);
  return result as string;
}

/**
 * Send a signal to a suspended job. Stores the signal and re-queues the job.
 * Returns 'ok' if the job was resumed, or 'not_suspended'.
 */
export async function signalJob(
  client: Client,
  k: QueueKeys,
  jobId: string,
  signalName: string,
  signalData: string,
  timestamp: number,
): Promise<string> {
  const result = await client.fcall(
    'glidemq_signal',
    [k.job(jobId), k.stream, k.events, k.suspended, k.signals(jobId)],
    [jobId, signalName, signalData, timestamp.toString()],
  );
  return result as string;
}

/**
 * Sweep expired suspended jobs whose timeout has passed.
 * Moves them to failed state. Returns the number of jobs failed.
 */
export async function sweepSuspended(
  client: Client,
  k: QueueKeys,
  timestamp: number,
  keyPrefix: string,
): Promise<number> {
  const result = await client.fcall(
    'glidemq_sweepSuspended',
    [k.suspended, k.events, k.failed, k.metricsFailed],
    [timestamp.toString(), keyPrefix],
  );
  return Number(result) || 0;
}

/**
 * Check whether a budget has been exceeded. Returns 'ok', 'exceeded', or 'no_budget'.
 */
export async function checkBudget(client: Client, budgetKey: string): Promise<string> {
  const result = await client.fcall('glidemq_checkBudget', [budgetKey], []);
  return result as string;
}

/**
 * Atomically record usage and check whether the budget is now exceeded.
 * Supports per-category tracking, weighted totals, and per-category limits.
 * Returns 'ok', 'exceeded', or 'no_budget'.
 */
export async function recordUsageAndCheckBudget(
  client: Client,
  budgetKey: string,
  tokens: Record<string, number>,
  costs: Record<string, number>,
  weightedTotal: number,
  totalCost: number,
  maxTokens: Record<string, number>,
  maxCosts: Record<string, number>,
): Promise<string> {
  const result = await client.fcall(
    'glidemq_recordUsageAndCheckBudget',
    [budgetKey],
    [
      JSON.stringify(tokens),
      JSON.stringify(costs),
      weightedTotal.toString(),
      totalCost.toString(),
      JSON.stringify(maxTokens),
      JSON.stringify(maxCosts),
    ],
  );
  return result as string;
}
