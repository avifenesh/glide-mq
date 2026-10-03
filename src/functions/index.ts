import { RequestError } from '@glidemq/speedkey';
import type { GlideReturnType, GlideString } from '@glidemq/speedkey';
import type { Client } from '../types';
import { buildKeys, isFunctionNotFound, parseCrossQueueParentNotification } from '../utils';
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
// Version 94: glidemq_reclaimStalled persists a per-group XAUTOCLAIM cursor and bounds each reclaim batch to 100 entries so repeated scheduler ticks progress through large PELs.
// Version 96: Honor queue pause in activation paths (moveToActive, completeAndFetchNext next-fetch, popLists, rpopAndReserve) so Queue.pause() stops workers from claiming new jobs. Pause-race defer restores list jobs in their original dispatch order, and broadcast claims stay in the subscription PEL (no XADD duplicate).
// Version 97: Preserve batch pause-race list claim order and skip stalled reclaim while the queue is paused so parked PEL/list claims survive until resume.
// Version 97: completion and per-job batch failures refuse jobs revoked while their processor was running.
// Version 98: glidemq_popListsReserve reserves list-active in the same FCALL as the list pop, closing the worker crash window between pop and INCRBY; legacy glidemq_popLists remains non-reserving for rolling compatibility.
// Version 102: rate-limited token-bucket promotion skips bounded tombstones and re-registers the group when more cleanup remains.
// Version 103: ordered rate-limited promotion advances nextSeq past missing head tombstones.
// Version 104: ordered tombstone cleanup also advances the worker orderdone frontier.
// Version 105: Terminal stalled recovery atomically advances repeatAfterComplete schedulers without lossy scheduler JSON reserialization.
// Version 106: Terminal TTL expiry atomically advances repeatAfterComplete schedulers through the shared expireJob path.
// Version 107: Terminal suspend timeout, capacity rejection, and group-rate failure atomically advance repeatAfterComplete schedulers.
// Version 99: closeOrderingHole wakes parked groupq successors after debounce/remove/TTL of an unrun seq; rateLimitGroup keeps the ordered slot on requeue; CAF priority path applies token-bucket and rate-limit gates.
// Version 100: rateLimitGroup fail pauses before promote; promoteRateLimited finds returning ordered jobs requeued at the back; CAF oversized priority jobs close the ordering hole and count as failed.
// Version 107: ordered rate-limit returns use an explicit marker so retained-slot jobs are promoted beyond bounded scans; oversized token failures advance ordering state.
// Version 108: revoke/remove delete list-backed waiting entries so getJobs pagination cannot expose or count stale jobs.
// Version 110: derive list keys in remove/revoke and remove waiting FIFO stream entries.
// Version 111: skip the FIFO stream scan when removing list-backed waiting jobs.
// Version 112: route remove/revoke FIFO cleanup by actual waiting-list membership.
// Version 112: ordered terminal paths resolve group-key ordering metadata before waking successors.
// Version 113: ordered rate-limit returns track every retained slot; terminal paths release retained slots; oversized-head cleanup is iterative and bounded.
// Version 114: ordered retry, delay, suspend, and waiting-children transitions mark their retained group slot.
// Version 115: token-bucket gate each ordered rate-limit returner before it resumes.
// Version 116: retained ordered-return slots use a namespace that cannot alias user group hashes.
// Version 117: migrate retained-return slots from the v115 namespace before consuming them.
// Version 101: complete/CAF enqueue cross-queue parent notifies on a same-slot pending set so a later scheduler tick can retry if the worker dies between complete and completeChild.
// Version 102: completeChild ignores stale notifications for deleted parent hashes.
// Version 104: encode cross-queue notifications as JSON and reconcile deleted children without recreating hashes.
// Version 107: extract the final queue hash tag for cross-queue DAG notifications so custom prefixes may contain braces.
// Version 108: all parent dependency completion paths ignore deleted parent hashes.
// Version 109: completion replies carry cross-queue parent notifications for eager delivery.
// Version 110: dedupe overlapping tree and DAG parent notifications per completion.
// Version 119: integrate the reviewed correctness queue and preserve Queue.pause in the reservation-aware list pop.
// Version 120: removeJob acknowledges a claimed FIFO entry in every stream consumer group before deleting it.
// Version 121: tbRefill uses Redis server time and stamps capacity so idle-full time is not later granted as extra tokens.
// Version 122: moveToWaitingChildren unparks immediately when no child deps exist.
// Version 123: glidemq_complete honors skipEvents/skipMetrics; deferActive can undo a CAF group reservation.
// Version 124: undoGroupClaim skips active/nextSeq rewind when the job holds retainedSlot.
// Version 125: removeJob resolves parents and clears an active job's PEL entry; drain closes ordering holes;
//   completion paths skip removed jobs; retry re-sequences ordered jobs; listSourced marker;
//   healListActive needs a full scan; moveToActive rejects stale claims ('STALE'); glidemq_retryJob;
//   changePriority/changeDelay handle list-held jobs; debounce resolves replaced children;
//   glidemq_updateJobFields writes only existing job hashes.
// Version 127: glidemq_casSchedulerEntry lets upsertJobScheduler write only over the entry it read.
// Version 126: cross-queue child completions that reach the parent before registration are parked
//   (depearly) and counted by glidemq_registerChildDep; debounce replacements inherit the replaced
//   cross-queue parent dependency (replacedIds); addJob/addFlow accept an optional trailing budgetKey;
//   CAF marks ordering done from the groupKey hint and no longer emits list-phase 'active' events;
//   broadcast retry counters expire 24h past the retry; removeOnComplete/removeOnFail skip writes to
//   the deleted hash; list claims are tracked in list-active-ids so list-active scans skip SCAN.
// Version 128: version 127 (casSchedulerEntry) plus the version 126 changes above.
// Version 129: reclaimStalled/reclaimStalledListJobs reply stalled IDs on an optional returnIds arg;
//   glidemq_fail honors optional skipEvents/skipMetrics args; addJob/dedup/addFlow reject a priority
//   outside 0..2048 integers with an error reply, and changePriority returns error:invalid_priority for it.
// Version 130: broadcast reclaimStalled counts stalls per subscription (job:<id>:sub:<group> 's') and,
//   on an optional redispatch arg, replies the reclaimed entries so the worker runs them again;
//   glidemq_trimBroadcast trims a broadcast stream and deletes the job data of trimmed messages;
//   promote records the newest broadcast retry entry in the job hash (bcastEntry).
// Version 131: glidemq_fail takes an optional requeueOnly arg (RateLimitError) that schedules the retry
//   without counting the attempt or writing failedReason; glidemq_updateFlowBudget changes budget limits
//   and re-evaluates the exceeded flag; glidemq_failAndFetchNext fails a job and fetches the next one
//   (the fetch phases of completeAndFetchNext) in one call; glidemq_trimBroadcast replies
//   {trimmed, unread} instead of the trimmed count.
// Version 132: scheduler entries record inflightJobId and glidemq_schedulerAwaitInflight parks a
//   repeatAfterComplete entry while the old mode's job still runs; completeAndFetchNext/failAndFetchNext
//   append optional trailing markers (__glidemq_failed_activations__ for jobs failed at activation,
//   __glidemq_lists_empty__ when lists and stream were empty); glidemq_moveToActive enforces
//   globalConcurrency for stream claims on an optional arg (GLOBAL_FULL); glidemq_removeIdleConsumer and
//   an optional reclaimStalled arg delete consumers with no pending entry; broadcast heartbeats and stall
//   checks use the per-subscription 'la' field, deferActive marks handed-back claims (hb) that reclaim
//   does not count as stalls, glidemq_recoverBroadcastClaims re-takes only owned entries, trimBroadcast
//   looks up pre-130 retry entries; glidemq_healEarlyDeps releases parents registered by a plain SADD;
//   healListActive re-seeds list-active-ids from a complete scan; no 'active' event is written anywhere.
// Version 133: token bucket tbRefill pulls a tbLastRefill ahead of the server clock back to server time
//   when the bucket is full, and group setup (addJob, dedup, addFlow) seeds tbLastRefill
//   from the server clock instead of the caller's, so a fast producer clock no longer drops the time
//   since a consumption from the next refill.
export const LIBRARY_VERSION = '133';

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
  budgetKey: string = '',
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
      // Appended optional arg: older libraries ignore it.
      ...(budgetKey ? [budgetKey] : []),
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
  budgetKey: string = '',
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
    budgetKey,
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
const PARENT_NOTIFICATIONS_MARKER = '__glidemq_parent_notifications__';
const FAILED_ACTIVATIONS_MARKER = '__glidemq_failed_activations__';
const LISTS_EMPTY_MARKER = '__glidemq_lists_empty__';
const COMPLETE_REVOKED_MARKER = '__glidemq_complete_revoked__';

/** A job the chain call failed at activation (server-side terminal failure). */
export interface FailedActivation {
  jobId: string;
  reason: string;
}

function parseFailedActivations(raw: unknown): FailedActivation[] {
  if (typeof raw !== 'string' || raw === '') return [];
  try {
    const parsed: unknown = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    const out: FailedActivation[] = [];
    for (const entry of parsed) {
      if (entry && typeof entry === 'object' && typeof (entry as { id?: unknown }).id === 'string') {
        out.push({ jobId: (entry as { id: string }).id, reason: String((entry as { reason?: unknown }).reason ?? '') });
      }
    }
    return out;
  } catch {
    return [];
  }
}

function parseParentNotifications(raw: unknown): string[] {
  if (typeof raw !== 'string' || raw === '') return [];
  try {
    const parsed: unknown = JSON.parse(raw);
    return Array.isArray(parsed) && parsed.every((member) => typeof member === 'string') ? parsed : [];
  } catch {
    return [];
  }
}

export function isCompleteJobRevoked(result: string[]): boolean {
  return result.length === 1 && result[0] === COMPLETE_REVOKED_MARKER;
}

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
  skipEvents?: boolean,
  skipMetrics?: boolean,
): Promise<string[]> {
  const { keys, args } = completeJobCall(
    k,
    jobId,
    entryId,
    returnvalue,
    timestamp,
    group,
    removeOnComplete,
    parentInfo,
    broadcastMode,
    skipEvents,
    skipMetrics,
  );
  return parseCompleteJobResult(await client.fcall('glidemq_complete', keys, args));
}

/** Keys and args of a glidemq_complete call, for direct FCALL or a pipeline. */
export function completeJobCall(
  k: QueueKeys,
  jobId: string,
  entryId: string,
  returnvalue: string,
  timestamp: number,
  group: string = CONSUMER_GROUP,
  removeOnComplete?: boolean | number | { age: number; count: number },
  parentInfo?: { depsMember: string; parentId: string; parentKeys: QueueKeys },
  broadcastMode?: boolean,
  skipEvents?: boolean,
  skipMetrics?: boolean,
): { keys: string[]; args: string[] } {
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

  args.push(broadcastMode ? '1' : '0', skipEvents ? '1' : '0', skipMetrics ? '1' : '0');
  return { keys, args };
}

/** Parse a glidemq_complete reply. See isCompleteJobRevoked. */
export function parseCompleteJobResult(raw: GlideReturnType): string[] {
  return String(raw) === 'REVOKED' ? [COMPLETE_REVOKED_MARKER] : parseParentNotifications(raw);
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
  next: false | 'REVOKED' | 'CURRENT_REVOKED' | Record<string, string>;
  nextJobId?: string;
  nextEntryId?: string;
  /** Retryable cross-queue parent notifications recorded by the completion FCALL. */
  parentNotifications: string[];
  /**
   * Jobs the call failed terminally at activation (cost over token bucket
   * capacity) while fetching the next job or promoting a group. A worker with a
   * dead letter queue adds their DLQ copies. Library 132+; empty before.
   */
  failedActivations: FailedActivation[];
  /**
   * NEXT_NONE because the priority list, the LIFO list and the stream were all
   * empty when the call looked (library 132+). The poll loop skips its
   * pre-block list pop. False for every other NEXT_NONE (pause, group parking,
   * token bucket) and for older libraries.
   */
  listsEmpty: boolean;
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
  if (Array.isArray(raw)) return parseChainReply(raw, jobId, 'glidemq_completeAndFetchNext');

  // Backward compatibility: JSON protocol (older library versions)
  const parsed = JSON.parse(String(raw));
  if (!parsed.next || parsed.next === false) {
    return {
      completed: parsed.completed,
      next: false,
      parentNotifications: [],
      failedActivations: [],
      listsEmpty: false,
    };
  }
  if (parsed.next === 'REVOKED') {
    return {
      completed: parsed.completed,
      next: 'REVOKED',
      nextJobId: parsed.nextJobId,
      nextEntryId: parsed.nextEntryId,
      parentNotifications: [],
      failedActivations: [],
      listsEmpty: false,
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
    parentNotifications: [],
    failedActivations: [],
    listsEmpty: false,
  };
}

/**
 * Parse the array reply of the chain functions: {'NEXT_NONE', jobId, ...},
 * {'CURRENT_REVOKED', jobId}, {'NEXT_REVOKED', jobId, nextJobId, nextEntryId, ...}
 * or {'NEXT_HASH', jobId, nextJobId, nextEntryId, field1, value1, ..., ...}.
 * Cross-queue completions append the parent notifications marker and payload.
 */
function parseChainReply(raw: unknown[], jobId: string, func: string): CompleteAndFetchResult {
  // Trailing markers, each followed by one payload: parent notifications last,
  // failed activations before it. Read from the end until no marker matches.
  let end = raw.length;
  let parentNotifications: string[] = [];
  let failedActivations: FailedActivation[] = [];
  let listsEmpty = false;
  while (end >= 2) {
    const marker = String(raw[end - 2]);
    if (marker === PARENT_NOTIFICATIONS_MARKER) {
      parentNotifications = parseParentNotifications(raw[end - 1]);
    } else if (marker === FAILED_ACTIVATIONS_MARKER) {
      failedActivations = parseFailedActivations(raw[end - 1]);
    } else if (marker === LISTS_EMPTY_MARKER) {
      listsEmpty = String(raw[end - 1]) === '1';
    } else {
      break;
    }
    end -= 2;
  }
  const tag = String(raw[0]);
  const completed = raw[1] != null ? String(raw[1]) : jobId;
  if (tag === 'NEXT_NONE') {
    return { completed, next: false, parentNotifications, failedActivations, listsEmpty };
  }
  if (tag === 'CURRENT_REVOKED') {
    return { completed, next: 'CURRENT_REVOKED', parentNotifications: [], failedActivations: [], listsEmpty: false };
  }
  if (tag === 'NEXT_REVOKED') {
    return {
      completed,
      next: 'REVOKED',
      nextJobId: String(raw[2]),
      nextEntryId: String(raw[3]),
      parentNotifications,
      failedActivations,
      listsEmpty: false,
    };
  }
  if (tag === 'NEXT_HASH') {
    const hash: Record<string, string> = Object.create(null);
    for (let i = 4; i + 1 < end; i += 2) {
      hash[String(raw[i])] = String(raw[i + 1]);
    }
    return {
      completed,
      next: hash,
      nextJobId: String(raw[2]),
      nextEntryId: String(raw[3]),
      parentNotifications,
      failedActivations,
      listsEmpty: false,
    };
  }
  throw new Error(`Unexpected ${func} tag: ${tag}`);
}

export interface FailAndFetchResult extends CompleteAndFetchResult {
  /** glidemq_fail outcome for the failed job: 'retrying', 'failed' or 'removed'. */
  failResult: string;
}

/**
 * Fail a job and fetch the next one in the same FCALL (glidemq_failAndFetchNext,
 * library 131). Same semantics as failJob for the failed job; the fetch
 * phases are those of completeAndFetchNext. Rejects with "Function not found"
 * on an older library; callers fall back to failJob.
 */
export async function failAndFetchNext(
  client: Client,
  k: QueueKeys,
  jobId: string,
  entryId: string,
  failedReason: string,
  timestamp: number,
  maxAttempts: number,
  backoffDelay: number,
  group: string,
  consumer: string,
  removeOnFail?: boolean | number | { age: number; count: number },
  skipEvents?: boolean,
  skipMetrics?: boolean,
): Promise<FailAndFetchResult> {
  const { mode, count, age } = encodeRetention(removeOnFail);
  const args = [
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
    '0',
    skipEvents ? '1' : '0',
    skipMetrics ? '1' : '0',
    consumer,
  ];
  const raw = await client.fcall(
    'glidemq_failAndFetchNext',
    [k.stream, k.failed, k.scheduled, k.events, k.job(jobId), k.metricsFailed],
    args,
  );
  if (!Array.isArray(raw) || raw.length < 2) {
    throw new Error(`Unexpected glidemq_failAndFetchNext reply: ${String(raw)}`);
  }
  return { failResult: String(raw[0]), ...parseChainReply(raw.slice(1), jobId, 'glidemq_failAndFetchNext') };
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
  skipEvents?: boolean,
  skipMetrics?: boolean,
  requeueOnly?: boolean,
): Promise<string> {
  const { keys, args } = failJobCall(
    k,
    jobId,
    entryId,
    failedReason,
    timestamp,
    maxAttempts,
    backoffDelay,
    group,
    removeOnFail,
    broadcastMode,
    skipEvents,
    skipMetrics,
    requeueOnly,
  );
  const result = await client.fcall('glidemq_fail', keys, args);
  return result as string;
}

/**
 * Build the KEYS/ARGS of a glidemq_fail call. `requeueOnly` (a RateLimitError)
 * schedules the retry after backoffDelay without counting the attempt; an
 * older library ignores the flag and counts it, so maxAttempts must still
 * allow the retry.
 */
export function failJobCall(
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
  skipEvents?: boolean,
  skipMetrics?: boolean,
  requeueOnly?: boolean,
): { keys: string[]; args: string[] } {
  const { mode, count, age } = encodeRetention(removeOnFail);
  const args = [
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
  ];
  if (requeueOnly) {
    args.push(broadcastMode ? '1' : '0', skipEvents ? '1' : '0', skipMetrics ? '1' : '0', '1');
  } else if (skipEvents || skipMetrics) {
    args.push(broadcastMode ? '1' : '0', skipEvents ? '1' : '0', skipMetrics ? '1' : '0');
  } else if (broadcastMode) {
    args.push('1');
  }
  return { keys: [k.stream, k.failed, k.scheduled, k.events, k.job(jobId), k.metricsFailed], args };
}

export interface TrimBroadcastResult {
  /** Stream entries trimmed by this call. */
  trimmed: number;
  /**
   * (entry, subscription) pairs dropped before that subscription read them.
   * 0 on a library older than 131, which reports only the trimmed count.
   */
  unread: number;
}

/**
 * Trim a broadcast stream to `maxLen` entries and delete the job hashes,
 * per-subscription hashes and completed/failed members of trimmed messages.
 * Messages a subscription still has claimed or scheduled for retry are kept
 * until they settle.
 */
export async function trimBroadcast(
  client: Client,
  k: QueueKeys,
  maxLen: number,
  timestamp: number,
): Promise<TrimBroadcastResult> {
  const result = await client.fcall(
    'glidemq_trimBroadcast',
    [k.stream, k.completed, k.failed, k.scheduled],
    [maxLen.toString(), timestamp.toString()],
  );
  if (Array.isArray(result)) {
    return { trimmed: Number(result[0]) || 0, unread: Number(result[1]) || 0 };
  }
  return { trimmed: Number(result) || 0, unread: 0 };
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
  idleConsumerMs: number = 0,
): Promise<number> {
  const args = [
    group,
    consumer,
    minIdleMs.toString(),
    maxStalledCount.toString(),
    timestamp.toString(),
    k.failed,
    broadcastMode ? '1' : '0',
    workerLockDuration.toString(),
  ];
  // Appended optional args (returnIds, redispatch, idleConsumerMs); older
  // libraries ignore them.
  if (idleConsumerMs > 0) args.push('0', '0', Math.trunc(idleConsumerMs).toString());
  const result = await client.fcall('glidemq_reclaimStalled', [k.stream, k.events], args);
  return result as number;
}

/**
 * Delete this consumer from the group when it holds no pending entry, in one
 * step with the check (glidemq_removeIdleConsumer, library 132). Returns
 * whether it was deleted; false on an older library.
 */
export async function removeIdleConsumer(
  client: Client,
  k: QueueKeys,
  group: string,
  consumer: string,
): Promise<boolean> {
  try {
    const result = await client.fcall('glidemq_removeIdleConsumer', [k.stream], [group, consumer]);
    return Number(result) === 1;
  } catch (err) {
    if (!isFunctionNotFound(err)) throw err;
    return false;
  }
}

/** Reply of a stalled reclaim that asked for the IDs of the jobs it returned to waiting. */
export interface ReclaimStalledResult {
  /** Jobs handled by this call (the integer reply of the count-only form). */
  count: number;
  /** Jobs that stalled and went back to waiting. Empty when the library predates the IDs reply. */
  stalledIds: string[];
  /**
   * Broadcast entries this call moved into the caller's PEL for it to run
   * again. Only filled on a redispatch call to a library that supports it.
   */
  redispatch: { jobId: string; entryId: string }[];
}

/**
 * Parse a reclaim reply: {count, stalledId...} from a library that honors the
 * returnIds arg, or the plain integer count from an older one.
 */
export function parseReclaimStalledResult(raw: GlideReturnType): ReclaimStalledResult {
  if (Array.isArray(raw)) {
    // Redispatch reply: {count, {stalledId...}, {jobId, entryId, ...}}.
    if (Array.isArray(raw[1])) {
      const pairs = Array.isArray(raw[2]) ? raw[2] : [];
      const redispatch: { jobId: string; entryId: string }[] = [];
      for (let i = 0; i + 1 < pairs.length; i += 2) {
        redispatch.push({ jobId: String(pairs[i]), entryId: String(pairs[i + 1]) });
      }
      return { count: Number(raw[0]) || 0, stalledIds: raw[1].map((v) => String(v)), redispatch };
    }
    return { count: Number(raw[0]) || 0, stalledIds: raw.slice(1).map((v) => String(v)), redispatch: [] };
  }
  return { count: Number(raw) || 0, stalledIds: [], redispatch: [] };
}

/**
 * reclaimStalled that also returns the IDs of the jobs that stalled and went
 * back to waiting (the ones that got a 'stalled' event). With `redispatch`
 * in broadcast mode it also returns the entries it moved into `consumer`'s PEL,
 * which the caller must run again (XCLAIM by ID).
 */
export async function reclaimStalledWithIds(
  client: Client,
  k: QueueKeys,
  consumer: string,
  minIdleMs: number,
  maxStalledCount: number,
  timestamp: number,
  group: string = CONSUMER_GROUP,
  broadcastMode?: boolean,
  workerLockDuration: number = 0,
  redispatch?: boolean,
  idleConsumerMs: number = 0,
): Promise<ReclaimStalledResult> {
  const args = [
    group,
    consumer,
    minIdleMs.toString(),
    maxStalledCount.toString(),
    timestamp.toString(),
    k.failed,
    broadcastMode ? '1' : '0',
    workerLockDuration.toString(),
    '1',
    broadcastMode && redispatch ? '1' : '0',
  ];
  if (idleConsumerMs > 0) args.push(Math.trunc(idleConsumerMs).toString());
  const result = await client.fcall('glidemq_reclaimStalled', [k.stream, k.events], args);
  return parseReclaimStalledResult(result);
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

/** reclaimStalledListJobs that also returns the IDs of the jobs that went back to waiting. */
export async function reclaimStalledListJobsWithIds(
  client: Client,
  k: QueueKeys,
  minIdleMs: number,
  maxStalledCount: number,
  timestamp: number,
  workerLockDuration: number = 0,
): Promise<ReclaimStalledResult> {
  const result = await client.fcall(
    'glidemq_reclaimStalledListJobs',
    [k.stream, k.events],
    [
      minIdleMs.toString(),
      maxStalledCount.toString(),
      timestamp.toString(),
      k.failed,
      workerLockDuration.toString(),
      '1',
    ],
  );
  return parseReclaimStalledResult(result);
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
 * Pop from priority and LIFO lists, reserving list-active atomically when the
 * reservation-aware function is available. During rolling upgrades, fall back
 * to the legacy pop and typed counter increment if that function is missing.
 * Returns job IDs popped, or empty array if both lists are empty.
 */
export async function popLists(client: Client, k: QueueKeys, count: number): Promise<string[]> {
  const safeCount = Math.max(1, Math.min(Math.floor(count) || 1, 1000));
  let result: GlideReturnType;
  try {
    result = await client.fcall('glidemq_popListsReserve', [k.priority, k.lifo, k.listActive], [safeCount.toString()]);
  } catch (error) {
    if (!(error instanceof RequestError) || !/function\s+not\s+found/i.test(error.message)) throw error;
    result = await client.fcall('glidemq_popLists', [k.priority, k.lifo, k.listActive], [safeCount.toString()]);
    if (Array.isArray(result) && result.length > 0) await client.incrBy(k.listActive, result.length);
  }
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
 * - 'PAUSED' if the queue is paused (job was not activated)
 * - 'GROUP_FULL' if the job's group is at max concurrency (job was parked)
 * - 'GROUP_RATE_LIMITED' if the job's group exceeded its rate limit (job was parked)
 * - 'GROUP_TOKEN_LIMITED' if the job's group has insufficient tokens (job was parked)
 * - 'ERR:COST_EXCEEDS_CAPACITY' if the job cost exceeds token bucket capacity (job was failed)
 * - 'GLOBAL_FULL' if `enforceGlobalConcurrency` was set and this stream claim is outside the
 *   queue's globalConcurrency (job left waiting in the PEL; the worker hands it back)
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
  enforceGlobalConcurrency?: boolean,
): Promise<MoveToActiveResult> {
  const { keys, args } = moveToActiveCall(
    k,
    jobId,
    timestamp,
    streamKey,
    entryId,
    group,
    broadcastMode,
    enforceGlobalConcurrency,
  );
  return parseMoveToActiveResult(await client.fcall('glidemq_moveToActive', keys, args));
}

export type MoveToActiveResult =
  | Record<string, string>
  | 'REVOKED'
  | 'PAUSED'
  | 'EXPIRED'
  | 'GROUP_FULL'
  | 'GROUP_RATE_LIMITED'
  | 'GROUP_TOKEN_LIMITED'
  | 'GROUP_ORDERED'
  | 'ERR:COST_EXCEEDS_CAPACITY'
  | 'STALE'
  | 'GLOBAL_FULL'
  | null;

/** Keys and args of a glidemq_moveToActive call, for direct FCALL or a pipeline. */
export function moveToActiveCall(
  k: QueueKeys,
  jobId: string,
  timestamp: number,
  streamKey: string = '',
  entryId: string = '',
  group: string = '',
  broadcastMode?: boolean,
  enforceGlobalConcurrency?: boolean,
): { keys: string[]; args: string[] } {
  const keys: string[] = [k.job(jobId)];
  const args: string[] = [timestamp.toString()];
  if (streamKey) {
    keys.push(streamKey);
    args.push(entryId, group, jobId);
    // Appended optional args: broadcast flag, then the global concurrency
    // gate (library 132; older libraries ignore it).
    if (broadcastMode) args.push('1');
    else if (enforceGlobalConcurrency) args.push('0');
    if (enforceGlobalConcurrency) args.push('1');
  }
  return { keys, args };
}

/** Parse a glidemq_moveToActive reply into a job hash or a status marker. */
export function parseMoveToActiveResult(result: GlideReturnType): MoveToActiveResult {
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
  if (str === 'PAUSED') return 'PAUSED';
  if (str === 'EXPIRED') return 'EXPIRED';
  if (str === 'GROUP_FULL') return 'GROUP_FULL';
  if (str === 'GROUP_RATE_LIMITED') return 'GROUP_RATE_LIMITED';
  if (str === 'GROUP_TOKEN_LIMITED') return 'GROUP_TOKEN_LIMITED';
  if (str === 'GROUP_ORDERED') return 'GROUP_ORDERED';
  if (str === 'ERR:COST_EXCEEDS_CAPACITY') return 'ERR:COST_EXCEEDS_CAPACITY';
  if (str === 'STALE') return 'STALE';
  if (str === 'GLOBAL_FULL') return 'GLOBAL_FULL';
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
 * stream entry and re-enqueuing the same jobId. Pause-race list jobs are restored
 * to their original priority/LIFO list, and broadcast claims remain in the PEL.
 * If the job hash no longer exists, it only removes the stream entry.
 */
export async function deferActive(
  client: Client,
  k: QueueKeys,
  jobId: string,
  entryId: string,
  group: string = CONSUMER_GROUP,
  broadcastMode?: boolean,
  opts?: { pausedRestore?: boolean; undoGroupClaim?: boolean; consumer?: string },
): Promise<void> {
  const args = [
    jobId,
    entryId,
    group,
    broadcastMode ? '1' : '0',
    opts?.pausedRestore ? '1' : '0',
    opts?.undoGroupClaim ? '1' : '0',
  ];
  // Broadcast pause: the hand-back mark is written only for a claim this
  // consumer still owns (appended optional arg, library 132).
  if (opts?.consumer) args.push(opts.consumer);
  await client.fcall('glidemq_deferActive', [k.stream, k.job(jobId), k.listActive], args);
}

/**
 * Re-take parked broadcast entries this consumer still owns
 * (glidemq_recoverBroadcastClaims, library 132). Entries another consumer
 * reclaimed meanwhile are skipped. Falls back to XCLAIM on an older library.
 * Returns entries as XREADGROUP does: entryId to field pairs.
 */
export async function recoverBroadcastClaims(
  client: Client,
  k: QueueKeys,
  group: string,
  consumer: string,
  entryIds: string[],
): Promise<Record<string, [GlideString, GlideString][]>> {
  let raw: unknown;
  try {
    raw = await client.fcall('glidemq_recoverBroadcastClaims', [k.stream], [group, consumer, ...entryIds]);
  } catch (err) {
    if (!isFunctionNotFound(err)) throw err;
    return client.xclaim(k.stream, group, consumer, 0, entryIds);
  }
  const out: Record<string, [GlideString, GlideString][]> = Object.create(null);
  if (!Array.isArray(raw)) return out;
  for (const entry of raw) {
    if (!Array.isArray(entry) || !Array.isArray(entry[1])) continue;
    const fields = entry[1] as unknown[];
    const pairs: [GlideString, GlideString][] = [];
    for (let i = 0; i + 1 < fields.length; i += 2) {
      pairs.push([String(fields[i]), String(fields[i + 1])]);
    }
    out[String(entry[0])] = pairs;
  }
  return out;
}

/**
 * Remove a job from all data structures (hash, stream, scheduled, completed, failed).
 * A removed child resolves its parents. Cross-queue parents are notified here;
 * a failed delivery stays in xq-pending for the scheduler to retry.
 * Returns 1 if removed, 0 if not found.
 */
export async function removeJob(client: Client, k: QueueKeys, jobId: string): Promise<number> {
  const result = await client.fcall(
    'glidemq_removeJob',
    [k.job(jobId), k.stream, k.scheduled, k.completed, k.failed, k.events, k.log(jobId)],
    [jobId],
  );
  if (typeof result !== 'string') return Number(result) || 0;
  const suffix = `:{${k.name}}:id`;
  if (!k.id.endsWith(suffix)) return 1;
  const prefix = k.id.slice(0, -suffix.length);
  for (const member of parseParentNotifications(result)) {
    const notification = parseCrossQueueParentNotification(member);
    if (!notification) continue;
    const [parentQueue, parentId, depsMember] = notification;
    try {
      await completeChild(client, buildKeys(parentQueue, prefix), parentId, depsMember);
      await client.srem(k.xqPending, [member]);
    } catch {
      // The scheduler retries members left in xq-pending.
    }
  }
  return 1;
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
 * Write fields on an existing job hash, optionally emitting an event with
 * `data`. Returns false (and writes nothing) when the job does not exist, so a
 * removed job is never recreated as a stateless hash.
 */
export async function updateJobFields(
  client: Client,
  k: QueueKeys,
  jobId: string,
  fields: Record<string, GlideString>,
  event?: { type: string; data: string },
): Promise<boolean> {
  const args: GlideString[] = [jobId, event?.type ?? '', event?.data ?? ''];
  for (const [field, value] of Object.entries(fields)) args.push(field, value);
  const result = await client.fcall('glidemq_updateJobFields', [k.job(jobId), k.events], args);
  return Number(result) === 1;
}

/**
 * Retry one failed job: move it to the scheduled ZSet (promoted on the next
 * cycle), reset attempts and re-arm its TTL. Ordered jobs get a fresh sequence.
 * Returns 'ok', 'error:not_found' or 'error:not_failed'.
 */
export async function retryJob(client: Client, k: QueueKeys, jobId: string, timestamp: number): Promise<string> {
  const result = await client.fcall(
    'glidemq_retryJob',
    [k.job(jobId), k.failed, k.scheduled],
    [jobId, timestamp.toString()],
  );
  return String(result);
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
  budgetKey: string = '',
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
  // Appended optional arg: older libraries ignore it.
  if (budgetKey) args.push(budgetKey);

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
 * Write a scheduler entry only if the stored value still equals `expected`
 * ('' when the entry was absent). Returns true when written.
 */
export async function casSchedulerEntry(
  client: Client,
  queueKeys: QueueKeys,
  name: string,
  expected: string,
  value: string,
): Promise<boolean> {
  const result = await client.fcall('glidemq_casSchedulerEntry', [queueKeys.schedulers], [name, expected, value]);
  return Number(result) === 1;
}

/**
 * Park a repeatAfterComplete entry (nextRun 0) while the job its previous mode
 * fired is still running, in one step with the state check. Returns 'parked',
 * 'finished' (the job is terminal or gone: fire normally) or 'changed' (the
 * entry no longer matches `expected`). Libraries before 132 reply 'finished'.
 */
export async function schedulerAwaitInflight(
  client: Client,
  queueKeys: QueueKeys,
  name: string,
  expected: string,
  jobId: string,
): Promise<'parked' | 'finished' | 'changed'> {
  const jobKey = `${queueKeys.id.slice(0, -2)}job:${jobId}`;
  let result: unknown;
  try {
    result = await client.fcall('glidemq_schedulerAwaitInflight', [queueKeys.schedulers, jobKey], [name, expected]);
  } catch (err) {
    if (!isFunctionNotFound(err)) throw err;
    return 'finished';
  }
  const code = Number(result);
  return code === 1 ? 'parked' : code === 0 ? 'finished' : 'changed';
}

/**
 * Register a cross-queue child in its parent's deps set after the child was
 * added. A completion that reached the parent first is counted here, so the
 * parent is not released early. Falls back to a plain SADD when the loaded
 * library predates glidemq_registerChildDep (rolling upgrade).
 */
export async function registerChildDep(
  client: Client,
  parentKeys: QueueKeys,
  parentId: string,
  depsMember: string,
): Promise<void> {
  try {
    await client.fcall(
      'glidemq_registerChildDep',
      [parentKeys.deps(parentId), parentKeys.job(parentId), parentKeys.stream, parentKeys.events],
      [depsMember, parentId],
    );
  } catch (error) {
    if (!(error instanceof RequestError) || !/function\s+not\s+found/i.test(error.message)) throw error;
    await client.sadd(parentKeys.deps(parentId), [depsMember]);
  }
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
export async function healListActive(
  client: Client,
  keys: QueueKeys,
  scan?: { maxIter?: number; count?: number },
): Promise<number> {
  // Optional SCAN bounds for a re-seed (pages per call, keys per page); the
  // library defaults to 500 x 100. Appended args, ignored by older libraries.
  const args = scan ? [String(scan.maxIter ?? 500), String(scan.count ?? 100)] : [];
  const result = await client.fcall('glidemq_healListActive', [keys.id], args);
  return Number(result) || 0;
}

/**
 * Count parked early child completions for parents whose registration came
 * through a plain SADD (producer on a library before 126) and release the
 * parents that are complete (glidemq_healEarlyDeps, library 132). Bounded to
 * `max` parents per call. Returns the number released; 0 on an older library.
 */
export async function healEarlyDeps(client: Client, keys: QueueKeys, max = 100): Promise<number> {
  try {
    const result = await client.fcall('glidemq_healEarlyDeps', [keys.id], [max.toString()]);
    return Number(result) || 0;
  } catch (err) {
    if (!isFunctionNotFound(err)) throw err;
    return 0;
  }
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
/**
 * Raise or lower the limits of a flow budget and re-evaluate its exceeded
 * flag. A null value deletes the field. Returns 'ok', 'exceeded' or 'no_budget'.
 */
export async function updateFlowBudget(
  client: Client,
  budgetKey: string,
  fields: Record<string, string | null>,
): Promise<string> {
  const args: string[] = [];
  for (const [field, value] of Object.entries(fields)) args.push(field, value ?? '');
  const result = await client.fcall('glidemq_updateFlowBudget', [budgetKey], args);
  return result as string;
}

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
