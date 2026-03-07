import type { Client } from '../types';
import type { GlideReturnType } from '@glidemq/speedkey';

export const LIBRARY_NAME = 'glidemq';
// Version 44: Added metrics recording (time-series data for getMetrics).
// Version 45: DAG multi-parent dependencies - glidemq_registerParent, multi-parent completion notification.
// Version 46: Broadcast fan-out safety: broadcastMode flag in glidemq_complete/completeAndFetchNext/fail/reclaimStalled skips XDEL and per-subscription retry tracking.
// Version 47: Skip removeOnFail job-hash deletion in glidemq_fail when broadcastMode=1 (matches removeOnComplete fix from v46).
export const LIBRARY_VERSION = '47';

// Consumer group name used by workers
export const CONSUMER_GROUP = 'workers';

// Embedded Lua library source loaded via FUNCTION LOAD.
// This string is the runtime source of truth for Valkey function loading.
export const LIBRARY_SOURCE = `#!lua name=glidemq

local PRIORITY_SHIFT = 4398046511104

local function emitEvent(eventsKey, eventType, jobId, extraFields)
  local fields = {'event', eventType, 'jobId', tostring(jobId)}
  if extraFields then
    for i = 1, #extraFields, 2 do
      fields[#fields + 1] = extraFields[i]
      fields[#fields + 1] = extraFields[i + 1]
    end
  end
  redis.call('XADD', eventsKey, 'MAXLEN', '~', '1000', '*', unpack(fields))
end

local function markOrderingDone(jobKey, jobId, hintOrderingKey, hintOrderingSeq)
  local orderingKey = hintOrderingKey
  if not orderingKey or orderingKey == '' then
    orderingKey = redis.call('HGET', jobKey, 'orderingKey')
  end
  if not orderingKey or orderingKey == '' then
    return
  end
  local orderingSeq = nil
  if hintOrderingSeq ~= nil and hintOrderingSeq ~= '' then
    orderingSeq = tonumber(hintOrderingSeq) or 0
  else
    orderingSeq = tonumber(redis.call('HGET', jobKey, 'orderingSeq')) or 0
  end
  if orderingSeq <= 0 then
    return
  end

  local prefix = string.sub(jobKey, 1, #jobKey - #('job:' .. jobId))
  local metaKey = prefix .. 'meta'
  local doneField = 'orderdone:' .. orderingKey
  local pendingKey = prefix .. 'orderdone:pending:' .. orderingKey

  local lastDone = tonumber(redis.call('HGET', metaKey, doneField)) or 0
  if orderingSeq <= lastDone then
    redis.call('HDEL', pendingKey, tostring(orderingSeq))
    return
  end

  redis.call('HSET', pendingKey, tostring(orderingSeq), '1')
  local advanced = lastDone
  while true do
    local nextSeq = advanced + 1
    if redis.call('HEXISTS', pendingKey, tostring(nextSeq)) == 0 then
      break
    end
    redis.call('HDEL', pendingKey, tostring(nextSeq))
    advanced = nextSeq
  end
  if advanced > lastDone then
    redis.call('HSET', metaKey, doneField, tostring(advanced))
  end
end

-- Refill token bucket using remainder accumulator for precision.
-- tbRefillRate is in millitokens/second. Returns current millitokens after refill.
-- Side effect: updates tbTokens, tbLastRefill, tbRefillRemainder on the group hash.
local function tbRefill(groupHashKey, g, now)
  local tbCapacity = tonumber(g.tbCapacity) or 0
  if tbCapacity <= 0 then return 0 end
  local tbTokens = tonumber(g.tbTokens) or tbCapacity
  local tbRefillRate = tonumber(g.tbRefillRate) or 0
  local tbLastRefill = tonumber(g.tbLastRefill) or now
  local tbRefillRemainder = tonumber(g.tbRefillRemainder) or 0
  local elapsed = now - tbLastRefill
  if elapsed <= 0 or tbRefillRate <= 0 then return tbTokens end
  -- Cap elapsed to prevent overflow in long-idle buckets
  local maxElapsed = math.ceil(tbCapacity * 1000 / tbRefillRate)
  if elapsed > maxElapsed then elapsed = maxElapsed end
  local raw = elapsed * tbRefillRate + tbRefillRemainder
  local added = math.floor(raw / 1000)
  local newRemainder = raw % 1000
  local newTokens = math.min(tbCapacity, tbTokens + added)
  redis.call('HSET', groupHashKey,
    'tbTokens', tostring(newTokens),
    'tbLastRefill', tostring(now),
    'tbRefillRemainder', tostring(newRemainder))
  return newTokens
end

local function releaseGroupSlotAndPromote(jobKey, jobId, now, hintGroupKey)
  local gk = hintGroupKey
  if not gk or gk == '' then
    gk = redis.call('HGET', jobKey, 'groupKey')
  end
  if not gk or gk == '' then return end
  local prefix = string.sub(jobKey, 1, #jobKey - #('job:' .. jobId))
  local groupHashKey = prefix .. 'group:' .. gk
  -- Load all group fields in one call
  local gFields = redis.call('HGETALL', groupHashKey)
  local g = {}
  for gf = 1, #gFields, 2 do g[gFields[gf]] = gFields[gf + 1] end
  local cur = tonumber(g.active) or 0
  local newActive = (cur > 0) and (cur - 1) or 0
  if cur > 0 then
    redis.call('HSET', groupHashKey, 'active', tostring(newActive))
  end
  local waitListKey = prefix .. 'groupq:' .. gk
  local waitLen = redis.call('LLEN', waitListKey)
  if waitLen == 0 then return end
  -- Concurrency gate: if still at or above max after decrement, do not promote
  local maxConc = tonumber(g.maxConcurrency) or 0
  if maxConc > 0 and newActive >= maxConc then return end
  -- Rate limit gate (skip if now is nil or 0 for safe fallback)
  -- Only blocks promotion; does NOT increment rateCount. moveToActive handles counting.
  local rateMax = tonumber(g.rateMax) or 0
  local rateRemaining = 0
  local ts = tonumber(now) or 0
  if ts > 0 and rateMax > 0 then
    local rateDuration = tonumber(g.rateDuration) or 0
    if rateDuration > 0 then
      local rateWindowStart = tonumber(g.rateWindowStart) or 0
      local rateCount = tonumber(g.rateCount) or 0
      if ts - rateWindowStart < rateDuration then
        if rateCount >= rateMax then
          -- Window active and at capacity: do not promote, register for scheduler
          local rateLimitedKey = prefix .. 'ratelimited'
          redis.call('ZADD', rateLimitedKey, rateWindowStart + rateDuration, gk)
          return
        end
        rateRemaining = rateMax - rateCount
      end
    end
  end
  -- Token bucket gate: check head job cost before promoting
  local tbCap = tonumber(g.tbCapacity) or 0
  if ts > 0 and tbCap > 0 then
    local tbTokensCur = tbRefill(groupHashKey, g, ts)
    -- Peek at head job, skipping tombstones and DLQ'd jobs (up to 10 iterations)
    local tbCheckPasses = 0
    local tbOk = false
    while tbCheckPasses < 10 do
      tbCheckPasses = tbCheckPasses + 1
      local headJobId = redis.call('LINDEX', waitListKey, 0)
      if not headJobId then break end
      local headJobKey = prefix .. 'job:' .. headJobId
      -- Tombstone guard: job hash deleted - pop and check next
      if redis.call('EXISTS', headJobKey) == 0 then
        redis.call('LPOP', waitListKey)
      else
        local headCost = tonumber(redis.call('HGET', headJobKey, 'cost')) or 1000
        -- DLQ guard: cost > capacity - pop, fail, check next
        if headCost > tbCap then
          local metricsKey = prefix .. 'metrics:failed'
          local processedOn = tonumber(redis.call('HGET', headJobKey, 'processedOn')) or ts
          redis.call('LPOP', waitListKey)
          redis.call('ZADD', prefix .. 'failed', ts, headJobId)
          redis.call('HSET', headJobKey,
            'state', 'failed',
            'failedReason', 'cost exceeds token bucket capacity',
            'finishedOn', tostring(ts))
          emitEvent(prefix .. 'events', 'failed', headJobId, {'failedReason', 'cost exceeds token bucket capacity'})
          recordMetrics(metricsKey, ts, ts - processedOn)
        elseif tbTokensCur < headCost then
          -- Not enough tokens: register delay and skip promotion
          local tbRateVal = tonumber(g.tbRefillRate) or 0
          if tbRateVal <= 0 then break end
          local tbDelayMs = math.ceil((headCost - tbTokensCur) * 1000 / tbRateVal)
          local rateLimitedKey = prefix .. 'ratelimited'
          redis.call('ZADD', rateLimitedKey, ts + tbDelayMs, gk)
          return
        else
          tbOk = true
          break
        end
      end
    end
    if not tbOk and tbCheckPasses >= 10 then return end
  end
  -- Calculate how many slots are available for promotion
  local available = 1
  if maxConc > 0 then
    available = maxConc - newActive
  else
    available = math.min(waitLen, 1000)
  end
  -- Cap by rate limit remaining if a window is active
  if rateRemaining > 0 then
    available = math.min(available, rateRemaining)
  end
  local streamKey = prefix .. 'stream'
  for p = 1, available do
    local nextJobId = redis.call('LPOP', waitListKey)
    if not nextJobId then break end
    redis.call('XADD', streamKey, '*', 'jobId', nextJobId)
    local nextJobKey = prefix .. 'job:' .. nextJobId
    redis.call('HSET', nextJobKey, 'state', 'waiting')
  end
end

local function expireJob(jobKey, jobId, prefix, now, curState, hintOrderingKey, hintOrderingSeq, hintGroupKey)
  if curState == 'failed' then return true end
  local wasActive = (curState == 'active')
  local failedKey = prefix .. 'failed'
  local eventsKey = prefix .. 'events'
  local metricsKey = prefix .. 'metrics:failed'
  local processedOn = tonumber(redis.call('HGET', jobKey, 'processedOn')) or now
  redis.call('ZADD', failedKey, now, jobId)
  redis.call('HSET', jobKey,
    'state', 'failed',
    'failedReason', 'expired',
    'finishedOn', tostring(now))
  markOrderingDone(jobKey, jobId, hintOrderingKey, hintOrderingSeq)
  -- Only release group slot if the job was actually active (held a slot)
  if wasActive then
    releaseGroupSlotAndPromote(jobKey, jobId, now, hintGroupKey)
  end
  emitEvent(eventsKey, 'expired', jobId, nil)
  recordMetrics(metricsKey, now, now - processedOn)
  return true
end

local function checkExpired(jobKey, jobId, prefix, now)
  local expireAt = tonumber(redis.call('HGET', jobKey, 'expireAt'))
  if not expireAt or expireAt <= 0 then return false end
  if now <= expireAt then return false end
  -- Idempotency guard: if already expired, skip side effects
  local curState = redis.call('HGET', jobKey, 'state')
  return expireJob(jobKey, jobId, prefix, now, curState, nil, nil, nil)
end

local function extractOrderingKeyFromOpts(optsJson)
  if not optsJson or optsJson == '' then
    return ''
  end
  local ok, decoded = pcall(cjson.decode, optsJson)
  if not ok or type(decoded) ~= 'table' then
    return ''
  end
  local ordering = decoded['ordering']
  if type(ordering) ~= 'table' then
    return ''
  end
  local key = ordering['key']
  if key == nil then
    return ''
  end
  return tostring(key)
end

local function extractGroupConcurrencyFromOpts(optsJson)
  if not optsJson or optsJson == '' then
    return 0
  end
  local ok, decoded = pcall(cjson.decode, optsJson)
  if not ok or type(decoded) ~= 'table' then
    return 0
  end
  local ordering = decoded['ordering']
  if type(ordering) ~= 'table' then
    return 0
  end
  local conc = ordering['concurrency']
  if conc == nil then
    return 0
  end
  return tonumber(conc) or 0
end

local function extractGroupRateLimitFromOpts(optsJson)
  if not optsJson or optsJson == '' then
    return 0, 0
  end
  local ok, decoded = pcall(cjson.decode, optsJson)
  if not ok or type(decoded) ~= 'table' then
    return 0, 0
  end
  local ordering = decoded['ordering']
  if type(ordering) ~= 'table' then
    return 0, 0
  end
  local rl = ordering['rateLimit']
  if type(rl) ~= 'table' then
    return 0, 0
  end
  local max = tonumber(rl['max']) or 0
  local duration = tonumber(rl['duration']) or 0
  return max, duration
end

local function extractTokenBucketFromOpts(optsJson)
  if not optsJson or optsJson == '' then return 0, 0 end
  local ok, decoded = pcall(cjson.decode, optsJson)
  if not ok or type(decoded) ~= 'table' then return 0, 0 end
  local ordering = decoded['ordering']
  if type(ordering) ~= 'table' then return 0, 0 end
  local tb = ordering['tokenBucket']
  if type(tb) ~= 'table' then return 0, 0 end
  local capacity = tonumber(tb['capacity']) or 0
  local refillRate = tonumber(tb['refillRate']) or 0
  return math.floor(capacity * 1000), math.floor(refillRate * 1000)
end

local function extractCostFromOpts(optsJson)
  if not optsJson or optsJson == '' then return 0 end
  local ok, decoded = pcall(cjson.decode, optsJson)
  if not ok or type(decoded) ~= 'table' then return 0 end
  local cost = tonumber(decoded['cost']) or 0
  return math.floor(cost * 1000)
end

local function extractTtlFromOpts(optsJson)
  if not optsJson or optsJson == '' then return 0 end
  local ok, decoded = pcall(cjson.decode, optsJson)
  if not ok or type(decoded) ~= 'table' then return 0 end
  return tonumber(decoded['ttl']) or 0
end

-- Remove excess jobs from a sorted set in capped, stack-safe batches.
-- Deletes job hashes and removes from the set in chunks of 1000.
local function removeExcessJobs(setKey, prefix, ids)
  for i = 1, #ids do
    redis.call('DEL', prefix .. 'job:' .. ids[i])
  end
  for i = 1, #ids, 1000 do
    redis.call('ZREM', setKey, unpack(ids, i, math.min(i + 999, #ids)))
  end
end

local function recordMetrics(metricsKey, timestamp, duration)
  local minuteTs = timestamp - (timestamp % 60000)
  local newCount = tonumber(redis.call('HINCRBY', metricsKey, 'm:' .. minuteTs .. ':c', 1))
  if duration > 0 then
    redis.call('HINCRBY', metricsKey, 'm:' .. minuteTs .. ':d', duration)
  end
  if newCount and (newCount % 100 == 0) then
    local cutoff = minuteTs - 86400000
    local fields = redis.call('HKEYS', metricsKey)
    local toDelete = {}
    for _, f in ipairs(fields) do
      local ts = tonumber(string.match(f, '^m:(%d+):'))
      if ts and ts < cutoff then
        toDelete[#toDelete + 1] = f
      end
    end
    if #toDelete > 0 and #toDelete <= 1000 then
      redis.call('HDEL', metricsKey, unpack(toDelete))
    elseif #toDelete > 1000 then
      for i = 1, #toDelete, 1000 do
        redis.call('HDEL', metricsKey, unpack(toDelete, i, math.min(i + 999, #toDelete)))
      end
    end
  end
end

redis.register_function('glidemq_version', function(keys, args)
  return '${LIBRARY_VERSION}'
end)

redis.register_function('glidemq_addJob', function(keys, args)
  local idKey = keys[1]
  local streamKey = keys[2]
  local scheduledKey = keys[3]
  local eventsKey = keys[4]
  local jobName = args[1]
  local jobData = args[2]
  local jobOpts = args[3]
  local timestamp = tonumber(args[4])
  local delay = tonumber(args[5]) or 0
  local priority = tonumber(args[6]) or 0
  local parentId = args[7] or ''
  local maxAttempts = tonumber(args[8]) or 0
  local orderingKey = args[9] or ''
  local groupConcurrency = tonumber(args[10]) or 0
  local groupRateMax = tonumber(args[11]) or 0
  local groupRateDuration = tonumber(args[12]) or 0
  local tbCapacity = tonumber(args[13]) or 0
  local tbRefillRate = tonumber(args[14]) or 0
  local jobCost = tonumber(args[15]) or 0
  local ttl = tonumber(args[16]) or 0
  local customJobId = args[17] or ''
  local parentQueue = args[18] or ''
  local schedulerName = args[19] or ''
  local prefix = string.sub(idKey, 1, #idKey - 2)
  local jobIdStr
  local jobKey
  if customJobId ~= '' then
    jobKey = prefix .. 'job:' .. customJobId
    if redis.call('EXISTS', jobKey) == 1 then
      return 'duplicate'
    end
    jobIdStr = customJobId
  else
    local jobId = redis.call('INCR', idKey)
    jobIdStr = tostring(jobId)
    jobKey = prefix .. 'job:' .. jobIdStr
    local retries = 0
    while redis.call('EXISTS', jobKey) == 1 do
      retries = retries + 1
      if retries >= 1000 then return 'ERR:ID_EXHAUSTED' end
      jobId = redis.call('INCR', idKey)
      jobIdStr = tostring(jobId)
      jobKey = prefix .. 'job:' .. jobIdStr
    end
  end
  local useGroupConcurrency = (orderingKey ~= '' and (groupConcurrency > 1 or groupRateMax > 0 or tbCapacity > 0))
  local orderingSeq = 0
  if orderingKey ~= '' and not useGroupConcurrency then
    local orderingMetaKey = prefix .. 'ordering'
    orderingSeq = redis.call('HINCRBY', orderingMetaKey, orderingKey, 1)
  end
  if useGroupConcurrency then
    local groupHashKey = prefix .. 'group:' .. orderingKey
    local curMax = tonumber(redis.call('HGET', groupHashKey, 'maxConcurrency')) or 0
    if curMax ~= groupConcurrency then
      redis.call('HSET', groupHashKey, 'maxConcurrency', tostring(groupConcurrency))
    end
    -- When rate limit or token bucket forces group path but concurrency is 0 or 1, ensure maxConcurrency >= 1
    if curMax == 0 and groupConcurrency <= 1 then
      redis.call('HSET', groupHashKey, 'maxConcurrency', '1')
    end
    -- Upsert rate limit fields on group hash
    if groupRateMax > 0 then
      local curRateMax = tonumber(redis.call('HGET', groupHashKey, 'rateMax')) or 0
      if curRateMax ~= groupRateMax then
        redis.call('HSET', groupHashKey, 'rateMax', tostring(groupRateMax))
      end
      local curRateDuration = tonumber(redis.call('HGET', groupHashKey, 'rateDuration')) or 0
      if curRateDuration ~= groupRateDuration then
        redis.call('HSET', groupHashKey, 'rateDuration', tostring(groupRateDuration))
      end
    else
      -- Clear stale rate limit fields if group was previously rate-limited
      local oldRateMax = tonumber(redis.call('HGET', groupHashKey, 'rateMax')) or 0
      if oldRateMax > 0 then
        redis.call('HDEL', groupHashKey, 'rateMax', 'rateDuration', 'rateWindowStart', 'rateCount')
      end
    end
    -- Upsert token bucket fields on group hash
    if tbCapacity > 0 then
      local curTbCap = tonumber(redis.call('HGET', groupHashKey, 'tbCapacity')) or 0
      if curTbCap ~= tbCapacity then
        redis.call('HSET', groupHashKey, 'tbCapacity', tostring(tbCapacity))
      end
      local curTbRate = tonumber(redis.call('HGET', groupHashKey, 'tbRefillRate')) or 0
      if curTbRate ~= tbRefillRate then
        redis.call('HSET', groupHashKey, 'tbRefillRate', tostring(tbRefillRate))
      end
      -- Initialize tokens on first setup
      if curTbCap == 0 then
        redis.call('HSET', groupHashKey,
          'tbTokens', tostring(tbCapacity),
          'tbLastRefill', tostring(timestamp),
          'tbRefillRemainder', '0')
      end
      -- Validate cost <= capacity at enqueue
      -- Validate cost (explicit or default 1000 millitokens) against capacity
      local effectiveCost = (jobCost > 0) and jobCost or 1000
      if effectiveCost > tbCapacity then
        return 'ERR:COST_EXCEEDS_CAPACITY'
      end
    else
      -- Clear stale tb fields
      local oldTbCap = tonumber(redis.call('HGET', groupHashKey, 'tbCapacity')) or 0
      if oldTbCap > 0 then
        redis.call('HDEL', groupHashKey, 'tbCapacity', 'tbRefillRate', 'tbTokens', 'tbLastRefill', 'tbRefillRemainder')
      end
    end
  end
  local hashFields = {
    'id', jobIdStr,
    'name', jobName,
    'data', jobData,
    'opts', jobOpts,
    'timestamp', tostring(timestamp),
    'attemptsMade', '0',
    'delay', tostring(delay),
    'priority', tostring(priority),
    'maxAttempts', tostring(maxAttempts)
  }
  if useGroupConcurrency then
    hashFields[#hashFields + 1] = 'groupKey'
    hashFields[#hashFields + 1] = orderingKey
  elseif orderingKey ~= '' then
    hashFields[#hashFields + 1] = 'orderingKey'
    hashFields[#hashFields + 1] = orderingKey
    hashFields[#hashFields + 1] = 'orderingSeq'
    hashFields[#hashFields + 1] = tostring(orderingSeq)
  end
  if jobCost > 0 then
    hashFields[#hashFields + 1] = 'cost'
    hashFields[#hashFields + 1] = tostring(jobCost)
  end
  if ttl > 0 then
    hashFields[#hashFields + 1] = 'expireAt'
    hashFields[#hashFields + 1] = tostring(timestamp + ttl)
  end
  if parentId ~= '' then
    hashFields[#hashFields + 1] = 'parentId'
    hashFields[#hashFields + 1] = parentId
    if parentQueue ~= '' then
      hashFields[#hashFields + 1] = 'parentQueue'
      hashFields[#hashFields + 1] = parentQueue
    end
  end
  if schedulerName ~= '' then
    hashFields[#hashFields + 1] = 'schedulerName'
    hashFields[#hashFields + 1] = schedulerName
  end
  if delay > 0 or priority > 0 then
    hashFields[#hashFields + 1] = 'state'
    hashFields[#hashFields + 1] = delay > 0 and 'delayed' or 'prioritized'
  else
    hashFields[#hashFields + 1] = 'state'
    hashFields[#hashFields + 1] = 'waiting'
  end
  redis.call('HSET', jobKey, unpack(hashFields))
  -- Register child in parent's deps set when parentDepsKey is provided (keys[5])
  if parentId ~= '' and parentQueue ~= '' and #keys >= 5 then
    local parentDepsKey = keys[5]
    -- prefix includes trailing colon (glide:{Q}:), so strip it for depsMember
    local queuePrefix = string.sub(prefix, 1, #prefix - 1)
    local depsMember = queuePrefix .. ':' .. jobIdStr
    redis.call('SADD', parentDepsKey, depsMember)
  end
  if delay > 0 then
    local score = priority * PRIORITY_SHIFT + (timestamp + delay)
    redis.call('ZADD', scheduledKey, score, jobIdStr)
  elseif priority > 0 then
    local score = priority * PRIORITY_SHIFT
    redis.call('ZADD', scheduledKey, score, jobIdStr)
  else
    redis.call('XADD', streamKey, '*', 'jobId', jobIdStr)
  end
  emitEvent(eventsKey, 'added', jobIdStr, {'name', jobName})
  return jobIdStr
end)

redis.register_function('glidemq_promote', function(keys, args)
  local scheduledKey = keys[1]
  local streamKey = keys[2]
  local eventsKey = keys[3]
  local now = tonumber(args[1])
  local MAX_PROMOTIONS = 1000
  local count = 0
  local cursorMin = 0
  while count < MAX_PROMOTIONS do
    local nextEntry = redis.call('ZRANGEBYSCORE', scheduledKey, string.format('%.0f', cursorMin), '+inf', 'WITHSCORES', 'LIMIT', 0, 1)
    if not nextEntry or #nextEntry == 0 then
      break
    end
    local firstScore = tonumber(nextEntry[2]) or 0
    local priority = math.floor(firstScore / PRIORITY_SHIFT)
    local minScore = priority * PRIORITY_SHIFT
    local maxDueScore = minScore + now
    local remaining = MAX_PROMOTIONS - count
    local members = redis.call(
      'ZRANGEBYSCORE',
      scheduledKey,
      string.format('%.0f', minScore),
      string.format('%.0f', maxDueScore),
      'LIMIT',
      0,
      remaining
    )
    for i = 1, #members do
      local jobId = members[i]
      local prefix = string.sub(scheduledKey, 1, #scheduledKey - 9)
      local jobKey = prefix .. 'job:' .. jobId
      redis.call('ZREM', scheduledKey, jobId)
      if not checkExpired(jobKey, jobId, prefix, now) then
        redis.call('XADD', streamKey, '*', 'jobId', jobId)
        redis.call('HSET', jobKey, 'state', 'waiting')
        emitEvent(eventsKey, 'promoted', jobId, nil)
        count = count + 1
      end
    end
    cursorMin = (priority + 1) * PRIORITY_SHIFT
  end
  return count
end)

redis.register_function('glidemq_nextDue', function(keys, args)
  local scheduledKey = keys[1]
  local rateLimitedKey = keys[2]
  local nextDue = nil

  local scheduled = redis.call('ZRANGE', scheduledKey, 0, 0, 'WITHSCORES')
  if scheduled and #scheduled >= 2 then
    local score = tonumber(scheduled[2]) or 0
    local due = score % PRIORITY_SHIFT
    nextDue = due
  end

  local limited = redis.call('ZRANGE', rateLimitedKey, 0, 0, 'WITHSCORES')
  if limited and #limited >= 2 then
    local limitedDue = tonumber(limited[2]) or 0
    if (not nextDue) or limitedDue < nextDue then
      nextDue = limitedDue
    end
  end

  if not nextDue then
    return -1
  end

  return math.floor(nextDue)
end)

redis.register_function('glidemq_tryLock', function(keys, args)
  local lockKey = keys[1]
  local token = args[1]
  local ttl = tonumber(args[2]) or 1000
  local result = redis.call('SET', lockKey, token, 'PX', tostring(ttl), 'NX')
  if result then
    return 1
  end
  return 0
end)

redis.register_function('glidemq_unlock', function(keys, args)
  local lockKey = keys[1]
  local token = args[1]
  local current = redis.call('GET', lockKey)
  if current == token then
    redis.call('DEL', lockKey)
    return 1
  end
  return 0
end)

redis.register_function('glidemq_renewLock', function(keys, args)
  local lockKey = keys[1]
  local token = args[1]
  local ttl = tonumber(args[2]) or 1000
  local current = redis.call('GET', lockKey)
  if current == token then
    redis.call('PEXPIRE', lockKey, ttl)
    return 1
  end
  return 0
end)

redis.register_function('glidemq_complete', function(keys, args)
  local streamKey = keys[1]
  local completedKey = keys[2]
  local eventsKey = keys[3]
  local jobKey = keys[4]
  local metricsKey = keys[5]
  local jobId = args[1]
  local entryId = args[2]
  local returnvalue = args[3]
  local timestamp = tonumber(args[4])
  local group = args[5]
  local removeMode = args[6] or '0'
  local removeCount = tonumber(args[7]) or 0
  local removeAge = tonumber(args[8]) or 0
  local depsMember = args[9] or ''
  local parentId = args[10] or ''
  local broadcastMode = args[11] or '0'
  local processedOn = tonumber(redis.call('HGET', jobKey, 'processedOn')) or timestamp
  redis.call('XACK', streamKey, group, entryId)
  if broadcastMode ~= '1' then
    redis.call('XDEL', streamKey, entryId)
  end
  redis.call('ZADD', completedKey, timestamp, jobId)
  redis.call('HSET', jobKey,
    'state', 'completed',
    'returnvalue', returnvalue,
    'finishedOn', tostring(timestamp)
  )
  markOrderingDone(jobKey, jobId)
  releaseGroupSlotAndPromote(jobKey, jobId, timestamp)
  emitEvent(eventsKey, 'completed', jobId, {'returnvalue', returnvalue})
  recordMetrics(metricsKey, timestamp, timestamp - processedOn)
  local prefix = string.sub(jobKey, 1, #jobKey - #('job:' .. jobId))
  if broadcastMode ~= '1' then
    if removeMode == 'true' then
      redis.call('ZREM', completedKey, jobId)
      redis.call('DEL', jobKey)
    elseif removeMode == 'count' and removeCount > 0 then
      local total = redis.call('ZCARD', completedKey)
      if total > removeCount then
        local excess = redis.call('ZRANGE', completedKey, 0, math.min(total - removeCount, 1000) - 1)
        if #excess > 0 then removeExcessJobs(completedKey, prefix, excess) end
      end
    elseif removeMode == 'age_count' then
      if removeAge > 0 then
        local cutoff = timestamp - (removeAge * 1000)
        local old = redis.call('ZRANGEBYSCORE', completedKey, '0', string.format('%.0f', cutoff), 'LIMIT', 0, 1000)
        if #old > 0 then removeExcessJobs(completedKey, prefix, old) end
      end
      if removeCount > 0 then
        local total = redis.call('ZCARD', completedKey)
        if total > removeCount then
          local excess = redis.call('ZRANGE', completedKey, 0, math.min(total - removeCount, 1000) - 1)
          if #excess > 0 then removeExcessJobs(completedKey, prefix, excess) end
        end
      end
    end
  end
  if depsMember ~= '' and parentId ~= '' and #keys >= 9 then
    local parentDepsKey = keys[6]
    local parentJobKey = keys[7]
    local parentStreamKey = keys[8]
    local parentEventsKey = keys[9]
    local depMarker = 'depdone:' .. depsMember
    if redis.call('HSETNX', parentJobKey, depMarker, '1') == 1 then
      local doneCount = redis.call('HINCRBY', parentJobKey, 'depsCompleted', 1)
      local totalDeps = redis.call('SCARD', parentDepsKey)
      local remaining = totalDeps - doneCount
      if remaining <= 0 then
        local parentState = redis.call('HGET', parentJobKey, 'state')
        if parentState == 'waiting-children' then
          redis.call('HSET', parentJobKey, 'state', 'waiting')
          redis.call('XADD', parentStreamKey, '*', 'jobId', parentId)
          emitEvent(parentEventsKey, 'active', parentId, nil)
        end
      end
    end
  end
  -- DAG multi-parent: notify additional same-queue parents via parents SET
  local parentsKey = prefix .. 'parents:' .. jobId
  local dagParents = redis.call('SMEMBERS', parentsKey)
  if dagParents and #dagParents > 0 then
    local childQueuePrefix = string.sub(prefix, 1, #prefix - 1)
    local dagDepsMember = childQueuePrefix .. ':' .. jobId
    for pi = 1, #dagParents do
      local pEntry = dagParents[pi]
      -- Format: "parentQueuePrefix:parentId" where prefix is glide:{qname}
      -- Must find LAST colon (not first, since prefix contains colons in {})
      local pSep = nil
      for i = #pEntry, 1, -1 do
        if string.sub(pEntry, i, i) == ':' then
          pSep = i
          break
        end
      end
      if pSep then
        local pQueue = string.sub(pEntry, 1, pSep - 1)
        local pId = string.sub(pEntry, pSep + 1)
        -- Only handle same-queue parents atomically in Lua.
        -- Cross-queue parents are skipped here because their keys use a different
        -- hash tag (different prefix), which may route to a different cluster slot.
        -- The TypeScript layer handles cross-queue parent notification separately.
        if pQueue == childQueuePrefix then
          local pPrefix = prefix
          local pJobKey = pPrefix .. 'job:' .. pId
          local pDepsKey = pPrefix .. 'deps:' .. pId
          local pStreamKey = pPrefix .. 'stream'
          local pEventsKey = pPrefix .. 'events'
          local pDepMarker = 'depdone:' .. dagDepsMember
          if redis.call('HSETNX', pJobKey, pDepMarker, '1') == 1 then
            local pDoneCount = redis.call('HINCRBY', pJobKey, 'depsCompleted', 1)
            local pTotalDeps = redis.call('SCARD', pDepsKey)
            if pTotalDeps - pDoneCount <= 0 then
              local pState = redis.call('HGET', pJobKey, 'state')
              if pState == 'waiting-children' then
                redis.call('HSET', pJobKey, 'state', 'waiting')
                redis.call('XADD', pStreamKey, '*', 'jobId', pId)
                emitEvent(pEventsKey, 'active', pId, nil)
              end
            end
          end
        end
      end
    end
  end
  return 1
end)

redis.register_function('glidemq_completeAndFetchNext', function(keys, args)
  local streamKey = keys[1]
  local completedKey = keys[2]
  local eventsKey = keys[3]
  local jobKey = keys[4]
  local metricsKey = keys[5]
  local jobId = args[1]
  local entryId = args[2]
  local returnvalue = args[3]
  local timestamp = tonumber(args[4])
  local group = args[5]
  local consumer = args[6]
  local removeMode = args[7] or '0'
  local removeCount = tonumber(args[8]) or 0
  local removeAge = tonumber(args[9]) or 0
  local depsMember = args[10] or ''
  local parentId = args[11] or ''
  local currentOrderingKey = args[12] or ''
  local currentOrderingSeq = args[13] or ''
  local currentGroupKey = args[14] or ''
  local broadcastMode = args[15] or '0'

  -- Phase 1: Complete current job (same as glidemq_complete)
  local processedOn = tonumber(redis.call('HGET', jobKey, 'processedOn')) or timestamp
  redis.call('XACK', streamKey, group, entryId)
  if broadcastMode ~= '1' then
    redis.call('XDEL', streamKey, entryId)
  end
  redis.call('ZADD', completedKey, timestamp, jobId)
  redis.call('HSET', jobKey,
    'state', 'completed',
    'returnvalue', returnvalue,
    'finishedOn', tostring(timestamp)
  )
  markOrderingDone(jobKey, jobId, currentOrderingKey, currentOrderingSeq)
  releaseGroupSlotAndPromote(jobKey, jobId, timestamp, currentGroupKey)
  emitEvent(eventsKey, 'completed', jobId, {'returnvalue', returnvalue})
  recordMetrics(metricsKey, timestamp, timestamp - processedOn)
  local prefix = string.sub(jobKey, 1, #jobKey - #('job:' .. jobId))

  -- Retention cleanup (skip in broadcast mode - job hash must persist for all subscriptions)
  if broadcastMode ~= '1' then
    if removeMode == 'true' then
      redis.call('ZREM', completedKey, jobId)
      redis.call('DEL', jobKey)
    elseif removeMode == 'count' and removeCount > 0 then
      local total = redis.call('ZCARD', completedKey)
      if total > removeCount then
        local excess = redis.call('ZRANGE', completedKey, 0, math.min(total - removeCount, 1000) - 1)
        if #excess > 0 then removeExcessJobs(completedKey, prefix, excess) end
      end
    elseif removeMode == 'age_count' then
      if removeAge > 0 then
        local cutoff = timestamp - (removeAge * 1000)
        local old = redis.call('ZRANGEBYSCORE', completedKey, '0', string.format('%.0f', cutoff), 'LIMIT', 0, 1000)
        if #old > 0 then removeExcessJobs(completedKey, prefix, old) end
      end
      if removeCount > 0 then
        local total = redis.call('ZCARD', completedKey)
        if total > removeCount then
          local excess = redis.call('ZRANGE', completedKey, 0, math.min(total - removeCount, 1000) - 1)
          if #excess > 0 then removeExcessJobs(completedKey, prefix, excess) end
        end
      end
    end
  end

  -- Parent deps
  if depsMember ~= '' and parentId ~= '' and #keys >= 9 then
    local parentDepsKey = keys[6]
    local parentJobKey = keys[7]
    local parentStreamKey = keys[8]
    local parentEventsKey = keys[9]
    local depMarker = 'depdone:' .. depsMember
    if redis.call('HSETNX', parentJobKey, depMarker, '1') == 1 then
      local doneCount = redis.call('HINCRBY', parentJobKey, 'depsCompleted', 1)
      local totalDeps = redis.call('SCARD', parentDepsKey)
      if totalDeps - doneCount <= 0 then
        local parentState = redis.call('HGET', parentJobKey, 'state')
        if parentState == 'waiting-children' then
          redis.call('HSET', parentJobKey, 'state', 'waiting')
          redis.call('XADD', parentStreamKey, '*', 'jobId', parentId)
          emitEvent(parentEventsKey, 'active', parentId, nil)
        end
      end
    end
  end
  -- DAG multi-parent: notify additional same-queue parents via parents SET
  local parentsKey = prefix .. 'parents:' .. jobId
  local dagParents = redis.call('SMEMBERS', parentsKey)
  if dagParents and #dagParents > 0 then
    local childQueuePrefix = string.sub(prefix, 1, #prefix - 1)
    local dagDepsMember = childQueuePrefix .. ':' .. jobId
    for pi = 1, #dagParents do
      local pEntry = dagParents[pi]
      -- Format: "parentQueuePrefix:parentId" where prefix is glide:{qname}
      -- Must find LAST colon (not first, since prefix contains colons in {})
      local pSep = nil
      for i = #pEntry, 1, -1 do
        if string.sub(pEntry, i, i) == ':' then
          pSep = i
          break
        end
      end
      if pSep then
        local pQueue = string.sub(pEntry, 1, pSep - 1)
        local pId = string.sub(pEntry, pSep + 1)
        -- Only handle same-queue parents atomically in Lua.
        -- Cross-queue parents are skipped here because their keys use a different
        -- hash tag (different prefix), which may route to a different cluster slot.
        -- The TypeScript layer handles cross-queue parent notification separately.
        if pQueue == childQueuePrefix then
          local pPrefix = prefix
          local pJobKey = pPrefix .. 'job:' .. pId
          local pDepsKey = pPrefix .. 'deps:' .. pId
          local pStreamKey = pPrefix .. 'stream'
          local pEventsKey = pPrefix .. 'events'
          local pDepMarker = 'depdone:' .. dagDepsMember
          if redis.call('HSETNX', pJobKey, pDepMarker, '1') == 1 then
            local pDoneCount = redis.call('HINCRBY', pJobKey, 'depsCompleted', 1)
            local pTotalDeps = redis.call('SCARD', pDepsKey)
            if pTotalDeps - pDoneCount <= 0 then
              local pState = redis.call('HGET', pJobKey, 'state')
              if pState == 'waiting-children' then
                redis.call('HSET', pJobKey, 'state', 'waiting')
                redis.call('XADD', pStreamKey, '*', 'jobId', pId)
                emitEvent(pEventsKey, 'active', pId, nil)
              end
            end
          end
        end
      end
    end
  end

  -- In broadcast mode: do not fetch next (avoids XDEL of next entry which would break other consumer groups)
  if broadcastMode == '1' then
    return {'NEXT_NONE', jobId}
  end

  -- Return protocol (array-based to avoid cjson encode/decode per job):
  -- {'NEXT_NONE', completedJobId}
  -- {'NEXT_REVOKED', completedJobId, nextJobId, nextEntryId}
  -- {'NEXT_HASH', completedJobId, nextJobId, nextEntryId, field1, value1, field2, value2, ...}

  -- Phase 2: Fetch next job (non-blocking XREADGROUP), skip expired (up to 3 attempts)
  local nextJobId, nextEntryId, nextJobKey
  for _fetchAttempt = 1, 3 do
    local nextEntries = redis.call('XREADGROUP', 'GROUP', group, consumer, 'COUNT', 1, 'STREAMS', streamKey, '>')
    if not nextEntries or #nextEntries == 0 then
      return {'NEXT_NONE', jobId}
    end
    local streamData = nextEntries[1]
    local entries = streamData[2]
    if not entries or #entries == 0 then
      return {'NEXT_NONE', jobId}
    end
    local nextEntry = entries[1]
    nextEntryId = nextEntry[1]
    local nextFields = nextEntry[2]
    nextJobId = nil
    for i = 1, #nextFields, 2 do
      if nextFields[i] == 'jobId' then
        nextJobId = nextFields[i + 1]
        break
      end
    end
    if not nextJobId then
      return {'NEXT_NONE', jobId}
    end
    nextJobKey = prefix .. 'job:' .. nextJobId
    local nextExists = redis.call('EXISTS', nextJobKey)
    if nextExists == 0 then
      return {'NEXT_NONE', jobId}
    end
    local revoked = redis.call('HGET', nextJobKey, 'revoked')
    if revoked == '1' then
      return {'NEXT_REVOKED', jobId, nextJobId, nextEntryId}
    end
    if checkExpired(nextJobKey, nextJobId, prefix, timestamp) then
      redis.call('XACK', streamKey, group, nextEntryId)
      redis.call('XDEL', streamKey, nextEntryId)
      nextJobId = nil
    else
      break
    end
  end
  if not nextJobId then
    return {'NEXT_NONE', jobId}
  end

  -- Phase 3: Activate next job (same as moveToActive)
  local nextGroupKey = redis.call('HGET', nextJobKey, 'groupKey')
  if nextGroupKey and nextGroupKey ~= '' then
    local nextGroupHashKey = prefix .. 'group:' .. nextGroupKey
    -- Load all group fields in one call
    local nGrpFields = redis.call('HGETALL', nextGroupHashKey)
    local nGrp = {}
    for nf = 1, #nGrpFields, 2 do nGrp[nGrpFields[nf]] = nGrpFields[nf + 1] end
    local nextMaxConc = tonumber(nGrp.maxConcurrency) or 0
    local nextActive = tonumber(nGrp.active) or 0
    -- Concurrency gate first (avoids burning rate/token slots on parked jobs)
    if nextMaxConc > 0 and nextActive >= nextMaxConc then
      redis.call('XACK', streamKey, group, nextEntryId)
      redis.call('XDEL', streamKey, nextEntryId)
      local nextWaitListKey = prefix .. 'groupq:' .. nextGroupKey
      redis.call('RPUSH', nextWaitListKey, nextJobId)
      redis.call('HSET', nextJobKey, 'state', 'group-waiting')
      return {'NEXT_NONE', jobId}
    end
    -- Token bucket gate (read-only)
    local nextTbCapacity = tonumber(nGrp.tbCapacity) or 0
    local nextTbBlocked = false
    local nextTbDelay = 0
    local nextTbTokens = 0
    local nextJobCostVal = 0
    if nextTbCapacity > 0 then
      nextTbTokens = tbRefill(nextGroupHashKey, nGrp, tonumber(timestamp))
      nextJobCostVal = tonumber(redis.call('HGET', nextJobKey, 'cost')) or 1000
      -- DLQ guard: cost > capacity
      if nextJobCostVal > nextTbCapacity then
        local nextProcessedOn = tonumber(redis.call('HGET', nextJobKey, 'processedOn')) or tonumber(timestamp)
        redis.call('XACK', streamKey, group, nextEntryId)
        redis.call('XDEL', streamKey, nextEntryId)
        redis.call('ZADD', prefix .. 'failed', tonumber(timestamp), nextJobId)
        redis.call('HSET', nextJobKey,
          'state', 'failed',
          'failedReason', 'cost exceeds token bucket capacity',
          'finishedOn', tostring(timestamp))
        emitEvent(prefix .. 'events', 'failed', nextJobId, {'failedReason', 'cost exceeds token bucket capacity'})
        recordMetrics(metricsKey, tonumber(timestamp), tonumber(timestamp) - nextProcessedOn)
        return {'NEXT_NONE', jobId}
      end
      if nextTbTokens < nextJobCostVal then
        nextTbBlocked = true
        local nextTbRefillRateVal = math.max(tonumber(nGrp.tbRefillRate) or 0, 1)
        nextTbDelay = math.ceil((nextJobCostVal - nextTbTokens) * 1000 / nextTbRefillRateVal)
      end
    end
    -- Sliding window gate (read-only)
    local nextRateMax = tonumber(nGrp.rateMax) or 0
    local nextRlBlocked = false
    local nextRlDelay = 0
    if nextRateMax > 0 then
      local nextRateDuration = tonumber(nGrp.rateDuration) or 0
      local nextRateWindowStart = tonumber(nGrp.rateWindowStart) or 0
      local nextRateCount = tonumber(nGrp.rateCount) or 0
      if nextRateDuration > 0 and timestamp - nextRateWindowStart < nextRateDuration and nextRateCount >= nextRateMax then
        nextRlBlocked = true
        nextRlDelay = (nextRateWindowStart + nextRateDuration) - timestamp
      end
    end
    -- If ANY gate blocked: park + register
    if nextTbBlocked or nextRlBlocked then
      redis.call('XACK', streamKey, group, nextEntryId)
      redis.call('XDEL', streamKey, nextEntryId)
      local nextWaitListKey = prefix .. 'groupq:' .. nextGroupKey
      redis.call('RPUSH', nextWaitListKey, nextJobId)
      redis.call('HSET', nextJobKey, 'state', 'group-waiting')
      local nextMaxDelay = math.max(nextTbDelay, nextRlDelay)
      local rateLimitedKey = prefix .. 'ratelimited'
      redis.call('ZADD', rateLimitedKey, tonumber(timestamp) + nextMaxDelay, nextGroupKey)
      return {'NEXT_NONE', jobId}
    end
    -- All gates passed: mutate state
    if nextTbCapacity > 0 then
      redis.call('HINCRBY', nextGroupHashKey, 'tbTokens', -nextJobCostVal)
    end
    if nextRateMax > 0 then
      local nextRateDuration = tonumber(nGrp.rateDuration) or 0
      if nextRateDuration > 0 then
        local nextRateWindowStart = tonumber(nGrp.rateWindowStart) or 0
        if timestamp - nextRateWindowStart >= nextRateDuration then
          redis.call('HSET', nextGroupHashKey, 'rateWindowStart', tostring(timestamp), 'rateCount', '1')
        else
          redis.call('HINCRBY', nextGroupHashKey, 'rateCount', 1)
        end
      end
    end
    redis.call('HINCRBY', nextGroupHashKey, 'active', 1)
  end
  redis.call('HSET', nextJobKey, 'state', 'active', 'processedOn', tostring(timestamp), 'lastActive', tostring(timestamp))
  local nextHash = redis.call('HGETALL', nextJobKey)
  local out = {'NEXT_HASH', jobId, nextJobId, nextEntryId}
  for i = 1, #nextHash do
    out[#out + 1] = nextHash[i]
  end
  return out
end)

redis.register_function('glidemq_fail', function(keys, args)
  local streamKey = keys[1]
  local failedKey = keys[2]
  local scheduledKey = keys[3]
  local eventsKey = keys[4]
  local jobKey = keys[5]
  local metricsKey = keys[6]
  local jobId = args[1]
  local entryId = args[2]
  local failedReason = args[3]
  local timestamp = tonumber(args[4])
  local maxAttempts = tonumber(args[5]) or 0
  local backoffDelay = tonumber(args[6]) or 0
  local group = args[7]
  local removeMode = args[8] or '0'
  local removeCount = tonumber(args[9]) or 0
  local removeAge = tonumber(args[10]) or 0
  local broadcastMode = args[11] or '0'
  local processedOn = tonumber(redis.call('HGET', jobKey, 'processedOn')) or timestamp
  redis.call('XACK', streamKey, group, entryId)
  if broadcastMode ~= '1' then
    redis.call('XDEL', streamKey, entryId)
  end
  local attemptsMade
  if broadcastMode == '1' then
    local subKey = jobKey .. ':sub:' .. group
    attemptsMade = redis.call('HINCRBY', subKey, 'a', 1)
    redis.call('EXPIRE', subKey, 86400)
  else
    attemptsMade = redis.call('HINCRBY', jobKey, 'attemptsMade', 1)
  end
  if maxAttempts > 0 and attemptsMade < maxAttempts then
    local retryAt = timestamp + backoffDelay
    local priority = tonumber(redis.call('HGET', jobKey, 'priority')) or 0
    local score = priority * PRIORITY_SHIFT + retryAt
    redis.call('ZADD', scheduledKey, score, jobId)
    redis.call('HSET', jobKey,
      'state', 'delayed',
      'failedReason', failedReason,
      'processedOn', tostring(timestamp)
    )
    releaseGroupSlotAndPromote(jobKey, jobId, timestamp)
    emitEvent(eventsKey, 'retrying', jobId, {
      'failedReason', failedReason,
      'attemptsMade', tostring(attemptsMade),
      'delay', tostring(backoffDelay)
    })
    return 'retrying'
  else
    redis.call('ZADD', failedKey, timestamp, jobId)
    redis.call('HSET', jobKey,
      'state', 'failed',
      'failedReason', failedReason,
      'finishedOn', tostring(timestamp),
      'processedOn', tostring(timestamp)
    )
    markOrderingDone(jobKey, jobId)
    releaseGroupSlotAndPromote(jobKey, jobId, timestamp)
    emitEvent(eventsKey, 'failed', jobId, {'failedReason', failedReason})
    recordMetrics(metricsKey, timestamp, timestamp - processedOn)
    local prefix = string.sub(jobKey, 1, #jobKey - #('job:' .. jobId))
    -- In broadcast mode, skip job hash deletion: the job must persist for all subscriptions
    if broadcastMode ~= '1' then
      if removeMode == 'true' then
        redis.call('ZREM', failedKey, jobId)
        redis.call('DEL', jobKey)
      elseif removeMode == 'count' and removeCount > 0 then
        local total = redis.call('ZCARD', failedKey)
        if total > removeCount then
          local excess = redis.call('ZRANGE', failedKey, 0, math.min(total - removeCount, 1000) - 1)
          if #excess > 0 then removeExcessJobs(failedKey, prefix, excess) end
        end
      elseif removeMode == 'age_count' then
        if removeAge > 0 then
          local cutoff = timestamp - (removeAge * 1000)
          local old = redis.call('ZRANGEBYSCORE', failedKey, '0', string.format('%.0f', cutoff), 'LIMIT', 0, 1000)
          if #old > 0 then removeExcessJobs(failedKey, prefix, old) end
        end
        if removeCount > 0 then
          local total = redis.call('ZCARD', failedKey)
          if total > removeCount then
            local excess = redis.call('ZRANGE', failedKey, 0, math.min(total - removeCount, 1000) - 1)
            if #excess > 0 then removeExcessJobs(failedKey, prefix, excess) end
          end
        end
      end
    end
    return 'failed'
  end
end)

redis.register_function('glidemq_reclaimStalled', function(keys, args)
  local streamKey = keys[1]
  local eventsKey = keys[2]
  local group = args[1]
  local consumer = args[2]
  local minIdleMs = tonumber(args[3])
  local maxStalledCount = tonumber(args[4]) or 1
  local timestamp = tonumber(args[5])
  local failedKey = args[6]
  local broadcastMode = args[7] or '0'
  local result = redis.call('XAUTOCLAIM', streamKey, group, consumer, minIdleMs, '0-0')
  local entries = result[2]
  if not entries or #entries == 0 then
    return 0
  end
  local prefix = string.sub(streamKey, 1, #streamKey - 6)
  local count = 0
  for i = 1, #entries do
    local entry = entries[i]
    local entryId = entry[1]
    local fields = entry[2]
    local jobId = nil
    if type(fields) == 'table' then
      for j = 1, #fields, 2 do
        if fields[j] == 'jobId' then
          jobId = fields[j + 1]
          break
        end
      end
    end
    if jobId then
      local jobKey = prefix .. 'job:' .. jobId
      if checkExpired(jobKey, jobId, prefix, timestamp) then
        redis.call('XACK', streamKey, group, entryId)
        if broadcastMode ~= '1' then
          redis.call('XDEL', streamKey, entryId)
        end
        count = count + 1
      else
      local lastActive = tonumber(redis.call('HGET', jobKey, 'lastActive'))
      if lastActive and (timestamp - lastActive) < minIdleMs then
        count = count + 1
      else
      local stalledCount = redis.call('HINCRBY', jobKey, 'stalledCount', 1)
      if stalledCount > maxStalledCount then
        local metricsKey = prefix .. 'metrics:failed'
        local processedOn = tonumber(redis.call('HGET', jobKey, 'processedOn')) or timestamp
        redis.call('XACK', streamKey, group, entryId)
        if broadcastMode ~= '1' then
          redis.call('XDEL', streamKey, entryId)
        end
        redis.call('ZADD', failedKey, timestamp, jobId)
        redis.call('HSET', jobKey,
          'state', 'failed',
          'failedReason', 'job stalled more than maxStalledCount',
          'finishedOn', tostring(timestamp)
        )
        markOrderingDone(jobKey, jobId)
        releaseGroupSlotAndPromote(jobKey, jobId, timestamp)
        emitEvent(eventsKey, 'failed', jobId, {
          'failedReason', 'job stalled more than maxStalledCount'
        })
        recordMetrics(metricsKey, timestamp, timestamp - processedOn)
      else
        redis.call('HSET', jobKey, 'state', 'active')
        emitEvent(eventsKey, 'stalled', jobId, nil)
      end
      count = count + 1
      end
      end
    end
  end
  return count
end)

redis.register_function('glidemq_pause', function(keys, args)
  local metaKey = keys[1]
  local eventsKey = keys[2]
  redis.call('HSET', metaKey, 'paused', '1')
  emitEvent(eventsKey, 'paused', '0', nil)
  return 1
end)

redis.register_function('glidemq_resume', function(keys, args)
  local metaKey = keys[1]
  local eventsKey = keys[2]
  redis.call('HSET', metaKey, 'paused', '0')
  emitEvent(eventsKey, 'resumed', '0', nil)
  return 1
end)

redis.register_function('glidemq_dedup', function(keys, args)
  local dedupKey = keys[1]
  local idKey = keys[2]
  local streamKey = keys[3]
  local scheduledKey = keys[4]
  local eventsKey = keys[5]
  local dedupId = args[1]
  local ttlMs = tonumber(args[2]) or 0
  local mode = args[3]
  local jobName = args[4]
  local jobData = args[5]
  local jobOpts = args[6]
  local timestamp = tonumber(args[7])
  local delay = tonumber(args[8]) or 0
  local priority = tonumber(args[9]) or 0
  local parentId = args[10] or ''
  local maxAttempts = tonumber(args[11]) or 0
  local orderingKey = args[12] or ''
  local groupConcurrency = tonumber(args[13]) or 0
  local groupRateMax = tonumber(args[14]) or 0
  local groupRateDuration = tonumber(args[15]) or 0
  local tbCapacity = tonumber(args[16]) or 0
  local tbRefillRate = tonumber(args[17]) or 0
  local jobCost = tonumber(args[18]) or 0
  local ttl = tonumber(args[19]) or 0
  local customJobId = args[20] or ''
  local parentQueue = args[21] or ''
  local prefix = string.sub(idKey, 1, #idKey - 2)
  local existing = redis.call('HGET', dedupKey, dedupId)
  if mode == 'simple' then
    if existing then
      local sep = string.find(existing, ':')
      if sep then
        local existingJobId = string.sub(existing, 1, sep - 1)
        local jobKey = prefix .. 'job:' .. existingJobId
        local state = redis.call('HGET', jobKey, 'state')
        if state and state ~= 'completed' and state ~= 'failed' then
          return 'skipped'
        end
      end
    end
  elseif mode == 'throttle' then
    if existing and ttlMs > 0 then
      local sep = string.find(existing, ':')
      if sep then
        local storedTs = tonumber(string.sub(existing, sep + 1))
        if storedTs and (timestamp - storedTs) < ttlMs then
          return 'skipped'
        end
      end
    end
  elseif mode == 'debounce' then
    if existing then
      local sep = string.find(existing, ':')
      if sep then
        local existingJobId = string.sub(existing, 1, sep - 1)
        local jobKey = prefix .. 'job:' .. existingJobId
        local state = redis.call('HGET', jobKey, 'state')
        if state == 'delayed' or state == 'prioritized' then
          redis.call('ZREM', scheduledKey, existingJobId)
          markOrderingDone(jobKey, existingJobId)
          redis.call('DEL', jobKey)
          emitEvent(eventsKey, 'removed', existingJobId, nil)
        elseif state and state ~= 'completed' and state ~= 'failed' then
          return 'skipped'
        end
      end
    end
  end
  local jobIdStr
  local jobKey
  if customJobId ~= '' then
    jobKey = prefix .. 'job:' .. customJobId
    if redis.call('EXISTS', jobKey) == 1 then
      return 'duplicate'
    end
    jobIdStr = customJobId
  else
    local jobId = redis.call('INCR', idKey)
    jobIdStr = tostring(jobId)
    jobKey = prefix .. 'job:' .. jobIdStr
    local retries = 0
    while redis.call('EXISTS', jobKey) == 1 do
      retries = retries + 1
      if retries >= 1000 then return 'ERR:ID_EXHAUSTED' end
      jobId = redis.call('INCR', idKey)
      jobIdStr = tostring(jobId)
      jobKey = prefix .. 'job:' .. jobIdStr
    end
  end
  local useGroupConcurrency = (orderingKey ~= '' and (groupConcurrency > 1 or groupRateMax > 0 or tbCapacity > 0))
  local orderingSeq = 0
  if orderingKey ~= '' and not useGroupConcurrency then
    local orderingMetaKey = prefix .. 'ordering'
    orderingSeq = redis.call('HINCRBY', orderingMetaKey, orderingKey, 1)
  end
  if useGroupConcurrency then
    local groupHashKey = prefix .. 'group:' .. orderingKey
    local curMax = tonumber(redis.call('HGET', groupHashKey, 'maxConcurrency')) or 0
    if curMax ~= groupConcurrency then
      redis.call('HSET', groupHashKey, 'maxConcurrency', tostring(groupConcurrency))
    end
    if curMax == 0 and groupConcurrency <= 1 then
      redis.call('HSET', groupHashKey, 'maxConcurrency', '1')
    end
    if groupRateMax > 0 then
      local curRateMax = tonumber(redis.call('HGET', groupHashKey, 'rateMax')) or 0
      if curRateMax ~= groupRateMax then
        redis.call('HSET', groupHashKey, 'rateMax', tostring(groupRateMax))
      end
      local curRateDuration = tonumber(redis.call('HGET', groupHashKey, 'rateDuration')) or 0
      if curRateDuration ~= groupRateDuration then
        redis.call('HSET', groupHashKey, 'rateDuration', tostring(groupRateDuration))
      end
    else
      local oldRateMax = tonumber(redis.call('HGET', groupHashKey, 'rateMax')) or 0
      if oldRateMax > 0 then
        redis.call('HDEL', groupHashKey, 'rateMax', 'rateDuration', 'rateWindowStart', 'rateCount')
      end
    end
    -- Upsert token bucket fields on group hash
    if tbCapacity > 0 then
      local curTbCap = tonumber(redis.call('HGET', groupHashKey, 'tbCapacity')) or 0
      if curTbCap ~= tbCapacity then
        redis.call('HSET', groupHashKey, 'tbCapacity', tostring(tbCapacity))
      end
      local curTbRate = tonumber(redis.call('HGET', groupHashKey, 'tbRefillRate')) or 0
      if curTbRate ~= tbRefillRate then
        redis.call('HSET', groupHashKey, 'tbRefillRate', tostring(tbRefillRate))
      end
      -- Initialize tokens on first setup
      if curTbCap == 0 then
        redis.call('HSET', groupHashKey,
          'tbTokens', tostring(tbCapacity),
          'tbLastRefill', tostring(timestamp),
          'tbRefillRemainder', '0')
      end
      -- Validate cost <= capacity at enqueue
      -- Validate cost (explicit or default 1000 millitokens) against capacity
      local effectiveCost = (jobCost > 0) and jobCost or 1000
      if effectiveCost > tbCapacity then
        return 'ERR:COST_EXCEEDS_CAPACITY'
      end
    else
      -- Clear stale tb fields
      local oldTbCap = tonumber(redis.call('HGET', groupHashKey, 'tbCapacity')) or 0
      if oldTbCap > 0 then
        redis.call('HDEL', groupHashKey, 'tbCapacity', 'tbRefillRate', 'tbTokens', 'tbLastRefill', 'tbRefillRemainder')
      end
    end
  end
  local hashFields = {
    'id', jobIdStr,
    'name', jobName,
    'data', jobData,
    'opts', jobOpts,
    'timestamp', tostring(timestamp),
    'attemptsMade', '0',
    'delay', tostring(delay),
    'priority', tostring(priority),
    'maxAttempts', tostring(maxAttempts)
  }
  if useGroupConcurrency then
    hashFields[#hashFields + 1] = 'groupKey'
    hashFields[#hashFields + 1] = orderingKey
  elseif orderingKey ~= '' then
    hashFields[#hashFields + 1] = 'orderingKey'
    hashFields[#hashFields + 1] = orderingKey
    hashFields[#hashFields + 1] = 'orderingSeq'
    hashFields[#hashFields + 1] = tostring(orderingSeq)
  end
  if jobCost > 0 then
    hashFields[#hashFields + 1] = 'cost'
    hashFields[#hashFields + 1] = tostring(jobCost)
  end
  if ttl > 0 then
    hashFields[#hashFields + 1] = 'expireAt'
    hashFields[#hashFields + 1] = tostring(timestamp + ttl)
  end
  if parentId ~= '' then
    hashFields[#hashFields + 1] = 'parentId'
    hashFields[#hashFields + 1] = parentId
    if parentQueue ~= '' then
      hashFields[#hashFields + 1] = 'parentQueue'
      hashFields[#hashFields + 1] = parentQueue
    end
  end
  if delay > 0 or priority > 0 then
    hashFields[#hashFields + 1] = 'state'
    hashFields[#hashFields + 1] = delay > 0 and 'delayed' or 'prioritized'
  else
    hashFields[#hashFields + 1] = 'state'
    hashFields[#hashFields + 1] = 'waiting'
  end
  redis.call('HSET', jobKey, unpack(hashFields))
  -- Register child in parent's deps set when parentDepsKey is provided (keys[6])
  if parentId ~= '' and parentQueue ~= '' and #keys >= 6 then
    local parentDepsKey = keys[6]
    local queuePrefix = string.sub(prefix, 1, #prefix - 1)
    local depsMember = queuePrefix .. ':' .. jobIdStr
    redis.call('SADD', parentDepsKey, depsMember)
  end
  if delay > 0 then
    local score = priority * PRIORITY_SHIFT + (timestamp + delay)
    redis.call('ZADD', scheduledKey, score, jobIdStr)
  elseif priority > 0 then
    local score = priority * PRIORITY_SHIFT
    redis.call('ZADD', scheduledKey, score, jobIdStr)
  else
    redis.call('XADD', streamKey, '*', 'jobId', jobIdStr)
  end
  redis.call('HSET', dedupKey, dedupId, jobIdStr .. ':' .. tostring(timestamp))
  emitEvent(eventsKey, 'added', jobIdStr, {'name', jobName})
  return jobIdStr
end)

redis.register_function('glidemq_rateLimit', function(keys, args)
  local rateKey = keys[1]
  local metaKey = keys[2]
  local maxPerWindow = tonumber(args[1])
  local windowDuration = tonumber(args[2])
  local now = tonumber(args[3])
  -- Fallback: read rate limit config from meta if not provided inline
  if maxPerWindow <= 0 then
    maxPerWindow = tonumber(redis.call('HGET', metaKey, 'rateLimitMax')) or 0
    windowDuration = tonumber(redis.call('HGET', metaKey, 'rateLimitDuration')) or 0
    if maxPerWindow <= 0 then return 0 end
  end
  local windowStart = tonumber(redis.call('HGET', rateKey, 'windowStart')) or 0
  local count = tonumber(redis.call('HGET', rateKey, 'count')) or 0
  if now - windowStart >= windowDuration then
    redis.call('HSET', rateKey, 'windowStart', tostring(now), 'count', '1')
    return 0
  end
  if count >= maxPerWindow then
    local delayMs = windowDuration - (now - windowStart)
    return delayMs
  end
  redis.call('HSET', rateKey, 'count', tostring(count + 1))
  return 0
end)

redis.register_function('glidemq_promoteRateLimited', function(keys, args)
  local rateLimitedKey = keys[1]
  local streamKey = keys[2]
  local now = tonumber(args[1])
  -- Derive prefix from the server-validated key instead of caller-supplied arg
  local prefix = string.sub(rateLimitedKey, 1, #rateLimitedKey - #'ratelimited')
  local expired = redis.call('ZRANGEBYSCORE', rateLimitedKey, '0', string.format('%.0f', now), 'LIMIT', 0, 100)
  if not expired or #expired == 0 then return 0 end
  local promoted = 0
  for i = 1, #expired do
    local gk = expired[i]
    redis.call('ZREM', rateLimitedKey, gk)
    local groupHashKey = prefix .. 'group:' .. gk
    local waitListKey = prefix .. 'groupq:' .. gk
    -- Load all group fields in one call for rate limit + token bucket checks
    local prGrpFields = redis.call('HGETALL', groupHashKey)
    local prGrp = {}
    for pf = 1, #prGrpFields, 2 do prGrp[prGrpFields[pf]] = prGrpFields[pf + 1] end
    local rateMax = tonumber(prGrp.rateMax) or 0
    local maxConc = tonumber(prGrp.maxConcurrency) or 0
    local active = tonumber(prGrp.active) or 0
    -- Token bucket pre-check: peek head job cost before promoting
    local prTbCap = tonumber(prGrp.tbCapacity) or 0
    local tbCheckPassed = true
    if prTbCap > 0 then
      local prTbTokens = tbRefill(groupHashKey, prGrp, now)
      local headJobId = redis.call('LINDEX', waitListKey, 0)
      if headJobId then
        local headJobKey = prefix .. 'job:' .. headJobId
        -- Tombstone guard
        if redis.call('EXISTS', headJobKey) == 0 then
          redis.call('LPOP', waitListKey)
          tbCheckPassed = false
        end
        if tbCheckPassed then
          local headCost = tonumber(redis.call('HGET', headJobKey, 'cost')) or 1000
          -- DLQ guard: cost > capacity
          if headCost > prTbCap then
            redis.call('LPOP', waitListKey)
            redis.call('ZADD', prefix .. 'failed', now, headJobId)
            redis.call('HSET', headJobKey,
              'state', 'failed',
              'failedReason', 'cost exceeds token bucket capacity',
              'finishedOn', tostring(now))
            emitEvent(prefix .. 'events', 'failed', headJobId, {'failedReason', 'cost exceeds token bucket capacity'})
            tbCheckPassed = false
          end
          if tbCheckPassed and prTbTokens < headCost then
            -- Not enough tokens: re-register with calculated delay
            local prTbRate = math.max(tonumber(prGrp.tbRefillRate) or 0, 1)
            local prTbDelay = math.ceil((headCost - prTbTokens) * 1000 / prTbRate)
            redis.call('ZADD', rateLimitedKey, now + prTbDelay, gk)
            tbCheckPassed = false
          end
        end
      end
    end
    if tbCheckPassed then
      -- Promote up to min(rateMax, available concurrency) jobs.
      -- Do NOT touch rateCount/rateWindowStart here - moveToActive handles
      -- window reset and counting when the worker picks up the promoted jobs.
      local canPromote = 1000
      if rateMax > 0 then
        canPromote = math.min(canPromote, rateMax)
      end
      if maxConc > 0 then
        canPromote = math.min(canPromote, math.max(0, maxConc - active))
      end
      for j = 1, canPromote do
        local nextJobId = redis.call('LPOP', waitListKey)
        if not nextJobId then break end
        local nextJobKey = prefix .. 'job:' .. nextJobId
        if not checkExpired(nextJobKey, nextJobId, prefix, now) then
          redis.call('XADD', streamKey, '*', 'jobId', nextJobId)
          redis.call('HSET', nextJobKey, 'state', 'waiting')
          promoted = promoted + 1
        end
      end
    end
  end
  return promoted
end)

redis.register_function('glidemq_checkConcurrency', function(keys, args)
  local metaKey = keys[1]
  local streamKey = keys[2]
  local group = args[1]
  local gc = tonumber(redis.call('HGET', metaKey, 'globalConcurrency')) or 0
  if gc <= 0 then
    return -1
  end
  local pending = redis.call('XPENDING', streamKey, group)
  local pendingCount = tonumber(pending[1]) or 0
  local remaining = gc - pendingCount
  if remaining <= 0 then
    return 0
  end
  return remaining
end)

redis.register_function('glidemq_moveToActive', function(keys, args)
  local jobKey = keys[1]
  local streamKey = keys[2] or ''
  local timestamp = args[1]
  local entryId = args[2] or ''
  local group = args[3] or ''
  local jobId = args[4] or ''
  local ts = tonumber(timestamp) or 0
  local timestampStr = tostring(ts)
  local fields = redis.call('HGETALL', jobKey)
  if not fields or #fields == 0 then
    return ''
  end
  local revoked = ''
  local expireAt = 0
  local curState = ''
  local orderingKey = ''
  local orderingSeq = ''
  local groupKey = ''
  local costVal = ''
  local stateValueIndex = nil
  local processedOnValueIndex = nil
  local lastActiveValueIndex = nil
  for f = 1, #fields, 2 do
    local field = fields[f]
    local value = fields[f + 1]
    if field == 'revoked' then
      revoked = value
    elseif field == 'expireAt' then
      expireAt = tonumber(value) or 0
    elseif field == 'state' then
      curState = value
      stateValueIndex = f + 1
    elseif field == 'orderingKey' then
      orderingKey = value
    elseif field == 'orderingSeq' then
      orderingSeq = value
    elseif field == 'groupKey' then
      groupKey = value
    elseif field == 'cost' then
      costVal = value
    elseif field == 'processedOn' then
      processedOnValueIndex = f + 1
    elseif field == 'lastActive' then
      lastActiveValueIndex = f + 1
    end
  end
  if revoked == '1' then
    return 'REVOKED'
  end
  local prefix = string.sub(jobKey, 1, #jobKey - #('job:' .. jobId))
  if expireAt > 0 and ts > expireAt then
    expireJob(jobKey, jobId, prefix, ts, curState, orderingKey, orderingSeq, groupKey)
    if streamKey ~= '' and entryId ~= '' and group ~= '' then
      redis.call('XACK', streamKey, group, entryId)
      redis.call('XDEL', streamKey, entryId)
    end
    return 'EXPIRED'
  end
  if groupKey and groupKey ~= '' then
    local groupHashKey = prefix .. 'group:' .. groupKey
    -- Load all group fields in one call
    local grpFields = redis.call('HGETALL', groupHashKey)
    local grp = {}
    for f = 1, #grpFields, 2 do grp[grpFields[f]] = grpFields[f + 1] end
    local maxConc = tonumber(grp.maxConcurrency) or 0
    local active = tonumber(grp.active) or 0
    -- Concurrency gate (checked first to avoid burning rate/token slots on parked jobs)
    if maxConc > 0 and active >= maxConc then
      if streamKey ~= '' and entryId ~= '' and group ~= '' then
        redis.call('XACK', streamKey, group, entryId)
        redis.call('XDEL', streamKey, entryId)
      end
      local waitListKey = prefix .. 'groupq:' .. groupKey
      redis.call('RPUSH', waitListKey, jobId)
      redis.call('HSET', jobKey, 'state', 'group-waiting')
      return 'GROUP_FULL'
    end
    -- Token bucket gate (read-only)
    local tbCapacity = tonumber(grp.tbCapacity) or 0
    local tbBlocked = false
    local tbDelay = 0
    local tbTokens = 0
    local jobCostVal = 0
    if tbCapacity > 0 then
      tbTokens = tbRefill(groupHashKey, grp, ts)
      jobCostVal = tonumber(costVal) or 1000
      -- DLQ guard: cost > capacity
      if jobCostVal > tbCapacity then
        if streamKey ~= '' and entryId ~= '' and group ~= '' then
          redis.call('XACK', streamKey, group, entryId)
          redis.call('XDEL', streamKey, entryId)
        end
        redis.call('ZADD', prefix .. 'failed', ts, jobId)
        redis.call('HSET', jobKey,
          'state', 'failed',
          'failedReason', 'cost exceeds token bucket capacity',
          'finishedOn', timestampStr)
        emitEvent(prefix .. 'events', 'failed', jobId, {'failedReason', 'cost exceeds token bucket capacity'})
        return 'ERR:COST_EXCEEDS_CAPACITY'
      end
      if tbTokens < jobCostVal then
        tbBlocked = true
        local tbRefillRateVal = tonumber(grp.tbRefillRate) or 0
        if tbRefillRateVal <= 0 then tbRefillRateVal = 1 end
        tbDelay = math.ceil((jobCostVal - tbTokens) * 1000 / tbRefillRateVal)
      end
    end
    -- Sliding window gate (read-only)
    local rateMax = tonumber(grp.rateMax) or 0
    local rlBlocked = false
    local rlDelay = 0
    if rateMax > 0 then
      local rateDuration = tonumber(grp.rateDuration) or 0
      local rateWindowStart = tonumber(grp.rateWindowStart) or 0
      local rateCount = tonumber(grp.rateCount) or 0
      local now = ts
      if rateDuration > 0 and now - rateWindowStart < rateDuration and rateCount >= rateMax then
        rlBlocked = true
        rlDelay = (rateWindowStart + rateDuration) - now
      end
    end
    -- If ANY gate blocked: park + register
    if tbBlocked or rlBlocked then
      if streamKey ~= '' and entryId ~= '' and group ~= '' then
        redis.call('XACK', streamKey, group, entryId)
        redis.call('XDEL', streamKey, entryId)
      end
      local waitListKey = prefix .. 'groupq:' .. groupKey
      redis.call('RPUSH', waitListKey, jobId)
      redis.call('HSET', jobKey, 'state', 'group-waiting')
      local maxDelay = math.max(tbDelay, rlDelay)
      local rateLimitedKey = prefix .. 'ratelimited'
      redis.call('ZADD', rateLimitedKey, ts + maxDelay, groupKey)
      if tbBlocked then return 'GROUP_TOKEN_LIMITED' end
      return 'GROUP_RATE_LIMITED'
    end
    -- All gates passed: mutate state
    if tbCapacity > 0 then
      redis.call('HINCRBY', groupHashKey, 'tbTokens', -jobCostVal)
    end
    if rateMax > 0 then
      local rateDuration = tonumber(grp.rateDuration) or 0
      if rateDuration > 0 then
        local rateWindowStart = tonumber(grp.rateWindowStart) or 0
        local now = ts
        if now - rateWindowStart >= rateDuration then
          redis.call('HSET', groupHashKey, 'rateWindowStart', tostring(now), 'rateCount', '1')
        else
          redis.call('HINCRBY', groupHashKey, 'rateCount', 1)
        end
      end
    end
    redis.call('HINCRBY', groupHashKey, 'active', 1)
  end
  redis.call('HSET', jobKey, 'state', 'active', 'processedOn', timestampStr, 'lastActive', timestampStr)
  if stateValueIndex ~= nil then
    fields[stateValueIndex] = 'active'
  else
    fields[#fields + 1] = 'state'
    fields[#fields + 1] = 'active'
  end
  if processedOnValueIndex ~= nil then
    fields[processedOnValueIndex] = timestampStr
  else
    fields[#fields + 1] = 'processedOn'
    fields[#fields + 1] = timestampStr
  end
  if lastActiveValueIndex ~= nil then
    fields[lastActiveValueIndex] = timestampStr
  else
    fields[#fields + 1] = 'lastActive'
    fields[#fields + 1] = timestampStr
  end
  return fields
end)

redis.register_function('glidemq_deferActive', function(keys, args)
  local streamKey = keys[1]
  local jobKey = keys[2]
  local jobId = args[1]
  local entryId = args[2]
  local group = args[3]
  local exists = redis.call('EXISTS', jobKey)
  redis.call('XACK', streamKey, group, entryId)
  redis.call('XDEL', streamKey, entryId)
  if exists == 0 then
    return 0
  end
  redis.call('XADD', streamKey, '*', 'jobId', jobId)
  redis.call('HSET', jobKey, 'state', 'waiting')
  return 1
end)

redis.register_function('glidemq_addFlow', function(keys, args)
  local parentIdKey = keys[1]
  local parentStreamKey = keys[2]
  local parentScheduledKey = keys[3]
  local parentEventsKey = keys[4]
  local parentName = args[1]
  local parentData = args[2]
  local parentOpts = args[3]
  local timestamp = tonumber(args[4])
  local parentDelay = tonumber(args[5]) or 0
  local parentPriority = tonumber(args[6]) or 0
  local parentMaxAttempts = tonumber(args[7]) or 0
  local numChildren = tonumber(args[8])
  local parentCustomId = args[9] or ''
  local parentPrefix = string.sub(parentIdKey, 1, #parentIdKey - 2)
  local parentJobIdStr
  local parentJobKey
  if parentCustomId ~= '' then
    parentJobKey = parentPrefix .. 'job:' .. parentCustomId
    if redis.call('EXISTS', parentJobKey) == 1 then
      return cjson.encode({'duplicate'})
    end
    parentJobIdStr = parentCustomId
  else
    local parentJobId = redis.call('INCR', parentIdKey)
    parentJobIdStr = tostring(parentJobId)
    parentJobKey = parentPrefix .. 'job:' .. parentJobIdStr
    local retries = 0
    while redis.call('EXISTS', parentJobKey) == 1 do
      retries = retries + 1
      if retries >= 1000 then return cjson.encode({'ERR:ID_EXHAUSTED'}) end
      parentJobId = redis.call('INCR', parentIdKey)
      parentJobIdStr = tostring(parentJobId)
      parentJobKey = parentPrefix .. 'job:' .. parentJobIdStr
    end
  end
  -- Pre-validate all children's custom IDs for duplicates before any writes
  local seenChildKeys = {}
  for i = 1, numChildren do
    local base = 9 + (i - 1) * 9
    local preChildCustomId = args[base + 9] or ''
    if preChildCustomId ~= '' then
      local ckBase = 4 + (i - 1) * 4
      local preChildIdKey = keys[ckBase + 1]
      local preChildPrefix = string.sub(preChildIdKey, 1, #preChildIdKey - 2)
      local preChildJobKey = preChildPrefix .. 'job:' .. preChildCustomId
      if preChildJobKey == parentJobKey or seenChildKeys[preChildJobKey] then
        return cjson.encode({'duplicate'})
      end
      seenChildKeys[preChildJobKey] = true
      if redis.call('EXISTS', preChildJobKey) == 1 then
        return cjson.encode({'duplicate'})
      end
    end
  end
  local depsKey = parentPrefix .. 'deps:' .. parentJobIdStr
  local parentOrderingKey = extractOrderingKeyFromOpts(parentOpts)
  local parentGroupConc = extractGroupConcurrencyFromOpts(parentOpts)
  local parentRateMax, parentRateDuration = extractGroupRateLimitFromOpts(parentOpts)
  local parentTbCapacity, parentTbRefillRate = extractTokenBucketFromOpts(parentOpts)
  local parentCost = extractCostFromOpts(parentOpts)
  local parentUseGroup = (parentOrderingKey ~= '' and (parentGroupConc > 1 or parentRateMax > 0 or parentTbCapacity > 0))
  local parentOrderingSeq = 0
  if parentOrderingKey ~= '' and not parentUseGroup then
    local parentOrderingMetaKey = parentPrefix .. 'ordering'
    parentOrderingSeq = redis.call('HINCRBY', parentOrderingMetaKey, parentOrderingKey, 1)
  end
  local parentHash = {
    'id', parentJobIdStr,
    'name', parentName,
    'data', parentData,
    'opts', parentOpts,
    'timestamp', tostring(timestamp),
    'attemptsMade', '0',
    'delay', tostring(parentDelay),
    'priority', tostring(parentPriority),
    'maxAttempts', tostring(parentMaxAttempts),
    'state', 'waiting-children'
  }
  if parentUseGroup then
    parentHash[#parentHash + 1] = 'groupKey'
    parentHash[#parentHash + 1] = parentOrderingKey
    local groupHashKey = parentPrefix .. 'group:' .. parentOrderingKey
    redis.call('HSET', groupHashKey, 'maxConcurrency', tostring(parentGroupConc > 1 and parentGroupConc or 1))
    redis.call('HSETNX', groupHashKey, 'active', '0')
    if parentRateMax > 0 then
      redis.call('HSET', groupHashKey, 'rateMax', tostring(parentRateMax))
      redis.call('HSET', groupHashKey, 'rateDuration', tostring(parentRateDuration))
    end
    if parentTbCapacity > 0 then
      if parentCost > 0 and parentCost > parentTbCapacity then
        return 'ERR:COST_EXCEEDS_CAPACITY'
      end
      redis.call('HSET', groupHashKey, 'tbCapacity', tostring(parentTbCapacity), 'tbRefillRate', tostring(parentTbRefillRate))
      redis.call('HSETNX', groupHashKey, 'tbTokens', tostring(parentTbCapacity))
      redis.call('HSETNX', groupHashKey, 'tbLastRefill', tostring(timestamp))
      redis.call('HSETNX', groupHashKey, 'tbRefillRemainder', '0')
    end
  elseif parentOrderingKey ~= '' then
    parentHash[#parentHash + 1] = 'orderingKey'
    parentHash[#parentHash + 1] = parentOrderingKey
    parentHash[#parentHash + 1] = 'orderingSeq'
    parentHash[#parentHash + 1] = tostring(parentOrderingSeq)
  end
  if parentCost > 0 then
    parentHash[#parentHash + 1] = 'cost'
    parentHash[#parentHash + 1] = tostring(parentCost)
  end
  local parentTtl = extractTtlFromOpts(parentOpts)
  if parentTtl > 0 then
    parentHash[#parentHash + 1] = 'expireAt'
    parentHash[#parentHash + 1] = tostring(timestamp + parentTtl)
  end
  redis.call('HSET', parentJobKey, unpack(parentHash))
  -- Pre-validate all children's cost vs capacity before any child writes
  local childArgOffset = 9
  local childKeyOffset = 4
  for i = 1, numChildren do
    local base = childArgOffset + (i - 1) * 9
    local preChildOpts = args[base + 3]
    local preChildTbCap, _ = extractTokenBucketFromOpts(preChildOpts)
    if preChildTbCap > 0 then
      local preChildCost = extractCostFromOpts(preChildOpts)
      local preEffective = (preChildCost > 0) and preChildCost or 1000
      if preEffective > preChildTbCap then
        return 'ERR:COST_EXCEEDS_CAPACITY'
      end
    end
  end
  local childIds = {}
  for i = 1, numChildren do
    local base = childArgOffset + (i - 1) * 9
    local childName = args[base + 1]
    local childData = args[base + 2]
    local childOpts = args[base + 3]
    local childDelay = tonumber(args[base + 4]) or 0
    local childPriority = tonumber(args[base + 5]) or 0
    local childMaxAttempts = tonumber(args[base + 6]) or 0
    local childQueuePrefix = args[base + 7]
    local childParentQueue = args[base + 8]
    local childCustomId = args[base + 9] or ''
    local ckBase = childKeyOffset + (i - 1) * 4
    local childIdKey = keys[ckBase + 1]
    local childStreamKey = keys[ckBase + 2]
    local childScheduledKey = keys[ckBase + 3]
    local childEventsKey = keys[ckBase + 4]
    local childPrefix = string.sub(childIdKey, 1, #childIdKey - 2)
    local childJobIdStr
    local childJobKey
    if childCustomId ~= '' then
      childJobKey = childPrefix .. 'job:' .. childCustomId
      childJobIdStr = childCustomId
    else
      local childJobId = redis.call('INCR', childIdKey)
      childJobIdStr = tostring(childJobId)
      childJobKey = childPrefix .. 'job:' .. childJobIdStr
      local cRetries = 0
      while redis.call('EXISTS', childJobKey) == 1 do
        cRetries = cRetries + 1
        if cRetries >= 1000 then return cjson.encode({'ERR:ID_EXHAUSTED'}) end
        childJobId = redis.call('INCR', childIdKey)
        childJobIdStr = tostring(childJobId)
        childJobKey = childPrefix .. 'job:' .. childJobIdStr
      end
    end
    local childOrderingKey = extractOrderingKeyFromOpts(childOpts)
    local childGroupConc = extractGroupConcurrencyFromOpts(childOpts)
    local childRateMax, childRateDuration = extractGroupRateLimitFromOpts(childOpts)
    local childTbCapacity, childTbRefillRate = extractTokenBucketFromOpts(childOpts)
    local childCost = extractCostFromOpts(childOpts)
    local childUseGroup = (childOrderingKey ~= '' and (childGroupConc > 1 or childRateMax > 0 or childTbCapacity > 0))
    local childOrderingSeq = 0
    if childOrderingKey ~= '' and not childUseGroup then
      local childOrderingMetaKey = childPrefix .. 'ordering'
      childOrderingSeq = redis.call('HINCRBY', childOrderingMetaKey, childOrderingKey, 1)
    end
    local childHash = {
      'id', childJobIdStr,
      'name', childName,
      'data', childData,
      'opts', childOpts,
      'timestamp', tostring(timestamp),
      'attemptsMade', '0',
      'delay', tostring(childDelay),
      'priority', tostring(childPriority),
      'maxAttempts', tostring(childMaxAttempts),
      'parentId', parentJobIdStr,
      'parentQueue', childParentQueue
    }
    if childUseGroup then
      childHash[#childHash + 1] = 'groupKey'
      childHash[#childHash + 1] = childOrderingKey
      local childGroupHashKey = childPrefix .. 'group:' .. childOrderingKey
      redis.call('HSETNX', childGroupHashKey, 'maxConcurrency', tostring(childGroupConc > 1 and childGroupConc or 1))
      redis.call('HSETNX', childGroupHashKey, 'active', '0')
      if childRateMax > 0 then
        redis.call('HSET', childGroupHashKey, 'rateMax', tostring(childRateMax))
        redis.call('HSET', childGroupHashKey, 'rateDuration', tostring(childRateDuration))
      end
      if childTbCapacity > 0 then
        redis.call('HSET', childGroupHashKey, 'tbCapacity', tostring(childTbCapacity), 'tbRefillRate', tostring(childTbRefillRate))
        redis.call('HSETNX', childGroupHashKey, 'tbTokens', tostring(childTbCapacity))
        redis.call('HSETNX', childGroupHashKey, 'tbLastRefill', tostring(timestamp))
        redis.call('HSETNX', childGroupHashKey, 'tbRefillRemainder', '0')
      end
    elseif childOrderingKey ~= '' then
      childHash[#childHash + 1] = 'orderingKey'
      childHash[#childHash + 1] = childOrderingKey
      childHash[#childHash + 1] = 'orderingSeq'
      childHash[#childHash + 1] = tostring(childOrderingSeq)
    end
    if childCost > 0 then
      childHash[#childHash + 1] = 'cost'
      childHash[#childHash + 1] = tostring(childCost)
    end
    local childTtl = extractTtlFromOpts(childOpts)
    if childTtl > 0 then
      childHash[#childHash + 1] = 'expireAt'
      childHash[#childHash + 1] = tostring(timestamp + childTtl)
    end
    if childDelay > 0 or childPriority > 0 then
      childHash[#childHash + 1] = 'state'
      childHash[#childHash + 1] = childDelay > 0 and 'delayed' or 'prioritized'
    else
      childHash[#childHash + 1] = 'state'
      childHash[#childHash + 1] = 'waiting'
    end
    redis.call('HSET', childJobKey, unpack(childHash))
    local depsMember = childQueuePrefix .. ':' .. childJobIdStr
    redis.call('SADD', depsKey, depsMember)
    if childDelay > 0 then
      local score = childPriority * PRIORITY_SHIFT + (timestamp + childDelay)
      redis.call('ZADD', childScheduledKey, score, childJobIdStr)
    elseif childPriority > 0 then
      local score = childPriority * PRIORITY_SHIFT
      redis.call('ZADD', childScheduledKey, score, childJobIdStr)
    else
      redis.call('XADD', childStreamKey, '*', 'jobId', childJobIdStr)
    end
    emitEvent(childEventsKey, 'added', childJobIdStr, {'name', childName})
    childIds[#childIds + 1] = childJobIdStr
  end
  local extraDepsOffset = childArgOffset + numChildren * 9
  local numExtraDeps = tonumber(args[extraDepsOffset + 1]) or 0
  for i = 1, numExtraDeps do
    local extraMember = args[extraDepsOffset + 1 + i]
    redis.call('SADD', depsKey, extraMember)
  end
  emitEvent(parentEventsKey, 'added', parentJobIdStr, {'name', parentName})
  local result = {parentJobIdStr}
  for i = 1, #childIds do
    result[#result + 1] = childIds[i]
  end
  return cjson.encode(result)
end)

redis.register_function('glidemq_completeChild', function(keys, args)
  local depsKey = keys[1]
  local parentJobKey = keys[2]
  local parentStreamKey = keys[3]
  local parentEventsKey = keys[4]
  local depsMember = args[1]
  local parentId = args[2]
  local depMarker = 'depdone:' .. depsMember
  if redis.call('HSETNX', parentJobKey, depMarker, '1') == 0 then
    local doneCount = tonumber(redis.call('HGET', parentJobKey, 'depsCompleted')) or 0
    local totalDeps = redis.call('SCARD', depsKey)
    return totalDeps - doneCount
  end
  local doneCount = redis.call('HINCRBY', parentJobKey, 'depsCompleted', 1)
  local totalDeps = redis.call('SCARD', depsKey)
  local remaining = totalDeps - doneCount
  if remaining <= 0 then
    local parentState = redis.call('HGET', parentJobKey, 'state')
    if parentState == 'waiting-children' then
      redis.call('HSET', parentJobKey, 'state', 'waiting')
      redis.call('XADD', parentStreamKey, '*', 'jobId', parentId)
      emitEvent(parentEventsKey, 'active', parentId, nil)
    end
  end
  return remaining
end)

redis.register_function('glidemq_registerParent', function(keys, args)
  -- Register an additional parent for an existing child job (DAG multi-parent).
  -- Keys: [childJobKey, childParentsKey, parentDepsKey, parentJobKey, parentStreamKey, parentEventsKey]
  -- Args: [childJobId, parentId, parentQueue, depsMember]
  local childJobKey = keys[1]
  local childParentsKey = keys[2]
  local parentDepsKey = keys[3]
  local parentJobKey = keys[4]
  local parentStreamKey = keys[5]
  local parentEventsKey = keys[6]
  local childJobId = args[1]
  local parentId = args[2]
  local parentQueue = args[3]
  local depsMember = args[4]
  -- Verify child exists
  if redis.call('EXISTS', childJobKey) == 0 then
    return 'error:child_not_found'
  end
  -- Add parent entry to child's parents SET (idempotent)
  redis.call('SADD', childParentsKey, parentQueue .. ':' .. parentId)
  -- Add child as dependency in parent's deps SET (idempotent)
  redis.call('SADD', parentDepsKey, depsMember)
  -- Race condition check: if child already completed, trigger parent notification immediately
  local childState = redis.call('HGET', childJobKey, 'state')
  if childState == 'completed' then
    local depMarker = 'depdone:' .. depsMember
    if redis.call('HSETNX', parentJobKey, depMarker, '1') == 1 then
      local doneCount = redis.call('HINCRBY', parentJobKey, 'depsCompleted', 1)
      local totalDeps = redis.call('SCARD', parentDepsKey)
      if totalDeps - doneCount <= 0 then
        local parentState = redis.call('HGET', parentJobKey, 'state')
        if parentState == 'waiting-children' then
          redis.call('HSET', parentJobKey, 'state', 'waiting')
          redis.call('XADD', parentStreamKey, '*', 'jobId', parentId)
          emitEvent(parentEventsKey, 'active', parentId, nil)
        end
      end
    end
    return 'already_completed'
  end
  return 'ok'
end)

redis.register_function('glidemq_removeJob', function(keys, args)
  local jobKey = keys[1]
  local streamKey = keys[2]
  local scheduledKey = keys[3]
  local completedKey = keys[4]
  local failedKey = keys[5]
  local eventsKey = keys[6]
  local logKey = keys[7]
  local jobId = args[1]
  local exists = redis.call('EXISTS', jobKey)
  if exists == 0 then
    return 0
  end
  local state = redis.call('HGET', jobKey, 'state')
  local groupKey = redis.call('HGET', jobKey, 'groupKey')
  if groupKey and groupKey ~= '' then
    if state == 'active' then
      releaseGroupSlotAndPromote(jobKey, jobId, 0)
    elseif state == 'group-waiting' then
      local prefix = string.sub(jobKey, 1, #jobKey - #('job:' .. jobId))
      local waitListKey = prefix .. 'groupq:' .. groupKey
      redis.call('LREM', waitListKey, 1, jobId)
    end
  end
  redis.call('ZREM', scheduledKey, jobId)
  redis.call('ZREM', completedKey, jobId)
  redis.call('ZREM', failedKey, jobId)
  markOrderingDone(jobKey, jobId)
  -- Clean up DAG parents SET
  local prefix = string.sub(jobKey, 1, #jobKey - #('job:' .. jobId))
  local parentsKey = prefix .. 'parents:' .. jobId
  redis.call('DEL', parentsKey)
  redis.call('DEL', jobKey)
  redis.call('DEL', logKey)
  emitEvent(eventsKey, 'removed', jobId, nil)
  return 1
end)

redis.register_function('glidemq_clean', function(keys, args)
  local setKey = keys[1]
  local eventsKey = keys[2]
  local idKey = keys[3]
  local cutoff = tonumber(args[1])
  local limit = tonumber(args[2])
  if not limit or limit <= 0 then return {} end
  local prefix = string.sub(idKey, 1, #idKey - 2)
  local ids = redis.call('ZRANGEBYSCORE', setKey, '-inf', string.format('%.0f', cutoff), 'LIMIT', 0, limit)
  if #ids == 0 then
    return {}
  end
  for i = 1, #ids do
    redis.call('DEL', prefix .. 'job:' .. ids[i], prefix .. 'log:' .. ids[i], prefix .. 'deps:' .. ids[i], prefix .. 'parents:' .. ids[i])
  end
  for i = 1, #ids, 1000 do
    redis.call('ZREM', setKey, unpack(ids, i, math.min(i + 999, #ids)))
  end
  emitEvent(eventsKey, 'cleaned', tostring(#ids), nil)
  return ids
end)

redis.register_function('glidemq_revoke', function(keys, args)
  local jobKey = keys[1]
  local streamKey = keys[2]
  local scheduledKey = keys[3]
  local failedKey = keys[4]
  local eventsKey = keys[5]
  local jobId = args[1]
  local timestamp = tonumber(args[2])
  local group = args[3]
  local exists = redis.call('EXISTS', jobKey)
  if exists == 0 then
    return 'not_found'
  end
  redis.call('HSET', jobKey, 'revoked', '1')
  local state = redis.call('HGET', jobKey, 'state')
  if state == 'group-waiting' then
    local gk = redis.call('HGET', jobKey, 'groupKey')
    if gk and gk ~= '' then
      local prefix = string.sub(jobKey, 1, #jobKey - #('job:' .. jobId))
      local waitListKey = prefix .. 'groupq:' .. gk
      redis.call('LREM', waitListKey, 1, jobId)
    end
    redis.call('ZADD', failedKey, timestamp, jobId)
    redis.call('HSET', jobKey,
      'state', 'failed',
      'failedReason', 'revoked',
      'finishedOn', tostring(timestamp)
    )
    emitEvent(eventsKey, 'revoked', jobId, nil)
    return 'revoked'
  end
  if state == 'waiting' or state == 'delayed' or state == 'prioritized' then
    redis.call('ZREM', scheduledKey, jobId)
    local entries = redis.call('XRANGE', streamKey, '-', '+')
    for i = 1, #entries do
      local entryId = entries[i][1]
      local fields = entries[i][2]
      for j = 1, #fields, 2 do
        if fields[j] == 'jobId' and fields[j+1] == jobId then
          redis.call('XACK', streamKey, group, entryId)
          redis.call('XDEL', streamKey, entryId)
          break
        end
      end
    end
    redis.call('ZADD', failedKey, timestamp, jobId)
    redis.call('HSET', jobKey,
      'state', 'failed',
      'failedReason', 'revoked',
      'finishedOn', tostring(timestamp)
    )
    markOrderingDone(jobKey, jobId)
    emitEvent(eventsKey, 'revoked', jobId, nil)
    return 'revoked'
  end
  emitEvent(eventsKey, 'revoked', jobId, nil)
  return 'flagged'
end)

redis.register_function('glidemq_changePriority', function(keys, args)
  local jobKey = keys[1]
  local streamKey = keys[2]
  local scheduledKey = keys[3]
  local eventsKey = keys[4]
  local jobId = args[1]
  local newPriority = tonumber(args[2])
  if newPriority == nil or newPriority < 0 then
    return 'error:invalid_priority'
  end
  local group = args[3]
  local exists = redis.call('EXISTS', jobKey)
  if exists == 0 then
    return 'error:not_found'
  end
  local state = redis.call('HGET', jobKey, 'state')
  if state == 'waiting' then
    if newPriority == 0 then
      return 'no_op'
    end
    local cursor = '-'
    local found = false
    while not found do
      local entries = redis.call('XRANGE', streamKey, cursor, '+', 'COUNT', 1000)
      if #entries == 0 then break end
      for i = 1, #entries do
        local entryId = entries[i][1]
        local fields = entries[i][2]
        for j = 1, #fields, 2 do
          if fields[j] == 'jobId' and fields[j+1] == jobId then
            pcall(redis.call, 'XACK', streamKey, group, entryId)
            redis.call('XDEL', streamKey, entryId)
            found = true
            break
          end
        end
        if found then break end
      end
      if not found then
        local lastId = entries[#entries][1]
        local dashPos = lastId:find('-')
        cursor = lastId:sub(1, dashPos) .. tostring(tonumber(lastId:sub(dashPos + 1)) + 1)
      end
    end
    if not found then
      return 'error:not_in_stream'
    end
    redis.call('ZADD', scheduledKey, string.format('%.0f', newPriority * PRIORITY_SHIFT), jobId)
    redis.call('HSET', jobKey, 'state', 'prioritized', 'priority', tostring(newPriority))
    emitEvent(eventsKey, 'priority-changed', jobId, {'priority', tostring(newPriority)})
    return 'ok'
  elseif state == 'prioritized' then
    if newPriority == 0 then
      redis.call('ZREM', scheduledKey, jobId)
      redis.call('XADD', streamKey, '*', 'jobId', jobId)
      redis.call('HSET', jobKey, 'state', 'waiting', 'priority', '0')
    else
      redis.call('ZADD', scheduledKey, string.format('%.0f', newPriority * PRIORITY_SHIFT), jobId)
      redis.call('HSET', jobKey, 'priority', tostring(newPriority))
    end
    emitEvent(eventsKey, 'priority-changed', jobId, {'priority', tostring(newPriority)})
    return 'ok'
  elseif state == 'delayed' then
    local rawScore = redis.call('ZSCORE', scheduledKey, jobId)
    if rawScore == false then
      return 'error:not_in_scheduled'
    end
    local oldScore = tonumber(rawScore) or 0
    local oldTimestamp = oldScore % PRIORITY_SHIFT
    local newScore = newPriority * PRIORITY_SHIFT + oldTimestamp
    redis.call('ZREM', scheduledKey, jobId)
    redis.call('ZADD', scheduledKey, string.format('%.0f', newScore), jobId)
    redis.call('HSET', jobKey, 'priority', tostring(newPriority))
    emitEvent(eventsKey, 'priority-changed', jobId, {'priority', tostring(newPriority)})
    return 'ok'
  else
    return 'error:invalid_state'
  end
end)

redis.register_function('glidemq_changeDelay', function(keys, args)
  local jobKey = keys[1]
  local streamKey = keys[2]
  local scheduledKey = keys[3]
  local eventsKey = keys[4]
  local jobId = args[1]
  local newDelay = tonumber(args[2])
  if newDelay == nil or newDelay < 0 then
    return 'error:invalid_delay'
  end
  local now = tonumber(args[3])
  local group = args[4]
  local exists = redis.call('EXISTS', jobKey)
  if exists == 0 then
    return 'error:not_found'
  end
  local state = redis.call('HGET', jobKey, 'state')
  if state == 'delayed' then
    if newDelay == 0 then
      local rawScore = redis.call('ZSCORE', scheduledKey, jobId)
      if rawScore == false then
        return 'error:not_in_scheduled'
      end
      local oldScore = tonumber(rawScore) or 0
      local priority = math.floor(oldScore / PRIORITY_SHIFT)
      if priority > 0 then
        redis.call('ZADD', scheduledKey, 'XX', string.format('%.0f', priority * PRIORITY_SHIFT), jobId)
        redis.call('HSET', jobKey, 'state', 'prioritized', 'delay', '0')
      else
        redis.call('ZREM', scheduledKey, jobId)
        redis.call('XADD', streamKey, '*', 'jobId', jobId)
        redis.call('HSET', jobKey, 'state', 'waiting', 'delay', '0')
      end
    else
      local rawScore = redis.call('ZSCORE', scheduledKey, jobId)
      if rawScore == false then
        return 'error:not_in_scheduled'
      end
      local oldScore = tonumber(rawScore) or 0
      local priority = math.floor(oldScore / PRIORITY_SHIFT)
      local newScore = priority * PRIORITY_SHIFT + (now + newDelay)
      redis.call('ZADD', scheduledKey, 'XX', string.format('%.0f', newScore), jobId)
      redis.call('HSET', jobKey, 'delay', tostring(newDelay))
    end
    emitEvent(eventsKey, 'delay-changed', jobId, {'delay', tostring(newDelay)})
    return 'ok'
  elseif state == 'waiting' then
    if newDelay == 0 then
      return 'no_op'
    end
    local priority = tonumber(redis.call('HGET', jobKey, 'priority')) or 0
    local cursor = '-'
    local found = false
    while not found do
      local entries = redis.call('XRANGE', streamKey, cursor, '+', 'COUNT', 1000)
      if #entries == 0 then break end
      for i = 1, #entries do
        local entryId = entries[i][1]
        local fields = entries[i][2]
        for j = 1, #fields, 2 do
          if fields[j] == 'jobId' and fields[j+1] == jobId then
            pcall(redis.call, 'XACK', streamKey, group, entryId)
            redis.call('XDEL', streamKey, entryId)
            found = true
            break
          end
        end
        if found then break end
      end
      if not found then
        cursor = '(' .. entries[#entries][1]
      end
    end
    if not found then
      return 'error:not_in_stream'
    end
    local newScore = priority * PRIORITY_SHIFT + (now + newDelay)
    redis.call('ZADD', scheduledKey, string.format('%.0f', newScore), jobId)
    redis.call('HSET', jobKey, 'state', 'delayed', 'delay', tostring(newDelay))
    emitEvent(eventsKey, 'delay-changed', jobId, {'delay', tostring(newDelay)})
    return 'ok'
  elseif state == 'prioritized' then
    if newDelay == 0 then
      return 'no_op'
    end
    local rawScore = redis.call('ZSCORE', scheduledKey, jobId)
    if rawScore == false then
      return 'error:not_in_scheduled'
    end
    local oldScore = tonumber(rawScore) or 0
    local priority = math.floor(oldScore / PRIORITY_SHIFT)
    local newScore = priority * PRIORITY_SHIFT + (now + newDelay)
    redis.call('ZADD', scheduledKey, 'XX', string.format('%.0f', newScore), jobId)
    redis.call('HSET', jobKey, 'state', 'delayed', 'delay', tostring(newDelay))
    emitEvent(eventsKey, 'delay-changed', jobId, {'delay', tostring(newDelay)})
    return 'ok'
  else
    return 'error:invalid_state'
  end
end)

redis.register_function('glidemq_promoteJob', function(keys, args)
  local jobKey = keys[1]
  local streamKey = keys[2]
  local scheduledKey = keys[3]
  local eventsKey = keys[4]
  local jobId = args[1]
  local exists = redis.call('EXISTS', jobKey)
  if exists == 0 then
    return 'error:not_found'
  end
  local state = redis.call('HGET', jobKey, 'state')
  if state ~= 'delayed' then
    return 'error:not_delayed'
  end
  redis.call('ZREM', scheduledKey, jobId)
  redis.call('XADD', streamKey, '*', 'jobId', jobId)
  redis.call('HSET', jobKey, 'state', 'waiting', 'delay', '0')
  emitEvent(eventsKey, 'promoted', jobId, nil)
  return 'ok'
end)

redis.register_function('glidemq_moveActiveToDelayed', function(keys, args)
  local jobKey = keys[1]
  local streamKey = keys[2]
  local scheduledKey = keys[3]
  local eventsKey = keys[4]
  local jobId = args[1]
  local entryId = args[2]
  local now = tonumber(args[3]) or 0
  local delayedUntil = tonumber(args[4]) or now
  local group = args[5]
  local nextData = args[6]

  if redis.call('EXISTS', jobKey) == 0 then
    return 'error:not_found'
  end

  local state = redis.call('HGET', jobKey, 'state')
  if state ~= 'active' then
    return 'error:not_active'
  end

  if delayedUntil < now then
    delayedUntil = now
  end

  local priority = tonumber(redis.call('HGET', jobKey, 'priority')) or 0
  local delay = delayedUntil - now
  local score = priority * PRIORITY_SHIFT + delayedUntil

  pcall(redis.call, 'XACK', streamKey, group, entryId)
  redis.call('XDEL', streamKey, entryId)
  redis.call('ZADD', scheduledKey, string.format('%.0f', score), jobId)
  if nextData and nextData ~= '' then
    redis.call('HSET', jobKey, 'data', nextData, 'state', 'delayed', 'delay', tostring(delay))
  else
    redis.call('HSET', jobKey, 'state', 'delayed', 'delay', tostring(delay))
  end
  releaseGroupSlotAndPromote(jobKey, jobId, now, nil)
  emitEvent(eventsKey, 'delay-changed', jobId, {'delay', tostring(delay)})
  return 'ok'
end)

redis.register_function('glidemq_moveToWaitingChildren', function(keys, args)
  local jobKey = keys[1]
  local streamKey = keys[2]
  local eventsKey = keys[3]
  local jobId = args[1]
  local entryId = args[2]
  local group = args[3]
  local now = tonumber(args[4]) or 0

  local state = redis.call('HGET', jobKey, 'state')
  if not state then
    return 'error:not_found'
  end
  if state ~= 'active' then
    return 'error:not_active'
  end

  pcall(redis.call, 'XACK', streamKey, group, entryId)
  redis.call('XDEL', streamKey, entryId)
  redis.call('HSET', jobKey, 'state', 'waiting-children')

  releaseGroupSlotAndPromote(jobKey, jobId, now)

  -- Race condition check: children may have already completed before this call
  local prefix = string.sub(jobKey, 1, #jobKey - #('job:' .. jobId))
  local depsKey = prefix .. 'deps:' .. jobId
  local totalDeps = redis.call('SCARD', depsKey)
  if totalDeps > 0 then
    local depsCompleted = tonumber(redis.call('HGET', jobKey, 'depsCompleted')) or 0
    if depsCompleted >= totalDeps then
      redis.call('HSET', jobKey, 'state', 'waiting')
      redis.call('XADD', streamKey, '*', 'jobId', jobId)
      emitEvent(eventsKey, 'active', jobId, nil)
      return 'completed'
    end
  end

  emitEvent(eventsKey, 'waiting-children', jobId, nil)
  return 'ok'
end)

redis.register_function('glidemq_searchByName', function(keys, args)
  local stateKey = keys[1]
  local stateType = args[1]
  local nameFilter = args[2]
  local limit = tonumber(args[3]) or 100
  local prefix = args[4]
  local matched = {}
  if stateType == 'zset' then
    local members = redis.call('ZRANGE', stateKey, 0, -1)
    for i = 1, #members do
      if #matched >= limit then break end
      local jobId = members[i]
      local jobKey = prefix .. 'job:' .. jobId
      local name = redis.call('HGET', jobKey, 'name')
      if name == nameFilter then
        matched[#matched + 1] = jobId
      end
    end
  elseif stateType == 'stream' then
    local entries = redis.call('XRANGE', stateKey, '-', '+')
    for i = 1, #entries do
      if #matched >= limit then break end
      local fields = entries[i][2]
      local jobId = nil
      for j = 1, #fields, 2 do
        if fields[j] == 'jobId' then
          jobId = fields[j + 1]
          break
        end
      end
      if jobId then
        local jobKey = prefix .. 'job:' .. jobId
        local name = redis.call('HGET', jobKey, 'name')
        if name == nameFilter then
          matched[#matched + 1] = jobId
        end
      end
    end
  end
  return matched
end)

redis.register_function('glidemq_drain', function(keys, args)
  local streamKey = keys[1]
  local scheduledKey = keys[2]
  local eventsKey = keys[3]
  local idKey = keys[4]
  local drainDelayed = args[1] == '1'
  local group = args[2]
  local prefix = string.sub(idKey, 1, #idKey - 2)
  local removed = 0

  -- Build set of active entry IDs from PEL via paginated XPENDING
  local activeSet = {}
  local ok, pending = pcall(redis.call, 'XPENDING', streamKey, group, '-', '+', '10000')
  if ok and pending and #pending > 0 then
    for i = 1, #pending do
      activeSet[pending[i][1]] = true
    end
    -- Page through remaining PEL entries if there were exactly 10000
    while #pending == 10000 do
      local lastId = pending[#pending][1]
      local dashPos = lastId:find('-')
      local seq = tonumber(lastId:sub(dashPos + 1))
      local nextStart = lastId:sub(1, dashPos) .. tostring(seq + 1)
      ok, pending = pcall(redis.call, 'XPENDING', streamKey, group, nextStart, '+', '10000')
      if ok and pending and #pending > 0 then
        for i = 1, #pending do
          activeSet[pending[i][1]] = true
        end
      else
        break
      end
    end
  end

  -- Paginated XRANGE to avoid loading entire stream into memory
  local cursor = '-'
  while true do
    local entries = redis.call('XRANGE', streamKey, cursor, '+', 'COUNT', 1000)
    if #entries == 0 then break end

    local toDelete = {}
    for i = 1, #entries do
      local entryId = entries[i][1]
      if not activeSet[entryId] then
        toDelete[#toDelete + 1] = entryId
        local fields = entries[i][2]
        for j = 1, #fields, 2 do
          if fields[j] == 'jobId' and fields[j + 1] ~= '' then
            local jobId = fields[j + 1]
            redis.call('DEL', prefix .. 'job:' .. jobId, prefix .. 'log:' .. jobId, prefix .. 'deps:' .. jobId)
            removed = removed + 1
            break
          end
        end
      end
    end
    if #toDelete > 0 then
      for i = 1, #toDelete, 1000 do
        redis.call('XDEL', streamKey, unpack(toDelete, i, math.min(i + 999, #toDelete)))
      end
    end

    -- Advance cursor past the last entry
    local lastId = entries[#entries][1]
    local dashPos = lastId:find('-')
    local seq = tonumber(lastId:sub(dashPos + 1))
    cursor = lastId:sub(1, dashPos) .. tostring(seq + 1)
  end

  -- Optionally drain delayed/scheduled jobs
  if drainDelayed then
    local offset = 0
    while true do
      local scheduled = redis.call('ZRANGE', scheduledKey, offset, offset + 999)
      if #scheduled == 0 then break end
      local batch = {}
      for j = 1, #scheduled do
        local jobId = scheduled[j]
        batch[#batch + 1] = prefix .. 'job:' .. jobId
        batch[#batch + 1] = prefix .. 'log:' .. jobId
        batch[#batch + 1] = prefix .. 'deps:' .. jobId
      end
      redis.call('DEL', unpack(batch))
      removed = removed + #scheduled
      offset = offset + 1000
    end
    redis.call('DEL', scheduledKey)
  end

  if removed > 0 then
    emitEvent(eventsKey, 'drained', tostring(removed), nil)
  end
  return removed
end)

redis.register_function('glidemq_retryJobs', function(keys, args)
  local failedKey = keys[1]
  local scheduledKey = keys[2]
  local eventsKey = keys[3]
  local idKey = keys[4]
  local count = tonumber(args[1]) or 0
  local timestamp = tonumber(args[2])
  if not timestamp then return redis.error_reply('ERR invalid timestamp') end
  local prefix = string.sub(idKey, 1, #idKey - 2)
  local retried = 0

  while true do
    if count > 0 and retried >= count then break end
    local batchSize = 1000
    if count > 0 then
      batchSize = math.min(1000, count - retried)
    end
    local ids = redis.call('ZRANGE', failedKey, 0, batchSize - 1)
    if #ids == 0 then break end
    redis.call('ZREM', failedKey, unpack(ids))
    for i = 1, #ids do
      local jobId = ids[i]
      local jobKey = prefix .. 'job:' .. jobId
      if redis.call('EXISTS', jobKey) == 1 then
        local priority = tonumber(redis.call('HGET', jobKey, 'priority')) or 0
        local score = priority * PRIORITY_SHIFT + timestamp
        redis.call('ZADD', scheduledKey, score, jobId)
        redis.call('HSET', jobKey,
          'state', 'delayed',
          'attemptsMade', '0',
          'failedReason', '',
          'finishedOn', ''
        )
        retried = retried + 1
      end
    end
  end
  if retried > 0 then
    emitEvent(eventsKey, 'retried', tostring(retried), nil)
  end
  return retried
end)
`;

// ---- Key set type ----

export type QueueKeys = ReturnType<typeof import('../utils').buildKeys>;

// ---- Typed FCALL wrappers ----

/**
 * Add a job to the queue atomically.
 * Returns the new job ID (string).
 */
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
  parentQueue: string = '',
  parentDepsKey: string = '',
  schedulerName: string = '',
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
      parentQueue,
      schedulerName,
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
  parentQueue: string = '',
  parentDepsKey: string = '',
  schedulerName: string = '',
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
    parentQueue,
    parentDepsKey,
    schedulerName,
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
  parentQueue: string = '',
  parentDepsKey: string = '',
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
    parentQueue,
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
function encodeRetention(opt?: boolean | number | { age: number; count: number }): {
  mode: string;
  count: number;
  age: number;
} {
  if (opt === true) {
    return { mode: 'true', count: 0, age: 0 };
  }
  if (typeof opt === 'number') {
    return { mode: 'count', count: opt, age: 0 };
  }
  if (opt && typeof opt === 'object') {
    return { mode: 'age_count', count: opt.count ?? 0, age: opt.age ?? 0 };
  }
  return { mode: '0', count: 0, age: 0 };
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

  args.push(broadcastMode ? '1' : '0');

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
  args.push(hints?.orderingKey ?? '', orderingSeqHint, hints?.groupKey ?? '');
  args.push(broadcastMode ? '1' : '0');

  const raw = await client.fcall('glidemq_completeAndFetchNext', keys, args);

  // Fast path: array protocol from Lua function
  if (Array.isArray(raw)) {
    const arr = raw.map((v) => String(v));
    const tag = arr[0];
    if (tag === 'NEXT_NONE') {
      return { completed: arr[1] ?? jobId, next: false };
    }
    if (tag === 'NEXT_REVOKED') {
      return {
        completed: arr[1] ?? jobId,
        next: 'REVOKED',
        nextJobId: arr[2],
        nextEntryId: arr[3],
      };
    }
    if (tag === 'NEXT_HASH') {
      const hash: Record<string, string> = Object.create(null);
      for (let i = 4; i + 1 < arr.length; i += 2) {
        hash[arr[i]] = arr[i + 1];
      }
      return {
        completed: arr[1] ?? jobId,
        next: hash,
        nextJobId: arr[2],
        nextEntryId: arr[3],
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
      broadcastMode ? '1' : '0',
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
    ],
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
  const result = await client.fcall('glidemq_checkConcurrency', [k.meta, k.stream], [group]);
  return result as number;
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
): Promise<
  | Record<string, string>
  | 'REVOKED'
  | 'EXPIRED'
  | 'GROUP_FULL'
  | 'GROUP_RATE_LIMITED'
  | 'GROUP_TOKEN_LIMITED'
  | 'ERR:COST_EXCEEDS_CAPACITY'
  | null
> {
  const keys: string[] = [k.job(jobId)];
  const args: string[] = [timestamp.toString()];
  if (streamKey) {
    keys.push(streamKey);
    args.push(entryId, group, jobId);
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
): Promise<void> {
  await client.fcall('glidemq_deferActive', [k.stream, k.job(jobId)], [jobId, entryId, group]);
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
    [k.stream, k.scheduled, k.events, k.id],
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
): Promise<string> {
  const result = await client.fcall(
    'glidemq_moveActiveToDelayed',
    [k.job(jobId), k.stream, k.scheduled, k.events],
    [jobId, entryId, timestamp.toString(), delayedUntil.toString(), group, serializedData ?? ''],
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
): Promise<string> {
  const result = await client.fcall(
    'glidemq_moveToWaitingChildren',
    [k.job(jobId), k.stream, k.events],
    [jobId, entryId, group, timestamp.toString()],
  );
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
