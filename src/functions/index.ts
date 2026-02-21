import type { Client } from '../types';
import type { GlideReturnType } from '@glidemq/speedkey';

export const LIBRARY_NAME = 'glidemq';
export const LIBRARY_VERSION = '16';

// Consumer group name used by workers
export const CONSUMER_GROUP = 'workers';

// Embedded Lua library source (from glidemq.lua)
// Loaded once via FUNCTION LOAD, persistent across Valkey restarts.
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

local function markOrderingDone(jobKey, jobId)
  local orderingKey = redis.call('HGET', jobKey, 'orderingKey')
  if not orderingKey or orderingKey == '' then
    return
  end
  local orderingSeq = tonumber(redis.call('HGET', jobKey, 'orderingSeq')) or 0
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

local function releaseGroupSlotAndPromote(jobKey, jobId)
  local gk = redis.call('HGET', jobKey, 'groupKey')
  if not gk or gk == '' then return end
  local prefix = string.sub(jobKey, 1, #jobKey - #('job:' .. jobId))
  local groupHashKey = prefix .. 'group:' .. gk
  local cur = tonumber(redis.call('HGET', groupHashKey, 'active')) or 0
  if cur > 0 then
    redis.call('HSET', groupHashKey, 'active', tostring(cur - 1))
  end
  local waitListKey = prefix .. 'groupq:' .. gk
  local nextJobId = redis.call('LPOP', waitListKey)
  if nextJobId then
    local streamKey = prefix .. 'stream'
    redis.call('XADD', streamKey, '*', 'jobId', nextJobId)
    local nextJobKey = prefix .. 'job:' .. nextJobId
    redis.call('HSET', nextJobKey, 'state', 'waiting')
  end
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
  local jobId = redis.call('INCR', idKey)
  local jobIdStr = tostring(jobId)
  local prefix = string.sub(idKey, 1, #idKey - 2)
  local jobKey = prefix .. 'job:' .. jobIdStr
  local useGroupConcurrency = (orderingKey ~= '' and groupConcurrency > 1)
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
  if parentId ~= '' then
    hashFields[#hashFields + 1] = 'parentId'
    hashFields[#hashFields + 1] = parentId
  end
  if delay > 0 or priority > 0 then
    hashFields[#hashFields + 1] = 'state'
    hashFields[#hashFields + 1] = delay > 0 and 'delayed' or 'prioritized'
  else
    hashFields[#hashFields + 1] = 'state'
    hashFields[#hashFields + 1] = 'waiting'
  end
  redis.call('HSET', jobKey, unpack(hashFields))
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
      redis.call('XADD', streamKey, '*', 'jobId', jobId)
      redis.call('ZREM', scheduledKey, jobId)
      local prefix = string.sub(scheduledKey, 1, #scheduledKey - 9)
      local jobKey = prefix .. 'job:' .. jobId
      redis.call('HSET', jobKey, 'state', 'waiting')
      emitEvent(eventsKey, 'promoted', jobId, nil)
      count = count + 1
    end
    cursorMin = (priority + 1) * PRIORITY_SHIFT
  end
  return count
end)

redis.register_function('glidemq_complete', function(keys, args)
  local streamKey = keys[1]
  local completedKey = keys[2]
  local eventsKey = keys[3]
  local jobKey = keys[4]
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
  redis.call('XACK', streamKey, group, entryId)
  redis.call('XDEL', streamKey, entryId)
  redis.call('ZADD', completedKey, timestamp, jobId)
  redis.call('HSET', jobKey,
    'state', 'completed',
    'returnvalue', returnvalue,
    'finishedOn', tostring(timestamp)
  )
  markOrderingDone(jobKey, jobId)
  releaseGroupSlotAndPromote(jobKey, jobId)
  emitEvent(eventsKey, 'completed', jobId, {'returnvalue', returnvalue})
  local prefix = string.sub(jobKey, 1, #jobKey - #('job:' .. jobId))
  if removeMode == 'true' then
    redis.call('ZREM', completedKey, jobId)
    redis.call('DEL', jobKey)
  elseif removeMode == 'count' and removeCount > 0 then
    local total = redis.call('ZCARD', completedKey)
    if total > removeCount then
      local excess = redis.call('ZRANGE', completedKey, 0, total - removeCount - 1)
      for i = 1, #excess do
        local oldId = excess[i]
        redis.call('DEL', prefix .. 'job:' .. oldId)
        redis.call('ZREM', completedKey, oldId)
      end
    end
  elseif removeMode == 'age_count' then
    if removeAge > 0 then
      local cutoff = timestamp - (removeAge * 1000)
      local old = redis.call('ZRANGEBYSCORE', completedKey, '0', tostring(cutoff))
      for i = 1, #old do
        local oldId = old[i]
        redis.call('DEL', prefix .. 'job:' .. oldId)
        redis.call('ZREM', completedKey, oldId)
      end
    end
    if removeCount > 0 then
      local total = redis.call('ZCARD', completedKey)
      if total > removeCount then
        local excess = redis.call('ZRANGE', completedKey, 0, total - removeCount - 1)
        for i = 1, #excess do
          local oldId = excess[i]
          redis.call('DEL', prefix .. 'job:' .. oldId)
          redis.call('ZREM', completedKey, oldId)
        end
      end
    end
  end
  if depsMember ~= '' and parentId ~= '' and #keys >= 8 then
    local parentDepsKey = keys[5]
    local parentJobKey = keys[6]
    local parentStreamKey = keys[7]
    local parentEventsKey = keys[8]
    local doneCount = redis.call('HINCRBY', parentJobKey, 'depsCompleted', 1)
    local totalDeps = redis.call('SCARD', parentDepsKey)
    local remaining = totalDeps - doneCount
    if remaining <= 0 then
      redis.call('HSET', parentJobKey, 'state', 'waiting')
      redis.call('XADD', parentStreamKey, '*', 'jobId', parentId)
      emitEvent(parentEventsKey, 'active', parentId, nil)
    end
  end
  return 1
end)

redis.register_function('glidemq_completeAndFetchNext', function(keys, args)
  local streamKey = keys[1]
  local completedKey = keys[2]
  local eventsKey = keys[3]
  local jobKey = keys[4]
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

  -- Phase 1: Complete current job (same as glidemq_complete)
  redis.call('XACK', streamKey, group, entryId)
  redis.call('XDEL', streamKey, entryId)
  redis.call('ZADD', completedKey, timestamp, jobId)
  redis.call('HSET', jobKey,
    'state', 'completed',
    'returnvalue', returnvalue,
    'finishedOn', tostring(timestamp)
  )
  markOrderingDone(jobKey, jobId)
  releaseGroupSlotAndPromote(jobKey, jobId)
  emitEvent(eventsKey, 'completed', jobId, {'returnvalue', returnvalue})
  local prefix = string.sub(jobKey, 1, #jobKey - #('job:' .. jobId))

  -- Retention cleanup
  if removeMode == 'true' then
    redis.call('ZREM', completedKey, jobId)
    redis.call('DEL', jobKey)
  elseif removeMode == 'count' and removeCount > 0 then
    local total = redis.call('ZCARD', completedKey)
    if total > removeCount then
      local excess = redis.call('ZRANGE', completedKey, 0, total - removeCount - 1)
      for i = 1, #excess do
        redis.call('DEL', prefix .. 'job:' .. excess[i])
        redis.call('ZREM', completedKey, excess[i])
      end
    end
  end

  -- Parent deps
  if depsMember ~= '' and parentId ~= '' and #keys >= 8 then
    local parentDepsKey = keys[5]
    local parentJobKey = keys[6]
    local parentStreamKey = keys[7]
    local parentEventsKey = keys[8]
    local doneCount = redis.call('HINCRBY', parentJobKey, 'depsCompleted', 1)
    local totalDeps = redis.call('SCARD', parentDepsKey)
    if totalDeps - doneCount <= 0 then
      redis.call('HSET', parentJobKey, 'state', 'waiting')
      redis.call('XADD', parentStreamKey, '*', 'jobId', parentId)
      emitEvent(parentEventsKey, 'active', parentId, nil)
    end
  end

  -- Phase 2: Fetch next job (non-blocking XREADGROUP)
  local nextEntries = redis.call('XREADGROUP', 'GROUP', group, consumer, 'COUNT', 1, 'STREAMS', streamKey, '>')
  if not nextEntries or #nextEntries == 0 then
    return cjson.encode({completed = jobId, next = false})
  end
  local streamData = nextEntries[1]
  local entries = streamData[2]
  if not entries or #entries == 0 then
    return cjson.encode({completed = jobId, next = false})
  end
  local nextEntry = entries[1]
  local nextEntryId = nextEntry[1]
  local nextFields = nextEntry[2]
  local nextJobId = nil
  for i = 1, #nextFields, 2 do
    if nextFields[i] == 'jobId' then
      nextJobId = nextFields[i + 1]
      break
    end
  end
  if not nextJobId then
    return cjson.encode({completed = jobId, next = false})
  end

  -- Phase 3: Activate next job (same as moveToActive)
  local nextJobKey = prefix .. 'job:' .. nextJobId
  local nextExists = redis.call('EXISTS', nextJobKey)
  if nextExists == 0 then
    return cjson.encode({completed = jobId, next = false, nextEntryId = nextEntryId})
  end
  local revoked = redis.call('HGET', nextJobKey, 'revoked')
  if revoked == '1' then
    return cjson.encode({completed = jobId, next = 'REVOKED', nextJobId = nextJobId, nextEntryId = nextEntryId})
  end
  local nextGroupKey = redis.call('HGET', nextJobKey, 'groupKey')
  if nextGroupKey and nextGroupKey ~= '' then
    local nextGroupHashKey = prefix .. 'group:' .. nextGroupKey
    local nextMaxConc = tonumber(redis.call('HGET', nextGroupHashKey, 'maxConcurrency')) or 0
    local nextActive = tonumber(redis.call('HGET', nextGroupHashKey, 'active')) or 0
    if nextMaxConc > 0 and nextActive >= nextMaxConc then
      redis.call('XACK', streamKey, group, nextEntryId)
      redis.call('XDEL', streamKey, nextEntryId)
      local nextWaitListKey = prefix .. 'groupq:' .. nextGroupKey
      redis.call('RPUSH', nextWaitListKey, nextJobId)
      redis.call('HSET', nextJobKey, 'state', 'group-waiting')
      return cjson.encode({completed = jobId, next = false})
    end
    redis.call('HINCRBY', nextGroupHashKey, 'active', 1)
  end
  redis.call('HSET', nextJobKey, 'state', 'active', 'processedOn', tostring(timestamp), 'lastActive', tostring(timestamp))
  local nextHash = redis.call('HGETALL', nextJobKey)
  return cjson.encode({completed = jobId, next = nextHash, nextJobId = nextJobId, nextEntryId = nextEntryId})
end)

redis.register_function('glidemq_fail', function(keys, args)
  local streamKey = keys[1]
  local failedKey = keys[2]
  local scheduledKey = keys[3]
  local eventsKey = keys[4]
  local jobKey = keys[5]
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
  redis.call('XACK', streamKey, group, entryId)
  redis.call('XDEL', streamKey, entryId)
  local attemptsMade = redis.call('HINCRBY', jobKey, 'attemptsMade', 1)
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
    releaseGroupSlotAndPromote(jobKey, jobId)
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
    releaseGroupSlotAndPromote(jobKey, jobId)
    emitEvent(eventsKey, 'failed', jobId, {'failedReason', failedReason})
    local prefix = string.sub(jobKey, 1, #jobKey - #('job:' .. jobId))
    if removeMode == 'true' then
      redis.call('ZREM', failedKey, jobId)
      redis.call('DEL', jobKey)
    elseif removeMode == 'count' and removeCount > 0 then
      local total = redis.call('ZCARD', failedKey)
      if total > removeCount then
        local excess = redis.call('ZRANGE', failedKey, 0, total - removeCount - 1)
        for i = 1, #excess do
          local oldId = excess[i]
          redis.call('DEL', prefix .. 'job:' .. oldId)
          redis.call('ZREM', failedKey, oldId)
        end
      end
    elseif removeMode == 'age_count' then
      if removeAge > 0 then
        local cutoff = timestamp - (removeAge * 1000)
        local old = redis.call('ZRANGEBYSCORE', failedKey, '0', tostring(cutoff))
        for i = 1, #old do
          local oldId = old[i]
          redis.call('DEL', prefix .. 'job:' .. oldId)
          redis.call('ZREM', failedKey, oldId)
        end
      end
      if removeCount > 0 then
        local total = redis.call('ZCARD', failedKey)
        if total > removeCount then
          local excess = redis.call('ZRANGE', failedKey, 0, total - removeCount - 1)
          for i = 1, #excess do
            local oldId = excess[i]
            redis.call('DEL', prefix .. 'job:' .. oldId)
            redis.call('ZREM', failedKey, oldId)
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
      local lastActive = tonumber(redis.call('HGET', jobKey, 'lastActive'))
      if lastActive and (timestamp - lastActive) < minIdleMs then
        count = count + 1
      else
      local stalledCount = redis.call('HINCRBY', jobKey, 'stalledCount', 1)
      if stalledCount > maxStalledCount then
        redis.call('XACK', streamKey, group, entryId)
        redis.call('XDEL', streamKey, entryId)
        redis.call('ZADD', failedKey, timestamp, jobId)
        redis.call('HSET', jobKey,
          'state', 'failed',
          'failedReason', 'job stalled more than maxStalledCount',
          'finishedOn', tostring(timestamp)
        )
        markOrderingDone(jobKey, jobId)
        releaseGroupSlotAndPromote(jobKey, jobId)
        emitEvent(eventsKey, 'failed', jobId, {
          'failedReason', 'job stalled more than maxStalledCount'
        })
      else
        redis.call('HSET', jobKey, 'state', 'active')
        emitEvent(eventsKey, 'stalled', jobId, nil)
      end
      count = count + 1
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
  local jobId = redis.call('INCR', idKey)
  local jobIdStr = tostring(jobId)
  local jobKey = prefix .. 'job:' .. jobIdStr
  local useGroupConcurrency = (orderingKey ~= '' and groupConcurrency > 1)
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
  if parentId ~= '' then
    hashFields[#hashFields + 1] = 'parentId'
    hashFields[#hashFields + 1] = parentId
  end
  if delay > 0 or priority > 0 then
    hashFields[#hashFields + 1] = 'state'
    hashFields[#hashFields + 1] = delay > 0 and 'delayed' or 'prioritized'
  else
    hashFields[#hashFields + 1] = 'state'
    hashFields[#hashFields + 1] = 'waiting'
  end
  redis.call('HSET', jobKey, unpack(hashFields))
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
  local exists = redis.call('EXISTS', jobKey)
  if exists == 0 then
    return ''
  end
  local revoked = redis.call('HGET', jobKey, 'revoked')
  if revoked == '1' then
    return 'REVOKED'
  end
  local groupKey = redis.call('HGET', jobKey, 'groupKey')
  if groupKey and groupKey ~= '' then
    local prefix = string.sub(jobKey, 1, #jobKey - #('job:' .. jobId))
    local groupHashKey = prefix .. 'group:' .. groupKey
    local maxConc = tonumber(redis.call('HGET', groupHashKey, 'maxConcurrency')) or 0
    local active = tonumber(redis.call('HGET', groupHashKey, 'active')) or 0
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
    redis.call('HINCRBY', groupHashKey, 'active', 1)
  end
  redis.call('HSET', jobKey, 'state', 'active', 'processedOn', timestamp, 'lastActive', timestamp)
  local fields = redis.call('HGETALL', jobKey)
  return cjson.encode(fields)
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
  local parentJobId = redis.call('INCR', parentIdKey)
  local parentJobIdStr = tostring(parentJobId)
  local parentPrefix = string.sub(parentIdKey, 1, #parentIdKey - 2)
  local parentJobKey = parentPrefix .. 'job:' .. parentJobIdStr
  local depsKey = parentPrefix .. 'deps:' .. parentJobIdStr
  local parentOrderingKey = extractOrderingKeyFromOpts(parentOpts)
  local parentGroupConc = extractGroupConcurrencyFromOpts(parentOpts)
  local parentOrderingSeq = 0
  if parentOrderingKey ~= '' and parentGroupConc <= 1 then
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
  if parentOrderingKey ~= '' and parentGroupConc > 1 then
    parentHash[#parentHash + 1] = 'groupKey'
    parentHash[#parentHash + 1] = parentOrderingKey
    local groupHashKey = parentPrefix .. 'group:' .. parentOrderingKey
    redis.call('HSET', groupHashKey, 'maxConcurrency', tostring(parentGroupConc))
    redis.call('HSETNX', groupHashKey, 'active', '0')
  elseif parentOrderingKey ~= '' then
    parentHash[#parentHash + 1] = 'orderingKey'
    parentHash[#parentHash + 1] = parentOrderingKey
    parentHash[#parentHash + 1] = 'orderingSeq'
    parentHash[#parentHash + 1] = tostring(parentOrderingSeq)
  end
  redis.call('HSET', parentJobKey, unpack(parentHash))
  local childIds = {}
  local childArgOffset = 8
  local childKeyOffset = 4
  for i = 1, numChildren do
    local base = childArgOffset + (i - 1) * 8
    local childName = args[base + 1]
    local childData = args[base + 2]
    local childOpts = args[base + 3]
    local childDelay = tonumber(args[base + 4]) or 0
    local childPriority = tonumber(args[base + 5]) or 0
    local childMaxAttempts = tonumber(args[base + 6]) or 0
    local childQueuePrefix = args[base + 7]
    local childParentQueue = args[base + 8]
    local ckBase = childKeyOffset + (i - 1) * 4
    local childIdKey = keys[ckBase + 1]
    local childStreamKey = keys[ckBase + 2]
    local childScheduledKey = keys[ckBase + 3]
    local childEventsKey = keys[ckBase + 4]
    local childJobId = redis.call('INCR', childIdKey)
    local childJobIdStr = tostring(childJobId)
    local childPrefix = string.sub(childIdKey, 1, #childIdKey - 2)
    local childJobKey = childPrefix .. 'job:' .. childJobIdStr
    local childOrderingKey = extractOrderingKeyFromOpts(childOpts)
    local childGroupConc = extractGroupConcurrencyFromOpts(childOpts)
    local childOrderingSeq = 0
    if childOrderingKey ~= '' and childGroupConc <= 1 then
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
    if childOrderingKey ~= '' and childGroupConc > 1 then
      childHash[#childHash + 1] = 'groupKey'
      childHash[#childHash + 1] = childOrderingKey
      local childGroupHashKey = childPrefix .. 'group:' .. childOrderingKey
      redis.call('HSETNX', childGroupHashKey, 'maxConcurrency', tostring(childGroupConc))
      redis.call('HSETNX', childGroupHashKey, 'active', '0')
    elseif childOrderingKey ~= '' then
      childHash[#childHash + 1] = 'orderingKey'
      childHash[#childHash + 1] = childOrderingKey
      childHash[#childHash + 1] = 'orderingSeq'
      childHash[#childHash + 1] = tostring(childOrderingSeq)
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
  local extraDepsOffset = childArgOffset + numChildren * 8
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
  local doneCount = redis.call('HINCRBY', parentJobKey, 'depsCompleted', 1)
  local totalDeps = redis.call('SCARD', depsKey)
  local remaining = totalDeps - doneCount
  if remaining <= 0 then
    redis.call('HSET', parentJobKey, 'state', 'waiting')
    redis.call('XADD', parentStreamKey, '*', 'jobId', parentId)
    emitEvent(parentEventsKey, 'active', parentId, nil)
  end
  return remaining
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
      releaseGroupSlotAndPromote(jobKey, jobId)
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
  redis.call('DEL', jobKey)
  redis.call('DEL', logKey)
  emitEvent(eventsKey, 'removed', jobId, nil)
  return 1
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
`;

// ---- Key set type ----

export type QueueKeys = ReturnType<typeof import('../utils').buildKeys>;

// ---- Typed FCALL wrappers ----

/**
 * Add a job to the queue atomically.
 * Returns the new job ID (string).
 */
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
): Promise<string> {
  const result = await client.fcall(
    'glidemq_addJob',
    [k.id, k.stream, k.scheduled, k.events],
    [
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
    ],
  );
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
): Promise<string> {
  const result = await client.fcall(
    'glidemq_dedup',
    [k.dedup, k.id, k.stream, k.scheduled, k.events],
    [
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
    ],
  );
  return result as string;
}

/**
 * Promote delayed/prioritized jobs whose score <= now from scheduled ZSet to stream.
 * Returns the number of jobs promoted.
 */
export async function promote(
  client: Client,
  k: QueueKeys,
  timestamp: number,
): Promise<number> {
  const result = await client.fcall(
    'glidemq_promote',
    [k.scheduled, k.stream, k.events],
    [timestamp.toString()],
  );
  return result as number;
}

/**
 * Encode a removeOnComplete/removeOnFail option into Lua args.
 */
function encodeRetention(
  opt?: boolean | number | { age: number; count: number },
): { mode: string; count: number; age: number } {
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
): Promise<GlideReturnType> {
  const { mode, count, age } = encodeRetention(removeOnComplete);

  const keys: string[] = [k.stream, k.completed, k.events, k.job(jobId)];
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
    keys.push(
      pk.deps(parentInfo.parentId),
      pk.job(parentInfo.parentId),
      pk.stream,
      pk.events,
    );
    args.push(parentInfo.depsMember, parentInfo.parentId);
  } else {
    args.push('', '');
  }

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
  next: false | 'REVOKED' | string[];
  nextJobId?: string;
  nextEntryId?: string;
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
): Promise<CompleteAndFetchResult> {
  const { mode, count, age } = encodeRetention(removeOnComplete);

  const keys: string[] = [k.stream, k.completed, k.events, k.job(jobId)];
  const args: string[] = [
    jobId, entryId, returnvalue, timestamp.toString(),
    group, consumer,
    mode, count.toString(), age.toString(),
  ];

  if (parentInfo) {
    const pk = parentInfo.parentKeys;
    keys.push(pk.deps(parentInfo.parentId), pk.job(parentInfo.parentId), pk.stream, pk.events);
    args.push(parentInfo.depsMember, parentInfo.parentId);
  } else {
    args.push('', '');
  }

  const raw = await client.fcall('glidemq_completeAndFetchNext', keys, args);
  const parsed = JSON.parse(String(raw));

  if (!parsed.next || parsed.next === false) {
    return { completed: parsed.completed, next: false };
  }
  if (parsed.next === 'REVOKED') {
    return { completed: parsed.completed, next: 'REVOKED', nextJobId: parsed.nextJobId, nextEntryId: parsed.nextEntryId };
  }

  // Parse the HGETALL array into a hash map
  const arr = parsed.next as string[];
  const hash: Record<string, string> = {};
  for (let i = 0; i < arr.length; i += 2) {
    hash[String(arr[i])] = String(arr[i + 1]);
  }
  return { completed: parsed.completed, next: hash as any, nextJobId: parsed.nextJobId, nextEntryId: parsed.nextEntryId };
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
): Promise<string> {
  const { mode, count, age } = encodeRetention(removeOnFail);
  const result = await client.fcall(
    'glidemq_fail',
    [k.stream, k.failed, k.scheduled, k.events, k.job(jobId)],
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
    ],
  );
  return result as number;
}

/**
 * Pause a queue: sets paused=1 in meta hash, emits event.
 */
export async function pause(
  client: Client,
  k: QueueKeys,
): Promise<void> {
  await client.fcall(
    'glidemq_pause',
    [k.meta, k.events],
    [],
  );
}

/**
 * Resume a queue: sets paused=0 in meta hash, emits event.
 */
export async function resume(
  client: Client,
  k: QueueKeys,
): Promise<void> {
  await client.fcall(
    'glidemq_resume',
    [k.meta, k.events],
    [],
  );
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
    [
      maxPerWindow.toString(),
      windowDuration.toString(),
      timestamp.toString(),
    ],
  );
  return result as number;
}

/**
 * Check global concurrency: returns -1 if no limit is set, 0 if blocked
 * (pending >= globalConcurrency), or a positive number indicating remaining
 * capacity (globalConcurrency - pending).
 */
export async function checkConcurrency(
  client: Client,
  k: QueueKeys,
  group: string = CONSUMER_GROUP,
): Promise<number> {
  const result = await client.fcall(
    'glidemq_checkConcurrency',
    [k.meta, k.stream],
    [group],
  );
  return result as number;
}

/**
 * Move a job to active state in a single round trip.
 * Reads the full job hash, checks revoked flag, sets state=active + processedOn + lastActive.
 * For group-concurrency jobs, checks if the group has capacity. If not, parks the job
 * in the group wait list and returns 'GROUP_FULL'.
 * Returns:
 * - null if job hash doesn't exist
 * - 'REVOKED' if the job's revoked flag is set
 * - 'GROUP_FULL' if the job's group is at max concurrency (job was parked)
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
): Promise<Record<string, string> | 'REVOKED' | 'GROUP_FULL' | null> {
  const keys: string[] = [k.job(jobId)];
  const args: string[] = [timestamp.toString()];
  if (streamKey) {
    keys.push(streamKey);
    args.push(entryId, group, jobId);
  }
  const result = await client.fcall(
    'glidemq_moveToActive',
    keys,
    args,
  );
  const str = String(result);
  if (str === '' || str === 'null') return null;
  if (str === 'REVOKED') return 'REVOKED';
  if (str === 'GROUP_FULL') return 'GROUP_FULL';
  // Parse the cjson.encode output: [field1, value1, field2, value2, ...]
  const arr = JSON.parse(str) as string[];
  const hash: Record<string, string> = {};
  for (let i = 0; i < arr.length; i += 2) {
    hash[String(arr[i])] = String(arr[i + 1]);
  }
  return hash;
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
  await client.fcall(
    'glidemq_deferActive',
    [k.stream, k.job(jobId)],
    [jobId, entryId, group],
  );
}

/**
 * Remove a job from all data structures (hash, stream, scheduled, completed, failed).
 * Returns 1 if removed, 0 if not found.
 */
export async function removeJob(
  client: Client,
  k: QueueKeys,
  jobId: string,
): Promise<number> {
  const result = await client.fcall(
    'glidemq_removeJob',
    [k.job(jobId), k.stream, k.scheduled, k.completed, k.failed, k.events, k.log(jobId)],
    [jobId],
  );
  return result as number;
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
  }[],
  extraDeps: string[] = [],
): Promise<string[]> {
  const keys: string[] = [
    parentKeys.id,
    parentKeys.stream,
    parentKeys.scheduled,
    parentKeys.events,
  ];
  const args: string[] = [
    parentName,
    parentData,
    parentOpts,
    timestamp.toString(),
    parentDelay.toString(),
    parentPriority.toString(),
    parentMaxAttempts.toString(),
    children.length.toString(),
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
