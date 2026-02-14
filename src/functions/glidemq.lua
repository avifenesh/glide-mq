#!lua name=glidemq

-- glide-mq function library v1
-- Loaded once via FUNCTION LOAD, persistent across restarts.
-- All keys use hash tag {queueName} for cluster safety.

local PRIORITY_SHIFT = 4398046511104 -- 2^42

-- Helper: emit an event to the events stream
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

-- glidemq_version()
-- Returns the library version string.
-- KEYS: (none)  ARGS: (none)
redis.register_function('glidemq_version', function(keys, args)
  return '1'
end)

-- glidemq_addJob(KEYS, ARGS)
-- KEYS[1] = id counter key
-- KEYS[2] = stream key
-- KEYS[3] = scheduled zset key
-- KEYS[4] = events stream key
-- (job hash key is derived from KEYS[1] prefix + ':job:' + id)
-- ARGS[1] = job name
-- ARGS[2] = job data (JSON string)
-- ARGS[3] = job opts (JSON string)
-- ARGS[4] = timestamp (ms)
-- ARGS[5] = delay (ms, 0 = no delay)
-- ARGS[6] = priority (0 = no priority)
-- ARGS[7] = parentId (empty string = no parent)
-- ARGS[8] = maxAttempts (0 = no retry)
-- Returns: job ID (string)
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

  -- Generate job ID via atomic increment
  local jobId = redis.call('INCR', idKey)
  local jobIdStr = tostring(jobId)

  -- Derive the job hash key from the id key prefix
  -- idKey is like "glide:{q}:id", job key is "glide:{q}:job:{id}"
  local prefix = string.sub(idKey, 1, #idKey - 2) -- strip ":id"
  local jobKey = prefix .. 'job:' .. jobIdStr

  -- Store job hash
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

  -- Route job: delayed/prioritized go to scheduled ZSet, otherwise to stream
  if delay > 0 then
    -- Delayed: score = (priority * 2^42) + (timestamp + delay)
    local score = priority * PRIORITY_SHIFT + (timestamp + delay)
    redis.call('ZADD', scheduledKey, score, jobIdStr)
  elseif priority > 0 then
    -- Prioritized but not delayed: score = (priority * 2^42) + 0
    -- timestamp=0 so it promotes immediately on next promote cycle
    local score = priority * PRIORITY_SHIFT
    redis.call('ZADD', scheduledKey, score, jobIdStr)
  else
    -- Immediate: add to stream
    redis.call('XADD', streamKey, '*', 'jobId', jobIdStr)
  end

  -- Emit 'added' event
  emitEvent(eventsKey, 'added', jobIdStr, {'name', jobName})

  return jobIdStr
end)

-- glidemq_promote(KEYS, ARGS)
-- Promotes jobs from the scheduled ZSet to the stream when their time has come.
-- KEYS[1] = scheduled zset key
-- KEYS[2] = stream key
-- KEYS[3] = events stream key
-- ARGS[1] = current timestamp (ms)
-- Returns: number of promoted jobs
redis.register_function('glidemq_promote', function(keys, args)
  local scheduledKey = keys[1]
  local streamKey = keys[2]
  local eventsKey = keys[3]

  local now = tonumber(args[1])

  -- Promote all jobs with score <= now
  -- For prioritized (non-delayed) jobs, their score has timestamp component = 0,
  -- which is always <= now, so they always promote.
  local members = redis.call('ZRANGEBYSCORE', scheduledKey, '0', tostring(now))

  local count = 0
  for i = 1, #members do
    local jobId = members[i]
    redis.call('XADD', streamKey, '*', 'jobId', jobId)
    redis.call('ZREM', scheduledKey, jobId)

    -- Update job state
    -- Derive the prefix from scheduledKey: "glide:{q}:scheduled" -> "glide:{q}:"
    local prefix = string.sub(scheduledKey, 1, #scheduledKey - 9) -- strip "scheduled"
    local jobKey = prefix .. 'job:' .. jobId
    redis.call('HSET', jobKey, 'state', 'waiting')

    emitEvent(eventsKey, 'promoted', jobId, nil)
    count = count + 1
  end

  return count
end)

-- glidemq_complete(KEYS, ARGS)
-- Moves a job from active (PEL) to completed.
-- After completion, if the job has a parent, triggers completeChild logic.
-- KEYS[1] = stream key
-- KEYS[2] = completed zset key
-- KEYS[3] = events stream key
-- KEYS[4] = job hash key (job:{id})
-- Optional parent keys (when job has parentId):
-- KEYS[5] = parent deps key (glide:{parentQueue}:deps:{parentId})
-- KEYS[6] = parent job hash key (glide:{parentQueue}:job:{parentId})
-- KEYS[7] = parent stream key (glide:{parentQueue}:stream)
-- KEYS[8] = parent events key (glide:{parentQueue}:events)
-- ARGS[1] = job ID
-- ARGS[2] = stream entry ID (for XACK)
-- ARGS[3] = return value (JSON string)
-- ARGS[4] = timestamp (ms)
-- ARGS[5] = consumer group name
-- ARGS[6] = removeMode ('0'|'true'|'count'|'age_count')
-- ARGS[7] = removeCount (number)
-- ARGS[8] = removeAge (seconds)
-- ARGS[9] = depsMember (childQueuePrefix:childId, empty string = no parent)
-- ARGS[10] = parentId (empty string = no parent)
-- Returns: 1 on success
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

  -- Acknowledge the stream entry
  redis.call('XACK', streamKey, group, entryId)

  -- Remove the message from the stream (we no longer need it)
  redis.call('XDEL', streamKey, entryId)

  -- Add to completed ZSet (score = timestamp for ordering)
  redis.call('ZADD', completedKey, timestamp, jobId)

  -- Update job hash
  redis.call('HSET', jobKey,
    'state', 'completed',
    'returnvalue', returnvalue,
    'finishedOn', tostring(timestamp)
  )

  -- Emit event
  emitEvent(eventsKey, 'completed', jobId, {'returnvalue', returnvalue})

  -- Retention cleanup
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

  -- If this job has a parent, handle completeChild inline
  if depsMember ~= '' and parentId ~= '' and #keys >= 8 then
    local parentDepsKey = keys[5]
    local parentJobKey = keys[6]
    local parentStreamKey = keys[7]
    local parentEventsKey = keys[8]

    -- Increment completed children counter on parent hash
    local doneCount = redis.call('HINCRBY', parentJobKey, 'depsCompleted', 1)
    local totalDeps = redis.call('SCARD', parentDepsKey)
    local remaining = totalDeps - doneCount

    if remaining <= 0 then
      -- All children done: move parent to stream
      redis.call('HSET', parentJobKey, 'state', 'waiting')
      redis.call('XADD', parentStreamKey, '*', 'jobId', parentId)
      emitEvent(parentEventsKey, 'active', parentId, nil)
    end
  end

  return 1
end)

-- glidemq_fail(KEYS, ARGS)
-- Handles job failure: retry with backoff or move to failed set.
-- KEYS[1] = stream key
-- KEYS[2] = failed zset key
-- KEYS[3] = scheduled zset key
-- KEYS[4] = events stream key
-- KEYS[5] = job hash key (job:{id})
-- ARGS[1] = job ID
-- ARGS[2] = stream entry ID
-- ARGS[3] = failed reason (string)
-- ARGS[4] = timestamp (ms)
-- ARGS[5] = max attempts (0 = no retry)
-- ARGS[6] = backoff delay (ms, 0 = no backoff)
-- ARGS[7] = consumer group name
-- ARGS[8] = removeMode ('0'|'true'|'count'|'age_count')
-- ARGS[9] = removeCount (number)
-- ARGS[10] = removeAge (seconds)
-- Returns: "failed" | "retrying"
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

  -- Acknowledge the stream entry
  redis.call('XACK', streamKey, group, entryId)
  redis.call('XDEL', streamKey, entryId)

  -- Increment attempt count
  local attemptsMade = redis.call('HINCRBY', jobKey, 'attemptsMade', 1)

  if maxAttempts > 0 and attemptsMade < maxAttempts then
    -- Retry: schedule with backoff
    local retryAt = timestamp + backoffDelay
    local priority = tonumber(redis.call('HGET', jobKey, 'priority')) or 0
    local score = priority * PRIORITY_SHIFT + retryAt

    redis.call('ZADD', scheduledKey, score, jobId)
    redis.call('HSET', jobKey,
      'state', 'delayed',
      'failedReason', failedReason,
      'processedOn', tostring(timestamp)
    )

    emitEvent(eventsKey, 'retrying', jobId, {
      'failedReason', failedReason,
      'attemptsMade', tostring(attemptsMade),
      'delay', tostring(backoffDelay)
    })

    return 'retrying'
  else
    -- No more retries: move to failed set
    redis.call('ZADD', failedKey, timestamp, jobId)
    redis.call('HSET', jobKey,
      'state', 'failed',
      'failedReason', failedReason,
      'finishedOn', tostring(timestamp),
      'processedOn', tostring(timestamp)
    )

    emitEvent(eventsKey, 'failed', jobId, {'failedReason', failedReason})

    -- Retention cleanup
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

-- glidemq_reclaimStalled(KEYS, ARGS)
-- Uses XAUTOCLAIM to reclaim stalled jobs from inactive consumers.
-- KEYS[1] = stream key
-- KEYS[2] = events stream key
-- ARGS[1] = consumer group name
-- ARGS[2] = claiming consumer name
-- ARGS[3] = min idle time (ms)
-- ARGS[4] = max stalled count (move to failed after this many reclaims)
-- ARGS[5] = timestamp (ms)
-- ARGS[6] = failed zset key
-- Returns: number of reclaimed jobs
redis.register_function('glidemq_reclaimStalled', function(keys, args)
  local streamKey = keys[1]
  local eventsKey = keys[2]

  local group = args[1]
  local consumer = args[2]
  local minIdleMs = tonumber(args[3])
  local maxStalledCount = tonumber(args[4]) or 1
  local timestamp = tonumber(args[5])
  local failedKey = args[6]

  -- XAUTOCLAIM returns: [nextStartId, [[entryId, fields...], ...], [deletedIds...]]
  local result = redis.call('XAUTOCLAIM', streamKey, group, consumer, minIdleMs, '0-0')

  local entries = result[2]
  if not entries or #entries == 0 then
    return 0
  end

  local prefix = string.sub(streamKey, 1, #streamKey - 6) -- strip "stream"
  local count = 0

  for i = 1, #entries do
    local entry = entries[i]
    local entryId = entry[1]
    local fields = entry[2]

    -- Extract jobId from stream entry fields
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

      -- Check if the job has a recent heartbeat (lastActive field).
      -- If (now - lastActive) < minIdleMs, the job is still being processed
      -- and should not be reclaimed.
      local lastActive = tonumber(redis.call('HGET', jobKey, 'lastActive'))
      if lastActive and (timestamp - lastActive) < minIdleMs then
        -- Job has a recent heartbeat - skip reclaiming.
        -- We need to reset the idle time on the PEL entry by re-claiming
        -- to the same consumer so it doesn't get picked up again next cycle.
        -- XCLAIM already happened via XAUTOCLAIM, so just skip the stall logic.
        count = count + 1
      else

      -- Track stall count on the job hash
      local stalledCount = redis.call('HINCRBY', jobKey, 'stalledCount', 1)

      if stalledCount > maxStalledCount then
        -- Exceeded max stalled count: move to failed
        redis.call('XACK', streamKey, group, entryId)
        redis.call('XDEL', streamKey, entryId)
        redis.call('ZADD', failedKey, timestamp, jobId)
        redis.call('HSET', jobKey,
          'state', 'failed',
          'failedReason', 'job stalled more than maxStalledCount',
          'finishedOn', tostring(timestamp)
        )
        emitEvent(eventsKey, 'failed', jobId, {
          'failedReason', 'job stalled more than maxStalledCount'
        })
      else
        -- Reclaimed: emit stalled event, job stays in PEL for the new consumer
        redis.call('HSET', jobKey, 'state', 'active')
        emitEvent(eventsKey, 'stalled', jobId, nil)
      end

      count = count + 1
      end -- close lastActive else
    end
  end

  return count
end)

-- glidemq_pause(KEYS, ARGS)
-- Pauses a queue by setting paused=1 in the meta hash.
-- KEYS[1] = meta hash key
-- KEYS[2] = events stream key
-- ARGS: (none)
-- Returns: 1
redis.register_function('glidemq_pause', function(keys, args)
  local metaKey = keys[1]
  local eventsKey = keys[2]

  redis.call('HSET', metaKey, 'paused', '1')
  emitEvent(eventsKey, 'paused', '0', nil)

  return 1
end)

-- glidemq_resume(KEYS, ARGS)
-- Resumes a queue by setting paused=0 in the meta hash.
-- KEYS[1] = meta hash key
-- KEYS[2] = events stream key
-- ARGS: (none)
-- Returns: 1
redis.register_function('glidemq_resume', function(keys, args)
  local metaKey = keys[1]
  local eventsKey = keys[2]

  redis.call('HSET', metaKey, 'paused', '0')
  emitEvent(eventsKey, 'resumed', '0', nil)

  return 1
end)

-- glidemq_dedup(KEYS, ARGS)
-- Deduplication: check dedup hash and either skip or add a new job.
-- KEYS[1] = dedup hash key
-- KEYS[2] = id counter key
-- KEYS[3] = stream key
-- KEYS[4] = scheduled zset key
-- KEYS[5] = events stream key
-- ARGS[1] = dedup_id
-- ARGS[2] = ttl_ms (0 = no TTL, used by throttle mode)
-- ARGS[3] = mode ("simple" | "throttle" | "debounce")
-- ARGS[4] = job name
-- ARGS[5] = job data (JSON string)
-- ARGS[6] = job opts (JSON string)
-- ARGS[7] = timestamp (ms)
-- ARGS[8] = delay (ms, 0 = no delay)
-- ARGS[9] = priority (0 = no priority)
-- ARGS[10] = parentId (empty string = no parent)
-- ARGS[11] = maxAttempts (0 = no retry)
-- Returns: "skipped" if deduplicated, otherwise the new job ID (string)
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

  local prefix = string.sub(idKey, 1, #idKey - 2) -- strip ":id"

  -- Check existing dedup entry: value is "jobId:timestamp"
  local existing = redis.call('HGET', dedupKey, dedupId)

  if mode == 'simple' then
    -- Simple: skip if dedup_id exists and job is not completed/failed
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
    -- Throttle: skip if (now - stored_timestamp) < ttl
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
    -- Debounce: if exists and job is delayed, remove old job and replace with new one
    if existing then
      local sep = string.find(existing, ':')
      if sep then
        local existingJobId = string.sub(existing, 1, sep - 1)
        local jobKey = prefix .. 'job:' .. existingJobId
        local state = redis.call('HGET', jobKey, 'state')
        if state == 'delayed' or state == 'prioritized' then
          -- Remove old job from scheduled ZSet and delete hash
          redis.call('ZREM', scheduledKey, existingJobId)
          redis.call('DEL', jobKey)
          emitEvent(eventsKey, 'removed', existingJobId, nil)
        elseif state and state ~= 'completed' and state ~= 'failed' then
          -- Job is waiting or active - skip (can't debounce non-delayed)
          return 'skipped'
        end
      end
    end
  end

  -- Add the new job (same logic as glidemq_addJob)
  local jobId = redis.call('INCR', idKey)
  local jobIdStr = tostring(jobId)
  local jobKey = prefix .. 'job:' .. jobIdStr

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

  -- Store dedup entry: field=dedup_id, value=jobId:timestamp
  redis.call('HSET', dedupKey, dedupId, jobIdStr .. ':' .. tostring(timestamp))

  emitEvent(eventsKey, 'added', jobIdStr, {'name', jobName})

  return jobIdStr
end)

-- glidemq_rateLimit(KEYS, ARGS)
-- Checks and enforces a sliding-window rate limit.
-- KEYS[1] = rate hash key (glide:{q}:rate)
-- KEYS[2] = meta key (glide:{q}:meta)
-- ARGS[1] = max per window
-- ARGS[2] = window duration (ms)
-- ARGS[3] = current timestamp (ms)
-- Returns: 0 if allowed, positive number = ms to wait before retrying
redis.register_function('glidemq_rateLimit', function(keys, args)
  local rateKey = keys[1]
  local metaKey = keys[2]

  local maxPerWindow = tonumber(args[1])
  local windowDuration = tonumber(args[2])
  local now = tonumber(args[3])

  local windowStart = tonumber(redis.call('HGET', rateKey, 'windowStart')) or 0
  local count = tonumber(redis.call('HGET', rateKey, 'count')) or 0

  -- If the current window has expired, reset
  if now - windowStart >= windowDuration then
    redis.call('HSET', rateKey, 'windowStart', tostring(now), 'count', '1')
    return 0
  end

  -- Window still active: check count
  if count >= maxPerWindow then
    -- Rate limited: return ms until window resets
    local delayMs = windowDuration - (now - windowStart)
    return delayMs
  end

  -- Allowed: increment count
  redis.call('HSET', rateKey, 'count', tostring(count + 1))
  return 0
end)

-- glidemq_checkConcurrency(KEYS, ARGS)
-- Checks global concurrency limit.
-- KEYS[1] = meta hash key
-- KEYS[2] = stream key
-- ARGS[1] = consumer group name
-- Returns: -1 if no limit set, 0 if blocked, or positive remaining capacity
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

-- glidemq_addFlow(KEYS, ARGS)
-- Atomically creates a parent job (waiting-children) and N child jobs.
-- KEYS[1] = parent id counter key (glide:{parentQueue}:id)
-- KEYS[2] = parent stream key
-- KEYS[3] = parent scheduled key
-- KEYS[4] = parent events key
-- Remaining KEYS are passed per child: 4 keys each (id, stream, scheduled, events)
-- ARGS layout:
--   ARGS[1] = parent job name
--   ARGS[2] = parent job data (JSON)
--   ARGS[3] = parent job opts (JSON)
--   ARGS[4] = timestamp (ms)
--   ARGS[5] = parent delay (ms)
--   ARGS[6] = parent priority
--   ARGS[7] = parent maxAttempts
--   ARGS[8] = number of children (N)
--   Then for each child (8 args each):
--     childName, childData, childOpts, childDelay, childPriority, childMaxAttempts, childQueuePrefix, childParentQueue
-- glidemq_moveToActive(KEYS, ARGS)
-- Single round trip: read job hash, check revoked, set state=active + processedOn + lastActive.
-- KEYS[1] = job hash key
-- ARGS[1] = timestamp
-- Returns: '' if not found, 'REVOKED' if revoked, cjson array of HGETALL fields otherwise
redis.register_function('glidemq_moveToActive', function(keys, args)
  local jobKey = keys[1]
  local timestamp = args[1]
  local exists = redis.call('EXISTS', jobKey)
  if exists == 0 then
    return ''
  end
  local revoked = redis.call('HGET', jobKey, 'revoked')
  if revoked == '1' then
    return 'REVOKED'
  end
  redis.call('HSET', jobKey, 'state', 'active', 'processedOn', timestamp, 'lastActive', timestamp)
  local fields = redis.call('HGETALL', jobKey)
  return cjson.encode(fields)
end)

-- Returns: cjson array [parentId, childId1, childId2, ...]
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

  -- Create parent job ID
  local parentJobId = redis.call('INCR', parentIdKey)
  local parentJobIdStr = tostring(parentJobId)

  -- Derive parent prefix from id key: "glide:{q}:id" -> "glide:{q}:"
  local parentPrefix = string.sub(parentIdKey, 1, #parentIdKey - 2)
  local parentJobKey = parentPrefix .. 'job:' .. parentJobIdStr
  local depsKey = parentPrefix .. 'deps:' .. parentJobIdStr

  -- Store parent job hash with state=waiting-children
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
  redis.call('HSET', parentJobKey, unpack(parentHash))

  -- Collect child IDs for result
  local childIds = {}

  -- Process each child
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

    -- Child keys: use child-specific keys from KEYS array
    local ckBase = childKeyOffset + (i - 1) * 4
    local childIdKey = keys[ckBase + 1]
    local childStreamKey = keys[ckBase + 2]
    local childScheduledKey = keys[ckBase + 3]
    local childEventsKey = keys[ckBase + 4]

    -- Create child job ID
    local childJobId = redis.call('INCR', childIdKey)
    local childJobIdStr = tostring(childJobId)

    -- Derive child prefix from child id key
    local childPrefix = string.sub(childIdKey, 1, #childIdKey - 2)
    local childJobKey = childPrefix .. 'job:' .. childJobIdStr

    -- Store child job hash with parentId and parentQueue
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

    if childDelay > 0 or childPriority > 0 then
      childHash[#childHash + 1] = 'state'
      childHash[#childHash + 1] = childDelay > 0 and 'delayed' or 'prioritized'
    else
      childHash[#childHash + 1] = 'state'
      childHash[#childHash + 1] = 'waiting'
    end

    redis.call('HSET', childJobKey, unpack(childHash))

    -- Add child to deps set (using queuePrefix:childId as identifier for cross-queue tracking)
    local depsMember = childQueuePrefix .. ':' .. childJobIdStr
    redis.call('SADD', depsKey, depsMember)

    -- Route child job
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

  -- Process extra deps members (pre-existing sub-flow children)
  local extraDepsOffset = childArgOffset + numChildren * 8
  local numExtraDeps = tonumber(args[extraDepsOffset + 1]) or 0
  for i = 1, numExtraDeps do
    local extraMember = args[extraDepsOffset + 1 + i]
    redis.call('SADD', depsKey, extraMember)
  end

  emitEvent(parentEventsKey, 'added', parentJobIdStr, {'name', parentName})

  -- Return JSON array: [parentId, child1Id, child2Id, ...]
  local result = {parentJobIdStr}
  for i = 1, #childIds do
    result[#result + 1] = childIds[i]
  end
  return cjson.encode(result)
end)

-- glidemq_completeChild(KEYS, ARGS)
-- Removes a child from the parent's deps set. If all children are done, re-queues the parent.
-- KEYS[1] = parent deps key (glide:{parentQueue}:deps:{parentId})
-- KEYS[2] = parent job hash key (glide:{parentQueue}:job:{parentId})
-- KEYS[3] = parent stream key (glide:{parentQueue}:stream)
-- KEYS[4] = parent events key (glide:{parentQueue}:events)
-- ARGS[1] = deps member string (childQueuePrefix:childId)
-- ARGS[2] = parent job ID
-- Returns: number of remaining children (0 = parent re-queued)
redis.register_function('glidemq_completeChild', function(keys, args)
  local depsKey = keys[1]
  local parentJobKey = keys[2]
  local parentStreamKey = keys[3]
  local parentEventsKey = keys[4]

  local depsMember = args[1]
  local parentId = args[2]

  -- Increment completed children counter on parent hash
  local doneCount = redis.call('HINCRBY', parentJobKey, 'depsCompleted', 1)

  -- Compare with total deps count
  local totalDeps = redis.call('SCARD', depsKey)
  local remaining = totalDeps - doneCount

  if remaining <= 0 then
    -- All children done: move parent to stream (re-queue)
    redis.call('HSET', parentJobKey, 'state', 'waiting')
    redis.call('XADD', parentStreamKey, '*', 'jobId', parentId)
    emitEvent(parentEventsKey, 'active', parentId, nil)
  end

  return remaining
end)

-- glidemq_removeJob(KEYS, ARGS)
-- Removes a job from all data structures.
-- KEYS[1] = job hash key (job:{id})
-- KEYS[2] = stream key
-- KEYS[3] = scheduled zset key
-- KEYS[4] = completed zset key
-- KEYS[5] = failed zset key
-- KEYS[6] = events stream key
-- ARGS[1] = job ID
-- Returns: 1 if removed, 0 if not found
redis.register_function('glidemq_removeJob', function(keys, args)
  local jobKey = keys[1]
  local streamKey = keys[2]
  local scheduledKey = keys[3]
  local completedKey = keys[4]
  local failedKey = keys[5]
  local eventsKey = keys[6]

  local jobId = args[1]

  -- Check if job exists
  local exists = redis.call('EXISTS', jobKey)
  if exists == 0 then
    return 0
  end

  -- Remove from scheduled, completed, failed ZSets
  redis.call('ZREM', scheduledKey, jobId)
  redis.call('ZREM', completedKey, jobId)
  redis.call('ZREM', failedKey, jobId)

  -- Delete the job hash
  redis.call('DEL', jobKey)

  -- Emit event
  emitEvent(eventsKey, 'removed', jobId, nil)

  return 1
end)
