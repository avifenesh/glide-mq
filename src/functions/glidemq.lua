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
-- KEYS[1] = stream key
-- KEYS[2] = completed zset key
-- KEYS[3] = events stream key
-- KEYS[4] = job hash key (job:{id})
-- ARGS[1] = job ID
-- ARGS[2] = stream entry ID (for XACK)
-- ARGS[3] = return value (JSON string)
-- ARGS[4] = timestamp (ms)
-- ARGS[5] = consumer group name
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
