# glide-mq Wire Protocol

This document specifies the exact Valkey commands and data formats needed to enqueue, query, and control glide-mq jobs from any language. All operations use standard Valkey commands - no Node.js dependency required.

## Prerequisites

### 1. Load the Function Library

glide-mq uses a single Valkey Server Function library (`glidemq`) loaded via `FUNCTION LOAD`. The library must be loaded before any FCALL commands will work.

From the Node.js side, `Queue` and `Worker` constructors handle this automatically. For non-Node producers you have two options:

- **Option A**: Start a Node.js process once to initialize the library, then use FCALL from any language.
- **Option B**: Extract the Lua source from `src/functions/index.ts` (the `LIBRARY_SOURCE` constant) and issue `FUNCTION LOAD` yourself.

In cluster mode, `FUNCTION LOAD` must be sent to all primary nodes.

### 2. Verify Library Version

```
FCALL glidemq_version 1 {glidemq}:_
```

Returns the library version as a string (e.g. `"40"`). The dummy key `{glidemq}:_` is required for cluster slot routing.

---

## Key Layout

All keys share a hash tag `{queueName}` to ensure cluster slot co-location. Default prefix is `glide`.

| Key | Type | Description |
|-----|------|-------------|
| `glide:{queueName}:id` | String | Auto-increment job ID counter |
| `glide:{queueName}:stream` | Stream | Ready jobs (primary queue) |
| `glide:{queueName}:scheduled` | Sorted Set | Delayed + prioritized staging area |
| `glide:{queueName}:completed` | Sorted Set | Completed jobs (score = timestamp) |
| `glide:{queueName}:failed` | Sorted Set | Failed jobs (score = timestamp) |
| `glide:{queueName}:events` | Stream | Lifecycle events (capped ~1000) |
| `glide:{queueName}:meta` | Hash | Queue metadata (paused flag, concurrency, rate limit) |
| `glide:{queueName}:dedup` | Hash | Deduplication entries (field=dedup_id, value=jobId:timestamp) |
| `glide:{queueName}:job:{id}` | Hash | Individual job data |
| `glide:{queueName}:log:{id}` | List | Per-job log entries |
| `glide:{queueName}:deps:{id}` | Set | Child job IDs for parent (flows) |
| `glide:{queueName}:ordering` | Hash | Per-key sequence counters |
| `glide:{queueName}:group:{key}` | Hash | Group state (concurrency, rate limit, token bucket) |
| `glide:{queueName}:groupq:{key}` | List | FIFO wait list for group-limited jobs |
| `glide:{queueName}:ratelimited` | Sorted Set | Rate-limited group promotion queue |
| `glide:{queueName}:schedulers` | Hash | Job scheduler configs |

---

## Job Hash Fields

Each job is stored as a hash at `glide:{queueName}:job:{id}` with these fields:

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Job ID |
| `name` | string | Job name |
| `data` | string | JSON-serialized (or compressed) payload |
| `opts` | string | JSON-serialized JobOptions |
| `timestamp` | string (int) | Enqueue timestamp in ms |
| `attemptsMade` | string (int) | Number of attempts made |
| `delay` | string (int) | Delay in ms |
| `priority` | string (int) | Priority (0 = highest) |
| `maxAttempts` | string (int) | Maximum retry attempts |
| `state` | string | `waiting`, `active`, `delayed`, `prioritized`, `completed`, `failed`, `waiting-children`, `group-waiting` |
| `returnvalue` | string | JSON-serialized return value (set on completion) |
| `failedReason` | string | Error message (set on failure) |
| `finishedOn` | string (int) | Completion/failure timestamp |
| `processedOn` | string (int) | Start-of-processing timestamp |
| `progress` | string | Progress (number or JSON object) |
| `parentId` | string | Parent job ID (flows) |
| `parentQueue` | string | Parent queue prefix (flows) |
| `orderingKey` | string | Ordering key (sequential mode) |
| `orderingSeq` | string (int) | Ordering sequence number |
| `groupKey` | string | Group key (group concurrency mode) |
| `cost` | string (int) | Token cost in millitokens |
| `expireAt` | string (int) | TTL deadline (timestamp + ttl) |
| `revoked` | string | `"1"` if revoked |

---

## FCALL glidemq_addJob

Atomically creates a job hash and enqueues it to the stream (or scheduled ZSet if delayed/prioritized).

### Keys (4)

| Position | Key |
|----------|-----|
| 1 | `glide:{queueName}:id` |
| 2 | `glide:{queueName}:stream` |
| 3 | `glide:{queueName}:scheduled` |
| 4 | `glide:{queueName}:events` |

### Args (17)

| Position | Name | Type | Description |
|----------|------|------|-------------|
| 1 | jobName | string | Job name |
| 2 | jobData | string | JSON-serialized job data (or `gz:` + base64(gzip(data)) if compressed) |
| 3 | jobOpts | string | JSON-serialized options object |
| 4 | timestamp | string (int) | Current time in ms (e.g. `Date.now()`) |
| 5 | delay | string (int) | Delay in ms, `"0"` for immediate |
| 6 | priority | string (int) | Priority, `"0"` for default (highest) |
| 7 | parentId | string | Parent job ID, `""` if none |
| 8 | maxAttempts | string (int) | Max retry attempts, `"0"` for no retries |
| 9 | orderingKey | string | Ordering key, `""` if none |
| 10 | groupConcurrency | string (int) | Group concurrency, `"0"` if none |
| 11 | groupRateMax | string (int) | Group rate limit max, `"0"` if none |
| 12 | groupRateDuration | string (int) | Group rate limit duration in ms, `"0"` if none |
| 13 | tbCapacity | string (int) | Token bucket capacity in millitokens, `"0"` if none |
| 14 | tbRefillRate | string (int) | Token bucket refill rate in millitokens/s, `"0"` if none |
| 15 | jobCost | string (int) | Job cost in millitokens, `"0"` for default (1000 = 1 token) |
| 16 | ttl | string (int) | Time-to-live in ms, `"0"` for no expiry |
| 17 | customJobId | string | Custom job ID, `""` for auto-generated |

### Return Values

| Value | Meaning |
|-------|---------|
| `"{jobId}"` | Numeric or custom job ID string |
| `"duplicate"` | Custom job ID already exists (silent skip) |
| `"ERR:COST_EXCEEDS_CAPACITY"` | Job cost exceeds token bucket capacity |
| `"ERR:ID_EXHAUSTED"` | Too many ID collisions |

### Behavior

- If `delay > 0`: job goes to scheduled ZSet with score `priority * 2^42 + (timestamp + delay)`, state = `delayed`
- If `priority > 0` and `delay == 0`: job goes to scheduled ZSet with score `priority * 2^42`, state = `prioritized`
- Otherwise: job goes to stream via `XADD`, state = `waiting`
- An `added` event is emitted on the events stream

### Example (redis-cli)

```
FCALL glidemq_addJob 4
  glide:{myqueue}:id
  glide:{myqueue}:stream
  glide:{myqueue}:scheduled
  glide:{myqueue}:events
  "send-email"
  "{\"to\":\"user@example.com\"}"
  "{}"
  "1709750400000"
  "0"
  "0"
  ""
  "0"
  ""
  "0"
  "0"
  "0"
  "0"
  "0"
  "0"
  "0"
  ""
```

---

## FCALL glidemq_dedup

Adds a job with deduplication. Checks the dedup hash first and either skips or creates the job.

### Keys (5)

| Position | Key |
|----------|-----|
| 1 | `glide:{queueName}:dedup` |
| 2 | `glide:{queueName}:id` |
| 3 | `glide:{queueName}:stream` |
| 4 | `glide:{queueName}:scheduled` |
| 5 | `glide:{queueName}:events` |

### Args (20)

| Position | Name | Type | Description |
|----------|------|------|-------------|
| 1 | dedupId | string | Deduplication identifier |
| 2 | ttlMs | string (int) | TTL for throttle mode in ms, `"0"` if not used |
| 3 | mode | string | `"simple"`, `"throttle"`, or `"debounce"` |
| 4-20 | (same as addJob args 1-17) | | Same 17 args as glidemq_addJob |

### Return Values

Same as `glidemq_addJob`, plus:

| Value | Meaning |
|-------|---------|
| `"skipped"` | Deduplicated - job was not created |

### Deduplication Modes

- **simple**: Skip if a non-terminal job with the same dedup ID exists
- **throttle**: Skip if the last job with the same dedup ID was created within `ttlMs`
- **debounce**: Cancel the previous delayed/prioritized job with the same dedup ID, then create a new one

---

## FCALL glidemq_addFlow

Atomically creates a parent job and all child jobs. The parent starts in `waiting-children` state and is re-queued when all children complete.

### Keys (4 + 4 per child)

| Position | Key |
|----------|-----|
| 1 | `glide:{parentQueue}:id` |
| 2 | `glide:{parentQueue}:stream` |
| 3 | `glide:{parentQueue}:scheduled` |
| 4 | `glide:{parentQueue}:events` |
| 4+(i-1)*4+1 | `glide:{childQueue_i}:id` |
| 4+(i-1)*4+2 | `glide:{childQueue_i}:stream` |
| 4+(i-1)*4+3 | `glide:{childQueue_i}:scheduled` |
| 4+(i-1)*4+4 | `glide:{childQueue_i}:events` |

### Args (9 parent + 9 per child + extra deps)

**Parent args (positions 1-9):**

| Position | Name | Type |
|----------|------|------|
| 1 | parentName | string |
| 2 | parentData | string (JSON) |
| 3 | parentOpts | string (JSON) |
| 4 | timestamp | string (int) |
| 5 | parentDelay | string (int) |
| 6 | parentPriority | string (int) |
| 7 | parentMaxAttempts | string (int) |
| 8 | numChildren | string (int) |
| 9 | parentCustomId | string |

**Child args (9 per child, starting at position 10):**

For child `i` (1-based), base = `9 + (i-1) * 9`:

| Offset | Name | Type |
|--------|------|------|
| base+1 | childName | string |
| base+2 | childData | string (JSON) |
| base+3 | childOpts | string (JSON) |
| base+4 | childDelay | string (int) |
| base+5 | childPriority | string (int) |
| base+6 | childMaxAttempts | string (int) |
| base+7 | childQueuePrefix | string |
| base+8 | childParentQueue | string |
| base+9 | childCustomId | string |

**Extra deps (after all children):**

| Offset | Name |
|--------|------|
| 9 + numChildren*9 + 1 | numExtraDeps (string int) |
| 9 + numChildren*9 + 2..N | extraDepsMember (string) |

### Return Value

JSON-encoded array: `["parentId", "child1Id", "child2Id", ...]`

Returns `["duplicate"]` if any custom job ID already exists, or `"ERR:COST_EXCEEDS_CAPACITY"` if a cost exceeds bucket capacity.

---

## FCALL glidemq_pause / glidemq_resume

### Keys (2)

| Position | Key |
|----------|-----|
| 1 | `glide:{queueName}:meta` |
| 2 | `glide:{queueName}:events` |

### Args

None.

### Example

```
FCALL glidemq_pause 2 glide:{myqueue}:meta glide:{myqueue}:events
FCALL glidemq_resume 2 glide:{myqueue}:meta glide:{myqueue}:events
```

---

## Priority Encoding

Jobs with delay or priority use a composite score in the scheduled ZSet:

```
score = priority * 2^42 + timestamp_ms
```

Where `2^42 = 4398046511104`.

- Priority 0 is highest. A priority-0 delayed job uses score `0 + (timestamp + delay)`.
- Priority 1 uses score `4398046511104 + timestamp_ms`.
- Within the same priority, FIFO by timestamp.
- Non-delayed priority jobs use score `priority * 2^42 + 0` (timestamp = 0) so they promote immediately.

---

## Compression

glide-mq supports transparent gzip compression of job data.

**Format**: `gz:` + base64(gzip(data))

The `data` field in the job hash is stored as:
- Plain JSON string when compression is off
- `gz:AAAB3...` prefixed base64 when compression is on

Maximum payload size: **1 MB** (1,048,576 bytes) - enforced before compression.

Any language reading job data must check for the `gz:` prefix and decompress if present.

---

## Custom Job IDs

Custom job IDs allow deterministic identity for idempotent producers.

**Validation rules:**
- Maximum 256 characters
- Must not contain control characters (0x00-0x1F, 0x7F)
- Must not contain curly braces (`{`, `}`)
- Must not contain colons (`:`)

When a custom job ID is provided and a job with that ID already exists, `glidemq_addJob` returns `"duplicate"` (silent skip).

---

## Reading Job State

### Get a single job

```
HGETALL glide:{queueName}:job:{id}
```

### Get job counts

```
XLEN glide:{queueName}:stream          -- waiting + active
ZCARD glide:{queueName}:completed      -- completed
ZCARD glide:{queueName}:failed         -- failed
ZCARD glide:{queueName}:scheduled      -- delayed + prioritized
XPENDING glide:{queueName}:stream workers  -- active count is first element
```

Waiting count = `XLEN(stream) - activeCount`.

### Check if queue is paused

```
HGET glide:{queueName}:meta paused
```

Returns `"1"` if paused, `"0"` or nil if not.

---

## Consumer Group

Workers use a single consumer group named `workers`.

To create the group (idempotent):

```
XGROUP CREATE glide:{queueName}:stream workers $ MKSTREAM
```

Workers consume via:

```
XREADGROUP GROUP workers worker-{uuid} COUNT {prefetch} BLOCK {timeout} STREAMS glide:{queueName}:stream >
```

---

## Token Bucket and Cost

Token bucket values are stored in **millitokens** (1 token = 1000 millitokens) for integer precision.

When setting `tbCapacity` and `tbRefillRate` in FCALL args:
- Multiply the user-facing value by 1000: `capacity=5` becomes `"5000"`, `refillRate=2.5` becomes `"2500"`
- Job cost follows the same convention: `cost=1` becomes `"1000"`, default cost is `1000` (1 token)

---

## Python Example

Using `redis-py`:

```python
import redis
import json
import time

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

queue_name = 'tasks'
prefix = 'glide'
p = f'{prefix}:{{{queue_name}}}'

# Add a simple job
job_data = json.dumps({'to': 'user@example.com', 'subject': 'Hello'})
job_opts = json.dumps({})
timestamp = str(int(time.time() * 1000))

result = r.fcall(
    'glidemq_addJob',
    4,                              # numkeys
    f'{p}:id',                      # key 1: id counter
    f'{p}:stream',                  # key 2: stream
    f'{p}:scheduled',               # key 3: scheduled ZSet
    f'{p}:events',                  # key 4: events stream
    'send-email',                   # arg 1:  jobName
    job_data,                       # arg 2:  jobData
    job_opts,                       # arg 3:  jobOpts
    timestamp,                      # arg 4:  timestamp
    '0',                            # arg 5:  delay
    '0',                            # arg 6:  priority
    '',                             # arg 7:  parentId
    '0',                            # arg 8:  maxAttempts
    '',                             # arg 9:  orderingKey
    '0',                            # arg 10: groupConcurrency
    '0',                            # arg 11: groupRateMax
    '0',                            # arg 12: groupRateDuration
    '0',                            # arg 13: tbCapacity
    '0',                            # arg 14: tbRefillRate
    '0',                            # arg 15: jobCost
    '0',                            # arg 16: ttl
    '',                             # arg 17: customJobId
)

print(f'Job created with ID: {result}')

# Read the job back
job_hash = r.hgetall(f'{p}:job:{result}')
print(f'Job data: {job_hash}')

# Get job counts
stream_len = r.xlen(f'{p}:stream')
completed = r.zcard(f'{p}:completed')
failed = r.zcard(f'{p}:failed')
delayed = r.zcard(f'{p}:scheduled')
print(f'Waiting: {stream_len}, Completed: {completed}, Failed: {failed}, Delayed: {delayed}')
```

---

## Go Example

Using `go-valkey`:

```go
package main

import (
    "context"
    "fmt"
    "strconv"
    "time"

    "github.com/valkey-io/valkey-go"
)

func main() {
    client, err := valkey.NewClient(valkey.ClientOption{
        InitAddress: []string{"localhost:6379"},
    })
    if err != nil {
        panic(err)
    }
    defer client.Close()

    ctx := context.Background()
    queueName := "tasks"
    prefix := "glide"
    p := fmt.Sprintf("%s:{%s}", prefix, queueName)

    timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
    jobData := `{"to":"user@example.com","subject":"Hello"}`
    jobOpts := `{}`

    result := client.Do(ctx, client.B().Fcall().
        Function("glidemq_addJob").
        Numkeys(4).
        Key(p+":id").
        Key(p+":stream").
        Key(p+":scheduled").
        Key(p+":events").
        Arg("send-email").    // jobName
        Arg(jobData).         // jobData
        Arg(jobOpts).         // jobOpts
        Arg(timestamp).       // timestamp
        Arg("0").             // delay
        Arg("0").             // priority
        Arg("").              // parentId
        Arg("0").             // maxAttempts
        Arg("").              // orderingKey
        Arg("0").             // groupConcurrency
        Arg("0").             // groupRateMax
        Arg("0").             // groupRateDuration
        Arg("0").             // tbCapacity
        Arg("0").             // tbRefillRate
        Arg("0").             // jobCost
        Arg("0").             // ttl
        Arg("").              // customJobId
        Build(),
    )

    jobId, err := result.ToString()
    if err != nil {
        panic(err)
    }
    fmt.Printf("Job created with ID: %s\n", jobId)
}
```

---

## Authentication Note

The HTTP proxy (see `glide-mq/proxy`) does not include built-in authentication. When exposing the proxy to a network, add your own auth middleware (JWT, API keys, etc.) before the proxy routes.
