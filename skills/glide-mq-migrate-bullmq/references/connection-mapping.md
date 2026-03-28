# Connection config mapping: BullMQ to glide-mq

BullMQ uses ioredis's flat connection format. glide-mq uses valkey-glide's structured format with an `addresses` array. This is the most common source of migration errors.

---

## Basic (standalone)

```ts
// BullMQ
const connection = { host: 'localhost', port: 6379 };
```

```ts
// glide-mq
const connection = { addresses: [{ host: 'localhost', port: 6379 }] };
```

---

## TLS

```ts
// BullMQ
const connection = {
  host: 'my-server.example.com',
  port: 6380,
  tls: {},
};
```

```ts
// glide-mq
const connection = {
  addresses: [{ host: 'my-server.example.com', port: 6380 }],
  useTLS: true,
};
```

Note: BullMQ uses an empty `tls: {}` object (or with TLS options). glide-mq uses a boolean `useTLS: true`.

---

## Password authentication

```ts
// BullMQ
const connection = {
  host: 'my-server.example.com',
  port: 6379,
  password: 'secret',
};
```

```ts
// glide-mq
const connection = {
  addresses: [{ host: 'my-server.example.com', port: 6379 }],
  credentials: { password: 'secret' },
};
```

---

## Username + password (ACL auth)

```ts
// BullMQ
const connection = {
  host: 'my-server.example.com',
  port: 6379,
  username: 'myuser',
  password: 'secret',
};
```

```ts
// glide-mq
const connection = {
  addresses: [{ host: 'my-server.example.com', port: 6379 }],
  credentials: { username: 'myuser', password: 'secret' },
};
```

---

## TLS + password + cluster

```ts
// BullMQ
const connection = {
  host: 'my-cluster.cache.amazonaws.com',
  port: 6379,
  tls: {},
  password: 'secret',
};
// BullMQ auto-detects cluster mode in some configurations, or you use natMap
```

```ts
// glide-mq
const connection = {
  addresses: [{ host: 'my-cluster.cache.amazonaws.com', port: 6379 }],
  useTLS: true,
  credentials: { password: 'secret' },
  clusterMode: true,
};
```

Key difference: glide-mq requires explicit `clusterMode: true` for Redis Cluster / ElastiCache cluster / MemoryDB.

---

## IAM authentication (AWS ElastiCache / MemoryDB)

BullMQ has no equivalent. This is glide-mq only.

```ts
// glide-mq only
const connection = {
  addresses: [{ host: 'my-cluster.cache.amazonaws.com', port: 6379 }],
  useTLS: true,
  clusterMode: true,
  credentials: {
    type: 'iam',
    serviceType: 'elasticache',   // or 'memorydb'
    region: 'us-east-1',
    userId: 'my-iam-user',
    clusterName: 'my-cluster',
  },
};
```

No credential rotation needed - the client handles IAM token refresh automatically.

---

## AZ-affinity routing (cluster only)

BullMQ has no equivalent. Reduces cross-AZ network cost and latency.

```ts
// glide-mq only
const connection = {
  addresses: [{ host: 'cluster.cache.amazonaws.com', port: 6379 }],
  clusterMode: true,
  useTLS: true,
  readFrom: 'AZAffinity',
  clientAz: 'us-east-1a',
};
```

---

## Multiple seed nodes (cluster)

```ts
// BullMQ - typically one host, or uses natMap for discovery
const connection = { host: 'node-1.example.com', port: 6379 };
```

```ts
// glide-mq - pass multiple seed addresses for cluster discovery
const connection = {
  addresses: [
    { host: 'node-1.example.com', port: 6379 },
    { host: 'node-2.example.com', port: 6379 },
    { host: 'node-3.example.com', port: 6379 },
  ],
  clusterMode: true,
};
```

---

## Option mapping table

| BullMQ (ioredis) | glide-mq (valkey-glide) | Notes |
|-------------------|-------------------------|-------|
| `host` | `addresses: [{ host }]` | Wrapped in array of address objects |
| `port` | `addresses: [{ port }]` | Part of address object |
| `password` | `credentials: { password }` | Nested under credentials |
| `username` | `credentials: { username }` | Nested under credentials |
| `tls: {}` | `useTLS: true` | Boolean instead of object |
| `db` | Not supported - Valkey GLIDE uses db 0 | Database selection not available |
| `natMap` | Multiple entries in `addresses` | Cluster topology handled automatically |
| `maxRetriesPerRequest` | Handled internally | valkey-glide manages reconnection |
| `enableReadyCheck` | Not needed | valkey-glide handles readiness internally |
| `lazyConnect` | Not applicable | Connection is managed by the client |
| - | `clusterMode: true` | Must be explicit for cluster deployments |
| - | `readFrom: 'AZAffinity'` | glide-mq only |
| - | `clientAz` | glide-mq only |
| - | `credentials: { type: 'iam' }` | glide-mq only |
| - | `requestTimeout` | Command timeout in ms (default: 500). glide-mq only |

---

## Common mistakes

1. **Forgetting the array wrapper**: `{ addresses: { host, port } }` will fail. It must be `{ addresses: [{ host, port }] }` - note the square brackets.

2. **Using `tls: {}` instead of `useTLS: true`**: valkey-glide does not accept a TLS options object. Pass the boolean flag.

3. **Omitting `clusterMode: true`**: Unlike ioredis which can auto-detect cluster mode, valkey-glide requires you to explicitly opt in.

4. **Using `password` at top level**: Must be `credentials: { password }`, not `password` directly.
