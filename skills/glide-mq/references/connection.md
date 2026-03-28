# Connection Reference

## ConnectionOptions Interface

```typescript
interface ConnectionOptions {
  addresses: { host: string; port: number }[];  // ARRAY of address objects
  useTLS?: boolean;
  credentials?: PasswordCredentials | IamCredentials;
  clusterMode?: boolean;
  readFrom?: ReadFrom;
  clientAz?: string;
  inflightRequestsLimit?: number;  // default: 1000
  requestTimeout?: number;         // command timeout in ms, default: 500
}
```

## Basic Connection

```typescript
const connection = { addresses: [{ host: 'localhost', port: 6379 }] };
const queue = new Queue('tasks', { connection });
```

## TLS

```typescript
const connection = {
  addresses: [{ host: 'my-server.com', port: 6379 }],
  useTLS: true,
};
```

## Authentication

### Password-based

```typescript
interface PasswordCredentials {
  username?: string;
  password: string;
}

const connection = {
  addresses: [{ host: 'server.com', port: 6379 }],
  useTLS: true,
  credentials: { password: 'secret' },
};
```

### IAM (AWS ElastiCache / MemoryDB)

```typescript
interface IamCredentials {
  type: 'iam';
  serviceType: 'elasticache' | 'memorydb';
  region: string;         // e.g. 'us-east-1'
  userId: string;         // IAM user ID (maps to username in AUTH)
  clusterName: string;
  refreshIntervalSeconds?: number;  // default: 300 (5 min)
}

const connection = {
  addresses: [{ host: 'my-cluster.cache.amazonaws.com', port: 6379 }],
  clusterMode: true,
  credentials: {
    type: 'iam',
    serviceType: 'elasticache',
    region: 'us-east-1',
    userId: 'my-iam-user',
    clusterName: 'my-cluster',
  },
};
```

## Cluster Mode

```typescript
const connection = {
  addresses: [
    { host: 'node1', port: 7000 },
    { host: 'node2', port: 7001 },
  ],
  clusterMode: true,
};
```

Keys are hash-tagged automatically (`glide:{queueName}:*`) for cluster compatibility.

## Read Strategies

```typescript
const connection = {
  addresses: [{ host: 'cluster.cache.amazonaws.com', port: 6379 }],
  clusterMode: true,
  readFrom: 'AZAffinity',
  clientAz: 'us-east-1a',
};
```

| `readFrom` value | Behavior |
|------------------|----------|
| `'primary'` | Always read from primary (default) |
| `'preferReplica'` | Round-robin across replicas, fallback to primary |
| `'AZAffinity'` | Route reads to replicas in same AZ |
| `'AZAffinityReplicasAndPrimary'` | Route reads to any node in same AZ |

AZ-based strategies require `clientAz` to be set.

## Shared Client Pattern

By default each component creates its own GLIDE client. You can inject a shared client to reduce connections.

```typescript
import { GlideClient } from '@glidemq/speedkey';

const client = await GlideClient.createClient({ addresses: [{ host: 'localhost' }] });

const queue  = new Queue('jobs', { client });           // borrows client
const flow   = new FlowProducer({ client });            // borrows client
const worker = new Worker('jobs', handler, {
  connection,         // REQUIRED - blocking client auto-created
  commandClient: client,  // shared client for non-blocking ops
});
const events = new QueueEvents('jobs', { connection }); // always own connection
// Total: 2 TCP connections (shared + worker's blocking client)
```

### What can share

Queue, FlowProducer, Worker's command client - all non-blocking operations.
GLIDE multiplexes up to 1000 in-flight requests over one TCP connection.

### What cannot share

- Worker's blocking client (`XREADGROUP BLOCK`) - always auto-created
- QueueEvents (`XREAD BLOCK`) - always own connection. Throws if you pass `client`.

### Close order

```typescript
// Close components first, then shared client
await queue.close();    // detaches (does not close shared client)
await worker.close();   // closes only auto-created blocking client
await flow.close();
client.close();         // now safe
```

### inflightRequestsLimit

Default 1000. At Worker concurrency=50, peak inflight is ~55 commands.

```typescript
const connection = {
  addresses: [{ host: 'localhost' }],
  inflightRequestsLimit: 2000,
};
```

### requestTimeout

Command timeout in milliseconds. Default: 500. Commands exceeding this throw a `TimeoutError`. Increase for operations that may take longer (e.g. `FT.CREATE` with many existing keys, `FUNCTION LOAD` with large libraries).

```typescript
const connection = {
  addresses: [{ host: 'localhost', port: 6379 }],
  requestTimeout: 2000,  // 2 seconds
};
```

## Valkey Modules (Search / JSON / Bloom)

Vector search (`queue.createJobIndex()`, `queue.vectorSearch()`) requires the `valkey-search` module loaded on the server. The easiest way to get all modules is to use `valkey-bundle`, which bundles search, JSON, bloom, and other modules:

```bash
# Docker (standalone with all modules)
docker run -p 6379:6379 valkey/valkey-bundle:latest

# Or load the search module explicitly
valkey-server --loadmodule /path/to/valkeysearch.so
```

Vector search is supported in standalone mode only (not cluster mode) due to Valkey Search module limitations.

## Gotchas

- `addresses` is an **array** of `{ host, port }` objects, not a single host/port.
- Worker always requires `connection` even when `commandClient` is provided.
- `commandClient` and `client` are aliases on Worker - use one, not both.
- Don't close shared client while components are alive.
- QueueEvents cannot accept an injected `client` - throws.
- Don't mutate shared client state externally (e.g., `SELECT`).
