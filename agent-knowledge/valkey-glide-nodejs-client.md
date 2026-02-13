# Learning Guide: Valkey GLIDE Node.js Client

**Generated**: 2026-02-13
**Sources**: 40 resources analyzed
**Depth**: deep

---

## Prerequisites

- Working knowledge of Node.js (v20+) and TypeScript
- Familiarity with Redis/Valkey data structures and commands
- Understanding of client-server architecture and TCP connections
- Basic understanding of clustering concepts (hash slots, sharding)
- npm or Yarn package manager installed

## TL;DR

- **Valkey GLIDE** (General Language Independent Driver for the Enterprise) is the official open-source Valkey client library with a **Rust core** and language-specific bindings for Node.js, Python, Java, and Go.
- The Node.js client provides `GlideClient` (standalone) and `GlideClusterClient` (cluster) with full TypeScript support, 200+ commands, and automatic topology management.
- Architecture uses a **single multiplexed connection per node** with protobuf-based communication between Node.js and Rust, delivering low-latency pipelining without connection pools.
- Key differentiators vs ioredis/node-redis: **AZ-affinity routing**, **automatic PubSub reconnection**, **OpenTelemetry integration**, **Rust-powered performance**, and **consistent cross-language behavior**.
- Production-ready (Apache 2.0 license), backed by AWS and GCP, with IAM authentication support for ElastiCache/MemoryDB.

---

## Table of Contents

1. [What is Valkey GLIDE](#1-what-is-valkey-glide)
2. [Architecture: Rust Core + Node.js Bindings](#2-architecture-rust-core--nodejs-bindings)
3. [Installation and Setup](#3-installation-and-setup)
4. [Client Types: Standalone vs Cluster](#4-client-types-standalone-vs-cluster)
5. [Configuration Reference](#5-configuration-reference)
6. [API Surface and Command Coverage](#6-api-surface-and-command-coverage)
7. [TypeScript Support and Type Safety](#7-typescript-support-and-type-safety)
8. [Connection Management](#8-connection-management)
9. [Batching: Transactions and Pipelines](#9-batching-transactions-and-pipelines)
10. [Pub/Sub Support](#10-pubsub-support)
11. [Cluster Features](#11-cluster-features)
12. [Authentication and Security](#12-authentication-and-security)
13. [Error Handling and Retry Strategies](#13-error-handling-and-retry-strategies)
14. [Observability: OpenTelemetry and Logging](#14-observability-opentelemetry-and-logging)
15. [Server Modules: JSON and Search](#15-server-modules-json-and-search)
16. [Performance Characteristics](#16-performance-characteristics)
17. [Comparison with ioredis and node-redis](#17-comparison-with-ioredis-and-node-redis)
18. [Migration from ioredis](#18-migration-from-ioredis)
19. [Valkey-Specific Features vs Redis Compatibility](#19-valkey-specific-features-vs-redis-compatibility)
20. [Production Readiness and Known Limitations](#20-production-readiness-and-known-limitations)
21. [GLIDE's Relationship to AWS and the Valkey Community](#21-glides-relationship-to-aws-and-the-valkey-community)
22. [Common Pitfalls](#22-common-pitfalls)
23. [Best Practices](#23-best-practices)
24. [Further Reading](#24-further-reading)

---

## 1. What is Valkey GLIDE

Valkey GLIDE (General Language Independent Driver for the Enterprise) is the **official open-source client library** for the Valkey datastore, maintained as part of the Valkey organization. It provides a unified driver framework built on a high-performance Rust core with language-specific extensions.

### Design Goals

1. **Reliability**: Built on over a decade of experience operating Redis OSS-compatible services at scale
2. **Performance**: Optimized for low latency through multiplexed connections and Rust-powered I/O
3. **High Availability**: Automatic topology tracking, reconnection with exponential backoff, and PubSub auto-resubscription
4. **Consistency**: Identical behavior across all supported languages (Node.js, Python, Java, Go)
5. **Enterprise-Ready**: IAM authentication, TLS/mTLS, OpenTelemetry integration, and AZ-affinity routing

### Supported Languages

| Language | Status |
|----------|--------|
| Node.js | Production (GA) |
| Python | Production (GA) |
| Java | Production (GA) |
| Go | Production (GA) |
| C# | Coming soon |
| C++ | Under development |

### Engine Compatibility

| Engine | Supported Versions |
|--------|-------------------|
| Valkey | 7.2, 8.0, 8.1, 9.0 |
| Redis OSS | 6.2, 7.0, 7.2 |

(Source: GitHub valkey-io/valkey-glide README, glide.valkey.io)

---

## 2. Architecture: Rust Core + Node.js Bindings

GLIDE's architecture is a layered system where the Rust core handles all networking, protocol parsing, and cluster management, while language-specific wrappers provide idiomatic APIs.

### Layer Diagram

```
+----------------------------------+
|  Node.js Application (TS/JS)    |
+----------------------------------+
|  TypeScript API Layer            |
|  (GlideClient, GlideCluster...) |
+----------------------------------+
|  Protobuf Serialization          |
|  (CommandRequest / Response)     |
+----------------------------------+
|  NAPI-RS Bridge                  |
|  (Rust <-> Node.js FFI)         |
+----------------------------------+
|  Rust Core (glide-core)          |
|  - Connection Management         |
|  - Command Routing               |
|  - Cluster Topology              |
|  - Retry Logic                   |
|  - PubSub Synchronization        |
+----------------------------------+
|  redis-rs (forked)               |
|  - RESP Protocol                 |
|  - TLS (rustls)                  |
|  - Multiplexed Connections       |
+----------------------------------+
|  Valkey/Redis Server             |
+----------------------------------+
```

### Rust Core (glide-core)

The Rust core is the heart of GLIDE. Key modules include:

- **client**: Connection lifecycle, lazy initialization, `ClientWrapper` enum (Standalone/Cluster/Lazy)
- **pubsub**: Subscription state tracking, auto-resubscription on reconnect
- **request_type**: All 380+ command variants as a Rust enum
- **iam**: AWS IAM token generation and refresh for ElastiCache/MemoryDB
- **reconnecting_connection**: Infinite backoff with PING health checks

Key Rust dependencies:
- **tokio** v1: Async runtime
- **redis-rs** (forked, local): Redis protocol, cluster, TLS via rustls
- **protobuf** v3: Node.js <-> Rust communication
- **tokio-retry2**: Retry logic with jitter
- **zstd** / **lz4**: Compression support
- **aws-sigv4**: IAM authentication

### Node.js Bindings (NAPI-RS)

The Node.js wrapper uses **napi-rs** to create native bindings:

- `#[napi]` procedural macros expose Rust functions as JavaScript-callable methods
- A dedicated **Tokio runtime** runs in a separate thread, with `runtime.block_on()` converting async Rust to Promise-based JavaScript
- **Pointer splitting**: Since JavaScript Numbers cannot represent full 64-bit pointers, the code splits them into high/low 32-bit segments for FFI
- Deferred promises bridge async execution between the Tokio runtime and Node.js event loop

### Communication Protocol

Node.js and Rust communicate via **Protocol Buffers**:
- Commands are serialized as `CommandRequest` protobuf messages
- Responses are deserialized back in `handleReadData()`
- Socket-based communication uses buffered protobuf message encoding with callback slot management for concurrent requests
- The package uses `protobufjs/minimal` (~6.5KB gzipped) rather than the full library

(Source: GitHub node/rust-client/src/lib.rs, glide-core/src/lib.rs, glide-core/Cargo.toml)

---

## 3. Installation and Setup

### Installation

```bash
npm install @valkey/valkey-glide
```

### System Requirements

| Requirement | Details |
|-------------|---------|
| Node.js | v20 or higher (v16/v18 support dropped in GLIDE 2.3) |
| npm | v11+ recommended on Linux (supports optional downloads based on libc) |
| Linux | glibc 2.17+ or musl libc 1.2.3+ (x86_64, arm64) |
| macOS | Darwin (x86_64, arm64 / Apple Silicon) |
| Windows | Not supported for Node.js client |
| Alpine Linux | Supported via musl libc 1.2.3+ |

### Quick Start

```typescript
import { GlideClient, GlideClusterClient } from "@valkey/valkey-glide";

// Standalone mode
const client = await GlideClient.createClient({
  addresses: [{ host: "localhost", port: 6379 }],
});

await client.set("hello", "world");
const value = await client.get("hello"); // "world"

client.close();
```

```typescript
// Cluster mode
const clusterClient = await GlideClusterClient.createClient({
  addresses: [{ host: "cluster-node-1", port: 7000 }],
});

await clusterClient.set("foo", "bar");
const result = await clusterClient.get("foo"); // "bar"

clusterClient.close();
```

### Package Details

- **Package name**: `@valkey/valkey-glide`
- **License**: Apache-2.0
- **Entry point**: `build-ts/index.js`
- **Types**: `build-ts/index.d.ts`
- **Dependencies**: `protobufjs` v7, `long` v5
- **Module format**: CommonJS (primary), with ESNext TypeScript target

(Source: GitHub node/README.md, node/package.json, glide.valkey.io)

---

## 4. Client Types: Standalone vs Cluster

### GlideClient (Standalone Mode)

For connecting to a single Valkey server or a primary with replicas (not using Valkey Cluster):

```typescript
import { GlideClient } from "@valkey/valkey-glide";

const client = await GlideClient.createClient({
  addresses: [{ host: "primary.example.com", port: 6379 }],
  requestTimeout: 500,
  clientName: "my-app",
});

// Database selection (standalone only)
await client.select(2);

// Database operations
const dbSize = await client.dbsize();
await client.flushdb();

// Scan keys
const [cursor, keys] = await client.scan("0", { match: "user:*", count: 100 });
```

**Standalone-specific features:**
- `select(db)` - Switch logical databases (0-15)
- `move(key, db)` - Move key between databases
- `flushdb()` / `flushall()` - Clear databases
- PubSub modes: Exact and Pattern

### GlideClusterClient (Cluster Mode)

For Valkey Cluster deployments with automatic topology discovery:

```typescript
import { GlideClusterClient } from "@valkey/valkey-glide";

const client = await GlideClusterClient.createClient({
  addresses: [{ host: "cluster-seed.example.com", port: 7000 }],
  requestTimeout: 500,
  clientName: "my-cluster-app",
  periodicChecks: "enabledDefaultConfigs", // topology refresh
});

// Routing to specific nodes
const info = await client.info({ route: "randomNode" });
const allInfo = await client.info({ route: "allNodes" });

// Cluster scan (unified iteration across shards)
let cursor = "0";
do {
  const [newCursor, keys] = await client.scan(cursor, { match: "user:*" });
  cursor = newCursor;
  // process keys
} while (cursor !== "0");
```

**Cluster-specific features:**
- Automatic topology discovery from seed nodes
- Periodic topology checks (configurable interval, default 60s)
- Command routing (random, slot-based, address-based, all nodes)
- Sharded PubSub (Valkey 7.0+)
- Multi-slot command splitting (MGET, MSET, DEL across slots)
- `refreshTopologyFromInitialNodes` option

### Routing Options (Cluster Only)

| Route Type | Description |
|------------|-------------|
| `"randomNode"` | Any random node |
| `"allPrimaries"` | Broadcast to all primary nodes |
| `"allNodes"` | Broadcast to all nodes |
| `{ type: "primarySlotKey", key: "mykey" }` | Primary owning the key's slot |
| `{ type: "replicaSlotKey", key: "mykey" }` | Replica holding the key's slot |
| `{ type: "primarySlotId", id: 8000 }` | Primary for slot 8000 |
| `{ type: "routeByAddress", host: "10.0.0.1", port: 7000 }` | Specific node |

(Source: GitHub GlideClient.ts, GlideClusterClient.ts, NodeJS-wrapper wiki)

---

## 5. Configuration Reference

### BaseClientConfiguration

```typescript
interface BaseClientConfiguration {
  // Required: server addresses
  addresses: { host: string; port: number }[];

  // Authentication
  credentials?: {
    username?: string;
    password: string;
  } | IamAuthConfig;

  // Connection
  useTLS?: boolean;
  requestTimeout?: number;           // ms, default 250
  clientName?: string;
  protocol?: ProtocolVersion;        // RESP2 or RESP3
  lazyConnect?: boolean;             // defer until first command

  // Read routing
  readFrom?: ReadFrom;               // primary, preferReplica, AZAffinity, etc.
  clientAz?: string;                 // client's availability zone

  // Resilience
  connectionBackoff?: {
    numberOfRetries: number;
    factor: number;
    exponentBase: number;
  };
  inflightRequestsLimit?: number;    // default 1000

  // Response handling
  defaultDecoder?: Decoder;          // String or Bytes

  // Advanced
  advancedConfiguration?: {
    connectionTimeout?: number;      // TCP/TLS establishment (ms)
    tcpNoDelay?: boolean;            // disable Nagle's algorithm
    tlsAdvancedConfiguration?: {
      insecure?: boolean;            // skip cert verification
      rootCertificatePem?: string | Buffer;
      clientCertificatePem?: string | Buffer;   // mTLS
      clientKeyPem?: string | Buffer;           // mTLS
    };
  };
}
```

### GlideClusterClientConfiguration (extends Base)

```typescript
interface GlideClusterClientConfiguration extends BaseClientConfiguration {
  periodicChecks?: "enabledDefaultConfigs" | "disabled" | {
    intervalMs: number;
  };
  pubsubSubscriptions?: {
    channelsAndPatterns: Map<PubSubChannelModes, Set<string>>;
    callback?: (msg: PubSubMsg, context?: any) => void;
    context?: any;
  };
  advancedConfiguration?: {
    refreshTopologyFromInitialNodes?: boolean;
    // ...plus base advanced options
  };
}
```

### IAM Authentication Config

```typescript
interface IamAuthConfig {
  type: "iam";
  serviceType: ServiceType;   // Elasticache or MemoryDB
  region: string;
  userId: string;
  refreshIntervalMs?: number; // default 300000 (5 min)
}
```

(Source: GitHub BaseClient.ts, GlideClusterClient.ts, NodeJS-wrapper wiki)

---

## 6. API Surface and Command Coverage

GLIDE's Rust core supports **380+ command variants**, and the Node.js wrapper exposes them through typed methods. The command coverage is comprehensive.

### Command Categories and Counts

| Category | Commands | Examples |
|----------|----------|---------|
| String | 22 | get, set, mget, mset, incr, append, getrange |
| Hash | 27 | hget, hset, hgetall, hdel, hexpire, hpersist |
| List | 22 | lpush, rpop, lrange, lmove, blpop, blmove |
| Set | 17 | sadd, srem, smembers, sinter, sdiff, sunion |
| Sorted Set | 35 | zadd, zrange, zscore, zpopmin, zunionstore |
| Stream | 21 | xadd, xread, xreadgroup, xclaim, xautoclaim |
| Bitmap | 7 | setbit, getbit, bitcount, bitop, bitfield |
| HyperLogLog | 3 | pfadd, pfcount, pfmerge |
| Pub/Sub | 20 | publish, subscribe, psubscribe, spublish |
| Scripting/Functions | 20 | eval, fcall, functionLoad, scriptExists |
| Cluster Management | 28 | clusterInfo, clusterShards, clusterNodes |
| Server | 62 | info, config, dbsize, flushall, acl, memory |
| Generic/Key | 30 | del, exists, expire, rename, scan, type |
| Connection | 19 | ping, auth, clientId, select, echo |
| Transaction | 5 | multi, exec, watch, unwatch, discard |
| JSON Module | 22 | json.set, json.get, json.arrappend, json.del |
| Search/FT Module | 13 | ft.create, ft.search, ft.aggregate, ft.info |
| Geospatial | 10 | geoadd, geodist, geosearch, geosearchstore |

### Common Method Signatures

```typescript
// String operations
get(key: GlideString, options?: DecoderOption): Promise<GlideString | null>
set(key: GlideString, value: GlideString, options?: SetOptions): Promise<"OK" | GlideString | null>
del(keys: GlideString[]): Promise<number>
mget(keys: GlideString[]): Promise<(GlideString | null)[]>
incr(key: GlideString): Promise<number>
incrByFloat(key: GlideString, amount: number): Promise<number>

// Hash operations
hset(key: GlideString, fieldValues: HashDataType): Promise<number>
hget(key: GlideString, field: GlideString): Promise<GlideString | null>
hgetall(key: GlideString): Promise<GlideRecord<GlideString>>

// List operations
lpush(key: GlideString, elements: GlideString[]): Promise<number>
rpop(key: GlideString, count?: number): Promise<GlideString | GlideString[] | null>
lrange(key: GlideString, start: number, end: number): Promise<GlideString[]>

// Sorted set
zadd(key: GlideString, members: ElementAndScore[], options?: ZAddOptions): Promise<number>
zrange(key: GlideString, range: RangeByIndex | RangeByScore | RangeByLex): Promise<GlideString[]>

// Custom command (for any command not yet wrapped)
customCommand(args: GlideString[], options?: DecoderOption): Promise<GlideReturnType>
```

### GlideString Type

GLIDE uses `GlideString` to represent Valkey strings, which is a union of `string | Buffer`:

```typescript
type GlideString = string | Buffer;
```

Return type conventions:
- Simple status strings (OK, type names): `string`
- Binary data (dump, functionDump): `Buffer`
- Most other commands: `GlideString`

Use the `Decoder` option to control response encoding:

```typescript
// Get as UTF-8 string (default)
const str = await client.get("key"); // string

// Get as Buffer
const buf = await client.get("key", { decoder: Decoder.Bytes }); // Buffer
```

(Source: GitHub Commands.ts, request_type.rs, BaseClient.ts)

---

## 7. TypeScript Support and Type Safety

GLIDE is built with **full TypeScript support** and strict type checking.

### Compiler Configuration

- **Target**: ESNext
- **Module**: CommonJS
- **Strict mode**: Enabled (no implicit any, strict null checks, strict function types)
- **Declaration files**: Generated `.d.ts` for all exports
- **Source maps**: Enabled for debugging

### Key Type Definitions

```typescript
// Core types
type GlideString = string | Buffer;
type GlideReturnType = string | number | null | boolean | bigint | Buffer
  | Set<GlideReturnType> | Record<string, GlideReturnType>
  | Map<GlideReturnType, GlideReturnType> | GlideReturnType[]
  | RequestError;

// Data structure types
type GlideRecord<T> = Array<{ key: GlideString; value: T }>;
type ElementAndScore = { element: GlideString; score: number };
type HashDataType = Array<{ field: GlideString; value: GlideString }>;
type StreamEntryDataType = Record<string, Array<[GlideString, GlideString]>>;

// Enums
enum Decoder { String, Bytes }
enum ProtocolVersion { RESP2, RESP3 }
enum ReadFrom {
  primary,
  preferReplica,
  AZAffinity,
  AZAffinityReplicasAndPrimary
}
enum FlushMode { SYNC, ASYNC }
enum ConditionalChange {
  ONLY_IF_EXISTS,        // XX
  ONLY_IF_DOES_NOT_EXIST, // NX
  ONLY_IF_EQUAL          // IFEQ (Valkey 8.1+)
}

// Options types
interface SetOptions {
  conditionalSet?: ConditionalChange;
  returnOldValue?: boolean;
  expiry?: { type: TimeUnit; count: number };
}

interface ZAddOptions {
  conditionalChange?: ConditionalChange;
  updateOptions?: UpdateByScore;
  changed?: boolean;
}
```

### Type-Safe Batch Operations

```typescript
const batch = new Batch(true); // atomic
batch
  .set("key1", "value1")
  .get("key1")
  .hset("hash1", [{ field: "f1", value: "v1" }])
  .hgetall("hash1");

// Results are typed as (GlideReturnType | null)[]
const results = await client.exec(batch, true);
```

(Source: GitHub node/tsconfig.json, BaseClient.ts, Commands.ts)

---

## 8. Connection Management

### Single Multiplexed Connection

Unlike traditional clients that use connection pools, GLIDE maintains a **single multiplexed connection per node**. This design:
- Minimizes total TCP connections to servers
- Reduces system call overhead
- Leverages Valkey's built-in pipelining capability
- Avoids connection pool management complexity

### Lazy Connect

Defer physical connection until the first command executes:

```typescript
const client = await GlideClient.createClient({
  addresses: [{ host: "localhost", port: 6379 }],
  lazyConnect: true,
});
// Connection established only when this runs:
await client.ping();
```

### Connection Backoff

Reconnection uses exponential backoff with jitter:

```
delay = rand(0 ... factor * (exponentBase ^ N))
```

where N is the number of failed attempts.

```typescript
const client = await GlideClient.createClient({
  addresses: [{ host: "localhost", port: 6379 }],
  connectionBackoff: {
    numberOfRetries: 10,
    factor: 100,          // base delay factor (ms)
    exponentBase: 2,      // exponential multiplier
  },
});
```

After `numberOfRetries` is exhausted, the delay becomes constant but reconnection attempts continue **indefinitely**.

### Inflight Request Limiting

Default limit: **1000 concurrent requests** per connection. This prevents out-of-memory conditions via Little's Law:

```typescript
const client = await GlideClient.createClient({
  addresses: [{ host: "localhost", port: 6379 }],
  inflightRequestsLimit: 500, // lower for memory-constrained environments
});
```

### When to Create Multiple Clients

Create separate client instances for:
- **Blocking commands** (BLPOP, BRPOP, XREAD with block): Prevents blocking the multiplexed connection
- **WATCH/UNWATCH operations**: Isolates transaction state
- **Large value operations**: Prevents queuing delays for concurrent small requests

### Health Checks

The Rust core validates connections via PING commands at 3-second intervals during reconnection. Connections that fail the PING check are rejected and the reconnection loop continues.

(Source: GitHub General-Concepts wiki, glide-core/src/client/mod.rs, reconnecting_connection.rs)

---

## 9. Batching: Transactions and Pipelines

GLIDE 2.0 unifies transactions and pipelines under the **Batch API**.

### Atomic Batch (Transaction)

Equivalent to MULTI/EXEC. All commands execute as an indivisible unit:

```typescript
import { Batch } from "@valkey/valkey-glide";

const batch = new Batch(true); // isAtomic = true
batch
  .set("account:1:balance", "1000")
  .decrby("account:1:balance", 200)
  .incrby("account:2:balance", 200)
  .get("account:1:balance");

// raiseOnError: true throws on first error
const results = await client.exec(batch, true);
// results: ["OK", 800, 200, "800"]
```

**Cluster constraint**: In cluster mode, all keys in an atomic batch must map to the same hash slot. Use hash tags: `{account}:1`, `{account}:2`.

### Non-Atomic Batch (Pipeline)

Commands sent in one round-trip without atomicity guarantees:

```typescript
const batch = new Batch(false); // isAtomic = false
batch
  .set("key1", "val1")
  .set("key2", "val2")
  .get("key1")
  .get("key2");

const results = await client.exec(batch, false);
```

In cluster mode, non-atomic batches **automatically**:
1. Calculate hash slots for each key
2. Group commands by destination node
3. Create sub-pipelines per node
4. Reassemble responses in original order

### Error Handling in Batches

```typescript
// raiseOnError = true: throws RequestException on first error
try {
  const results = await client.exec(batch, true);
} catch (e) {
  // RequestException with the first error
}

// raiseOnError = false: errors returned in response array
const results = await client.exec(batch, false);
// results[2] might be a RequestError object
```

### Batch Options

```typescript
interface BatchOptions {
  timeout?: number;              // override client requestTimeout
  retryStrategy?: {
    retryServerError?: boolean;  // retry TRYAGAIN errors (may reorder)
    retryConnectionError?: boolean; // retry entire batch (risk duplicates)
  };
  route?: SingleNodeRoute;       // cluster routing
}
```

### Migration Note

GLIDE 2.0 deprecates `Transaction`/`ClusterTransaction` in favor of `Batch(true)`:

```typescript
// Old (deprecated)
const tx = new Transaction();
tx.set("k", "v").get("k");
await client.exec(tx);

// New (GLIDE 2.0+)
const batch = new Batch(true);
batch.set("k", "v").get("k");
await client.exec(batch, true);
```

(Source: GitHub NodeJS-wrapper wiki, Batch.ts, General-Concepts wiki)

---

## 10. Pub/Sub Support

GLIDE provides robust Pub/Sub with **automatic reconnection** and **resubscription**.

### Subscription Modes

| Mode | Standalone | Cluster | Description |
|------|-----------|---------|-------------|
| Exact | Yes | Yes | Subscribe to specific channel names |
| Pattern | Yes | Yes | Subscribe to channel patterns (e.g., `news.*`) |
| Sharded | No | Yes | Slot-aware Pub/Sub (Valkey 7.0+) |

### Configuration-Based Subscriptions

Subscriptions are configured at client creation time for automatic resubscription on reconnect:

```typescript
import {
  GlideClient,
  GlideClusterClient,
  GlideClientConfiguration,
  GlideClusterClientConfiguration,
} from "@valkey/valkey-glide";

// Standalone with callback
const client = await GlideClient.createClient({
  addresses: [{ host: "localhost", port: 6379 }],
  protocol: ProtocolVersion.RESP3, // Required for push notifications
  pubsubSubscriptions: {
    channelsAndPatterns: new Map([
      [GlideClientConfiguration.PubSubChannelModes.Exact, new Set(["news", "alerts"])],
      [GlideClientConfiguration.PubSubChannelModes.Pattern, new Set(["events.*"])],
    ]),
    callback: (msg, context) => {
      console.log(`Channel: ${msg.channel}, Message: ${msg.message}`);
    },
  },
});

// Publishing
await client.publish("Hello, subscribers!", "news");
```

### Message Consumption Patterns

**1. Callback-driven** (recommended for real-time processing):
```typescript
pubsubSubscriptions: {
  channelsAndPatterns: channels,
  callback: (msg, context) => {
    context.messages.push(msg);
  },
  context: { messages: [] },
}
```

**2. Async polling**:
```typescript
const msg = await client.getPubSubMessage(); // blocks until message
```

**3. Sync polling**:
```typescript
const msg = client.tryGetPubSubMessage(); // returns null if no message
```

### Sharded Pub/Sub (Cluster)

Sharded Pub/Sub routes messages through cluster hash slots, reducing fan-out:

```typescript
const client = await GlideClusterClient.createClient({
  addresses: [{ host: "cluster.example.com", port: 7000 }],
  pubsubSubscriptions: {
    channelsAndPatterns: new Map([
      [GlideClusterClientConfiguration.PubSubChannelModes.Sharded,
       new Set(["orders.{shop1}", "orders.{shop2}"])],
    ]),
    callback: handleMessage,
  },
});

// Sharded publish
await client.publish("New order!", "orders.{shop1}", true); // sharded=true
```

### Auto-Reconnection

When a connection drops, GLIDE automatically:
1. Reconnects using the configured backoff strategy
2. Resubscribes to all channels and patterns
3. Delivers queued messages through the configured callback or polling interface
4. Rejects pending PubSub futures with ConnectionError if the connection is permanently closed

(Source: GitHub GlideClient.ts, GlideClusterClient.ts, BaseClient.ts, PubSub.test.ts)

---

## 11. Cluster Features

### Automatic Topology Discovery

Provide any seed node address; the client discovers the full topology:

```typescript
const client = await GlideClusterClient.createClient({
  addresses: [{ host: "any-cluster-node.example.com", port: 7000 }],
});
// Client automatically discovers all primaries and replicas
```

### Periodic Topology Checks

```typescript
// Default: enabled with 60-second interval
periodicChecks: "enabledDefaultConfigs"

// Custom interval
periodicChecks: { intervalMs: 30000 }

// Disabled (not recommended)
periodicChecks: "disabled"
```

### Multi-Slot Command Handling

GLIDE automatically splits commands that operate on keys across different hash slots:

**Supported multi-slot commands**: MGET, MSET, DEL, UNLINK, EXISTS, TOUCH, WATCH, JSON.MGET

```typescript
// These keys may be in different slots - GLIDE handles it
await client.mset([
  ["user:1:name", "Alice"],
  ["user:2:name", "Bob"],
  ["user:3:name", "Charlie"],
]);

const names = await client.mget(["user:1:name", "user:2:name", "user:3:name"]);
```

**Important caveat**: Multi-slot commands guarantee atomicity only at individual slot levels. Partial failures can occur across slots.

### Cluster Scan

Unified key iteration across all shards:

```typescript
let cursor = "0";
const allKeys: string[] = [];

do {
  const [newCursor, keys] = await client.scan(cursor, {
    match: "session:*",
    count: 100,
  });
  allKeys.push(...keys);
  cursor = newCursor;
} while (cursor !== "0");
```

### AZ-Affinity Routing

Routes reads to replicas in the same availability zone as the client, reducing cross-AZ latency (from ~500-1000us to ~300us) and data transfer costs:

```typescript
const client = await GlideClusterClient.createClient({
  addresses: [{ host: "cluster.example.com", port: 7000 }],
  readFrom: ReadFrom.AZAffinity,
  clientAz: "us-east-1a",
});
```

| Strategy | Behavior |
|----------|----------|
| `primary` | Always read from primary (freshest data) |
| `preferReplica` | Round-robin replicas, fallback to primary |
| `AZAffinity` | Same-AZ replicas, then other replicas, then primary |
| `AZAffinityReplicasAndPrimary` | Same-AZ replicas, then same-AZ primary, then other zones |

**Cost impact example**: For a 250MB/s read-heavy cluster on AWS, AZ affinity can reduce monthly cost from $4,373 to $1,088 by eliminating cross-AZ data transfer charges.

### MOVED/ASK Handling

The client automatically handles cluster redirection:
- **MOVED**: Updates topology and retries on the correct node
- **ASK**: Sends ASKING + retries on the indicated node

(Source: GitHub GlideClusterClient.ts, az-affinity blog post, General-Concepts wiki, glide-core/src/client/mod.rs)

---

## 12. Authentication and Security

### Username/Password Authentication

```typescript
const client = await GlideClient.createClient({
  addresses: [{ host: "localhost", port: 6379 }],
  credentials: {
    username: "myuser",      // ACL username (optional)
    password: "mypassword",
  },
});
```

### IAM Authentication (AWS ElastiCache/MemoryDB)

Built-in token generation with automatic refresh:

```typescript
const client = await GlideClusterClient.createClient({
  addresses: [{ host: "my-cluster.cache.amazonaws.com", port: 6379 }],
  credentials: {
    type: "iam",
    serviceType: ServiceType.Elasticache, // or ServiceType.MemoryDB
    region: "us-east-1",
    userId: "my-iam-user",
    refreshIntervalMs: 300000, // 5 minutes (default)
  },
  useTLS: true,
});
```

### Password Update (Runtime)

```typescript
await client.updateConnectionPassword("newPassword");
```

### TLS/SSL

```typescript
// Basic TLS
const client = await GlideClient.createClient({
  addresses: [{ host: "secure.example.com", port: 6380 }],
  useTLS: true,
});

// Custom CA certificate
const client = await GlideClient.createClient({
  addresses: [{ host: "secure.example.com", port: 6380 }],
  useTLS: true,
  advancedConfiguration: {
    tlsAdvancedConfiguration: {
      rootCertificatePem: fs.readFileSync("ca.pem", "utf-8"),
    },
  },
});

// mTLS (mutual TLS) - GLIDE 2.3+
const client = await GlideClient.createClient({
  addresses: [{ host: "secure.example.com", port: 6380 }],
  useTLS: true,
  advancedConfiguration: {
    tlsAdvancedConfiguration: {
      rootCertificatePem: fs.readFileSync("ca.pem", "utf-8"),
      clientCertificatePem: fs.readFileSync("client.pem", "utf-8"),
      clientKeyPem: fs.readFileSync("client-key.pem", "utf-8"),
    },
  },
});

// Insecure mode (skip cert verification - NOT for production)
const client = await GlideClient.createClient({
  addresses: [{ host: "dev.example.com", port: 6380 }],
  useTLS: true,
  advancedConfiguration: {
    tlsAdvancedConfiguration: {
      insecure: true,
    },
  },
});
```

TLS is implemented via **rustls** with **aws-lc-rs** cryptography in the Rust core.

(Source: GitHub NodeJS-wrapper wiki, BaseClient.ts, CHANGELOG.md)

---

## 13. Error Handling and Retry Strategies

### Error Hierarchy

```
ValkeyError (abstract base)
  |
  +-- ClosingError           # Client closed, no longer operational
  |
  +-- RequestError           # Base for request-level errors
       |
       +-- TimeoutError      # Operation exceeded timeout
       |
       +-- ExecAbortError    # Transaction aborted (EXECABORT)
       |
       +-- ConnectionError   # Connection lost (may be temporary)
       |
       +-- ConfigurationError # Invalid configuration
```

### Handling Errors

```typescript
import {
  ClosingError,
  ConnectionError,
  TimeoutError,
  RequestError,
} from "@valkey/valkey-glide";

try {
  await client.get("key");
} catch (error) {
  if (error instanceof ClosingError) {
    // Client is permanently closed - create a new one
    client = await GlideClient.createClient(config);
  } else if (error instanceof TimeoutError) {
    // Operation timed out - retry or increase timeout
  } else if (error instanceof ConnectionError) {
    // Connection lost but client may reconnect automatically
    // Wait and retry
  } else if (error instanceof RequestError) {
    // General request error
  }
}
```

### Retry Strategies

**Connection-level retries** are handled automatically by the Rust core:
- Exponential backoff with jitter
- PING health checks between retry attempts
- **Permanent errors** (auth failures, config errors) halt retries immediately
- **Transient errors** trigger continued retry attempts
- Connection timeout wraps the entire retry loop

**Batch-level retries** (non-atomic pipelines only):

```typescript
const results = await clusterClient.exec(batch, false, {
  retryStrategy: {
    retryServerError: true,       // retry on TRYAGAIN
    retryConnectionError: true,   // retry entire batch on disconnect
  },
});
```

**Warning**: `retryServerError` may cause out-of-order execution. `retryConnectionError` may cause duplicate execution.

### Request Timeout

```typescript
// Per-client default
const client = await GlideClient.createClient({
  requestTimeout: 500, // 500ms default for all commands
});

// Per-batch override
await client.exec(batch, true, { timeout: 2000 });
```

For blocking commands (BLPOP, XREAD), GLIDE automatically extracts the timeout from the command arguments and adds a 0.5-second buffer.

(Source: GitHub Errors.ts, glide-core/src/client/mod.rs, reconnecting_connection.rs)

---

## 14. Observability: OpenTelemetry and Logging

### OpenTelemetry Integration

GLIDE provides native OpenTelemetry support for traces and metrics:

```typescript
import { OpenTelemetry } from "@valkey/valkey-glide";

OpenTelemetry.init({
  traces: {
    endpoint: "http://localhost:4318/v1/traces",
    samplePercentage: 10, // 10% sampling (default: 1%)
  },
  metrics: {
    endpoint: "http://localhost:4318/v1/metrics",
    flushIntervalMs: 5000, // default: 5000ms
  },
});
```

**Important constraints**:
- `OpenTelemetry.init()` can only be called **once per process**
- At least one of traces or metrics must be configured
- Supported protocols: `http://`, `https://`, `grpc://`, `file://`

**Emitted metrics**:
- Request timeouts
- Retry operations
- MOVED errors (cluster slot rebalancing)

**Tracing**: Creates spans per Valkey command with:
- Full lifecycle from creation to completion
- Nested `send_command` span measuring server communication
- Success/error status tagging

**Runtime sampling control**:
```typescript
OpenTelemetry.setSamplePercentage(50); // Increase to 50%
const current = OpenTelemetry.getSamplePercentage();
const shouldTrace = OpenTelemetry.shouldSample();
```

**Limitations**: SCAN family commands and Lua scripting (EVAL, EVALSHA) are not yet instrumented.

### Logging

```typescript
import { Logger } from "@valkey/valkey-glide";

// Initialize with level and optional file
Logger.init("info", "glide-app"); // writes to files postfixed with "glide-app"
Logger.init("debug");             // console output

// Log levels: error, warn, info, debug, trace, off
Logger.setLoggerConfig("debug", "app-debug");

// Usage
Logger.log("info", "MyModule", "Connection established");
Logger.log("error", "MyModule", "Operation failed", error);
```

### Statistics

```typescript
const stats = await client.getStatistics();
// { total_connections: number, total_clients: number }
```

(Source: GitHub OpenTelemetry.ts, Logger.ts, NodeJS-wrapper wiki)

---

## 15. Server Modules: JSON and Search

### JSON Module (GlideJson)

Requires the Valkey JSON module (or RedisJSON) on the server:

```typescript
import { GlideJson } from "@valkey/valkey-glide";

// Set JSON value
await GlideJson.set(client, "user:1", "$", JSON.stringify({
  name: "Alice",
  age: 30,
  hobbies: ["reading", "coding"],
}));

// Get JSON value
const user = await GlideJson.get(client, "user:1", { path: "$" });

// Array operations
await GlideJson.arrappend(client, "user:1", "$.hobbies", '"gaming"');
const len = await GlideJson.arrlen(client, "user:1", "$.hobbies");

// Numeric operations
await GlideJson.numincrby(client, "user:1", "$.age", 1);

// Object operations
const keys = await GlideJson.objkeys(client, "user:1", "$");
const size = await GlideJson.objlen(client, "user:1", "$");

// Multi-key get
const users = await GlideJson.mget(client, ["user:1", "user:2"], "$");

// Toggle boolean
await GlideJson.toggle(client, "user:1", "$.active");

// Type checking
const type = await GlideJson.type(client, "user:1", "$.name"); // "string"

// Memory usage
const bytes = await GlideJson.debugMemory(client, "user:1", "$");
```

### Search Module (GlideFt)

Requires the Valkey Search module (or RediSearch):

```typescript
import { GlideFt, FtCreateOptions } from "@valkey/valkey-glide";

// Create an index
await GlideFt.create(client, "idx:users", {
  dataType: "HASH",
  prefixes: ["user:"],
  schema: [
    { name: "name", type: "TEXT" },
    { name: "age", type: "NUMERIC" },
    { name: "city", type: "TAG", separator: "," },
    {
      name: "embedding",
      type: "VECTOR",
      algorithm: "HNSW",
      dimensions: 128,
      distanceMetric: "COSINE",
    },
  ],
});

// Search
const results = await GlideFt.search(client, "idx:users", "@city:{NYC}", {
  returnFields: [{ name: "name" }, { name: "age" }],
  limit: { offset: 0, count: 10 },
});

// Aggregate
const agg = await GlideFt.aggregate(client, "idx:users", "*", {
  clauses: [
    { type: "GROUPBY", properties: ["@city"], reducers: [{ type: "COUNT", as: "count" }] },
    { type: "SORTBY", properties: [{ property: "@count", order: "DESC" }] },
    { type: "LIMIT", offset: 0, count: 5 },
  ],
});

// Index management
const indexes = await GlideFt.list(client);
const info = await GlideFt.info(client, "idx:users");
await GlideFt.aliasadd(client, "users", "idx:users");
await GlideFt.dropindex(client, "idx:users");

// Query profiling
const [results2, profile] = await GlideFt.profileSearch(client, "idx:users", "@name:Alice");
```

(Source: GitHub GlideJson.ts, GlideFt.ts, GlideFtOptions.ts)

---

## 16. Performance Characteristics

### Architecture-Level Optimizations

1. **Single multiplexed connection**: Eliminates connection pool overhead and reduces TCP connections
2. **Rust core**: Network I/O and protocol parsing in compiled Rust, not interpreted JavaScript
3. **Protobuf serialization**: Efficient binary communication between Node.js and Rust layers
4. **Automatic pipelining**: Commands are naturally pipelined over the multiplexed connection
5. **Inflight request limiting**: Prevents memory exhaustion under high load (default: 1000)

### Benchmark Methodology

GLIDE includes a Node.js benchmark (`benchmarks/node/node_benchmark.ts`) that compares:
- **GLIDE** vs **ioredis** vs **node-redis**
- Operations: GET (existing keys), GET (non-existing keys), SET
- Workload: ~80% reads, ~20% writes
- Metrics: P50, P90, P99 latency, average, standard deviation, TPS
- Configurable: data size, client count, concurrent tasks, cluster mode, TLS

### Performance Considerations

- GLIDE's Rust core handles all I/O, freeing the Node.js event loop
- The protobuf serialization layer adds a small constant overhead per command
- For very simple, single-command workloads, pure-JavaScript clients may have lower per-command overhead due to no FFI crossing
- For concurrent, high-throughput workloads, GLIDE's Rust core and multiplexed connection provide significant advantages
- Batch operations (non-atomic) automatically parallelize across cluster nodes
- AZ-affinity routing reduces latency by 40-70% for cross-AZ reads

### TCP No Delay

```typescript
const client = await GlideClient.createClient({
  advancedConfiguration: {
    tcpNoDelay: true, // Disable Nagle's algorithm for lower latency
  },
});
```

(Source: GitHub node_benchmark.ts, General-Concepts wiki, az-affinity blog)

---

## 17. Comparison with ioredis and node-redis

| Feature | GLIDE | ioredis | node-redis |
|---------|-------|---------|------------|
| **Language** | Rust core + TS bindings | Pure JavaScript | Pure JavaScript |
| **Connection model** | Single multiplexed per node | Connection pool | Connection pool |
| **Cluster support** | Built-in, auto-discovery | Built-in | Built-in |
| **AZ-Affinity routing** | Yes (unique) | No | No |
| **PubSub auto-reconnect** | Yes (built-in) | Manual | Manual |
| **Sharded PubSub** | Yes | No | Yes (v4.6+) |
| **IAM auth (AWS)** | Built-in | Plugin/manual | Plugin/manual |
| **OpenTelemetry** | Built-in | External middleware | External middleware |
| **TypeScript** | First-class, strict | Bundled types | First-class |
| **Multi-slot commands** | Auto-split | Auto-split | Manual |
| **Cluster scan** | Unified | Per-node | Per-node |
| **Batch API** | Atomic + non-atomic | pipeline + multi | pipeline + multi |
| **Custom commands** | `customCommand()` | `call()` | `sendCommand()` |
| **Boolean returns** | `true/false` | `1/0` | `true/false` |
| **Lua scripts** | Auto-caching Script class | EVALSHA manual | Script class |
| **Compression** | Built-in (zstd/lz4) | No | No |
| **mTLS** | Yes (GLIDE 2.3+) | Yes | Yes |
| **Windows (Node.js)** | No | Yes | Yes |
| **Min Node.js** | v20 | v12 | v14 |
| **npm weekly downloads** | Growing | ~5M+ | ~4M+ |
| **Maintained by** | Valkey/AWS | Community | Redis Ltd |

### When to Choose GLIDE

- Running on AWS ElastiCache or MemoryDB (IAM auth, AZ-affinity)
- Need consistent behavior across multiple languages
- Want built-in observability (OpenTelemetry)
- Running large-scale cluster deployments
- Need automatic PubSub reconnection

### When to Stay with ioredis/node-redis

- Need Windows support for the Node.js client
- Running on Node.js < 20
- Need older glibc support (< 2.17)
- Require extensive community plugins/middleware ecosystem
- Need maximum npm ecosystem maturity

(Source: GitHub Migration-Guide-ioredis wiki, node_benchmark.ts, feature comparison)

---

## 18. Migration from ioredis

### Connection Setup

```typescript
// ioredis - standalone
const Redis = require("ioredis");
const redis = new Redis({ host: "localhost", port: 6379, password: "secret" });

// GLIDE - standalone
import { GlideClient } from "@valkey/valkey-glide";
const client = await GlideClient.createClient({
  addresses: [{ host: "localhost", port: 6379 }],
  credentials: { password: "secret" },
});

// ioredis - cluster
const cluster = new Redis.Cluster([{ host: "node1", port: 7000 }]);

// GLIDE - cluster
const cluster = await GlideClusterClient.createClient({
  addresses: [{ host: "node1", port: 7000 }],
});
```

### Configuration Mapping

| ioredis | GLIDE |
|---------|-------|
| `host` / `port` | `addresses: [{ host, port }]` |
| `username` / `password` | `credentials: { username, password }` |
| `tls: {}` | `useTLS: true` |
| `commandTimeout` | `requestTimeout` |
| `connectTimeout` | `advancedConfiguration: { connectionTimeout }` |
| `db` | `client.select(db)` after connection |
| `readOnly` | `readFrom: "preferReplica"` |

### Command API Changes

**Multi-argument to array**:
```typescript
// ioredis
await redis.del("key1", "key2", "key3");
await redis.mget("k1", "k2", "k3");
await redis.lpush("list", "a", "b", "c");

// GLIDE
await client.del(["key1", "key2", "key3"]);
await client.mget(["k1", "k2", "k3"]);
await client.lpush("list", ["a", "b", "c"]);
```

**Object-based parameters**:
```typescript
// ioredis
await redis.hset("hash", "field1", "value1", "field2", "value2");
await redis.zadd("zset", 1, "member1", 2, "member2");

// GLIDE
await client.hset("hash", [{ field: "field1", value: "value1" }, { field: "field2", value: "value2" }]);
await client.zadd("zset", [{ score: 1, element: "member1" }, { score: 2, element: "member2" }]);
```

**Return value changes**:
```typescript
// ioredis returns 1/0, GLIDE returns true/false
const exists = await client.hexists("hash", "field"); // true, not 1
const renamed = await client.renamenx("old", "new");  // true, not 1
const expired = await client.expire("key", 60);       // true, not 1
```

**SETEX / SETNX**:
```typescript
// ioredis
await redis.setex("key", 5, "value");
await redis.setnx("key", "value");

// GLIDE
await client.set("key", "value", { expiry: { type: TimeUnit.Seconds, count: 5 } });
await client.set("key", "value", { conditionalSet: "onlyIfDoesNotExist" });
```

### Transactions

```typescript
// ioredis
const result = await redis.multi().set("k", "v").get("k").exec();
// result: [[null, "OK"], [null, "v"]]

// GLIDE
const batch = new Batch(true);
batch.set("k", "v").get("k");
const result = await client.exec(batch, true);
// result: ["OK", "v"]
```

### Lua Scripts

```typescript
// ioredis - manual EVALSHA management
await redis.eval("return KEYS[1]", 1, "mykey");

// GLIDE - automatic script caching
import { Script } from "@valkey/valkey-glide";
const script = new Script("return { KEYS[1], ARGV[1] }");
const result = await client.invokeScript(script, { keys: ["foo"], args: ["bar"] });
```

### Disconnection

```typescript
// ioredis
redis.disconnect();

// GLIDE
client.close();
```

(Source: GitHub Migration-Guide-ioredis wiki)

---

## 19. Valkey-Specific Features vs Redis Compatibility

### Compatibility Matrix

| Engine | Supported Versions |
|--------|-------------------|
| Valkey | 7.2, 8.0, 8.1, 9.0 |
| Redis OSS | 6.2, 7.0, 7.2 |

GLIDE maintains **backward compatibility** with Redis OSS 6.2-7.2. The version detection logic checks for both `valkey_version:` and `redis_version:` in server INFO responses.

### Valkey-Specific Features

Features that require Valkey (not available in Redis OSS):

1. **AZ-Affinity routing** (Valkey 8.0+ or ElastiCache): `availability-zone` config parameter
2. **Hash field expiration** (Valkey 7.4+): `HEXPIRE`, `HPEXPIRE`, `HEXPIREAT`, `HPEXPIREAT`, `HPERSIST`, `HTTL`, `HPTTL`, `HEXPIRETIME`, `HPEXPIRETIME`
3. **HGETEX / HSETEX**: Get/set with expiry in one command
4. **Multi-database cluster support** (Valkey 9.0): `SELECT` in cluster mode
5. **Conditional set with IFEQ** (Valkey 8.1+): `ConditionalChange.ONLY_IF_EQUAL`
6. **Sharded Pub/Sub** (Valkey 7.0+): `SPUBLISH`, `SSUBSCRIBE`

### Redis-Compatible Features

All standard Redis commands work with both Valkey and Redis OSS. The `customCommand()` method provides a fallback for any command not yet wrapped in the typed API:

```typescript
// Works with both Redis and Valkey
const result = await client.customCommand(["CLIENT", "INFO"]);
```

(Source: GitHub README, CHANGELOG.md, TestUtilities.ts)

---

## 20. Production Readiness and Known Limitations

### Production Readiness

- **License**: Apache 2.0 (fully open source)
- **Stability**: All releases GPG-signed, CI-tested across Valkey 7.2-9.0 and Redis 6.2-7.2
- **Contributors**: 104+ contributors, 690+ GitHub stars
- **Backing**: Supported by AWS and GCP engineering teams
- **Release cadence**: Regular releases (latest: v2.3 as of Feb 2026)

### Known Limitations

| Limitation | Details |
|------------|---------|
| **No Windows support** | Node.js client does not have Windows binaries |
| **Node.js 20+ required** | v16 and v18 support dropped in GLIDE 2.3 |
| **glibc 2.17+ required** | Older Linux distros (Debian 11, Ubuntu 20.04, Amazon Linux 2) are NOT supported |
| **No ESM native export** | Package uses CommonJS; ESM requires `esModuleInterop` |
| **OpenTelemetry limitations** | SCAN family and Lua scripting not yet instrumented |
| **OpenTelemetry single-init** | `OpenTelemetry.init()` can only be called once per process |
| **Multi-slot atomicity** | Multi-slot commands (MGET, MSET) are NOT atomic across slots |
| **Docker/container issues** | Some users report decoder errors in Docker that don't occur on bare metal |
| **Protobuf overhead** | Small constant overhead per command for Node<->Rust serialization |
| **Module format** | CommonJS primary; TypeScript target is ESNext but module output is CJS |

### GLIBC Compatibility

Unsupported GLIBC versions: 2.26, 2.27, 2.30, 2.31. Users on affected systems see:
```
/lib/x86_64-linux-gnu/libc.so.6: version 'GLIBC_2.34' not found
```

**Workaround**: Upgrade to Debian 12+, Ubuntu 22.04+, or use Alpine Linux (musl libc).

### Node.js-Specific Issues (Resolved)

- Circular import dependencies between BaseClient, GlideClient, and GlideClusterClient (fixed v2.1.2)
- "Failed to convert napi value Undefined into rust type u32" error (fixed)
- npm publishing complications that held Node.js at v2.1.1 while other clients advanced (resolved v2.2.5+)

(Source: GitHub Known-Issues wiki, CHANGELOG.md, issues search)

---

## 21. GLIDE's Relationship to AWS and the Valkey Community

### Origin

GLIDE originated from AWS's internal client library for **Amazon ElastiCache** and **Amazon MemoryDB**. After Redis Ltd changed the Redis license (moving away from open source), AWS helped fork Redis into **Valkey** under the Linux Foundation and open-sourced their internal client as GLIDE.

### AWS Involvement

- GLIDE is **sponsored and primarily developed by AWS** with contributions from GCP
- Built-in **IAM authentication** for ElastiCache and MemoryDB
- **AZ-Affinity routing** optimized for AWS multi-AZ deployments
- AWS engineers are core maintainers

### Valkey Community

- GLIDE is an **official Valkey project** under the Valkey organization on GitHub
- Apache 2.0 license ensures it remains fully open source
- Community contributions welcome via GitHub issues and PRs
- Valkey Slack channel for community support
- Regular contributors' meetings documented in the wiki

### Governance

GLIDE follows the Valkey project governance model, which operates under the Linux Foundation. This ensures no single company can change the license or restrict usage.

(Source: GitHub README, AWS blog post metadata, Valkey blog, wiki)

---

## 22. Common Pitfalls

| Pitfall | Why It Happens | How to Avoid |
|---------|---------------|--------------|
| Using blocking commands on shared client | Blocks the multiplexed connection for all operations | Create a dedicated client instance for BLPOP, BRPOP, XREAD with block |
| Atomic batch with multi-slot keys (cluster) | All keys must be in the same hash slot for transactions | Use hash tags: `{prefix}:key1`, `{prefix}:key2` |
| Expecting 1/0 returns like ioredis | GLIDE returns `true/false` for boolean commands | Update conditional checks: `if (result)` not `if (result === 1)` |
| Not handling ClosingError | Client is permanently closed after ClosingError | Recreate the client when catching ClosingError |
| Setting inflightRequestsLimit too high | May cause OOM under sustained high load | Keep at default (1000) or tune based on memory profiling |
| Using RESP2 with PubSub push | RESP3 required for push-based PubSub notifications | Set `protocol: ProtocolVersion.RESP3` |
| Forgetting `client.close()` | Leaves connections open, prevents clean shutdown | Always call `close()` in finally/shutdown handlers |
| Deploying on unsupported glibc | Native Rust binary requires glibc 2.17+ | Use Alpine (musl) or upgrade OS to supported version |
| Calling `OpenTelemetry.init()` multiple times | Only first call takes effect, subsequent calls are silently ignored | Initialize once at application startup |
| Not awaiting `createClient()` | `createClient` is async; using before await leads to undefined | Always `await GlideClient.createClient(...)` |

---

## 23. Best Practices

1. **Use cluster mode for production**: Even with a single shard, `GlideClusterClient` provides better topology awareness and failover handling

2. **Enable AZ-affinity when on AWS**: Configure `readFrom: ReadFrom.AZAffinity` and `clientAz` to reduce latency and cross-AZ costs

3. **Use non-atomic batches for bulk operations**: Pipelines automatically parallelize across cluster nodes for maximum throughput

4. **Set appropriate request timeouts**: Default 250ms is aggressive; increase to 500-1000ms for production workloads with larger payloads

5. **Handle all error types**: Implement handlers for ClosingError, ConnectionError, TimeoutError, and ExecAbortError separately

6. **Use hash tags for related keys**: Ensure transactionally-related keys share a hash slot with `{tag}:key` notation

7. **Enable OpenTelemetry early**: Built-in tracing helps diagnose production issues without additional middleware

8. **Use TypeScript**: Full type coverage catches API misuse at compile time (e.g., wrong parameter types, missing required options)

9. **Separate blocking operations**: Create dedicated client instances for BLPOP, BRPOP, XREAD with blocking timeouts

10. **Monitor with `getStatistics()`**: Track connection counts and client counts for operational health

11. **Use `tcpNoDelay: true`**: Disabling Nagle's algorithm reduces latency for small commands at the cost of slightly higher bandwidth

12. **Pin to specific GLIDE versions**: Native binaries can behave differently across versions; test upgrades thoroughly

(Source: Synthesized from all sources)

---

## 24. Further Reading

| Resource | Type | Why Recommended |
|----------|------|-----------------|
| [GitHub: valkey-io/valkey-glide](https://github.com/valkey-io/valkey-glide) | Repository | Source code, issues, and latest releases |
| [GLIDE Documentation](https://glide.valkey.io) | Official docs | API reference and guides |
| [GLIDE Node.js README](https://github.com/valkey-io/valkey-glide/blob/main/node/README.md) | Getting started | Installation and quick start |
| [Migration Guide: ioredis](https://github.com/valkey-io/valkey-glide/wiki/Migration-Guide-ioredis) | Migration | Step-by-step ioredis to GLIDE migration |
| [Node.js Wrapper Wiki](https://github.com/valkey-io/valkey-glide/wiki/NodeJS-wrapper) | API reference | Detailed Node.js API documentation |
| [General Concepts Wiki](https://github.com/valkey-io/valkey-glide/wiki/General-Concepts) | Architecture | Connection model, pipelining, clustering |
| [AZ Affinity Blog](https://valkey.io/blog/az-affinity-strategy/) | Performance | Latency reduction and cost savings |
| [GLIDE Benchmarks](https://github.com/valkey-io/valkey-glide/tree/main/benchmarks) | Performance | Benchmark scripts and methodology |
| [GLIDE Changelog](https://github.com/valkey-io/valkey-glide/blob/main/CHANGELOG.md) | Release notes | Version history and breaking changes |
| [Known Issues Wiki](https://github.com/valkey-io/valkey-glide/wiki/Known-Issues) | Troubleshooting | Platform compatibility and workarounds |
| [Valkey Documentation](https://valkey.io/docs/) | Server docs | Valkey server documentation |
| [DEVELOPER.md](https://github.com/valkey-io/valkey-glide/blob/main/node/DEVELOPER.md) | Contributing | Build process and development setup |

---

*This guide was synthesized from 40 sources including the official GLIDE repository, documentation site, wiki pages, source code analysis, blog posts, and community resources. See `resources/valkey-glide-nodejs-client-sources.json` for the full source list with quality scores.*
