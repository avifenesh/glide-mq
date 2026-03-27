# Vector Search Reference

Create Valkey Search indexes over job hashes for vector similarity search (KNN).

Requires the `valkey-search` module loaded on the Valkey server (standalone mode only).

## Creating an Index

```typescript
import { Queue } from 'glide-mq';
import type { Field } from 'glide-mq';

const queue = new Queue('embeddings', { connection });

// Minimal index (base fields only, no vector search)
await queue.createJobIndex();

// Index with vector field for KNN search
await queue.createJobIndex({
  name: 'embeddings-idx',            // default: '{queueName}-idx'
  vectorField: {
    name: 'embedding',               // field name in the job hash
    dimensions: 1536,                // vector dimensions (e.g. OpenAI ada-002)
    algorithm: 'HNSW',              // 'HNSW' (default) or 'FLAT'
    distanceMetric: 'COSINE',       // 'COSINE' (default), 'L2', or 'IP'
  },
  fields: [                          // additional schema fields
    { type: 'TAG', name: 'category' } as Field,
    { type: 'TEXT', name: 'summary' } as Field,
    { type: 'NUMERIC', name: 'score' } as Field,
  ],
});
```

### Auto-Included Base Fields

Every index automatically includes:

| Field | Type | Description |
|-------|------|-------------|
| `name` | TAG | Job name |
| `state` | TAG | Job state (waiting, active, completed, etc.) |
| `timestamp` | NUMERIC | Job creation timestamp |
| `priority` | NUMERIC | Job priority |

### JobIndexOptions

```typescript
interface JobIndexOptions {
  name?: string;                      // index name, default: '{queueName}-idx'
  fields?: Field[];                   // additional schema fields
  vectorField?: {
    name: string;                     // field name where vector is stored
    dimensions: number;               // vector dimensions
    algorithm?: 'HNSW' | 'FLAT';     // default: 'HNSW'
    distanceMetric?: 'COSINE' | 'L2' | 'IP';  // default: 'COSINE'
  };
  createOptions?: IndexCreateOptions; // pass-through to FT.CREATE
}
```

### IndexCreateOptions

```typescript
interface IndexCreateOptions {
  score?: number;              // default document score
  language?: string;           // default stemming language
  skipInitialScan?: boolean;   // skip indexing existing docs
  minStemSize?: number;
  withOffsets?: boolean;
  noOffsets?: boolean;
  noStopWords?: boolean;
  stopWords?: string[];
  punctuation?: string;
}
```

## Vector Search (KNN)

```typescript
// Generate an embedding for the query
const queryEmbedding = await openai.embeddings.create({
  model: 'text-embedding-ada-002',
  input: 'machine learning optimization',
});

const results = await queue.vectorSearch(
  queryEmbedding.data[0].embedding,   // number[] or Float32Array
  {
    k: 10,                             // nearest neighbours (default: 10)
    filter: '@state:{completed}',      // pre-filter expression
    indexName: 'embeddings-idx',       // default: '{queueName}-idx'
    returnFields: ['name', 'summary'], // fields to return (default: all)
    scoreField: '__score',             // score field name (default: '__score')
  },
);

for (const { job, score } of results) {
  console.log(`Job ${job.id}: ${job.name} (score: ${score})`);
  console.log('  Data:', job.data);
}
```

### VectorSearchOptions

```typescript
interface VectorSearchOptions {
  indexName?: string;           // default: '{queueName}-idx'
  k?: number;                   // nearest neighbours (default: 10)
  filter?: string;              // pre-filter expression (default: '*')
  returnFields?: string[];      // fields to return
  scoreField?: string;          // score field name (default: '__score')
  searchOptions?: SearchQueryOptions;
}
```

### VectorSearchResult

```typescript
interface VectorSearchResult<D = any, R = any> {
  job: Job<D, R>;     // fully hydrated Job object
  score: number;       // distance/similarity score
}
```

Score interpretation depends on distance metric:
- **COSINE**: 0 = identical, 2 = opposite (lower is more similar)
- **L2**: 0 = identical (lower is more similar)
- **IP** (inner product): higher is more similar

### SearchQueryOptions

```typescript
interface SearchQueryOptions {
  nocontent?: boolean;          // return only IDs
  dialect?: number;             // query dialect version
  verbatim?: boolean;           // disable stemming
  inorder?: boolean;            // proximity terms must be in order
  slop?: number;                // proximity matching slop
  sortby?: { field: string; order?: 'ASC' | 'DESC' };
  scorer?: string;              // scoring function name
}
```

## Storing Vectors in Jobs

Store the embedding as a Buffer in the job data. The vector field must be written to the job hash as a raw binary FLOAT32 buffer for Valkey Search to index it.

```typescript
// When adding jobs with embeddings
const embedding = await getEmbedding(text);
const vecBuffer = Buffer.from(new Float32Array(embedding).buffer);

await queue.add('document', {
  text,
  summary: 'A document about...',
  category: 'research',
}, {
  // Store the vector in a custom hash field via job data
  // The actual vector must be set on the job hash after creation
});

// Write the vector field to the job hash
const job = await queue.getJob(jobId);
// Use the client directly to HSET the binary vector field
```

## Dropping an Index

```typescript
// Drop by default name
await queue.dropJobIndex();

// Drop by custom name
await queue.dropJobIndex('embeddings-idx');
```

Dropping an index does not delete the job hashes - only the search index is removed.

## Pre-Filter Expressions

Use Valkey Search query syntax for pre-filtering before KNN:

```typescript
// Filter by state
await queue.vectorSearch(embedding, { filter: '@state:{completed}' });

// Filter by job name
await queue.vectorSearch(embedding, { filter: '@name:{summarize}' });

// Filter by priority range
await queue.vectorSearch(embedding, { filter: '@priority:[0 5]' });

// Combine filters
await queue.vectorSearch(embedding, {
  filter: '@state:{completed} @name:{embed|summarize}',
});

// No filter (search all indexed jobs)
await queue.vectorSearch(embedding, { filter: '*' });
```

## Gotchas

- Requires `valkey-search` module loaded on the Valkey server (standalone mode only, not cluster).
- When no `vectorField` is specified in `createJobIndex()`, a minimal 2-dimensional placeholder vector field (`_vec`) is added because valkey-search requires at least one vector field.
- The index prefix is automatically scoped to this queue's job hashes.
- `dropJobIndex()` only removes the index, not the underlying job data.
- Vector search returns fully hydrated `Job` objects - each result triggers an HMGET to fetch the full job hash.
- The `Field` type is re-exported from `@glidemq/speedkey`.
