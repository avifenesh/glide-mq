/**
 * Vector Search - Semantic search with Valkey
 *
 * Real scenario: Store documents as JSON with vector embeddings, TAG, and
 * NUMERIC fields, then query by KNN similarity with optional tag pre-filters.
 *
 * Uses speedkey GlideFt/GlideJson directly (no glide-mq Queue needed).
 * Vectors are synthetic (hash-based) since OpenRouter lacks embedding endpoints.
 *
 * Run: npx tsx examples/vector-search.ts
 */
import { GlideClient, GlideFt, GlideJson } from '@glidemq/speedkey';

const CONNECTION = { addresses: [{ host: 'localhost', port: 6379 }] };

const DIM = 32;
const INDEX = `vec-idx-${Date.now()}`;
const PREFIX = `doc:${Date.now()}:`;

const DOCS = [
  { id: 'ml-intro', category: 'ml', score: 95, title: 'Introduction to Machine Learning', body: 'Machine learning is a subset of artificial intelligence.' },
  { id: 'queue-basics', category: 'infra', score: 88, title: 'Message Queue Fundamentals', body: 'Message queues decouple producers from consumers.' },
  { id: 'vector-db', category: 'ml', score: 92, title: 'Vector Databases Explained', body: 'Vector databases store high-dimensional embeddings.' },
  { id: 'llm-serving', category: 'infra', score: 90, title: 'Serving Large Language Models', body: 'LLM serving requires GPU infrastructure and batching.' },
  { id: 'rag-pattern', category: 'ml', score: 97, title: 'Retrieval-Augmented Generation', body: 'RAG grounds LLM responses in relevant documents.' },
];

/** Deterministic hash-based embedding: maps text to a normalized float32 vector. */
function hashEmbed(text: string, dim: number): number[] {
  const vec = new Array(dim).fill(0);
  for (let i = 0; i < text.length; i++) {
    const idx = i % dim;
    vec[idx] += text.charCodeAt(i) * (1 + (i / text.length));
  }
  const mag = Math.sqrt(vec.reduce((s, v) => s + v * v, 0)) || 1;
  return vec.map(v => v / mag);
}

function toBuffer(vec: number[]): Buffer {
  const buf = Buffer.alloc(vec.length * 4);
  vec.forEach((v, i) => buf.writeFloatLE(v, i * 4));
  return buf;
}

/** Parse FT.SEARCH result fields into a flat object, extracting JSON doc fields. */
function parseResultFields(value: any): Record<string, string> {
  const fields: Record<string, string> = {};
  if (!Array.isArray(value)) return fields;
  for (const f of value) {
    const k = String(f.key);
    const v = String(f.value);
    if (k === '$') {
      // JSON document - parse and merge
      try {
        const doc = JSON.parse(v);
        for (const [dk, dv] of Object.entries(doc)) {
          if (dk !== 'vec') fields[dk] = String(dv);
        }
      } catch {}
    } else {
      fields[k] = v;
    }
  }
  return fields;
}

async function main() {
  const client = await GlideClient.createClient({ addresses: CONNECTION.addresses });

  // 1. Create index with TAG + NUMERIC + VECTOR fields on JSON docs
  console.log(`Creating index "${INDEX}" with TAG, NUMERIC, and VECTOR fields...`);
  await GlideFt.create(client, INDEX, [
    { type: 'TAG', name: '$.category', alias: 'category' },
    { type: 'NUMERIC', name: '$.score', alias: 'score' },
    {
      type: 'VECTOR',
      name: '$.vec',
      alias: 'VEC',
      attributes: {
        algorithm: 'HNSW',
        dimensions: DIM,
        distanceMetric: 'COSINE',
        type: 'FLOAT32',
        numberOfEdges: 16,
      },
    },
  ], { dataType: 'JSON', prefixes: [PREFIX] });
  console.log('[OK] Index created\n');

  // 2. Store documents with embeddings
  console.log('Storing 5 documents...');
  for (const doc of DOCS) {
    const vec = hashEmbed(doc.title + ' ' + doc.body, DIM);
    const payload = JSON.stringify({
      category: doc.category,
      score: doc.score,
      title: doc.title,
      body: doc.body,
      vec,
    });
    await GlideJson.set(client, `${PREFIX}${doc.id}`, '$', payload);
    console.log(`  ${doc.id}: "${doc.title}" [${doc.category}]`);
  }

  // Brief pause for indexing
  await new Promise(r => setTimeout(r, 500));

  // 3. KNN search - find 3 docs most similar to "AI models and embeddings"
  console.log('\n--- KNN Search: "AI models and embeddings" (top 3) ---');
  const queryVec = hashEmbed('AI models and embeddings', DIM);
  const queryBuf = toBuffer(queryVec);

  const knnResults = await GlideFt.search(
    client, INDEX, '*=>[KNN 3 @VEC $query_vec]',
    { params: [{ key: 'query_vec', value: queryBuf }] },
  );
  console.log(`  ${knnResults[0]} result(s)`);
  if (knnResults[1]) {
    for (const entry of knnResults[1]) {
      const fields = parseResultFields(entry.value);
      console.log(`  key=${entry.key}  category=${fields.category}  score=${fields.score}  distance=${fields.__VEC_score}`);
    }
  }

  // 4. Filtered KNN - only "ml" category documents
  console.log('\n--- Filtered KNN: category=ml (top 3) ---');
  const filteredResults = await GlideFt.search(
    client, INDEX, '(@category:{ml})=>[KNN 3 @VEC $query_vec]',
    { params: [{ key: 'query_vec', value: queryBuf }] },
  );
  console.log(`  ${filteredResults[0]} result(s) (ml only)`);
  if (filteredResults[1]) {
    for (const entry of filteredResults[1]) {
      const fields = parseResultFields(entry.value);
      console.log(`  key=${entry.key}  category=${fields.category}  distance=${fields.__VEC_score}`);
    }
  }

  // 5. Different query vector - "distributed systems and queues"
  console.log('\n--- KNN Search: "distributed systems and queues" (top 2) ---');
  const q2Vec = hashEmbed('distributed systems and queues', DIM);
  const q2Buf = toBuffer(q2Vec);
  const infraResults = await GlideFt.search(
    client, INDEX, '*=>[KNN 2 @VEC $query_vec]',
    { params: [{ key: 'query_vec', value: q2Buf }] },
  );
  console.log(`  ${infraResults[0]} result(s)`);
  if (infraResults[1]) {
    for (const entry of infraResults[1]) {
      const fields = parseResultFields(entry.value);
      console.log(`  key=${entry.key}  category=${fields.category}  distance=${fields.__VEC_score}`);
    }
  }

  // 6. Index info
  console.log('\n--- Index Info ---');
  const info = await GlideFt.info(client, INDEX);
  console.log(`  index_name=${info.index_name}, num_docs=${info.num_docs}, status=${info.index_status}`);

  // Cleanup
  console.log('\nCleaning up...');
  await GlideFt.dropindex(client, INDEX);
  for (const doc of DOCS) await client.del([`${PREFIX}${doc.id}`]);
  client.close();

  console.log('[OK] Vector search example complete');
  process.exit(0);
}

main().catch((err) => {
  console.error('[ERROR]', err);
  process.exit(1);
});
