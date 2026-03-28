# Handover

## Current State

- **Branch**: main
- **Version**: 0.14.0 - released and published to npm
- **All packages published**: glide-mq 0.14.0, speedkey 0.3.0, dashboard 0.3.0, fastify 0.2.0, hapi 0.3.0, hono 0.3.0, nestjs 0.2.0
- **CI**: All green across all repos
- **Tests**: 2312/2312 pass (94/94 files), all plugin tests pass
- **Website**: Updated and deployed (dual MQ + AI positioning)

## What Was Done (2026-03-28)

### glide-mq 0.14.0 (PR #183 merged, then direct push for JSDoc review)
- JobUsage redesigned: Record-based tokens/costs replacing flat fields
- BudgetOptions: per-category caps, weighted totals, currency-agnostic costs
- job.streamChunk(type, content?) convenience method
- Extracted shared helpers: parseJsonRecord, computeWeightedTotal, validateAndResolveUsage
- 9 new examples (agent-budget-loop, multi-model-cost, fallback-usage, streaming-sse, batch-embed-tpm, thinking-model, cost-breakdown, budget-weighted, reasoning-stream)
- ConnectionOptions.requestTimeout added
- valkey-search 1.2 in compose.yaml and CI
- Full JSDoc on all public API (22 interfaces, 8 error classes, 3 main classes)
- All docs/skills/website updated for dual MQ+AI positioning
- WIRE_PROTOCOL, ARCHITECTURE, MIGRATION docs corrected

### speedkey 0.3.0
- Published stable (was rc1)
- Bloom Filter, JSON.MSET, search 1.1/1.2 options
- All 6 platform targets

### All 5 plugins updated
- Dashboard 0.3.0: 3 AI endpoints, AI fields in serialization, 14 new tests
- Fastify 0.2.0: 3 AI endpoints (24 total), AI types, AI SSE events
- Hapi 0.3.0: 3 AI endpoints, fixed stream route API pattern
- Hono 0.3.0: 3 AI endpoints, streamSSE, AI SSE events
- NestJS 0.2.0: AI usage examples in README

### Website (glide-mq.dev)
- Dual positioning: "Fast, Reliable, AI-Ready"
- All integration docs updated: versions, endpoint counts (24), AI endpoint tables
- All old API refs removed

## What Was Done (2026-03-28, continued)

### Validation suite (C:\Users\avife\glidemq\glide-mq-validation)
- All 7 packages installed from npm and validated
- **In-memory suite** (`npm run validate`): 178 checks across 6 apps
  - Rich main package app: lifecycle, reportUsage, streamChunk, readStream, flows, budget, suspend/resume, fallbacks, bulk, metrics, serializer
  - Hono, Fastify, Hapi: createTestApp, HTTP CRUD, AI endpoints (usage, budget, stream SSE)
  - Dashboard: import validation, Express mount, HTML + API
  - NestJS: DI injection, @Processor, WorkerHost, testing mode
- **Live suite** (`npm run validate:live`): 134 checks against real Valkey
  - Standalone (:6379): core 45/45, search 10/10, plugins 12/12
  - Cluster (:7000): core 45/45, search 10/10, plugins 12/12
  - Tests: Queue/Worker, FlowProducer with budget, getFlowUsage/getFlowBudget, createJobIndex, searchJobs, vectorSearch (COSINE), Hono plugin with real connection

### Cluster now uses valkey-bundle with search
- compose.yaml: cluster image changed from valkey:8.0 to valkey-bundle:9.1.0-rc1
- Cluster nodes load search/json/bloom modules via loadmodule in config
- CI: standalone service + cluster binary extraction both use valkey-bundle:9.1.0-rc1
- start-cluster.sh: auto-loads modules if /usr/lib/valkey/*.so exists

## Next Steps

- Bun/Deno NAPI compatibility testing
- Fix the 2 flaky CI tests (broadcast dedup fanout, TPM timing - both timing-sensitive)
- Consider valkey-bundle stable when 9.1.0 releases (currently on rc1)

## API Design Decisions (locked)

- JobUsage.tokens: Record<string, number> not flat fields
- Budget tokenWeights: computed in TS, not Lua
- TPM uses raw (unweighted) totalTokens
- costs/costUnit: currency-agnostic
- streamChunk: thin wrapper over stream(), not new infrastructure
- Search 1.1+ options: forward-compatible types, graceful skip on older servers
- Plugins: AI endpoints under /flows/:id/usage, /flows/:id/budget, /jobs/:id/stream
