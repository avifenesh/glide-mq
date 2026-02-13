# Handover

## Current State (2026-02-14)

### speedkey package
- Extracted valkey-glide Node.js + Rust core from PR #5325 (ipc-replacement-exploration)
- Repo: github.com/avifenesh/speedkey (private)
- Local path: C:\Users\avife\speedkey
- Package renamed to `speedkey` (unscoped npm)
- Targets: linux x64/arm64 (gnu+musl), darwin x64/arm64, win32 x64 msvc
- CD triggered via v0.1.0 tag push (run 22004711981)
- Build status: 6/7 passed, arm64-gnu queued waiting on runner
- `prepare-and-publish` blocked until arm64-gnu completes
- NPM_TOKEN secret set on repo
- Local build verified on Windows

### glide-mq project
- Research phase. No code scaffolded yet.
- Two deep research guides (82 sources total) in agent-knowledge/
- CLAUDE.md written with project rules

### Research completed
- agent-knowledge/nodejs-queue-libs-redis-valkey.md - BullMQ, Bull, Bee-Queue internals, Redis data structures, Lua scripts, job state machines, reliability patterns
- agent-knowledge/valkey-glide-nodejs-client.md - GLIDE architecture (Rust core + NAPI), 380+ commands, cluster, pub/sub, batching, migration from ioredis

### Architecture (approved)
- docs/ARCHITECTURE.md - full architecture plan
- Streams-first design, PEL as active set, XAUTOCLAIM for stalled recovery
- Valkey Functions (FUNCTION LOAD/FCALL) instead of EVAL scripts - single library `glidemq`
- Cluster-native from day one (hash tags on all keys)
- speedkey-only (no ioredis/node-redis abstraction)
- 4 phases: Core -> Advanced -> Flows+Events -> Production Hardening

### Next steps
- Scaffold TypeScript project with speedkey as dependency
- Implement Phase 1: connection factory, key utils, core Lua functions, Queue, Worker, Job
- Wait for speedkey CD to finish (arm64-gnu runner still queued)
- Rotate NPM token
