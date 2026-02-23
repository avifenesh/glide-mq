# HANDOVER

Current state of the glide-mq repository as of 2026-02-23.

## Branch

`main` - v0.8.0 release prep merged. Documentation audit in progress.

## Current task

Comprehensive documentation audit - fixing 28 issues found across all docs (wrong API signatures, stale content, missing methods, incorrect examples).

## Recent history

v0.8.0 release prep merged (PR #52). Key changes:
- Added `"files"` field to package.json (critical npm packaging fix)
- Added Apache-2.0 LICENSE file
- Fixed stale docs, bumped version to 0.8.0, finalized CHANGELOG
- Added retroactive git tags: v0.5.0, v0.7.0

## What comes next

1. Tag `v0.8.0` on main and push to trigger npm-cd workflow
2. Verify npm package on registry

## Known state

- Valkey must be on `:6379` (standalone) and `:7000-7005` (cluster) for full integration tests.
- `npm run build` compiles TypeScript to `dist/`.
- `npm test` runs full suite.
- LIBRARY_VERSION is `28`.
- npm-cd workflow triggers on `v*.*.*` tags.
- 26 Lua functions in single library (`src/functions/index.ts`).
