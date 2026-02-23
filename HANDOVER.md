# HANDOVER

Current state of the glide-mq repository as of 2026-02-23.

## Branch

`release-prep` - preparing v0.8.0 release. Ready for merge and tag.

## Current task

Release preparation for v0.8.0. All changes are on the `release-prep` branch.

## What changed since v0.7.0 (28 commits)

### Features (12)
- `queue.getJobScheduler(name)` (#51)
- `queue.getWorkers()` (#49)
- `queue.retryJobs(opts?)` (#47)
- `job.promote()` (#46)
- `job.changeDelay(newDelay)` (#45)
- `job.changePriority(newPriority)` (#43)
- `queue.drain(delayed?)` (#41)
- `job.discard()` and `UnrecoverableError` (#40)
- `queue.clean(grace, limit, type)` (#39)
- `active` and `drained` events on Worker/TestWorker (#38)
- Sandboxed processor (#36), sandbox pool stress tests (#37)

### Performance
- Batch `getChildrenValues` for O(1) network trips (#50)
- Batch scheduler operations into single pipeline RTT

### Fixes
- Sanitize stack traces in sandbox runner (#44)
- Replace hardcoded sleeps with `waitFor` in flaky CI tests (#48)
- Resolve 4 source bugs and 12 flaky test files (#34)

### Infrastructure
- CI pipeline rewrite from scratch
- ESLint + Prettier with TypeScript support
- README comprehensive rewrite (#35)

## Release prep changes (this branch)
- Added `"files"` field to package.json (critical fix - `dist/` was not included in npm package)
- Added `.jules/` and `demo/` to `.npmignore`
- Added Apache-2.0 LICENSE file
- Fixed stale docs: MIGRATION.md (active/drained events marked as gaps), USAGE.md (wrong waitUntilFinished signature), ARCHITECTURE.md (wrong LIBRARY_VERSION)
- Bumped version to 0.8.0
- Finalized CHANGELOG.md with dated 0.8.0 section
- Added retroactive git tags: v0.5.0 and v0.7.0

## What comes next

1. Merge `release-prep` into `main`
2. Tag `v0.8.0` on the merge commit
3. Push tags (`git push --tags`) - triggers npm-cd workflow for automatic publish
4. Verify npm package with `npm pack --dry-run` includes `dist/`

## Known state

- Valkey must be on `:6379` (standalone) and `:7000-7005` (cluster) for full integration tests.
- `npm run build` compiles TypeScript to `dist/`.
- `npm test` runs full suite.
- LIBRARY_VERSION is `28`.
- npm-cd workflow triggers on `v*.*.*` tags.
