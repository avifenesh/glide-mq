# Critical LIFO Fixes Needed

## Status

Completed:
- ✅ Fix #3: Args order in dedup (customJobId before lifo)

Remaining Critical Fixes:

## Fix #1: Worker Initial Fetch (P1 - BLOCKING)

**File**: `src/worker.ts` line ~385

**Problem**: Worker only uses XREADGROUP for initial fetch. LIFO jobs in LIST never fetched until a stream job completes.

**Solution**: Before XREADGROUP (line 388), try RPOP from LIFO list:

```ts
// NEW CODE before line 386
// Try LIFO list first (non-blocking fetch)
try {
  const lifoJobId = await this.commandClient.rpop(this.queueKeys.lifo);
  if (lifoJobId) {
    // Fetch job hash
    const jobHash = await this.commandClient.hgetall(this.queueKeys.job(String(lifoJobId)));
    if (jobHash && Object.keys(jobHash).length > 0) {
      // Activate and dispatch LIFO job (no entryId)
      await this.activate WithoutEntryId(String(lifoJobId), jobHash);
      return; // Skip XREADGROUP this cycle
    }
  }
} catch (err) {
  // Log but don't fail - fall through to stream fetch
}

// EXISTING CODE: XREADGROUP (line 386-391)
```

**New helper method needed**:
```ts
private async activateWithoutEntryId(jobId: string, jobHash: Record<string, string>): Promise<void> {
  // Similar to existing activate logic but with entryId = ''
  // Store '' as entryId so completeJob/failJob can handle it
}
```

## Fix #2: Empty EntryId Handling (P1 - BLOCKING)

**Files**: `src/functions/index.ts` (completeJob, failJob Lua functions)

**Problem**: LIFO jobs have entryId='', but functions unconditionally call XACK/XDEL.

**Solution**: Add conditional XACK/XDEL only if entryId != '':

In `glidemq_complete` (line ~590):
```lua
-- BEFORE
redis.call('XACK', streamKey, group, entryId)
redis.call('XDEL', streamKey, entryId)

-- AFTER
if entryId ~= '' then
  redis.call('XACK', streamKey, group, entryId)
  redis.call('XDEL', streamKey, entryId)
end
```

Same fix needed in:
- `glidemq_fail`
- `glidemq_completeAndFetchNext` (line 691-692)
- Any other function that uses entryId for XACK/XDEL

## Fix #4: addBulk Support (P2)

**File**: `src/queue.ts` addBulk method

Add lifo to bulk job args. Currently addBulk doesn't pass lifo to Lua.

## Fix #5: FlowProducer Support (P2)

**File**: `src/flow-producer.ts`

Add lifo to flow job options. FlowProducer.add needs to pass lifo through.

## Fix #6: Cleanup Functions (P2)

**Files**:
- `src/queue.ts` obliterate method
- `tests/helpers/fixture.ts` flushQueue helper

Add `k.lifo` to cleanup key enumeration.

## Fix #7: Documentation (P2)

- Remove `defaultJobOptions` from PR description (not supported)
- Fix MIGRATION.md line 1123 table formatting (remove 3-column row)

## Testing After Fixes

1. Build: `npm run build`
2. Unit tests: `npx vitest run tests/lifo.test.ts`
3. Integration tests: `npm test`
4. Manual test: Add LIFO jobs before starting worker

## Estimated Complexity

High. Worker polling change is non-trivial and touches core architecture.
Empty entryId handling requires updating multiple Lua functions.

Consider: Is the LIST approach worth the complexity vs alternatives?
