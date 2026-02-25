## 2024-05-23 - [Worker Polling Loop Saturation]
**Learning:** The worker used a fixed 5ms sleep loop when saturated (`activeCount >= prefetch`). This caused unnecessary CPU usage (waking up 200 times/sec) just to check capacity.
**Action:** Replaced busy-wait with an event-driven mechanism using an internal `EventEmitter`. The loop now waits for a `slotFree` event (emitted when a job completes) or a 100ms safety timeout. This reduces CPU usage and improves responsiveness when slots open up.

## 2026-02-25 - [Synchronizing Lua Library]
**Learning:** The Lua library source (`src/functions/glidemq.lua`) was out of sync with the embedded string in `src/functions/index.ts`. This caused a risk of regression when modifying the Lua file and propagating it to TypeScript. Always verify source of truth.
**Action:** When modifying embedded code, check consistency between the standalone file and the embedded version first. Restore from the embedded version if the standalone file seems stale.
