## 2024-05-23 - [Worker Polling Loop Saturation]
**Learning:** The worker used a fixed 5ms sleep loop when saturated (`activeCount >= prefetch`). This caused unnecessary CPU usage (waking up 200 times/sec) just to check capacity.
**Action:** Replaced busy-wait with an event-driven mechanism using an internal `EventEmitter`. The loop now waits for a `slotFree` event (emitted when a job completes) or a 100ms safety timeout. This reduces CPU usage and improves responsiveness when slots open up.

## 2026-03-01 - [Valkey Stream Hot Path Parsing]
**Learning:** `Object.entries()` and `for...of` loops with destructuring (`for (const [key, val] of array)`) cause high garbage collection pressure in hot polling paths like `Worker.pollOnce` because they allocate iterators and intermediate array pairs for every iteration.
**Action:** Used `for...in` loops (with `hasOwnProperty` check) for object iteration and traditional index-based `for` loops (`for (let i = 0; i < len; i++)`) for arrays to prevent unnecessary heap allocations during high-throughput message processing.
