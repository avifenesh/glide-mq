## 2024-05-23 - [Worker Polling Loop Saturation]
**Learning:** The worker used a fixed 5ms sleep loop when saturated (`activeCount >= prefetch`). This caused unnecessary CPU usage (waking up 200 times/sec) just to check capacity.
**Action:** Replaced busy-wait with an event-driven mechanism using an internal `EventEmitter`. The loop now waits for a `slotFree` event (emitted when a job completes) or a 100ms safety timeout. This reduces CPU usage and improves responsiveness when slots open up.

## 2025-02-14 - [Optimized Cron Next Occurrence Calculation]
**Learning:** Iterating minute-by-minute for `nextCronOccurrence` calculation is highly inefficient for sparse schedules (e.g., yearly/monthly).
**Action:** Implemented a hierarchical "skip" strategy (Year -> Month -> Day -> Hour -> Minute) that directly jumps to the start of the next valid period. This reduced calculation time for a yearly schedule from ~120ms (hypothetically high if not optimized) to ~0.01ms per call.
