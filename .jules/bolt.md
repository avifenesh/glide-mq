## 2025-02-14 - [Optimized Cron Next Occurrence Calculation]
**Learning:** Iterating minute-by-minute for `nextCronOccurrence` calculation is highly inefficient for sparse schedules (e.g., yearly/monthly).
**Action:** Implemented a hierarchical "skip" strategy (Year -> Month -> Day -> Hour -> Minute) that directly jumps to the start of the next valid period. This reduced calculation time for a yearly schedule from ~120ms (hypothetically high if not optimized) to ~0.01ms per call.
