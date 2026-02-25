## 2024-10-24 - DoS Vulnerability in Cron Parser

**Vulnerability:** The cron parser allowed a step value of `0` (e.g. `*/0`), causing an infinite loop in the `nextCronOccurrence` function. This could lead to a Denial of Service (DoS) if a malicious user submitted a job with such a pattern.

**Learning:** Input validation is crucial for loops where the increment depends on user input. The regex used for parsing allowed `0` as a valid step because `\d+` matches `0`.

**Prevention:** Explicitly validate that loop increments (steps) are positive. Add bounds checking for all user-controlled numeric inputs to prevent excessive resource consumption (memory or CPU).
