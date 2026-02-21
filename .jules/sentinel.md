## 2025-02-18 - Insecure Randomness in ID Generation
**Vulnerability:** Used `Math.random()` for generating job IDs and consumer IDs.
**Learning:** `Math.random()` is not cryptographically secure and can lead to predictable IDs, which is a security risk (CWE-330).
**Prevention:** Use `crypto.randomBytes()` or `crypto.randomUUID()` for generating security-sensitive random values.
