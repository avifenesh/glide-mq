## 2024-05-24 - Prototype Pollution via JSON.parse
**Vulnerability:** Internal state objects parsed via `JSON.parse` lacked protection against prototype pollution when the underlying data is modified.
**Learning:** Even internal system data loaded via Redis might be susceptible to tampering if user payloads are commingled or improperly sanitized before storage.
**Prevention:** Introduce a custom `jsonReviver` function to explicitly drop `__proto__`, `constructor`, and `prototype` keys during JSON deserialization of critical system objects.
