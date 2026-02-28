## 2024-05-15 - [Security Enhancements to Utilities]
**Vulnerability:** `hashDataToRecord` instantiated a standard object `{}` which could be vulnerable to prototype pollution if a `__proto__` key is received from the datastore.
**Learning:** Deserialization of key-value pairs from external stores must use prototype-less objects (`Object.create(null)`) to prevent prototype pollution attacks. Note: Changing backoff jitter to CSPRNG is unnecessary security theater and causes performance regressions.
**Prevention:** Always use `Object.create(null)` when building dictionaries from external data.