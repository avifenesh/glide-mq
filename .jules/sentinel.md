## 2026-02-22 - Stack Trace Leakage in Sandbox
**Vulnerability:** Sandboxed processors leaked absolute paths from the worker environment in error stack traces.
**Learning:** Worker processes often run in different environments or with different privileges. Trusting their stack traces blindly leaks internal path information.
**Prevention:** Sanitize stack traces at the boundary (sandbox runner) before sending them over IPC.
