# Runtime compatibility

glide-mq is built for Node.js. Bun and Deno run the same `dist/` build through their
Node compatibility layers and load the `@glidemq/speedkey` NAPI binary through the
same `require()` path Node uses. This page records what was tested, on which
versions, and what is known not to work.

## Supported runtimes

| Runtime | Status                                                        | Versions tested              |
| ------- | ------------------------------------------------------------- | ---------------------------- |
| Node.js | Supported (primary). CI runs the full suite on 20 and 22.     | 20, 22 (CI), 26.10.0 (local) |
| Bun     | Smoke-tested. In-memory suite and a Valkey-backed slice pass. | 1.4.2 (linux/x64)            |
| Deno    | Smoke-tested. In-memory suite and a Valkey-backed slice pass. | 2.9.7 (linux/x64)            |

Tested against Valkey 9.0.4 (standalone :6379 and a 6-node cluster :7000-7005),
`@glidemq/speedkey` 0.3.0, vitest 4.1.11, on Linux x86_64 with glibc. macOS and
Windows under Bun or Deno were not tested.

## What passed

The same checks ran under all three runtimes:

- NAPI load: `require('@glidemq/speedkey')` resolves `speedkey.linux-x64-gnu.node`, `PING` works.
- `tests/testing-mode.test.ts` (110 tests, no Valkey): vitest under Bun (`bunx --bun vitest`), Bun's own runner (`bun test`, which remaps `vitest` imports to `bun:test`), and vitest under Deno (`deno run -A npm:vitest`).
- Valkey-backed slice via vitest, standalone and cluster: `sandbox.test.ts`, `sandbox-integration.test.ts`, `flow.test.ts`, `broadcast.test.ts`, `compression.test.ts`, `graceful-shutdown.test.ts` (102 tests standalone, 88 more with cluster).
- `scripts/compat/` smoke (10 steps): NAPI load, dist load, Queue + Worker + QueueEvents, gzip compression (zlib + Buffer round trip), sandbox processor in `worker_threads`, sandbox processor in `child_process.fork`, FlowProducer parent/children with `getChildrenValues`, Broadcast fan-out to two BroadcastWorkers, `process.on(signal)` round trip plus `gracefulShutdown` registration, obliterate cleanup.

Sandbox children were verified to run inside the host runtime: `child_process.fork`
and `new Worker()` spawn Bun under Bun and Deno under Deno, not Node.

No glide-mq code change was needed for either runtime.

## Known limitations

### Bun

- `bun x vitest` (or `bunx vitest`) honors vitest's `#!/usr/bin/env node` shebang and runs the test workers under Node. Use `bunx --bun vitest run ...` to run them in Bun.
- `bun test` passes for `tests/testing-mode.test.ts`. The rest of the suite was not run under `bun test`; use vitest for it.

### Deno

- Node compat needs a `node_modules` directory (`npm install` next to a `package.json`). Deno detects it automatically; `--node-modules-dir=manual` is equivalent. `npm:@glidemq/speedkey` also loads the NAPI binary without `node_modules`.
- Permissions: the smoke needs `--allow-ffi` (NAPI), `--allow-read`, `--allow-env`, `--allow-net`, and `--allow-sys` (`os.hostname()` for the Worker consumer name, error: `Requires sys access to "hostname"`). Sandboxed processors with `sandbox.useWorkerThreads: false` additionally need `--allow-run`; without it the fork is denied and the job fails with `Requires --allow-run permissions to spawn subprocess with LD_LIBRARY_PATH environment variable. Alternatively, spawn with the environment variable unset.` (Deno 2.9.7). `-A` covers all of these.
- `deno test` was not used; vitest runs under Deno via `deno run -A npm:vitest`.

### Not covered by this smoke

- TLS, IAM credentials, the HTTP proxy (`glide-mq/proxy`), search indexes, and the OpenTelemetry hooks were not exercised under Bun or Deno.
- Windows and macOS under Bun or Deno.

## Running the smoke

Requires `npm run build` and Valkey on `localhost:6379` (`VALKEY_HOST` / `VALKEY_PORT`
override). Queue names are unique per run and obliterated at the end.

```bash
npm run compat:node   # node scripts/compat/node-smoke.mts (Node 22.18+ type stripping)
npm run compat:bun    # bun scripts/compat/bun-smoke.ts
npm run compat:deno   # deno run -A scripts/compat/deno-smoke.ts
```

Each step prints `[OK]` or `[ERROR]` with the failure text; the process exits 1 on any
failure. The scenario is shared in `scripts/compat/smoke-core.mts`; the entry files
only pick the runtime. CI runs the Bun and Deno smoke against a Valkey service on
every push and pull request (job `compat-runtimes`).
