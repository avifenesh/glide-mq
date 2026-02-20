# Project

- glide-mq: Message queue library for Node.js built on Valkey/Redis using valkey-glide native client
- Language: TypeScript, Rust core (via NAPI bindings)
- Status: v0.4.0 published on npm. Feature-complete with 750+ test executions.
- Client: @glidemq/speedkey (npm) - valkey-glide with direct NAPI bindings
- Dashboard: @glidemq/dashboard (separate repo) - Express middleware
- Repos: github.com/avifenesh/glide-mq, github.com/avifenesh/speedkey, github.com/avifenesh/glidemq-dashboard
- Research guides in agent-knowledge/ (gitignored)

# Architecture

- All queue operations use Valkey Server Functions (FUNCTION LOAD + FCALL), not EVAL scripts.
- Function library: src/functions/index.ts (loaded once per connection, persistent across restarts).
- Keys are hash-tagged (glide:{queueName}:\*) for cluster compatibility.
- Streams-first: XREADGROUP + consumer groups + PEL for at-least-once delivery.
- completeAndFetchNext: single FCALL that completes current job + fetches next (1 RTT/job).

<critical-rules>

# Rules

## Communication

- No emojis. Plain text markers only: [OK], [ERROR], [WARN].
- Concise, direct, brutally precise. Say what is needed, nothing more.
- Save tokens. No verbose summaries, no fluff.
- Never summarize end of task in a file unless instructed. No _\_SUMMARY.md, _\_AUDIT.md, \*\_COMPLETION.md files.
- Do not over-clutter with docs. Only what is truly necessary.
- If unsure, ask. Never assume.
- Tell me when I am wrong. Do not sugarcoat.
- Never ignore my instructions. If I instruct, nothing is more valuable than that.
- In prose, use a single dash (-) not double dash (--) for separators and asides.

## Code Quality

- Correctness above all. We can afford friction or edge-case failures over incorrectness.
- Always check the speedkey/valkey-glide API before using it. Read the actual type signatures. Never assume you know the API.
- Never use `customCommand`. Always use the typed API methods. If FCALL needs deterministic routing in cluster mode, pass a dummy hash-tagged key like `{glidemq}:_` instead of using customCommand with route options.
- Before every commit, review your own code.
- Commit every logical change separately - makes bisect/revert possible and review cleaner.
- Run /deslop before every push (skill that removes dead code, debug logs, ghost code, unnecessary comments).
- A task is not done unless covered by tests, review orchestration, and delivery approval.
- Fix all test failures. Never skip as "out of scope" or "pre-existing".
- Always run git hooks. If a hook blocks, fix the reported issue.

## Reviews

- Address ALL review comments before merging - even minor ones.
- If you disagree with a comment, respond in the review. Do not silently ignore.

## Problem Solving

- Never guess-fail-guess-fail. If something does not work, search the web for the correct way.
- Fetch the web frequently. Consult codex frequently for anything that might raise a question.
- Do not give up easily. Keep digging when a challenge appears.
- Report script/tool failures before manual fallback. Never silently work around broken tooling - fix the tool.

## Workflow

- For every task that is more than a few simple changes, go into plan mode unless instructed not to.
- Keep HANDOVER.md at project root up to date. We never know when the next session will be.

</critical-rules>

# Commands

- `npm run build` - compile TypeScript to dist/
- `npm test` - run all tests (unit + integration, needs Valkey on :6379 and cluster on :7000-7005)
- `npx vitest run tests/integration.test.ts` - standalone integration only
- `npx vitest run tests/search.test.ts` - search feature tests
- `npx vitest run tests/testing-mode.test.ts` - in-memory testing mode (no Valkey needed)
- `npm run bench` - run benchmarks vs BullMQ
- Import `{ TestQueue, TestWorker }` from `glide-mq/testing` for in-memory testing (no Valkey needed)

<end-reminder>

**REMEMBER**: Correctness over speed. Ask when unsure. Fix failures, do not skip them. Keep HANDOVER.md current. Never use customCommand - use typed API with dummy keys for routing.

</end-reminder>
