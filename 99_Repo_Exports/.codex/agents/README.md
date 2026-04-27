# Codex Agent Roles

See `agents.md` for the full specialist catalog.

Codex does not need to launch named Claude agents to use these roles. For this repository, use the roles as structured review perspectives:

- `@trade-lead`: default dispatcher and merger for `tr:` requests.
- `@quality-gatekeeper`: pass/fail criteria and regression barriers.
- `@contract-governor`: Redis, WebSocket, REST, and storage compatibility.
- `@latency-benchmarker`: p50/p95/p99, allocations, throughput, and backpressure.
- `@sre-rollout`: observability, alerts, rollout, rollback, and stop conditions.

Only pull in additional role perspectives when the task scope requires them.

