# AGENTS.md — Social AI Infra Operating Contract

Shared operating contract for all AI harnesses (Claude Code, Codex, Antigravity, Cursor). This is the project-agnostic entry; harness-specific packs live in `.claude/`, `.codex/`, `.agent/`, `.cursor/`.

## Project
**social-ai-infra** — production-grade content operating system for TikTok / Instagram / YouTube. Authoritative spec: `scanner_infra_social_ai_migration_plan.md`.

Object of control: `social event → trend → content hypothesis → asset → publish → outcome → governor`.

Stack: Go collectors → Redis Streams → Python (enrich/score/LLM) → NestJS control plane → Next.js UI → human/policy gate → publishing adapters → outcome tracking → governors/replay. Storage: Postgres+Timescale, MinIO, Qdrant. LLM: Ollama/vLLM/llama.cpp.

## Language & entrypoint
- Always respond to the user in **Russian**.
- `s:` / `SOCIAL:` is the canonical entrypoint; route through `@social-lead` first.

## Invariants (apply to every task)
1. Separate **Facts / Assumptions / Risks**.
2. Start with the cheapest sufficient local path; escalate only on explicit high-risk triggers.
3. Preserve backward compatibility of stream envelopes, REST/WS, JSON & LLM schemas, adapter ports, storage — unless a breaking change is explicitly approved (`*_v1` versioning).
4. Time explicit (`epoch_ms` UTC default); bad data = detect → sanitize → quarantine → metrics.
5. LLM output = JSON validated against a pinned schema with `reason_codes`; never act on raw text; LLM never decides publish directly.
6. Publishing defaults to draft/private/unlisted/shadow until SLOs stable; no silent autopublish.
7. Governors: off → shadow → canary → enforce, with cohort lift, dwell, TTL fail-safe, rollback.
8. Production changes ship with tests + observability + rollout/rollback.

## Escalation triggers
>2 subsystems affected · ambiguous prod root cause · architecture/schema/retention/ML/replay/governor redesign · possible breaking contract change · LLM prompt/schema version redesign · platform-adapter autopublish enablement · production incident RCA · rollout go/no-go.

## Specialist roles
Full catalog: `.claude/agents/agents.md` (mirrored to `.codex/agents/agents.md`, `.agent/agents.md`).
`@social-lead` (orchestrator) · `@go-collector-engineer` · `@python-worker-engineer` · `@llm-content-engineer` · `@platform-adapter-engineer` · `@media-pipeline-engineer` · `@control-plane-engineer` · `@timeseries-dba` · `@commerce-attribution-engineer` · `@policy-critic` · `@ml-replay-engineer` · `@strategy-governor` · `@sre-rollout` · `@contract-governor` · `@quality-gatekeeper`.

## Skills & workflows
Knowledge skills: `social-project-core`, `social-go-redis-ingest`, `social-data-quality-time`, `social-timescale-postgres`, `social-observability-rollout`, `social-governor`, `social-llm-content-planner`, `social-platform-adapter`, `social-media-pipeline`, `social-publish-policy`, `social-outcome-attribution`, `social-trend-scoring`. Plus generic stack skills (golang/python patterns+testing, nestjs/nextjs/react, redis/postgres patterns, api-design, LLM eval/cost, content-engine, canary-watch, …).

Orchestration workflows / commands: `social-rollout`, `social-replay`, `social-contract-check`, `social-new-trend`, `social-quality-gate`, `social-release-gate`, `social-postmortem`, `social-failure-drill`, `social-parallel-investigation`, `social-sequential-review`, `social-fast-{fix,contract-check,test-gen,log-triage,doc-update}`, `tasks`.

## Response format (when proposing changes)
Goal · What we have · Plan · Details (code/SQL/ENV/schema) · Tests · Metrics/logs/alerts · Rollout/rollback · Prod checklist.
