# CLAUDE.md

Guidance for Claude Code (and Codex / Antigravity) when working in this repository.

## What this project is
**social-ai-infra** — a production-grade content operating system for TikTok / Instagram / YouTube. Not an SMM bot. It is the social-domain port of the `scanner_infra` trade platform's decision framework. See `scanner_infra_social_ai_migration_plan.md` for the full migration blueprint (the authoritative spec).

Object of control:
```
social event → trend → content hypothesis → asset → publish → outcome → governor
```

## Pipeline / stack
```
Go collectors (TikTok/IG/YouTube/Ads/owned) → Redis Streams → Python enrichment/scoring/LLM
→ NestJS control plane → Next.js operator UI → human approval / policy gate
→ publishing adapters → outcome tracking → governors / experiments / replay
```
- Ingestion: Go · Event bus: Redis Streams · Workers: Python · API: NestJS · UI: Next.js
- DB: Postgres + TimescaleDB · Object store: S3/MinIO · Vector: Qdrant
- LLM serving: Ollama (dev) / vLLM (prod) / llama.cpp (golden replay)
- Monitoring: Prometheus + Grafana + Alertmanager · ChatOps: Telegram

## Target repo layout (see plan §8)
`apps/{api,web,chatops-bot}` · `services/{go-gateway,collectors,platform-adapters,python-workers,media}` · `packages/{contracts,config,redis-streams,observability,db,policy,llm-client,shared-types}` · `infra/{docker,compose,migrations,grafana,prometheus,alertmanager}` · `schemas/` · `docs/` · `tests/`

## Non-negotiable invariants
- Answer the user in **Russian**.
- Separate **Facts / Assumptions / Risks**.
- Preserve backward compatibility of Redis-stream envelopes, REST/WS payloads, JSON & LLM-output schemas, adapter ports, and storage — unless a breaking change is explicitly approved (version with `*_v1`).
- Time is explicit: `epoch_ms` UTC by default; bad data = **detect → sanitize → quarantine → metrics**.
- LLM emits **structured JSON validated against a pinned schema** (with `reason_codes`); never act on raw text; LLM never decides "publish or not" directly.
- Publishing defaults to **draft / private / unlisted / shadow** until SLOs are stable. No silent autopublish.
- Governors promote via **off → shadow → canary → enforce** with cohort lift, dwell, TTL fail-safe, and rollback.
- Production changes ship with tests, observability, and a rollout/rollback plan.

## AI tooling in this repo
Three harnesses share one adapted operating pack:
- **Claude Code** → `.claude/` (skills, commands, agents, hooks, settings)
- **Codex** → `.codex/` (skills, workflows, agents, rules, settings)
- **Antigravity** → `.agent/` (skills, workflows, rules)

Entry hint: `s:` / `SOCIAL:` routes through `@social-lead`. Use `social-project-core` for any non-trivial task. Orchestration commands: `/social-rollout`, `/social-replay`, `/social-contract-check`, `/social-new-trend`, `/social-quality-gate`, `/social-release-gate`, `/social-postmortem`, `/social-failure-drill`, `/social-parallel-investigation`, `/social-sequential-review`, and the `social-fast-*` lane for small bounded work. See `.claude/skills/` and `AGENTS.md`.

## Commands (will exist once services are scaffolded)
```bash
# Go collectors
cd services/collectors/<name> && go build ./... && go test ./... && go vet ./...
# Python workers
cd services/python-workers/<name> && python -m pytest
# Control plane / UI
cd apps/api && pnpm test         # NestJS
cd apps/web && pnpm test         # Next.js
# Full stack via modular compose
docker compose up -d
```
