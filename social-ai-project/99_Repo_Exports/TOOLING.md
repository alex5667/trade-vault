# AI Tooling — Social AI Infra

This repo ships one adapted operating pack across three AI harnesses, ported and adapted from `scanner_infra` (trade) and `everything-claude-code`, themed for the social content-OS domain (TikTok / Instagram / YouTube). Authoritative project spec: `scanner_infra_social_ai_migration_plan.md`.

## Harness layout

| Harness | Dir | Contains |
|---|---|---|
| Claude Code | `.claude/` | `skills/`, `commands/`, `agents/`, `hooks/`, `settings.json`, `hooks.json` |
| Codex | `.codex/` | `skills/`, `workflows/`, `agents/`, `rules/`, `hooks/`, `settings.json`, `README.md` |
| Antigravity | `.agent/` | `skills/`, `workflows/`, `rules/`, `agents.md` |
| Cursor | `.cursor/` | `rules/` |

Antigravity convention: `agents → .agent/skills`, `commands → .agent/workflows`, flat `.agent/rules`. The same `SKILL.md` bodies are mirrored across `.claude`, `.codex`, `.agent`.

Root contracts: `CLAUDE.md` (project guidance) and `AGENTS.md` (shared operating contract).

## Skills (53 per harness)

**Adapted social domain skills (26)** — written for this project:
- Knowledge: `social-project-core`, `social-go-redis-ingest`, `social-data-quality-time`, `social-timescale-postgres`, `social-observability-rollout`, `social-governor`, `social-llm-content-planner`, `social-platform-adapter`, `social-media-pipeline`, `social-publish-policy`, `social-outcome-attribution`, `social-trend-scoring`
- Orchestration / commands: `social-rollout`, `social-replay`, `social-contract-check`, `social-new-trend`, `social-quality-gate`, `social-release-gate`, `social-postmortem`, `social-failure-drill`, `social-parallel-investigation`, `social-sequential-review`, `social-fast-{fix,contract-check,test-gen,log-triage,doc-update}`
- `tasks` (Telegram/ChatOps inbox runner)

**Generic stack skills (26)** — copied as-is, language/stack-neutral:
- From scanner_infra: `golang-patterns`, `golang-testing`, `python-patterns`, `python-testing`, `mle-workflow`
- From everything-claude-code: `nestjs-patterns`, `nextjs-turbopack`, `react-patterns`, `react-performance`, `react-testing`, `postgres-patterns`, `redis-patterns`, `api-design`, `backend-patterns`, `api-connector-builder`, `cost-aware-llm-pipeline`, `prompt-optimizer`, `agent-eval`, `eval-harness`, `ai-regression-testing`, `iterative-retrieval`, `content-engine`, `canary-watch`, `coding-standards`, `e2e-testing`

## Reviewer agents (`.claude/agents/`)
`go-reviewer`, `python-reviewer`, `typescript-reviewer`, `react-reviewer`, `fastapi-reviewer`, `database-reviewer`, `security-reviewer`, `performance-optimizer`, `mle-reviewer`, `go-build-resolver`, `code-reviewer`, plus the `agents.md` specialist role catalog.

## Domain mapping (trade → social)
| scanner_infra | social-ai-infra |
|---|---|
| signal / detector | trend candidate / content strategy |
| forward gate | publish gate (`social-publish-policy`) |
| alpha_forecast_v2 governor | `social-governor` (off→shadow→canary→enforce) |
| tick/book archive + replay | raw event/media archive + `social-replay` |
| feature registry contract | trend/content feature registry (`social-trend-scoring`) |
| Go ingest → Redis | Go collectors → Redis Streams (`social-go-redis-ingest`) |
| PnL / R-multiple | watch time / CTR / CVR / revenue / LTV (`social-outcome-attribution`) |

## Conventions
- User-facing language: **Russian**. Entrypoint `s:` → `@social-lead`.
- Fast lane (`social-fast-*`) for bounded 1–2 file changes; escalate per `.codex/settings.json` triggers.
- Hooks (`pre_tool_use.py` / `post_tool_use.py`) are generic guards, active in Claude Code via `hooks.json`; reference-only for Codex.
