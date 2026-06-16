# social-ai-infra

Production-grade **content operating system** for TikTok / Instagram / YouTube — the social-domain port of the `scanner_infra` trade decision framework. Not an SMM bot.

> Authoritative spec: [`scanner_infra_social_ai_migration_plan.md`](./scanner_infra_social_ai_migration_plan.md). AI tooling: [`TOOLING.md`](./TOOLING.md). Working contract: [`CLAUDE.md`](./CLAUDE.md) / [`AGENTS.md`](./AGENTS.md).

Object of control:
```
social event → trend → content hypothesis → asset → publish → outcome → governor
```

## Pipeline
```
Go collectors → Redis Streams → Python (enrich/score/LLM)
→ NestJS control plane → Next.js operator UI → human approval / policy gate
→ publishing adapters → outcome tracking → governors / experiments / replay
```

## Quick start (Phase 0)
```bash
make env          # create .env from .env.example
make up           # Redis ×6 + Postgres/Timescale + MinIO + Qdrant + Prometheus + Grafana + Alertmanager
make ps
```
Then:
- Grafana → http://localhost:3001 (admin/admin) — "Social AI Infra — Overview" dashboard
- Prometheus → http://localhost:9090
- MinIO console → http://localhost:9001
- Postgres → `make psql` (migrations in `infra/migrations/` auto-applied on first boot)

Bring up the rest as you build it (modular fragments, plan §4.1):
```bash
make up-backend     # go-gateway (:8080) + NestJS api (:3000)
make up-front       # Next.js web (:3002)
make up-chatops     # Telegram bot
make up-workers     # python workers
make up-collectors  # go collectors
make up-llm         # ollama
make up-governors   # governor worker (Phase 10)
make up-replay      # run a replay job
make up-all         # everything — full Phase 0 acceptance (infra + monitoring + API + UI)
```

## Layout (plan §8)
```
apps/        api (NestJS) · web (Next.js) · chatops-bot
services/    go-gateway · collectors/* · platform-adapters/* · python-workers/* · media/*
packages/    contracts · config · redis-streams · observability · db · policy · llm-client · shared-types
infra/       compose/ · docker/ · migrations/ · grafana/ · prometheus/ · alertmanager/
schemas/     social_event.v1 · trend_candidate.v1 · content_brief.v1 · render_job.v1 · publish_job.v1 · publish_status.v1 · outcome_event.v1 · policy_decision.v1
docs/        architecture · runbooks · rollout · policy · api
tests/       golden · contract · replay · e2e
```

## Safety invariants (plan §23)
- Publishing defaults to **draft / private / unlisted / shadow**. `AUTOPUBLISH_ENABLED=false` until SLOs are stable.
- LLM emits **structured JSON validated against a pinned schema** with `reason_codes`; never acts on raw text; never decides "publish or not" directly.
- Bad data = **detect → sanitize → quarantine → metrics**. Time is explicit (`epoch_ms` UTC).
- Contracts are versioned (`*_v1`) and backward-compatible by default; governors promote via **off → shadow → canary → enforce** with rollback.

## Roadmap (plan §19)
Phase 0 bootstrap → 1 Contracts+Streams → 2 Collectors → 3 Enrichment → 4 Trend scoring → 5 LLM planner → 6 NestJS control plane → 7 Next.js dashboard → 8 YouTube adapter → 9 TikTok adapter → 10 Governors. Start with the control loop, **not** video generation.

## Implemented so far ✅
The full **control loop** runs end-to-end on real Redis + TimescaleDB
(`collect → rank → brief(LLM) → policy → approve → publish(draft) → outcome → governor`).
See [`docs/architecture/control-loop.md`](./docs/architecture/control-loop.md).

- Phase 1: contracts (`schemas/*.json`) + `social_streams` (EventEnvelope, producer, consumer groups, PEL recovery, DLQ, quarantine, replay)
- Phase 2: `collector-manual-trends`
- Phase 3: trend persistence to Timescale (`trend_observations`)
- Phase 4: rule-based `worker-trend-rank`
- Phase 5: `worker-content-plan` (LLM, JSON-validated) + `worker-policy-critic`
- Phase 6: NestJS control plane (Trend/Review/Publish over Redis Streams)
- Phase 2: `collector-youtube` (YouTube Data API v3 owned-channel metrics → `social_event.v1`; OAuth/API-key/dry-run) + `go-gateway` (`/commands`, `/ingest`, rate-limit, idempotency)
- Phase 8-9: sandbox publish adapter (draft-first, idempotent) + `youtube-publisher` (real Data API v3, private-first) + `worker-outcome-attribution`
- Phase 10: `content_strategy_governor` (cohort lift, off→shadow→canary→enforce, rollback, TTL fail-safe)
- CI: `.github/workflows/ci.yml` — python/contract tests, schema validation, go build/vet, nest build, redis-service e2e

```bash
make test        # 34 unit tests
make e2e         # collector → worker (throwaway redis)
make test-stack-up && make e2e-full && make test-stack-down   # full loop + DB
```

In progress: TikTok/Instagram adapters, media pipeline (ASR/OCR/render), Qdrant visual embeddings, vLLM serving.
Not production-ready yet: OAuth hardening, real media generation, full CI/e2e, rights-ledger engine.
