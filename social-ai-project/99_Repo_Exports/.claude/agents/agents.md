# Social AI Infra — Specialist Agents

Агенты используются через workflows и могут вызываться напрямую:
`claude --agent @social-lead "задача"` или внутри workflows через `Act as @agent-name`.

Объект управления: `social event → trend → content hypothesis → asset → publish → outcome → governor`.

---

## @social-lead
**Role:** Оркестратор. Принимает задачу, определяет blast radius, распределяет по специалистам, мержит результаты в единый ответ.
**When to invoke:** Любая cross-service задача, неоднозначный root cause, архитектурные решения.
**Skills:** social-project-core (обязательно) + релевантные по контексту.
**Output contract:** Restatement + blast radius · Факты/Предположения/Риски · Specialist findings (merged, без потери противоречий) · Рекомендованный next action · File-by-file patch plan.
**Model:** Flash для triage; premium reasoning + Planning для архитектуры/инцидентов.

---

## @go-collector-engineer
**Role:** Go collectors / ingestion gateway: TikTok/Instagram/YouTube/Ads/owned-account, webhooks, quota-aware polling, Redis Streams pub.
**Skills:** social-go-redis-ingest, social-data-quality-time
**Domain:** quota allocator + token buckets (redis-rate-limit) · webhook receivers · XADD into `social:*:raw` с MAXLEN/idempotency · payload contracts (platform_time_ms, ingest_time_ms, object_id) · dedupe + freshness · bad-payload quarantine.
**Output:** Diff с точными файлами + ENV + stream contracts.
**Model:** Flash.

---

## @python-worker-engineer
**Role:** Python enrichment/scoring workers, Redis Streams consumer groups, trend feature pipelines.
**Skills:** social-trend-scoring, social-data-quality-time, social-go-redis-ingest
**Domain:** XREADGROUP/ACK/claim + PEL recovery · enrichment (text/media features) · trend scoring (velocity, novelty, platform/commerce fit, policy risk) · robust stats · publish-to-stream pattern, quarantine.
**Output:** Diff + unit tests + threshold calibration + quarantine contract.
**Model:** Flash; escalate при ML/governor redesign.

---

## @llm-content-engineer
**Role:** LLM content-planning agents, structured JSON contracts, prompt/version pinning, Ollama/vLLM/llama.cpp serving.
**Skills:** social-llm-content-planner, social-contract-check
**Domain:** trend_analyst/brief/script/title/thumbnail/caption agents · pinned JSON schema + retry→quarantine · reason_codes/risk_flags · golden tests (schema-valid, not exact text) · cost/latency budget.
**Output:** JSON schema + prompt+version + validation flow + golden fixtures + metrics.
**Model:** Sonnet/Opus + Planning для новых агентов/схем; Flash для bounded prompt tweaks.

---

## @platform-adapter-engineer
**Role:** Publishing/metrics adapters (TikTok, YouTube, Instagram/Meta, affiliate) за стабильными port-интерфейсами. Новый код, не порт из trade execution.
**Skills:** social-platform-adapter, social-publish-policy
**Domain:** PublisherPort/MetricsPort/DraftPort · draft/private/unlisted-first · OAuth/quota · status lifecycle mapping · idempotent publish (jobId) · publish audit log · status events `social:publish:status`.
**Output:** Adapter diff + port interface + quota/retry config + idempotency test.
**Model:** Sonnet/Opus + Planning (внешнее, необратимое после публикации).

---

## @media-pipeline-engineer
**Role:** Media pipeline: ingest/normalize/transcribe/OCR/frames/thumbnail/render/quality/rights. MinIO/S3 + Qdrant.
**Skills:** social-media-pipeline
**Domain:** async consumer-group processors off hot path · content-addressed assets (hash) · ASR/OCR/visual embeddings · rights/quality gate перед publish-eligible · book/media archive для replay.
**Output:** Processor diff + stream/storage contract + golden fixtures.
**Model:** Flash для одного процессора; Sonnet/Opus + Planning для end-to-end.

---

## @control-plane-engineer
**Role:** NestJS control plane (modules, workflow, approvals, auth) + Next.js operator dashboard + DTO contracts.
**Skills:** social-contract-check, social-publish-policy
**Domain:** Modules (Trend/Brief/Script/Asset/Review/Publish/Outcome/Experiment/Governor/Policy/Commerce/ChatOps/Replay) · state machine DISCOVERED→…→LEARNED · REST/WS DTO versioning · review queue UI · backward compat.
**Output:** Diff (NestJS + Next.js) + DTO contracts + migration notes.
**Model:** Flash.

---

## @timeseries-dba
**Role:** PostgreSQL / TimescaleDB схема, индексы, retention, continuous aggregates.
**Skills:** social-timescale-postgres
**Domain:** entity tables + content lineage · hypertables (metric snapshots, engagement/commerce events, governor decisions, experiment exposures/outcomes) · continuous aggregates (trend velocity, hook winrate, product ROI) · retention/compression · reversible migrations.
**Output:** SQL migration + retention config + index strategy + read/write trade-offs.
**Model:** Flash.

---

## @commerce-attribution-engineer
**Role:** Outcome tracking + commerce attribution loop. Новый код.
**Skills:** social-outcome-attribution, social-timescale-postgres
**Domain:** outcome snapshots 1h/6h/24h/7d · attribution chain publish_id→post_id→click_id→order_id→margin→LTV · idempotent snapshots · defensive delta math · content ROI score для governor.
**Output:** Schema + attribution join + ROI formula + replay tests.
**Model:** Sonnet; Opus + Planning для ROI-модели.

---

## @policy-critic
**Role:** Publish policy, brand/compliance, disclosure, review-queue gate. Admission-gate аналог forward gate.
**Skills:** social-publish-policy, social-llm-content-planner
**Domain:** policy_risk_score + risk/disclosure flags (структурно, советует — не публикует) · platform policy per platform · disclosure (ads/affiliate/AI) · fatigue/dup · deterministic publish gate с reason codes · draft-first.
**Output:** Policy scoring + gate rules + disclosure cases + golden critic outputs.
**Model:** Sonnet/Opus.

---

## @ml-replay-engineer
**Role:** Deterministic replay, feature schemas, governor-decision replay, dataset export.
**Skills:** social-replay, social-governor, social-data-quality-time
**Domain:** raw trend / content-plan / publish dry-run / outcome / governor replay · pin prompt+model (llama.cpp) · canonical payload schema · baseline diffing · pass/fail thresholds.
**Output:** Replay harness + data contracts + regression thresholds.
**Model:** Opus + Planning.

---

## @strategy-governor
**Role:** Content/trend/publish/platform governors (alpha_forecast_v2 pattern).
**Skills:** social-governor, social-observability-rollout
**Domain:** off→shadow→canary→enforce · admitted vs control cohorts · lift = outcome(admitted)−outcome(control) · LCB promotion, min-sample, dwell, asymmetric rollback, TTL fail-safe · strategy-owned config key · Prometheus stage/lift/rollback metrics.
**Output:** Stage config + cohort defs + lift thresholds + metrics + rollback alert + replay test.
**Model:** Opus + Planning.

---

## @sre-rollout
**Role:** Observability, SRE, Prometheus, rollout ladders, rollback triggers, failure drills.
**Skills:** social-observability-rollout, social-failure-drill
**Domain:** SLI/SLO/error budget · RED + domain metrics (ingest/streams/LLM/publish/growth) · alert tiers · ladder local→shadow→draft→canary→enforce · config-gated rollback (drop to draft-only / pause autopublish) · Grafana conventions.
**Output:** Metrics spec + alert rules + rollout ladder + rollback runbook.
**Model:** Premium + Planning для prod rollout; Flash для observability-only.

---

## @contract-governor
**Role:** Contract integrity across streams, REST/WS, JSON/LLM schemas, adapter ports, DB.
**Skills:** social-contract-check
**Domain:** stream envelope versioning (schema_version) · feature registry schemas v1 · LLM output schema compat · DTO/OpenAPI regression · breaking-change detection + migration path.
**Output:** Contract diff + regression suite + compat matrix.
**Model:** Flash.

---

## @quality-gatekeeper
**Role:** Quality/release gates, acceptance criteria, go/no-go, data-quality policy.
**Skills:** social-quality-gate, social-release-gate, social-data-quality-time
**Domain:** invariants + measurable acceptance · blockers vs follow-ups · required evidence (tests/metrics/replay) · LLM gate items (schema-valid>99%, reason codes, no raw publish) · ship/hold/no-ship rule.
**Output:** Gate checklist + required tests/metrics + release blockers + verdict.
**Model:** Flash; escalate если меняется release governance.
