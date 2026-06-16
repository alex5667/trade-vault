# План переноса каркаса `scanner_infra`, `trade-back` и `trade-front` в новый AI-проект для TikTok / Instagram / YouTube

## 0. Контекст и ограничения

Этот документ оформляет полный migration blueprint на основе:

1. текущего диалога;
2. анализа доступного GitHub repo `alex5667/scanner_infra`;
3. известной архитектуры trade-проекта из предыдущих обсуждений;
4. выводов глубокого исследования по AI content factory, trend intelligence, YouTube/TikTok/Instagram automation, local LLM, Redis Streams, TimescaleDB, Qdrant, multi-agent системам, observability и rollout/rollback.

Ограничение: точные репозитории с именами `trade-back` и `trade-front` через GitHub search не были найдены. Поэтому для них используется архитектурная модель из предыдущих обсуждений: `trade-back` как NestJS backend/control plane, `trade-front` как Next.js dashboard/operator UI.

Доступные GitHub-репозитории:

- `alex5667/scanner_infra` — private repo, default branch `main`;
- `alex5667/trade-vault` — public repo, default branch `main`.

---

# 1. Главная идея переноса

Новый проект не должен быть просто ботом, который генерирует и публикует ролики. Он должен стать **production-grade content operating system**.

В `scanner_infra/trade` уже есть зрелый каркас:

- ingestion;
- Redis-based event bus;
- Python workers;
- Go gateway/collectors;
- strategy governors;
- shadow/canary/enforce rollout;
- replay;
- observability;
- Grafana/Prometheus;
- Telegram/ChatOps;
- data quality;
- CI/CD;
- feature contracts;
- SRE-мышление.

В trading-системе объект управления:

```text
market event → signal → gate → trade → outcome → governor
```

В новом проекте объект управления:

```text
social event → trend → content hypothesis → asset → publish → outcome → governor
```

Соответствие доменов:

| Trade project | Новый social-проект |
|---|---|
| market data | TikTok / Instagram / YouTube signals |
| signal | trend candidate |
| alpha strategy | content strategy |
| forward gate | publish gate |
| EV-sizer | content/commercial scorer |
| trade execution | publish execution |
| PnL / R-multiple | watch time / CTR / CVR / revenue / LTV |
| governor | rollout governor for content strategies |
| Telegram alerts | ChatOps / approval / alerts |
| Grafana dashboard | content/SRE/growth dashboard |

---

# 2. Что реально видно в `scanner_infra`

## 2.1 Repo и общий размер

`scanner_infra` найден как `alex5667/scanner_infra`, default branch — `main`, repo приватный и не архивный.

В `docker-compose.yml` видно, что это не маленький скрипт, а большая modular-compose система:

- shared/infrastructure;
- enforcement seed;
- EV-sizer;
- rollout governors;
- retrace watch;
- tick/book archives;
- Go workers;
- Python workers;
- news pipeline;
- backend;
- hub;
- process janitor;
- monitoring;
- ChatOps agent;
- Telegram notifications;
- Binance;
- feature producers;
- ML training;
- forward forecaster;
- OPE ranker;
- sim-real validity.

Вывод: переносить нужно не один сервис, а **модель сборки платформы из compose-фрагментов**.

## 2.2 Redis / infra backbone

В `docker-compose-infrastructure.yml` есть основной Redis с AOF, healthcheck, resource limits, ulimits и отдельные Redis worker instances.

Это переносится в новый проект как:

```text
redis-main
redis-events
redis-workers
redis-cache
redis-rate-limit
```

Redis Streams хорошо подходят под эту роль, потому что они дают append-only event log, consumer groups, pending entries, retry/reclaim и controlled trimming.

## 2.3 Backend / gateway / Telegram / tick ingest

В `docker-compose-backend.yml` есть:

- `go-gateway`;
- `telegram-worker`;
- `notify-worker`;
- `tick-ingest-server`;
- `signal-performance-tracker`.

Это ценно для нового проекта:

| `scanner_infra` | Новый проект |
|---|---|
| `go-gateway` | social ingestion gateway / command gateway |
| `telegram-worker` | user command listener / approval bot |
| `notify-worker` | alerts / review queue notifications |
| `tick-ingest-server` | social-event ingest server |
| `signal-performance-tracker` | content-performance tracker |

## 2.4 Monitoring / Alerting / Contract checks

В monitoring-compose есть:

- Grafana;
- Alertmanager;
- Telegram webhook;
- feature-registry contract exporter;
- canary contract checks для версий схем.

Это переносится почти без изменений:

```text
feature registry contract → content feature contract
signal schema check → social event schema check
ML feature schema → trend/content feature schema
alerts → Telegram/Slack/Email notifications
```

## 2.5 Alpha Forecast V2 как образец rollout-механики

`alpha_forecast_v2_governor_v1.py` описывает зрелый strategy-level governor:

- forward-validates admission decision;
- сравнивает admitted/control cohorts;
- ведёт режимы `off → shadow → canary → enforce`;
- умеет rollback;
- пишет только свой strategy-owned config key;
- использует TTL fail-safe;
- экспортирует Prometheus metrics.

В новом проекте этот механизм переносится как:

```text
content_strategy_governor
trend_strategy_governor
publish_policy_governor
creative_family_governor
```

Недавние изменения в `alpha_forecast_v2` показывают зрелые production-паттерны:

- A/B review;
- Prometheus gauges;
- retryable `no_path`;
- archive-first path resolution;
- CI workflow;
- PEL recovery;
- poison-row isolation;
- Grafana dashboard.

Вывод: каркас уже умеет работать по принципу:

```text
shadow → measurement → promotion → rollback
```

---

# 3. Целевая архитектура нового проекта

Рабочие названия:

```text
social_ai_infra
creator_growth_os
social_content_os
```

## 3.1 Целевой pipeline

```text
TikTok / Instagram / YouTube / Ads / Own accounts
        ↓
Go collectors / API adapters / webhook receivers
        ↓
Redis Streams
        ↓
Python enrichment workers
        ↓
Trend scoring / content planning / LLM agents
        ↓
NestJS control plane
        ↓
Next.js operator UI
        ↓
Human approval / policy gate
        ↓
Publishing adapters
        ↓
Outcome tracking
        ↓
Governors / experiments / replay
```

## 3.2 Основной стек

| Слой | Технология | Назначение |
|---|---|---|
| Ingestion | Go | быстрые collectors, quota-aware polling, webhooks |
| Event bus | Redis Streams | reliable events, consumer groups, replay |
| Analytics workers | Python | scoring, ML, ASR/OCR, LLM orchestration |
| API/control | NestJS | workflow, approvals, adapters, auth |
| UI | Next.js | dashboard, review, experiments |
| DB | Postgres + Timescale | events, outcomes, aggregates |
| Object storage | S3/MinIO | видео, кадры, thumbnails, transcripts |
| Vector DB | Qdrant | creative memory, RAG, reference search |
| LLM serving | vLLM / Ollama / llama.cpp | local LLM 7B–14B |
| Monitoring | Prometheus + Grafana + Alertmanager | SRE |
| ChatOps | Telegram bot | команды, approve/reject, alerts |

---

# 4. Что переносить из `scanner_infra` без изменений

## 4.1 Compose-фрагментную архитектуру

В `scanner_infra` всё собрано через много compose-фрагментов. Это правильно: каждый сервис можно включать/выключать отдельно, rollout делать частями, а не одним монолитом.

В новом проекте сделать так же:

```text
docker-compose.yml
docker-compose-infrastructure.yml
docker-compose-go-collectors.yml
docker-compose-python-workers.yml
docker-compose-backend.yml
docker-compose-front.yml
docker-compose-monitoring.yml
docker-compose-chatops.yml
docker-compose-llm.yml
docker-compose-media-pipeline.yml
docker-compose-platform-adapters.yml
docker-compose-governors.yml
docker-compose-experiments.yml
docker-compose-replay.yml
```

## 4.2 Redis backbone

Переносить:

- Redis healthchecks;
- AOF;
- resource limits;
- ulimits;
- separated Redis instances;
- stable config;
- worker-specific Redis;
- stream retention discipline.

В новом проекте:

```text
redis-main        → configs, state, locks
redis-events      → social streams
redis-cache       → temporary platform cache
redis-rate-limit  → quota/token buckets
redis-llm         → LLM task queues
```

## 4.3 Monitoring

Переносить:

- Grafana;
- Alertmanager;
- Telegram webhook;
- contract exporters;
- Prometheus metric naming style;
- separate scrape targets;
- dashboards per subsystem.

Примеры переименования метрик:

```text
scanner-feature-registry-contract-exporter
→ social-feature-registry-contract-exporter

afv2_gov_stage_idx
→ content_strategy_gov_stage_idx

afv2_pel_pending_total
→ social_stream_pel_pending_total

tm_alpha_forecast_v2_acted_mismatch_total
→ publish_decision_action_mismatch_total
```

## 4.4 Governor pattern

Самый важный перенос:

```text
off → shadow → canary → enforce
```

Перенести механику:

- cohort comparison;
- promotion lift;
- lower confidence bound;
- min sample size;
- dwell time;
- rollback lift;
- TTL fail-safe;
- no conflict with other governors;
- metrics for stage/lift/rollback.

Новые governors:

```text
trend_discovery_governor
content_strategy_governor
hook_family_governor
publish_policy_governor
platform_adapter_governor
commerce_roi_governor
```

## 4.5 Replay / archive thinking

В `scanner_infra` есть durable tick archive и book archive, потому что Redis stream retention короткий.

В новом проекте аналог:

```text
tick archive       → raw social event archive
book archive       → raw media/enrichment archive
entry-fill replay  → content performance replay
path resolution    → outcome-window resolution
```

Хранить:

- raw post data;
- raw metrics snapshots;
- transcripts;
- OCR;
- frame descriptors;
- generated briefs;
- generated scripts;
- asset hashes;
- publish attempts;
- outcomes after 1h/6h/24h/7d.

---

# 5. Что переносить с адаптацией

## 5.1 Go gateway

В `scanner_infra` `go-gateway` работает с Telegram, Redis, order queue, metrics, runtime handlers. В коде видны typed command object, normalization, timestamping, Redis queue, dequeue/requeue logic.

В новом проекте это превращается в:

```text
go-social-gateway
```

Новые команды:

```ts
type ContentCommand =
  | "collect_trends"
  | "generate_brief"
  | "render_asset"
  | "schedule_publish"
  | "cancel_publish"
  | "replay_window"
  | "approve"
  | "reject";
```

`OrderCommand` превращается в `PublishCommand`:

```ts
type PublishCommand = {
  action: "draft" | "schedule" | "publish" | "cancel" | "retry";
  jobId: string;
  platform: "tiktok" | "instagram" | "youtube";
  accountId: string;
  assetId: string;
  caption?: string;
  title?: string;
  description?: string;
  scheduledAtMs?: number;
  metadata: Record<string, unknown>;
  source: "agent" | "operator" | "governor" | "replay";
  timestampMs: number;
};
```

## 5.2 Telegram worker

Сейчас есть `telegram-worker` и `notify-worker`.

В новом проекте:

```text
telegram-worker → chatops-agent
notify-worker   → content-alert-worker
```

Команды:

```text
/status
/trends
/brief <topic>
/approve <job_id>
/reject <job_id> <reason>
/publish-status <job_id>
/replay <window>
/governor <name>
/pause-autopublish
/resume-draft-only
```

## 5.3 Tick ingest server

Сейчас `tick-ingest-server` пишет tick/book streams.

В новом проекте:

```text
social-ingest-server
```

Источники:

- TikTok own account metrics;
- TikTok content posting status;
- Instagram account metrics;
- YouTube channel/video analytics;
- YouTube upload status;
- product catalog;
- affiliate/conversion events;
- ad library/commercial content signals;
- manually curated trend signals.

## 5.4 Feature registry

Сейчас есть `feature-registry-contract-exporter`, canary checks для схем `v14_of`, `v15_of`.

В новом проекте:

```text
social-feature-registry
```

Версии:

```text
trend_features_v1
content_features_v1
asset_features_v1
publish_features_v1
commerce_features_v1
```

Пример:

```json
{
  "schema": "trend_features_v1",
  "keys": [
    "platform",
    "topic_cluster_id",
    "velocity_1h",
    "velocity_6h",
    "creator_adoption_delta",
    "audio_reuse_delta",
    "comment_sentiment",
    "visual_pattern_id",
    "commerce_fit_score",
    "policy_risk_score"
  ]
}
```

---

# 6. Что переписать с нуля

## 6.1 Platform adapters

Это нельзя переносить из trading напрямую.

Нужно писать заново:

```text
platform-adapter-tiktok
platform-adapter-youtube
platform-adapter-instagram
platform-adapter-meta
platform-adapter-affiliate
```

Причины:

- auth другой;
- quota другая;
- status lifecycle другой;
- platform policies другие;
- publish result не похож на trade execution.

## 6.2 Media pipeline

В trade нет полноценного media render pipeline.

Нужно создать:

```text
media-ingest
media-normalize
media-transcribe
media-ocr
media-frame-sampler
media-thumbnail-generator
media-render
media-quality-check
media-rights-check
```

## 6.3 Content planning agent

Нужно писать заново, но по принципам `alpha_forecast_v2`:

```text
hypothesis → shadow → compare outcome → promote/demote
```

Новые агенты:

```text
trend_analyst_agent
hook_generator_agent
script_writer_agent
policy_critic_agent
platform_optimizer_agent
commerce_fit_agent
thumbnail_agent
youtube_title_agent
```

## 6.4 Attribution / commerce loop

Нужно писать заново:

```text
publish_id → post_id → click_id → order_id → margin → LTV
```

Минимальные entities:

```text
content_asset
publish_job
platform_post
tracking_link
affiliate_click
commerce_order
content_outcome
```

---

# 7. Что не переносить

Не переносить напрямую:

- trading-specific orderflow logic;
- Binance/MT5 execution;
- SL/TP/TP1/TP2 logic;
- R-multiple semantics как бизнес-метрику;
- forward gate в текущем виде;
- EV-sizer как trading sizer;
- market microstructure features;
- trade-specific dashboards.

Но переносить их принципы:

| Trading feature | Принцип | Social аналог |
|---|---|---|
| SL/TP | outcome window | 1h/6h/24h/7d content outcome |
| R-multiple | normalized result | normalized content ROI |
| EV-sizer | expected value | expected content value |
| forward gate | admission gate | publish gate |
| alpha governor | safe promotion | strategy governor |
| signal diagnostics | explainability | content decision diagnostics |
| shadow trades | safe simulation | draft/private/shadow publish |

---

# 8. Целевая структура нового repo

```text
social-ai-infra/
  apps/
    api/                         # NestJS control plane
    web/                         # Next.js dashboard
    chatops-bot/                 # Telegram bot
  services/
    go-gateway/
    collectors/
      tiktok/
      youtube/
      instagram/
      ads/
      owned-accounts/
    platform-adapters/
      tiktok-publisher/
      youtube-publisher/
      instagram-publisher/
    python-workers/
      enrich/
      trend-rank/
      content-plan/
      policy-critic/
      outcome-attribution/
      replay/
      governors/
    media/
      transcribe/
      ocr/
      frame-sampler/
      render/
      thumbnail/
  packages/
    contracts/
    config/
    redis-streams/
    observability/
    db/
    policy/
    llm-client/
    shared-types/
  infra/
    docker/
    compose/
    migrations/
    grafana/
    prometheus/
    alertmanager/
  schemas/
    social_event.v1.json
    trend_candidate.v1.json
    content_brief.v1.json
    render_job.v1.json
    publish_job.v1.json
    outcome_event.v1.json
  docs/
    architecture/
    runbooks/
    rollout/
    policy/
    api/
  tests/
    golden/
    contract/
    replay/
    e2e/
```

---

# 9. Mapping `scanner_infra → social-ai-infra`

| `scanner_infra` | Новый проект | Действие |
|---|---|---|
| `docker-compose.yml` modular includes | `docker-compose.yml` modular includes | перенести шаблон |
| `docker-compose-infrastructure.yml` | Redis/Postgres/Timescale/Qdrant/MinIO | адаптировать |
| `docker-compose-backend.yml` | gateway + chatops + ingest API | адаптировать |
| `docker-compose-python-workers.yml` | enrichment + LLM + scoring workers | адаптировать |
| `docker-compose-monitoring.yml` | Grafana/Prometheus/Alertmanager | перенести |
| `go-gateway` | `go-social-gateway` | адаптировать |
| `telegram-worker` | `chatops-agent` | адаптировать |
| `notify-worker` | `content-alert-worker` | почти 1:1 |
| `tick-ingest-server` | `social-ingest-server` | переписать входные DTO |
| `tick archive` | `raw social event archive` | адаптировать |
| `book archive` | `media/frame archive` | адаптировать |
| `feature registry contract` | `content feature registry contract` | перенести принцип |
| `alpha_forecast_v2_governor` | `content_strategy_governor` | почти 1:1 |
| `opened persister` | `publish decision persister` | адаптировать |
| `trades_closed` | `content_outcomes` | переписать |
| `Grafana alpha dashboard` | strategy/content dashboard | адаптировать |
| `news pipeline` | trend/news/social listening pipeline | адаптировать |
| `ChatOps` | approval/command bot | перенести |
| `CI workflow` | social contract/replay CI | адаптировать |

---

# 10. Event streams нового проекта

## 10.1 Raw ingestion

```text
social:tiktok:raw
social:instagram:raw
social:youtube:raw
social:ads:raw
social:owned:raw
```

## 10.2 Enrichment

```text
social:media:downloaded
social:media:transcribed
social:media:ocr
social:media:frames
social:media:enriched
```

## 10.3 Trend intelligence

```text
social:trend:candidates
social:trend:features
social:trend:clusters
social:trend:ranked
```

## 10.4 Content generation

```text
social:brief:requests
social:brief:results
social:script:requests
social:script:results
social:asset:render_requests
social:asset:render_results
```

## 10.5 Policy and approval

```text
social:policy:checks
social:policy:results
social:review:queue
social:review:decisions
```

## 10.6 Publishing

```text
social:publish:requests
social:publish:attempts
social:publish:status
social:publish:failures
```

## 10.7 Outcomes

```text
social:outcome:1h
social:outcome:6h
social:outcome:24h
social:outcome:7d
social:commerce:events
```

## 10.8 DLQ / quarantine

```text
social:dlq
social:quarantine
social:replay:requests
social:replay:results
```

---

# 11. База данных

## 11.1 Postgres entities

```sql
accounts
platform_accounts
creators
products
campaigns
content_assets
content_briefs
content_scripts
publish_jobs
platform_posts
tracking_links
affiliate_clicks
commerce_orders
review_decisions
policy_decisions
```

## 11.2 Timescale hypertables

```sql
trend_observations
platform_metric_snapshots
engagement_events
publish_status_events
commerce_events
dq_events
governor_decisions
experiment_exposures
experiment_outcomes
```

## 11.3 Главные outcome windows

```text
1h  → ранний сигнал hook/retention
6h  → первичный platform fit
24h → основной social outcome
7d  → commerce / LTV / delayed conversion
```

## 11.4 Continuous aggregates

```text
trend_velocity_1h
trend_velocity_6h
content_family_performance_24h
platform_account_health_24h
hook_family_winrate_7d
product_content_roi_7d
creator_product_fit_30d
```

---

# 12. Backend: `trade-back → social-control-plane`

`trade-back` нужно переносить как **control plane**, не как trading backend.

## 12.1 NestJS modules

```text
AuthModule
AccountsModule
PlatformsModule
TrendModule
ContentBriefModule
ScriptModule
AssetModule
ReviewModule
PublishModule
OutcomeModule
ExperimentModule
GovernorModule
PolicyModule
CommerceModule
ChatOpsModule
ReplayModule
ObservabilityModule
```

## 12.2 Основные API

```text
GET    /trends
GET    /trends/:id
POST   /briefs/generate
POST   /scripts/generate
POST   /assets/render
GET    /review/queue
POST   /review/:id/approve
POST   /review/:id/reject
POST   /publish/:id/schedule
POST   /publish/:id/cancel
GET    /outcomes
GET    /experiments
POST   /governors/:name/mode
POST   /replay
```

## 12.3 State machine

```text
DISCOVERED
→ ENRICHED
→ SCORED
→ BRIEF_GENERATED
→ SCRIPT_GENERATED
→ ASSET_RENDERED
→ POLICY_CHECKED
→ READY_FOR_REVIEW
→ APPROVED
→ SCHEDULED
→ PUBLISHING
→ PUBLISHED
→ OUTCOME_PENDING
→ OUTCOME_READY
→ LEARNED
```

Ошибочные состояния:

```text
QUARANTINED
REJECTED
FAILED
RETRY_WAIT
DLQ
```

---

# 13. Frontend: `trade-front → social-ops-dashboard`

`trade-front` нужно переносить как **operator dashboard**.

## 13.1 Основные экраны

```text
/trends
/trends/:id
/briefs
/scripts
/assets
/review
/publish
/outcomes
/experiments
/governors
/alerts
/replay
/settings/platforms
```

## 13.2 Главный dashboard

Показывать:

- новые тренды;
- score;
- confidence;
- reason codes;
- suggested content angle;
- policy risk;
- expected commercial value;
- status in pipeline;
- latest outcome;
- recommended action.

## 13.3 Review экран

Для каждого content job:

```text
trend evidence
generated brief
script
caption/title/description
thumbnail
policy result
disclosure flags
platform-specific preview
approve/reject buttons
```

---

# 14. AI / LLM слой

## 14.1 Локальная модель — не центр истины

LLM не должен решать напрямую:

```text
“публиковать или нет”
```

Он должен генерировать structured proposals:

```json
{
  "hook_variants": [],
  "script": "",
  "shot_list": [],
  "caption": "",
  "title": "",
  "risk_flags": [],
  "reason_codes": [],
  "confidence": 0.0
}
```

## 14.2 Роли агентов

```text
trend_analyst_agent
content_brief_agent
youtube_title_agent
thumbnail_agent
policy_critic_agent
commerce_fit_agent
platform_adapter_agent
experiment_explainer_agent
```

## 14.3 Serving

Для MVP:

```text
Ollama → local dev
vLLM → production GPU serving
llama.cpp → cheap/offline/golden replay
```

---

# 15. Platform strategy

## 15.1 YouTube

YouTube лучше сделать первым полноценным adapter-ом, потому что у него понятный upload workflow и сильная роль metadata/title/thumbnail.

MVP:

```text
upload private/unlisted
set title
set description
set thumbnail
collect metrics
compare thumbnails/titles
```

## 15.2 TikTok

TikTok начинать с:

```text
draft upload
manual review
direct post только после approval
```

## 15.3 Instagram

Instagram держать за interface boundary:

```text
InstagramPublisherPort
InstagramMetricsPort
InstagramDraftPort
```

В MVP можно сделать:

```text
manual export
asset preparation
caption generation
review queue
```

Autopublish — позже, после отдельной проверки Meta API.

---

# 16. Governor design для нового проекта

Взять идею `alpha_forecast_v2_governor` почти напрямую.

## 16.1 Content strategy governor

```text
off → shadow → canary → enforce
```

Когорты:

```text
admitted = контент, который стратегия рекомендовала публиковать
control = контент, который стратегия отклонила / оставила в shadow
```

Метрика lift:

```text
content_lift = outcome(admitted) - outcome(control)
```

Где outcome:

```text
0.35 * retention_score
+ 0.25 * engagement_score
+ 0.25 * commerce_score
+ 0.15 * account_growth_score
- policy_penalty
- fatigue_penalty
```

## 16.2 Hook family governor

Проверяет:

```text
“крючок такого типа действительно лучше?”
```

Примеры hook families:

```text
problem-solution
before-after
controversy
checklist
mistake
reaction
comparison
story
tutorial
```

## 16.3 Platform governor

Отдельный governor на каждую платформу:

```text
youtube_shorts_governor
tiktok_governor
instagram_reels_governor
```

Один и тот же контент может выигрывать на YouTube и проигрывать в TikTok.

---

# 17. Тестовая стратегия

## 17.1 Unit tests

```text
DTO validation
event envelope validation
stream idempotency
dedupe key generation
policy scoring
publish state transitions
caption/title/schema validation
```

## 17.2 Integration tests

```text
collector → stream
stream → worker
worker → DB
DB → API
API → UI
approval → publish adapter
publish adapter → status event
```

## 17.3 Golden tests

Как в trade:

```text
fixed input
fixed model version
fixed prompt version
fixed expected structured output
```

Для LLM допускать не exact text, а:

```text
schema valid
required fields present
reason codes valid
policy flags valid
score range valid
```

## 17.4 Replay tests

Обязательные сценарии:

```text
raw trend replay
content plan replay
publish dry-run replay
outcome attribution replay
governor decision replay
```

---

# 18. SRE / Observability

## 18.1 Метрики ingestion

```text
social_ingest_events_total
social_ingest_errors_total
social_source_freshness_lag_ms
social_duplicate_rate
social_schema_validation_failed_total
```

## 18.2 Метрики Redis Streams

```text
social_stream_lag
social_stream_pel_pending_total
social_stream_claimed_total
social_stream_dlq_total
social_stream_replay_total
```

## 18.3 Метрики LLM

```text
llm_requests_total
llm_latency_ms
llm_json_invalid_total
llm_schema_reject_total
llm_cost_per_plan
llm_timeout_total
```

## 18.4 Метрики publish

```text
publish_jobs_total
publish_success_total
publish_failure_total
publish_retry_total
publish_duplicate_blocked_total
publish_policy_reject_total
```

## 18.5 Метрики growth

```text
content_views_1h
content_retention_1h
content_ctr_24h
content_cvr_7d
content_revenue_7d
content_margin_7d
content_ltv_proxy_30d
```

---

# 19. Roadmap переноса

## Phase 0 — Inventory и repo bootstrap

Цель: создать новый repo и перенести skeleton.

Сделать:

```text
создать social-ai-infra
перенести compose style
перенести config/env style
перенести monitoring skeleton
перенести Redis helpers
перенести contract-test pattern
перенести Telegram alert pattern
```

Результат:

```text
docker compose up
Redis + Postgres/Timescale + Grafana + API + UI стартуют
```

## Phase 1 — Contracts + Streams

Цель: сначала контракты, потом код.

Создать схемы:

```text
social_event.v1
trend_candidate.v1
content_brief.v1
asset_job.v1
publish_job.v1
outcome_event.v1
policy_decision.v1
```

Создать streams:

```text
social:ingest:raw
social:trend:candidates
social:brief:requests
social:publish:requests
social:outcomes
social:dlq
social:quarantine
```

Acceptance:

```text
producer writes valid envelope
consumer validates schema
bad event goes to quarantine
metrics exported
```

## Phase 2 — Go collectors

Цель: один минимальный source end-to-end.

Лучше начать с YouTube или manually-curated trend source.

Сервисы:

```text
collector-youtube
collector-owned-metrics
collector-manual-trends
```

Acceptance:

```text
events arrive
dedupe works
freshness metrics work
quota metrics work
```

## Phase 3 — Python enrichment

Сервисы:

```text
worker-enrich-text
worker-transcribe
worker-ocr
worker-frame-sampler
worker-feature-builder
```

На MVP можно начать без полного video processing:

```text
caption + title + metadata + manual notes
```

Потом добавить:

```text
ASR
OCR
frames
visual embeddings
```

## Phase 4 — Trend scoring

Создать:

```text
trend_score_v1
commerce_fit_score_v1
platform_fit_score_v1
policy_risk_score_v1
```

Не пытаться сразу обучать сложную ML-модель.

Сначала rule-based + features:

```text
velocity
novelty
platform fit
content repeatability
product fit
risk
```

## Phase 5 — LLM content planner

Сервисы:

```text
content_brief_agent
script_agent
youtube_title_agent
caption_agent
policy_critic_agent
```

Все outputs только JSON.

Acceptance:

```text
valid schema > 99%
no raw hallucinated publish
all decisions have reason_codes
```

## Phase 6 — NestJS control plane

Перенести `trade-back` подход.

Сделать:

```text
TrendModule
BriefModule
AssetModule
ReviewModule
PublishModule
OutcomeModule
GovernorModule
ReplayModule
```

Acceptance:

```text
можно увидеть trend
сгенерировать brief
отправить в review
approve/reject
создать publish job
```

## Phase 7 — Next.js dashboard

Перенести `trade-front` подход.

Экраны MVP:

```text
Trend Feed
Brief Review
Publish Queue
Outcomes
Governors
Alerts
```

Acceptance:

```text
оператор может управлять всем pipeline без CLI
```

## Phase 8 — YouTube adapter

Первым делать YouTube:

```text
private/unlisted upload
title
description
thumbnail
status
metrics
```

Autopublish выключен.

Acceptance:

```text
asset uploaded as private/unlisted
status tracked
outcome snapshot collected
```

## Phase 9 — TikTok adapter

Сначала:

```text
draft upload
manual approval
status polling/webhook
```

Потом:

```text
direct post
```

Acceptance:

```text
draft created
operator approves
status appears in system
```

## Phase 10 — Governors

Перенести `alpha_forecast_v2_governor` как шаблон.

Создать:

```text
content_strategy_governor_v1
hook_family_governor_v1
publish_policy_governor_v1
```

Acceptance:

```text
shadow mode
canary mode
enforce mode
rollback mode
metrics
Telegram alert
```

---

# 20. Что делать в первую очередь

Самый правильный порядок:

1. Создать новый repo skeleton.
2. Перенести compose/infrastructure/monitoring/config.
3. Создать contracts package.
4. Создать Redis event envelope.
5. Создать минимальный NestJS API.
6. Создать минимальный Next.js dashboard.
7. Создать один collector.
8. Создать один Python worker.
9. Создать review queue.
10. Создать YouTube private upload adapter.
11. Добавить TikTok draft adapter.
12. Добавить LLM planner.
13. Добавить policy critic.
14. Добавить outcome tracking.
15. Добавить governor.

Не начинать с генерации видео.

Сначала должен заработать контур управления:

```text
event → decision → review → publish/draft → outcome → replay
```

---

# 21. MVP scope

## MVP-1: Trend → Brief → Review

```text
manual/YouTube/TikTok trend input
↓
trend score
↓
LLM brief
↓
operator review
```

Без публикации.

## MVP-2: Brief → Asset → YouTube private

```text
brief
↓
script
↓
asset placeholder/manual upload
↓
YouTube private/unlisted
↓
status tracking
```

## MVP-3: TikTok draft

```text
asset
↓
TikTok draft upload
↓
manual post
↓
status/outcome tracking
```

## MVP-4: Governed content strategy

```text
content strategy shadow
↓
outcome comparison
↓
canary
↓
manual enforce
```

---

# 22. Первый backlog разработки

## Epic 1 — Repo skeleton

```text
SOC-001 create repo structure
SOC-002 add docker compose modular layout
SOC-003 add config/env validation
SOC-004 add shared logger
SOC-005 add Prometheus metrics package
SOC-006 add Redis client package
SOC-007 add Postgres/Timescale migrations
```

## Epic 2 — Contracts

```text
SOC-010 define EventEnvelope v1
SOC-011 define TrendCandidate v1
SOC-012 define ContentBrief v1
SOC-013 define PublishJob v1
SOC-014 define OutcomeEvent v1
SOC-015 add contract tests
SOC-016 add schema compatibility CI
```

## Epic 3 — Streams

```text
SOC-020 create stream producer helper
SOC-021 create consumer group helper
SOC-022 add PEL recovery
SOC-023 add DLQ writer
SOC-024 add quarantine writer
SOC-025 add replay runner
```

## Epic 4 — Backend

```text
SOC-030 create NestJS API
SOC-031 add TrendModule
SOC-032 add BriefModule
SOC-033 add ReviewModule
SOC-034 add PublishModule
SOC-035 add OutcomeModule
SOC-036 add GovernorModule
```

## Epic 5 — Frontend

```text
SOC-040 create Next.js dashboard
SOC-041 trend feed screen
SOC-042 review queue screen
SOC-043 publish queue screen
SOC-044 outcome dashboard
SOC-045 governor dashboard
```

## Epic 6 — Collectors

```text
SOC-050 collector-manual-trends
SOC-051 collector-youtube-search/seed
SOC-052 collector-youtube-own-channel
SOC-053 collector-tiktok-owned
SOC-054 collector-instagram-shell
```

## Epic 7 — LLM

```text
SOC-060 LLM client interface
SOC-061 Ollama adapter
SOC-062 vLLM adapter
SOC-063 content brief agent
SOC-064 script agent
SOC-065 policy critic
SOC-066 structured output validation
```

## Epic 8 — Publishing

```text
SOC-070 YouTube private upload
SOC-071 YouTube thumbnail upload
SOC-072 TikTok draft upload
SOC-073 publish status polling
SOC-074 publish retry/idempotency
SOC-075 publish audit log
```

## Epic 9 — Outcome

```text
SOC-080 outcome snapshots 1h/6h/24h/7d
SOC-081 platform metrics storage
SOC-082 commerce event ingestion
SOC-083 attribution join
SOC-084 content ROI score
```

## Epic 10 — Governors

```text
SOC-090 port alpha_forecast_v2 governor pattern
SOC-091 content_strategy_governor_v1
SOC-092 hook_family_governor_v1
SOC-093 publish_policy_governor_v1
SOC-094 Grafana dashboard
SOC-095 rollback alerts
```

---

# 23. Риски

## 23.1 Технические риски

| Риск | Как снизить |
|---|---|
| Слишком ранний autopublish | Сначала draft/private/manual approval |
| LLM hallucinations | JSON schema, reason-codes, policy critic, no direct action |
| API quotas | quota allocator, backoff, budget model |
| Неустойчивые platform APIs | adapter boundary, retry, status polling, DLQ |
| Нечистые данные | quarantine, schema validation, raw archive |
| Невоспроизводимость LLM | pinned prompt/model/version, golden tests |
| Сложный media pipeline | MVP без автогенерации видео, сначала asset placeholders |

## 23.2 Продуктовые риски

| Риск | Как снизить |
|---|---|
| Много просмотров, мало продаж | commerce-aware score, attribution loop |
| Контент не соответствует бренду | brand policy critic, manual review |
| Платформенные санкции | disclosure, platform policy checks, slow rollout |
| Слишком много автоматизации | human-in-the-loop до стабильных SLO |

---

# 24. Итоговое решение

Переносить нужно так:

```text
scanner_infra → infra/event/observability/governor/replay scaffold
trade-back     → NestJS control plane
trade-front    → Next.js operator UI
новый код      → platform adapters, media pipeline, content agents, attribution
```

Главная ценность `scanner_infra` — не trading-код. Главная ценность — **производственный каркас принятия решений**:

```text
collect → normalize → score → decide → shadow → measure → promote → rollback
```

В новом проекте это станет:

```text
collect trends → enrich media → generate hypothesis → review/publish → measure outcome → promote strategy
```

Такой перенос даст не SMM-бота, а управляемую AI-систему, которую можно развивать без хаоса: с логами, replay, contract tests, governance, rollback и понятными метриками.
