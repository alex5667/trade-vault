# trade — Claude Code Project Context

## Язык / Language
Всегда отвечай на русском языке. Always respond in Russian.

## Pipeline (Truth)
```
Go (klines/ticks/orderbook/news)
  → Redis Streams + cache/pubsub
    → Python (analysis / signals / feature pipelines / news_agent)
      → NestJS (aggregation / API / WebSocket)
        → Next.js UI
          → Postgres / TimescaleDB (history / metrics / research)
```

**Hosts:**
- `main` — Ubuntu 24.04, основной стек
- `minik` — i3/32GB, вспомогательные сервисы, локальный LLM

## Services
| Service | Stack | Role |
|---|---|---|
| `trade_back` | NestJS | Aggregation, REST API, WebSocket |
| `trade_front` | Next.js | UI, Signal display |
| `scanner_infra` | Go + Python + Redis | Ingest, signals, feature pipelines |
| `ml_analysis` | Python | ML features, replay, labeling |
| `news_agent` | Go + Python + LiteLLM | News ingestion → LLM reasoning → recos |

## Core Engineering Rules

### Time
- Формат: `epoch_ms` если не оговорено иное
- Всегда разделять: `event_time_ms` / `ingest_time_ms` / `process_time_ms`
- Проверять: monotonicity, future skew, stale lag, duplicates, gaps
- Bad time policy: detect → classify → sanitize/quarantine → metrics

### Data Contracts (обязательны на всех границах)
```
schema_version, event_id, source, symbol,
event_time_ms, ingest_time_ms, trace_id, quality_flags
```

### Redis
- Надёжная доставка → Redis Streams + consumer groups
- Pub/Sub только там, где потеря допустима
- Каждый stream: key naming, maxlen, delivery semantics, DLQ policy
- Lag-метрики + alert thresholds обязательны

### Postgres / Timescale
- Метрики и история → hypertable по времени
- Continuous aggregates для downsampling
- Raw retention policy + compression policy

### Code Quality
- Typed DTOs / validation на всех границах
- Минимум скрытых зависимостей
- Явные контракты state transitions
- Reason-коды для критичных решений

### Observability (обязательно для critical path)
- SLI / SLO / error budget определены
- Метрики: latency (p95/p99), errors, traffic, saturation, freshness, DQ
- Алерты: page (симптом) / ticket / dashboard-only
- trace_id сквозной по сервисам

### Rollout Policy
- Порядок: local → test → shadow → canary → partial prod → full prod
- Изменения в signals/fills/PnL/risk → shadow/canary обязателен
- Каждый rollout: feature flag, success criteria, rollback trigger, rollback steps

### Robust Stats
- Для шумных данных: median/MAD, bounded robust z-score (не mean/std)

## Response Format
Для production-задач:
1. Цель
2. Что есть сейчас
3. План / варианты (2-3 с trade-offs для архитектурных)
4. Детали (код/SQL/ENV/contracts)
5. Тесты (unit / integration / replay / load)
6. Метрики / логи / алерты
7. Rollout / rollback
8. Prod checklist

Всегда явно: **Факты / Предположения / Риски**

## Agents
Специализированные агенты описаны в `.claude/agents/`.
Ключевые роли: @trade-lead · @go-ingest-engineer · @python-signal-engineer ·
@platform-api-ui-engineer · @timeseries-dba · @ml-replay-engineer ·
@sre-rollout · @microstructure-analyst · @execution-risk-analyst ·
@contract-governor · @latency-benchmarker · @resilience-drillmaster ·
@backtest-validity-reviewer · @exchange-adapter-engineer · @storage-retention-governor

## Skills (автозагрузка по контексту)
| Skill | Триггер |
|---|---|
| `trade-project-core` | TRADE:/tr:, архитектура, cross-service |
| `trade-python-signal-engine` | Python workers, детекторы, signals |
| `trade-go-redis-ingest` | Go, Redis Streams, ingest, биржа |
| `trade-data-quality-time` | DQ, time validation, bad data |
| `trade-observability-rollout` | rollout, SRE, Prometheus, alerts |
| `trade-timescale-postgres` | DB, schema, TimescaleDB |
| `trade-api-ui-contracts` | NestJS, Next.js, DTO, WebSocket |
| `trade-ml-replay-gating` | ML, replay, feature schemas |
| `trade-contract-regression` | API regression, contract tests |
| `trade-latency-benchmarking` | latency profiling, benchmarks |
| `trade-quality-gates` | DQ gate, shadow/enforce |
| `trade-execution-risk` | FSM, fills, risk controls |
| `trade-resilience-failure-drills` | chaos, failure modes |
| `trade-exchange-adapter` | exchange adapters, Binance |
| `trade-storage-retention` | retention, compression, tiering |
| `trade-backtest-validity` | backtest hygiene, leakage |

## Slash Commands (Workflows)
| Command | Use |
|---|---|
| `/trade-new-signal` | Новый сигнал / детектор / gate |
| `/trade-rollout` | Release plan со staged rollout |
| `/trade-parallel-investigation` | Параллельное расследование cross-service |
| `/trade-sequential-review` | Последовательный review |
| `/trade-release-gate` | Pre-release checklist |
| `/trade-audit` | Полный аудит сервиса |
| `/trade-replay` | Replay / regression pack |
| `/trade-quality-gate` | DQ gate review |
| `/trade-postmortem` | Postmortem |
| `/trade-failure-drill` | Failure drill |
| `/trade-latency-audit` | Latency audit |
| `/trade-regression-pack` | Regression тесты |
| `/trade-contract-check` | Contract / DTO check |
| `/trade-execution-policy-review` | Execution FSM review |
| `/trade-fast-fix` | Быстрый изолированный fix |
| `/trade-fast-test-gen` | Генерация тестов |
| `/trade-fast-log-triage` | Triage логов |
| `/tasks` | Выполнить задачи из Telegram inbox |
| `/trade_virtual_trades_last_hour` | Анализ виртуальных сделок |

## Execution Preferences
- Maker-first на Binance
- Conservative leverage/risk caps
- Kill switches + forced flatten при PROTECTION_ARM_TIMEOUT
- Reconcile-first при unknown/timeout states
- Prometheus histograms → `histogram_quantile` для p95/p99
- E2E latency instrumentation on hot path

## ML Feature Schemas
- v5/v6 в prod, v7 (Hawkes/VPIN), v8 планируется (liqmap + DQ)
- Ключевые метрики: `depth_imbalance_5`, `qimb_wmean`, `lob_dw_obi_z`
- Liquidity map windows: 5m / 1h / 4h / 24h → `liqmap_{window}_*`
