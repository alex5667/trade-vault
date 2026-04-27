# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Go Worker
```bash
cd go-worker
go build -o main ./cmd/worker        # сборка
go test ./...                         # все тесты
go test ./internal/liquidation/... -run TestDQ  # один пакет / один тест
go vet ./...                          # lint
```

### Python Worker
```bash
cd python-worker
python -m pytest                      # все тесты
python -m pytest tests/core/test_triple_barrier.py  # один файл
python -m pytest tests/path/to/test.py::test_func   # одна функция
```

### Docker Compose (основная точка запуска)
```bash
# Поднять всё (docker-compose.yml include-ит все sub-composes)
docker compose up -d

# Пересобрать и перезапустить один сервис
docker compose up -d --build scanner-go-worker-1m
docker compose up -d --build scanner-python-worker

# Логи
docker compose logs -f scanner-python-worker
docker compose logs -f scanner-go-worker-1m
```

## Architecture

### Repository Layout
```
scanner_infra/
  go-worker/                  # Go ingest service
    cmd/worker/main.go        # entrypoint (go build -o main ./cmd/worker)
    binance/                  # Binance WS/REST adapters (multiplexed_manager.go, symbol_consumer.go)
    bybit/                    # Bybit adapter
    internal/
      app/                    # startup/shutdown, Prometheus HTTP (:PROMETHEUS_PORT, default 2112)
      streams/keys.go         # ★ CANONICAL Redis key names (Go side)
      connections/manager.go  # WebSocket connection lifecycle
      liquidation/            # liq event DQ and parsing
      crossasset/             # cross-asset tracker
      orderflow/              # orderflow context
    infra/redisclient/        # Redis client factory
  python-worker/              # Python analysis / signals service
    main.py                   # monolith entrypoint (scanner-python-worker container)
    main_multi_symbol.py      # multi-symbol orderflow entrypoint
    main_multi_symbol_dynamic.py  # dynamic-symbols variant (DYNAMIC_SYMBOLS=true)
    core/
      redis_keys.py           # ★ CANONICAL Redis key names (Python mirror of streams/keys.go)
      redis_client.py         # main Redis client factory
      triple_barrier.py       # triple-barrier labeling core
    domain/                   # models.py, handlers.py, normalizers.py — shared DTOs
    handlers/crypto_orderflow/
      orchestrator.py         # pipeline orchestrator
      pipeline/               # candidate_pipeline, scoring, tracking
    services/                 # per-feature microservices (each runs in its own container)
    runners/                  # lightweight entrypoints: trade_monitor_runner.py etc.
    orderflow_services/       # calibration, confidence, latency-contract sub-services
    binance_execution/        # execution engine (p4x–p5x runbooks, tests)
    tests/                    # pytest suite (conftest.py, fakeredis, fixtures)
  py-obi/                     # GPU OBI service (book_obi_service.py, :8088)
  docker-compose.yml          # root — includes all sub-composes via `include:`
  docker-compose-infrastructure.yml  # Redis × 6, Postgres
  docker-compose-go-workers.yml      # go-worker-1m / 5m / 15m
  docker-compose-python-workers.yml  # all Python containers (~30 services)
```

### Redis Topology
| Instance | Port (ext) | Role |
|---|---|---|
| `redis` | 6379 | Main — cache, pub/sub |
| `redis-worker-1` | 63791 | Worker streams (candles, signals) |
| `redis-worker-1b` | — | Replica of worker-1 |
| `redis-worker-2` | — | Signals secondary |
| `redis-worker-2b` | — | Replica of worker-2 |
| `redis-ticks` | — | Tick/book streams (`stream:tick_{SYMBOL}`, `stream:book_{SYMBOL}`) |

### Key Contract: Redis Keys
**Always update BOTH** when adding a new stream/key:
- Go: `go-worker/internal/streams/keys.go`
- Python: `python-worker/core/redis_keys.py` (`from core.redis_keys import RedisStreams as RS`)

### Signal Flow (внутри scanner_infra)
```
Binance/Bybit WS → Go worker (multiplexed_manager)
  → XADD stream:tick_{SYMBOL} / stream:book_{SYMBOL}  (redis-ticks)
  → XADD stream:kline_{tf}:{SYMBOL}                   (redis-worker-1)
  → XADD stream:liq_evt                               (liquidation)
    → Python worker (handlers/crypto_orderflow/orchestrator.py)
      → scoring → signals:cryptoorderflow:{symbol}
        → stream:signals:outbox
          → signal-outbox-router → signal-target-worker-{notify,audit,…}
            → NestJS trade_back
```

### Prometheus Endpoints
- Go worker: `:2112/metrics` (env `PROMETHEUS_PORT`)
- Python worker (main): `:8000/metrics`
- py-obi: `:8088/healthz`
- latency-contract-exporter: `:9830/metrics`
- decision-snapshot-writer: `:9825/metrics`

## Key Analysis Tools
```bash
# (запускать из python-worker/)
python -m tools.tick_time_autotune --hours 6          # tune tick time policy
python -m tools.of_gate_missing_leg_report --hours 24 --top 20   # OF gate analysis
python -m tools.of_gate_missing_leg_report --hours 24 --by-symbol
```

### Triple-Barrier Labeling (v10, path-based)
```bash
python tools/export_ticks_window_ndjson_v1.py         # → /tmp/ticks_24h.ndjson
python tools/label_triple_barrier_from_ticks_v1.py    # → /tmp/tb_labels.ndjson
python tools/build_dataset_from_inputs_outcomes_v4_tb.py  # → /tmp/ml_dataset_tb.parquet
```
Core: `python-worker/core/triple_barrier.py` · Tests: `python-worker/tests/test_triple_barrier.py`

## Cost-aware Model Policy
По умолчанию — Flash + Fast для локальных задач; Premium + Planning только для архитектуры, ambiguous RCA, ML/replay redesign, breaking contracts.

| Тип задачи | Lane | Эскалация |
|---|---|---|
| Small fix / contract check / log triage / test gen | Flash Fast | >2 subsystems или breaking change |
| New signal | Flash → Premium | regime/ML/execution redesign |
| Incident RCA / Architecture / Schema lifecycle | Premium Planning | always |

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

### Outbox Data Contracts
- The outbox payload strictly requires `schema_version` (int). 
- **Current OUTBOX `SCHEMA_VERSION` is 1.**
- Handled via `OutboxEnvelope.schema_version`. The dispatcher rejects any payload that does not match the active `SCHEMA_VERSION`.

