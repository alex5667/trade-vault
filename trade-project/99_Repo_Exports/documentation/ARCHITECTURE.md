# Архитектура Scanner Infrastructure (версия 4.1.0, 2026-04-02)

Документ описывает целостную архитектуру платформы с учетом **P4.1 Unified Latency Contract** и модели **Journal-First Execution**. Используйте его как отправную точку при проектировании новых фич и оценке влияния изменений.

---

## Содержание

1. [Введение и цели](#введение-и-цели)
2. [Карта доменов и ответственных](#карта-доменов-и-ответственных)
3. [Архитектурные слои](#архитектурные-слои)
4. [Сквозные потоки данных](#сквозные-потоки-данных)
5. [Инвентаризация сервисов](#инвентаризация-сервисов)
6. [Структуры данных и хранилища](#структуры-данных-и-хранилища)
7. [Интеграции и внешние системы](#интеграции-и-внешние-системы)
8. [Надёжность, масштабирование, безопасность](#надёжность-масштабирование-безопасность)
9. [Наблюдаемость и SLO](#наблюдаемость-и-slo)
10. [Архитектурные решения (ADR)](#архитектурные-решения-adr)
11. [Контроль актуальности](#контроль-актуальности)

---

## Введение и цели

**Scanner Infrastructure** — модульная event-driven платформа, которая позволяет:

- получать рыночные данные с бирж и MT5 с задержкой < 200 мс;
- генерировать сигналы на основе совмещённых данных и режимов рынка;
- отправлять команды на исполнение в MT5 и управлять трейлингом;
- собирать аналитику по сигналам, профилям и TP1→TP2 конверсиям;
- обеспечивать наблюдаемость с помощью Prometheus/Grafana.

Основные принципы:

1. **Чёткие границы** между ingestion, сигналами, исполнением и аналитикой.
2. **Идемпотентность** всех команд и событий.
3. **P4.1 Unified Latency Contract** — сквозная типизация и замеры задержек (t0-t5).
4. **Journal-First Execution** — персистентность состояния в журнале (`orders:exec`) перед действием.
5. **Документация = контракт** — каждое изменение отражается здесь.

---

## Карта доменов и ответственных

| Домен                          | Владелец                             | Репозитории / директории                               | SLA / SLO (P4.1)                        |
| ------------------------------ | ------------------------------------ | ------------------------------------------------------ | --------------------------------------- |
| Data acquisition & ingestion   | `@market-data-team`                  | `go-worker/`, `python-worker/services/tick_*`          | **t0-t1** ≤ 1 мс (P99)                  |
| Signal intelligence & processing| `@python-team`, `@quant-team`        | `python-worker/services/orderflow/`, `signal_hub/`     | **t1-t3** ≤ 5 мс (P99)                  |
| Experiment layer (A/B testing)| `@python-team`, `@quant-team`        | `python-worker/services/ab_*`                          | Детерминированное назначение ≤ 5 мс     |
| Risk management & validation  | `@trading-ops`, `@quant-team`        | `python-worker/services/risk_*`, `execution_gate_*`    | Валидация в рамках **t1-t3** budget     |
| Trade execution (Binance/MT5) | `@go-team`, `@trading-ops`           | `go-gateway/`, `binance_executor/`, `mt5_executor/`    | **t4-t5** ≤ 200 мс (P99) (Exchange)     |
| Post-trade analytics & reporting| `@trading-analytics`, `@python-team` | `projection_worker/`, `tracker/`, `reporting/`         | Материализация стейта ≤ 100 мс          |
| Observability & monitoring    | `@sre-team`, `@devops`               | `prometheus.yml`, `grafana/`, `health_monitor/`        | Метрики P4.1 в реальном времени         |
| Notification & communication  | `@trading-ops`                       | `telegram_worker/`, `notify_bridge/`                   | Доставка уведомлений ≤ 30 с             |
| Specialized analytics         | `@quant-team`, `@trading-analytics`  | `sl_quantile/`, `trailing_metrics/`, `news_agent/`     | Аналитика по запросу ≤ 10 мин           |

---

## Архитектурные слои

### Data Acquisition Layer

- **Go ingestion workers** (`go-worker/`) — подписки Binance (`aggTrade`, `depth`, `kline`). Публикация в Redis Streams `stream:tick_<symbol>` и Hash `candles:<symbol>:<tf>`. Встроенный Prometheus экспорт, контроль reconnect, watchdog latency.
- **MT5 TickBridge** (`mt5/TickBridge.mq5`) — пересылает тики и стакан в FastAPI (`tick_ingest_server`). Поддерживает повторные попытки (до 3), буферизацию в терминале, throttling 50 req/s.
- **Tick ingest server v2** (`python-worker/services/tick_ingest_server.py`) — HTTP `/tick`, `/book`; DualRedis (основной + `redis-ticks`); health-проверки `/healthz`, `/readyz`; счётчики `total_ticks`, `errors`, `dualredis_failovers`.
- **Book analytics** (`python-worker/services/book_analytics_service.py`) — строит OBI, визуализацию стакана, публикует статистику DOM, хранит ring buffer в Redis.

### Signal Intelligence Layer

- **Aggregated Signal Hub V2** — выравнивает таймлайны Binance/MT5, учитывает режимы (`regime-worker`), ATR кэши, latency; выбирает trailing профили; публикует enriched сигналы в Redis.
- **Crypto OrderFlow Handler** (`python-worker/handlers/crypto_orderflow_handler.py`) — высокопроизводительный обработчик криптовалютных тиков с pipeline V2, cost/edge gate интеграцией и расширенной телеметрией.
- **Crypto Futures OrderFlow Handler** (`python-worker/services/crypto_futures_orderflow_handler.py`) — специализированный обработчик фьючерсных контрактов с учетом специфики деривативов, маржинальных требований и ликвидаций.
- **Regime Services** — комплексная система анализа рыночных режимов: `regime-worker` (вычисление режимов), `regime_engine` (движок анализа), `regime_service` (PostgreSQL интеграция и хранение).
- **Signal Processing Pipeline** — многоуровневая обработка сигналов: `signal_preprocess` (предварительная валидация), `signal_publisher` (публикация), `async_signal_publisher` (высокопроизводительная async обработка), `signal_confidence` (ML-базированная оценка уверенности), `signal_quality` (метрики качества).
- **Experiment Layer** — полноценная система A/B-тестирования фильтров сигналов. Включает `experiment_manager.py` для детерминированного назначения вариантов на основе хэша сигнала, `experiment_metrics.py` для расчета метрик качества (precision, recall, expectancy, Sharpe ratio), PostgreSQL хранилище для результатов экспериментов. Интегрирован с базовым обработчиком сигналов для сравнения эффективности фильтров в реальном времени. Поддерживает множественные эксперименты одновременно с изоляцией метрик. Включает серию `ab_winner_*` сервисов для оценки и применения winning вариантов.
- **Risk Management** — комплексная система управления рисками: `risk_position_sizer` (размер позиций), `validate_signals` (валидация сигналов), `execution_gate_service` (ворот контроля исполнения), `burstiness_tracker` (отслеживание рыночных всплесков), `cancellation_spike_gate` (защита от отмен спайков).
- **Risk filters** (`python-worker/core/filtered_signal_writer.py`, `risk/`) — проверяют лимиты, quality-флаги, дедуплицируют сигналы, формируют `trade:state` и помещают команды в `orders:queue`.

### Trade Execution Layer

- **Order Management System** — комплексная система управления ордерами: `orders_router` (маршрутизация), `orders_http_bridge` (HTTP API), `mt5_event_executor` (MT5 интеграция), `signal_dispatcher*` (диспетчеризация сигналов), `signal_target_deliverer` (целевая доставка).
- **Order queue** (`orders:queue`, `orders:inflight`, `orders:history`) — обрабатывает `market`, `modify`, `trail`, `cancel`; приоритет трейлинга обеспечивается LPUSH, остальное RPOP; idempotency по `id`.
- **Go Gateway** (`go-gateway/internal/...`) — ServeMux 1.22, токеновая авторизация, rate limiting (`RPS=100` по умолчанию), HMAC (опционально), health `/health`.
- **TP Event Stack** — расширенная система обработки событий TP: `tp_event_listener.py` (прослушка), `tp1_trailing_orchestrator.py` (оркестрация), `order_trailing_dispatcher.py` (диспетчеризация); взаимодействуют с Redis, формируют команды `trail`, интеграция с Signal Performance Tracker.
- **MT5 Integration** — полная интеграция с MT5: скрипты `mt5/`, Go handler `internal/handlers/events_handler.go` для приёма событий и публикации в Redis, `mt5_trailing_move_logger.py` (логирование перемещений трейлинга).

### Post-Trade & Analytics Layer

- **Core Analytics Services**:
  - **Signal Performance Tracker** — главный оркестратор аналитики сигналов, координирует все компоненты аналитики. Читает `signals:*`, `stream:tick_*`, `notify:telegram`; обновляет `stats:*`, отправляет отчёты, экспортирует Prometheus метрики. Интегрирован с PostgreSQL для хранения метрик экспериментов и поддерживает многопоточную обработку.
  - **Trade Monitor** (`services/trade_monitor.py`) — отслеживает виртуальные позиции, обрабатывает TP/SL события, выполняет частичное закрытие позиций по TP1/TP2/TP3 с настраиваемыми долями. Поддерживает thread-safe операции, атомарные обновления состояния и интеграцию с experiment layer для A/B-тестирования.
  - **P&L Math Module** (`services/pnl_math.py`) — обеспечивает корректный расчет прибыли/убытков с учетом спецификаций символов (тиковая/линейная модель), хранит данные в Redis. Устраняет хардкод в расчетах, поддерживает различные модели расчета и fallback значения для основных символов.
  - **Stats Aggregator** (`services/stats_aggregator.py`) — агрегирует статистику по стратегиям, символам, таймфреймам с атомарными обновлениями через Redis pipeline. Включает EV gate EMA stats, empirical levels buffers для квантилей и интеграцию с экспериментальными метриками.

- **Reporting & Communication**:
  - **Reporting Service** (`services/reporting_service.py`) — формирует HTML-отчёты с графиками и метриками, отправляет в Telegram через `notify:telegram` stream. Поддерживает постраничную выборку закрытых сделок, периодические сводки и уведомления в реальном времени.
  - **Periodic Reporter** (`services/periodic_reporter.py`) — автоматические отчеты каждые N сделок с гибкой конфигурацией интервалов.
  - **Embedded Periodic Reporter** (`services/embedded_periodic_reporter.py`) — интегрированные отчеты в основные сервисы с минимальным overhead.
  - **Telegram Services** — `telegram_bot_commands.py` (команды бота), `telegram_labeler.py` (разметка), `telegram_worker` (доставка уведомлений), `notify_bridge` (маршрутизация), `notify_receiver` (обработка).

- **Specialized Analytics**:
  - **Stop-Loss Analytics** — `sl_quantile_aggregator.py` (квантили SL), `slq_risk_adjust.py` (риск-корректировка), `slq_store.py` (хранение SL данных).
  - **Trailing Analytics** — `trailing_metrics.py` (метрики трейлинга), `trailing_edge_analyzer.py` (анализ краев), `trail_giveback_stats.py` (статистика отдачи).
  - **Execution Analytics** — `execution_cost_ema.py` (стоимость исполнения), `execution_slippage_stats.py` (проскальзывание), `slippage_model*.py` (модели проскальзывания).
  - **Expected Value Analytics** — `ev_giveback_stats.py` (отдача EV), `ev_tp1_stats.py` (TP1 EV статистика).

- **Data Management**:
  - **Trade Events Logger** (`services/trade_events_logger.py`) — фиксирует `trade:timeline:{sid}`, `trade:events:{sid}`, TTL 7 дней с метаданными о типах событий. Записывает все события в Redis hash и stream для trade_back анализа, поддерживает сжатие и оптимизацию хранения.
  - **Trade Metrics Service** (`services/trade_metrics_service.py`) — специализированные метрики по сделкам и производительности.
  - **Analytics API Service** (`services/analytics_api_service.py`) — REST API для доступа к аналитическим данным.
  - **Analytics DB** (`services/analytics_db.py`) — интеграция с PostgreSQL для хранения аналитических данных.

### Infrastructure Layer

- **Redis инстансы** — `scanner-redis-core`, `scanner-redis-trades`, `scanner-redis-ticks`, `scanner-redis-metrics`; конфиги в `config/redis/*.conf`; AOF + snapshot; мониторинг через `make redis-stats`.
- **PostgreSQL** — `scanner-postgres`; используется для логирования сигналов, экспериментов, аналитики и метрик A/B-тестирования. Схема включает таблицы сигналов, экспериментов, метрик качества и результатов тестирования. Миграции в `python-worker/migrations/` с поддержкой rollback. Интегрирован с experiment_manager и experiment_metrics для A/B-тестирования, поддерживает индексы для быстрого поиска и партиционирование по времени.
- **Docker Compose профили** — `docker-compose.yml`, `docker-compose-optimized.yml`, `docker-compose.tp-trailing.yml`, `docker-compose.mt5-executor.yml`, `docker-compose.hub-v2.yml`, `docker-compose-postgres.yml`.
- **Observability stack** — Prometheus, Grafana, Alertmanager, Makefile диагностика (`make diagnose`, `make full-status`).

---

## Сквозные потоки данных

### Tick → Signal → Order

```
Binance WS ─▶ Go Worker ─▶ Redis stream:tick_<symbol>
                                      │
MT5 TickBridge ─HTTP▶ Tick Ingest ────┘
                                      │
                                      ▼
                          Aggregated Signal Hub V2
                                      │
                         Regime filters & risk checks
                                      │
                                      ▼
                         Redis signals:{sid} & orders:queue
                                      │
                                      ▼
                               Go Gateway API
                                      │
                                      ▼
                                  MT5 Executor
```

- Задержка `tick → stream:tick_*` ≤ 400 мс (P95).
- Задержка `signal → orders:queue` ≤ 1.5 с (P95).
- Idempotency: `sid` и `order.id` используются как ключи.

### TP1 Trailing

```
TP1_HIT (MT5) ─▶ POST /events/mt5 ─▶ events:trades stream
                                        │
                                        ▼
                               TP Event Listener
                                        │
                                        ▼
                           TP1 Trailing Orchestrator
                                        │
                                        ▼
                      Order Trailing Dispatcher (HTTP /orders/push)
                                        │
                                        ▼
                                   MT5 Executor
                                        │
                                        ▼
                            TRAILING_MOVE events & timeline
```

- SLA: `TP1_HIT → trail command` ≤ 2.5 с (P95), `TRAILING_MOVE` зарегистрирован ≤ 5 с.
- Контроль метрик: `trailing_latency_ms`, `trailing_started_total`, `mt5_events_total`.

### Analytics & Reporting

```
signals:{sid}, stream:tick_* ─▶ Signal Performance Tracker
                                         │
                                         ├─▶ stats:{strategy}:{symbol}:{tf}
                                         ├─▶ notify:telegram (live alerts)
                                         └─▶ reports/*.csv, Grafana annotations

events:trades, trade:timeline ─▶ Trade Events Logger ─▶ dashboards / exports
```

- Регулярность отчётов: каждые `REPORT_TRIGGER_COUNT` сделок (по умолчанию 100) через `PeriodicReporter`, ежедневно в заданный час UTC через `DAILY_SUMMARY_HOUR`.
- Telegram уведомления: `notify:telegram` stream.

---

## Инвентаризация сервисов

### Data Acquisition Layer

| Сервис / компонент           | Технология             | Важные зависимости                    | Критичность    | Документация                                         |
| ---------------------------- | ---------------------- | ------------------------------------- | -------------- | ---------------------------------------------------- |
| `go-worker`                  | Go 1.22                | Redis core/ticks, Binance WS          | Высокая        | `ticks/TICKS_ARCHITECTURE.md`                        |
| `tick-ingest-server`         | Python (FastAPI)       | Redis core/ticks, MT5 HTTP            | Высокая        | `ticks/TICKS_DEVELOPMENT.md`                         |
| `crypto-htf-aggregator`      | Python                 | Redis ticks, high timeframe data      | Средняя        | `ticks/TICKS_ARCHITECTURE.md`                        |
| `dom-ingester`               | Python                 | Redis ticks, MT5 streams              | Средняя        | `ticks/TICKS_ARCHITECTURE.md`                        |
| `ohlc-aggregator`            | Python                 | Redis ticks, timeframes               | Средняя        | `ticks/TICKS_DEVELOPMENT.md`                         |
| `news-analyzer`              | Python                 | News sources, Redis                   | Средняя        | `news_pipeline/README.md`                            |
| `news-feature-store`         | Python                 | News data, feature extraction         | Средняя        | `news_pipeline/README.md`                            |
| `news-ingestor-go`           | Go                     | News APIs, Redis                      | Средняя        | `news_pipeline/README.md`                            |
| `news-ingestor-py`           | Python                 | News sources, Redis                   | Средняя        | `news_pipeline/README.md`                            |
| `news-watchdog`              | Python                 | News pipeline monitoring              | Средняя        | `news_pipeline/README.md`                            |

### Signal Intelligence Layer

| Сервис / компонент           | Технология             | Важные зависимости                    | Критичность    | Документация                                         |
| ---------------------------- | ---------------------- | ------------------------------------- | -------------- | ---------------------------------------------------- |
| `aggregated-hub`             | Python                 | streams ticks, regime-worker          | Высокая        | `trading_workflow/ticks_ingestion.md`                |
| `atr-worker`                 | Python                 | Market data, ATR calculations         | Средняя        | `crypto_tick_processing/README.md`                   |
| `crypto-orderflow-service`   | Python                 | Redis ticks, PostgreSQL experiments   | Высокая        | `crypto_tick_processing/README.md`                   |
| `multi-symbol-orderflow`     | Python                 | Multiple symbols, Redis streams       | Высокая        | `crypto_tick_processing/README.md`                   |
| `signal-dispatcher`          | Python                 | Redis streams, order routing          | Высокая        | `trading_workflow/order_creation.md`                 |
| `signal-generator`           | Python                 | Market data, signal algorithms        | Высокая        | `crypto_tick_processing/README.md`                   |
| `signal-hub`                 | Python                 | Signal aggregation, Redis             | Высокая        | `trading_workflow/ticks_ingestion.md`                |
| `signal-outbox-router`       | Python                 | Signal routing, Redis streams         | Средняя        | `trading_workflow/order_creation.md`                 |
| `signal-parser-worker`       | Python                 | Signal parsing, validation            | Средняя        | `crypto_tick_processing/README.md`                   |
| `signal-performance-tracker` | Python                 | signals streams, ticks, notify stream | Высокая        | `signal_analytics/README.md`                         |
| `signal-target-worker-*`     | Python                 | Target routing, Redis streams         | Средняя        | `trading_workflow/order_creation.md`                 |
| `regime-quantiles-job`       | Python                 | Market regime analysis                | Средняя        | `ARCHITECTURE.md#signal-intelligence-layer`          |
| `regime-storage`             | Python                 | PostgreSQL, Redis regime data         | Средняя        | `signal_analytics/README.md`                         |
| `regime-worker`              | Python                 | Redis core, market data               | Средняя        | `ARCHITECTURE.md#signal-intelligence-layer`          |

### Experiment Layer (A/B Testing)

| Сервис / компонент           | Технология             | Важные зависимости                    | Критичность    | Документация                                         |
| ---------------------------- | ---------------------- | ------------------------------------- | -------------- | ---------------------------------------------------- |
| `ab-policy-suggester-timer`  | Python                 | A/B testing, policy suggestions       | Средняя        | `python-worker/EXPERIMENT_LAYER_README.md`           |
| `ab-winner-apply-runner`     | Python                 | Winner application, Redis             | Средняя        | `python-worker/EXPERIMENT_LAYER_README.md`           |
| `ab-winner-evaluator`        | Python                 | PostgreSQL experiments, Redis         | Средняя        | `python-worker/EXPERIMENT_LAYER_README.md`           |
| `ab-winner-lcb-timer`        | Python                 | Lower confidence bound analysis       | Средняя        | `python-worker/EXPERIMENT_LAYER_README.md`           |
| `scanner-ab-winner`          | Python                 | A/B testing winner selection          | Средняя        | `python-worker/EXPERIMENT_LAYER_README.md`           |
| `scanner-ab-winner-job`      | Python                 | Winner job execution                  | Средняя        | `python-worker/EXPERIMENT_LAYER_README.md`           |

### Trade Execution Layer

| Сервис / компонент           | Технология             | Важные зависимости                    | Критичность    | Документация                                         |
| ---------------------------- | ---------------------- | ------------------------------------- | -------------- | ---------------------------------------------------- |
| `go-gateway`                 | Go 1.22                | Redis core, MT5 EA                    | Высокая        | `CONFIGURATION.md`, `DEVELOPMENT.md`                 |
| `mt5-event-executor`         | Python                 | MT5 API, Redis events                 | Высокая        | `trading_workflow/order_creation.md`                 |
| `of-confirm-service`         | Python                 | Order flow confirmations              | Высокая        | `trading_workflow/order_creation.md`                 |
| `paper-executor`             | Python                 | Paper trading simulation              | Средняя        | `trading_workflow/order_creation.md`                 |
| `scanner-autopilot`          | Python                 | Automated trading logic               | Высокая        | `trading_workflow/order_creation.md`                 |
| `scanner-autopilot-guardrail`| Python                 | Risk controls, guardrails             | Высокая        | `trading_workflow/order_creation.md`                 |
| `scanner-autopilot-reporter` | Python                 | Autopilot performance reporting       | Средняя        | `signal_analytics/reporting.md`                      |
| `scanner-autopilot-tm`       | Python                 | Trade management autopilot            | Высокая        | `trading_workflow/tp1_trailing.md`                   |
| `scanner-cooldown-suggester` | Python                 | Cooldown period suggestions           | Средняя        | `trading_workflow/order_creation.md`                 |
| `scanner-entry-policy-autopilot`| Python               | Entry policy automation               | Высокая        | `trading_workflow/order_creation.md`                 |
| `scanner-network`            | Python                 | Network communication management      | Средняя        | `CONFIGURATION.md`                                    |
| `scanner-smt-aggregator`     | Python                 | Smart aggregation logic               | Средняя        | `trading_workflow/order_creation.md`                 |
| `scanner-tm-autopilot`       | Python                 | Trade management automation           | Высокая        | `trading_workflow/tp1_trailing.md`                   |
| `scanner-trailing-autotune`  | Python                 | Trailing stop autotuning              | Средняя        | `trading_workflow/tp1_trailing.md`                   |
| `smt-entry-candidate-service`| Python                 | Smart entry candidate selection       | Средняя        | `crypto_tick_processing/README.md`                   |

### Post-Trade & Analytics Layer

| Сервис / компонент           | Технология             | Важные зависимости                    | Критичность    | Документация                                         |
| ---------------------------- | ---------------------- | ------------------------------------- | -------------- | ---------------------------------------------------- |
| `signal-performance-tracker` | Python (async + Redis) | signals streams, ticks, notify stream | Средне-высокая | `signal_analytics/README.md`                         |
| `trade-monitor`              | Python                 | signals, ticks, pnl_math              | Высокая        | `signal_analytics/pnl_analysis.md`                   |
| `post-sl-analyzer`           | Python                 | Stop-loss analysis, trade data        | Средняя        | `signal_analytics/sl_quantile_analysis.md`           |
| `sl-quantile-aggregator`     | Python                 | Stop-loss quantiles, statistics       | Средняя        | `signal_analytics/sl_quantile_analysis.md`           |
| `trade-back`                 | Python                 | Historical trade data, backtesting    | Средняя        | `signal_analytics/README.md`                         |
| `tm-reporter-bot`            | Python                 | Telegram reporting, trade metrics     | Средняя        | `signal_analytics/reporting.md`                      |
| `trailing-recommender-timer` | Python                 | Trailing recommendations, timing      | Средняя        | `trading_workflow/tp1_trailing.md`                   |
| `trailing-tuner`             | Python                 | Trailing optimization, parameters     | Средняя        | `trading_workflow/tp1_trailing.md`                   |
| `periodic-reporter`          | Python                 | Stats data, configurable intervals    | Средняя        | `signal_analytics/reporting.md`                      |

### Risk Management & Validation Layer

| Сервис / компонент           | Технология             | Важные зависимости                    | Критичность    | Документация                                         |
| ---------------------------- | ---------------------- | ------------------------------------- | -------------- | ---------------------------------------------------- |
| `burstiness-tracker`         | Python                 | Market burst detection, throttling     | Средняя        | `crypto_tick_processing/README.md`                   |
| `edge-gate-ingestor`         | Python                 | Edge gate data ingestion              | Высокая        | `crypto_tick_processing/README.md`                   |
| `entry-policy-lcb-guard-service`| Python               | Entry policy guardrails, risk control | Высокая        | `trading_workflow/order_creation.md`                 |
| `execution-gate-service`     | Python                 | Execution gates, throttling           | Высокая        | `trading_workflow/order_creation.md`                 |
| `scanner-entry-policy-safety-guard`| Python             | Safety guards for entry policies      | Высокая        | `trading_workflow/order_creation.md`                 |

### Infrastructure & Observability

| Сервис / компонент           | Технология             | Важные зависимости                    | Критичность    | Документация                                         |
| ---------------------------- | ---------------------- | ------------------------------------- | -------------- | ---------------------------------------------------- |
| `docker-watchdog`            | Python                 | Container health monitoring           | Высокая        | `CONFIGURATION.md#мониторинг-и-алертинг`             |
| `golden-calibration-replay`  | Python                 | Calibration replay, testing           | Средняя        | `DEVELOPMENT.md#интеграционные-сценарии`             |
| `grafana`                    | OSS                    | Dashboards, visualization             | Высокая        | `CONFIGURATION.md#мониторинг-и-алертинг`             |
| `health-monitor`             | Python                 | Service health checks, Redis          | Высокая        | `CONFIGURATION.md#мониторинг-и-алертинг`             |
| `migration-runner`           | Python                 | Database migrations, PostgreSQL       | Высокая        | `CONFIGURATION.md#база-данных`                       |
| `prometheus`                 | OSS                    | Metrics collection, alerting          | Высокая        | `CONFIGURATION.md#мониторинг-и-алертинг`             |
| `redis`                      | Redis                  | Core data store, caching              | Высокая        | `CONFIGURATION.md#redis-инфраструктура`              |
| `redis-cleanup`              | Python                 | Redis cleanup, maintenance            | Средняя        | `CONFIGURATION.md#redis-инфраструктура`              |
| `redis-monitor`              | Python                 | Redis monitoring, health checks       | Средняя        | `CONFIGURATION.md#redis-инфраструктура`              |
| `redis-ticks`                | Redis                  | High-frequency tick data              | Высокая        | `ticks/TICKS_ARCHITECTURE.md`                        |
| PostgreSQL                   | PostgreSQL 15+         | Analytics, experiments, persistence   | Высокая        | `CONFIGURATION.md#база-данных`                       |

### Notification & Communication Layer

| Сервис / компонент           | Технология             | Важные зависимости                    | Критичность    | Документация                                         |
| ---------------------------- | ---------------------- | ------------------------------------- | -------------- | ---------------------------------------------------- |
| `notify-worker`              | Python                 | Notification routing, Redis           | Средняя        | `signal_analytics/reporting.md`                      |
| `telegram-worker`            | Python                 | Telegram delivery, batch processing   | Средняя        | `signal_analytics/reporting.md`                      |
| `tg-sessions`                | Python                 | Telegram session management           | Средняя        | `signal_analytics/reporting.md`                      |

### Specialized Analytics Services

| Сервис / компонент           | Технология             | Важные зависимости                    | Критичность    | Документация                                         |
| ---------------------------- | ---------------------- | ------------------------------------- | -------------- | ---------------------------------------------------- |
| `backtest-data`              | Python                 | Historical data, backtesting framework| Средняя        | `signal_analytics/README.md`                         |
| `binance-iceberg-detector`   | Python                 | Binance order book analysis           | Средняя        | `crypto_tick_processing/README.md`                   |
| `calendar-feature-store`     | Python                 | Calendar data, feature extraction     | Средняя        | `news_pipeline/README.md`                            |
| `htf-zones-publisher`        | Python                 | Higher timeframe zones, Redis         | Средняя        | `crypto_tick_processing/README.md`                   |
| `py-obi-service`             | Python                 | Order book imbalance calculations     | Средняя        | `crypto_tick_processing/README.md`                   |
| `stream-trimmer`             | Python                 | Redis stream trimming, maintenance    | Средняя        | `CONFIGURATION.md#redis-инфраструктура`              |

---

## Структуры данных и хранилища

### Стандарты данных

**Временные метки (Timestamps):**

- Все временные метки в Redis хранятся в формате **Unix timestamp в миллисекундах (UTC)**.
- Это стандарт проекта для всех данных, записываемых в Redis Streams, Hash, Sorted Sets.
- Python: используется `get_current_timestamp_ms()` из `common/time_utils.py` (возвращает UTC).
- Go: используется `timeutil.GetCurrentTimestampMs()` (явно `time.Now().UTC().UnixMilli()`).
- Примеры полей: `ts`, `timestamp`, `written_at`, `tick_ts`, `closeTime` — все в UTC миллисекундах.

**Redis Connection Management:**

- Все сервисы используют **singleton pattern с connection pool** для переиспользования соединений.
- Python: `get_redis()` из `core/redis_client.py` и `get_ticks_redis()` из `core/ticks_redis_client.py` создают connection pool при первом вызове.
- Go: используется `timeutil` пакет с явным UTC для всех временных меток.
- Настройки pool: `max_connections=100`, `socket_keepalive=True`, `health_check_interval=30`.
- Автоматическое переподключение при разрыве соединения (проверка через `ping()`).
- Thread-safe создание соединения с использованием блокировок.
- Преимущества: устранение множественных переподключений, снижение нагрузки на Redis, изоляция высокочастотных тиков от основных данных.

### Redis профили

| Инстанс                 | Назначение                             | Конфиг                                | Ключевые схемы                               |
| ----------------------- | -------------------------------------- | ------------------------------------- | -------------------------------------------- |
| `scanner-redis-core`    | Сигналы, ордера, события               | `redis-stable.conf`                   | `signals:*`, `orders:*`, `events:trades`     |
| `scanner-redis-ticks`   | Высокочастотные тики и DOM             | `redis-ticks.conf`                    | `stream:tick_*`, `stream:book_*`, `tick:*`   |
| `scanner-redis-trades`  | Таймлайны, аналитика трейдов           | `redis-optimized-for-trade-back.conf` | `trade:timeline:*`, `trade:events:*`         |
| `scanner-redis-metrics` | Статистика, метрики, профили трейлинга | `redis-metrics.conf`                  | `stats:*`, `profiles:trailing:*`, `health:*` |

### Ключевые шаблоны

| Шаблон                           | Тип    | TTL / политика          | Описание                              |
| -------------------------------- | ------ | ----------------------- | ------------------------------------- |
| `stream:tick_<symbol>`           | Stream | 24 ч, MAXLEN 200k       | Объединённый поток Binance + MT5      |
| `stream:book_<symbol>`           | Stream | 12 ч                    | Снимки DOM                            |
| `signals:{sid}`                  | Hash   | TTL 14 дней             | Обогащённый сигнал                    |
| `trade:state:{sid}`              | Hash   | TTL 14 дней             | Текущее состояние сделки              |
| `orders:queue`                   | List   | Без TTL                 | Очередь команд                        |
| `orders:inflight`                | Hash   | TTL 1 день              | Команды в обработке                   |
| `orders:history`                 | List   | Ограничение 500 записей | Лог команд                            |
| `events:trades`                  | Stream | TTL 14 дней, MAXLEN 10k | События MT5/трейлинга                 |
| `trade:timeline:{sid}`           | ZSet   | TTL 7 дней              | Таймлайн сделки                       |
| `stats:{strategy}:{symbol}:{tf}` | Hash   | Без TTL                 | Статистика Signal Performance Tracker |
| `profiles:trailing:*`            | Hash   | Без TTL                 | Параметры профилей трейлинга          |

Резервное копирование и восстановление описаны в `CONFIGURATION.md#redis-инфраструктура`.

---

## Интеграции и внешние системы

### MT5

- **EA**: `mt5/TickBridge.mq5`, `mt5/MT5_TP_EVENTS_INTEGRATION_EXAMPLE.mq5`.
- **Коммуникация**:
  - `/orders/poll` — MT5 забирает команды;
  - `/orders/ack` — подтверждает исполнение;
  - `/events/mt5` — отправляет `TP1_HIT`, `TRAILING_MOVE`, `SL_HIT`, `ERROR`.
- **Безопасность**: токен `MT5_EVENT_TOKEN`, опциональный HMAC (`go-gateway/internal/auth/hmac.go`), IP whitelist на уровне reverse proxy.

### Биржевые и дополнительные источники

- Binance (WS + REST fallback) — адаптеры в `go-worker`.
- Новые источники: реализовать адаптер в `adapters/<source>`, добавить конфиг в `config/feeds/*.yaml`, обновить `ticks/`.
- Historical replay (`analysis/replay_ticks.py`) для QA и бэктестов.

### Уведомления

- `notify:telegram` — основные события сигналов и трейлинга.
- `telegram-worker/` — доставка в Telegram; конфиги в `config/services/telegram.env`.
- Опциональные интеграции: email, Slack (через `reports/`).

### Observability

- Prometheus (`prometheus.yml`) собирает метрики из Go/Python сервисов.
- Grafana dashboards: `grafana_dashboard_websocket.json`, `grafana_multi_symbol_dashboard.json`, `grafana_tp_trailing.json`.
- Alertmanager (опционально) отсылает уведомления в Slack/Telegram (описано в `CONFIGURATION.md#мониторинг-и-алертинг`).

---

## Надёжность, масштабирование, безопасность

### Надёжность

- DualRedis fallback для ingest и tracker.
- Раздельные Compose профили позволяют перезапускать подсистемы без простоя всего стека.
- Retriable операции: acknowledgement в Redis stream с requeue при ошибках.
- Бэкапы Redis (`make backup-redis`), тест восстановления (`make restore-redis`) не реже 1 раза в две недели.

### Масштабирование

- Горизонтальное масштабирование `go-worker`, `tp-event-listener`, `tp1-trailing-orchestrator`, `signal_performance_tracker` через Docker replicas.
- Разделение нагрузки по Redis инстансам и consumer группам.
- Использование `docker-compose-optimized.yml` для high-load стендов.

### Безопасность

- Токены и секреты в `.env.local` (dev) / Vault (prod).
- Авторизация `/orders/push`, `/orders/poll`, `/events/mt5` через Bearer токены и rate limiting.
- Поддержка HTTPS/mTLS через `infra/nginx`.
- Разделение сетей: `scanner_internal` для приватных сервисов, `scanner_public` для MT5/ingress.

---

## Наблюдаемость и SLO

| Метрика                   | Источник                     | Цель / порог          | Алерт                                            |
| ------------------------- | ---------------------------- | --------------------- | ------------------------------------------------ |
| `tick_gap_seconds`        | Tick ingest server           | P95 ≤ 0.4 с           | > 5 с в течение 1 мин → PagerDuty                |
| `trailing_latency_ms`     | Trailing metrics exporter    | P95 ≤ 2500 мс         | > 4000 мс в течение 3 мин → Slack `#scanner_ops` |
| `signals_generated_total` | Aggregated Hub V2            | Мониторинг объёма     | Падение > 10% от медианы → investigate           |
| `orders_queue_length`     | Go Gateway                   | Норма < 5             | > 20 → проверить MT5/gateway                     |
| `stats_report_latency_ms` | Signal Performance Tracker   | < 5 мин от расписания | > 10 мин → оповещение                            |
| `/health` endpoints       | Go Gateway, trackers, ingest | HTTP 200              | 5xx → перезапуск / эскалация                     |

Makefile цели `make diagnose`, `make trailing-stats`, `make tick-streams`, `make tracker-stats` — основной инструмент ручных проверок.

---

## Архитектурные решения (ADR)

- **ADR-001 DualRedis** — стратегия записи в два Redis и правила failover (`docs/adr/ADR-001-dual-redis.md`).
- **ADR-002 ServeMux 1.22** — выбор нового роутера Go с поддержкой wildcard и REST (`docs/adr/ADR-002-servemux.md`).
- **ADR-003 Order queue LPUSH/RPOP** — обоснование приоритетов без отдельной priority queue (`docs/adr/ADR-003-order-queue.md`).
- **ADR-004 Signal Performance Tracker architecture** — многопоточность, consumer группы, reporting (`docs/adr/ADR-004-signal-tracker.md`).

Перед изменением архитектурных компонентов проверяйте соответствующий ADR и обновляйте его при необходимости.

---

## Контроль актуальности

- Последний аудит: 2026-04-02.

**Последние изменения (2026-04-02):**

- **Синхронизация с P4.1**: Внедрен сквозной контроль задержек (t0-t5).
- **Journal-First Execution**: Переход на модель персистентного журнала ордеров.
- **Signal Gating G0-G15**: Полное описание цепочки гейтов в CryptoOrderFlow.
- **Инфраструктура**: Обновлена сегментация Redis шардов и модель ACL.
- **Новые компоненты**: BinanceExecutor, ProjectionWorker, BootstrapSupervisor, NewsAgent.
- Ответственные: `@system-architect`, `@python-team-lead`, `@go-team-lead`, `@market-data-team`, `@quant-team`.
- Требуется обновление при изменении:
  - схем Redis, PostgreSQL, API Gateway, MT5 протоколов, SLO;
  - составов Docker профилей, ADR, экспериментальных фич, новых обработчиков сигналов.
- Проверка линк-чекером и Markdown lint обязательна (`make docs-lint`).

Для предложений по улучшению пишите в `#scanner_architecture`.
