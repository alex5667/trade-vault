# 🛠️ Руководство по разработке (2026-01-27)

> Обновлено после интеграции TP1 Trailing, MT5 Event Executor и расширения Makefile.  
> Авторы: Senior Go/Python Developer + Senior Trading Systems Analyst.

---

## 📋 Содержание

1. [Быстрый старт](#быстрый-старт)
2. [Makefile и полезные команды](#makefile-и-полезные-команды)
3. [Python-сервисы](#python-сервисы)
4. [Go Gateway и Go-сервисы](#go-gateway-и-go-сервисы)
5. [Интеграционные сценарии](#интеграционные-сценарии)
6. [Тестирование](#тестирование)
7. [Отладка и профилирование](#отладка-и-профилирование)
8. [Стандарты кодирования](#стандарты-кодирования)
9. [Частые вопросы](#частые-вопросы)

---

## 🚀 Быстрый старт

```bash
git clone git@github.com:front/trade/scanner_infra.git
cd scanner_infra
cp config/env.example .env.local     # заполните токены
make up-bg                           # базовая инфраструктура
make trailing-start                  # включить TP Event Listener
make status                          # убедиться, что всё зелёное
```

### Обязательные шаги

1. **Redis** — должен быть доступен (`make redis-stats`).
2. **Go Gateway** — `make gateway-status`, потом `curl http://localhost:8090/health`.
3. **Python Worker** — `docker logs python-worker` без ошибок.
4. **Prometheus/Grafana** — опционально, но рекомендуем (`make monitor-status`).
5. **MT5 интеграция** — локально запускаем через `START_TP1_TRAILING.sh`.

---

## 🧰 Makefile и полезные команды

```bash
make help                # общий обзор
make full-status         # все сервисы и их состояние
make logs                # tail -f ключевых сервисов
make diagnose            # комплексная диагностика

make trailing-status     # TP Event Listener
make trailing-logs       # live логи трейлинга
make trailing-stats      # статистика трейлинга
make trailing-test       # интеграционный тест трейлинга
make tracker-status      # состояние Signal Performance Tracker
make tracker-logs        # tail трекера
make tracker-stats       # агрегированная статистика по стратегиям/источникам
make tracker-restart     # перезапуск трекера
make experiment-status   # статус экспериментального слоя
make postgres-status     # проверка PostgreSQL и миграций

make gateway-status      # состояние Go Gateway
make gateway-logs        # логи gateway
make gateway-test        # тестовые запросы к API

make tick-status         # состояние tick_ingest_server и book analytics
make tick-streams        # лаги consumer групп stream:tick_*
make redis-stats         # Redis info
make redis-flush         # очистка (осторожно!)
```

**Очистка данных по сигналам и сделкам:**

Для выборочной очистки только сигналов, ордеров и сделок (без очистки всех данных Redis) используйте специализированный скрипт:

```bash
# Интерактивный режим (с подтверждением)
./scripts/clear_trades_and_signals.sh

# Автоматическое подтверждение
./scripts/clear_trades_and_signals.sh --yes
```

Скрипт очищает данные из всех Redis контейнеров (`scanner-redis`, `scanner-redis-worker-1`, `scanner-redis-worker-2`), удаляя:

- Все сигналы (`signals:*`, `signal:*`)
- Все ордера (`order:*`)
- Все сделки (`trade:*`)
- Все события (`events:trades`, `trades:closed`)

**Примечание:** Скрипт был упрощен для улучшения читаемости и поддержки (используются массивы и циклы вместо дублирования кода).

**Docker**:

```bash
make compose-up          # docker compose up -d с текущим профилем
make compose-down        # остановка
make compose-restart     # перезапуск
make trailing-restart    # перезапуск только TP Event Listener
```

---

## 🐍 Python-сервисы

### Структура

```
python-worker/
  ├── core/                    # базовые компоненты (фильтры, сериализация)
  ├── services/                # бизнес-логика (trailing, trackers, listeners)
  ├── adapters/                # внешние клиенты, http/redis wrappers
  ├── tests/                   # pytest
  └── requirements.txt
```

### Ключевые модули

#### Data Acquisition & Ingestion Services
- `services/tick_ingest_server.py` — FastAPI-сервис для MT5 тиков/DOM.
- `services/book_analytics_service.py` — OBI/DOM аналитика, PNG рендеры.
- `services/crypto_futures_orderflow_handler.py` — специализированный обработчик фьючерсных контрактов.
- `services/ohlc_aggregator.py` — агрегация OHLC данных по таймфреймам.
- `services/capture_microbars.py` — захват микробаров с высокой точностью.

#### Signal Intelligence & Processing
- `aggregated_signal_hub_v2.py` — ядро сигналов (отдельный процесс).
- `services/signal_preprocess.py` — предварительная обработка и валидация сигналов.
- `services/signal_publisher.py` — публикация сигналов в Redis streams.
- `services/async_signal_publisher.py` — высокопроизводительная async публикация.
- `services/sync_signal_publisher.py` — синхронная публикация с гарантией доставки.
- `services/signal_confidence.py` — ML-базированная оценка уверенности сигналов.
- `services/signal_quality.py` — метрики качества и валидация сигналов.
- `handlers/crypto_orderflow_handler.py` — высокопроизводительный обработчик криптовалютных тиков с pipeline V2.

#### Experiment Layer (A/B Testing)
- `handlers/experiment_manager.py` — управление A/B экспериментами, детерминированное назначение вариантов.
- `handlers/experiment_metrics.py` — расчет метрик экспериментов (expectancy, Sharpe ratio, precision/recall).
- `services/ab_winner_evaluator_*.py` — серия сервисов оценки winning вариантов A/B тестов.
- `services/ab_winner_suggester_*.py` — сервисы предложения вариантов для тестирования.
- `services/ab_router.py` — маршрутизация на основе результатов A/B тестирования.
- `services/entry_policy_ab_gate.py` — фильтры входа на основе A/B результатов.

#### Risk Management & Validation
- `services/risk_position_sizer.py` — расчет размера позиций на основе риск-менеджмента.
- `services/validate_signals.py` — валидация сигналов по правилам качества.
- `services/execution_gate_service.py` — контроль исполнения с throttling.
- `services/burstiness_tracker.py` — отслеживание рыночных всплесков.
- `services/cancellation_spike_gate.py` — защита от спайков отмен ордеров.

#### Trade Execution & Order Management
- `services/orders_router.py` — маршрутизация ордеров и приоритизация.
- `services/orders_http_bridge.py` — HTTP API для ордеров.
- `services/mt5_event_executor.py` — исполнение ордеров в MT5.
- `services/signal_dispatcher*.py` — диспетчеризация сигналов (несколько реализаций).
- `services/signal_target_deliverer.py` — целевая доставка сигналов.
- `services/tp_event_listener.py` — подписка на `events:trades`.
- `services/tp1_trailing_orchestrator.py` — оркестратор трейлинга.
- `services/order_trailing_dispatcher.py` — HTTP клиент для Go Gateway.
- `services/trailing_profiles.py` — профили трейлинга (`ATR`, `POINTS`).

#### Post-Trade Analytics & Reporting
- `services/signal_performance_tracker.py` — аналитика сигналов и отчёты.
- `services/trade_monitor.py` — отслеживание виртуальных позиций, обработка TP/SL, частичное закрытие позиций, thread-safe операции и атомарные обновления состояния.
- `services/pnl_math.py` — корректный расчет P&L с учетом спецификаций символов.
- `services/stats_aggregator.py` — агрегация статистики по стратегиям/символам с атомарными обновлениями.
- `services/reporting_service.py` — формирование HTML-отчётов и отправка в Telegram.
- `services/periodic_reporter.py` — автоматические периодические отчеты.
- `services/trade_events_logger.py` — логирование событий сделок в Redis с метаданными.
- `services/trade_metrics_service.py` — специализированные метрики по торговым операциям.
- `services/analytics_api_service.py` — REST API для аналитических данных.
- `services/analytics_db.py` — интеграция с PostgreSQL для аналитики.

#### Specialized Analytics Services
- `services/sl_quantile_aggregator.py` — квантильный анализ стоп-лоссов.
- `services/slq_risk_adjust.py` — корректировка рисков на основе SL-аналитики.
- `services/slq_store.py` — хранение данных SL-квантилей.
- `services/trailing_metrics.py` — метрики эффективности трейлинга.
- `services/trailing_edge_analyzer.py` — анализ эффективности трейлинг стратегий.
- `services/ev_giveback_stats.py` — статистика отдачи ожидаемой доходности (EV giveback).
- `services/ev_tp1_stats.py` — статистика EV для TP1 уровней.
- `services/execution_cost_ema.py` — экспоненциальное сглаживание стоимости исполнения.
- `services/execution_slippage_stats.py` — статистика проскальзывания при исполнении.
- `services/slippage_model*.py` — моделирование и предсказание проскальзывания.

#### Notification & Communication
- `services/telegram_bot_commands.py` — команды Telegram бота.
- `services/telegram_labeler.py` — разметка для Telegram.
- `services/notify_bridge.py` — маршрутизация уведомлений.
- `services/notify_receiver.py` — обработка уведомлений.
- `services/telegram_worker.py` — доставка в Telegram.

#### Infrastructure & Monitoring
- `services/health_monitor.py` — мониторинг здоровья сервисов.
- `services/error_monitor.py` — агрегация и алертинг ошибок.
- `services/redis_stream_runner_base.py` — базовый фреймворк для Redis streams.
- `services/stream_worker.py` — обработчик Redis streams.
- `services/persistence_manager.py` — управление персистентностью данных.

#### Core Components
- `core/filtered_signal_writer.py` — запись сигналов в Redis и публикация в очередь.
- `core/redis_client.py` — singleton Redis client с connection pool.
- `core/ticks_redis_client.py` — специализированный клиент для тиков.
- `common/time_utils.py` — утилиты работы со временем (UTC timestamps).

#### Signal Performance Tracker

- Конфигурация: `config/signal_tracker_config.json` (symbols, strategies, consumer groups, reporting).
- Потоки: `signals:{strategy}:{symbol}`, `notify:telegram`, `stream:tick_{symbol}`.
- Компоненты: `TradeMonitor`, `StatsAggregator`, `ReportingService`, `PnlMath`, `EmbeddedPeriodicReporter`, `RegimeGuardService`.
- Интеграция: поддержка многопоточной обработки сигналов и тиков, A/B-тестирование через experiment layer.
- Запуск: `python -m services.signal_performance_tracker` (использует `TRACKER_CONFIG_PATH` либо ENV).
- Makefile: `make tracker-status`, `make tracker-logs`, `make tracker-stats`, `make tracker-restart`.
- Скрипты диагностики: `scripts/test_tracker_telegram.py`, `scripts/send_report_now.py`, `scripts/experiment_eval_job.py`.

#### Trade Monitor & P&L Math

- **Trade Monitor** (`services/trade_monitor.py`): отслеживает виртуальные позиции, обрабатывает TP/SL события, выполняет частичное закрытие позиций (TP1: 50%, TP2: 30%, TP3: 20%).
- **P&L Math** (`services/pnl_math.py`): обеспечивает корректный расчет прибыли/убытков с учетом спецификаций символов (тиковая/линейная модель).
- **Stats Aggregator** (`services/stats_aggregator.py`): агрегирует статистику по стратегиям/символам/таймфреймам с атомарными обновлениями через Redis pipeline.
- **Reporting Service** (`services/reporting_service.py`): формирует HTML-отчёты с метриками winrate, P&L, TP hit rates и отправляет в Telegram.
- Конфигурация: `TP_RATIO` для долей закрытия при TP1/TP2/TP3 (формат: "0.5,0.3,0.2" или "50,30,20", по умолчанию "0.5,0.3,0.2").
- Спецификации символов: хранятся в Redis по ключу `symbol_specs:{symbol}` с полями `tick_size`, `tick_value`, `contract_size`.
- Документация: `signal_analytics/pnl_analysis.md`, `signal_analytics/signal_lifecycle.md`, `signal_analytics/reporting.md`.

#### Tick ingest & DOM

- Запуск локально: `uvicorn services.tick_ingest_server:app --host 0.0.0.0 --port 8087`.
- Симуляция: `python -m services.tick_emulator --source mt5 --symbol XAUUSD --rate 50/s`.
- Book analytics: `uvicorn services.book_analytics_service:app --port 8090` (PNG/OBI).
- DualRedis: при необходимости задайте `REDIS_TICKS_URL` / `REDIS_TICKS_HOST`.

### Локальный запуск

```bash
poetry install                 # или pip install -r requirements.txt
export PYTHONPATH=$PWD/python-worker
python -m services.tp_event_listener
python -m services.tp1_trailing_orchestrator
python -m services.signal_performance_tracker
uvicorn services.tick_ingest_server:app --reload --port 8087
```

Для запуска тестов:

```bash
cd python-worker
pytest -m "not slow"
```

Интеграционные тесты (требуют Redis/Gateway):

```bash
pytest tests/integration/test_tp1_trailing.py
```

---

## 🦾 Go Gateway и Go-сервисы

### Go Gateway (`go-gateway/`)

- Использует Go 1.22 ServeMux (+ route patterns).
- Основные пакеты:
  - `internal/handlers` — HTTP обработчики (`orders`, `events`, `health`).
  - `internal/orders` — очередь команд, валидация, dedupe.
  - `internal/events` — публикация торговых событий в Redis.
  - `internal/auth` — токены, HMAC, rate limiting.

```bash
cd go-gateway
go test ./...
go run cmd/gateway/main.go --config ../config/gateway.env
```

### API (сводно)

| Метод  | Путь           | Назначение                              |
| ------ | -------------- | --------------------------------------- |
| `POST` | `/orders/push` | Поставить команду (market/modify/trail) |
| `POST` | `/events/mt5`  | Приём событий от MT5 (`TP1_HIT`, `SL`)  |
| `POST` | `/orders/ack`  | Подтверждение исполнения                |
| `GET`  | `/orders/poll` | Запрос очереди MT5 клиентом             |
| `GET`  | `/health`      | Healthcheck                             |

Примеры payload см. `docs/tp1-trailing/TP1_TRAILING_SYSTEM.md` и `trading_workflow/order_creation.md`.

### Go Workers (`go-worker/`)

- Подписка на Binance stream, публикация в Redis.
- Локальный запуск:

```bash
cd go-worker
go run cmd/worker/main.go --config ../config/services/go-worker.env
```

---

## 🔄 Интеграционные сценарии

### 1. Полный TP1 Trailing цикл

```bash
make up-bg
make trailing-start
python -m services.tp_event_emulator --event TP1_HIT --sid signal-XAUUSD-123
# Проверяем:
make trailing-logs
make trailing-stats
make gateway-logs
```

### 2. MT5 ↔ Gateway

1. Запустить `START_TP1_TRAILING.sh` (поднимает необходимые контейнеры).
2. В MT5 загрузить `MT5_TP_EVENTS_INTEGRATION_EXAMPLE.mq5`.
3. Проверить `/events/mt5` через `make gateway-logs`.
4. Тестировать `/orders/poll` — убедиться, что команды доходят до MT5.

### 3. Signal Pipeline

```bash
make compose-up
make hub-test                   # тест Aggregated Hub V2
make signal-test                # генерация тестового сигнала
make redis-stats | grep signals
```

### 4. Reporting

```bash
make send-real-report
python analysis/export_trade_back.py --sid signal-XAUUSD-123
```

### 5. Signal Performance Tracker

```bash
make tracker-status            # убедиться, что контейнер жив
python -m services.tp_event_emulator --event SIGNAL_SAMPLE --sid test-signal --source aggregated
sleep 5
make tracker-stats             # проверить обновление stats:* по стратегии
scripts/test_tracker_telegram.py  # отправить тестовый отчёт в Telegram (dev)
```

### 6. Experiment Layer

```bash
# Создание и запуск эксперимента
python scripts/setup_sample_experiment.py create_sample
python scripts/setup_sample_experiment.py activate confidence_threshold_boost_v1

# Проверка статуса экспериментов
python scripts/setup_sample_experiment.py list
python scripts/setup_sample_experiment.py results confidence_threshold_boost_v1

# Запуск джоба расчета метрик
python scripts/experiment_eval_job.py
```

### 7. Crypto OrderFlow Handler

```bash
# Запуск обработчика криптовалютных тиков
python -m handlers.crypto_orderflow_handler

# Проверка логов и статуса
make logs | grep crypto_orderflow
redis-cli --scan --pattern 'signals:orderflow:*' | head -10

# Тестирование pipeline V2
python -m pytest handlers/crypto_orderflow/pipeline/test_pipeline_v2.py
```

---

## ✅ Тестирование

| Уровень         | Команда                              | Комментарий                                |
| --------------- | ------------------------------------ | ------------------------------------------ |
| Unit (Python)   | `pytest`                             | Фильтруйте маркеры: `pytest -m "not slow"` |
| Unit (Go)       | `go test ./...`                      | Используйте `-race` для критичных пакетов  |
| Integration     | `make trailing-test`                 | Полный сценарий TP1 → trailing → MT5       |
| Signal tracker  | `scripts/test_tracker_telegram.py`   | Проверка отчётов трекера в Telegram stream |
| End-to-end      | `Makefile.trailing integration-test` | Запускает сценарий из `Makefile.trailing`  |
| Static analysis | `make lint`                          | flake8 + gofmt + golangci-lint             |

### Coverage

- Python: `pytest --cov=services --cov=core` (отчёт в `htmlcov/`).
- Go: `go test ./... -coverprofile=coverage.out`.

---

## 🐞 Отладка и профилирование

### Redis инспекция

```bash
redis-cli -h localhost -p 6379 monitor
redis-cli --scan --pattern 'signals:*'
redis-cli xinfo stream events:trades
```

### Python

- Уровень логирования: `export LOG_LEVEL=DEBUG`.
- Для профилирования трейлинга: `python -m cProfile -o profile.out services/tp1_trailing_orchestrator.py`.
- Live reload (dev): используйте `watchfiles` или `uvicorn --reload` (для http сервисов).

### Go

- `go test -run TestName -v` для конкретных кейсов.
- Включить pprof: `--pprof=:6060` (если доступно в конфиге).
- Логи: `LOG_LEVEL=debug go run ...`.

### MT5

- Логи в терминале (`Experts` таб).
- Временный режим `DEBUG_MODE=true` в `config/trailing_config.json`.

---

## 📐 Стандарты кодирования

### Python

- PEP8 + black (120 символов max, если не указано иное).
- Типизация обязательна для новых модулей (`from __future__ import annotations`).
- Используйте `structlog` или стандартный `logging` с JSON форматером.

### Go

- Go 1.22+, `gofmt` + `golangci-lint`.
- HTTP handlers только через новый `http.ServeMux` (pattern matching).
- Контракты описываем в `internal/contracts`.
- Ошибки: использовать `errors.Join`, `errors.Is`, `errors.As`.

### Документация

- Markdown с эмодзи (при необходимости), таблицы для структур.
- Ссылки на исходники — относительные пути.
- Каждое изменение фиксировать в `README.md` и changelog.

---

## ❓ Частые вопросы

- **Как добавить новый профиль трейлинга?**  
  См. `services/trailing_profiles.py`. Добавьте в JSON (`config/trailing_config.json`) или через код:

  ```python
  from services.trailing_profiles import TrailingProfilesRegistry, TrailingProfile
  TrailingProfilesRegistry().add(TrailingProfile(
      name="tight_scalp",
      mode="ATR",
      atr_mult=0.4,
      comment="Scalp profile for high momentum"
  ), save_to_redis=True)
  ```

- **Как протестировать MT5 Event Executor без MT5?**  
  Используйте `python -m services.tp_event_emulator` или `curl`:

  ```bash
  curl -X POST http://localhost:8090/events/mt5 \
       -H "Authorization: Bearer $MT5_EVENT_TOKEN" \
       -H "Content-Type: application/json" \
       -d '{"event_type":"TP1_HIT","sid":"signal-XAUUSD-123","price":2769.9}'
  ```

- **Где смотреть статистику трейлинга?**  
  `make trailing-stats`, `prometheus` (`/metrics`), Grafana dashboard `TP1 Trailing`.

- **Как отключить трейлинг для отладки?**  
  Установите `TRAILING_ENABLED=false` и перезапустите `tp-event-listener`.

---

## ✅ Контроль качества документа

- Синхронизация с кодом: `git grep` и ручная проверка после релиза.
- Обновлённые разделы: команды Makefile, интеграционные тесты, токены API, модули `pnl_math.py`, `stats_aggregator.py`, `reporting_service.py`, PostgreSQL интеграция, Experiment Layer, Crypto OrderFlow Handler.
- Последнее обновление: 2026-01-01.
- Ответственные: `@go-team`, `@python-team`, `@trading-analytics`.

Удачной разработки! Если нужно больше примеров — обращайтесь к `trading_workflow/` и `ticks/`.
