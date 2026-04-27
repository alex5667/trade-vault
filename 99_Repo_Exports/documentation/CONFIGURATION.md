# ⚙️ Конфигурация Scanner Infrastructure (2026-01-27)

> Сводник для DevOps/SRE, разработчиков и аналитиков. Обновлено после релиза TP1 Trailing & MT5 Event Executor.  
> Авторство: команда Senior Go/Python Developer + Senior Trading Systems Analyst.

---

## 📚 Оглавление

1. [Docker Compose профили](#docker-compose-профили)
2. [ENV-файлы и секреты](#env-файлы-и-секреты)
3. [Сервисы и ключевые переменные](#сервисы-и-ключевые-переменные)
4. [Redis инфраструктура](#redis-инфраструктура)
5. [PostgreSQL инфраструктура](#postgresql-инфраструктура)
6. [Мониторинг и алертинг](#мониторинг-и-алертинг)
7. [CI/CD и деплой](#cicd-и-деплой)
8. [Безопасность и доступ](#безопасность-и-доступ)
9. [Troubleshooting](#troubleshooting)

---

## 🐳 Docker Compose профили

| Файл                                  | Назначение                                                     | Основные сервисы                                       |
| ------------------------------------- | -------------------------------------------------------------- | ------------------------------------------------------ |
| `docker-compose.yml`                  | Базовый стек (Redis, Go gateway, python-worker, monitoring)    | `scanner-redis`, `scanner-go-gateway`, `python-worker` |
| `docker-compose.tp-trailing.yml`      | Дополнение для TP Event Listener и трейлинговых сервисов       | `tp-event-listener`, `tp1-trailing-orchestrator`       |
| `docker-compose.mt5-executor.yml`     | MT5 Event Executor (приём событий от терминала)                | `mt5-event-executor`                                   |
| `docker-compose-optimized.yml`        | Производительная конфигурация (разделение Redis, workers)      | `scanner-redis-worker-*`, `scanner-redis-trade`        |
| `docker-compose-optimized-simple.yml` | Упрощённый high-load профиль                                   | Урезанный набор Redis + ключевые сервисы               |
| `docker-compose-ultra.yml`            | Максимальная производительность (несколько ingestion-воркеров) | По 3+ реплики go/python workers                        |
| `docker-compose-with-init.yml`        | Профиль с первичной инициализацией данных                      | `init-*` контейнеры, seeds                             |
| `docker-compose.hub-v2.yml`           | Фокус на aggregated hub v2 и связанных сервисах                | `aggregated-hub-v2`, `regime-worker`, `risk`           |
| `docker-compose-postgres.yml`          | PostgreSQL для экспериментов и логирования                     | `scanner-postgres`, миграции                           |
| `docker-compose-experiments.yml`       | Экспериментальный слой A/B-тестирования                        | `experiment-manager`, `experiment-metrics`             |
| `docker-compose.tp-trailing.yml`      | (см. выше)                                                     |                                                        |
| `docker-compose.mt5-executor.yml`     | (см. выше)                                                     |                                                        |

Локальный запуск:

```bash
export COMPOSE_FILE=docker-compose.yml
make up-bg

# Для трейлинга
make trailing-start

# Для MT5 executor
COMPOSE_FILE="docker-compose.yml:docker-compose.mt5-executor.yml" make up-bg
```

---

## 🧾 ENV-файлы и секреты

### Структура

```
./config/
  ├── env.example
  ├── gateway.env
  ├── redis/
  │   ├── redis-stable.conf
  │   ├── redis-ultra-high-load.conf
  │   └── ...
  └── services/
      ├── python-worker.env
      ├── go-worker.env
      └── telegram.env
```

- `config/env.example` — шаблон глобальных переменных (скопируйте в `.env.local`).
- `config/gateway.env` — токены, URL, лимиты для Go Gateway.
- `config/services/*.env` — профильные настройки для сервисов.
- Секреты (пароли, токены) хранятся в `.env.local` (не коммитим). Для production — Vault/Secret Manager.

### Ключевые переменные

| Переменная               | Описание                                                                             |
| ------------------------ | ------------------------------------------------------------------------------------ |
| `REDIS_URL`              | Основное подключение Redis (`redis://scanner-redis:6379/0`)                          |
| `REDIS_TRADES_URL`       | Специальный Redis для трейлинга (при high-load профиле)                              |
| `REDIS_TICKS_URL`        | Выделенный Redis для тиков (`redis://scanner-redis-ticks:6379/0`)                    |
| `REDIS_TICKS_HOST`       | Алиас Redis тиков (используется ingest/aggregated/трекером)                          |
| `REDIS_HOST`             | Хост Redis (по умолчанию `scanner-redis-worker-1`)                                   |
| `REDIS_PORT`             | Порт Redis (по умолчанию `6379`)                                                     |

**Примечание:** Все сервисы используют singleton pattern с connection pool для Redis соединений. Это означает:

- Одно соединение создаётся при первом вызове `get_redis()` (Python) или `timeutil.GetCurrentTimestampMs()` (Go)
- Последующие вызовы переиспользуют существующее соединение через pool
- Автоматическое переподключение при разрыве соединения
- Настройки pool: `max_connections=100`, `socket_keepalive=True`, `health_check_interval=30`
| `GATEWAY_BASE_URL`       | URL Go Gateway (`http://scanner-go-gateway:8090`)                                    |
| `TRACKER_CONFIG_PATH`    | Путь к конфигурации Signal Performance Tracker (`config/signal_tracker_config.json`) |
| `TP_RATIO`               | Доли закрытия позиций при достижении TP1, TP2, TP3 (формат: "0.5,0.3,0.2" или "50,30,20", по умолчанию "0.5,0.3,0.2") |
| `REPORT_TRIGGER_COUNT`           | Количество сделок для автоматической отправки отчета (по умолчанию 100)            |
| `PERIODIC_REPORT_WINDOW_SECONDS` | Окно времени для сбора статистики (секунды, по умолчанию 3600)                     |
| `PERIODIC_REPORT_RECENT_LIMIT`   | Максимальное количество записей для анализа (по умолчанию 500)                      |
| `EXPERIMENT_LAYER_ENABLED`       | Включение экспериментального слоя A/B-тестирования (true/false)                     |
| `EXPERIMENT_DB_DSN`              | DSN для PostgreSQL базы экспериментов                                                |
| `EXPERIMENT_BASELINE_HORIZON_DAYS` | Горизонт для расчета baseline метрик (по умолчанию 180)                           |
| `SYMBOL_SPECS_REDIS_KEY`         | Шаблон ключа для спецификаций символов в Redis (по умолчанию `symbol_specs:{symbol}`) |
| `TRAILING_ENABLED`               | Флаг активации TP1 трейлинга (`true/false`)                                          |
| `REGIME_GUARD_ENABLED`           | Включение контроля качества сигналов через regime-guard (true/false)                |
| `TRAILING_PROFILES_PATH` | Путь к JSON с профилями (`config/trailing_config.json`)                              |
| `PROMETHEUS_PUSHGATEWAY` | URL pushgateway (если используем)                                                    |
| `TELEGRAM_TOKEN`         | Токен бота для уведомлений                                                           |
| `TELEGRAM_CHAT_ID`       | Целевой чат                                                                          |
| `NOTIFY_STREAM`          | Redis stream для уведомлений (по умолчанию `notify:telegram`)                        |
| `MT5_EVENT_TOKEN`        | Токен для авторизации `/events/mt5`                                                  |
| `API_AUTH_TOKEN`         | Токен для `/orders/push`                                                             |

---

## 🧩 Сервисы и ключевые переменные

### Go Gateway (`go-gateway/`)

- **ENV**: `config/gateway.env`.
- **Основные переменные**:
  - `GATEWAY_PORT` (по умолчанию 8090)
  - `ORDERS_QUEUE_KEY` (`orders:queue`)
  - `MT5_EVENTS_STREAM` (`events:trades`)
  - `API_AUTH_TOKEN`, `MT5_EVENT_TOKEN` (обязательны для prod)
  - `RATE_LIMIT_RPS` (по умолчанию 100)
- **Логи**: JSON, уровень `INFO`, поддержка `TRACE` через `LOG_LEVEL`.

### Python Worker (`python-worker/`)

- **ENV**: `config/services/python-worker.env`.
- **Ключи**:
  - `REDIS_URL`, `REDIS_TRADES_URL`, `REDIS_TICKS_URL`.
  - `TRAILING_ENABLED`, `TRAILING_PROFILES_PATH`.
  - `SIGNALS_STREAM_KEY`, `ORDERS_QUEUE_KEY`.
  - `PROMETHEUS_PORT` (метрики трейлинга).
  - `TRACKER_CONFIG_PATH`, `REPORT_TRIGGER_COUNT`, `PERIODIC_REPORT_HOURS`, `NOTIFY_STREAM`.
- **Сервисы** (178 сервисов всего):
  - **Data Acquisition**: `services/tick_ingest_server.py`, `services/book_analytics_service.py`, `services/crypto_futures_orderflow_handler.py`, `services/ohlc_aggregator.py`
  - **Signal Processing**: `services/signal_preprocess.py`, `services/signal_publisher.py`, `services/async_signal_publisher.py`, `services/sync_signal_publisher.py`, `services/signal_confidence.py`, `services/signal_quality.py`
  - **Experiment Layer**: `services/ab_winner_*`, `services/ab_router.py`, `services/entry_policy_ab_gate.py`
  - **Risk Management**: `services/risk_position_sizer.py`, `services/validate_signals.py`, `services/execution_gate_service.py`, `services/burstiness_tracker.py`, `services/cancellation_spike_gate.py`
  - **Order Management**: `services/orders_router.py`, `services/orders_http_bridge.py`, `services/mt5_event_executor.py`, `services/signal_dispatcher*.py`, `services/signal_target_deliverer.py`
  - **Trading Operations**: `services/tp_event_listener.py`, `services/tp1_trailing_orchestrator.py`, `services/order_trailing_dispatcher.py`, `services/trailing_profiles.py`
  - **Analytics Core**: `services/signal_performance_tracker.py`, `services/trade_monitor.py`, `services/pnl_math.py`, `services/stats_aggregator.py`
  - **Reporting**: `services/reporting_service.py`, `services/periodic_reporter.py`, `services/trade_events_logger.py`, `services/trade_metrics_service.py`, `services/analytics_api_service.py`, `services/analytics_db.py`
  - **Specialized Analytics**: `services/sl_quantile_aggregator.py`, `services/trailing_metrics.py`, `services/execution_cost_ema.py`, `services/slippage_model*.py`, `services/ev_*_stats.py`
  - **Communication**: `services/telegram_*`, `services/notify_*`
  - **Infrastructure**: `services/health_monitor.py`, `services/error_monitor.py`, `services/redis_stream_runner_base.py`, `services/persistence_manager.py`

#### ATR Timeframe Calibration & Unified Gate

- **ATR Calibration** (`core.atr_tf_calibrator`):
  - `ATR_TF_CALIB_ENABLE`: Включить динамический выбор ATR TF (1/0, default: 1).
  - `ATR_TF_CANDIDATES`: Список кандидатов (default: "1m,5m,15m").
  - `ATR_TF_MIN_ATR_BPS`: Мин. порог волатильности (default: 5.0).
  - `ATR_TF_MAX_ATR_BPS`: Макс. порог волатильности (default: 50.0).
  - `ATR_TF_MAX_JUMP_MULT`: Макс. скачок ATR между обновлениями (default: 3.0).

- **Unified ATR Gate**:
  - `ATR_GATE_MODE`: Режим работы гейта ("OFF", "SHADOW", "ENFORCE").
  - `DEBUG_VETO`: Включить подробное логирование причин отсева сигналов (1/0).
  - `CRYPTO_NOTIFY_SIGNAL_EVERY_N`: Сэмплирование уведомлений в Telegram (default: 1 — без сэмплирования).

#### Signal Performance Tracker & Trade Monitor

- Конфигурация по умолчанию: `config/signal_tracker_config.json` (symbols, strategies, consumer groups, reporting).
- Docker переменные: `TRACKER_CONFIG_PATH`, `REDIS_TICKS_URL` (резерв), `PERIODIC_REPORT_HOURS`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, `TP_RATIO`.
- Makefile: `make tracker-status`, `make tracker-logs`, `make tracker-stats`, `make tracker-restart`.
- Логи: `scanner-signal-tracker` (stdout), включает статистику (`signals_processed`, `ticks_processed`, `open_now`).
- Healthcheck: `pgrep -f signal_performance_tracker.py`.

#### Trade Monitor & P&L Math

- **Trade Monitor** (`services/trade_monitor.py`): отслеживает виртуальные позиции, обрабатывает TP/SL события, выполняет частичное закрытие позиций.
- **P&L Math** (`services/pnl_math.py`): обеспечивает корректный расчет прибыли/убытков с учетом спецификаций символов.
- **Конфигурация**:
  - `TP_RATIO` — доли закрытия при достижении TP1/TP2/TP3 (по умолчанию: `0.5,0.3,0.2`)
  - Спецификации символов хранятся в Redis по ключу `symbol_specs:{symbol}` (например, `symbol_specs:XAUUSD`)
  - Поддерживаются fallback значения для основных символов (XAUUSD, BTCUSDT, ETHUSDT)
- **Структура спецификации символа в Redis**:

  ```json
  {
    "tick_size": 0.01,
    "tick_value": 1.0,
    "contract_size": 100.0,
    "point": 0.01,
    "tick_value_per_lot": 1.0
  }
  ```

- **Документация**: `signal_analytics/pnl_analysis.md`.

#### Tick Ingest Server v2

- Запуск: `uvicorn services.tick_ingest_server:app --host 0.0.0.0 --port 8087` (см. docker-compose секцию `tick-ingest-server`).
- Переменные: `REDIS_URL`, `XAU_TICK_STREAM`, `XAU_BOOK_STREAM`, `ALLOW_SYMBOLS`, `XAU_TICK_STREAM_MAXLEN`.
- Поддержка DualRedis (`core.dual_redis_client`) — при недоступности основного Redis fallback.
- **Connection pooling**: Все Redis клиенты используют singleton pattern с connection pool для переиспользования соединений.
- Healthcheck: `pgrep -f tick_ingest_server.py`, Makefile `make tick-status`, `make tick-streams`.

### Go Workers (`go-worker/`)

- **ENV**: `config/services/go-worker.env`.
- Подключения к Binance: `BINANCE_API_KEY`, `BINANCE_API_SECRET` (для подписок не обязательно).
- `WS_RECONNECT_INTERVAL`, `SYMBOLS`, `TIMEFRAMES`.
- Пишут в Redis с префиксом `stream:tick_`, `candles:`.

### Monitoring

- **Prometheus**: `prometheus.yml`.
  - Скрап `scanner-go-gateway:9100`, `python-worker:9200`, `tp-event-listener:9210`.
- **Grafana**: `.env` содержит креды (`GRAFANA_USER`, `GRAFANA_PASS`).
- **Alertmanager** (опционально) в `docker-compose-optimized.yml`.

---

## 🗃️ Redis инфраструктура

### Конфиги

| Файл                                  | Назначение                                 |
| ------------------------------------- | ------------------------------------------ |
| `redis-stable.conf`                   | Основной стабильный профиль                |
| `redis-ultra-high-load.conf`          | Профиль для high-frequency ingestion       |
| `redis-stable-high-load.conf`         | Сбалансированный вариант                   |
| `redis-worker-stable.conf`            | Рабочие очереди                            |
| `redis-trade-back.conf`               | Хранение trade_back аналитики              |
| `redis-optimized-for-trade-back.conf` | Оптимизированная конфигурация TTL/eviction |
| `redis-ticks.conf`                    | Выделенный инстанс для тиков и DOM         |

**Рекомендации:**

- Для production используем отдельные инстансы:
  - `scanner-redis-core` (сигналы, ордера)
  - `scanner-redis-trades` (events, timeline)
  - `scanner-redis-metrics` (метрики, профили)
  - `scanner-redis-ticks` (тики, DOM, consumer groups `ticks-*`)
- В high-load профиле настроены eviction policies (`volatile-lru`) для timeline.
- Persistence: `appendonly yes`, регулярный `bgrewriteaof`.

### Ключевые настройки

- `maxmemory` задаётся через ENV `REDIS_MAXMEMORY`.
- `notify-keyspace-events` включает события для мониторинга.
- `client-output-buffer-limit` настроен для Pub/Sub каналов.

---

## 🐘 PostgreSQL инфраструктура

### Конфигурация

- **Docker сервис**: `scanner-postgres` (из `docker-compose-postgres.yml`)
- **Версия**: PostgreSQL 15
- **Порт**: 5432 (внутренний), 5433 (внешний для dev)
- **База данных**: `scanner_db`
- **Пользователь**: `scanner_user` (пароль в `.env.local`)

### ENV переменные

| Переменная           | Описание                                      | Значение по умолчанию |
|---------------------|-----------------------------------------------|----------------------|
| `POSTGRES_HOST`     | Хост PostgreSQL                              | `scanner-postgres`   |
| `POSTGRES_PORT`     | Порт PostgreSQL                               | `5432`              |
| `POSTGRES_DB`       | Имя базы данных                               | `scanner_db`        |
| `POSTGRES_USER`     | Пользователь                                  | `scanner_user`      |
| `POSTGRES_PASSWORD` | Пароль (из .env.local)                        | -                   |

### Миграции

- **Папка**: `python-worker/migrations/`
- **Применение**: `python apply_migration.py`
- **Текущие таблицы**:
  - `signals` — логирование сигналов с experiment_id/variant
  - `signal_experiment` — определения экспериментов
  - `signal_experiment_snapshot` — метрики экспериментов
  - `experiment_metrics` — результаты A/B-тестирования (expectancy, Sharpe ratio, precision/recall)
- **Связанные сервисы**: `experiment_manager.py`, `experiment_metrics.py`

### Резервное копирование

```bash
# Создание бэкапа
docker exec scanner-postgres pg_dump -U scanner_user scanner_db > backup.sql

# Восстановление
docker exec -i scanner-postgres psql -U scanner_user scanner_db < backup.sql
```

---

## 📊 Мониторинг и алертинг

### Makefile цели

```bash
make full-status          # все контейнеры
make redis-stats          # info по Redis
make trailing-status      # TP Event Listener состояние
make trailing-stats       # статистика трейлинга
make gateway-status       # состояние Go Gateway
make tracker-status       # Signal Performance Tracker up?
make tracker-logs         # Логи трекера
make tracker-stats        # Redis статистика по стратегиям/источникам
make tick-status          # Tick ingest / book analytics состояние
make tick-streams         # Лаги consumer групп stream:tick_*
make diagnose             # комплексная диагностика (Redis, Docker, логи)
```

### Prometheus метрики

- `trailing_started_total`, `trailing_latency_ms`, `trailing_failures_total`.
- `gateway_requests_total`, `gateway_request_duration_seconds`.
- `tick_ingest_lag_seconds`, `ws_reconnects_total`.
- `signals_generated_total`, `orders_enqueued_total`.
- (через Redis hash) `stats:{strategy}:{symbol}:{tf}` — показатели Signal Performance Tracker (`make tracker-stats`).

### Grafana dashboards

- `grafana_multi_symbol_dashboard.json` — latency, volume, trailing.
- `grafana_dashboard_websocket.json` — WebSocket ingestion.
- Дополнительно: создаём панели для `events:trades` (monitor trailing).

### Логирование

- Go сервисы → stdout JSON (парсится Loki/ELK).
- Python сервисы → структурированные логи, поддержка `LOG_FORMAT=json`.
- MT5 → локальные логи + публикация важных событий в Redis stream.
- Signal Performance Tracker → `scanner-signal-tracker` (stdout, агрегированная статистика каждые 60 секунд).

---

## 🚀 CI/CD и деплой

1. **Проверки**: `make lint`, `make test`, `make integration-test` (см. `DEVELOPMENT.md`).
2. **Сборка Docker образов**: `make docker-build`, `make docker-push`.
3. **Rolling deploy**:
   - `make compose-up` (или `docker compose up -d --build`).
   - Для трейлинга: `make trailing-restart` (оставшиеся сервисы — без даунтайма).
4. **Пост-деплой проверки**:
   - `make trailing-health`
   - `make gateway-health`
   - Просмотреть метрики (`trailing_latency_ms`, `gateway_requests_total`).
5. **Релизные заметки**: фиксируем в `FINAL_COMPLETE_INTEGRATION_*.md`.

---

## 🔐 Безопасность и доступ

- **Авторизация API**

  - `/orders/push` и `/events/mt5` требуют токены (`API_AUTH_TOKEN`, `MT5_EVENT_TOKEN`).
  - Токены передаются в заголовке `Authorization: Bearer <token>`.

- **MT5 Endpoint**

  - Для PROD рекомендуем reverse-proxy (nginx) с IP whitelist.
  - Возможность включить HMAC-подписи (см. `go-gateway/internal/auth/hmac.go`).

- **Secrets Management**

  - Локально — `.env.local` (в `.gitignore`).
  - PROD — Vault/KMS (см. `deploy/` инструкции).
  - Rotate токены раз в 30 дней, используя `make gateway-rotate-token`.

- **Сети**

  - Docker сети: `scanner_internal`, `scanner_public`.
  - Gateway и Redis доступны только во внутренней сети.
  - Для внешних интеграций используем `infra/nginx` или VPN.

- **Backups**
  - Redis snapshot: `make backup-redis`.
  - Restore: `make restore-redis`.
  - Конфиги — `config_backups/`.

---

## 🛠️ Troubleshooting

| Симптом                      | Проверка / Решение                                                             |
| ---------------------------- | ------------------------------------------------------------------------------ |
| Нет событий трейлинга        | `make trailing-status`, `docker logs scanner-tp-event-listener`                |
| MT5 не принимает команды     | Проверить `/orders/poll` (gateway логи), убедиться в верном токене             |
| Высокая задержка трейлинга   | Метрика `trailing_latency_ms`, проверить нагрузку Redis                        |
| WS дисконнекты               | `docker logs scanner-go-worker`, коэффициент reconnect (`ws_reconnects_total`) |
| Redis близок к лимиту памяти | `make redis-stats`, проверить eviction, расширить `maxmemory`                  |
| Нужна очистка сигналов/сделок | `./scripts/clear_trades_and_signals.sh --yes` (очистка только торговых данных) |
| Нет метрик в Grafana         | Проверить Prometheus таргеты (`/targets`), убедиться в scrape портов           |
| Ошибка `Unauthorized` на API | Проверить `API_AUTH_TOKEN`, заголовок `Authorization`                          |
| **Сигналы отсеиваются (VETO)** | Включить `DEBUG_VETO=1` в `docker-compose-crypto-orderflow.yml` и проверить логи. |
| **Мало сигналов в Telegram**   | Проверить `CRYPTO_NOTIFY_SIGNAL_EVERY_N`. Если > 1, то работает сэмплирование. |

---

## ✅ Контроль версий

- Текущий документ обновлён 2025-11-28 (добавлена информация о Trade Monitor и P&L Math модуле).
- Все изменения в конфигурации сопровождаются PR с ссылкой на этот документ.
- Для дрейфа конфигов используем `safe_rebuild.sh` и `RUN_AFTER_FIX.sh` (см. `deploy/`).

Вопросы и предложения — создавайте issue в `docs-config` или обращайтесь в канал `#scanner_ops`.
