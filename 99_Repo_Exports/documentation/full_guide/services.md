# Справочник по Сервисам Scanner Infrastructure

Этот документ содержит расширенное описание всех ключевых сервисов, их API, конфигураций, зависимостей и точек наблюдения. Используйте его, чтобы понять, как сервис устроен внутри, какие параметры нужно настраивать и как масштабировать без риска.

---

## 1. Формат описания

Для каждого сервиса указаны:

- **Назначение** — какая бизнес-задача решается.
- **Технологии** — стек, основные библиотеки.
- **Запуск** — как включить/перезапустить.
- **Конфигурация** — критичные переменные окружения и файлы.
- **API/Интерфейсы** — внешние и внутренние точки взаимодействия.
- **Зависимости** — на что опирается сервис.
- **Метрики и логирование** — что мониторить.
- **Масштабирование** — как увеличивать пропускную способность.
- **Диагностика** — команды и локации логов.

---

## 2. Market Data Ingestion

### 2.1 `go-worker` (Ingest V2)

- **Назначение**: подписка на Binance/Bybit WS, нормализация тиков с проставлением `t0`, обеспечение паритета полей `qty`/`quantity`.
- **Технологии**: Go 1.25, `t0` tagging, Redis Streams.
- **Конфигурация**: `SYMBOL_MAP`, `VERSION=1`.
- **Метрики**: `ingest_latency_t0_ms`, `stream_write_success_total`.
- **Особенности**: поддержка `v: 1` в payload для обеспечения совместимости.

### 2.2 `market_data_janitor` [NEW]

- **Назначение**: Очистка и обрезка Redis Streams (`XTRIM`).
- **Технологии**: Go, неблокирующие операции.
- **Метрики**: `janitor_trimmed_keys_total`.

---

## 3. Signal Intelligence (G-Gates)

### 3.1 `CryptoOrderFlow` (G0-G15 Pipeline)

- **Назначение**: Основной пайплайн генерации сигналов. Каждый сигнал проходит через цепочку гейтов.
- **Гейты**:
  - G0-G5: Data Health & ATR Filters.
  - G6: Strong Gate (OrderFlow confirmation).
  - G10: ML Gate (Champion/Challenger logic).
  - G12: Isotonic Confidence.
- **Конфигурация**: `GATE_ENFORCE_LIST`, `ML_MODEL_PATH`.
- **Метрики**: `gate_veto_total{gate="G6"}`, `confidence_score_distribution`.

### 3.2 `NewsAgent` [NEW]

- **Назначение**: LLM-агент для анализа новостей и извлечения `NewsTightenRecoDTO`.
- **Технологии**: Python, LangChain, GPT-4 / Local LLM.
- **Интерфейсы**: Публикует в `trade:cache:news_reco_map`.

### 3.3 `MLGovernanceService` [NEW]

- **Назначение**: Отслеживание дрейфа признаков (PSI/KS) и калибровка моделей.
- **Метрики**: `feature_drift_psi`, `brier_score_nightly`.

---

## 4. Execution Subsystem (Journal-First)

### 4.1 `BinanceExecutor` [NEW]

- **Назначение**: Исполнение ордеров на Binance Futures. Читает `orders:exec`, проставляет `t5`.
- **Особенности**: Изоляция по символам, поддержка Fallback TP/SL (Error -4045).
- **Метрики**: `execution_latency_t5_ms`, `binance_api_errors_total`.

### 4.2 `ProjectionWorker` [NEW]

- **Назначение**: Материализация состояния из журнала в Redis/Postgres.
- **Особенности**: Journal-First integrity check.

### 4.3 `BootstrapSupervisor` [NEW]

- **Назначение**: Resilience-контроллер для старта экзекуторов.
- **Health Checks**: User-Stream + Projection Cluster + L3 connectivity.

---

## 5. Analytics & Monitoring

### 5.1 `P4.1 Latency SLO Exporter`

- **Назначение**: Сбор и экспорт задержек t0-t5 в Prometheus.
- **Метрики**: `p41_slo_violation_count`, `stage_latency_p99`.

### 5.2 `SignalPerformanceTracker` (V8)

- **Назначение**: Анализ MFE/MAE/TTD, расчет доходности.
- **Метрики**: `p_edge`, `kelly_sizing_reco`.

---

## 7. Observability & Support

### 6.1 Prometheus

- Сбор метрик с Go/Python сервисов.
- Конфиг: `prometheus.yml`.
- Экспортеры: встроенные HTTP `/metrics`.
- Метрики контроля: `up{job="..."}`, `scrape_duration_seconds`.

### 6.2 Grafana

- Дашборды: хранение в `grafana_*.json`.
- Настройка datasource автоматизирована через `Makefile`.
- Пользовательские алерты создаются через UI, экспортируются в JSON при изменениях.

### 6.3 Makefile утилиты

- `make diagnose`, `make status`, `make metrics-check`.
- `make redis-stats`, `make stream-sample`, `make stream-lag`.
- Обновление списка команд см. `documentation/README.md`.

---

## 7. Зависящие сервисы и очереди

| Сервис / очередь       | Используется где          | Комментарии                            |
| ---------------------- | ------------------------- | -------------------------------------- |
| `orders:queue`         | Go gateway, трейлинг      | LPUSH/RPOP, приоритет трейлинга.       |
| `events:trades`        | Trailing, tracker, logger | Stream, MAXLEN 10k, TTL 14 дней.       |
| `tp:commands`          | Trailing orchestrator     | Канал для внутренних команд.           |
| `notify:telegram`      | Tracker → Telegram worker | Stream, ретрай при неудачной доставке. |
| `trade:timeline:{sid}` | Logger → аналитика        | Sorted set, TTL 7 дней.                |
| `profiles:trailing:*`  | Hub, orchestrator         | Hash, содержит параметры профилей.     |

---

## 8. Процедуры обслуживания

- **Рестарт сервиса**: `make <service>-restart`.
- **Ротация логов**: осуществляется автоматически, но можно вызвать `make logs-rotate`.
- **Обновление зависимостей**: Go (`make go-mod-tidy`), Python (`make py-update`).
- **Smoke-тесты**: `make gateway-test`, `make trailing-test`, `make tracker-smoke`, `make tick-ingest-test`.
- **Локальные профили**: `make <service>-profile`.

---

## 9. Добавление нового сервиса

1. Создайте директорию и минимальную документацию (`services.md` раздел).
2. Определите конфиги (`config/services/<service>.env.example`).
3. Добавьте цели в Makefile (`make <service>-start`, `make <service>-logs`).
4. Подключите мониторинг (Prometheus exporter, Dashboards).
5. Обновите `architecture.md`, `data_flow.md`, `operations.md`.
6. Убедитесь, что тесты/линтеры обновлены.

---

## 10. Сводные таблицы

### 10.1 Критичные сервисы

| Сервис                       | Класс критичности | Основной владелец   | Резервирование               |
| ---------------------------- | ----------------- | ------------------- | ---------------------------- |
| `go-worker`                  | P1                | `@market-data-team` | Реплики + reconnect watchdog |
| `tick_ingest_server`         | P1                | `@market-data-team` | DualRedis                    |
| `aggregated_signal_hub_v2`   | P1                | `@python-team`      | Consumer groups              |
| `go-gateway`                 | P1                | `@go-team`          | Горутины, rate limiting      |
| `signal_performance_tracker` | P2                | `@python-team`      | DualRedis + retries          |
| `tp_event_listener`          | P1                | `@python-team`      | Event stream + idempotency   |

### 10.2 Частота обновлений

| Сервис                       | Релизный цикл                   | Примечание                 |
| ---------------------------- | ------------------------------- | -------------------------- |
| `go-worker`                  | По необходимости (1-2 раза/мес) | При изменении API Binance  |
| `aggregated_signal_hub_v2`   | Каждые 2-3 недели               | Новые стратегии/режимы     |
| `go-gateway`                 | Раз в квартал или при фичах     | Требует согласования с MT5 |
| `signal_performance_tracker` | Раз в месяц                     | Новые отчёты/метрики       |

---

## 11. Связанные документы

- `architecture.md` — архитектурный контекст.
- `data_flow.md` — последовательности и форматы данных.
- `operations.md` — мониторинг, инциденты.
- `troubleshooting.md` — типовые проблемы и решения.
- `roadmap.md` — планы по развитию сервисов.

Обновляйте разделы при каждом изменении конфигурации или API. Вопросы можно адресовать в `#scanner_docs` или соответствующим владельцам.
