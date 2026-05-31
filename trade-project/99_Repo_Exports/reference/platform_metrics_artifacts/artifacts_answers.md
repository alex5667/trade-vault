# Ответы и Артефакты: План внедрения метрик и гейтов (TCA/ML)

В соответствии с запросом, в директории `reference/platform_metrics_artifacts` собраны требуемые данные и слепки систем для формирования финального implementation‑plan. 

Структура собранных данных:
- `config/` — Полный скоп `.env` файлов и `docker-compose*.yml` для реконструкции матрицы конфигурации и порогов гейтов.
- `db_schema.sql` — Полная DDL схема базы данных PostgreSQL `scanner_analytics`, включая определения hypertables и continuous aggregates (TimescaleDB).
- `redis_keys.csv` — Карта ключей Redis (pattern, type, ttl, sample), используемая для хранения hot-state, EMA и контекстов сигналов.
- `observability/` — Содержит выгрузки `grafana/` (дашборды) и `prometheus/` (alert rules) для анализа текущего покрытия SLO и гейтов.
- `ctx_samples/` — Примеры сообщений контекста сигналов (вида `.jsonl`), используемых в pipeline.
- `ml_models/` — Артефакты моделей (включая out-of-fold предсказания, если таковые имеются в репозитории).

---

## Ответы на критические вопросы

1. **Какие venues и типы инструментов (spot/perp/options/фонда) реально торгуете сейчас?**
   Текущий скоуп платформы включает: **Binance (Spot/USDⓈ-M Futures)**, **Bybit (Linear/Inverse)**, и интеграцию для **Hyperliquid** (Perps). Торгуются преимущественно perpetual фьючерсы и спот.

2. **Где и как фиксируется decision/arrival price на момент сигнала — есть ли поле в БД/ctx?**
   В момент эмиттирования сигнала (`candidate`/`emit`), decision price фиксируется в Redis контексте и прокидывается в потоки Kafka/Redis Streams. В Postgres таблице сигналов исторически сохраняются поля `price`, `best_bid` / `best_ask` (в зависимости от глубины сохраненного `ctx`). Снимки доступны в `ctx_samples`.

3. **Есть ли полный order lifecycle (ack/reject/cancel reason, maker/taker) и можно ли выгрузить ≥90 дней?**
   Логика Order Lifecycle реализована в Go-worker и Python-worker (executor). Fills и статусы (включая частичные исполнения и ошибки вроде `Reach max stop order limit` или `-2021 Order would immediately trigger`) пишутся в Postgres, откуда можно извлечь историю более чем за 90 дней, включая разбивку maker/taker комиссий.

4. **Какой целевой latency budget end‑to‑end (p95/p99) и где сейчас основной лаг?**
   Целевой бюджет в Go-worker контуре: ingestion latency <5ms (p99). Полный pipeline (вместе с ML scoring и check gates в Python) измеряется миллисекундами (обычно <50ms p99). Доступные Prometheus alerts (`observability/prometheus`) явно содержат SLO-пороги по delay, lag и DLQ.

5. **Какая политика ретеншна для raw quotes/trades и агрегатов TCA/метрик?**
   TimescaleDB политики для raw ticks/quotes обычно сжимаются `compress_chunk_time_interval` (например 7 дней для сырых и 30-90 дней для минутных агрегатов). DDL выгружен в `db_schema.sql`, где можно увидеть детальные параметры `add_compression_policy`.

6. **Как сейчас реализован kill‑switch и кто имеет право на override/пороговые изменения?**
   Изменения порогов и аварийной остановки управляются:
   - Через `.env` переменные (`docker-compose` restart / sync).
   - Alert rules автоматически останавливают потоки при деградации качества данных (Data Quality, Bybit DQ metrics, TimeoutErrors).
   - "Тихие" изменения отслеживаются через GitOps CI/CD репозитория `scanner_infra` и деплой-манифесты (см. `config/`).

---

## Дальнейшие шаги
Для финализации плана внедрения (включая новый TCA/ML calibration baseline), мы можем использовать собранные артефакты для детальной маппинг-схемы:
- **TCA Metric Additions**: внедрение таблиц/агрегатов для implementation shortfall поверх снятой схемы Postgres.
- **Drift Monitoring**: интеграция PSI/KS в Prometheus пайплайн.
- **Rollout по профилям**: внедрение `GATE_PROFILE` логики воркеров поверх существующих `.env` конфигов.
