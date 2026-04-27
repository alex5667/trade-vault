# Runbook: OF-gate metrics archive + quarantine + Timescale rollups (P2/P3)

## Цель
- Архивировать `metrics:of_gate` в таблицу `of_gate_metrics` (per-event raw).
- Архивировать DQ-карантин `quarantine:metrics:of_gate` в `of_gate_metrics_quarantine` ("грязь" отдельно).
- Считать ok_rate rollups через Timescale continuous aggregates:
  - `of_gate_ok_rate_5m`
  - `of_gate_ok_rate_1h`
- Настроить retention (по умолчанию 30 дней raw и quarantine).

## 1) Миграции БД
Файл миграции:
- `services/archivers/sql/20260224_of_gate_metrics_rollups_p2.sql`

Применение (пример):
```sql
\i services/archivers/sql/20260224_of_gate_metrics_rollups_p2.sql
```

Примечания:
- Если TimescaleDB не установлен, блоки `create_hypertable / continuous aggregate / policies` безопасно no-op (EXCEPTION handlers).
- Для Timescale: должен быть включен extension `timescaledb` и права на `CREATE MATERIALIZED VIEW`.

## 2) Включение архивации (ENV)
В контейнере `services/archivers/stream_archiver.py`:

### Raw metrics
```bash
OF_GATE_METRICS_ARCHIVE_ENABLED=1
OF_GATE_METRICS_STREAM=metrics:of_gate
OF_GATE_METRICS_CG=of_gate_metrics_archiver
OF_GATE_METRICS_CONSUMER=archiver_of_gate_1
OF_GATE_METRICS_BATCH=5000
OF_GATE_METRICS_BLOCK_MS=1000
OF_GATE_METRICS_MIN_IDLE_MS=60000
OF_GATE_METRICS_DLQ_STREAM=stream:dlq:of_gate_metrics
OF_GATE_METRICS_AUTO_MIGRATE=1
OF_GATE_METRICS_ROLLUPS_AUTO_MIGRATE=0   # включайте только если хотите auto CAGG
```

### Quarantine
```bash
OF_GATE_QUARANTINE_ARCHIVE_ENABLED=1
OF_GATE_QUARANTINE_STREAM=quarantine:metrics:of_gate
OF_GATE_QUARANTINE_CG=of_gate_quarantine_archiver
OF_GATE_QUARANTINE_CONSUMER=archiver_of_gate_q_1
OF_GATE_QUARANTINE_BATCH=5000
OF_GATE_QUARANTINE_BLOCK_MS=1000
OF_GATE_QUARANTINE_MIN_IDLE_MS=60000
OF_GATE_QUARANTINE_DLQ_STREAM=stream:dlq:of_gate_quarantine
OF_GATE_QUARANTINE_AUTO_MIGRATE=1
```

Общее:
```bash
REDIS_URL=redis://redis-worker-1:6379/0
TRADES_DB_DSN=postgresql://...
```

## 3) Проверка
### Проверка наполнения таблиц
```sql
SELECT count(*) FROM of_gate_metrics WHERE ts > now() - interval '15 minutes';
SELECT dq_code, count(*) FROM of_gate_metrics_quarantine
WHERE ts > now() - interval '1 hour'
GROUP BY 1 ORDER BY 2 DESC LIMIT 20;
```

### Проверка rollups
```sql
SELECT * FROM of_gate_ok_rate_5m
WHERE bucket > now() - interval '1 hour'
ORDER BY bucket DESC, symbol LIMIT 200;
```

## 4) Миграция истории / пересчёт графиков
Скрипт: `orderflow_services/of_gate_history_migration_v1.py`

### A) Пересчитать (refresh) continuous aggregates
```bash
TRADES_DB_DSN=postgresql://... \
python -m orderflow_services.of_gate_history_migration_v1 refresh --days 30
```

### B) Backfill raw таблицы из Redis stream
Тяжёлая операция. Делайте на staging или в off-peak.
```bash
TRADES_DB_DSN=postgresql://... \
REDIS_URL=redis://redis-worker-1:6379/0 \
python -m orderflow_services.of_gate_history_migration_v1 backfill \
  --start-id 0-0 --max-messages 2000000
```

После backfill — выполнить refresh (A).

## 5) Rollback
- Выключить флаги:
  - `OF_GATE_METRICS_ARCHIVE_ENABLED=0`
  - `OF_GATE_QUARANTINE_ARCHIVE_ENABLED=0`
- Rollups/retention не влияют на runtime сигналов; при необходимости можно удалить CAGG вручную.
