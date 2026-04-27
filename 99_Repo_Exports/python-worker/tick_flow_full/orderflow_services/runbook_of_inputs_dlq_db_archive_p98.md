# P98 — Runbook: OFInputs DLQ/quarantine → DB archive

## Цель
Сохранить историю событий DLQ и quarantine для OFInputs в Postgres/Timescale:
- постмортемы по деградации book_state (V3→V2 downgrade)
- топ причин/символов/регимов
- возможность строить тренды и корреляции с latency/exec

## Компоненты
- Archiver: `orderflow_services/of_inputs_dlq_archive_to_db_p98.py`
- Exporter: `orderflow_services/of_inputs_archiver_exporter_p98.py`
- Table/DDL: `services/archivers/sql/20260225_of_inputs_dlq_events_p98.sql`

## Потоки
- DLQ: `stream:dlq:of_inputs`
- Quarantine: `quarantine:signals:of:inputs`

## Схема таблицы
`of_inputs_dlq_events(stream, dlq_id, ts_ms, ts, src_stream, src_stream_id, err, dq_code, attempt_version, published_version, missing_fields, payload_json, inserted_at)`

## Запуск вручную
```bash
export REDIS_URL=redis://redis-worker-1:6379/0
export TRADES_DB_DSN='postgresql://...'
python -m orderflow_services.of_inputs_dlq_archive_to_db_p98 --once --auto-migrate
```

Backfill (без чекпоинта):
```bash
python -m orderflow_services.of_inputs_dlq_archive_to_db_p98 --once --tail 200000 --no-checkpoint
```

## Автоматизация (варианты)
### Вариант A: systemd timer (рекомендовано)
- Период: каждые 5–10 минут или раз в час (если объёмы маленькие).
- Команда: `python -m orderflow_services.of_inputs_dlq_archive_to_db_p98 --once`

### Вариант B: docker-compose timers
Добавьте сервис/command в ваш timers stack и включите ENV.

## Метрики/экспорт
Archiver пишет статусы в Redis hashes:
- `metrics:of_inputs_dlq_db_archive`
- `metrics:of_inputs_quarantine_db_archive`

Exporter:
```bash
export OF_INPUTS_ARCHIVER_EXPORTER_PORT=9156
python -m orderflow_services.of_inputs_archiver_exporter_p98
```

## Триггеры/алерты
См. `orderflow_services/prometheus_alerts_of_inputs_archiver_p98.yml`.

## Диагностика
1) DLQ растёт, а inserted_total не растёт
- проверить логи archiver
- проверить DSN/доступность Postgres

2) staleness_sec растёт
- job не запускается или падает до записи метрик

3) Конфликт схемы/таблицы
- прогнать `psql $TRADES_DB_DSN -f services/archivers/sql/20260225_of_inputs_dlq_events_p98.sql`

## Rollback
- остановить archiver + exporter
- таблица остаётся как история (append-only)
