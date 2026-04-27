# Runbook: OF-gate archiver exporter + alerts (P78)

## Цель
Дать SRE-видимость, что архивация `metrics:of_gate` и `quarantine:metrics:of_gate` реально работает:
- процесс archiver жив (обновляет статус)
- есть счётчики вставок/ошибок
- есть алерты на staleness и ошибки

## Компоненты
- **Archiver**: `services/archivers/stream_archiver.py`
  - пишет статус в Redis hashes:
    - `metrics:of_gate_metrics_archiver`
    - `metrics:of_gate_quarantine_archiver`
  - job `orderflow_services/of_gate_history_migration_v1.py` пишет `metrics:of_gate_rollups_refresh`

- **Exporter**: `orderflow_services/of_gate_archiver_exporter_v1.py`
  - читает hashes и экспонирует Prometheus metrics на порту 9152

- **Prometheus alerts**: `orderflow_services/prometheus_alerts_of_gate_archiver_p78.yml`

## ENV

### Archiver (stream_archiver)
```bash
OF_GATE_ARCHIVER_METRICS_KEY=metrics:of_gate_metrics_archiver
OF_GATE_QUARANTINE_ARCHIVER_METRICS_KEY=metrics:of_gate_quarantine_archiver
```

### Exporter
```bash
REDIS_URL=redis://redis-worker-1:6379/0
OF_GATE_ARCHIVER_EXPORTER_PORT=9152
OF_GATE_ARCHIVER_METRICS_KEY=metrics:of_gate_metrics_archiver
OF_GATE_QUARANTINE_ARCHIVER_METRICS_KEY=metrics:of_gate_quarantine_archiver
OF_GATE_ROLLUPS_REFRESH_METRICS_KEY=metrics:of_gate_rollups_refresh
```

## Метрики
- `of_gate_archiver_last_run_ts_ms{kind="metrics|quarantine|rollups_refresh"}`
- `of_gate_archiver_staleness_sec{kind=...}`
- `of_gate_archiver_last_stream_ts_ms{kind=...}`
- `of_gate_archiver_inserted_total{kind=...}`
- `of_gate_archiver_error_total{kind=...}`

## Troubleshooting

### 1) Stale алерт
- Проверьте логи контейнера archiver.
- Проверьте доступность Redis по `REDIS_URL`.
- Убедитесь что флаги включены:
  - `OF_GATE_METRICS_ARCHIVE_ENABLED=1`
  - `OF_GATE_QUARANTINE_ARCHIVE_ENABLED=1`

### 2) Errors алерт
- Посмотрите DLQ:
  - `stream:dlq:of_gate_metrics`
  - `stream:dlq:of_gate_quarantine`
- Если DLQ растёт из-за схемы — сначала исправить producer/contract, потом чистить backlog.

## Rollback
Отключить exporter/alerts безопасно.
Отключить archiver flags безопасно (влияет только на историю/аналитику, не на real-time).
