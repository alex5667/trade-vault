# P5.6 Audit Chain

## Что проверяется

`check_execution_audit_chain.py` берет seed rows из `execution_orders` и проверяет, что для них сохраняется связность:

- `signals`
- `signal_execution_plan`
- `trades_closed`
- `position_events`
- `entry_policy_audit`
- `decision_snapshot`

## Артефакты

- JSON report: `latest_execution_audit_chain.json`
- Prometheus textfile: `latest_execution_audit_chain.prom`
- Runbook API: `GET /api/audit-chain/latest`

## Базовый запуск

```bash
python scripts/check_execution_audit_chain.py \
  --dsn "$TRADES_DB_DSN" \
  --report-json /var/lib/node_exporter/textfile_collector/latest_execution_audit_chain.json \
  --report-prom /var/lib/node_exporter/textfile_collector/latest_execution_audit_chain.prom
```

## Основные метрики

- `trade_execution_audit_chain_report_freshness_seconds`
- `trade_execution_audit_chain_report_stale`
- `trade_execution_audit_chain_total_broken`
- `trade_execution_audit_chain_broken_total{kind=...}`

## Типы ошибок

- `broken_signal_link` — нет строки в `signals` для данного sid+signal_id
- `broken_signal_plan` — нет строки в `signal_execution_plan` для данного sid+signal_id
- `broken_trade_link` — нет строки в `trades_closed` для данного sid+closed_trade_id
- `broken_position_event_link` — нет строки в `position_events` для данного sid+closed_trade_id
- `broken_entry_policy_link` — нет строки в `entry_policy_audit` для данного sid+signal_id
- `broken_analytics_link` — нет строки в `decision_snapshot` для данного sid+signal_id

## Диагностика

1. Открыть `/api/audit-chain/latest?limit=100`
2. Отфильтровать по `sid`, `signal_id` или `closed_trade_id`
3. Проверить, где впервые теряется linkage
4. Если broken count растет сериями, смотреть backfill/bridge pipeline между execution и analytics слоями

## Переменные окружения

| Переменная | По умолчанию | Описание |
|---|---|---|
| `TRADES_DB_DSN` | — | DSN для подключения к PostgreSQL |
| `EXEC_AUDIT_LOOKBACK_HOURS` | 24 | Окно для seed rows |
| `EXEC_AUDIT_LIMIT` | 1000 | Максимум seed rows |
| `EXEC_AUDIT_REPORT_JSON` | `latest_execution_audit_chain.json` | Путь к JSON-отчету |
| `EXEC_AUDIT_REPORT_PROM` | `latest_execution_audit_chain.prom` | Путь к .prom textfile |
| `EXEC_AUDIT_REPORT_STALE_SECONDS` | 900 | Порог для stale-флага |
| `RUNBOOK_SERVER_PORT` | 18080 | Порт HTTP сервера |
| `EXEC_AUDIT_CHAIN_INTERVAL_SEC` | 3600 | Интервал timer-контейнера |
