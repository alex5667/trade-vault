# P3.3 ops-complete: replay / rebuild / retention guard

## Что добавлено
- `/api/rebuild/latest` в runbook server (JSON отчёт последнего rebuild)
- quarantine ledger запись при `replay_mismatch` (executor + consistency checker)
- health report включает Redis stream retention guard (retention_guard section)
- Prometheus: `trade_execution_replay_retention_guard_breached` gauge + retention guard counter
- Prometheus: `trade_execution_replay_latency_ms` histogram + p95 alert
- периодический scrubber для replay checkpoints (удаляет orphan/stale cursor keys)

## Компоненты и файлы

| Файл | Изменение |
|------|-----------|
| `services/execution_state_replay.py` | `stream_retention_guard_report()`, `_retention_guard_triggered()`, `_stream_oldest_id()`, расширен `ReplayBuildResult` (`retention_guard_triggered`, `latency_ms`), latency histogram |
| `services/binance_executor.py` | QuarantineLedgerSink import, `_replay_checkpoint_key()`, `_quarantine_sid_for_replay_mismatch()`, upgrade `_recover_state_from_exec_stream()` |
| `scripts/rebuild_orders_state_from_exec.py` | пишет `latest_rebuild_state.json`, считает p95 latency / source_counts |
| `scripts/check_execution_replay_consistency.py` | `--ledger-dsn`, `retention_guard_triggered` / `replay_latency_ms` в report |
| `scripts/execution_healthcheck.py` | `retention_guard` в report, retention guard breach → critical |
| `scripts/scrub_replay_checkpoints.py` | **новый** — periodic scrubber orphan/stale cursor keys |
| `runbooks/server/runbook_server.py` | `/api/rebuild/latest` endpoint |
| `monitoring/prometheus_rules_execution_p33_ops_complete.yml` | alerts: `TradeExecutionReplayRetentionGuard`, `TradeExecutionReplayP95High` |
| `deploy/systemd/trade-execution-checkpoint-scrubber.service/.timer` | systemd для scrubber |
| `config/docker-compose.execution-p33-ops-complete.override.yml` | volumes для runbook-server |

## Проверки
1. `curl http://trade-runbook-server:18080/api/rebuild/latest`
2. `systemctl status trade-execution-checkpoint-scrubber.timer`
3. `python3 scripts/check_execution_replay_consistency.py --quarantine-on-critical`
4. `python3 scripts/scrub_replay_checkpoints.py --dry-run`
5. Prometheus: проверь наличие метрик `trade_execution_replay_latency_ms_bucket` и `trade_execution_replay_retention_guard_total`

## Timer — интеграция в Docker (make up)
Scrubber запускается как задача внутри python-worker контейнера через `docker-compose-timers.yml`.
Для systemd-окружений установи `.service` + `.timer` из `deploy/systemd/`.
