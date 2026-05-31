# P3.3 autonomy: replay / rehydrate

## Что делает слой
- читает `latest_execution_health.json`
- автоматически запускает scrubber checkpoint keys
- при breached retention guard переводит `sid` в quarantine
- публикует `latest_auto_scrubber.json`
- обновляет materialized summary `execution_replay_slo_summary_mv`

## Когда включать
- после стабилизации `P3.3-ops-complete`
- только при работающем SQL journal mirror

## Что смотреть
- `/api/health/latest`
- `/api/rebuild/latest`
- `/api/autonomy/latest`
- `latest_replay_slo_summary.json`

## Endpoints runbook-server
| Endpoint | Файл | Описание |
|---|---|---|
| `/api/autonomy/latest` | `latest_auto_scrubber.json` | Последнее решение auto-trigger |
| `/api/replay-slo/latest` | `latest_replay_slo_summary.json` | SLO summary (1h/24h/7d) |

## Auto-trigger логика
Скрипт `auto_trigger_checkpoint_scrubber.py` запускается таймером каждые 10 мин:
1. Читает health report
2. Если `overall_status == critical` или `warning` → trigger scrubber
3. Если `retention_guard.breached_checkpoints > 0` → trigger + quarantine

## Quarantine политика
Скрипт `apply_retention_guard_quarantine.py` для каждого SID из breach-списка:
1. Пробует rebuild из stream — если успешно, skip
2. Иначе: записывает в Redis set `orders:quarantine:state:sids`, stream event `RETENTION_GUARD_QUARANTINED`, SQL ledger

## Alerts
- `TradeExecutionAutonomyScrubberTriggered` (warning, 5m)
- `TradeExecutionRebuildRetentionGuardHigh` (critical, 10m)
- `TradeExecutionRetentionGuardQuarantineSpike` (critical, 0m)
