# Canary rollout checklist

## Before enablement
- Confirm `EXEC_ALGO_CANONICAL_V2=1` only in canary env.
- Confirm `EXEC_FORCE_SAFETY_FIRST=1` for first canary slice.
- Confirm `EXEC_MAKER_TP_ENABLE=0` unless the symbol list is explicitly approved.
- Confirm `EXEC_USER_STREAM_ENABLE=1` and user stream freshness is green.
- Confirm `EXECUTION_JOURNAL_DSN` is set and SQL migrations are applied.
- Confirm `EXEC_HEALTHCHECK_TEXTFILE_PATH` points to the node-exporter textfile collector.
- Confirm `trade-execution-healthcheck.timer` and `trade-runbook-server.service` are active.

## During canary
- Watch Grafana dashboard `trade_execution_p7_panels.json`.
- Watch `trade_execution_health_status_code` and ensure it stays `0`.
- Watch `trade_execution_consistency_critical_mismatches` and keep it at `0`.
- Watch `trade_dq_level`, `trade_risk_level`, and `execution_emergency_flatten_total`.
- Sample at least 10 real `sid` and verify all three mirrors:
  - `orders:state:*`
  - `orders:exec`
  - `execution_orders`

## Promote or stop
Promote only if all of the following stay true for the agreed observation window:
- no emergency flatten events,
- no critical consistency mismatches,
- no stale user stream alerts,
- no hard DQ veto burst,
- realized slippage remains within policy.

Stop and rollback immediately if any of the following happen:
- `execution_emergency_flatten_total > 0`,
- `trade_execution_consistency_critical_mismatches > 0`,
- `trade_execution_user_stream_stale = 1`,
- repeated quarantine actions for new `sid`.
