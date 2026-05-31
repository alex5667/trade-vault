# Runbook — Degraded Mode

## Что такое degraded mode
Режим, в котором система продолжает работать, но приоритет меняется:
- safety > maker fee optimization
- deterministic close > aggressive entry rate
- less new risk > more throughput

## Когда включать
- Redis stream timeout burst
- negative age / clock drift incidents
- Binance 503 unknown spike
- user stream reconnect churn
- queue lag / book staleness / tick staleness breaches

## Флаги degraded mode
```env
EXEC_DEGRADED_MODE_FORCE_SAFETY_FIRST=1
EXEC_DEGRADED_MODE_DISABLE_MAKER=1
TRADE_DQ_HARD_VETO_ENABLE=1
TRADE_RISK_ENGINE_V2_ENABLE=1
```

## Operational effect
- new maker ladder exits are disabled
- resolver forces `SAFETY_FIRST`
- pre-publish DQ/risk veto stays active
- journal + metrics stay on for incident visibility

## Recommended actions
1. Проверить Grafana dashboard `Trade Execution P5`.
2. Проверить, не растёт ли:
   - `execution_reconcile_pending_total`
   - `trade_dq_hard_veto_total`
   - `trade_execution_journal_write_fail_total`
3. Если degraded mode не stabilizes system:
   - включить `EXEC_FORCE_SAFETY_FIRST=1`
   - при необходимости сделать P5 rollback.
