# ExecHealth Freeze Client Name Audit (v1)

## Purpose

Detect Redis-side client identity regressions that are visible only from `CLIENT LIST`:

- trusted service started without `CLIENT SETNAME`
- wrong `lib-name` after reconnect
- duplicate trusted client names across hosts/deploys

## Signals

- `exec_health_freeze_client_name_violation{kind=...}`
- `exec_health_freeze_client_name_match{service,field}`
- `exec_health_freeze_client_name_active_connections{service}`
- `exec_health_freeze_client_name_distinct_addrs{service}`

## Self-healing

Trusted writer/audit/bootstrap services now attempt reconnect self-healing on live connections:

- `CLIENT SETNAME` re-assert
- `CLIENT SETINFO LIB-NAME` re-assert
- recovery event: `kind=redis_client_identity_recovered` in `ops:exec_health:freeze_events:v1`

Additional metrics:

- `exec_health_freeze_client_name_recovery_total{service}`
- `exec_health_freeze_client_name_last_recovery_ts_ms{service}`
- `exec_health_freeze_client_name_repair_failed_total{service}`

If `repair_failed_total` increases, treat it as a hard regression in reconnect or Redis client auth/identity setup.
