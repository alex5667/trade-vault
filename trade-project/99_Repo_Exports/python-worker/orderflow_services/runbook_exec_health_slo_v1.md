# ExecHealth Rollout SLO (v1)

## What it checks
- rollout drift inside each scope (`edge`, `pipeline`, `entry_policy`)
- cross-scope mode mismatch
- cross-scope threshold mismatch
- apply / veto / pass share by scope
- deploy fan-out inside a scope

## Data flow
1. Each scope updates a local in-process ExecHealth snapshot.
2. Snapshot is flushed to Redis hash `metrics:exec_health:scope_state:<scope>:<instance>`.
3. `exec_health_slo_checker_v1.py` scans these hashes and writes compact summary to `metrics:exec_health:slo:last`.
4. `exec_health_slo_exporter_v1.py` exposes Prometheus metrics.

## Primary queries
- `exec_health_slo_rollout_drift_instances_total`
- `exec_health_slo_cross_scope_mode_distinct`
- `exec_health_slo_cross_scope_threshold_distinct`
- `exec_health_slo_share{outcome="veto"}`

## First actions on alert
1. Check whether a new deploy is partially rolled out.
2. Compare `mode_distinct_*` and `threshold_distinct_*` for the affected scope.
3. Confirm `GATE_PROFILE`, `EXEC_HEALTH_MODE`, scope overrides and threshold env vars.
4. If veto share jumped without intended hardening, roll back the new config or deployment.
