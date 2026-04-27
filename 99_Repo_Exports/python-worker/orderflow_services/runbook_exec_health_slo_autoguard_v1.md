# ExecHealth AutoGuard (v1)

Triggers auto-freeze and optional rollback when either condition is sustained:
- `cross_scope_mode_distinct > 1`
- `rollout_drift_instances_total >= threshold`

## Inputs
- summary hash: `metrics:exec_health:slo:last`

## Outputs
- state hash: `metrics:exec_health:slo:autoguard:state`
- freeze key: `cfg:orderflow:exec_health:auto_freeze:v1`
- optional rollback marker: `cfg:orderflow:overrides:v1:rollback:<active_sid>`

## Rollback path
If enabled and `active_sid != prev_sid`, autoguard sets:
- `cfg:orderflow:overrides:v1:active_sid = prev_sid`

## Primary metrics
- `exec_health_slo_autoguard_freeze_active`
- `exec_health_slo_autoguard_mode_mismatch_active`
- `exec_health_slo_autoguard_rollout_drift_active`
- `exec_health_slo_autoguard_rollback_total`
