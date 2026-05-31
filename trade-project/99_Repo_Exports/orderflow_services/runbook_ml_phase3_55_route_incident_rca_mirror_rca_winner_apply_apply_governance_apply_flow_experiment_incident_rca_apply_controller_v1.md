# Phase 3.55 — Apply-Flow Experiment Incident RCA Usefulness Winner/Apply Controller

## Цель
Перевести usefulness decisions нового incident RCA contour в controlled bridge-mode apply plan,
сначала только в `DRY_RUN`, и сохранить rollback-ready state для следующего safety loop.

## Что делает
- читает usefulness decisions из:
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_usefulness_decisions`
- читает текущий bridge mode из:
  - `cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_bridge:global`
- строит apply plan:
  - `APPLY_VERTEX_ONLY`
  - `APPLY_LOCAL_ONLY`
  - `APPLY_AUTO`
  - `KEEP_CURRENT_MODE`
  - `HOLD`

## Safe behavior
- default:
  - `ADVISORY_ONLY=1`
  - `EXECUTOR_MODE=DRY_RUN`
  - `ALLOW_COMMIT=0`
- even in future commit, controller changes only:
  - `cfg:...incident_rca_bridge:global.mode`
- rollback-ready previous mode is persisted separately

## Rollback-ready state
- `state:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_controller:rollback_ready`
- stores:
  - `previous_mode`
  - `target_mode`
  - `applied_reason_code`
  - `applied_ts_ms`

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_CONTROLLER_ADVISORY_ONLY=1
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_CONTROLLER_EXECUTOR_MODE=DRY_RUN
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_CONTROLLER_ALLOW_COMMIT=0
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_CONTROLLER_COOLDOWN_SEC=21600
```

## Smoke checks
```bash
curl -s localhost:9990/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_controller_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_controller:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_controller_decisions + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_controller_journal + - COUNT 5
redis-cli HGETALL cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_bridge:global
redis-cli HGETALL state:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_controller:rollback_ready
```

## Notes
- scope only `scanner_infra`
- hot path untouched
- next step: post-apply verification + rollback loop for this incident RCA bridge-mode controller
