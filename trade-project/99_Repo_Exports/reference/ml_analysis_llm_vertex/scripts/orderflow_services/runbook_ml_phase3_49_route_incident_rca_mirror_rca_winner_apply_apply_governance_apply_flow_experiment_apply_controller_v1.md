# Phase 3.49 — Route Incident RCA Mirror RCA Winner-Apply Apply Governance Apply-Flow Experiment Winner-Aware Apply Controller

## Цель
Сделать winner-aware apply controller уже для нового `apply-flow experiment contour`,
сначала только в `DRY_RUN`, чтобы boundedly переводить winner recommendation
в controlled experiment profile / weight update plan.

## Что делает
- читает winner recommendations из:
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_winner_decisions`
- читает текущую experiment policy из:
  - `cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment:global`
- читает текущий incumbent arm из:
  - `cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_winner:global`
- строит bounded apply plan:
  - `vertex_primary_profile`
  - `vertex_compact_profile`
  - `local_profile`

## Decisions
- `HOLD`
- `KEEP_CURRENT_WEIGHTS`
- `APPLY_VERTEX_COMPACT_PROFILE`
- `APPLY_LOCAL_PROFILE`

## Safe behavior
- default:
  - `ADVISORY_ONLY=1`
  - `EXECUTOR_MODE=DRY_RUN`
  - `ALLOW_COMMIT=0`
- even if someone switches `EXECUTOR_MODE=COMMIT`, controller still does not write until:
  - `ALLOW_COMMIT=1`

## Profiles
- `vertex_primary_profile`:
  - 50 / 30 / 20
- `vertex_compact_profile`:
  - 30 / 50 / 20
- `local_profile`:
  - 25 / 25 / 50

## What commit would update later
- `cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment:global`
  - weights
  - last rebalance metadata
- `cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_winner:global`
  - incumbent_arm

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_APPLY_CONTROLLER_ADVISORY_ONLY=1
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_APPLY_CONTROLLER_EXECUTOR_MODE=DRY_RUN
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_APPLY_CONTROLLER_ALLOW_COMMIT=0
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_APPLY_CONTROLLER_MIN_SCORE_MARGIN=0.05
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_APPLY_CONTROLLER_COOLDOWN_SEC=21600
```

## Smoke checks
```bash
curl -s localhost:9981/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_apply_controller_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_apply_controller:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_apply_controller_decisions + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_apply_controller_journal + - COUNT 5
redis-cli HGETALL cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment:global
redis-cli HGETALL cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_winner:global
```

## Notes
- scope only `scanner_infra`
- hot path untouched
- this phase is still plan/journal-first
- next phase can add post-apply verification for the experiment contour
