# Phase 3.47 — Route Incident RCA Mirror RCA Winner-Apply Apply Governance Apply-Flow Experiment Harness

## Цель
Сделать отдельный experiment harness уже для нового isolated `apply-flow RCA contour`,
чтобы сравнивать отдельные strategies / prompts / providers внутри нового loop,
а не на старом governance RCA path.

## Что делает
- читает incident bundles из:
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_incident_bundles`
- deterministic arm assignment по `bundle_id`
- пишет exposure log в:
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_exposures`
- пишет experiment decisions в:
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_decisions`
- шлёт dedicated experiment requests в:
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_vertex_requests`
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_local_requests`

## Arms
- `vertex_primary`
- `vertex_compact_candidate`
- `local_candidate`

## Safe behavior
- mode по умолчанию:
  - `SHADOW`
- harness не меняет текущий primary apply-flow bridge
- harness не пишет в старые RCA request streams
- harness не смешивает experiment path со старым governance RCA path

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_MODE=SHADOW
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_VERTEX_PRIMARY_WEIGHT=50
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_VERTEX_COMPACT_WEIGHT=30
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_LOCAL_CANDIDATE_WEIGHT=20
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_ALLOW_SEVERITIES=warning,critical
```

## Smoke checks
```bash
curl -s localhost:9978/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_exposures + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_decisions + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_vertex_requests + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_local_requests + - COUNT 5
```

## Notes
- scope только `scanner_infra`
- hot path не затронут
- next step: experiment result consumer + feedback + winner selection for this new apply-flow contour
