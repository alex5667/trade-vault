# Phase 3.54 — Apply-Flow Experiment Incident RCA Result Consumer + Feedback/Usefulness Loop

## Цель
Замкнуть новый dedicated incident RCA contour:
- request
- result
- feedback
- usefulness governance

## Что делает
- `incident_rca_result_consumer`:
  - читает dedicated RCA requests из:
    - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_vertex_rca_requests`
    - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_local_rca_requests`
  - строит deterministic bounded RCA result
  - пишет в:
    - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_results`

- `incident_rca_usefulness_governor`:
  - читает feedback из:
    - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_feedback`
  - join с incident RCA results
  - строит provider rollups:
    - `VERTEX`
    - `LOCAL`
  - пишет usefulness decisions

## Feedback contract
- `request_id`
- `bundle_id`
- `quality_score`
- `usefulness_score`
- `accepted`
- `reason_code`

## Governance decisions
- `HOLD`
- `KEEP_AUTO`
- `KEEP_VERTEX_ONLY`
- `KEEP_LOCAL_ONLY`
- `PREFER_VERTEX_ONLY`
- `PREFER_LOCAL_ONLY`
- `RETURN_TO_AUTO`

## Safe behavior
- result consumer deterministic only
- usefulness governor default:
  - `ADVISORY_ONLY=1`
  - `EXECUTOR_MODE=DRY_RUN`
- commit would touch only:
  - `cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_bridge:global`

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_RESULTS_HANDLER_MODE=DETERMINISTIC
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_USEFULNESS_ADVISORY_ONLY=1
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_USEFULNESS_EXECUTOR_MODE=DRY_RUN
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_USEFULNESS_MIN_VERTEX_SAMPLES=5
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_USEFULNESS_MIN_LOCAL_SAMPLES=5
```

## Smoke checks
```bash
curl -s localhost:9988/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_results_'
curl -s localhost:9989/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_results:last
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_usefulness:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_results + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_usefulness_rollups + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_usefulness_decisions + - COUNT 5
```

## Notes
- scope only `scanner_infra`
- hot path untouched
- new loop is isolated from regular routing RCA usefulness path
- next step: winner/apply controller for this new incident RCA usefulness loop
