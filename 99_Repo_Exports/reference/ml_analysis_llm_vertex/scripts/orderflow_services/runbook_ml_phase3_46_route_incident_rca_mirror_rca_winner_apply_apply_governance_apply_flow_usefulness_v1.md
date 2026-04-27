# Phase 3.46 — Route Incident RCA Mirror RCA Winner-Apply Apply Governance Apply-Flow Usefulness Governor

## Цель
Сделать suppress/promote для нового `apply-flow RCA contour` уже по этому новому loop,
отдельно от старого governance RCA path.

## Что делает
- читает feedback из:
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_feedback`
- читает results из:
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_results`
- join по `request_id`
- строит отдельные provider rollups:
  - `VERTEX`
  - `LOCAL`
- читает текущий bridge mode из:
  - `cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_bridge:global`

## Decisions
- `HOLD`
- `KEEP_AUTO`
- `KEEP_LOCAL_ONLY`
- `SUPPRESS_TO_LOCAL_ONLY`
- `PROMOTE_TO_AUTO`

## Suppress logic
- работает, когда bridge mode не `LOCAL_ONLY`
- требует:
  - достаточно samples по `VERTEX`
  - достаточно samples по `LOCAL`
  - `VERTEX` usefulness/accepted плохие
  - `LOCAL` usefulness/accepted хорошие
  - `LOCAL` лучше `VERTEX` на bounded delta

## Promote logic
- работает, когда bridge mode == `LOCAL_ONLY`
- требует:
  - достаточно `LOCAL` samples
  - `LOCAL` stable high usefulness
  - `LOCAL` stable high quality
  - cooldown истёк

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_USEFULNESS_ADVISORY_ONLY=1
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_USEFULNESS_EXECUTOR_MODE=DRY_RUN
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_USEFULNESS_MIN_VERTEX_SAMPLES_TO_SUPPRESS=10
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_USEFULNESS_MIN_LOCAL_SAMPLES_TO_SUPPRESS=5
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_USEFULNESS_MIN_LOCAL_SAMPLES_TO_PROMOTE=15
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_USEFULNESS_COOLDOWN_SEC=21600
```

## Smoke checks
```bash
curl -s localhost:9977/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_usefulness_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_usefulness:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_usefulness_rollups + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_usefulness_decisions + - COUNT 5
redis-cli HGETALL cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_bridge:global
```

## Notes
- scope только `scanner_infra`
- hot path не затронут
- this loop is separate from old governance RCA usefulness path
- promotion back to `AUTO` is bounded and cooldown-protected
