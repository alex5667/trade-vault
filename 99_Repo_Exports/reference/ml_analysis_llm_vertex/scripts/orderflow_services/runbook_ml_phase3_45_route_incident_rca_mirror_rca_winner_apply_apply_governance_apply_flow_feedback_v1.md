# Phase 3.45 — Route Incident RCA Mirror RCA Winner-Apply Apply Governance Apply-Flow RCA Consumer + Feedback Loop

## Цель
Замкнуть новый dedicated apply-flow RCA contour из `Phase 3.44`:
- request
- result
- feedback
- governance

без reuse старых governance feedback streams из `3.37`.

## Что делает
- `apply_flow_rca_consumer`:
  - читает requests из dedicated streams:
    - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_vertex_rca_requests`
    - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_local_rca_requests`
  - строит bounded RCA result
  - пишет:
    - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_results`

- `apply_flow_feedback_governor`:
  - читает feedback из:
    - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_feedback`
  - считает rollups
  - пишет:
    - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_governance_decisions`

## Feedback contract
- `request_id`
- `bundle_id`
- `quality_score`
- `usefulness_score`
- `accepted`
- `reason_code`

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_RCA_HANDLER_MODE=DETERMINISTIC
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_RCA_GOVERNANCE_MIN_SAMPLES=10
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_RCA_GOVERNANCE_MIN_AVG_QUALITY=0.55
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_RCA_GOVERNANCE_MIN_AVG_USEFULNESS=0.60
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_RCA_GOVERNANCE_MIN_ACCEPTED_RATE=0.60
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_RCA_GOVERNANCE_MAX_LOW_QUALITY_RATE=0.35
```

## Smoke checks
```bash
curl -s localhost:9975/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_'
curl -s localhost:9976/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_governance_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca:last
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_governance:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_results + - COUNT 5
redis-cli XADD stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_feedback * \
  request_id af-1 \
  bundle_id apply-flow-b1 \
  quality_score 0.8 \
  usefulness_score 0.9 \
  accepted 1 \
  reason_code helpful \
  ts_ms $(date +%s%3N)
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_feedback_rollups + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_governance_decisions + - COUNT 5
```

## Notes
- scope только `scanner_infra`
- hot path не затронут
- result/governance layer isolated from `3.37`
- consumer bounded/deterministic by default
