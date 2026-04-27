# Phase 3.37 — Route Incident RCA Mirror RCA Winner-Apply Apply Governance Vertex RCA Consumer + Feedback Loop

## Цель
Замкнуть dedicated `route_incident_rca mirror RCA winner-apply apply governance RCA` contour:
- request
- result
- feedback
- governance

без смешивания с предыдущим `apply RCA` и обычным operator RCA.

## Что делает
- `governance_vertex_rca_consumer`:
  - читает `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca_requests`
  - строит bounded RCA result
  - пишет `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca_results`
- `governance_vertex_feedback_governor`:
  - читает `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca_feedback`
  - считает quality/usefulness rollups
  - пишет governance decisions в
    - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca_governance_decisions`

## Feedback contract
Минимальные поля feedback event:
- `request_id`
- `bundle_id`
- `quality_score` in [0..1]
- `usefulness_score` in [0..1]
- `accepted` in {0,1}
- `reason_code`

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_RCA_HANDLER_MODE=DETERMINISTIC
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_RCA_GOVERNANCE_MIN_SAMPLES=10
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_RCA_GOVERNANCE_MIN_AVG_QUALITY=0.55
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_RCA_GOVERNANCE_MIN_AVG_USEFULNESS=0.60
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_RCA_GOVERNANCE_MIN_ACCEPTED_RATE=0.60
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_RCA_GOVERNANCE_MAX_LOW_QUALITY_RATE=0.35
```

## Smoke checks
```bash
curl -s localhost:9964/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca_'
curl -s localhost:9965/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_governance_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca:last
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca_governance:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca_results + - COUNT 5
redis-cli XADD stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca_feedback * \
  request_id test-r1 \
  bundle_id test-b1 \
  quality_score 0.8 \
  usefulness_score 0.9 \
  accepted 1 \
  reason_code helpful \
  ts_ms $(date +%s%3N)
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca_feedback_rollups + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca_governance_decisions + - COUNT 5
```

## Notes
- scope только `scanner_infra`
- hot path не затронут
- consumer по умолчанию deterministic/bounded
- governance по умолчанию advisory-only
