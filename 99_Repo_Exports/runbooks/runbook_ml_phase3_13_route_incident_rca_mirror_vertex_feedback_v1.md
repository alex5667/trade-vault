# Phase 3.13 — Route Incident RCA Mirror Vertex RCA Consumer + Feedback Loop

## Цель
Этот шаг внедряет консьюмер для взаимодействия с Vertex RCA API (сейчас в режиме DETERMINISTIC) и сервис-Governor, который собирает feedback по каждому сгенерированному ответу и на основе бизнес-метрик (quality_score, usefulness_score, accepted) переводит Bridge в режим работы LOCAL_ONLY при плохих результатах.

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_VERTEX_RCA_HANDLER_MODE=DETERMINISTIC
export ML_ROUTE_INCIDENT_RCA_MIRROR_VERTEX_RCA_GOVERNANCE_MIN_SAMPLES=10
export ML_ROUTE_INCIDENT_RCA_MIRROR_VERTEX_RCA_GOVERNANCE_MIN_AVG_QUALITY=0.55
export ML_ROUTE_INCIDENT_RCA_MIRROR_VERTEX_RCA_GOVERNANCE_MIN_AVG_USEFULNESS=0.60
export ML_ROUTE_INCIDENT_RCA_MIRROR_VERTEX_RCA_GOVERNANCE_MIN_ACCEPTED_RATE=0.60
export ML_ROUTE_INCIDENT_RCA_MIRROR_VERTEX_RCA_GOVERNANCE_MAX_LOW_QUALITY_RATE=0.35
```

## Smoke checks
```bash
curl -s localhost:9931/metrics | grep '^ml_route_incident_rca_mirror_vertex_rca_'
curl -s localhost:9932/metrics | grep '^ml_route_incident_rca_mirror_vertex_governance_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_vertex_rca:last
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_vertex_rca_governance:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_vertex_rca_results + - COUNT 5

# Test feedback
redis-cli XADD stream:ml:route_incident_rca_mirror_vertex_rca_feedback * \
  request_id test-r1 \
  bundle_id test-b1 \
  quality_score 0.8 \
  usefulness_score 0.9 \
  accepted 1 \
  reason_code helpful \
  ts_ms $(date +%s%3N)

redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_vertex_rca_feedback_rollups + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_vertex_rca_governance_decisions + - COUNT 5
```
