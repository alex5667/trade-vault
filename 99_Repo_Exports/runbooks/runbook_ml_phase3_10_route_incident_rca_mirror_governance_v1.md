# Phase 3.10 — Route Incident RCA Mirror SLO/MTTR + Retry/Escalation

## Цель
Дать `route_incident_rca mirror lifecycle` полный governance contour:
- SLO / MTTR analytics
- bounded retry
- auto-escalation summaries

## Что делает
- `mirror_slo_analytics`:
  - считает promotion / rollback apply-rate
  - считает rollback MTTR p50 / p95
- `mirror_retry_controller`:
  - bounded re-apply target mode, если rollout decision был commit-like, а mode не перешёл
- `mirror_auto_escalation_summarizer`:
  - поднимает severity summary по SLO + retry

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_RETRY_MAX_ATTEMPTS=2
export ML_ROUTE_INCIDENT_RCA_MIRROR_RETRY_BACKOFF_SEC=120
export ML_ROUTE_INCIDENT_RCA_MIRROR_ROLLBACK_MTTR_SLO_SEC=120
```

## Smoke checks
```bash
curl -s localhost:9926/metrics | grep '^ml_route_incident_rca_mirror_'
curl -s localhost:9927/metrics | grep '^ml_route_incident_rca_mirror_retry_'
curl -s localhost:9928/metrics | grep '^ml_route_incident_rca_mirror_escalations_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_slo:last
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_retry:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_slo_rollups + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_retry_results + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_escalations + - COUNT 5
```

## Notes
- scope только `scanner_infra`
- hot path не затронут
- retry bounded
- escalation advisory-only
