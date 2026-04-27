# Phase 3.34 — Route Incident RCA Mirror RCA Winner-Apply Apply SLO/MTTR + Retry/Escalation

## Цель
Дать `winner-apply apply/verify contour` такой же governance layer, как у других safety loops:
- SLO / MTTR analytics
- bounded retry
- auto-escalation summaries

## Что делает
- `winner_apply_apply_slo_analytics`:
  - считает apply rate
  - считает verify keep rate
  - считает rollback MTTR p50 / p95
- `winner_apply_apply_retry_controller`:
  - bounded re-apply rollback target, если verification loop потребовал rollback, а live policy ещё не сошлась
- `winner_apply_apply_auto_escalation_summarizer`:
  - поднимает severity summary по SLO + retry

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_RETRY_MAX_ATTEMPTS=2
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_RETRY_BACKOFF_SEC=120
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_ROLLBACK_MTTR_SLO_SEC=120
```

## Smoke checks
```bash
curl -s localhost:9959/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_'
curl -s localhost:9960/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_retry_'
curl -s localhost:9961/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_escalations_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_slo:last
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_retry:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_slo_rollups + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_retry_results + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_escalations + - COUNT 5
```

## Notes
- scope только `scanner_infra`
- hot path не затронут
- retry bounded
- escalation advisory-only
