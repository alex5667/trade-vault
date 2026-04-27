# Phase 3.42 — Route Incident RCA Mirror RCA Winner-Apply Apply Governance SLO/MTTR + Retry/Escalation

## Цель
Дать `winner-apply apply governance apply/verify contour` такой же governance layer, как у других safety loops:
- SLO / MTTR analytics
- bounded retry
- auto-escalation summaries

## Что делает
- `governance_slo_analytics`:
  - считает apply rate
  - считает verify keep rate
  - считает rollback MTTR p50 / p95
- `governance_retry_controller`:
  - bounded re-apply rollback target, если verification loop потребовал rollback, а live policy ещё не сошлась
- `governance_auto_escalation_summarizer`:
  - поднимает severity summary по SLO + retry

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_RETRY_MAX_ATTEMPTS=2
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_RETRY_BACKOFF_SEC=120
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_ROLLBACK_MTTR_SLO_SEC=120
```

## Smoke checks
```bash
curl -s localhost:9970/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_'
curl -s localhost:9971/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_retry_'
curl -s localhost:9972/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_escalations_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_slo:last
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_retry:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_slo_rollups + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_retry_results + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_escalations + - COUNT 5
```

## Notes
- scope только `scanner_infra`
- hot path не затронут
- retry bounded
- escalation advisory-only
