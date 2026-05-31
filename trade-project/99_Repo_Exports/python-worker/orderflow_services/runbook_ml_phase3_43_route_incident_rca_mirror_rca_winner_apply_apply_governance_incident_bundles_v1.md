# Phase 3.43 — Route Incident RCA Mirror RCA Winner-Apply Apply Governance Incident Bundle Builder

## Цель
Собрать единый forensic bundle для нового `route_incident_rca mirror RCA winner-apply apply governance contour`
из `3.40–3.42`, чтобы его можно было использовать для:
- RCA
- reporting
- Vertex/local summarization
- post-incident analysis

## Что делает
- слушает triggers из:
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_controller_decisions`
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_verification_results`
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_retry_results`
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_escalations`
- собирает контекст из:
  - apply controller decisions
  - apply journal
  - verification results
  - rollback journal
  - slo rollups
  - retry results
  - escalations
- пишет:
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_incident_bundles`
  - `metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_incident_bundles:last`

## Triggers
- apply decisions:
  - `APPLY_PRIMARY_ARM_SHADOW`
  - `APPLY_SINGLE_ARM`
- verification decisions:
  - `ROLLBACK_PREVIOUS_POLICY`
- retry decisions:
  - `EXHAUSTED`
- escalation severity:
  - `warning`
  - `critical`

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_INCIDENT_BUNDLES_LOOKBACK_COUNT=80
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_INCIDENT_BUNDLES_RECENT_WINDOW_MIN=360
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_INCIDENT_BUNDLES_ONLY_SEVERITIES=warning,critical
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_INCIDENT_BUNDLES_TRIGGER_APPLY_DECISIONS=APPLY_PRIMARY_ARM_SHADOW,APPLY_SINGLE_ARM
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_INCIDENT_BUNDLES_TRIGGER_VERIFY_DECISIONS=ROLLBACK_PREVIOUS_POLICY
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_INCIDENT_BUNDLES_TRIGGER_RETRY_DECISIONS=EXHAUSTED
```

## Smoke checks
```bash
curl -s localhost:9973/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_incident_bundles_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_incident_bundles:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_incident_bundles + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_incident_bundles_audit + - COUNT 5
```

## Notes
- scope только `scanner_infra`
- hot path не затронут
- это отдельный bundle-ingress именно для `3.40–3.42` contour
- bundle builder advisory-only
