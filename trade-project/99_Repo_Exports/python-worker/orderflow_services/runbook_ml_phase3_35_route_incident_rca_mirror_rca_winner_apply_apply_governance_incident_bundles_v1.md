# Phase 3.35 — Route Incident RCA Mirror RCA Winner-Apply Apply Governance Incident Bundle Builder

## Цель
Собрать единый forensic bundle для `route_incident_rca mirror RCA winner-apply apply governance contour`,
чтобы его можно было использовать для:
- RCA
- local / Vertex summarization
- отчётов
- пост-анализа apply / verify / rollback инцидентов

## Что делает
- слушает triggers из:
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_controller_journal`
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_rollback_journal`
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_escalations`
- для trigger собирает недавний контекст из:
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_controller_journal`
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_verification_results`
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_retry_results`
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_rollback_journal`
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_escalations`
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_slo_rollups`
- строит единый bundle JSON
- пишет:
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_incident_bundles`
  - `metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_incident_bundles:last`

## Triggers
- apply transitions:
  - `APPLY_PRIMARY_ARM_SHADOW`
  - `APPLY_SINGLE_ARM`
- rollback journal entries
- escalation severity:
  - `warning`
  - `critical`

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_INCIDENT_BUNDLES_LOOKBACK_COUNT=50
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_INCIDENT_BUNDLES_RECENT_WINDOW_MIN=240
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_INCIDENT_BUNDLES_ONLY_SEVERITY=warning,critical
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_INCIDENT_BUNDLES_TRIGGER_ON_APPLY_DECISIONS=APPLY_PRIMARY_ARM_SHADOW,APPLY_SINGLE_ARM
```

## Smoke checks
```bash
curl -s localhost:9962/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_incident_bundles_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_incident_bundles:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_incident_bundles + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_incident_bundles_audit + - COUNT 5
```

## Notes
- scope только `scanner_infra`
- hot path не затронут
- bundle builder advisory-only
- это единый forensic ingress для следующего RCA/reporting слоя apply-governance contour
