# Phase 3.41 — Route Incident RCA Mirror RCA Winner-Apply Apply Governance Verification + Rollback Loop

## Цель
После первого реального governance apply получить полный safety contour:
- apply
- verify
- rollback back to previous experiment policy

для `route_incident_rca mirror RCA winner-apply apply governance` path.

## Что делает
- читает последний actionable apply из:
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_controller_journal`
- читает post-apply exposures из:
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_experiment_exposures`
- читает live experiment policy из:
  - `cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_experiment:global`
- принимает решения:
  - `HOLD`
  - `KEEP_APPLIED`
  - `ROLLBACK_PREVIOUS_POLICY`

## Что проверяет
- current live policy matches target policy after apply
- enough post-apply exposures exist
- primary exposures mostly match target primary arm
- unexpected primary rate is bounded
- for `SINGLE_ARM`, shadow exposure rate stays bounded

## Что делает при rollback
- возвращает:
  - `mode_before`
  - `primary_arm_before`
- для `SHADOW` reconstructs `shadow_arms_json` как все arms кроме restored primary

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERIFICATION_ADVISORY_ONLY=1
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERIFICATION_EXECUTOR_MODE=DRY_RUN
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERIFICATION_MIN_EXPOSURES=5
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERIFICATION_MIN_PRIMARY_MATCH_RATE=0.80
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERIFICATION_MAX_UNEXPECTED_PRIMARY_RATE=0.20
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERIFICATION_MAX_SHADOW_RATE_SINGLE_ARM=0.05
```

## Smoke checks
```bash
curl -s localhost:9969/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_verification_'
curl -s localhost:9969/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_verification:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_verification_results + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_rollback_journal + - COUNT 5
redis-cli HGETALL cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_experiment:global
```

## Notes
- scope только `scanner_infra`
- hot path не затронут
- verification loop checks only the latest actionable governance apply
- rollback remains bounded to previous experiment policy
