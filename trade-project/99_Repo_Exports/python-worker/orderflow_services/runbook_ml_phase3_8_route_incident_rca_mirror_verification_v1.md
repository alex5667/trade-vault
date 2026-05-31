# Phase 3.8 — Route Incident RCA Mirror Verification Loop

## Цель
После первого controlled `MIRROR` rollout:
- измерять post-switch mismatch / drift / pending / freshness
- автоматически откатывать:
  - `MIRROR -> AUDIT_ONLY`

при деградации.

## Что делает
- читает comparator results:
  - `stream:ml:route_incident_rca_shadow_comparator_results`
- читает comparator freshness:
  - `metrics:ml:route_incident_rca_shadow_comparator:last`
- оценивает pending comparator rows:
  - `state:ml:route_incident_rca_shadow_comparator:pending:*`
- читает текущий mode:
  - `cfg:ml:route_incident_rca_shadow_handoff:global`
- принимает решения:
  - `KEEP_MIRROR`
  - `ROLLBACK_TO_AUDIT`
  - `HOLD`

## Условия rollback
- `mismatch_rate > max_mismatch_rate`
- `drift_rate > max_drift_rate`
- `match_rate < min_match_rate`
- `pending_total > max_pending_total`
- `comparator_age_ms > max_comparator_age_ms`
- `total < min_sample`

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_VERIFICATION_ADVISORY_ONLY=1
export ML_ROUTE_INCIDENT_RCA_MIRROR_VERIFICATION_EXECUTOR_MODE=DRY_RUN
export ML_ROUTE_INCIDENT_RCA_MIRROR_VERIFICATION_MIN_SAMPLE=20
export ML_ROUTE_INCIDENT_RCA_MIRROR_VERIFICATION_MAX_MISMATCH_RATE=0.00
export ML_ROUTE_INCIDENT_RCA_MIRROR_VERIFICATION_MAX_DRIFT_RATE=0.25
export ML_ROUTE_INCIDENT_RCA_MIRROR_VERIFICATION_MIN_MATCH_RATE=0.65
export ML_ROUTE_INCIDENT_RCA_MIRROR_VERIFICATION_MAX_PENDING_TOTAL=10
export ML_ROUTE_INCIDENT_RCA_MIRROR_VERIFICATION_MAX_COMPARATOR_AGE_MS=1800000
```

## Smoke checks
```bash
curl -s localhost:9924/metrics | grep '^ml_route_incident_rca_mirror_verification_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_verification:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_verification_results + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rollback_journal + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_verification_audit + - COUNT 5
redis-cli HGETALL cfg:ml:route_incident_rca_shadow_handoff:global
```

## Notes
- scope только `scanner_infra`
- hot path не затронут
- default = advisory-only
- auto rollback работает только при:
  - `ADVISORY_ONLY=0`
  - `EXECUTOR_MODE=COMMIT`
