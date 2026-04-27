# Phase 3.7 — Route Incident RCA Mirror Governor

## Цель
Разрешать переход:
- `AUDIT_ONLY -> MIRROR`

только при стабильных `route_incident_rca shadow comparator` metrics.

## Что делает
- читает comparator results:
  - `stream:ml:route_incident_rca_shadow_comparator_results`
- читает comparator freshness:
  - `metrics:ml:route_incident_rca_shadow_comparator:last`
- оценивает pending comparator rows:
  - `state:ml:route_incident_rca_shadow_comparator:pending:*`
- читает текущий shadow mode:
  - `cfg:ml:route_incident_rca_shadow_handoff:global`
- принимает решение:
  - `PROMOTE_TO_MIRROR`
  - `KEEP_AUDIT_ONLY`
  - `KEEP_MIRROR`
  - `DEMOTE_TO_AUDIT`
  - `HOLD`

## Критерии стабильности
- `total >= min_sample`
- `mismatch_rate <= max_mismatch_rate`
- `drift_rate <= max_drift_rate`
- `match_rate >= min_match_rate`
- `pending_total <= max_pending_total`
- `comparator_age_ms <= max_comparator_age_ms`

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_GOVERNOR_ADVISORY_ONLY=1
export ML_ROUTE_INCIDENT_RCA_MIRROR_GOVERNOR_EXECUTOR_MODE=DRY_RUN
export ML_ROUTE_INCIDENT_RCA_MIRROR_GOVERNOR_MIN_SAMPLE=20
export ML_ROUTE_INCIDENT_RCA_MIRROR_GOVERNOR_MAX_MISMATCH_RATE=0.00
export ML_ROUTE_INCIDENT_RCA_MIRROR_GOVERNOR_MAX_DRIFT_RATE=0.20
export ML_ROUTE_INCIDENT_RCA_MIRROR_GOVERNOR_MIN_MATCH_RATE=0.70
export ML_ROUTE_INCIDENT_RCA_MIRROR_GOVERNOR_MAX_PENDING_TOTAL=10
export ML_ROUTE_INCIDENT_RCA_MIRROR_GOVERNOR_MAX_COMPARATOR_AGE_MS=1800000
```

## Smoke checks
```bash
curl -s localhost:9923/metrics | grep '^ml_route_incident_rca_mirror_governor_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_governor:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_governor_decisions + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_governor_audit + - COUNT 5
redis-cli HGETALL cfg:ml:route_incident_rca_shadow_handoff:global
```

## Notes
- scope только `scanner_infra`
- hot path не затронут
- default = advisory-only
- пока governor не показывает устойчивый `PROMOTE_TO_MIRROR`, mode должен оставаться `AUDIT_ONLY`
