# Phase 3.9 — Route Incident RCA Mirror Rollout Controller

## Цель
Связать вместе:
- `mirror-governor`
- `mirror verification loop`
- bounded mode transitions
- rollback journal

и получить полный controlled lifecycle:
- `AUDIT_ONLY -> MIRROR -> AUDIT_ONLY`

для `route_incident_rca`.

## Что делает
- читает события из:
  - `stream:ml:route_incident_rca_mirror_governor_decisions`
  - `stream:ml:route_incident_rca_mirror_verification_results`
- поддерживает единый rollout state:
  - `AUDIT_ONLY_STABLE`
  - `PROMOTION_APPLIED`
  - `MIRROR_ACTIVE`
  - `ROLLBACK_APPLIED`
  - `UNKNOWN`
- разрешает только bounded transitions:
  - `AUDIT_ONLY -> MIRROR` только по governor
  - `MIRROR -> AUDIT_ONLY` только по verification
- обновляет:
  - `cfg:ml:route_incident_rca_shadow_handoff:global`
  - `state:ml:route_incident_rca_mirror_rollout:state`

## Решения controller
- `PROMOTE`
- `ROLLBACK`
- `HOLD`

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_ROLLOUT_ADVISORY_ONLY=1
export ML_ROUTE_INCIDENT_RCA_MIRROR_ROLLOUT_EXECUTOR_MODE=DRY_RUN
export ML_ROUTE_INCIDENT_RCA_MIRROR_ROLLOUT_PROMOTION_COOLDOWN_SEC=21600
export ML_ROUTE_INCIDENT_RCA_MIRROR_ROLLOUT_ALLOW_GOVERNOR_PROMOTION=1
export ML_ROUTE_INCIDENT_RCA_MIRROR_ROLLOUT_ALLOW_VERIFICATION_ROLLBACK=1
```

## Smoke checks
```bash
curl -s localhost:9925/metrics | grep '^ml_route_incident_rca_mirror_rollout_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rollout:last
redis-cli HGETALL state:ml:route_incident_rca_mirror_rollout:state
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rollout_decisions + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rollout_journal + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rollout_audit + - COUNT 5
redis-cli HGETALL cfg:ml:route_incident_rca_shadow_handoff:global
```

## Notes
- scope только `scanner_infra`
- hot path не затронут
- default = advisory-only
- auto apply работает только при:
  - `ADVISORY_ONLY=0`
  - `EXECUTOR_MODE=COMMIT`
