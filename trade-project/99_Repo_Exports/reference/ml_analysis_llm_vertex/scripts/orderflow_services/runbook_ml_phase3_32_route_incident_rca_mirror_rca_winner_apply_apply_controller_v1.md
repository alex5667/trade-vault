# Phase 3.32 — Route Incident RCA Mirror RCA Winner-Apply Apply Apply Controller

## Цель
Брать recommendation из `Phase 3.31` и boundedly переводить experiment harness:
- из `SHADOW` в controlled `SINGLE_ARM`
- или менять `primary_arm` внутри `SHADOW`

сначала только в `DRY_RUN`.

## Что делает
- читает evaluator decisions из:
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_evaluator_decisions`
- читает текущую policy harness из:
  - `cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_experiment:global`
- принимает только bounded promotions:
  - `PROMOTE_VERTEX_CANDIDATE`
  - `PROMOTE_LOCAL_FALLBACK_CANDIDATE`
- пишет:
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_controller_decisions`
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_controller_journal`

## Apply strategies
- `SHADOW_PRIMARY`
  - mode остаётся `SHADOW`
  - меняется только `primary_arm`
- `SINGLE_ARM`
  - mode переводится в `SINGLE_ARM`
  - winner становится `primary_arm`

## Что НЕ делает
- не применяет `KEEP_*`
- не работает с любыми arm вне:
  - `vertex_candidate`
  - `local_fallback_candidate`
- не включает auto-apply, пока:
  - `ADVISORY_ONLY=1`
  - `EXECUTOR_MODE=DRY_RUN`

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_CONTROLLER_ADVISORY_ONLY=1
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_CONTROLLER_EXECUTOR_MODE=DRY_RUN
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_CONTROLLER_STRATEGY=SHADOW_PRIMARY
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_CONTROLLER_COOLDOWN_SEC=21600
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_CONTROLLER_ALLOW_ARMS_JSON='["vertex_candidate","local_fallback_candidate"]'
```

## Smoke checks
```bash
curl -s localhost:9957/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_controller_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_controller:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_controller_decisions + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_controller_journal + - COUNT 5
redis-cli HGETALL cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_experiment:global
```

## Notes
- scope только `scanner_infra`
- hot path не затронут
- bounded surface: only SHADOW -> SHADOW(primary change) or SHADOW/SINGLE_ARM controlled apply
- следующий шаг — verification/rollback loop именно для winner-apply apply path
