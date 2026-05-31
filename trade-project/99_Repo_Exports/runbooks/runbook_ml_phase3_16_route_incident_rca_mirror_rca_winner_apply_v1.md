# Phase 3.16 — Route Incident RCA Mirror RCA Winner Apply Controller

## Цель
Добавляет Controller, который слушает решения Evaluator'а (из фазы 3.15) и имеет право переключать Harness-настройки. Поддерживаются мягкие переводы (SHADOW_PRIMARY) и жесткие переводы в единственный канал (SINGLE_ARM). По умолчанию работает в режиме DRY_RUN и требует ADVISORY_ONLY (то есть не выполняет реального переключения).

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_ADVISORY_ONLY=1
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXECUTOR_MODE=DRY_RUN
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_STRATEGY=SHADOW_PRIMARY
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_COOLDOWN_SEC=21600
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_ALLOW_ARMS_JSON='["vertex_candidate","local_fallback_candidate"]'
```

## Smoke checks
```bash
curl -s localhost:9935/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_decisions + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_journal + - COUNT 5
redis-cli HGETALL cfg:ml:route_incident_rca_mirror_rca_experiment:global
```
