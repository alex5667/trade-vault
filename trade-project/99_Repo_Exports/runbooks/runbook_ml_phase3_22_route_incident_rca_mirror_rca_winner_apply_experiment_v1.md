# Phase 3.22 — Route Incident RCA Mirror RCA Winner Apply Experiment Harness

## Цель
Поддержка A/B тестов (SHADOW / MULTI_ARM режимов) для контура Winner Apply RCA.
Раньше бандлы уходили жёстко в Bridge (Phase 3.20).
Теперь Harness читает бандлы, и в завимости от режима отправляет их "теневым" моделям (напр. `vertex_candidate`, `local_fallback`), при этом фиксируя Exposure: какой бандл попал на какой LLM-провайдер/алгоритм. 

Эта Exposure база данных позволит дальше оценить, чей развернутый ответ на RCA был лучше (evaluator).

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_MODE=SHADOW
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_PRIMARY_ARM=deterministic
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_SHADOW_ARMS_JSON='["vertex_candidate","local_fallback_candidate"]'
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_ARM_WEIGHTS_JSON='{"deterministic":70,"vertex_candidate":20,"local_fallback_candidate":10}'
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_ALLOW_SEVERITIES=warning,critical
```

## Smoke checks
```bash
curl -s localhost:9944/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_experiment_'
curl -s localhost:9944/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_exposures_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_experiment:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_experiment_decisions + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_experiment_exposures + - COUNT 10
```
