# Phase 3.14 — Route Incident RCA Mirror RCA Experiment Harness

## Цель
Добавляет A/B experiment harness (deterministic routing по arms, весовое распределение, SHADOW and MULTI-ARM режимы) для маршрутизации сгенерированных бандлов в различные RCA процессы: `deterministic`, `vertex_candidate`, `local_fallback_candidate`. Позволяет логировать честные exposures в отдельный стрим и персистить в БД.

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EXPERIMENT_MODE=SHADOW
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EXPERIMENT_PRIMARY_ARM=deterministic
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EXPERIMENT_SHADOW_ARMS_JSON='["vertex_candidate","local_fallback_candidate"]'
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EXPERIMENT_ARM_WEIGHTS_JSON='{"deterministic":70,"vertex_candidate":20,"local_fallback_candidate":10}'
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EXPERIMENT_ALLOW_SEVERITIES=warning,critical
```

## Smoke checks
```bash
curl -s localhost:9933/metrics | grep '^ml_route_incident_rca_mirror_rca_experiment_'
curl -s localhost:9933/metrics | grep '^ml_route_incident_rca_mirror_rca_exposures_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_experiment:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_experiment_decisions + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_experiment_exposures + - COUNT 10
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_requests + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_vertex_rca_requests + - COUNT 5
redis-cli XREVRANGE stream:ml:local_fallback_requests + - COUNT 5
```
