# Phase 3.15 — Route Incident RCA Mirror RCA Evaluator

## Цель
Добавляет Evaluator, который агрегирует exposures, results и feedback. Позволяет выстраивать per-arm Scorecards (отслеживая result coverage, quality и usefulness), на базе которых выдаётся Bounded Recommendation о возможности Promote-перевода кандидатов (vertex или local_fallback).

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EVALUATOR_MIN_EXPOSURES=10
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EVALUATOR_MIN_FEEDBACK=5
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EVALUATOR_MIN_RESULT_COVERAGE=0.30
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EVALUATOR_MIN_FEEDBACK_COVERAGE=0.20
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EVALUATOR_MIN_QUALITY=0.55
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EVALUATOR_MIN_USEFULNESS=0.60
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EVALUATOR_MIN_ACCEPTED_RATE=0.60
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EVALUATOR_MIN_SCORE_MARGIN=0.05
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EVALUATOR_INCUMBENT_ARM=deterministic
```

## Smoke checks
```bash
curl -s localhost:9934/metrics | grep '^ml_route_incident_rca_mirror_rca_evaluator_'
curl -s localhost:9934/metrics | grep '^ml_route_incident_rca_mirror_rca_evaluator_arm_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_evaluator:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_scorecards + - COUNT 9
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_evaluator_decisions + - COUNT 5
```
