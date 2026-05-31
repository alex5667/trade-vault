# Phase 3.23 — Route Incident RCA Mirror RCA Winner Apply Evaluator

## Цель
Подсчет "Scorecards" (табеля успеваемости) для каждой модели, на которую отправлялись RCA задачи в Winner Apply контуре (через harness из шага 3.22). 
Вывод рекомендации по продвижению модели (A/B testing outcome). 

## Safe start
Работает в advisory (пассивном) режиме, периодически (10s) собирая метрики из трех источников стримов: Exposures, Results, Feedbacks. 

```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EVALUATOR_MIN_EXPOSURES=10
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EVALUATOR_MIN_FEEDBACK=5
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EVALUATOR_MIN_RESULT_COVERAGE=0.30
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EVALUATOR_MIN_FEEDBACK_COVERAGE=0.20
```

## Smoke checks
```bash
curl -s localhost:9945/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_evaluator_'
curl -s localhost:9945/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_evaluator_arm_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_evaluator:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_scorecards + - COUNT 9
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_evaluator_decisions + - COUNT 5
```
