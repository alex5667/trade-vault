# Phase 3.31 — Route Incident RCA Mirror RCA Winner-Apply Apply Evaluator + Winner Selection

## Цель
Читать:
- experiment exposures
- result streams
- feedback/usefulness evidence

и выпускать bounded recommendation:
- `KEEP_DETERMINISTIC`
- `PROMOTE_VERTEX_CANDIDATE`
- `PROMOTE_LOCAL_FALLBACK_CANDIDATE`

без auto-promotion.

## Что делает
- читает exposures из:
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_experiment_exposures`
- читает results из configurable result streams
- читает feedback из configurable feedback streams
- строит per-arm scorecards
- сравнивает candidate arms против incumbent arm
- пишет:
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_scorecards`
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_evaluator_decisions`

## Scorecard metrics per arm
- exposure_n
- result_n
- feedback_n
- avg_quality
- avg_usefulness
- accepted_rate
- result_coverage
- feedback_coverage
- coverage_multiplier
- score_raw
- score
- eligible

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_EVALUATOR_MIN_EXPOSURES=10
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_EVALUATOR_MIN_FEEDBACK=5
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_EVALUATOR_MIN_RESULT_COVERAGE=0.30
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_EVALUATOR_MIN_FEEDBACK_COVERAGE=0.20
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_EVALUATOR_MIN_QUALITY=0.55
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_EVALUATOR_MIN_USEFULNESS=0.60
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_EVALUATOR_MIN_ACCEPTED_RATE=0.60
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_EVALUATOR_MIN_SCORE_MARGIN=0.05
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_EVALUATOR_INCUMBENT_ARM=deterministic
```

## Smoke checks
```bash
curl -s localhost:9956/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_evaluator_'
curl -s localhost:9956/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_evaluator_arm_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_evaluator:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_scorecards + - COUNT 9
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_evaluator_decisions + - COUNT 5
```

## Notes
- scope только `scanner_infra`
- hot path не затронут
- evaluator recommendation-only
- следующий шаг — apply controller для bounded promotion winner
