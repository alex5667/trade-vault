# Phase 3.38 — Route Incident RCA Mirror RCA Winner-Apply Apply Governance Experiment Harness

## Цель
Сравнивать на одном и том же `route_incident_rca mirror RCA winner-apply apply governance bundle` contour:
- `deterministic`
- `vertex_candidate`
- `local_fallback_candidate`

через deterministic A/B buckets и exposure logging.

## Что делает
- читает bundles из:
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_incident_bundles`
- deterministic assignment по `bundle_id + hash_salt`
- пишет exposures в:
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_experiment_exposures`
- пишет primary/shadow requests в отдельные streams:
  - `deterministic` -> `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca_requests`
  - `vertex_candidate` -> `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca_candidate_requests`
  - `local_fallback_candidate` -> `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_local_rca_candidate_requests`

## Режимы
- `DISABLED`
- `SHADOW`
- `SINGLE_ARM`
- `MULTI_ARM`

## Что важно
- в `SHADOW`:
  - один `primary_arm`
  - дополнительные `shadow_arms`
- в `MULTI_ARM`:
  - deterministic weighted assignment между всеми arms
- все exposures логируются отдельно от result/feedback loop

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_EXPERIMENT_MODE=SHADOW
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_EXPERIMENT_PRIMARY_ARM=deterministic
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_EXPERIMENT_SHADOW_ARMS_JSON='["vertex_candidate","local_fallback_candidate"]'
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_EXPERIMENT_ARM_WEIGHTS_JSON='{"deterministic":70,"vertex_candidate":20,"local_fallback_candidate":10}'
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_EXPERIMENT_ALLOW_SEVERITIES=warning,critical
```

## Smoke checks
```bash
curl -s localhost:9966/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_experiment_'
curl -s localhost:9966/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_exposures_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_experiment:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_experiment_decisions + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_experiment_exposures + - COUNT 10
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca_requests + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca_candidate_requests + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_local_rca_candidate_requests + - COUNT 5
```

## Notes
- scope только `scanner_infra`
- hot path не затронут
- harness сам не выбирает winner
- следующий шаг после накопления exposure/result/feedback — evaluator/winner selection
