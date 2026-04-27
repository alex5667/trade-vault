# Phase 2.1 — operator RCA persistence / quality / feedback

## Scope
- only `scanner_infra`
- advisory-only
- no UI
- no hot-path changes

## Components
- `operator_rca_results_persister_v2_1`
- `operator_rca_quality_scorer_v2_1`
- `operator_rca_feedback_loop_v2_1`

## Rollout
1. apply SQL:
```sql
\i orderflow_services/sql/ml_phase2_1_v1.sql
```
2. connect compose fragment:
- `orderflow_services/docker_compose_fragment_ml_phase2_1_v1.yml`
3. connect Prometheus rules:
- `orderflow_services/prometheus_alerts_ml_phase2_1_v1.yml`

## Smoke checks
```bash
redis-cli XREVRANGE stream:ml:operator_rca_results + - COUNT 3
redis-cli XREVRANGE stream:ml:operator_rca_quality_results + - COUNT 3
redis-cli XREVRANGE stream:ml:operator_rca_feedback_summary + - COUNT 3
curl -s localhost:9870/metrics | grep '^ml_operator_rca_results_'
curl -s localhost:9871/metrics | grep '^ml_operator_rca_quality_'
curl -s localhost:9872/metrics | grep '^ml_operator_rca_feedback_'
```

## Feedback event example
```json
{
  "recommendation_id": "rec-123",
  "ts_ms": 1775370000123,
  "reviewer": "ops_user",
  "decision": "USEFUL",
  "action_type": "propose_threshold_canary",
  "note": "helped narrow RCA quickly"
}
```
