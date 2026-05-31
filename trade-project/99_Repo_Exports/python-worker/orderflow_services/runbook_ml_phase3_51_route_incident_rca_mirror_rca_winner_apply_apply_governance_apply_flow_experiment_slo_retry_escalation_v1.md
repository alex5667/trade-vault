# Phase 3.51 — Apply-Flow Experiment SLO/MTTR + Retry/Escalation

## Цель
Дать новому experiment verification/rollback contour полный governance layer:
- SLO rollups
- rollback MTTR
- bounded retry
- escalation

## Что делает
- `experiment_slo_rollup`:
  - читает verification results
  - читает rollback journal
  - читает retry results
  - читает escalations
  - пишет `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_slo_rollups`

- `experiment_retry_escalation_controller`:
  - читает verification results
  - решает:
    - `HOLD`
    - `RETRY_REAPPLY_TARGET_PROFILE`
    - `ESCALATE`
  - пишет:
    - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_retry_results`
    - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_escalations`

## SLO fields
- `verify_keep_rate`
- `rollback_plan_rate`
- `rollback_applied_rate`
- `rollback_mttr_p95_sec`
- `escalation_rate`

## Retry policy
- retry only for bounded reasons
- default retry reason:
  - `TARGET_SHARE_TOO_LOW_AFTER_APPLY`
- mismatch reasons escalate directly
- retry attempts tracked by:
  - target profile
  - target incumbent arm
  - reason_code

## Safe behavior
- retry controller default:
  - `ADVISORY_ONLY=1`
  - `EXECUTOR_MODE=DRY_RUN`
  - `ALLOW_COMMIT=0`
- no infinite retries
- escalation after attempts exhausted or non-retryable reason

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_RETRY_ADVISORY_ONLY=1
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_RETRY_EXECUTOR_MODE=DRY_RUN
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_RETRY_ALLOW_COMMIT=0
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_RETRY_MAX_ATTEMPTS=2
```

## Smoke checks
```bash
curl -s localhost:9984/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_'
curl -s localhost:9985/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_slo:last
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_retry:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_slo_rollups + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_retry_results + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_escalations + - COUNT 5
```

## Notes
- scope only `scanner_infra`
- hot path untouched
- retry remains bounded and opt-in
- next step can add forensic bundle builder for this experiment safety contour
