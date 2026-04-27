# Phase 3.56 — Incident RCA Bridge-Mode Post-Apply Verification + Rollback Loop

## Цель
Добавить safety-loop уже для нового incident RCA bridge-mode controller:
- post-apply verification
- bounded rollback plan / controller

чтобы после будущего controlled mode-switch можно было автоматически проверить effect
и откатывать bridge mode обратно.

## Что делает
- `post_apply_verifier`:
  - читает apply journal из `3.55`
  - читает current bridge mode
  - читает latest incident RCA usefulness rollup
  - решает:
    - `HOLD`
    - `VERIFIED`
    - `ROLLBACK_PREVIOUS_MODE`

- `rollback_controller`:
  - читает verification results
  - читает rollback-ready state
  - строит rollback plan
  - по умолчанию only journal-first

## Verification checks
- apply реально был сделан
- verify delay уже истёк
- current mode совпадает с target mode
- для `VERTEX_ONLY`:
  - vertex usefulness не ниже floor
  - vertex accepted rate не ниже floor
- для `LOCAL_ONLY`:
  - local usefulness не ниже floor
  - local accepted rate не ниже floor

## Rollback triggers
- `BRIDGE_MODE_MISMATCH_AFTER_APPLY`
- `VERTEX_ONLY_UNDERPERFORMS_AFTER_APPLY`
- `VERTEX_ONLY_LOW_ACCEPTED_RATE_AFTER_APPLY`
- `LOCAL_ONLY_UNDERPERFORMS_AFTER_APPLY`
- `LOCAL_ONLY_LOW_ACCEPTED_RATE_AFTER_APPLY`

## Safe behavior
- verifier совместим с `DRY_RUN`
- rollback controller default:
  - `ADVISORY_ONLY=1`
  - `EXECUTOR_MODE=DRY_RUN`
  - `ALLOW_COMMIT=0`

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_VERIFICATION_DELAY_SEC=900
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_VERIFICATION_MIN_PROVIDER_SAMPLES=5
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_VERIFICATION_MIN_PROVIDER_USEFULNESS=0.60
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_VERIFICATION_MIN_PROVIDER_ACCEPTED=0.60
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_ROLLBACK_ADVISORY_ONLY=1
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_ROLLBACK_EXECUTOR_MODE=DRY_RUN
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_ROLLBACK_ALLOW_COMMIT=0
```

## Smoke checks
```bash
curl -s localhost:9991/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_verification_'
curl -s localhost:9992/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_rollback_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_verification:last
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_rollback:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_verification_results + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_rollback_journal + - COUNT 5
```

## Notes
- scope only `scanner_infra`
- hot path untouched
- next step can add SLO/MTTR + retry/escalation for this bridge-mode safety loop
