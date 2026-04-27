# Phase 3.50 — Route Incident RCA Mirror RCA Winner-Apply Apply Governance Apply-Flow Experiment Post-Apply Verification + Rollback Loop

## Цель
Добавить safety-loop уже для нового experiment contour:
- post-apply verification
- bounded rollback plan / controller

чтобы после будущего controlled apply можно было автоматически проверить effect
и откатывать profile / incumbent обратно.

## Что делает
- `post_apply_verifier`:
  - читает apply journal из `3.49`
  - читает current cfg state
  - читает post-apply exposures
  - решает:
    - `HOLD`
    - `VERIFIED`
    - `ROLLBACK_PREVIOUS_PROFILE`

- `rollback_controller`:
  - читает verification results
  - строит rollback plan
  - по умолчанию только journal-first

## Verification checks
- apply реально был сделан
- verify delay уже истёк
- current weights совпадают с target weights
- current incumbent совпадает с target incumbent
- post-apply exposures >= min threshold
- observed target share не ниже bounded floor

## Rollback triggers
- `WEIGHTS_MISMATCH_AFTER_APPLY`
- `INCUMBENT_MISMATCH_AFTER_APPLY`
- `TARGET_SHARE_TOO_LOW_AFTER_APPLY`

## Safe behavior
- verifier совместим с `DRY_RUN`
- rollback controller default:
  - `ADVISORY_ONLY=1`
  - `EXECUTOR_MODE=DRY_RUN`
  - `ALLOW_COMMIT=0`

## What commit would rollback later
- `cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment:global`
  - previous weights
- `cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_winner:global`
  - previous incumbent arm

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_VERIFICATION_DELAY_SEC=900
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_VERIFICATION_MIN_POST_APPLY_EXPOSURES=5
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_VERIFICATION_MIN_TARGET_SHARE_FLOOR=0.25
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_VERIFICATION_SHARE_TOLERANCE=0.20
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_ROLLBACK_ADVISORY_ONLY=1
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_ROLLBACK_EXECUTOR_MODE=DRY_RUN
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_ROLLBACK_ALLOW_COMMIT=0
```

## Smoke checks
```bash
curl -s localhost:9982/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_verification_'
curl -s localhost:9983/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_rollback_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_verification:last
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_rollback:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_verification_results + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_rollback_journal + - COUNT 5
```

## Notes
- scope only `scanner_infra`
- hot path untouched
- this phase still assumes commit stays off by default
- next step can add SLO/MTTR + retry/escalation for this experiment safety loop
