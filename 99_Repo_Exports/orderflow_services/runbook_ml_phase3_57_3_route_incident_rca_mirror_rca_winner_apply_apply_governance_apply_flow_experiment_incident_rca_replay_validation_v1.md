# Phase 3.57.3 runbook

## Purpose

- deterministic replay validation for fixed time window
- compare Redis stream rows vs Timescale rows
- produce golden report for slo/retry/escalation aliases

## Safe order

1. Freeze a validation window.
2. Ensure backfill/reconcile are clean for that window.
3. Run replay validator once.
4. Inspect report stream and hashes.

## Commands

```bash
\i orderflow_services/sql/ml_phase3_57_3_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_replay_validation_v1.sql

export ML_ROUTE_INCIDENT_RCA_APPLY_REPLAY_RUNNER_WINDOW_MIN=60
export ML_ROUTE_INCIDENT_RCA_APPLY_REPLAY_RUNNER_WINDOW_END_TS_MS=$(date +%s%3N)

docker compose -f orderflow_services/docker_compose_fragment_ml_phase3_57_3_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_replay_validation_v1.yml up ml-route-incident-rca-apply-replay-validator-v3-57-3

redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_validation_reports + - COUNT 10
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_validation:last
```

## Notes

- use fixed epoch-ms window
- do not validate an actively mutating window
- hash is computed only on canonical subset fields
