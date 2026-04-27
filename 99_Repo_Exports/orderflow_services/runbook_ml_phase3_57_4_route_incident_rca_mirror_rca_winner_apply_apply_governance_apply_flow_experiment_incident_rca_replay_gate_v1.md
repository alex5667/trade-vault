# Phase 3.57.4 runbook

## Purpose

- run fixed-window replay validation
- convert validator reports into PASS/BLOCK gate decision
- use same decision in CI and nightly

## Safe order

1. Choose closed window only.
2. Run validator.
3. Run gate.
4. Inspect gate_decisions stream.
5. Use non-zero exit code to fail CI/nightly.

## Commands

```bash
\i orderflow_services/sql/ml_phase3_57_4_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_replay_gate_v1.sql

export ML_ROUTE_INCIDENT_RCA_APPLY_REPLAY_GATE_RUNNER_WINDOW_MIN=60
export ML_ROUTE_INCIDENT_RCA_APPLY_REPLAY_GATE_RUNNER_LAG_MIN=15

docker compose -f orderflow_services/docker_compose_fragment_ml_phase3_57_4_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_replay_gate_v1.yml up ml-route-incident-rca-apply-replay-gate-v3-57-4

redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_gate_decisions + - COUNT 10
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_gate:last
```
