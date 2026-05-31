# Phase 3.57.4.1 runbook

## Purpose

- persist replay gate decisions to Timescale
- build scanner_infra dashboard snapshot for validator -> gate chain

## Safe order

1. Apply SQL.
2. Start gate-decision persister.
3. Verify Timescale rows for replay_gate_decisions.
4. Start dashboard snapshot worker.
5. Verify snapshot stream/hash/table.

## Commands

```bash
\i orderflow_services/sql/ml_phase3_57_4_1_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_replay_gate_persist_dashboard_v1.sql

docker compose -f orderflow_services/docker_compose_fragment_ml_phase3_57_4_1_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_replay_gate_persist_dashboard_v1.yml up -d

curl -s localhost:10002/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_gate_decisions_persister_v3_57_4_1_'
curl -s localhost:10003/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_dashboard_snapshot_v3_57_4_1_'

redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_dashboard_snapshots + - COUNT 5
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_dashboard:last
```
