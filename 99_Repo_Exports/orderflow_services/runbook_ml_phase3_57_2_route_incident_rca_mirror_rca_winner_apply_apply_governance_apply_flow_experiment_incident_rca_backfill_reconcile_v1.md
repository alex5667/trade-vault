# Phase 3.57.2 runbook

## Purpose

- historical backfill from 3.57 streams into Timescale
- replay-safe upsert
- Redis↔Timescale reconciliation

## Safe order

1. Apply SQL.
2. Run backfill in DRY_RUN.
3. Inspect backfill_runs + DLQ.
4. Run same window in COMMIT.
5. Start reconcile auditor.

## Commands

```bash
\i orderflow_services/sql/ml_phase3_57_2_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_backfill_reconcile_v1.sql

export ML_ROUTE_INCIDENT_RCA_APPLY_BACKFILL_MODE=DRY_RUN
export ML_ROUTE_INCIDENT_RCA_APPLY_BACKFILL_START_ID=0-0
export ML_ROUTE_INCIDENT_RCA_APPLY_BACKFILL_END_ID=+
docker compose -f orderflow_services/docker_compose_fragment_ml_phase3_57_2_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_backfill_reconcile_v1.yml up ml-route-incident-rca-apply-backfill-replayer-v3-57-2

redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_backfill_runs + - COUNT 10
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_backfill_dlq + - COUNT 10

export ML_ROUTE_INCIDENT_RCA_APPLY_BACKFILL_MODE=COMMIT
docker compose -f orderflow_services/docker_compose_fragment_ml_phase3_57_2_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_backfill_reconcile_v1.yml up ml-route-incident-rca-apply-backfill-replayer-v3-57-2

docker compose -f orderflow_services/docker_compose_fragment_ml_phase3_57_2_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_backfill_reconcile_v1.yml up -d ml-route-incident-rca-apply-reconcile-auditor-v3-57-2
```
