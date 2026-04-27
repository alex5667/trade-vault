# Phase 3.53 — Apply-Flow Experiment Incident Bundle -> Vertex/Local RCA Bridge

## Цель
Сделать новый isolated `experiment incident bundle -> Vertex/local RCA bridge`,
чтобы experiment-forensics contour получил свой отдельный RCA flow
и не смешивался с обычным routing incident RCA.

## Что делает
- читает только:
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_bundles`
- принимает isolated routing decision
- шлёт RCA requests в dedicated streams:
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_vertex_rca_requests`
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_local_rca_requests`
- пишет bridge decisions в:
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_bridge_decisions`

## Routing
- `AUTO`
  - `critical` -> `VERTEX`
  - `warning` -> `LOCAL`
- override modes:
  - `VERTEX_ONLY`
  - `LOCAL_ONLY`
  - `DISABLED`

## Safe behavior
- no change to old routing RCA bridge
- no change to regular operator_rca path
- only new experiment incident bundles enter this bridge

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_BRIDGE_MODE=AUTO
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_BRIDGE_ALLOW_SEVERITIES=warning,critical
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_BRIDGE_VERTEX_FOR_SEVERITIES=critical
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_BRIDGE_LOCAL_FOR_SEVERITIES=warning
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_BRIDGE_TASK_TIMEOUT_SEC=900
```

## Smoke checks
```bash
curl -s localhost:9987/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_bridge_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_bridge:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_bridge_decisions + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_vertex_rca_requests + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_local_rca_requests + - COUNT 5
```

## Notes
- scope only `scanner_infra`
- hot path untouched
- next step: dedicated experiment incident RCA result consumer + feedback loop
