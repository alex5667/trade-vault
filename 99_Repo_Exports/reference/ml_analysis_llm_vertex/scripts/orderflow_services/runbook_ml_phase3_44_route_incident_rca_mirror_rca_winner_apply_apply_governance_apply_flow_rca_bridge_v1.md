# Phase 3.44 — Route Incident RCA Mirror RCA Winner-Apply Apply Governance Apply-Flow Bundle -> Vertex/Local RCA Bridge

## Цель
Дать новому `route_incident_rca mirror RCA winner-apply apply governance apply-flow incident bundle` contour
собственный RCA flow, не смешанный с `Phase 3.36/3.37`.

## Что делает
- читает bundles из:
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_incident_bundles`
- читает Vertex health из:
  - `metrics:ml:vertex_health:last`
- принимает routing decision:
  - `ROUTE_VERTEX`
  - `ROUTE_LOCAL`
  - `REJECT`

## Routing policy
- `AUTO`
- `VERTEX_ONLY`
- `LOCAL_ONLY`
- `DISABLED`

## Куда пишет
- dedicated Vertex RCA stream:
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_vertex_rca_requests`
- dedicated local RCA stream:
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_local_rca_requests`
- bridge decisions:
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_bridge_decisions`

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_RCA_BRIDGE_MODE=AUTO
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_RCA_BRIDGE_REQUIRE_VERTEX_DEGRADED_FOR_LOCAL=1
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_RCA_BRIDGE_MAX_BUNDLE_BYTES=196608
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_RCA_BRIDGE_MAX_PROMPT_CHARS=12000
```

## Smoke checks
```bash
curl -s localhost:9974/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_bridge_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_bridge:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca_bridge_decisions + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_vertex_rca_requests + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_local_rca_requests + - COUNT 5
```

## Notes
- scope только `scanner_infra`
- hot path не затронут
- dedicated streams separate this contour from previous governance bridge
- next step: dedicated apply-flow RCA consumer + feedback loop
