# Phase 3.36 — Route Incident RCA Mirror RCA Winner-Apply Apply Governance Bundle -> Vertex/Local RCA Bridge

## Цель
Дать новому `route_incident_rca mirror RCA winner-apply apply governance incident bundle` contour собственный RCA flow,
а не смешивать его с другими RCA-контурами.

## Что делает
- читает bundle events из:
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_incident_bundles`
- читает Vertex health из:
  - `metrics:ml:vertex_health:last`
- принимает routing decision:
  - `ROUTE_VERTEX`
  - `ROUTE_LOCAL`
  - `REJECT`
- пишет:
  - dedicated Vertex RCA requests:
    - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca_requests`
  - local fallback requests:
    - `stream:ml:local_fallback_requests`

## Routing policy
- `AUTO`
- `VERTEX_ONLY`
- `LOCAL_ONLY`
- `DISABLED`

## Что отправляется
- в Vertex stream:
  - отдельный `route_incident_rca_mirror_rca_winner_apply_apply_governance_rca` request с `bundle_json`
- в local fallback:
  - bounded `vertex_unavailable_fallback`
  - `task_family=route_incident_rca_mirror_rca_winner_apply_apply_governance_rca`
  - `source=route_incident_rca_mirror_rca_winner_apply_apply_governance_bundle_rca_bridge_v3_36`

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_RCA_BRIDGE_MODE=AUTO
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_RCA_BRIDGE_REQUIRE_VERTEX_DEGRADED_FOR_LOCAL=1
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_RCA_BRIDGE_MAX_BUNDLE_BYTES=131072
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_RCA_BRIDGE_MAX_PROMPT_CHARS=12000
```

## Smoke checks
```bash
curl -s localhost:9963/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_rca_bridge_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_rca_bridge:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_rca_bridge_decisions + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca_requests + - COUNT 5
redis-cli XREVRANGE stream:ml:local_fallback_requests + - COUNT 5
```

## Notes
- scope только `scanner_infra`
- hot path не затронут
- dedicated Vertex RCA stream не смешивается с предыдущим apply bundle bridge
- local fallback path остаётся bounded
