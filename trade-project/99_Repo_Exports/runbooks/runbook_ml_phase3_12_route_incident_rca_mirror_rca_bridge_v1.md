# Phase 3.12 — Route Incident RCA Mirror RCA Bridge

## Цель
Этот шаг даёт новому mirror incident bundle контуру собственный RCA flow.
Слушает события из `stream:ml:route_incident_rca_mirror_incident_bundles` и и направляет запросы либо в dedicated Vertex RCA stream, либо в bounded local fallback (при недоступности облачного провайдера).

## Routing Policy Modes
- `AUTO`: Использует Vertex, если он здоровый (основано на метрике Vertex health). Если degraded, падает на local fallback.
- `VERTEX_ONLY`: Принудительно направляет весь RCA трафик в Vertex, игнорируя здоровье.
- `LOCAL_ONLY`: Принудительно направляет весь трафик на локальный RCA fallback.
- `DISABLED`: Отклоняет все бандлы.

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_BRIDGE_MODE=AUTO
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_BRIDGE_REQUIRE_VERTEX_DEGRADED_FOR_LOCAL=1
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_BRIDGE_MAX_BUNDLE_BYTES=131072
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_BRIDGE_MAX_PROMPT_CHARS=12000
```

## Smoke checks
```bash
curl -s localhost:9930/metrics | grep '^ml_route_incident_rca_mirror_rca_bridge_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_bridge:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_bridge_decisions + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_vertex_rca_requests + - COUNT 5
redis-cli XREVRANGE stream:ml:local_fallback_requests + - COUNT 5
```
