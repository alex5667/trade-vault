# Phase 3.20 — Route Incident RCA Mirror RCA Winner Apply RCA Bridge

## Цель
Забирает сгенерированные Forensic-пакеты (Incident Bundles) из контура Winner Apply (созданные на предыдущем этапе 3.19) и маршрутизирует их на RCA-решение. Обеспечивает fallback на local Inference Plane (Ollama) если основной шлюз Vertex AI недоступен.

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_RCA_BRIDGE_MODE=AUTO
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_RCA_BRIDGE_REQUIRE_VERTEX_DEGRADED_FOR_LOCAL=1
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_RCA_BRIDGE_MAX_BUNDLE_BYTES=131072
```

## Smoke checks
```bash
curl -s localhost:9941/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_rca_bridge_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_rca_bridge:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_rca_bridge_decisions + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_vertex_rca_requests + - COUNT 5
redis-cli XREVRANGE stream:ml:local_fallback_requests + - COUNT 5
```
