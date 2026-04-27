# Phase 3.28 — Route Incident RCA Mirror RCA Winner Apply Apply RCA Bridge

## Цель
Умный роутер (Bridge) для Forensic Bundles созданных в Phase 3.27.
Определяет, куда направить сформированный Apply-инцидент для аналитики:
- В облачный Vertex AI (через stream `route_incident_rca_mirror_rca_winner_apply_apply_vertex_rca_requests`).
- В локальный Fallback OLLAMA (если Vertex недоступен; через поток `local_fallback_requests`).

Незначительные bundle (`severity=info`) игнорируются для экономии.

## Smoke checks
```bash
curl -s localhost:9952/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_rca_bridge_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_rca_bridge:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_rca_bridge_decisions + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_vertex_rca_requests + - COUNT 5
redis-cli XREVRANGE stream:ml:local_fallback_requests + - COUNT 5
```
