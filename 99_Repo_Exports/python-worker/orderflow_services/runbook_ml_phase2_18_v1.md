# Phase 2.18 — Routing Incident RCA Vertex Bridge

## Цель
Формирование моста (Bridge) между `stream:ml:operator_routing_incident_rca_route_incident_bundle_results` и инфраструктурой LLM Vertex.

## Компоненты 
1. `operator_routing_incident_rca_route_incident_to_vertex_bridge_v2_18.py` (Bridge payload construction)
2. `ml_operator_routing_incident_route_rca_orchestrator_v2_18.py` (Vertex LLM execution)

## Потоки
- in: `stream:ml:operator_routing_incident_rca_route_incident_bundle_results` 
- out: `stream:ml:operator_routing_incident_rca_route_rca_requests`
- in: `stream:ml:operator_routing_incident_rca_route_rca_requests`
- out: `stream:ml:operator_routing_incident_rca_route_rca_results`
- out: `stream:ml:recommendation_proposals`
- dlq: `stream:ml:operator_routing_incident_rca_route_rca_dlq`

## Smoke checks
```bash
redis-cli XREVRANGE stream:ml:operator_routing_incident_rca_route_rca_results + - COUNT 2
```
