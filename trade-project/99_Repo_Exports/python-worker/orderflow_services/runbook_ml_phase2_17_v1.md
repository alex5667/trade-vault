# Phase 2.17 — Routing Incident RCA Route Governance Bundle

## Цель
Формирование единого 'Forensic' бандла (агрегированного отчета) по всем событиям RCA Governance.

## Компоненты 
1. `operator_routing_incident_rca_route_incident_bundle_builder_v2_17.py`

## Потоки
- in: `stream:ml:operator_routing_incident_rca_route_incident_bundle_requests`
- out: `stream:ml:operator_routing_incident_rca_route_incident_bundle_results`
- hash: `metrics:ml:operator_routing_incident_rca_route_incident_bundle:last`
- list: `llm_operator_routing_incident_rca_route_incident_bundles`

## Smoke checks
```bash
redis-cli XREVRANGE stream:ml:operator_routing_incident_rca_route_incident_bundle_results + - COUNT 2
redis-cli HGETALL metrics:ml:operator_routing_incident_rca_route_incident_bundle:last
```
