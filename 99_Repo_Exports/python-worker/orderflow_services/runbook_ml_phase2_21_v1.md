# Phase 2.21 — Routing Incident Route RCA Routing Controller

## Цель
Централизованный выбор провайдера, модели и версии промпта для RCA на основе данных от губернатора.

## Компоненты 
1. `operator_routing_incident_route_rca_routing_controller_v2_21.py`

## Потоки
- in: `stream:ml:operator_routing_incident_rca_route_rca_requests`
- out: `stream:ml:operator_routing_incident_route_rca_routing_decisions`
- out: `stream:ml:operator_routing_incident_route_rca_routing_audit`
- out: `stream:ml:operator_routing_incident_rca_route_rca_requests_routed`

## Редис
- metric: `metrics:ml:operator_routing_incident_route_rca_routing:last`

## Smoke checks
```bash
redis-cli XREVRANGE stream:ml:operator_routing_incident_route_rca_routing_decisions + - COUNT 2
redis-cli HGETALL metrics:ml:operator_routing_incident_route_rca_routing:last
```
