# Phase 2.20 — Routing Incident Route RCA Governance Usefulness

## Цель
Трекинг качества результатов RCA и сбор человеческой/автоматизированной обратной связи (Usefulness score).

## Компоненты 
1. `operator_routing_incident_route_rca_usefulness_governor_v2_20.py`

## Потоки
- in: `stream:ml:operator_routing_incident_rca_route_rca_feedback_summary` 
- out: `stream:ml:operator_routing_incident_route_rca_governor_decisions`

## Редис
- hash: `cfg:ml:operator_routing_incident_route_rca_governor:action:*`
- hash: `cfg:ml:operator_routing_incident_route_rca_governor:provider:*`

## Smoke checks
```bash
redis-cli XREVRANGE stream:ml:operator_routing_incident_route_rca_governor_decisions + - COUNT 2
redis-cli HGETALL cfg:ml:operator_routing_incident_route_rca_governor:action:open_incident:routing_incident_route_rca_v1:policy_v1
```
