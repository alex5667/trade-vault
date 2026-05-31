# Phase 2.23 — Routing Incident Route RCA Winner Routing Apply

## Цель
Автоматическое (или консультативное) применение выигравшей политики эксперимента к дефолтным настройкам маршрутизации RCA.

## Компоненты 
1. `operator_routing_incident_route_rca_winner_routing_apply_controller_v2_23.py`

## Потоки
- in: `stream:ml:operator_routing_incident_route_rca_experiment_winner_decisions`
- out: `stream:ml:operator_routing_incident_route_rca_routing_apply_results`
- audit: `stream:ml:operator_routing_incident_route_rca_routing_apply_audit`

## Редис
- key: `cfg:ml:operator_routing_incident_route_rca_routing:default` (Default routing policy)

## Smoke checks
```bash
redis-cli XREVRANGE stream:ml:operator_routing_incident_route_rca_routing_apply_results + - COUNT 2
redis-cli HGETALL cfg:ml:operator_routing_incident_route_rca_routing:default
```
