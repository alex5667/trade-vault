# Phase 2.22 — Routing Incident Route RCA Experiment Harness

## Цель
Проведение A/B экспериментов по выбору лучшей политики (маршрутизации) для RCA аналитических инцидентов.

## Компоненты 
1. `operator_routing_incident_route_rca_experiment_router_v2_22.py` (Деление на бакеты: control/challenger)
2. `operator_routing_incident_route_rca_experiment_winner_selector_v2_22.py` (Выбор победителя)

## Потоки
- in: `stream:ml:operator_routing_incident_route_rca_routing_decisions`
- out: `stream:ml:operator_routing_incident_route_rca_requests_experimented`
- out: `stream:ml:operator_routing_incident_route_rca_exposures`
- audit: `stream:ml:operator_routing_incident_route_rca_experiment_audit`

## Редис
- key: `cfg:ml:operator_routing_incident_route_rca_experiment:winner:<experiment_id>`

## Smoke checks
```bash
redis-cli XREVRANGE stream:ml:operator_routing_incident_route_rca_exposures + - COUNT 2
redis-cli HGETALL cfg:ml:operator_routing_incident_route_rca_experiment:winner:route_incident_rca_ab_v1
```
