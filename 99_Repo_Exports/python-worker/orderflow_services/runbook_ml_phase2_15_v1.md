# Phase 2.15 — Routing Incident RCA Route Safety Loop

## Цель
Автоматический контроль за default routing policy. Post-apply verifier смотрит на метрики и принимает решение об отмене (rollback) если метрики падают. Rollback executor восстанавливает предыдущую конфигурацию (или hardcoded fallback).

## Компоненты 
1. `operator_routing_incident_rca_routing_post_apply_verifier_v2_15.py`
2. `operator_routing_incident_rca_routing_rollback_executor_v2_15.py`

## Потоки
- in: `stream:ml:operator_routing_incident_rca_routing_apply_results`
- in: `stream:ml:operator_routing_incident_rca_routing_rollback_requests`
- out: `stream:ml:operator_routing_incident_rca_routing_verify_results`
- out: `stream:ml:operator_routing_incident_rca_routing_rollback_results`

## Smoke checks
```bash
redis-cli XREVRANGE stream:ml:operator_routing_incident_rca_routing_verify_results + - COUNT 5
redis-cli XREVRANGE stream:ml:operator_routing_incident_rca_routing_rollback_requests + - COUNT 5
redis-cli XREVRANGE stream:ml:operator_routing_incident_rca_routing_rollback_results + - COUNT 5
```
