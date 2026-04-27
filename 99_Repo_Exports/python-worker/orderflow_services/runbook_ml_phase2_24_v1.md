# Phase 2.24 — Routing Incident Route RCA Safety Loop

## Цель
Пост-верификация примененных изменений маршрутизации и автоматический откат при нарушении SLO или деградации полезности (usefulness).

## Компоненты 
1. `operator_routing_incident_route_rca_routing_post_apply_verifier_v2_24.py`
2. `operator_routing_incident_route_rca_routing_rollback_executor_v2_24.py`

## Потоки
- in: `stream:ml:operator_routing_incident_route_rca_routing_apply_results`
- verify_out: `stream:ml:operator_routing_incident_route_rca_routing_verify_results`
- rollback_req: `stream:ml:operator_routing_incident_route_rca_routing_rollback_requests`
- rollback_res: `stream:ml:operator_routing_incident_route_rca_routing_rollback_results`

## Smoke checks
```bash
redis-cli XREVRANGE stream:ml:operator_routing_incident_route_rca_routing_verify_results + - COUNT 2
redis-cli XREVRANGE stream:ml:operator_routing_incident_route_rca_routing_rollback_journal + - COUNT 2
```
