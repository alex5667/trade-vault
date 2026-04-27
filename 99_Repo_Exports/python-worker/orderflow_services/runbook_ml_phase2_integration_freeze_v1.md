# Phase 2 — Integration Freeze Auditor

## Цель
Единый гейт для проверки готовности всех компонентов Phase 2 перед переходом к Phase 3 (Enforce Mode). Проверяет живость и свежесть всех критических стримов и хэшей.

## Verdicts
- **GO**: Все критические и опциональные проверки пройдены.
- **WARN**: Все критические проверки пройдены, но есть проблемы с опциональными или свежестью данных.
- **NO_GO**: Хотя бы одна критическая проверка провалена (стрим пуст или отсутствует).

## Smoke checks
```bash
curl -s localhost:9915/metrics | grep '^ml_phase2_integration_freeze_'
redis-cli HGETALL metrics:ml:phase2_integration_freeze:last
```

## Critical Streams to Watch
1. `stream:ml:operator_routing_incident_rca_experiment_winner_decisions`
2. `stream:ml:operator_routing_incident_route_rca_routing_apply_results`
3. `stream:ml:operator_routing_incident_route_rca_routing_verify_results`
