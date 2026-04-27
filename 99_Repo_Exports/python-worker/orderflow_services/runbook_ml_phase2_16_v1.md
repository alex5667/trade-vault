# Phase 2.16 — Routing Incident RCA Route Governance

## Цель
Поднятие верхнеуровневого governance слоя: аналитика метрик (SLO/MTTR_p95), ограниченные ретраи при падении, автоматическая эскалация severity событий, касающихся аналитических маршрутов RCA.

## Компоненты 
1. `operator_routing_incident_rca_route_slo_analytics_v2_16.py`
2. `operator_routing_incident_rca_route_retry_controller_v2_16.py`
3. `operator_routing_incident_rca_route_auto_escalation_summarizer_v2_16.py`

## Потоки
- in: `stream:ml:operator_routing_incident_rca_routing_verify_results`
- out: `stream:ml:operator_routing_incident_rca_route_slo_rollups`
- out: `stream:ml:operator_routing_incident_rca_route_retry_requests`
- out: `stream:ml:operator_routing_incident_rca_route_escalations`
- hash: `metrics:ml:operator_routing_incident_rca_route_slo:last`

## Упавшие пути
`LOW_EXPOSURE` / `FEEDBACK_STALE` / `ROUTE_MISMATCH` → Retry `v2.16`

## Эскалация
`SLO_BREACH` → CRITICAL
Иные сбои → WARNING
