# Phase 2.25 — Routing Incident Route RCA Governance

## Цель
Аналитика SLO (MTTR/Success Rate), управление повторными попытками (Retry) и агрегация эскалаций для контура маршрутизации RCA.

## Компоненты 
1. `operator_routing_incident_route_rca_route_slo_analytics_v2_25.py`
2. `operator_routing_incident_route_rca_route_retry_controller_v2_25.py`
3. `operator_routing_incident_route_rca_route_auto_escalation_summarizer_v2_25.py`

## Потоки
- SLO in: `stream:ml:operator_routing_incident_route_rca_routing_verify_results`
- SLO out: `stream:ml:operator_routing_incident_route_rca_route_slo_rollups`
- Retry out: `stream:ml:operator_routing_incident_route_rca_route_retry_requests`
- Escalation out: `stream:ml:operator_routing_incident_route_rca_route_escalations`

## Smoke checks
```bash
redis-cli HGETALL metrics:ml:operator_routing_incident_route_rca_route_slo:last
redis-cli XREVRANGE stream:ml:operator_routing_incident_route_rca_route_retry_requests + - COUNT 2
redis-cli XREVRANGE stream:ml:operator_routing_incident_route_rca_route_escalations + - COUNT 2
```
