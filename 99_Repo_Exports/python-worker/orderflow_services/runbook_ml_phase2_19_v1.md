# Phase 2.19 — Routing Incident Route RCA Governance

## Цель
Трекинг качества результатов RCA и сбор человеческой/автоматизированной обратной связи (Usefulness score).

## Компоненты 
1. `operator_routing_incident_route_rca_results_persister_v2_19.py`
2. `operator_routing_incident_route_rca_quality_scorer_v2_19.py`
3. `operator_routing_incident_route_rca_feedback_loop_v2_19.py`

## Потоки
- in: `stream:ml:operator_routing_incident_rca_route_rca_results` 
- out: `stream:ml:operator_routing_incident_rca_route_rca_quality`
- in: `stream:ml:operator_routing_incident_rca_route_rca_quality`
- out: `stream:ml:operator_routing_incident_rca_route_rca_quality_results`
- in: `stream:ml:operator_routing_incident_rca_route_rca_feedback`
- out: `stream:ml:operator_routing_incident_rca_route_rca_feedback_summary`

## Базы данных SQL
- `llm_operator_routing_incident_route_rca_results`
- `llm_operator_routing_incident_route_rca_quality`
- `llm_operator_routing_incident_route_rca_feedback`

## Smoke checks
```bash
redis-cli XREVRANGE stream:ml:operator_routing_incident_rca_route_rca_results + - COUNT 2
```
