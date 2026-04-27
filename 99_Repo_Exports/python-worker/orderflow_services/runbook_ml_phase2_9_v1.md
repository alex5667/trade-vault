# Phase 2.9 — Incident Bundle → Vertex Routing RCA Bridge

## Цель
Преобразовать forensic bundle по route change в compact Vertex RCA input и получить structured RCA result.

## Компоненты
- `operator_rca_routing_incident_to_vertex_bridge_v2_9.py`
- `vertex_routing_incident_rca_provider_v2_9.py`
- `ml_operator_routing_incident_rca_orchestrator_v2_9.py`

## Потоки
- input: `stream:ml:operator_rca_routing_incident_bundle_results`
- request: `stream:ml:operator_rca_routing_rca_requests`
- result: `stream:ml:operator_rca_routing_rca_results`
- proposals: `stream:ml:recommendation_proposals`
- dlq: `stream:ml:operator_rca_routing_rca_dlq`

## Safe start
```bash
export VERTEX_ROUTING_INCIDENT_RCA_DRY_RUN=1
export VERTEX_PROJECT_ID=...
export VERTEX_LOCATION=global
export VERTEX_ROUTING_INCIDENT_RCA_MODEL=gemini-2.5-flash-lite
```

## Smoke checks
```bash
redis-cli XREVRANGE stream:ml:operator_rca_routing_incident_bundle_results + - COUNT 3
redis-cli XREVRANGE stream:ml:operator_rca_routing_rca_requests + - COUNT 3
redis-cli XREVRANGE stream:ml:operator_rca_routing_rca_results + - COUNT 3
redis-cli XREVRANGE stream:ml:recommendation_proposals + - COUNT 3
curl -s localhost:9884/metrics | grep '^ml_operator_rca_routing_rca_bridge_'
curl -s localhost:9885/metrics | grep '^ml_operator_routing_incident_rca_'
```

## Notes
- scope только `scanner_infra`
- hot path не затронут
- advisory-only
- proposals выходят review-only
